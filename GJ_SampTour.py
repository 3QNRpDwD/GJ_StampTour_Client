#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GJ_SampTour_Client.py
Rust 서버(Actix-web) 규격에 맞춘 어드민 클라이언트 스크립트

수정 내역:
  - 서버가 이제 'stamp status' 요청에 대해 표준 JSON 문자열을 반환합니다.
  - 이에 따라 불안정한 텍스트 파싱 로직을 제거하고 표준 JSON 파싱으로 교체했습니다.
"""

import requests
import json
import pandas as pd
import threading
import time
import logging
import csv
import os
import argparse
import unicodedata
import re

# -------------------------
# 설정
# -------------------------
ADMIN_URL = "http://localhost:80/admin"
AUTOSAVE_INTERVAL_MIN = 1
DEFAULT_EXCEL_PATH = "./stamp_user.xlsx"
DEFAULT_CSV_PATH = "./final_event_result.csv"
DEFAULT_INPUT_JSON = "resources/database/stamp_status.json"
LOG_FILE = "stamp_processor.log"
MIN_REQUIRED_UNIQUE = 5
# -------------------------

# -------------------------
# 로거 설정
# -------------------------
logger = logging.getLogger("stamp_processor")
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s"))
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S"))
logger.addHandler(ch)
# -------------------------

def normalize_user_name(name: str) -> str:
    if name is None:
        return ""
    s = str(name)
    s = unicodedata.normalize("NFKC", s)
    s = s.strip()
    s = re.sub(r"\s+", "", s)
    try:
        s = s.lower()
    except Exception:
        pass
    return s

def dedupe_user_count_by_name(user_count: dict):
    name_to_best = {}
    for uid, info in user_count.items():
        name = info.get("user_name", "")
        cnt = int(info.get("count", 0))
        if name == "":
            name = f"<unknown:{uid}>"
        cur = name_to_best.get(name)
        if cur is None or cnt > cur["count"]:
            name_to_best[name] = {"user_id": uid, "count": cnt, "user_name": name}
    result = {}
    for name, chosen in name_to_best.items():
        result[chosen["user_id"]] = {"count": chosen["count"], "user_name": chosen["user_name"]}
    return result

def process_history_to_outputs(history: dict, 
                               excel_path=DEFAULT_EXCEL_PATH, 
                               csv_path=DEFAULT_CSV_PATH, 
                               min_required_unique=MIN_REQUIRED_UNIQUE):
    """
    기존 파라미터 규격을 유지하면서, 
    CSV 저장 시에만 '학번이름'을 스탬프 개수(10개당 1번)만큼 반복 저장합니다.
    """
    logger.info("데이터 집계 및 출력 시작. Excel: '%s', CSV: '%s'", excel_path, csv_path)
    
    user_stats = {} 
    # 1. 데이터 집계
    logger.debug("===== 1. 데이터 집계 시작 =====")
    for stamp_id, users in history.items():
        logger.debug("스탬프 ID '%s' 처리 중. 사용자 수: %d", stamp_id, len(users))
        for u in users:
            if isinstance(u, dict):
                uid = u.get("student_id") or u.get("user_id") or u.get("id")
                raw_name = u.get("user_name") or u.get("name") or ""
            else:
                uid = str(u)
                raw_name = ""
            
            if uid is None:
                logger.warning("UID가 없어 건너뜁니다: %s", u)
                continue
            
            norm_name = normalize_user_name(raw_name)
            
            if uid not in user_stats:
                logger.debug("새 사용자 발견: UID=%s, 이름=%s", uid, norm_name)
                user_stats[uid] = {"name": norm_name, "locations": set()}
            
            if str(stamp_id) not in user_stats[uid]["locations"]:
                user_stats[uid]["locations"].add(str(stamp_id))
                logger.debug("UID '%s'에 스탬프 '%s' 추가. 현재 총 %d개", uid, stamp_id, len(user_stats[uid]["locations"]))
            else:
                logger.debug("UID '%s'에 스탬프 '%s'는 이미 존재하여 건너뜁니다.", uid, stamp_id)

    logger.info("데이터 집계 완료. 총 %d명의 고유 사용자 발견.", len(user_stats))
    logger.debug("===== 1. 데이터 집계 종료 =====")

    # 2. 엑셀 저장
    logger.debug("===== 2. Excel 파일 저장 시작 =====")
    try:
        df_data = []
        for uid, info in user_stats.items():
            count = len(info["locations"])
            df_data.append({"user_id": uid, "user_name": info["name"], "count": count})
            logger.debug("Excel 데이터 준비: UID=%s, 이름=%s, 스탬프 수=%d", uid, info["name"], count)
        
        df = pd.DataFrame(df_data)
        df.to_excel(excel_path, index=False)
        logger.info("Excel 저장 완료: '%s'에 %d명 데이터 저장", excel_path, len(df_data))
    except Exception as e:
        logger.error("Excel 저장 실패: %s", e)
    logger.debug("===== 2. Excel 파일 저장 종료 =====")

    # 3. CSV 저장
    logger.debug("===== 3. CSV 추첨 명단 저장 시작 =====")
    count_total_rows = 0
    try:
        with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            logger.info("CSV 파일 개방 완료: '%s'", csv_path)
            
            for uid, info in user_stats.items():
                cnt = len(info['locations'])
                display_name = info['name']
                logger.debug("CSV 처리 사용자: UID=%s, 이름=%s, 스탬프 수=%d", uid, display_name, cnt)
                
                # 10개 미만은 제외
                if cnt < min_required_unique:
                    logger.debug("사용자 '%s' (UID: %s)는 스탬프 개수(%d)가 최소 요건(%d) 미만이라 제외.", display_name, uid, cnt, min_required_unique)
                    continue
                
                # 출력 횟수: 10개 이상 1번, 20개 이상 2번...
                repeat_count = cnt // 10
                logger.debug("사용자 '%s' (UID: %s)는 추첨 명단에 %d번 포함됩니다.", display_name, uid, repeat_count)
                
                for i in range(repeat_count):
                    writer.writerow([display_name])
                    logger.debug(" > '%s' 1회 기록 (총 %d회 중 %d번째)", display_name, repeat_count, i+1)
                    count_total_rows += 1
            
            logger.info("CSV 저장 완료: 총 %d줄 기록됨", count_total_rows)
    except Exception as e:
        logger.error("CSV 저장 실패: %s", e)
    logger.debug("===== 3. CSV 추첨 명단 저장 종료 =====")

    return {"total_users": len(user_stats), "recorded_rows": count_total_rows}

def parse_admin_response_output(output_text: str):
    """
    [수정됨]
    Rust 서버가 이제 serde_json::to_string으로 직렬화된 유효한 JSON 문자열을 반환합니다.
    복잡한 문자열 처리 없이 json.loads로 파싱합니다.
    """
    if not output_text:
        return None

    try:
        # 1. JSON 문자열 파싱
        parsed = json.loads(output_text)
    except json.JSONDecodeError:
        # "save all" 등의 명령어는 일반 텍스트(예: "All databases saved")를 반환하므로 무시
        return None

    # 2. stamp_history 추출
    if isinstance(parsed, dict):
        # Rust 구조체: struct StampHistory { stamp_history: HashMap<...> }
        # JSON 형태: {"stamp_history": { ... }}
        if "stamp_history" in parsed:
            return parsed["stamp_history"]
        
        # 만약 구조가 직접 Map이라면 그대로 반환
        return parsed

    return None

def process_file(input_file=DEFAULT_INPUT_JSON):
    logger.info("로컬 파일 처리 시작: %s", input_file)
    if not os.path.exists(input_file):
        logger.error("파일을 찾을 수 없습니다: %s", input_file)
        return
    
    try:
        logger.debug("파일 열기: %s", input_file)
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.debug("JSON 파싱 완료. 최상위 키: %s", list(data.keys()))
        
        # 'stamp_history' 키가 있는지 확인, 없으면 data 자체를 history로 간주
        if 'stamp_history' in data:
            history = data['stamp_history']
            logger.info("'stamp_history' 키를 발견하여 사용합니다. 스탬프 종류: %d개", len(history))
        else:
            history = data
            logger.info("최상위 객체를 history로 사용합니다. 스탬프 종류: %d개", len(history))
            
        process_history_to_outputs(history)
        logger.info("로컬 파일 처리 완료: %s", input_file)
        
    except json.JSONDecodeError as e:
        logger.error("JSON 파싱 오류: %s - %s", input_file, e)
    except Exception as e:
        logger.error("파일 처리 중 예외 발생: %s", e)

# -------------------------
# 스레드 함수들
# -------------------------
def auto_save_loop(interval_min, admin_url):
    logger.info("Autosave 시작 (주기: %d분)", interval_min)
    while True:
        try:
            time.sleep(interval_min * 60)
            logger.debug("Autosave: save all 전송")
            # save all은 데이터를 반환하지 않으므로 전송만 함
            requests.post(admin_url, json={"command": "save all", "output": ""}, timeout=10)
        except Exception:
            pass # 조용히 무시

def handle_cmd_loop(admin_url):
    logger.info("명령 대기 중... (사용 가능 명령: save all, stamp status, process_file, exit)")
    
    while True:
        try:
            cmd = input("Command> ").strip()
            if not cmd:
                continue
                
            if cmd.lower() in ("exit", "quit"):
                logger.info("종료합니다.")
                os._exit(0)

            # 1. 로컬 파일 처리 명령
            if cmd.startswith("process_file"):
                parts = cmd.split(maxsplit=1)
                logger.debug("'process_file' 명령 분리 결과: %s", parts)
                target_file = parts[1].strip() if len(parts) > 1 else DEFAULT_INPUT_JSON
                logger.info("'process_file' 명령 실행. 대상 파일: %s", target_file)
                process_file(target_file)
                continue

            # 2. 서버 전송 명령
            logger.debug("서버 전송: %s", cmd)
            try:
                r = requests.post(admin_url, json={"command": cmd, "output": ""}, timeout=10)
                r.raise_for_status() # HTTP 에러 체크
                
                resp_json = r.json()
                output_text = resp_json.get("output", "")
            except Exception as e:
                logger.error("통신 오류 또는 잘못된 응답: %s", e)
                continue

            # 3. 명령어에 따른 응답 처리 분기
            if cmd == "save all":
                # save all은 데이터를 주지 않음. 메시지만 출력.
                logger.info("서버 응답: %s", output_text)
            
            elif cmd == "stamp status":
                # stamp status는 이제 JSON String을 줌
                logger.info("데이터 수신 완료. JSON 파싱 시작...")
                
                # [수정됨] 새 파싱 함수 호출
                history = parse_admin_response_output(output_text)
                
                if history:
                    summary = process_history_to_outputs(history)
                    logger.info("처리 완료: %s", summary)
                else:
                    logger.warning("데이터 파싱 실패 혹은 데이터 없음.")
                    if output_text and len(output_text) > 200:
                         logger.debug("Raw Output (앞부분): %s", output_text[:200])
                    else:
                         logger.debug("Raw Output: %s", output_text)
            
            else:
                # 그 외 명령
                logger.info("서버 응답: %s", output_text)

        except Exception as e:
            logger.error("명령 처리 중 예외: %s", e)

# -------------------------
# Main
# -------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-autosave", action="store_true")
    parser.add_argument("--admin-url", default=ADMIN_URL)
    args = parser.parse_args()

    if not args.no_autosave:
        t = threading.Thread(target=auto_save_loop, args=(AUTOSAVE_INTERVAL_MIN, args.admin_url), daemon=True)
        t.start()

    try:
        handle_cmd_loop(args.admin_url)
    except KeyboardInterrupt:
        logger.info("사용자 중단.")

if __name__ == "__main__":
    main()
