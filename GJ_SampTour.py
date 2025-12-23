#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
통합 스크립트: (1) 로컬 어드민에 주기적 autosave POST 전송
               (2) 콘솔 명령 수신 -> 어드민 응답에서 stamp_history 처리 -> 엑셀/CSV 저장
               (3) 파일(resources/database/stamp_status.json) 직접 처리 -> 중복 제거 / 티켓 계산 / CSV 저장
로깅: 콘솔(INFO) + 파일(DEBUG)로 처리 과정을 상세히 기록합니다.
사용법:
  - 그냥 실행하면 autosave와 명령 입력 루프가 동작합니다.
  - 콘솔에서:
      save all                -> 서버에 "save all" 전송 (원래 동작)
      stamp status            -> 스템프 데이터베이스 출력
      process file            -> resources/database/stamp_status.json 파일을 읽어 처리
      process file /경로/파일  -> 지정한 파일을 읽어 처리
      exit / quit             -> 프로그램 종료
      (그 외 입력은 admin에 그대로 전송되고, stamp_history가 있으면 자동으로 처리/저장)
"""
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GJ_SampTour_Client.py
Rust 서버(Actix-web) 규격에 맞춘 어드민 클라이언트 스크립트

수정 내역:
  - 서버의 'save all' 응답(단순 문자열)과 'stamp status' 응답(Rust Debug Struct)을 구분하여 처리
  - Rust Debug 포맷 문자열을 JSON으로 변환하기 위한 파싱 로직 강화
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
MIN_REQUIRED_UNIQUE = 10 
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

def safe_json_loads(s: str):
    try:
        return json.loads(s)
    except Exception:
        # JSON 형식이 아닐 경우, 중괄호 구간만 추출 시도
        try:
            first = s.find('{')
            last = s.rfind('}')
            if first != -1 and last != -1 and last > first:
                candidate = s[first:last+1]
                return json.loads(candidate)
        except Exception:
            pass
    return None

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
    logger.debug("데이터 집계 시작 (기존 규격 유지)")
    
    user_stats = {} 
    # 1. 데이터 집계 (기존 로직 유지)
    for stamp_id, users in history.items():
        for u in users:
            if isinstance(u, dict):
                uid = u.get("student_id") or u.get("user_id") or u.get("id")
                raw_name = u.get("user_name") or u.get("name") or ""
            else:
                uid = str(u)
                raw_name = ""
            
            if uid is None: continue
            
            norm_name = normalize_user_name(raw_name)
            if uid not in user_stats:
                user_stats[uid] = {"name": norm_name, "locations": set()}
            user_stats[uid]["locations"].add(str(stamp_id))

    # 2. 엑셀 저장 (기존 규격대로 생성하되 내용은 간소화 가능)
    # (요청하신 내용에 엑셀에 대한 언급은 없었으나 기존 코드 호환을 위해 유지)
    try:
        df_data = []
        for uid, info in user_stats.items():
            df_data.append({"user_id": uid, "user_name": info["name"], "count": len(info["locations"])})
        pd.DataFrame(df_data).to_excel(excel_path, index=False)
    except Exception as e:
        logger.error("Excel 저장 실패: %s", e)

    # 3. CSV 저장 (핵심 변경 부분: 학번이름 반복 출력)
    try:
        with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            count_total_rows = 0
            
            for uid, info in user_stats.items():
                cnt = len(info['locations'])
                # 10개 미만은 제외 (기존 로직 조건 유지)
                if cnt < min_required_unique:
                    continue
                
                # 출력 횟수: 10개 이상 1번, 20개 이상 2번, 30개 이상 3번
                repeat_count = cnt // 10
                display_name = f"{uid}{info['name']}" # 예: 12345김유저
                
                for _ in range(repeat_count):
                    writer.writerow([display_name])
                    count_total_rows += 1
            
            logger.info("CSV 저장 완료: 총 %d줄 기록됨", count_total_rows)
    except Exception as e:
        logger.error("CSV 저장 실패: %s", e)

    # 리턴 형식 유지 (기존 코드에서 summary 출력 시 사용)
    return {"total_users": len(user_stats), "recorded_rows": count_total_rows}

  def parse_rust_debug_output(output_text: str):
    """
    Rust 서버가 반환하는 Debug 포맷({:?}) 문자열을 JSON으로 변환하여 파싱합니다.
    형태 예시: StampHistory { stamp_history: {"1": [StampUserInfo { ... }]} }
    """
    if not output_text:
        return None

    # 1. 구조체 이름 제거 (StampHistory { ... } -> { ... })
    # 영문자+숫자+언더바 뒤에 공백(선택)과 중괄호가 오면, 그냥 중괄호로 변경
    s = re.sub(r'[a-zA-Z0-9_]+\s*\{', '{', output_text)
    
    # 2. 키 값에 따옴표 붙이기 (key: -> "key":)
    # 주의: URL(http:)이나 시간(12:00) 등이 값에 포함될 경우 오동작 가능성이 있으나,
    # 현재 데이터 구조(단순 필드명)에서는 이 정규식으로 충분합니다.
    s = re.sub(r'([a-zA-Z0-9_]+):', r'"\1":', s)
    
    # 3. Trailing Comma 제거 (콤마 뒤에 닫는 괄호/중괄호가 오면 콤마 삭제)
    s = re.sub(r',\s*([\]}])', r'\1', s)

    try:
        # 4. JSON 변환 시도
        data = json.loads(s)
        
        # 5. [중요] 데이터 구조 맞추기
        # 서버 응답이 { "stamp_history": { ... } } 형태라면,
        # 내부의 { ... } 만 반환해야 기존 로직(process_history_to_outputs)이 정상 작동함
        if isinstance(data, dict) and "stamp_history" in data:
            return data["stamp_history"]
        
        return data

    except json.JSONDecodeError as e:
        # 파싱 실패 시 원본 문자열 일부를 로그에 남겨 디버깅 도움
        logger.error(f"JSON 파싱 실패: {e}")
        logger.debug(f"파싱 시도한 문자열(앞부분): {s[:100]}")
        return None
    except Exception as e:
        logger.error(f"알 수 없는 오류: {e}")
        return None

def process_file(input_file=DEFAULT_INPUT_JSON):
    logger.info("로컬 파일 처리: %s", input_file)
    if not os.path.exists(input_file):
        logger.error("파일 없음: %s", input_file)
        return
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        history = data.get('stamp_history') or data
        process_history_to_outputs(history)
    except Exception as e:
        logger.error("파일 처리 중 오류: %s", e)

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
                target_file = parts[1].strip() if len(parts) > 1 else DEFAULT_INPUT_JSON
                process_file(target_file)
                continue

            # 2. 서버 전송 명령
            logger.debug("서버 전송: %s", cmd)
            r = requests.post(admin_url, json={"command": cmd, "output": ""}, timeout=10)
            
            try:
                resp_json = r.json()
                output_text = resp_json.get("output", "")
            except:
                output_text = r.text

            # 3. 명령어에 따른 응답 처리 분기
            if cmd == "save all":
                # save all은 데이터를 주지 않음. 성공 메시지만 출력.
                logger.info("서버 응답: %s", output_text)
            
            elif cmd == "stamp status":
                # stamp status는 데이터를 줌 (Rust Debug Format)
                logger.info("데이터 수신 완료. 파싱 시작...")
                history = parse_rust_debug_output(output_text)
                if history:
                    summary = process_history_to_outputs(history)
                    logger.info("처리 완료: %s", summary)
                else:
                    logger.warning("데이터 파싱 실패. 원본 응답 확인 필요.")
                    logger.debug("Raw Output: %s", output_text[:200]) # 너무 기니 앞부분만
            
            else:
                # 그 외 명령 (unknown 등)
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
