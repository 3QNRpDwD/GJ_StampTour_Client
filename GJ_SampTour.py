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
import requests
import json
import pandas as pd
import threading
import time
import logging
import traceback
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
MIN_REQUIRED_UNIQUE = 10  # 10개 이상인 사용자만 최종 CSV에 포함 (원문 로직 유지)
# -------------------------

# -------------------------
# 로거 설정 (콘솔 INFO, 파일 DEBUG)
# -------------------------
logger = logging.getLogger("stamp_processor")
logger.setLevel(logging.DEBUG)

# 파일 핸들러 (모든 로그 저장, 디버그 포함)
fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setLevel(logging.DEBUG)
fh_formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
fh.setFormatter(fh_formatter)
logger.addHandler(fh)

# 콘솔 핸들러 (정보성 출력)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S")
ch.setFormatter(ch_formatter)
logger.addHandler(ch)
# -------------------------

def normalize_user_name(name: str) -> str:
    """
    사용자 이름 정규화:
      - Unicode 정규화 (NFKC)
      - 앞뒤 공백 제거
      - 내부 공백 제거 (모든 공백을 제거하여 '12345김유저' / '12345 김유저'를 동일 처리)
      - 영문은 소문자화 (대소문자 통일)
    결정적(deterministic)이고 단방향이며, 집계 전에 항상 호출해야 함.
    """
    if name is None:
        return ""
    s = str(name)
    s = unicodedata.normalize("NFKC", s)
    s = s.strip()
    # 모든 공백 제거 (예: '12345 김유저' -> '12345김유저')
    s = re.sub(r"\s+", "", s)
    try:
        s = s.lower()
    except Exception:
        pass
    return s

def safe_json_loads(s: str):
    """
    다양한 포맷(경우에 따라 출력 앞뒤에 불필요한 텍스트가 붙은 경우)을 잘 처리하려고 시도함.
    실패 시 None 반환.
    """
    try:
        return json.loads(s)
    except Exception:
        # 시도: 불필요한 앞뒤 텍스트 제거해서 JSON 부분만 골라내기
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
    """
    user_count: { user_id: {"count":n, "user_name":name} }
    같은 user_name 을 가진 여러 user_id가 있을 수 있으므로,
    각 user_name 당 count가 가장 큰 user_id 하나를 선택하여 반환.
    반환값: dict 선택된 user_id -> {"count":n, "user_name":name}
    """
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
    history: stamp_history 형태 (stamp_id -> list of user entries)
    - 각 entry는 student_id 또는 user_id, user_name 등을 포함할 수 있다.
    - 방문한 장소(stamp_id)를 집합으로 모아 중복 제거
    - 엑셀과 CSV 파일 생성 (엑셀은 사용자명 인덱스)
    - CSV는 티켓 계산(10개당 1티켓) 및 방문 장소 목록 포함
    이름은 항상 normalize_user_name()으로 정규화한 값을 사용하여 집계 및 중복 제거를 수행합니다.
    """
    logger.debug("process_history_to_outputs 시작 (stamp entries=%d)", len(history))
    # 1) 사용자별 집계: id 기준
    user_stats = {}  # id -> { 'name': normalized_name, 'locations': set(), 'raw_names': set() }
    # permit multiple possible id keys
    for stamp_id, users in history.items():
        for u in users:
            # 여러 포맷 허용
            uid = None
            raw_name = ""
            if isinstance(u, dict):
                uid = u.get("student_id") or u.get("user_id") or u.get("id") or u.get("studentId")
                raw_name = u.get("user_name") or u.get("name") or u.get("userName") or u.get("display_name") or u.get("username") or ""
            else:
                # 만약 단순 문자열이면 user id로 취급
                uid = str(u)
                raw_name = ""
            if uid is None:
                logger.debug("경고: user entry에서 id를 찾을 수 없음: %s", u)
                # 생성 임시 id
                uid = f"unknown_{hash(str(u))}"
            norm_name = normalize_user_name(raw_name)
            if uid not in user_stats:
                user_stats[uid] = {"name": norm_name, "locations": set(), "raw_names": set()}
            # raw_names 추적(디버깅/감사 목적)
            if raw_name:
                user_stats[uid]["raw_names"].add(raw_name)
            user_stats[uid]["locations"].add(str(stamp_id))

    logger.info("사용자 집계 완료: 전체 유니크 ID 수 = %d", len(user_stats))

    # 2) 엑셀용: dedupe by normalized user_name (원래 의도) — 중복 이름이 있을 때 방문 수가 많은 ID를 채택
    # 우선 user_count 형태로 변환
    user_count = {}
    for uid, info in user_stats.items():
        # name은 이미 정규화된 값
        user_count[uid] = {"count": len(info["locations"]), "user_name": info["name"] or f"<unknown:{uid}>"}
    logger.debug("user_count 샘플(최대10): %s", list(user_count.items())[:10])

    deduped = dedupe_user_count_by_name(user_count)
    logger.info("이름 기준 중복 제거 완료: 선택된 ID 수 = %d", len(deduped))

    # 만들어둘 엑셀 데이터프레임
    try:
        df_excel = pd.DataFrame([
            {"user_id": uid, "user_name": deduped[uid]["user_name"], "count": deduped[uid]["count"]}
            for uid in deduped
        ])
        if not df_excel.empty:
            df_excel.set_index("user_name", inplace=True)
            df_excel.to_excel(excel_path)
            logger.info("엑셀 파일 저장됨: %s (rows=%d)", os.path.abspath(excel_path), len(df_excel))
        else:
            logger.info("엑셀 저장 대상이 없음 (빈 데이터프레임).")
    except Exception as e:
        logger.exception("엑셀 저장 중 예외 발생: %s", e)

    # 3) CSV 결과: 파일 기반 처리 (원본 두번째 스크립트 로직과 유사)
    # 여기서는 student_id 우선, 그리고 'unique location 수 >= min_required_unique' 필터 적용
    try:
        # CSV 헤더 기록
        with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(['학번(ID)', '이름(정규화된)', '인정 스탬프 수(중복제외)', '당첨 기회(Ticket)', '방문한 장소 목록', 'raw_names_sample'])
            count_qualified = 0
            logger.info("--- CSV 분석 시작 ---")
            for uid, info in user_stats.items():
                unique_locations = info['locations']
                cnt = len(unique_locations)
                if cnt < min_required_unique:
                    logger.debug("건너뜀: %s (%s) - 장소수=%d 미만", info.get("name"), uid, cnt)
                    continue
                tickets = cnt // 10
                location_str = ", ".join(sorted(unique_locations))
                # raw_names_sample: 같은 uid에서 관측된 원시 이름 일부를 CSV에 남겨 감사에 도움
                raw_sample = ";".join(list(info.get("raw_names", []))[:3])
                writer.writerow([uid, info.get("name", ""), cnt, tickets, location_str, raw_sample])
                logger.info("대상: %s (%s) -> %d곳 방문 -> %d회 응모", info.get("name",""), uid, cnt, tickets)
                count_qualified += 1
            logger.info("CSV 처리 완료: %d명 조건 충족 (파일=%s)", count_qualified, os.path.abspath(csv_path))
    except Exception as e:
        logger.exception("CSV 생성 중 예외 발생: %s", e)

    # 요약 리턴
    summary = {
        "total_ids": len(user_stats),
        "deduped_selected": len(deduped),
        "qualified_count": count_qualified if 'count_qualified' in locals() else 0,
        "excel_path": os.path.abspath(excel_path),
        "csv_path": os.path.abspath(csv_path)
    }
    logger.debug("process_history_to_outputs 요약: %s", summary)
    return summary

def process_file(input_file=DEFAULT_INPUT_JSON, csv_path=DEFAULT_CSV_PATH):
    logger.info("파일 처리 시작: %s", input_file)
    if not os.path.exists(input_file):
        logger.error("파일을 찾을 수 없습니다: %s", input_file)
        return None
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        history = data.get('stamp_history') or data.get('StampHistory') or {}
        if not isinstance(history, dict):
            logger.error("stamp_history가 딕셔너리 형태가 아닙니다. 타입=%s", type(history))
            return None
        return process_history_to_outputs(history)
    except Exception as e:
        logger.exception("파일 처리 중 오류 발생: %s", e)
        return None

def parse_admin_response_output(output_text: str):
    """
    admin API가 반환하는 output 필드의 내용을 가능한 범용적으로 파싱하여
    stamp_history를 찾아 반환. 실패하면 None.
    """
    logger.debug("admin output raw (길이=%d): %s", len(output_text or ""), (output_text[:1000] + '...') if output_text and len(output_text) > 1000 else output_text)
    # 보정: 원래 코드에서 했던 치환들 시도
    try_variants = []
    try_variants.append(output_text)
    # replace common non-json tokens
    s = output_text or ""
    s = s.replace("StampHistory ", "")
    s = s.replace("StampUserInfo ", "")
    s = s.replace("user_name", "\"user_name\"")
    s = s.replace("user_id", "\"user_id\"")
    s = s.replace("timestamp", "\"timestamp\"")
    s = s.replace("stamp_history", "\"stamp_history\"")
    try_variants.append(s)
    # 시도해서 JSON 로드
    for candidate in try_variants:
        parsed = safe_json_loads(candidate)
        if parsed is None:
            continue
        # stamp_history 키 찾기
        if isinstance(parsed, dict):
            if "stamp_history" in parsed and isinstance(parsed["stamp_history"], dict):
                logger.debug("admin output에서 stamp_history 찾음 (직접 포함).")
                return parsed["stamp_history"]
            # 혹시 최상위가 바로 stamp_history 구조일 수도 있음
            # 예: output이 "{ 'someKey': {...}, 'stamp_history': {...} }"
            # 혹은 output 자체가 stamp_history dict일 수 있음
            # 확인: 만약 최상위 키들이 모두 스탬프 id라면 (값이 list) stamp_history로 간주
            top_keys = list(parsed.keys())
            if top_keys and isinstance(parsed[top_keys[0]], list):
                # heuristic: 모든 값이 list로 구성되면 stamp_history로 간주
                all_lists = all(isinstance(parsed[k], list) for k in top_keys)
                if all_lists:
                    logger.debug("admin output이 직접 stamp_history 구조로 판단됨.")
                    return parsed
        # else 계속 시도
    logger.warning("admin output에서 stamp_history를 파싱하지 못했습니다.")
    return None

# -------------------------
# 스레드 함수들
# -------------------------
def auto_save_loop(interval_min=AUTOSAVE_INTERVAL_MIN, admin_url=ADMIN_URL):
    logger.info("Autosave enabled. Interval = %d min", interval_min)
    while True:
        try:
            time.sleep(interval_min * 60)
            logger.debug("Autosave: POST %s {'command':'save all'}", admin_url)
            r = requests.post(admin_url, json={"command": "save all", "output": ""}, timeout=10)
            logger.debug("Autosave response status=%s, text(len)=%d", r.status_code, len(r.text) if r.text else 0)
        except Exception:
            logger.exception("Autosave 중 예외 발생")

def handle_cmd_loop(admin_url=ADMIN_URL):
    logger.info("명령 모드 시작: admin=%s", admin_url)
    while True:
        try:
            cmd = input("Server command: ").strip()
            if cmd.lower() in ("exit", "quit"):
                logger.info("프로그램 종료 명령 수신. 종료합니다.")
                os._exit(0)
            # 파일 처리 명령
            if cmd.startswith("process file"):
                parts = cmd.split(maxsplit=1)
                if len(parts) == 1:
                    infile = DEFAULT_INPUT_JSON
                else:
                    infile = parts[1].strip()
                logger.info("process file 명령: input=%s", infile)
                process_file(infile)
                continue

            # 기본: admin으로 POST 전송
            logger.debug("POST to admin: command=%s", cmd)
            r = requests.post(admin_url, json={"command": cmd, "output": ""}, timeout=10)
            try:
                # admin의 json 응답 전체 로깅
                resp_json = r.json()
                logger.debug("admin 응답 JSON 키: %s", list(resp_json.keys()))
                output_text = resp_json.get("output", "")
            except Exception:
                output_text = r.text
                logger.debug("admin 응답은 JSON 아님, text 길이=%d", len(output_text) if output_text else 0)

            # stamp_history 추출 시도
            stamp_history = parse_admin_response_output(output_text)
            if stamp_history:
                logger.info("admin 응답에서 stamp_history 발견. 처리 시작.")
                summary = process_history_to_outputs(stamp_history)
                logger.info("stamp_history 처리 요약: %s", summary)
            else:
                # 원래 코드처럼, output 내용을 출력 (콘솔)
                logger.info("Admin output (non-stamp):\n%s", output_text)
        except Exception as e:
            logger.exception("명령 처리 중 예외 발생: %s", e)
            # 계속 루프

# -------------------------
# entrypoint
# -------------------------
def main():
    parser = argparse.ArgumentParser(description="Stamp processor 통합 스크립트")
    parser.add_argument("--no-autosave", action="store_true", help="autosave 스레드 실행 안함")
    parser.add_argument("--admin-url", default=ADMIN_URL, help="admin URL (기본: http://localhost:80/admin)")
    args = parser.parse_args()

    # update admin url if provided
    admin_url = args.admin_url

    # start autosave 스레드 (옵션으로 끌 수 있음)
    if not args.no_autosave:
        t_autosave = threading.Thread(target=auto_save_loop, args=(AUTOSAVE_INTERVAL_MIN, admin_url), daemon=True)
        t_autosave.start()
        logger.debug("autosave 스레드 시작 (데몬)")

    # 명령 루프 (메인 스레드에서 실행)
    try:
        handle_cmd_loop(admin_url=admin_url)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt 수신. 종료합니다.")
    except Exception:
        logger.exception("메인 루프에서 예기치 못한 예외 발생")

if __name__ == "__main__":
    main()
