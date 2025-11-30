
import requests
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import threading
from pathlib import Path

# ===== Configuration =====
API_URL = "https://www1.hkexnews.hk/search/prefix.do"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=en",
    "x-requested-with": "XMLHttpRequest",
    "Cookie": "JSESSIONID=tIziozGLaQ0QHYcZpnPHEu80sAJxP_y9NINJaFh4.s106; TS014a5f8b=015e7ee60367b53fc87ca2608b8d9cf01cd2fd2042d69854e4c30fcaeb047b8ec20ae65adfffb15ff2e4a9d6d702e84006acdca7fa3ab43633693136c3f0926afdb066804b; ..."  # FULL cookie string here
}
DEFAULT_MAX_WORKERS = 8
DEFAULT_INPUT_PATH = Path("data/stock_codes.txt")
DEFAULT_OUTPUT_FULL = Path("data/stock_mapping_filtered.csv")
DEFAULT_OUTPUT_PARTIAL = Path("data/stock_mapping_partial.csv")

# Shared state (will be reset in run_scrape)
results = []
seen_stockIds = set()
seen_codes = set()
codes_lock = threading.Lock()
ids_lock = threading.Lock()
results_lock = threading.Lock()

# ===== Helpers =====
def parse_jsonp(raw_text: str) -> dict:
    s = raw_text.strip()
    if not s.startswith("callback("):
        return {}
    s = s[len("callback("):]
    if s.endswith(");"):
        s = s[:-2]
    elif s.endswith(")"):
        s = s[:-1]
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return {}

def classify_market(actual_code: int) -> str:
    return "GEM" if 8000 <= actual_code <= 8999 else "SEHK"

def fetch_stock_info(code: int):
    with codes_lock:
        if code in seen_codes:
            print(f"⏩ Skipping API call for {code}")
            return
        seen_codes.add(code)

    params = {
        "callback": "callback",
        "lang": "EN",
        "type": "A",
        "name": str(code),
        "market": "SEHK"
    }
    try:
        resp = requests.get(API_URL, headers=HEADERS, params=params, timeout=15)
        if resp.status_code != 200:
            print(f"❌ HTTP {resp.status_code} for code {code}")
            return
        data = parse_jsonp(resp.text)
        stock_info = data.get("stockInfo", []) or []

        with codes_lock:
            for item in stock_info:
                returned_code = int(item.get("code"))
                seen_codes.add(returned_code)

        for item in stock_info:
            stock_id = item.get("stockId")
            actual_code = int(item.get("code"))
            with ids_lock:
                if stock_id in seen_stockIds:
                    continue
                seen_stockIds.add(stock_id)
            row = {
                "stock_code": actual_code,
                "stockId": stock_id,
                "name": item.get("name"),
                "market": classify_market(actual_code)
            }
            with results_lock:
                results.append(row)
    except Exception as e:
        print(f"⚠️ Error for code {code}: {e}")

# ===== Public API =====
def run_scrape(input_path=DEFAULT_INPUT_PATH,
               output_full=DEFAULT_OUTPUT_FULL,
               output_partial=DEFAULT_OUTPUT_PARTIAL,
               max_workers=DEFAULT_MAX_WORKERS) -> Path:
    global results, seen_stockIds, seen_codes, codes_lock, ids_lock, results_lock
    results = []
    seen_stockIds = set()
    seen_codes = set()
    codes_lock = threading.Lock()
    ids_lock = threading.Lock()
    results_lock = threading.Lock()

    start_time = time.time()
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    with input_path.open("r", encoding="utf-8") as f:
        raw_codes = [int(line.strip()) for line in f if line.strip()]

    stock_codes = []
    seen_tmp = set()
    for c in raw_codes:
        if c not in seen_tmp:
            seen_tmp.add(c)
            stock_codes.append(c)

    print(f"🔎 Starting scrape for {len(stock_codes)} codes from {input_path} ...")
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_stock_info, code) for code in stock_codes]
            total = len(futures)
            for i, _ in enumerate(as_completed(futures), start=1):
                if i % 100 == 0 or i == total:
                    with results_lock:
                        print(f"Progress: {i}/{total} | Rows: {len(results)}")
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user. Saving partial results...")
    finally:
        df = pd.DataFrame(results)
        out_path = output_full if len(df) else output_partial
        df.to_csv(out_path, index=False)
        elapsed = time.time() - start_time
        print(f"\n✅ Done! Collected {len(df)} rows in {elapsed:.2f}s")
        return out_path

if __name__ == "__main__":
    run_scrape()
