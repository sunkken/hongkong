
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

# Concurrency: lower value = gentler on the site/IP
MAX_WORKERS = 8

# Input file (as requested)
INPUT_PATH = Path("data/stock_codes.txt")

# Output files
OUTPUT_FULL = Path("data/stock_mapping_filtered.csv")
OUTPUT_PARTIAL = Path("data/stock_mapping_partial.csv")

# ===== Shared state (thread-safe) =====
results = []
seen_stockIds = set()    # Deduplicate by stockId
seen_codes = set()       # Prevent API calls for codes already seen in any response

codes_lock = threading.Lock()
ids_lock = threading.Lock()
results_lock = threading.Lock()  # optional; list append is atomic in CPython, but safer

# ===== Helpers =====
def parse_jsonp(raw_text: str) -> dict:
    """Robustly strip 'callback(...)' JSONP wrapper and return parsed JSON dict."""
    s = raw_text.strip()
    if not s.startswith("callback("):
        return {}
    # Trim leading 'callback(' and trailing ')' or ');'
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
    """Fetch stock info for a single code, skipping the API call if we already encountered the code earlier."""
    # ✅ Skip the API call entirely if this code has already appeared in any previous response
    with codes_lock:
        if code in seen_codes:
            print(f"⏩ Skipping API call for {code} (already encountered earlier)")
            return
        # Mark as scheduled to avoid racing duplicates
        seen_codes.add(code)

    params = {
        "callback": "callback",
        "lang": "EN",
        "type": "A",
        "name": str(code),   # numeric content; API expects string
        "market": "SEHK"     # payload is always SEHK (per your manual testing)
    }

    try:
        resp = requests.get(API_URL, headers=HEADERS, params=params, timeout=15)
        if resp.status_code != 200:
            print(f"❌ HTTP {resp.status_code} for code {code}")
            return

        data = parse_jsonp(resp.text)
        stock_info = data.get("stockInfo", []) or []

        # Record all returned codes into seen_codes first (so future requests will skip)
        with codes_lock:
            for item in stock_info:
                returned_code = int(item.get("code"))
                seen_codes.add(returned_code)

        # Append unique rows by stockId using actual code from response
        for item in stock_info:
            stock_id = item.get("stockId")
            actual_code = int(item.get("code"))
            with ids_lock:
                if stock_id in seen_stockIds:
                    continue
                seen_stockIds.add(stock_id)

            market_col = classify_market(actual_code)
            row = {
                "stock_code": actual_code,        # ✅ use code from API response
                "stockId": stock_id,
                "name": item.get("name"),
                "market": market_col
            }
            # Append result (protected)
            with results_lock:
                results.append(row)

    except Exception as e:
        print(f"⚠️ Error for code {code}: {e}")

# ===== Main =====
if __name__ == "__main__":
    start_time = time.time()

    # Load codes as integers; preserve order, drop duplicates in input itself
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH.resolve()}")

    with INPUT_PATH.open("r", encoding="utf-8") as f:
        raw_codes = [int(line.strip()) for line in f if line.strip()]
        # Deduplicate input while preserving order
        seen_tmp = set()
        stock_codes = []
        for c in raw_codes:
            if c not in seen_tmp:
                seen_tmp.add(c)
                stock_codes.append(c)

    print(f"🔍 Starting scrape for {len(stock_codes)} codes from {INPUT_PATH} ...")

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(fetch_stock_info, code) for code in stock_codes]

            total = len(futures)
            for i, _ in enumerate(as_completed(futures), start=1):
                # Progress indicators
                if i % 100 == 0 or i == total:
                    with results_lock:
                        print(f"Progress: {i}/{total} tasks completed | Rows collected: {len(results)}")
                if i % 500 == 0:
                    with results_lock:
                        print(f"✅ Intermediate: {len(results)} unique rows so far")

    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user. Saving partial results...")
    finally:
        # Final/partial save — guaranteed to run
        df = pd.DataFrame(results)
        df.to_csv(OUTPUT_FULL if len(df) else OUTPUT_PARTIAL, index=False)
        elapsed = time.time() - start_time
        print(f"\n✅ Done! Collected {len(df)} unique rows in {elapsed:.2f}s")
        if not df.empty:
            print("📊 Sample:")
            print(df.head(10).to_string(index=False))
        else:
            print("ℹ️ No rows collected.")