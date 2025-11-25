
import requests
import json
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random

# ----------------------------
# Configuration
# ----------------------------
save_dir = Path("./data/press")
save_dir.mkdir(parents=True, exist_ok=True)

API_URL = "https://www1.hkexnews.hk/search/titleSearchServlet.do"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=en",
    "x-requested-with": "XMLHttpRequest",
    "Cookie": "JSESSIONID=tIziozGLaQ0QHYcZpnPHEu80sAJxP_y9NINJaFh4.s106; TS014a5f8b=015e7ee60367b53fc87ca2608b8d9cf01cd2fd2042d69854e4c30fcaeb047b8ec20ae65adfffb15ff2e4a9d6d702e84006acdca7fa3ab43633693136c3f0926afdb066804b; ..."  # FULL cookie string here
}

FROM_DATE = "19990401"
TO_DATE = "20251124"
ROW_RANGE = "10000"
MAX_WORKERS = 10  # Adjust based on your network/API tolerance
RETRIES = 3
summary_records = []  # Collect results for summary CSV

# ----------------------------
# Fetch with retry
# ----------------------------
def fetch_press_releases(stock_id):
    params = {
        "sortDir": "0",
        "sortByOptions": "DateTime",
        "category": "0",
        "market": "SEHK", # SEHK = Main Board; GEM = Growth Enterprise Market
        "stockId": stock_id,
        "documentType": "-1",
        "fromDate": FROM_DATE,
        "toDate": TO_DATE,
        "title": "",
        "searchType": "0",
        "t1code": "-2",
        "t2Gcode": "-2",
        "t2code": "-2",
        "rowRange": ROW_RANGE,
        "lang": "E"
    }

    for attempt in range(RETRIES):
        try:
            resp = requests.get(API_URL, headers=HEADERS, params=params, timeout=40)
            if resp.status_code == 200:
                return stock_id, resp.json()
        except Exception as e:
            print(f"⚠️ {stock_id}: Attempt {attempt+1} failed ({e})")
        time.sleep(2 ** attempt + random.random())  # backoff
    return stock_id, None


# ----------------------------
# Save CSV only if records exist
# ----------------------------
def download_press_releases(stock_id):
    stock_id, data = fetch_press_releases(stock_id)
    if not data or "result" not in data:
        summary_records.append({"stock_id": stock_id, "status": "failed", "row_count": 0})
        return False

    try:
        results = json.loads(data["result"])
    except json.JSONDecodeError:
        summary_records.append({"stock_id": stock_id, "status": "failed", "row_count": 0})
        return False

    if not results:
        print(f"ℹ️ {stock_id}: No press releases found")
        summary_records.append({"stock_id": stock_id, "status": "skipped", "row_count": 0})
        return False

    # Save CSV only
    df = pd.DataFrame(results)
    csv_file = save_dir / f"press_releases_{stock_id}.csv"
    df.to_csv(csv_file, index=False)

    print(f"✅ {stock_id}: {len(df)} rows saved")
    summary_records.append({"stock_id": stock_id, "status": "saved", "row_count": len(df)})
    return True

# ----------------------------
# Parallel Execution
# ----------------------------
def run_parallel(stock_ids):
    start = time.time()
    success_count = 0
    fail_count = 0
    skipped_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_press_releases, sid): sid for sid in stock_ids}

        for i, future in enumerate(as_completed(futures), 1):
            sid = futures[future]
            try:
                result = future.result()
                if result:
                    success_count += 1
                else:
                    skipped_count += 1
            except Exception as e:
                print(f"❌ Exception for {sid}: {e}")
                fail_count += 1

            if i % 50 == 0:
                print(f"Progress: {i}/{len(stock_ids)} processed")

    elapsed = time.time() - start
    print(f"\n✅ Done! Saved: {success_count}, Skipped (0 records): {skipped_count}, Fail: {fail_count}, Time: {elapsed:.2f}s")

    # Save summary CSV
    summary_df = pd.DataFrame(summary_records)
    summary_file = save_dir / "press_summary.csv"
    summary_df.to_csv(summary_file, index=False)
    print(f"📊 Summary CSV saved to {summary_file}")

# ----------------------------
# Mini Test Entry Point
# ----------------------------
def test_single_stock(stock_id):
    print(f"\n🔍 Testing single stock ID: {stock_id}")
    sid, data = fetch_press_releases(stock_id)
    if not data:
        print("❌ No response or failed request")
        return
    try:
        results = json.loads(data["result"])
        print(f"✅ Found {len(results)} records for {stock_id}")
        print(json.dumps(results[:5], indent=2))  # Print first 5 records for preview
    except json.JSONDecodeError:
        print("❌ Failed to parse JSON")

# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    txt_file = Path("data/stock_code_list_main.txt")
    if not txt_file.exists():
        print(f"❌ {txt_file} not found.")
    else:
        with open(txt_file, "r") as f:
            stock_ids = [line.strip() for line in f if line.strip()]

        print(f"ℹ️ Found {len(stock_ids)} stock IDs")

        # Uncomment one of these:
        # run_parallel(stock_ids)  # Full run
        test_single_stock("00007")   # Quick test for one stock
