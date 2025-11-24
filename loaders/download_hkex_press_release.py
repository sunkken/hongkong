
import requests
import json
import pandas as pd
from pathlib import Path

# ----------------------------
# Configuration
# ----------------------------
save_dir = Path("./data/raw/press")
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

# ----------------------------
# Fetch function
# ----------------------------
def fetch_press_releases(stock_id):
    params = {
        "sortDir": "0",
        "sortByOptions": "DateTime",
        "category": "0",
        "market": "SEHK", # THIS IS ONLY HKEX MAIN BOARD, GEM IS OTHER
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
    resp = requests.get(API_URL, headers=HEADERS, params=params)
    if resp.status_code == 200:
        return resp.json()
    return None

# ----------------------------
# Download and save
# ----------------------------
def download_press_releases(stock_id):
    data = fetch_press_releases(stock_id)
    if not data or "result" not in data:
        print(f"❌ No data returned for stockId {stock_id}")
        return

    try:
        results = json.loads(data["result"])
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse JSON for stockId {stock_id}: {e}")
        return

    print(f"✅ Fetched {len(results)} records for stockId {stock_id}")

    # Save raw JSON
    json_file = save_dir / f"press_releases_{stock_id}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"📂 Saved raw JSON to {json_file}")

    # Save to CSV
    df = pd.DataFrame(results)
    csv_file = save_dir / f"press_releases_{stock_id}.csv"
    df.to_csv(csv_file, index=False)
    print(f"📊 Saved CSV with {len(df)} rows to {csv_file}")

# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    txt_file = Path("data/stock_code_list.txt")
    if not txt_file.exists():
        print(f"❌ {txt_file} not found. Please create it with one stock ID per line.")
    else:
        with open(txt_file, "r") as f:
            stock_ids = [line.strip() for line in f if line.strip()]

        print(f"ℹ️ Found {len(stock_ids)} stock IDs in {txt_file}")
        for sid in stock_ids:
            download_press_releases(sid)