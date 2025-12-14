
# ============================================================
# download_hkex_auditor_reports.py
#
# Scrapes HKEX Auditor Reports table and saves as CSV
# Adds 'pdf_path' column with relative path for portability
# ============================================================

import os
import requests
from bs4 import BeautifulSoup
import csv

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
URL = "https://www3.hkexnews.hk/reports/auditorreport/ncms/auditorreport_anntdate_des.htm"
SAVE_DIR = "./data/raw"
SAVE_PATH = os.path.join(SAVE_DIR, "auditor_reports.csv")

# ------------------------------------------------------------
# Core logic
# ------------------------------------------------------------
def fetch_auditor_reports() -> bool:
    os.makedirs(SAVE_DIR, exist_ok=True)
    print("\n📡 Fetching HKEX Auditor Reports table...")
    try:
        r = requests.get(URL, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # Locate table
        table = soup.find("table")
        if not table:
            print("❌ No table found on page.")
            return False

        rows = table.find_all("tr")
        data = []

        for row in rows[1:]:  # skip header
            tds = row.find_all("td")
            cols = [td.get_text(strip=True) for td in tds]

            if len(cols) < 3:
                continue

            # Extract hyperlink and derive relative PDF path
            link_tag = row.find("a", href=True)
            link = link_tag["href"] if link_tag else ""
            raw_name = os.path.basename(link.split("?")[0]) if link else ""
            # derive document_name (basename without extension) to match other tables
            document_name = os.path.splitext(raw_name)[0] if raw_name else ""
            pdf_path = f"data/raw/auditor_pdfs/{raw_name}" if raw_name else ""

            data.append([
                cols[0],  # stock_code
                cols[1],  # listed_company_name
                cols[2],  # announcement_date
                link,     # hyperlink
                pdf_path, # relative path for portability
                document_name  # extracted document_name (no extension)
            ])

        # Save to CSV
        with open(SAVE_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "stock_code",
                "listed_company_name",
                "announcement_date",
                "hyperlink",
                "pdf_path",
                "document_name"
            ])
            writer.writerows(data)

        print(f"✅ Saved {len(data)} rows → {SAVE_PATH.replace(os.sep, '/')}")
        return True

    except requests.exceptions.RequestException as e:
        print(f"❌ Download failed: {e}")
        return False

# ------------------------------------------------------------
# Entry point
# ------------------------------------------------------------
if __name__ == "__main__":
    success = fetch_auditor_reports()
    if not success:
        raise SystemExit(1)
