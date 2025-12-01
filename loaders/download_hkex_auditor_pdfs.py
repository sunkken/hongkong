
# ============================================================
# download_hkex_auditor_pdfs.py
#
# Downloads all PDF files linked in HKEX Auditor Reports table
# Parallelized with skip-if-exists logic and summary of skips
# ============================================================

import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
BASE_URL = "https://www3.hkexnews.hk/reports/auditorreport/ncms/auditorreport_anntdate_des.htm"
SAVE_DIR = "./data/raw/auditor_pdfs"
MAX_WORKERS = 8  # adjust based on system/network

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def download_file(pdf_url, save_path):
    if os.path.exists(save_path):
        return "SKIPPED"
    try:
        resp = requests.get(pdf_url, timeout=60)
        resp.raise_for_status()
        with open(save_path, "wb") as f:
            f.write(resp.content)
        return "DOWNLOADED"
    except Exception as e:
        return f"FAILED: {e}"

# ------------------------------------------------------------
# Core logic
# ------------------------------------------------------------
def download_auditor_pdfs() -> bool:
    os.makedirs(SAVE_DIR, exist_ok=True)
    print("\n📡 Fetching HKEX Auditor Reports links...")
    try:
        r = requests.get(BASE_URL, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # Locate table and extract links
        table = soup.find("table")
        if not table:
            print("❌ No table found on page.")
            return False

        links = []
        for a in table.find_all("a", href=True):
            href = a["href"]
            if href.lower().endswith(".pdf"):
                full_url = urljoin(BASE_URL, href)
                links.append(full_url)

        print(f"🔗 Found {len(links)} PDF links.")
        if not links:
            return False

        skipped_count = 0
        downloaded_count = 0
        failed_count = 0

        # Parallel download
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for pdf_url in links:
                filename = os.path.basename(pdf_url.split("?")[0])
                save_path = os.path.join(SAVE_DIR, filename)
                futures.append(executor.submit(download_file, pdf_url, save_path))

            for i, future in enumerate(as_completed(futures), start=1):
                result = future.result()
                if result == "SKIPPED":
                    skipped_count += 1
                elif result == "DOWNLOADED":
                    downloaded_count += 1
                    print(f"[{i}/{len(futures)}] ✅ Downloaded")
                else:
                    failed_count += 1
                    print(f"[{i}/{len(futures)}] ❌ {result}")

        # Summary
        print(f"\n📦 Completed downloading PDFs → {SAVE_DIR.replace(os.sep, '/')}")
        print(f"✅ Downloaded: {downloaded_count}")
        print(f"⏩ Skipped (already exists): {skipped_count}")
        print(f"❌ Failed: {failed_count}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch page: {e}")
        return False

# ------------------------------------------------------------
# Entry point
# ------------------------------------------------------------
if __name__ == "__main__":
    success = download_auditor_pdfs()
    if not success:
        raise SystemExit(1)