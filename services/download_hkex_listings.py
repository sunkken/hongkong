import requests
from pathlib import Path

# ----------------------------
# Configuration
# ----------------------------
save_dir = Path("../data/raw")
save_dir.mkdir(parents=True, exist_ok=True)

MAX_CONSECUTIVE_MISSING = 3  # stop after this many missing files

# Main Board known year ranges
main_years = {
    "new": range(2020, 2026),
    "middle": range(2012, 2020),
    "old": range(1994, 2012)
}

# GEM known years
gem_short = range(1999, 2022)  # 1999-2021
gem_special = [2022]           # 2022 special case
gem_long = range(2024, 2026)   # 2024-2025

# Base URLs
main_base = "https://www2.hkexnews.hk/-/media/HKEXnews/Homepage/New-Listings/New-Listing-Information/New-Listing-Report/Main/"
gem_base = "https://www2.hkexnews.hk/-/media/HKEXnews/Homepage/New-Listings/New-Listing-Information/New-Listing-Report/GEM/"

# ----------------------------
# Helper function
# ----------------------------
def download_file(url, filename):
    """Download a file if it exists and not already downloaded."""
    if filename.exists():
        print(f"Already exists: {filename}")
        return True

    # Check if the URL exists first (HEAD request)
    head = requests.head(url)
    if head.status_code != 200:
        print(f"Skipped (not found): {url}")
        return False

    # Download file
    resp = requests.get(url)
    if resp.status_code == 200:
        with open(filename, "wb") as f:
            f.write(resp.content)
        print(f"Downloaded {filename}")
        return True
    else:
        print(f"Failed to download: {url}")
        return False

# ----------------------------
# Download Main Board
# ----------------------------
print("\nDownloading Main Board reports...")
consec_missing = 0
for year in range(1994, 2030):  # stop dynamically after consecutive misses
    urls = []

    # new
    if year in main_years["new"]:
        urls.append(f"{main_base}NLR{year}_Eng.xlsx")
    
    # middle
    if year in main_years["middle"]:
        urls.append(f"{main_base}NLR{year}_Eng.xls")
    
    # old
    if year in main_years["old"]:
        urls.append(f"{main_base}{year}.XLS")
        urls.append(f"{main_base}{year}.xls")

    success = False
    for url in urls:
        ext = url.split('.')[-1].lower()
        filename = save_dir / f"Main_{year}.{ext}"
        if download_file(url, filename):
            success = True
            break

    if not success:
        consec_missing += 1
        if consec_missing >= MAX_CONSECUTIVE_MISSING:
            print("Reached max consecutive missing files. Stopping Main Board download.")
            break
    else:
        consec_missing = 0

# ----------------------------
# Download GEM
# ----------------------------
print("\nDownloading GEM reports...")
consec_missing = 0
year = 1999
while year < 2030:  # stop dynamically
    urls = []

    # 1999-2021: short year
    if year in gem_short:
        short_year = str(year)[-2:]
        urls.append(f"{gem_base}e_newlistings{short_year}.xls")
        urls.append(f"{gem_base}e_newlistings{short_year}.XLS")

    # 2022 special
    if year in gem_special:
        urls.append(f"{gem_base}e_newlistings.xls")
        urls.append(f"{gem_base}e_newlistings.XLS")

    # 2023 missing
    if year == 2023:
        year += 1
        continue

    # 2024+
    if year in gem_long:
        urls.append(f"{gem_base}e_newlistings{year}.xlsx")

    success = False
    for url in urls:
        ext = url.split('.')[-1].lower()
        filename = save_dir / f"GEM_{year}.{ext}"
        if download_file(url, filename):
            success = True
            break

    if not success:
        consec_missing += 1
        if consec_missing >= MAX_CONSECUTIVE_MISSING:
            print("Reached max consecutive missing files. Stopping GEM download.")
            break
    else:
        consec_missing = 0

    year += 1

print("\nDownload complete.")
