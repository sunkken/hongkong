import requests
from pathlib import Path
from datetime import datetime

# ----------------------------
# Configuration
# ----------------------------
save_dir = Path("./data/raw")
save_dir.mkdir(parents=True, exist_ok=True)

MAX_CONSECUTIVE_MISSING = 3
MAX_CACHED_CHECKS = 2  # how many cached URLs to test quickly
CURRENT_YEAR = datetime.now().year

main_years = {
    "new": range(2020, CURRENT_YEAR + 1),
    "middle": range(2012, 2020),
    "old": range(1994, 2012),
}

gem_short = range(1999, 2022)
gem_special = [2022]
gem_long = range(2024, CURRENT_YEAR + 1)

main_base = "https://www2.hkexnews.hk/-/media/HKEXnews/Homepage/New-Listings/New-Listing-Information/New-Listing-Report/Main/"
gem_base = "https://www2.hkexnews.hk/-/media/HKEXnews/Homepage/New-Listings/New-Listing-Information/New-Listing-Report/GEM/"


# ----------------------------
# Helper function
# ----------------------------
def download_file(url, filename):
    """Download a file if available."""
    if filename.exists():
        return "cached"

    try:
        head = requests.head(url, timeout=10)
        if head.status_code != 200:
            return "missing"

        resp = requests.get(url, timeout=20)
        if resp.status_code == 200:
            with open(filename, "wb") as f:
                f.write(resp.content)
            return "downloaded"
        return "failed"
    except Exception:
        return "error"


# ----------------------------
# Main logic
# ----------------------------
def process_section(label, base_url, year_ranges, save_prefix):
    """Generic section handler (Main Board or GEM)."""
    summary = {"downloaded": 0, "cached": 0, "missing": 0, "failed": 0, "error": 0}
    cached_urls = []
    consec_missing = 0

    start_year = 1994 if label == "Main Board" else 1999
    years = range(start_year, CURRENT_YEAR + 1)

    for year in years:
        urls = []

        if label == "Main Board":
            if year in year_ranges["new"]:
                urls.append(f"{base_url}NLR{year}_Eng.xlsx")
            if year in year_ranges["middle"]:
                urls.append(f"{base_url}NLR{year}_Eng.xls")
            if year in year_ranges["old"]:
                urls += [f"{base_url}{year}.XLS", f"{base_url}{year}.xls"]
        else:
            if year in gem_short:
                short = str(year)[-2:]
                urls += [f"{base_url}e_newlistings{short}.xls", f"{base_url}e_newlistings{short}.XLS"]
            if year in gem_special:
                urls += [f"{base_url}e_newlistings.xls", f"{base_url}e_newlistings.XLS"]
            if year in gem_long:
                urls.append(f"{base_url}e_newlistings{year}.xlsx")
            if year == 2023:
                continue

        success = False
        for url in urls:
            ext = url.split(".")[-1].lower()
            filename = save_dir / f"{save_prefix}_{year}.{ext}"
            result = download_file(url, filename)
            summary[result] = summary.get(result, 0) + 1

            if result == "cached":
                cached_urls.append((label, year, url))

            if result in ("downloaded", "cached"):
                success = True
                break

        if not success:
            consec_missing += 1
            if consec_missing >= MAX_CONSECUTIVE_MISSING:
                break
        else:
            consec_missing = 0

    return summary, cached_urls


def quick_check_cached(cached_urls, label):
    """Fast sanity check for cached URLs (soft warnings only)."""
    if not cached_urls:
        return

    print(f"   🔍 Checking cached URLs for {label}...")
    checked = 0
    broken = 0
    for lbl, year, url in cached_urls:
        if checked >= MAX_CACHED_CHECKS:
            break
        try:
            r = requests.head(url, timeout=3)
            if r.status_code != 200:
                print(f"      ⚠️  Broken cached URL ({lbl} {year}): {url}")
                broken += 1
        except Exception:
            print(f"      ⚠️  Broken cached URL ({lbl} {year}): {url}")
            broken += 1
        checked += 1

    if broken > 0:
        print(f"⚠️  {label}: {broken}/{checked} cached URLs returned invalid responses (soft warning).")


def download_all():
    """Download all Main Board and GEM listing files with compact summary output."""
    print("\n📘 Downloading Main Board reports...")
    main_summary, main_cached = process_section("Main Board", main_base, main_years, "Main")

    if len(main_cached) >= MAX_CACHED_CHECKS:
        quick_check_cached(main_cached, "Main Board")

    print("💎 Downloading GEM reports...")
    gem_summary, gem_cached = process_section("GEM", gem_base, main_years, "GEM")

    if len(gem_cached) >= MAX_CACHED_CHECKS:
        quick_check_cached(gem_cached, "GEM")

    # --- Final Summary ---
    print("\n📊 Download Summary:")
    print(f"  📘 Main Board: "
          f"{main_summary['downloaded']} new, "
          f"{main_summary['cached']} cached, "
          f"{main_summary['missing']} missing")
    print(f"  💎 GEM: "
          f"{gem_summary['downloaded']} new, "
          f"{gem_summary['cached']} cached, "
          f"{gem_summary['missing']} missing")

    total_failures = (
        main_summary["failed"] + main_summary["error"] +
        gem_summary["failed"] + gem_summary["error"]
    )
    if total_failures > 0:
        raise RuntimeError(f"{total_failures} downloads failed.")

    print("\n✅ Download complete.")


# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    try:
        download_all()
    except Exception as e:
        print(f"\n❌ Download process failed: {e}")
        raise
