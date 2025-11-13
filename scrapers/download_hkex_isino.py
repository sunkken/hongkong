import os
import json
import requests

# ----------------------------
# Configuration
# ----------------------------
URL = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/ISINs-assigned-by-Other-Numbering-Agencies/isino.xls"
SAVE_DIR = "./data/raw"
FILENAME = "isino.xls"
SAVE_PATH = os.path.join(SAVE_DIR, FILENAME)
META_PATH = SAVE_PATH + ".metadata"


# ----------------------------
# Helper functions
# ----------------------------
def format_size(bytes_size: int) -> str:
    """Convert bytes to human-readable size."""
    for unit in ["B", "KB", "MB", "GB"]:
        if bytes_size < 1024:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024
    return f"{bytes_size:.1f} TB"


# ----------------------------
# Main logic
# ----------------------------
def fetch_file():
    """Download the ISINO reference file with ETag/Last-Modified caching."""
    os.makedirs(SAVE_DIR, exist_ok=True)
    headers = {}
    meta = {}

    if os.path.exists(META_PATH):
        try:
            with open(META_PATH, "r") as meta_file:
                meta = json.load(meta_file)
                if "ETag" in meta:
                    headers["If-None-Match"] = meta["ETag"]
                if "Last-Modified" in meta:
                    headers["If-Modified-Since"] = meta["Last-Modified"]
        except Exception:
            meta = {}

    print("\n📡 Checking HKEX ISINO source...")

    try:
        response = requests.get(URL, headers=headers, timeout=30)

        # Up to date
        if response.status_code == 304:
            file_size = os.path.getsize(SAVE_PATH) if os.path.exists(SAVE_PATH) else 0
            print(f"✅ ISINO file up to date ({format_size(file_size)})")
            return True

        response.raise_for_status()

        # Save file
        with open(SAVE_PATH, "wb") as f:
            f.write(response.content)

        # Update metadata
        meta_out = {}
        if "ETag" in response.headers:
            meta_out["ETag"] = response.headers["ETag"]
        if "Last-Modified" in response.headers:
            meta_out["Last-Modified"] = response.headers["Last-Modified"]
        with open(META_PATH, "w") as meta_file:
            json.dump(meta_out, meta_file)

        print(f"✅ Downloaded updated ISINO file → {SAVE_PATH}")
        size_str = format_size(len(response.content))
        if "Last-Modified" in meta_out:
            print(f"🕒 Last modified (server): {meta_out['Last-Modified']}")
        print(f"📦 Size: {size_str}")
        return True

    except requests.exceptions.RequestException as e:
        print(f"❌ ISINO download failed: {e}")
        return False


# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    success = fetch_file()
    if not success:
        raise SystemExit(1)
