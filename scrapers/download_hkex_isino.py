import os
import json
import requests
from datetime import datetime

URL = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/ISINs-assigned-by-Other-Numbering-Agencies/isino.xls"
SAVE_DIR = "/data/raw"
FILENAME = "isino.xls"
SAVE_PATH = os.path.join(SAVE_DIR, FILENAME)
META_PATH = SAVE_PATH + ".metadata"


def format_size(bytes_size):
    """Convert bytes to human-readable size."""
    for unit in ["B", "KB", "MB", "GB"]:
        if bytes_size < 1024:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024
    return f"{bytes_size:.1f} TB"


def fetch_file():
    os.makedirs(SAVE_DIR, exist_ok=True)
    headers = {}

    # Load metadata if available
    meta = {}
    if os.path.exists(META_PATH):
        with open(META_PATH, "r") as meta_file:
            meta = json.load(meta_file)
            if "ETag" in meta:
                headers["If-None-Match"] = meta["ETag"]
            if "Last-Modified" in meta:
                headers["If-Modified-Since"] = meta["Last-Modified"]

    print(f"Checking {URL} for updates...")

    try:
        response = requests.get(URL, headers=headers, timeout=30)

        # File not modified since last download
        if response.status_code == 304:
            file_size = os.path.getsize(SAVE_PATH)
            last_modified = meta.get("Last-Modified", "Unknown")
            print("✅ File is up to date.")
            print(f"📦 Size: {format_size(file_size)}")
            print(f"🕒 Last modified (server): {last_modified}")
            return

        # Raise error if not successful
        response.raise_for_status()

        # Save file
        with open(SAVE_PATH, "wb") as f:
            f.write(response.content)
        print(f"✅ File downloaded and saved to {SAVE_PATH}")

        # Update metadata
        meta = {}
        if "ETag" in response.headers:
            meta["ETag"] = response.headers["ETag"]
        if "Last-Modified" in response.headers:
            meta["Last-Modified"] = response.headers["Last-Modified"]
        with open(META_PATH, "w") as meta_file:
            json.dump(meta, meta_file)

        print("💾 Metadata updated.")
        if "Last-Modified" in meta:
            print(f"🕒 Last modified (server): {meta['Last-Modified']}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Download failed: {e}")


if __name__ == "__main__":
    fetch_file()
