import os
import json
import requests

# ----------------------------
# Configuration
# ----------------------------
FILES = {
    "isino": "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/ISINs-assigned-by-Other-Numbering-Agencies/isino.xls",
    "isinsehk": "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/ISINs-assigned-by-HKEX/isinsehk.xls",
    "secstkorder": "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
}

SAVE_DIR = "./data/raw"


# ----------------------------
# Helpers
# ----------------------------
def format_size(n: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def load_metadata(path: str):
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def save_metadata(path: str, headers: dict):
    meta = {k: headers[k] for k in ("ETag", "Last-Modified") if k in headers}
    with open(path, "w") as f:
        json.dump(meta, f)
    return meta


# ----------------------------
# Core download logic
# ----------------------------
def fetch_file(name: str, url: str) -> bool:
    os.makedirs(SAVE_DIR, exist_ok=True)

    save_path = os.path.join(SAVE_DIR, f"{name}.xls")
    meta_path = save_path + ".metadata"

    meta = load_metadata(meta_path)
    headers = {
        "If-None-Match": meta.get("ETag", ""),
        "If-Modified-Since": meta.get("Last-Modified", "")
    }

    print(f"\n📡 Checking HKEX {name} source...")

    try:
        r = requests.get(url, headers=headers, timeout=30)

        # Not modified
        if r.status_code == 304:
            size = os.path.getsize(save_path) if os.path.exists(save_path) else 0
            print(f"✅ {name} up to date ({format_size(size)})")
            return True

        r.raise_for_status()

        # Save file
        with open(save_path, "wb") as f:
            f.write(r.content)

        meta = save_metadata(meta_path, r.headers)

        print(f"✅ Downloaded updated {name} → {save_path}")
        print(f"📦 Size: {format_size(len(r.content))}")
        if "Last-Modified" in meta:
            print(f"🕒 Last modified (server): {meta['Last-Modified']}")

        return True

    except requests.exceptions.RequestException as e:
        print(f"❌ Download failed for {name}: {e}")
        return False


# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    results = [fetch_file(name, url) for name, url in FILES.items()]
    if not all(results):
        raise SystemExit(1)
