
# ============================================================
# auditor_opinion_flags.py
#
# Scans PDFs for opinion types and creates dummy variables.
# Supports:
#   - Optional stock_code list to process subset
#   - Cache to skip already processed files
# ============================================================

import os
import sqlite3
import pandas as pd
import re
from pathlib import Path

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
DB_PATH = "data/hongkong.db"
PDF_DIR = "data/raw/auditor_pdfs"
TEXT_DIR = "data/raw/auditor_reports_txt"
OUTPUT_CSV = "data/processed/auditor_opinion_flags.csv"

# Regex patterns (case-insensitive). We prefer searching `.txt` files in TEXT_DIR;
# fall back to PDF extraction when a `.txt` is missing.
PATTERNS = {
    "qualified_opinion": re.compile(r"\bqualified opinion\b", re.I),
    "adverse_opinion": re.compile(r"\badverse opinion\b", re.I),
    "disclaimer_of_opinion": re.compile(r"\bdisclaimer (of )?opinion\b", re.I),
    "emphasis_of_matter": re.compile(r"\bemphasis of matter\b", re.I),
    # Conservative going-concern pattern; avoid matching bare 'going concern' without context
    "going_concern": re.compile(r"material uncertainty (related to|regarding) going concern", re.I),
}

# ------------------------------------------------------------
# Helper: Get PDFs to process from database
# ------------------------------------------------------------
def get_pdf_list(stock_codes=None):
    """
    Query the hkex_auditor_reports table for pdf_path values.
    Optionally filter by stock_codes.
    """
    conn = sqlite3.connect(DB_PATH)
    
    if stock_codes:
        # Build parameterized query for stock codes
        placeholders = ','.join('?' * len(stock_codes))
        query = f"""
            SELECT DISTINCT pdf_path
            FROM hkex_auditor_reports
            WHERE stock_code IN ({placeholders})
              AND pdf_path IS NOT NULL 
              AND TRIM(pdf_path) != ''
        """
        df = pd.read_sql(query, conn, params=stock_codes)
    else:
        query = """
            SELECT DISTINCT pdf_path
            FROM hkex_auditor_reports
            WHERE pdf_path IS NOT NULL 
              AND TRIM(pdf_path) != ''
        """
        df = pd.read_sql(query, conn)
    
    conn.close()
    
    pdf_paths = df['pdf_path'].tolist()
    print(f"DEBUG: Found {len(pdf_paths)} PDF paths from database")
    
    if stock_codes:
        print(f"       (filtered by {len(stock_codes)} stock codes)")
    
    return pdf_paths

# ------------------------------------------------------------
# Main logic
# ------------------------------------------------------------
def scan_opinions(stock_codes=None):
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    # Load cache if exists and non-empty
    cache_df = pd.DataFrame()
    processed = set()
    if os.path.exists(OUTPUT_CSV) and os.path.getsize(OUTPUT_CSV) > 0:
        try:
            cache_df = pd.read_csv(OUTPUT_CSV)
            # normalize cached document names to stems (no extension)
            processed = set(Path(x).stem for x in cache_df["document_name"].tolist())
        except Exception:
            cache_df = pd.DataFrame()

    pdf_paths = get_pdf_list(stock_codes)

    if not pdf_paths:
        print("⚠️ No PDF files found in the directory or matching the stock codes.")
        print(f"   PDF directory: {PDF_DIR}")
        print(f"   Subset passed: {len(stock_codes) if stock_codes else 'None'} codes")
        return

    skipped_count = 0
    failed_count = 0
    processed_count = 0
    total_pdfs = len(pdf_paths)

    # Use cache_df as the working dataframe (we'll append to it incrementally)
    working_df = cache_df.copy() if not cache_df.empty else pd.DataFrame()

    print(f"ℹ️ Starting scan of {total_pdfs} PDFs (cache has {len(processed)} files)...")
    
    for idx, full_path in enumerate(pdf_paths, 1):
        filename = os.path.basename(full_path)
        doc_stem = Path(filename).stem
        # Attempt to parse leading date from filename stem: yyyymmdd<...>
        m = re.match(r"^(\d{4})(\d{2})(\d{2})", doc_stem)
        if m:
            report_date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        else:
            report_date = ""

        # Skip if already processed (compare stems)
        if doc_stem in processed:
            skipped_count += 1
            continue

        # Prefer extracted .txt file
        txt_path = Path(TEXT_DIR) / f"{doc_stem}.txt"
        text = ""

        if txt_path.exists():
            try:
                with open(txt_path, 'r', encoding='utf-8') as fh:
                    text = fh.read()
            except Exception as e:
                print(f"⚠️ [{idx}/{total_pdfs}] Failed to read TXT {txt_path.name}: {e} — will try PDF fallback")

        # If no .txt file available, skip (no PDF fallback configured)
        if not text:
            print(f"⚠️ [{idx}/{total_pdfs}] TXT missing for {doc_stem}; skipping (no PDF fallback)")
            failed_count += 1
            continue

        # Normalize text: lowercase and collapse whitespace
        text_proc = re.sub(r"\s+", " ", text).lower()

        # Check for opinion patterns
        flags = {k: int(bool(p.search(text_proc))) for k, p in PATTERNS.items()}

        row_df = pd.DataFrame([{"document_name": doc_stem, "report_date": report_date, **flags}])
        
        # Append to working dataframe and save incrementally
        working_df = pd.concat([working_df, row_df], ignore_index=True)
        working_df.to_csv(OUTPUT_CSV, index=False)
        
        processed_count += 1
        
        # Progress output every 50 files
        if processed_count % 50 == 0:
            print(f"✅ Progress: {processed_count} processed, {skipped_count} skipped, {failed_count} failed | [{idx}/{total_pdfs}]")

    print(f"\n{'='*70}")
    print("✅ SCAN COMPLETE")
    print(f"   Newly processed: {processed_count}")
    print(f"   Skipped (cached): {skipped_count}")
    print(f"   Failed: {failed_count}")
    print(f"   Total in cache: {len(working_df)} → {OUTPUT_CSV}")
    print(f"{'='*70}")

# ------------------------------------------------------------
# Entry point
# ------------------------------------------------------------
if __name__ == "__main__":
    subset_file = Path("data/stock_codes_subset.txt")
    if subset_file.exists():
        with open(subset_file) as f:
            subset = [line.strip() for line in f if line.strip()]
        print(f"ℹ️ Subset mode: {len(subset)} stock codes from {subset_file}")
        scan_opinions(stock_codes=subset)
    else:
        scan_opinions()  # full scan
