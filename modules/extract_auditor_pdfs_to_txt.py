# ============================================================
# extract_auditor_pdfs_to_txt.py
#
# Extracts text from PDFs and saves as .txt files
# Uses database to identify PDFs, caches processed filenames
# ============================================================

import os
import sqlite3
import pdfplumber
import pandas as pd
from pathlib import Path

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
DB_PATH = "data/hongkong.db"
TXT_OUTPUT_DIR = "data/raw/auditor_reports_txt"
CACHE_FILE = "data/processed/extract_auditor_pdfs.cache"

# ------------------------------------------------------------
# Helper: Get PDFs to process from database
# ============================================================
def get_pdf_list(stock_codes=None):
    """
    Query the hkex_auditor_reports table for pdf_path values.
    Optionally filter by stock_codes.
    """
    conn = sqlite3.connect(DB_PATH)
    
    if stock_codes:
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

# ============================================================
# Main extraction logic
# ============================================================
def extract_pdfs(stock_codes=None):
    """Extract text from PDFs and save as .txt files."""
    
    # Create output directory
    os.makedirs(TXT_OUTPUT_DIR, exist_ok=True)
    
    # Load cache (list of already-processed filenames)
    processed = set()
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                processed = set(line.strip() for line in f if line.strip())
        except Exception as e:
            print(f"⚠️ Warning reading cache: {e}")
    
    pdf_paths = get_pdf_list(stock_codes)
    
    if not pdf_paths:
        print("⚠️ No PDF files found in database.")
        print(f"   Subset passed: {len(stock_codes) if stock_codes else 'None'} codes")
        return
    
    total_pdfs = len(pdf_paths)
    extracted_count = 0
    skipped_count = 0
    failed_count = 0
    
    print(f"ℹ️ Starting extraction of {total_pdfs} PDFs...")
    print(f"   Output directory: {TXT_OUTPUT_DIR}\n")
    
    for idx, pdf_path in enumerate(pdf_paths, 1):
        filename = os.path.basename(pdf_path)
        txt_filename = filename.replace('.pdf', '.txt')
        
        # Skip if already processed
        if txt_filename in processed:
            skipped_count += 1
            continue
        
        pdf_path_obj = Path(pdf_path)
        
        # Verify file exists
        if not pdf_path_obj.exists():
            print(f"❌ [{idx}/{total_pdfs}] Missing: {filename}")
            failed_count += 1
            continue
        
        # Extract text from PDF
        text = ""
        try:
            with pdfplumber.open(pdf_path_obj) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
        except Exception as e:
            print(f"❌ [{idx}/{total_pdfs}] Failed to extract {filename}: {e}")
            failed_count += 1
            continue
        
        # Save to txt file
        try:
            txt_output_path = Path(TXT_OUTPUT_DIR) / txt_filename
            with open(txt_output_path, 'w', encoding='utf-8') as f:
                f.write(text)
        except Exception as e:
            print(f"❌ [{idx}/{total_pdfs}] Failed to save {txt_filename}: {e}")
            failed_count += 1
            continue
        
        # Update cache
        extracted_count += 1
        with open(CACHE_FILE, 'a') as f:
            f.write(txt_filename + '\n')
        
        # Progress output every 50 files
        if extracted_count % 50 == 0:
            print(f"✅ Progress: {extracted_count} extracted, {skipped_count} skipped, {failed_count} failed | [{idx}/{total_pdfs}]")
    
    print(f"\n{'='*70}")
    print("✅ EXTRACTION COMPLETE")
    print(f"   Newly extracted: {extracted_count}")
    print(f"   Skipped (cached): {skipped_count}")
    print(f"   Failed: {failed_count}")
    print(f"   Output directory: {TXT_OUTPUT_DIR}")
    print(f"{'='*70}\n")

# ============================================================
# Entry point
# ============================================================
if __name__ == "__main__":
    subset_file = Path("data/stock_codes_subset.txt")
    if subset_file.exists():
        with open(subset_file) as f:
            subset = [line.strip() for line in f if line.strip()]
        print(f"ℹ️ Subset mode: {len(subset)} stock codes from {subset_file}\n")
        extract_pdfs(stock_codes=subset)
    else:
        extract_pdfs()  # full extraction
