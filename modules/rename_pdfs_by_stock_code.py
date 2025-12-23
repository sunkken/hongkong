#!/usr/bin/env python
"""
rename_pdfs_by_stock_code.py

Renames PDF files in data/raw/auditor_pdfs to include stock_code prefix
and updates the pdf_path in hkex_auditor_reports table accordingly.
Run this once after backing up the database and PDF folder.
"""

import os
import sqlite3
import shutil
import sys

DB_PATH = "data/hongkong.db"
PDF_DIR = "data/raw/auditor_pdfs"
RENAMED_DIR = "data/processed/auditor_pdfs"

def rename_pdfs():
    if not os.path.exists(DB_PATH):
        print("❌ Database not found.")
        return False
    
    os.makedirs(PDF_DIR, exist_ok=True)
    os.makedirs(RENAMED_DIR, exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all pdf_path and stock_code
    cursor.execute("SELECT stock_code, pdf_path, document_name FROM hkex_auditor_reports WHERE pdf_path IS NOT NULL AND TRIM(pdf_path) != ''")
    rows = cursor.fetchall()
    
    renamed_count = 0
    skipped_count = 0
    for row in rows:
        stock_code, pdf_path, document_name = row
        filename = os.path.basename(pdf_path)
        if not filename.startswith(f"[c{stock_code}]-["):
            # Construct new filename
            new_filename = f"[c{stock_code}]-[{filename}]"
            new_pdf_path = os.path.join("data", "processed", "auditor_pdfs", new_filename).replace(os.sep, "/")
            
            # Copy file if it exists (keep original)
            old_full_path = os.path.join(PDF_DIR, filename)
            new_full_path = os.path.join(RENAMED_DIR, new_filename)
            if os.path.exists(new_full_path):
                skipped_count += 1
                continue
            if os.path.exists(old_full_path):
                shutil.copy(old_full_path, new_full_path)
                if os.path.exists(new_full_path):
                    print(f"📁 Copied: {filename} -> {new_filename}")
                    # Update DB
                    cursor.execute("UPDATE hkex_auditor_reports SET pdf_path = ? WHERE pdf_path = ?", (new_pdf_path, pdf_path))
                    renamed_count += 1
                else:
                    print(f"❌ Failed to copy: {filename}")
            else:
                print(f"⚠️ Source file not found: {old_full_path}")
    
    conn.commit()
    conn.close()
    print(f"✅ Copied {renamed_count} PDFs to {RENAMED_DIR}.")
    if skipped_count > 0:
        print(f"⏭️ Skipped {skipped_count} already existing files.")
    return True

def revert_db_paths():
    if not os.path.exists(DB_PATH):
        print("❌ Database not found.")
        return False
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get rows where pdf_path is in processed
    cursor.execute("SELECT stock_code, pdf_path FROM hkex_auditor_reports WHERE pdf_path LIKE 'data/processed/auditor_pdfs/%'")
    rows = cursor.fetchall()
    
    reverted_count = 0
    for row in rows:
        stock_code, pdf_path = row
        filename = os.path.basename(pdf_path)
        if filename.startswith(f"[c{stock_code}]-["):
            # Remove prefix
            original_filename = filename[len(f"[c{stock_code}]-["):-1]  # Remove "[c<stock_code>]-[" and the trailing ]
            original_pdf_path = os.path.join("data", "raw", "auditor_pdfs", original_filename).replace(os.sep, "/")
            cursor.execute("UPDATE hkex_auditor_reports SET pdf_path = ? WHERE pdf_path = ?", (original_pdf_path, pdf_path))
            reverted_count += 1
    
    conn.commit()
    conn.close()
    print(f"✅ Reverted {reverted_count} DB paths to original.")
    return True

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == 'revert':
        success = revert_db_paths()
    else:
        success = rename_pdfs()
    if not success:
        raise SystemExit(1)