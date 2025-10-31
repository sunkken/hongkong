
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
import re
import pandas as pd


def find_header_row(df, max_scan=40):
    """Return the header row index (0-based) or None."""
    n = min(max_scan, len(df))
    for i in range(n):
        row_text = " ".join(str(x).lower() for x in df.iloc[i].dropna().tolist())
        # Works for Main files (e.g., "Company Name at time of listing", "Stock Code")
        if "company" in row_text and "code" in row_text:
            return i
    return None


def is_footer_row(row) -> bool:
    txt = " ".join(str(v) for v in row.dropna().tolist()).strip().lower()
    return txt.startswith("total") or txt.startswith("remarks")


def ingest_main_files(input_dir: Path, pattern: str = "Main_*.xls*") -> pd.DataFrame:
    files = sorted(input_dir.glob(pattern))
    frames = []

    for file in files:
        engine = "openpyxl" if file.suffix.lower() == ".xlsx" else "xlrd"

        try:
            xfile = pd.ExcelFile(file, engine=engine)
        except Exception as e:
            print(f"[skip] Cannot open {file.name}: {e}")
            continue

        for sheet in xfile.sheet_names:
            try:
                raw = pd.read_excel(file, sheet_name=sheet, header=None, engine=engine, dtype="object")
            except Exception as e:
                print(f"[skip] {file.name} | sheet '{sheet}': {e}")
                continue

            hdr = find_header_row(raw)
            if hdr is None:
                continue

            df = raw.iloc[hdr + 1 :].copy()
            df.columns = raw.iloc[hdr].tolist()
            df = df.dropna(how="all")
            if df.empty:
                continue

            # Drop obvious footer rows
            df = df[~df.apply(is_footer_row, axis=1)]

            # Lineage
            ym = re.search(r"(\d{4})", file.name)
            df["year"] = int(ym.group(1)) if ym else None
            df["source_file"] = file.name
            df["sheet_name"] = sheet

            frames.append(df)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def parse_args():
    p = argparse.ArgumentParser(description="Ingest Main Board Excel files into a single Parquet file (optional CSV).")
    p.add_argument(
        "--input",
        default=None,
        help="Input folder with Main Excel files (default: ../data/raw relative to this script)",
    )
    p.add_argument("--pattern", default="Main_*.xls*", help='Glob pattern (default: "Main_*.xls*")')
    p.add_argument(
        "--out-parquet",
        default="../data/listings_main.parquet",
        help="Output Parquet path (default: ../data/listings_main.parquet)",
    )
    p.add_argument(
        "--csv",
        action="store_true",
        help="Also write a CSV next to the Parquet output (same path, .csv extension).",
    )
    return p.parse_args()


def main():
    args = parse_args()
    base_dir = Path(__file__).resolve().parent
    input_dir = Path(args.input).expanduser().resolve() if args.input else (base_dir / "../data/raw").resolve()
    out_path = Path(args.out_parquet).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = ingest_main_files(input_dir=input_dir, pattern=args.pattern)
    if df.empty:
        print(f"[info] No data found in {input_dir} matching {args.pattern}")
        return
    # Parquet
    df.to_parquet(out_path, index=False)
    print(f"[ok] Wrote {len(df)} rows -> {out_path}")

    # Optional CSV
    if args.csv:
        csv_path = out_path.with_suffix(".csv")
        df.to_csv(csv_path, index=False)
        print(f"[ok] Also wrote CSV -> {csv_path}")


if __name__ == "__main__":
    main()
