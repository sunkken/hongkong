
# hongkong/helpers/query_list_builder.py
import csv
from pathlib import Path
from typing import Iterable, Set, Tuple

# ----------------------------
# Basic I/O helpers
# ----------------------------
def _read_ids(file_path: str | Path) -> Set[str]:
    """
    Read IDs from a file into a set (used for ignore/existing).
    Returns an empty set if file does not exist.
    """
    p = Path(file_path)
    if not p.exists():
        return set()
    with p.open("r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}

def _write_ids_preserve_order(file_path: str | Path, ids_in_order: Iterable[str]) -> None:
    """
    Write IDs exactly in the given iterable order (no sorting, no deduplication).
    The caller is responsible for filtering/dedup if needed.
    """
    p = Path(file_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for sid in ids_in_order:
            f.write(f"{sid}\n")

# ----------------------------
# Existing CSV detection
# ----------------------------
def _already_downloaded_ids(save_dir: str | Path, pattern: str = "press_releases_*.csv") -> Set[str]:
    """
    Return a set of IDs inferred from existing per-ID CSV filenames.
    Expects files like 'press_releases_<ID>.csv' in `save_dir`.
    """
    d = Path(save_dir)
    if not d.exists():
        return set()
    ids: Set[str] = set()
    for file in d.glob(pattern):
        stem = file.stem
        if stem.startswith("press_releases_"):
            sid = stem.replace("press_releases_", "")
            if sid:
                ids.add(sid)
    return ids

# ----------------------------
# Build pending query list (preserve order)
# ----------------------------
def build_query_list(
    full_list_file: str | Path,
    ignore_list_file: str | Path,
    output_pending_file: str | Path,
    *,
    skip_existing: bool = True,
    existing_save_dir: str | Path = "data/press",
    existing_pattern: str = "press_releases_*.csv",
) -> Tuple[int, int, int, int]:
    """
    Creates a pending query list from a full list and an ignore list.
    Preserves the original order from full_list_file.
    Optionally excludes IDs that already have CSVs in `existing_save_dir`.

    Returns: (total_full, total_ignore, total_existing, total_pending)
    """
    # Read full list in order
    full_ids_list: list[str] = []
    p_full = Path(full_list_file)
    if p_full.exists():
        with p_full.open("r", encoding="utf-8") as f:
            for line in f:
                sid = line.strip()
                if sid:
                    full_ids_list.append(sid)

    # Sets for ignore/existing checks
    ignore_ids   = _read_ids(ignore_list_file)
    existing_ids = _already_downloaded_ids(existing_save_dir, existing_pattern) if skip_existing else set()

    # Filter while preserving order
    pending_ordered = [sid for sid in full_ids_list if sid not in ignore_ids and sid not in existing_ids]

    # Write pending exactly in this order
    _write_ids_preserve_order(output_pending_file, pending_ordered)

    return (len(full_ids_list), len(ignore_ids), len(existing_ids), len(pending_ordered))

# ----------------------------
# Update ignore list from summary CSV
# ----------------------------
def update_ignore_from_summary(
    ignore_list_file: str | Path,
    summary_csv_file: str | Path,
    *,
    id_column: str = "stock_id",
    status_column: str = "status",
    statuses_to_add: Tuple[str, ...] = ("saved",),
) -> int:
    """
    Reads summary_csv_file (written by the downloader) and appends IDs with
    status in `statuses_to_add` to ignore_list_file.
    Keeps ignore list in ascending order of first appearance (no enforced sorting).
    Returns: number of newly added IDs.
    """
    # Read existing ignore IDs in order (if present)
    p_ignore = Path(ignore_list_file)
    existing_order: list[str] = []
    if p_ignore.exists():
        with p_ignore.open("r", encoding="utf-8") as f:
            existing_order = [line.strip() for line in f if line.strip()]
    existing_set = set(existing_order)

    # Extract new IDs from summary
    new_ids: list[str] = []
    p_summary = Path(summary_csv_file)
    if not p_summary.exists():
        return 0

    with p_summary.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sid = str(row.get(id_column, "")).strip()
            status = str(row.get(status_column, "")).strip()
            if sid and status in statuses_to_add and sid not in existing_set:
                new_ids.append(sid)
                existing_set.add(sid)

    if not new_ids:
        return 0

    # Append new IDs to the end, preserving the existing order + new arrivals
    p_ignore.parent.mkdir(parents=True, exist_ok=True)
    with p_ignore.open("w", encoding="utf-8") as f:
        for sid in existing_order:
            f.write(f"{sid}\n")
        for sid in new_ids:
            f.write(f"{sid}\n")

    return len(new_ids)

# ----------------------------
# Optional CLI usage
# ----------------------------
if __name__ == "__main__":
    FL = "data/lists/stock_ids.txt"
    IG = "data/lists/ignore_ids.txt"
    OUT = "data/lists/pending_ids.txt"
    totals = build_query_list(FL, IG, OUT, skip_existing=True, existing_save_dir="data/press")
    print(f"[build] full={totals[0]} ignore={totals[1]} existing={totals[2]} pending={totals[3]}")

    added = update_ignore_from_summary(IG, "data/press/press_summary.csv")
    print(f"[update] ignore added={added}")