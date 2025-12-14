# ============================================================
# slice_auditor_reports.py
#
# Read `.txt` files, remove table-like lines, and write cleaned files to
# `data/raw/auditor_reports_sliced/` preserving filenames.
# ============================================================

from pathlib import Path
import re

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
SRC_DIR = Path("data/raw/auditor_reports_txt")
OUT_DIR = Path("data/raw/auditor_reports_sliced")
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def remove_table_lines(text: str) -> str:
    """Return text with table-like lines removed.

    Heuristic: line is table-like when >30% of chars are digits and length > 20.
    """
    out_lines = []
    for ln in text.splitlines():
        s = ln.strip()
        if not s:
            continue
        digit_frac = sum(ch.isdigit() for ch in s) / max(1, len(s))
        if digit_frac > 0.3 and len(s) > 20:
            continue
        out_lines.append(s)
    return "\n".join(out_lines)


def remove_non_ascii(text: str) -> str:
    """Strip characters outside the 7-bit ASCII range to remove CJK columns.

    Keeps newline and standard whitespace; removes characters with ord >= 128.
    This is intentionally aggressive to focus analysis on the English column.
    """
    return ''.join(ch for ch in text if ord(ch) < 128)

START_KEYWORDS = [
    "audit",
    "opinion",
    "management",
    "material uncertainty",
    "going concern",
    "emphasis of matter",
]

STOP_KEYWORDS = [
    "financial",
    "statements",
    "consolidated",
    "balance",
    "income",
    "notes",
    "appendix",
    "schedule",
    "result",
    "assets",
    "liabilities",
    "profit",
]

MIN_EXTRACT_CHARS = 2000


def extract_section_by_heading(text: str) -> str:
    """Conservative extraction: find first heading line containing any START_HEADINGS
    (case-insensitive, don't rely on capitalization), then return everything from
    that heading until the next major section break or EOF. Ensure at least
    MIN_EXTRACT_CHARS are returned (extend to next section or EOF if needed).

    If no heading is found, return the table-cleaned full text (conservative fallback).
    """
    cleaned = remove_table_lines(text)
    lines = cleaned.splitlines()
    lower_lines = [ln.lower() for ln in lines]

    # find start line
    start_idx = None
    for i, ln in enumerate(lower_lines):
        if any(re.search(r"\b" + re.escape(k) + r"\b", ln) for k in START_KEYWORDS):
            start_idx = i
            break

    if start_idx is None:
        # no heading found: fallback to cleaned text
        return cleaned

    # find end line (next stop heading or a major all-caps/numbered heading)
    end_idx = None
    for j in range(start_idx + 1, len(lines)):
        ln = lines[j].strip()
        low = ln.lower()
        # STOP keywords end the section unless the same line also matches a START keyword
        if any(re.search(r"\b" + re.escape(k) + r"\b", low) for k in STOP_KEYWORDS):
            also_start = any(re.search(r"\b" + re.escape(k) + r"\b", low) for k in START_KEYWORDS)
            if not also_start:
                end_idx = j
                break
        if end_idx is not None:
            break

        # heuristic: a line that looks like a major heading (many uppercase letters)
        # but be conservative: require at least 3 letters and most chars uppercase/non-alpha
        sstr = ln.strip()
        if len(sstr) >= 3:
            # treat as major heading if it's mostly uppercase/number/punct
            upper_like = re.fullmatch(r"[A-Z0-9 \-\'\(\)\,\.]+", sstr)
            if upper_like:
                end_idx = j
                break

    if end_idx is None:
        end_idx = len(lines)

    # map line indexes to character offsets
    cum = [0]
    for ln in lines:
        cum.append(cum[-1] + len(ln) + 1)

    start_char = cum[start_idx]
    end_char = cum[end_idx] if end_idx <= len(lines) else len(cleaned)

    # ensure minimum size: extend end_char until MIN_EXTRACT_CHARS or EOF
    if end_char - start_char < MIN_EXTRACT_CHARS:
        desired = start_char + MIN_EXTRACT_CHARS
        if desired <= len(cleaned):
            # find the line index that reaches desired
            k = start_idx
            while k < len(lines) and cum[k + 1] < desired:
                k += 1
            end_char = cum[min(k + 1, len(lines))]
        else:
            end_char = len(cleaned)

    return cleaned[start_char:end_char].strip()


def remove_stop_blocks(text: str) -> str:
    """Remove blocks that start with a STOP keyword and end at the next START keyword.

    Algorithm (conservative):
    - Work on the ASCII-filtered text with original line breaks preserved.
    - Iterate through lines; when a line contains a STOP keyword (and not a START
      keyword on the same line), skip lines until a line containing a START
      keyword is found. When a START is found, include that START and continue
      adding lines until at least MIN_EXTRACT_CHARS characters have been added
      for that kept region (to avoid tiny fragments). Then resume scanning.
    - If EOF occurs while collecting minimum chars, include until EOF and stop.
    """
    lines = text.splitlines(keepends=True)
    n = len(lines)
    i = 0
    out = []

    def line_matches_start(s: str) -> bool:
        low = s.lower()
        return any(re.search(r"\b" + re.escape(k) + r"\b", low) for k in START_KEYWORDS)

    def line_matches_stop(s: str) -> bool:
        low = s.lower()
        return any(re.search(r"\b" + re.escape(k) + r"\b", low) for k in STOP_KEYWORDS)

    while i < n:
        ln = lines[i]
        low = ln.lower()
        if line_matches_stop(low) and not line_matches_start(low):
            # skip until next start
            j = i + 1
            found_start = None
            while j < n:
                if line_matches_start(lines[j]):
                    found_start = j
                    break
                j += 1
            if found_start is None:
                # no start found: drop the remainder and finish
                break
            # include from found_start, ensuring minimum characters
            cum_chars = 0
            k = found_start
            while k < n and cum_chars < MIN_EXTRACT_CHARS:
                out.append(lines[k])
                cum_chars += len(lines[k])
                k += 1
            i = k
            continue
        else:
            out.append(ln)
            i += 1

    return ''.join(out)


def slice_file(src_path: Path, out_dir: Path = OUT_DIR) -> None:
    """Read `src_path`, remove table-like lines, and write to `out_dir`."""
    try:
        text = src_path.read_text(encoding="utf-8")
    except Exception:
        text = src_path.read_text(encoding="latin-1")

    # remove non-ASCII (strip CJK/Chinese columns), then remove STOP->START blocks
    text_ascii = remove_non_ascii(text)
    # First try the new remove-stop-blocks strategy which preserves most content
    processed = remove_stop_blocks(text_ascii)
    if not processed:
        # fallback to older start-based extraction
        section = extract_section_by_heading(text_ascii)
    else:
        section = processed

    # preserve line breaks and structure: normalize spaces within lines, keep blank lines
    lines = section.splitlines()
    norm_lines = [re.sub(r"\s+", " ", ln).rstrip() for ln in lines]
    # collapse runs of blank lines to at most one
    out_lines = []
    blank = False
    for ln in norm_lines:
        if not ln:
            if not blank:
                out_lines.append("")
            blank = True
        else:
            out_lines.append(ln)
            blank = False
    out_text = "\n".join(out_lines).strip()
    out_path = out_dir / src_path.name
    out_path.write_text(out_text, encoding="utf-8")


# ------------------------------------------------------------
# Main logic
# ------------------------------------------------------------
def slice_all(verbose: bool = True) -> None:
    if not SRC_DIR.exists():
        raise SystemExit(f"Source dir missing: {SRC_DIR}")
    files = sorted(SRC_DIR.glob("*.txt"))
    if not files:
        raise SystemExit(f"No txt files in {SRC_DIR}")

    for i, p in enumerate(files, 1):
        slice_file(p)
        if verbose and i % 200 == 0:
            print(f"Sliced {i}/{len(files)} files")

    if verbose:
        print(f"Done — wrote {len(files)} sliced files to {OUT_DIR}")


# ------------------------------------------------------------
# Entry point
# ------------------------------------------------------------
if __name__ == "__main__":
    # Helpful console output similar to other scripts
    files = list(SRC_DIR.glob("*.txt")) if SRC_DIR.exists() else []
    print(f"ℹ️ Slicer: {len(files)} files in {SRC_DIR} -> {OUT_DIR}")
    if not files:
        print("⚠️ No files to process; check the source directory.")
    else:
        slice_all()
    
