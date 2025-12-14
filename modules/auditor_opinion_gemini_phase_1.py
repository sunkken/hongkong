import configparser
import fitz  # Same as your colleague's notebook
import pandas as pd
from google import genai
from pathlib import Path
import json
import random
import time

# ============================================================
# Step 1: Read API key (EXACTLY like your colleague's notebook)
# ============================================================
print("📖 Reading API key from google-gemini.cfg...")
cfg = configparser.ConfigParser()
cfg.read('google-gemini.cfg')
api_key = cfg['Authentication']['api_key']
client = genai.Client(api_key=api_key)

# Step 2: Define model (EXACTLY like your colleague)
MODEL = 'gemini-2.5-flash'
# Keep free tier happy: slow pace with retries on 429s
REQUEST_PAUSE_SEC = 7.5  # ~8 req/min
RETRY_BACKOFF_SEC = 30.0
MAX_RETRIES = 3
# For long reports, keep everything except statements/notes
SKIP_PAGE_PATTERNS = [
    "consolidated statement",  # consolidated statements section
    "statement of profit or loss",
    "statement of financial position",
    "balance sheet",
    "statement of changes in equity",
    "statement of cash flows",
    "cash flow statement",
    "notes to the consolidated financial statements",
    "notes to the financial statements",
]

# If a page matches a skip pattern but contains any of these, we still keep it
IMPORTANT_KEEP_TOKENS = [
    "opinion",
    "auditor",
    "auditors",
    "independent auditor",
    "report on the",
    "basis for opinion",
    "material uncertainty",
    "going concern",
    "emphasis of matter",
]
print(f"🤖 Using model: {MODEL}\n")

# ============================================================
# Step 3: Extract text function (EXACTLY like your colleague)
# ============================================================
def extract_text_from_pdf(file_path):
    """Extract all pages, skipping only pages that look like pure statements/notes without opinion cues."""

    text_parts = []
    try:
        with fitz.open(file_path) as doc:
            for page_idx in range(len(doc)):
                page_text = doc.load_page(page_idx).get_text()
                lower_page = page_text.lower()

                has_skip = any(pat in lower_page for pat in SKIP_PAGE_PATTERNS)
                has_keep = any(tok in lower_page for tok in IMPORTANT_KEEP_TOKENS)

                # Skip if it matches a statement/notes page and has no opinion-related cues
                if has_skip and not has_keep:
                    continue

                text_parts.append(page_text)

    except Exception as e:
        print(f"      ❌ Read error: {str(e)[:50]}")
        return ""

    return "\n".join(text_parts)

# ============================================================
# Step 4: Classification function
# ============================================================
def classify_audit_opinion(pdf_path: str, retries: int = MAX_RETRIES) -> dict:
    """Classify single audit opinion PDF with simple retry/backoff."""

    # Extract text
    text = extract_text_from_pdf(pdf_path)
    if not text or len(text) < 100:
        return {
            "filename": Path(pdf_path).name,
            "error": "Insufficient text",
            "opinion_type": None,
            "is_uncertain": 1,
            "going_concern": 0,
            "emphasis_of_matter": 0
        }

    prompt = f"""You are an expert Hong Kong auditor opinion classifier.

Classify this audit opinion into EXACTLY ONE category:
1 = Qualified Opinion (reservations or qualifications)
2 = Adverse Opinion (material misstatement)
3 = Disclaimer Opinion (cannot express opinion)

Also check independently for:

If unsure which of 1-3, pick most severe (2 > 3 > 1) and set is_uncertain=1.

RETURN ONLY VALID JSON (no markdown, no explanation):
{{"opinion_type": 1|2|3, "is_uncertain": 0|1, "going_concern": 0|1, "emphasis_of_matter": 0|1}}

AUDIT REPORT TEXT:
    {text}"""

    for attempt in range(retries):
        try:
            response = client.models.generate_content(
                model=MODEL,
                contents=[prompt],
            )

            result = json.loads(response.text.strip())
            result["filename"] = Path(pdf_path).name
            return result

        except json.JSONDecodeError:
            if attempt == retries - 1:
                print("      ⚠️  JSON parse failed")
                return {
                    "filename": Path(pdf_path).name,
                    "error": "JSON parse failed",
                    "opinion_type": None,
                    "is_uncertain": 1,
                    "going_concern": 0,
                    "emphasis_of_matter": 0,
                }
        except Exception as e:
            msg = str(e)
            if ("RESOURCE_EXHAUSTED" in msg) or ("429" in msg):
                print("      ⏳ 429 hit, backing off...")
                time.sleep(RETRY_BACKOFF_SEC)
                continue
            if attempt == retries - 1:
                print(f"      ❌ API error: {msg[:80]}")
                return {
                    "filename": Path(pdf_path).name,
                    "error": msg[:80],
                    "opinion_type": None,
                    "is_uncertain": 1,
                    "going_concern": 0,
                    "emphasis_of_matter": 0,
                }

    # Safety fallback if all retries fail without returning
    return {
        "filename": Path(pdf_path).name,
        "error": "No response after retries",
        "opinion_type": None,
        "is_uncertain": 1,
        "going_concern": 0,
        "emphasis_of_matter": 0,
    }

# ============================================================
# Step 5: Phase 1 Validation - Run on 100 PDFs
# ============================================================

print("="*70)
print("PHASE 1: VALIDATION ON 100 RANDOM PDFs")
print("="*70 + "\n")

# Get all PDFs
pdf_dir = Path("data/raw/auditor_pdfs")
all_pdfs = list(pdf_dir.glob("*.pdf"))

if not all_pdfs:
    print(f"❌ ERROR: No PDFs found in {pdf_dir}")
    exit()

# Sample 100 random PDFs
sample = random.sample(all_pdfs, min(100, len(all_pdfs)))
print(f"📊 Sampling {len(sample)} PDFs from {len(all_pdfs)} total")
print(f"🕐 Estimated time: ~10 minutes (Gemini rate limit: 15 req/min)")
print(f"💰 Cost: $0.00 (free tier)\n")

results = []
errors = 0
start_time = time.time()

for i, pdf_path in enumerate(sample, 1):
    # Progress indicator
    print(f"[{i:3d}/100] {pdf_path.name:45s}", end=" ", flush=True)
    
    # Classify
    result = classify_audit_opinion(str(pdf_path))
    results.append(result)
    
    # Status
    if result.get("error"):
        errors += 1
        print(f"❌ Error")
    else:
        op = result['opinion_type']
        unc = "?" if result['is_uncertain'] else "✓"
        gc = "GC" if result['going_concern'] else "--"
        em = "EM" if result['emphasis_of_matter'] else "--"
        print(f"✅ Op={op} {unc} {gc} {em}")
    
    # Rate limiting: keep under free-tier quota
    if i < len(sample):
        time.sleep(REQUEST_PAUSE_SEC)

elapsed = time.time() - start_time

# ============================================================
# Step 6: Save and analyze results
# ============================================================

df = pd.DataFrame(results)
output_path = Path("testing/phase1_validation_results.csv")
output_path.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(output_path, index=False)

print("\n" + "="*70)
print("PHASE 1 COMPLETE")
print("="*70)
print(f"✅ Classified: {len(results) - errors}/100")
print(f"❌ Errors: {errors}")
print(f"⏱️  Time elapsed: {elapsed/60:.1f} minutes")
print(f"💾 Results saved: {output_path}")
print(f"💰 Cost: $0.00 (free tier)\n")

# Summary statistics
successful = df[df['error'].isna()]
print("📊 CLASSIFICATION SUMMARY:")
print(f"   Opinion 1 (Qualified):  {(successful['opinion_type'] == 1).sum()}")
print(f"   Opinion 2 (Adverse):    {(successful['opinion_type'] == 2).sum()}")
print(f"   Opinion 3 (Disclaimer): {(successful['opinion_type'] == 3).sum()}")
print(f"   Uncertain cases:        {successful['is_uncertain'].sum()}")
print(f"   Going concern flags:    {successful['going_concern'].sum()}")
print(f"   Emphasis of matter:     {successful['emphasis_of_matter'].sum()}\n")

# Show sample results
print("📋 SAMPLE RESULTS (first 10):")
print(successful[['filename', 'opinion_type', 'is_uncertain', 'going_concern', 'emphasis_of_matter']].head(10))