# NLP Model Considerations — Auditor Report Classification

Date: 2025-12-14
Author: Toni + GPT

## Purpose
Summarise options and an execution plan for classifying auditor reports into these flags:
1. Qualified Opinion
2. Adverse Opinion
3. Disclaimer Opinion
4. Material uncertainty regarding going concern
5. Emphasis of matter
6. Unclear / ambiguous (optional)

We will first run a fast, exact-phrase baseline on extracted `.txt` files, then (only if needed) try a lightweight NLP pipeline using chunking + retrieval and a managed LLM (Gemini) or a local transformer (ModernBERT) later.

---

## Dataset & constraints
- ~4,400 PDF reports → extracted as `.txt` files (script: `modules/extract_auditor_pdfs_to_txt.py`).
- Typical length: ~50k–150k characters per document (~8k–25k tokens).
- Large variety and inconsistent placement of relevant sections; financial statements/notes (tables) are irrelevant and should be trimmed if possible.
- Goal: a reliable baseline with minimal cost, quick to iterate.

---

## Phase 1 — Exact-phrase baseline (current plan)
1. Extract all PDFs to `.txt` (done). Extraction caches progress in `data/processed/extract_auditor_pdfs.cache`.
2. Run `modules/auditor_opinion_flags.py` adapted to read `.txt` files instead of PDFs.
   - Use normalized text (lowercase, collapse whitespace, remove non-essential punctuation).
   - Match targeted regex patterns (e.g., `r"\bqualified opinion\b"`, `r"\badverse opinion\b"`, `r"disclaimer (of )?opinion"`, `r"emphasis of matter"`, `r"material uncertainty related to going concern"`).
   - Incrementally save results to `data/processed/auditor_opinion_flags.csv` with cache/resume.
3. Validate on the subset (`data/stock_codes_subset.txt`). Produce a short review: ambiguous files, frequent misses.

Pros: free, deterministic, transparent, fast. Good for early validation and QC.
Cons: brittle to wording variations; may miss paraphrases.

---

## Phase 2 — Improved slicing & targeted matching
If Phase 1 shows many false negatives due to noisy document body, add slicing before matching:
1. Implement `extract_relevant_text(path)` that finds likely sections using heading heuristics (common headings include: "Opinion", "Basis for Opinion", "Emphasis of Matter", "Going Concern", "Auditor's Opinion").
2. Remove heavy numeric tables: detect blocks with high digit/line-density and drop them.
3. Match phrases only within the extracted sections (much shorter text → faster, fewer false positives).

Notes:
- Heuristics are rule-based but should capture most standard auditor report formats.
- Keep the full-text fallback if no heading found.

---

## Phase 3 — Lightweight NLP (only if baseline+slicing insufficient)
Two routes:

Option A — Managed LLMs (Google Gemini)
- Pipeline: chunk → (optional) embed → relevance filtering → classification.
- Steps:
  1. Chunk each document into overlapping chunks (e.g., 2k–4k token chunks with 20% overlap).
  2. Compute embeddings for each chunk (embedding model e.g., `text-embedding-3-small` or sentence-transformers locally).
  3. Use the auditor-opinion category queries to retrieve top-k relevant chunks per doc.
  4. Send only those relevant chunks to the LLM for classification or use a zero-shot prompt to get a structured JSON output.
  5. Aggregate chunk-level labels to document-level flags (majority, weighted by relevance confidence).
-- Pros: High accuracy, few examples needed, can provide rationales.
-- Cons: Costs (but manageable if using embeddings + filtering), needs API credentials.
-- Cost estimate: with good filtering, cost can be small (tens of dollars); raw full-doc classification is expensive.

Option B — Local / Open-source zero-shot / fine-tune (e.g., ModernBERT)
- `facebook/bart-large-mnli` for zero-shot classification on chunks, or fine-tune `ModernBERT` with labeled subset.
- For fine-tuning: label ~500–1,000 documents (or chunks) to obtain decent accuracy.
- Pros: No API costs after setup, private.
- Cons: Requires compute (GPU) for training and may still struggle with long-context without chunking.

Recommendation between A & B:
- If you have Gemini API access available, use Option A (best mix of speed & accuracy).
- Otherwise, test zero-shot via Hugging Face M-NLI models on the chunked+retrieved text. If performance poor, consider labeling a training set and fine-tuning ModernBERT.

---

## Practical pipeline (recommended minimal-cost approach)
1. Baseline: run exact-phrase script across all `.txt` and gather metrics.
2. Evaluate output: identify 200–500 ambiguous examples for further inspection / labeling if needed.
3. Slicing: implement section-slicing to remove numeric tables and isolate opinion sections.
4. If necessary, run chunking+embedding retrieval + LLM classification only on filtered chunks.

For chunking + embeddings (prototype):
- Use a script `modules/auditor_opinion_flags_nlp.py` that:
  - Loads a document, chunks into token-sized windows (use tiktoken or sentencepiece),
  - Computes embeddings (Gemini or `sentence-transformers` locally),
  - Retrieves top-K chunks, then classifies with zero-shot LLM or M-NLI model.

---

## Unclear / edge-case handling
- Add a sixth `unclear` flag when none of (1,2,3) is true or multiple conflicting labels occur.
- Keep original PDFs/txts for manual review; export ambiguous files to a folder for QA.

---

## Model limits & chunking guidance (practical reminder)

A few pragmatic notes to guide chunking and model selection:

- Token ↔ characters (rule of thumb): 1 token ≈ 3–4 English characters. Using that:
  - 50,000 characters ≈ 12k–17k tokens
  - 150,000 characters ≈ 37k–50k tokens
  These are rough estimates — exact counts depend on tokeniser and language.

- Google Gemini (managed LLMs):
  - Some Gemini variants offer very long-context capabilities (check your Gemini plan and model docs for exact limits).
  - If you have a long-context model available, you can use larger chunks (tens of thousands of tokens). If not, treat Gemini like other LLMs and use smaller chunks.

- ModernBERT / transformer models (local):
  - Typical transformer sequence-length limits are in the range ~512–4096 tokens depending on the model and architecture.
  - Many BERT-family variants are limited to 512 tokens; some modern long-context variants extend this to a few thousand tokens.

- Practical chunking defaults (safe across systems):
  - Default chunk size: **1,000–2,000 tokens** (≈ 3k–8k characters). This is a conservative, portable choice that works for local transformers and managed LLMs with moderate context.
  - If you confirm a long-context managed model, you can increase chunk size (e.g., 8k–16k tokens) to reduce the number of chunks per document.
  - Use 10–20% overlap between chunks to preserve context across boundaries.

- Cost / speed considerations:
  - Smaller chunks → more API calls but cheaper per-call token usage and safer for local models.
  - Use embeddings + retrieval to pre-filter chunks: compute embeddings once, retrieve top-k relevant chunks per document, then classify only those chunks with an LLM — this drastically reduces cost.

- Aggregation strategy:
  - Do per-chunk classification, then aggregate to document-level (majority vote or weighted by embedding similarity/confidence).
  - Add a final `unclear` flag when (1,2,3) are all false or when chunk-level labels conflict strongly.

Keep this section as a quick reminder when you design chunking and when you talk to IT about which managed model / plan is available.
