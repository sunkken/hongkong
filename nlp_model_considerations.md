# NLP Model Considerations — Auditor Report Classification

Date: 2025-12-19
Author: TS

## Purpose
Summarise options and an execution plan for classifying auditor reports into these flags:
1. Qualified Opinion
2. Adverse Opinion
3. Disclaimer Opinion
4. Material uncertainty regarding going concern
5. Emphasis of matter
6. Unclear / ambiguous (optional)

We will first run a fast, exact-phrase baseline on extracted `.txt` files, then (only if needed) try a lightweight NLP pipeline using chunking + retrieval and a managed LLM (Gemini) or a local transformer (ModernBERT) later.

### Classification Criteria — Audit Report Outcomes

| Report Element | Type | Severity / Impact | Meaning |
|---|---|---|---|
| **Qualified Opinion** | Modified Opinion | Moderate | "Except for" one or two specific issues, the financial statements are fairly presented in all material respects. The auditor has reservations about limited aspects of the business or accounting treatment. |
| **Adverse Opinion** | Modified Opinion | **Highest** | The financial statements are materially misstated or misleading and cannot be relied upon as a whole. The auditor cannot certify their fairness. |
| **Disclaimer of Opinion** | Modified Opinion | High (Unknown) | The auditor cannot form an opinion because they lacked sufficient evidence, independence, or scope during the audit. The financial statements remain unverified and potentially unreliable. |
| **Emphasis of Matter** | Communication (Clean Opinion) | Informational | The financial statements are fairly presented and the audit opinion is unmodified. However, the auditor draws attention to a specific matter (e.g., related-party transactions, significant uncertainty, subsequent events) that users should carefully consider. |
| **Material Uncertainty Regarding Going Concern (MURGC)** | Communication (Clean Opinion) | Informational | The financial statements are fairly presented, but the auditor has identified substantial doubt about the company's ability to continue as a going concern in the foreseeable future. Users should assess solvency and liquidation risk. |

---

---

## Dataset & constraints
- ~4,400 PDF reports → extracted as `.txt` files (script: `modules/extract_auditor_pdfs_to_txt.py`).
- Typical length: ~50k–300k characters per document (~20k–120k tokens).
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

Option A — Managed LLMs (Google Gemini, GPT, Claude)
- Pipeline: chunk → (optional) embed → relevance filtering → classification.
- Steps:
  1. Chunk each document into overlapping chunks (e.g., 20k–40k token chunks with 20% overlap for moderate context; larger if long-context confirmed).
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

- **Token ↔ characters (rule of thumb):**  
  1 token ≈ 3–4 English characters. Using that:  
  - 50,000 characters ≈ 10k–20k tokens  
  - 300,000 characters ≈ 60k–120k tokens  
  These are rough estimates — exact counts depend on tokenizer and language.

- **Managed LLMs (Gemini, GPT, Claude, etc.):**  
  - Many current long-context variants support **128k tokens or more** (some even 200k+).  
  - If you confirm access to a long-context model, you can process **entire documents or very large chunks** (e.g., 50k–100k tokens) without aggressive splitting.  
  - For smaller-context plans (8k–32k tokens), chunking is still required.

- **Local transformer models (BERT-family, ModernBERT, etc.):**  
  - Typical sequence-length limits remain **512–4096 tokens** for most local models.  
  - Long-context transformer variants exist (e.g., RoPE-based or sliding-window architectures) but rarely exceed **16k tokens** in practical deployments.

- **Practical chunking defaults (safe across systems):**  
  - **Conservative default:** 1,000–2,000 tokens (≈ 3k–8k characters). Works everywhere.  
  - **If long-context managed model confirmed:**  
    - Use **8k–16k tokens** for efficiency, or even larger (e.g., 32k+) if the model supports it and latency/cost are acceptable.  
  - Maintain **10–20% overlap** between chunks to preserve context across boundaries.

- **Cost / speed considerations:**  
  - Larger chunks reduce API calls but increase per-call cost and latency.  
  - Use **embeddings + retrieval** to pre-filter chunks: compute embeddings once, retrieve top-k relevant chunks per document, then classify only those chunks with an LLM — this drastically reduces cost.

- **Aggregation strategy:**  
  - Do per-chunk classification, then aggregate to document-level (majority vote or weighted by embedding similarity/confidence).  
  - Add a final `unclear` flag when (1,2,3) are all false or when chunk-level labels conflict strongly.

Keep this section as a quick reminder when you design chunking and when you talk to IT about which managed model / plan is available.

