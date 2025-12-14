# Todo List (portable)

This mirrors the tracked todo list for the auditor-opinion pipeline. Do not commit unless you want these saved in the repo history.

1. Adapt classifier to .txt
   - Description: Update `modules/auditor_opinion_flags.py` to read `data/raw/auditor_reports_txt/`, normalize text (lowercase, collapse whitespace), and implement targeted regex phrase-matching (qualified, adverse, disclaimer (of )?opinion, emphasis of matter, material uncertainty going concern). Keep incremental CSV writes to `data/processed/auditor_opinion_flags.csv` and preserve resume/caching behavior.
   - Status: not-started

2. Run exact-phrase baseline
   - Description: Run the adapted `modules/auditor_opinion_flags.py` across the `.txt` files after extraction finishes. Validate matches manually on a sample set and export ambiguous examples for QA.
   - Status: not-started

3. Implement section-slicing helper
   - Description: Add `extract_relevant_text(path)` (in `modules/auditor_opinion_flags.py` or `modules/text_utils.py`) to detect opinion headings, extract surrounding prose, and drop high digit-density blocks (tables). Make the helper extensible.
   - Status: not-started

4. Run sliced baseline
   - Description: Run the exact-phrase matcher on sliced text, compare runtime and results with Phase 1, and produce a diff/metrics report.
   - Status: not-started

5. Scaffold NLP prototype
   - Description: Create `modules/auditor_opinion_flags_nlp.py` skeleton implementing chunking, embedding computation (configurable for Gemini or `sentence-transformers`), retrieval of top-k chunks, and a zero-shot classification interface. The module should support a dry-run mode (no API keys required).
   - Status: not-started


# Quick note
- These files are portable context for the VS Code chat; the Copilot Chat session history may not transfer between machines, so keep these notes and the `data/processed/` caches to resume efficiently.
