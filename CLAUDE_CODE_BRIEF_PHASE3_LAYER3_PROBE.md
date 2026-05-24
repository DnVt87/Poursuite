# Brief for Claude Code — Phase 3 + Layer 3 Combined Probe (v1.2)

> **v1.1 revisions (2026-05-21):** seven corrections folded in after pre-implementation review — petição cd_documento premise corrected; recall denominator reframed as coverage-vs-expected-count; tribunal list narrowed to TJSP/TJRJ/TJMG/TRT-2/TRF-3 (STJ + TST excluded as appellate-inflating); explicit disposition rule added for Probe C Layer-3 scope; PyMuPDF dep change called out; artifacts policy cleaned up; per-case runtime reported as (min, median, max).
>
> **v1.2 revisions (2026-05-21):** snapshot store reality check applied. Store has 4 loaded cases (2020/2022/2023×2; no 1990, no 2015–2020) and 0 cases with `other_processes > 1`. Probe A runs against the 4 available cases (case-mix-unmet is itself a finding). Probe B splits: common-name archetype uses **synthetic names** (no ground truth needed — just measuring DataJud noise); distinctive + PJ archetype uses the 4 real debtors; parallel-case archetype **dropped** with note "no candidates in store today." Also: eSAJ download auth and DataJud name-search query field added as explicit step-0 discoveries; disambiguation reference-case made explicit; dataAjuizamento window left to probe to tune.

## Goal

Two probes in one workstream, both reusable diagnostics under `poursuite/probes/`. Each answers empirical questions whose answers determine whether/how to build Phase 3 (deep search) and Layer 3 (DataJud production module). After both probes run, produce a synthesis report with an explicit recommendation for sequencing.

Per the project's methodology (see `PLAN.md` §11): verify before building. The first DataJud probe and the eSAJ inventory both surfaced findings that materially changed the design. Same discipline here.

## Probe A — Phase 3 (document download + extraction)

### What we want to learn

Given that Phase 2's snapshot store captures `cd_documento` references on movimentos, what is the actual reality of downloading and extracting text from those documents? Specifically:

**Scope note — petição-side documents are out of scope for this probe.** The `peticao.cd_documento` column exists for forward-compat but is always NULL today: eSAJ does not expose document IDs in the "Petições diversas" section (see `poursuite/scraper/peticoes.py` header comment, lines 15–19). Probe A operates exclusively on `movimento.cd_documento`. The petição-document gap is itself a finding to surface in the report — Phase 3 will need a separate inventory before it can include petição documents.

- For documents where `cd_documento` is captured: how many are publicly downloadable vs. gated behind `#liberarAutoPorSenha` (per-case password)?
- For the publicly downloadable ones: what's the typical file size and page count?
- Text-PDF vs. image-PDF ratio (PyMuPDF can distinguish — image-PDFs have no extractable text layer)?
- For text-PDFs: how clean is extracted text? Encoding issues, layout artifacts, footer/header noise?
- What's a realistic per-case "deep search" runtime (download + extract for an average case)?
- What URL pattern actually works for downloads? The eSAJ inventory cataloged the link form (`abrirDocumentoVinculadoMovimentacao.do?cdDocumento=...`); confirm it works in practice and document quirks.

### Methodology

0. **Step 0 — eSAJ download auth discovery.** Pick one cdDocumento from the store. Try unauthenticated HTTP GET on the abrirDocumentoVinculadoMovimentacao.do URL. Record what happens: 200 with PDF body, 200 with HTML login redirect, 401, 403, or 302. If unauthenticated doesn't work, extract cookies from a Selenium session that just opened the case page and retry with those cookies. Document the working approach as the report's first finding.

1. Run against the **4 currently-loaded cases** in the snapshot store (the v1.2 reality check):
   - `1002177-13.2020.8.26.0100` — Banco ABC vs Royal Coffee (PJ), 39 cd_documento refs — high-volume slot
   - `1063559-02.2023.8.26.0100` — Banco Daycoval vs Jean Cássio Luna Santos (PF), 16 refs — recent
   - `1185092-25.2023.8.26.0100` — Itaú vs BARUK Joias (PJ), 10 refs — recent
   - `1033164-10.2022.8.26.0602` — Itaú vs Crb Imóveis (PJ), 4 refs — mid-recent
   - **Case-mix-unmet finding:** no 1990 legacy case, no 2015–2020 mid-period case, no case known to have penhora movements separately tagged. The probe report calls this out explicitly so Phase 3 design doesn't pretend coverage we don't have.

2. For each case, enumerate every `cd_documento` from the snapshot store. Cap at 20 documents per case for the probe — we're sampling, not exhaustively downloading.

3. For each document, attempt download via the eSAJ link pattern. Record:
   - HTTP status (200, 401, 403, 404)
   - Whether it required the `#liberarAutoPorSenha` flow (skip these — note them as gated)
   - File size in bytes
   - For successful downloads: PyMuPDF can extract pages, count them, detect text vs. image

4. For the successful text-PDFs, extract text from a sample. Look at:
   - Extraction quality (does the text read naturally, or is it broken?)
   - Encoding (UTF-8 clean, or accented characters mangled?)
   - Structural noise (page headers/footers repeated per page, line numbers, etc.)
   - Word count / character count

5. **OCR is out of scope for v1 deep search.** If the probe finds image-PDFs that would need OCR, just **count them and note the proportion**. Don't run Tesseract or similar. The operator's view: lawyers do their own deep reading on individual cases, and OCR-needing PDFs are an "interesting later" path (maybe LLM-input preprocessing) but not a Phase 3 requirement. Surface as observation, defer.

### Output

`<POURSUITE_LOG_DIR>/probes/phase3_probe_<ts>/`:

- `raw_pdfs/` — every successfully downloaded PDF. Keep in the probe-run directory for the audit; operator decides deletion after reviewing the report.
- `extracted_text/` — extracted text per PDF
- `phase3_findings.md` — the report

The report covers:
- Aggregate stats: of N documents attempted, X downloaded, Y were text-PDFs, Z were image-PDFs (needs OCR), W were gated
- Per-case breakdown: how many docs were available, how many extracted cleanly
- Per-case runtime reported as **(min, median, max)** with an explicit note: "estimate from small sample (N=5), network-bound — treat as order-of-magnitude, not precise." The synthesis (Probe C) uses it as a rough sizing input, not a forecast.
- Specific extraction quality notes — examples of clean vs. noisy text
- Recommendations for the Phase 3 build (chunk size for storage, FTS5 indexing strategy on extracted text, file-size limits, etc.)

### What this is NOT

- Not building the deep-search production module
- Not running OCR
- Not solving the password-gated case (per-case credentials aren't pursuable from a probe; production might surface them in a "manual deep search" UI affordance, but that's design for later)
- Not making decisions about storage architecture (snapshot store reuse vs. new DB) — that's design after the probe

---

## Probe B — Layer 3 (DataJud name-search quality)

### What we want to learn

The first DataJud probe established that the API works and returns capa + movimentações reliably. The remaining unknown is **production-grade name search**: when we query DataJud for a debtor's name across multiple tribunals, how usable are the results?

Specifically:
- For common Brazilian names (José Silva, Maria Santos): how high is the false-positive rate? Do we get 50 hits, of which 5 are the actual debtor?
- For distinctive names: does the API return them all reliably?
- What metadata does each hit carry that we can use to disambiguate? Other parties, dates, foros, classes?
- What confidence-scoring heuristic, applied to those metadata signals, produces a usable ranking?
- Cross-tribunal coverage: for a debtor we know has parallel cases (e.g., one in TJSP and one in TJRJ), does DataJud return both?

### Methodology

0. **Step 0 — DataJud name-search query field discovery.** The current `datajud.py` queries only `numeroProcesso`. Party fields are stripped from responses, but we don't yet know what field name-search *matches against*. Try `match` queries against likely candidates (`nomeParte`, `partes.nome`, `nomePessoa`, full-text `_all` / `query_string`) on TJSP with a known debtor name from the store; record which return hits, which return 0, which return error. Document the working query field as the report's first finding.

1. **Sample composition — split per v1.2 reality check** (snapshot store has 4 debtors, 0 parallel-case candidates):

   - **Common-name archetype (synthetic, 5-7 names).** No ground truth needed — we're measuring *DataJud's noise floor*, not classifying hits against known cases. Pick typical Brazilian names: "José da Silva", "Maria Santos", "João Oliveira", "Ana Pereira", "Carlos Souza" (plus 1-2 the operator wants to sanity-check). Report raw hit volume + classification distribution (definitely-different / uncertain / definitely-same is undefined here since we have no reference case; collapse to just hit-volume + foro/classe/data spread + visible duplicate-rate).

   - **Distinctive-name + PJ archetype (4 debtors from the store).** The full pool: Royal Coffee Comercial e Exportadora de Café Ltda. (PJ), Crb Imóveis Ltda (PJ), BARUK Joias e Presentes Ltda. (PJ), Jean Cássio Luna Santos (PF, distinctive). For these we *do* have a reference case (the source case in the snapshot store) — disambiguation classification applies.

   - **Parallel-case archetype — DROPPED.** Snapshot store has 0 cases with `other_processes > 1`. Worth checking whether the field is being populated correctly at scrape time (see scraper at [poursuite/scraper/esaj.py:439](poursuite/scraper/esaj.py#L439)); the probe report should flag this as a candidate scraper-bug to investigate separately. Cross-tribunal coverage measurement deferred.

2. For each debtor, query DataJud by name across the major tribunals: **TJSP, TJRJ, TJMG, TRT-2, TRF-3** — 5 tribunals × 25 debtors ≈ 125 queries, well within rate limits.

   **Tribunal selection rationale.** The current `CROSS_TRIBUNAL_INDICES` in `poursuite/probes/datajud.py` lists only `tjsp, tjrj, trf1, tst`. TJMG, TRT-2, TRF-3 indices must be added and a one-shot discovery query must verify each responds before counting it in the sample. STJ and TST are **excluded**: STJ is appellate-tier (recursos), so the same case traveling up inflates parallel-case counts without representing new debtor activity; TST is the labour-court appellate analogue with the same issue. If TRF-1 (currently in code) is retained, treat it as an additional comparison datapoint, not a substitute for TRF-3 in São Paulo's circuit.

3. For each hit, capture: process number, classe, dataAjuizamento, orgaoJulgador (the tribunal-level metadata that helps disambiguate). DataJud does not return parties; we work with what's available.

4. **Disambiguation classification.** Applies to the distinctive-name + PJ archetype only (where we have a reference case). The reference is **the source case in the snapshot store from which the debtor was drawn**. For each DataJud hit on that debtor's name, classify it as:
   - "Definitely same person" — strong signals: matching foro + matching dataAjuizamento window + matching classe family vs. the reference case
   - "Definitely different person" — clear signals against: distant foro, wildly different class, dataAjuizamento far outside the reference case's window
   - "Uncertain" — ambiguous signals

   The dataAjuizamento "window" is intentionally undefined here — the probe should try 6 months, 2 years, 5 years and document which produced the most-defensible classification rate. Report the chosen window and the rule.

   Document the rule used for each classification. The classification is a *heuristic*, not ground truth — that's exactly what we're testing. The probe report should be explicit about which classifications a human reviewer would override.

   **Anticipated outcome:** DataJud responses carry only `orgaoJulgador + classe + dataAjuizamento` for disambiguation (no parties). For common-name PF searches this is a thin signal set, so "Uncertain" is expected to be the modal classification. That is a **finding, not a probe failure** — it's exactly the empirical answer to "can name search alone disambiguate?" Report the rate explicitly.

5. Calculate per-debtor-archetype:
   - Hit count (raw)
   - Precision (definitely-same-person / total hits)
   - Coverage relative to expected count: does DataJud return ≥ `other_processes` hits for the same debtor across all queried tribunals combined? Strict recall is **not computable** — `other_processes` is an integer count, not a list of parallel case numbers, so we have no ground-truth list to compare against. The signal we can report: "no surprising gaps" (DataJud hit count meets or exceeds the scraper-reported count) vs. "suspected gap" (DataJud returned fewer hits than the scraper saw).
   - False-positive density: how many "definitely different" hits, especially on common names?

6. Test 2-3 confidence-scoring heuristics on the data. Examples:
   - Foro proximity (same comarca = high confidence, same UF = medium, different UF = low unless other signals support)
   - dataAjuizamento clustering (cases filed within months of each other on related classes)
   - Class family matching (execution-family cases together)

### Output

`<POURSUITE_LOG_DIR>/probes/layer3_probe_<ts>/`:

- `raw_responses/` — DataJud JSON per query
- `layer3_findings.md` — the report

The report covers:
- Aggregate stats: across N debtors, X hits returned, of which Y were classified same / Z different / W uncertain
- Breakdown by debtor archetype (common name, distinctive name, PJ, parallel-cases)
- Cross-tribunal coverage: for the parallel-cases sample, what was found vs. what was expected
- Confidence-scoring proposal: which heuristic best ranked hits; concrete formula
- Explicit "what won't work" findings: name shapes or query patterns that produced unusable results

### Tribunal-code mapping caveat

Recall from the May 2026 probe: TPU movement codes are NOT national-uniform. Same applies here — different tribunals may render the same metadata differently (foro names, classe names). The probe must surface this as a finding if it appears, not paper over it.

### What this is NOT

- Not building the production module
- Not solving entity resolution at scale (the probe's classifications are heuristic; production would need a more rigorous identity resolution layer — that's a design question for later)
- Not making decisions about how DataJud responses integrate with the snapshot store

---

## Probe C — Synthesis

After both probes run, produce a third document:

`<POURSUITE_LOG_DIR>/probes/synthesis_<ts>/probe_synthesis.md`

Three sections:

1. **Phase 3 disposition.** Based on Probe A: how hard is the Phase 3 build, realistically? Per-case runtime, complexity of failure handling, surprises that weren't in the v3.1 plan. Concrete: 1-2 weeks (the brief's estimate), or more, or less?

2. **Layer 3 disposition.** Based on Probe B: is DataJud name search usable as a production filter signal, or is the false-positive rate so high it'd require constant lawyer-in-the-loop disambiguation? Specific recommendation about whether to ship Layer 3 as planned, or scope it differently (e.g., only on PJ debtors initially, since CNPJ-keyed search is cleaner than name search).

   **Explicit disposition rule.** If >40% of common-name hits land in "Uncertain" classification, recommend Layer 3 v1 scopes to **PJ/CNPJ and distinctive-name PF only** — common-name PF search defers to a later iteration where party-cross-reference (matching other parties on the case against the snapshot store) can disambiguate. This threshold is a starting heuristic; the report should propose a final cut after seeing the data.

3. **Sequencing recommendation.** Given what we now know about both: which to build first, and why. The plan currently says Phase 3 → Layer 3; the probes may confirm, reverse, or interleave.

The recommendation should be specific and defensible. Operator reviews and decides.

---

## Constraints

- **Don't build either production module.** Probes only.
- **Reuse existing infrastructure:** `poursuite/probes/datajud.py` is already in the repo from the first DataJud probe; extend it rather than duplicating.
- **Both probes write to disk** (raw downloads, raw JSON, extracted text) so we have artifacts for the report. Artifacts stay in the probe-run directory; operator decides cleanup after review.
- **Follow the methodology disciplines** from PLAN.md §11: record everything, don't pre-judge during inventory, surface unexpected findings as findings.
- **Dependencies.** Probe A requires PyMuPDF. Verified at brief-revision time: it is **not** currently in `pyproject.toml`. Adding it is a real dep change — first step of Probe A is to add `pymupdf` to `pyproject.toml` dependencies and document the addition in the report. HTTP can use stdlib `urllib.request` (consistent with existing `poursuite/probes/datajud.py`) or `requests` if preferred; either is fine. No external services.

## Definition of done

- `poursuite/probes/phase3_probe.py` exists, runs end-to-end against 5 sample cases, produces `phase3_findings.md`
- `poursuite/probes/layer3_probe.py` (or an extension to `datajud.py`) exists, runs end-to-end against 20-30 debtors, produces `layer3_findings.md`
- `probe_synthesis.md` produced, with sequencing recommendation
- A summary at the end of the work flagging: what surprised you most, what hard questions remain, what the operator should sanity-check before committing to next workstream

## When done

Don't commit. Capture findings, save artifacts, report back. Operator reviews the three documents and picks the next workstream.
