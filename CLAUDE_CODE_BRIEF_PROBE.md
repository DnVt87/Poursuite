# Brief for Claude Code — DataJud & Receita CNPJ Probe Tool

## Goal

Build a reusable diagnostic tool under `poursuite/probes/` that runs exploratory queries against:
1. The CNJ DataJud public API
2. The Receita Federal CNPJ public bulk dataset

The output is a structured findings report (Markdown) that fills the empirical gaps flagged in `PROBE_FINDINGS.md` (in the project root). **Read `PROBE_FINDINGS.md` first** — it documents what's already known from research, the schemas, the limitations, and the specific empirical questions that need answering. Don't re-research; verify and extend.

This is not throwaway code. It lives in the repo as a long-term diagnostic.

## Constraints

- **Don't touch the runtime package** (`poursuite/api/`, `poursuite/db/`, `poursuite/scraper/`, `poursuite/cli.py`) — this is investigation, not feature work.
- **Reuse what's already there:** `poursuite/config.py` for paths, `poursuite/utils.py::setup_logging` for logging.
- **No new heavy dependencies.** Standard library where possible. `requests` is fine. For Receita ingestion, prefer wrapping/calling `caiopizzol/cnpj-data-pipeline` (Docker-based) or `rictom/cnpj-sqlite` over reimplementing the download — see "Receita strategy" below.
- **Be defensive.** This is a probe — every API response or file format might surprise us. Catch broadly, log clearly, never silently coerce.

## Module structure

```
poursuite/probes/
├── __init__.py
├── README.md             ← How to run the probe; what each output means
├── datajud.py            ← DataJud probe logic
├── receita.py            ← Receita CNPJ probe logic
├── tpu.py                ← TPU code lookup (small embedded subset; downloadable full table)
└── cli.py                ← `python -m poursuite.probes` entry point
```

CLI surface:
```
python -m poursuite.probes datajud --processes <file>      # 6 sample + edge cases
python -m poursuite.probes datajud --process <single>
python -m poursuite.probes receita --sample                # download minimal slice, sample
python -m poursuite.probes receita --schema-check          # verify layout matches PROBE_FINDINGS.md §2.4
python -m poursuite.probes report                          # consolidate latest probe results into a Markdown report
```

Outputs go to `<POURSUITE_LOG_DIR>/probes/<timestamp>/` (raw JSON dumps, schema diffs) plus a top-level `<POURSUITE_LOG_DIR>/probes/latest_report.md` (human-readable).

## DataJud probe

### Inputs
A YAML or JSON file with sample process numbers. Default file ships at `poursuite/probes/sample_processes.yaml` containing the 6 user-provided numbers:
```yaml
samples:
  - "1018045-50.2015.8.26.0506"   # 2015 mid-tier
  - "0141625-04.2009.8.26.0100"   # 2009 capital
  - "1002273-51.2024.8.26.0629"   # 2024 recent
  - "0700505-13.1990.8.26.0547"   # 1990 — coverage edge
  - "1185179-78.2023.8.26.0100"   # 2023 capital
  - "1046043-29.2020.8.26.0114"   # 2020 mid-tier
```
Easy to extend. Operator can pass `--processes path/to/other.yaml` to override.

### What to query
For each number:
1. POST to `https://api-publica.datajud.cnj.jus.br/api_publica_tjsp/_search` with `{"query": {"match": {"numeroProcesso": "<digits-only-form>"}}, "size": 5}`.
2. Note: try **both** the formatted form (`1018045-50.2015.8.26.0506`) and the digits-only form (`10180455020158260506`) — the API may match one but not the other; document which.
3. Capture full raw response JSON to disk under the run timestamp directory.

### What to report
For each sample, the per-process report should include:
- **Hit/miss**: did the API return any hits at all?
- **Field completeness**: of the fields listed in `PROBE_FINDINGS.md` §1.3, which are populated, which are null/missing, which are completely absent from the response?
- **`valorCausa` specifically**: per the research, this is documented but often empty for TJSP. Confirm or refute on real data.
- **Movimentos summary**: total count, date range (first to last `dataHora`), top 10 most-frequent `codigo` values with their `nome`.
- **Penhora detection**: presence of codes `11382` (Bloqueio/penhora online) or `11383` (Penhora) and any other penhora-family codes encountered.
- **Citação detection**: presence of code `12284` (Citação) or related.
- **Latency**: gap between `dataHoraUltimaAtualizacao` and the current date. For the 2024 sample specifically, also compute the gap between the latest `movimento.dataHora` in DataJud and the case's last activity per eSAJ (the operator will provide eSAJ snapshot data manually if needed; for v0 just report DataJud's latest date).
- **Sealed handling**: if any sample returns no hits, try once more with broader query (`match_phrase` on the digits-only form) and report what comes back.

### Cross-process aggregate report
After all samples processed:
- Count of samples returning hits / no hits
- Histogram of movimentos counts
- All distinct movimento codes encountered, with sample count
- Any error patterns (HTTP 4xx/5xx, malformed responses)

### Challenge experiments — don't trust the documentation, test it

The research that produced `PROBE_FINDINGS.md` relied on community blog posts, wiki pages, and worked examples. **None of those are authoritative.** A primary purpose of this probe is to verify or refute the claims directly. Run each of these experiments and report what actually happens:

**Experiment 1 — Are parties really stripped?**
The community claim is that party data is removed per Portaria 160/2020. Test:
- For each sample, request all fields (no `_source` filter, no `fields` restriction): `{"query": {"match": {"numeroProcesso": "..."}}}`.
- For each hit, dump every key at every level. Look for ANY field containing party-shaped data: `partes`, `pessoas`, `nomePessoa`, `nomeParte`, `documentoPrincipal`, `cpf`, `cnpj`, `polo`, etc.
- Try alternative request shapes that might bypass field stripping if it's a query-time decision: explicit `_source: true`, `_source: ["*"]`, `fields: ["*"]`, including via URL query params.
- Try the "explain" endpoint variant if available: `_search?explain=true`.
- Document everything. If parties really are stripped at the index level (which is what the docs say), all of these return the same stripped shape. If they're stripped at the query layer, one of these may surprise us.

**Experiment 2 — What does CPF/CNPJ search actually do?**
The community claim is that CPF/CNPJ aren't indexed for search. Test:
- Try `{"query": {"match": {"numeroDocumentoPrincipal": "<known CNPJ>"}}}`.
- Try `{"query": {"match": {"partes.numeroDocumentoPrincipal": "<known CNPJ>"}}}`.
- Try `{"query": {"query_string": {"query": "<known CNPJ>"}}}` (full-text fallback).
- Try `{"query": {"term": {"_all": "<known CNPJ>"}}}` if the API supports it.
- Use a CNPJ you know appears in one of the sample processes (extract from the eSAJ-scraped data Poursuite already has, if available; otherwise pick a publicly-known company).
- Document each: HTTP status, error message if any, hit count, whether the known case appears.
- Even if all return zero hits, the *error messages* tell us whether the field exists in the schema but isn't indexed (common case) vs. doesn't exist at all.

**Experiment 3 — Sealed (segredo de justiça) cases.**
The community claim is that sealed cases either don't appear or appear with nulled fields. Test:
- Identify a known sealed TJSP case if possible (Poursuite's existing scraper already detects these — pull a process number from its skip log if available).
- Query DataJud for it; record the response.
- Query the same case via the `match` form AND a broader `query_string` form.
- Do they show up at all? With what fields? The current code's behavior of detecting via the eSAJ "Segredo de Justiça" label may translate cleanly if DataJud returns nothing — or there may be a `nivelSigilo` field in the response we can use directly.

**Experiment 4 — Index aliases and undocumented endpoints.**
- Try `GET /api_publica_tjsp/_mapping` — if the API exposes the Elasticsearch mapping, we get the canonical field list directly from the source.
- Try `GET /api_publica_tjsp/_search?size=0&aggs={"distinct_fields": {"terms": {"field": "_field_names"}}}` — this works on raw Elasticsearch if the proxy doesn't restrict it.
- Try `POST /api_publica_tjsp/_search` with `_source_includes: ["partes*"]` — explicit field include patterns sometimes return data that wildcards don't.
- Document HTTP responses. We're looking for any indication that the "stripped" claim is wrong, partial, or version-dependent.

**Experiment 5 — Cross-tribunal field availability.**
Run a single representative sample query against 2-3 other tribunals (TRF1, TJRJ, TST) using their endpoints. Compare returned fields. The Portaria 160 stripping is national but the TPU and per-tribunal extras may differ. **If TJRJ returns `partes` and TJSP doesn't**, that's a major finding — the stripping might be configurable per tribunal.

For each experiment, the report section should be: hypothesis, what the docs/community say, what we tested, what came back, conclusion (confirmed / refuted / inconclusive). Save raw responses for everything.

### TPU table handling
The probe needs the TPU codes table to render `codigo` → `nome` mappings. Two options:
- Embed a small subset (the ~50 codes most relevant to execução cases — listed in `PROBE_FINDINGS.md` §1.4 plus the family of 11xxx penhora codes) in `tpu.py`.
- Download the full TPU table from CNJ on first run, cache locally. Size is small (<1 MB).

Probably do both: ship the embedded subset for offline operation, plus a `--refresh-tpu` flag to fetch the full table.

## Receita CNPJ probe

This is where the strategy matters. Don't reimplement the downloader.

### Receita strategy
The recommended path is Python-only — no Docker. The community reference for this is `rictom/cnpj-sqlite` (https://github.com/rictom/cnpj-sqlite), updated March 2026 to handle the post-Jan-2026 RFB layout migration. It's pure Python (pandas, dask, sqlalchemy), produces SQLite directly (which suits us since we're a SQLite shop), and handles the WebDAV/Nextcloud download flow that broke after RFB's January 2026 infrastructure change.

1. **For schema verification** (`receita schema-check`): no download required. Read the official `cnpj-metadados.pdf` schema description (vendored as a static text file under `poursuite/probes/cnpj_layout.txt`) and produce a report comparing it to what `PROBE_FINDINGS.md` §2.4 documents. This catches any drift we missed during research.

2. **For sample data** (`receita sample`): use `rictom/cnpj-sqlite` as the foundation. Two integration options, in order of preference:
   - **Vendor the relevant scripts** — copy `dados_cnpj_baixa.py` and `dados_cnpj_para_sqlite.py` (or their key functions) into `poursuite/probes/_rictom/` with attribution and license notice. Modify only what's needed to make them callable from our probe (config-driven paths, logging via `setup_logging`, sample-mode flags). This keeps us Python-only and gives us full control over the probe behavior.
   - **Pip install** if rictom publishes the package (check at probe-build time). If yes, depend on the released version. If no, fall back to vendoring.

   Either way, the probe's `receita sample` command should:
   - Download only the partition files needed for the sample: `Empresas0.zip`, `Estabelecimentos0.zip`, `Socios0.zip` (one of ten of each — ~10% of the dataset, plenty for sampling).
   - Skip the smaller reference files only if they're already cached locally.
   - Load into a fresh SQLite at `<POURSUITE_LOG_DIR>/probes/<ts>/cnpj_sample.db`.
   - Default to "latest available snapshot" — let rictom's downloader resolve which one that is.
   - Be **resumable**: if the download fails mid-way, the next run picks up. Receita's servers are flaky; this matters.

3. **Don't try to download all ~5 GB.** A 1-of-10 partition slice is sufficient for the probe's purpose (verifying schema, checking masking, eyeballing data quality). Full ingestion is a separate workstream.

4. **Disk space preflight.** Before downloading, check that there's at least 10 GB free at the target location. Bail loudly if not — same pattern as the orchestrator's preflight from the maintenance refactor.

### What to report
- **URL resolution**: what Nextcloud paths did the pipeline resolve to? (Helps us know if we need to update anything if URLs drift again.)
- **Schema verification**: for `empresas`, `socios`, `estabelecimentos`: do the columns match `PROBE_FINDINGS.md` §2.4 exactly? Any extras? Any missing? Any type surprises (e.g. is `capital_social` a number or a comma-decimal string)?
- **Row counts** per table in the sample.
- **CPF masking format**: pull 10 random PF sócios; report the exact mask pattern observed.
- **Address spot-check**: pull 100 random `estabelecimentos`, report:
  - % with `complemento` populated
  - % with valid 8-digit `cep`
  - 10 sample full addresses for visual eyeballing
- **Cross-table integrity**: pick 100 random `cnpj_basico` from `empresas`, count how many have at least one `estabelecimentos` row, how many have `socios` rows.
- **Encoding spot-check**: pull 20 razão social fields with characters like ç ã é, confirm UTF-8 clean (no `Ã§`, `Ã£`, etc.).
- **Active vs. inactive density**: count of `situacao_cadastral` per code in the sample.

### CNPJ alphanumeric readiness
Add an explicit check: scan a sample of CNPJs for any non-digit characters in positions 1-12. **Today this should be 0%** (the alphanumeric format is July 2026), but the probe should keep checking once that goes live, and flag the first appearance loudly. This makes the probe useful as a continuous-canary tool.

## Logging & errors

- Use `setup_logging("poursuite.probes")` from `poursuite/utils.py`.
- Write all raw API responses + raw downloaded slices to disk before parsing them, so we have artifacts when something looks weird.
- Catch and log network errors gracefully — a probe that crashes mid-run loses everything.
- The final report should include a "warnings" section at the top listing anything unexpected, even if it didn't fail.

## Definition of done

- `poursuite/probes/` module exists with the structure above
- `python -m poursuite.probes datajud` runs end-to-end against the 6 sample numbers and produces a Markdown report including the Challenge Experiments §
- `python -m poursuite.probes receita schema-check` runs (no download required) and produces a schema-diff report
- `python -m poursuite.probes receita sample` runs Python-only (no Docker), downloads + loads + reports on a one-partition slice
- `python -m poursuite.probes report` produces `<POURSUITE_LOG_DIR>/probes/latest_report.md` consolidating the most recent runs
- README.md in `poursuite/probes/` explains all of the above
- A short note at the end of the work summarizing what the runs actually revealed — i.e. **the answers to the empirical questions in PROBE_FINDINGS.md §1.7 and §2.8**, plus the conclusions of the Challenge Experiments.

## Out of scope

- Building the actual ingestion modules for either source (this is investigation, not feature work)
- Schema migrations, data model changes
- Touching `poursuite/api/`, `poursuite/db/`, `poursuite/scraper/`, `poursuite/cli.py`
- Any UI work
- Any LLM integration

## When done

Don't commit. Run the probe, capture the findings, and report back. We'll use the findings to update `PLAN.md` before deciding what to build next.
