# Phase 2 — notes captured during Phase 1

Two items the operator flagged during Phase 1 review that should be revisited when Phase 2 is briefed. Both are decisions to revisit before code is written, not blockers to capture now.

## 1. Reconsider the append-per-scrape snapshot model

**During Phase 1 design**, the snapshot semantics were set to "append a new (process_number, snapshot_ts) row on every scrape". That preserves history and enables diff-against-prior-scrape rules — both good properties.

**Operator concern surfaced after the decision:** at Poursuite scale, weekly re-scrapes balloon the movimento table fast.

- Per case: ~70 movimentos avg in operator data, up to 470 on busy cases. Call it 200 avg.
- Weekly scrapes for one year: 52 snapshots × 200 movs = 10,400 rows **per process**.
- 5,000 processes in a typical seller portfolio × 10,400 = ~52M movimento rows for one portfolio after a year of weekly scraping.
- Most snapshots will be identical to the prior one (movements don't change retroactively; new ones append at the end).

**Suggested alternative for Phase 2: scrape-then-diff.** Only insert a new snapshot when either:
- The header_json differs from the latest stored snapshot, OR
- The movimento set differs (new entries since last seen, or any change to existing entries)

Same audit capability (operator can ask "what changed between March and now"), much less storage. The diff check is a single SQL query against the latest snapshot per process.

**Decision point for Phase 2 brief:** which model wins? Append-every-time is simpler; scrape-then-diff is leaner. The lean version is probably right for production use, but worth the explicit choice rather than inheriting Phase 1's assumption.

## 2. Phase 2 scope inflation

The 1-2 week estimate for Phase 2 assumes **only movimentações timeline** is added to the snapshot store. That's the highest-value extraction (prescrição/citação/penhora rules depend on it).

But §5.1 of PLAN.md also lists **three other structural sections** the pre-filter rules want:

- **Petições diversas** — petition listings with metadata (procedural density signal). 7–63 rows per case in inventory.
- **Apensos / Incidentes** — formal linked-process numbers (embargos à execução, IDPJ incidents). 0–1 useful rows per case (most cases show "Não há"; one operator case had non-empty Apensos).
- **Histórico de classes** — conditional section showing procedural reclassification history. **Rare but high-signal** (1/13 cases in inventory, only the 2009 pre-2010 case).

Each of these adds: a new walker function, a new schema decision, new persistence, new tests. Roughly **1 week each** if done thoroughly, including snapshot-diff considerations.

**Decision point for Phase 2 brief:** scope explicitly. Three options the operator should pick from:

1. **Movimentações only** (1-2 weeks). Petições/Apensos/Histórico deferred to Phase 2.5.
2. **Movimentações + Apensos/Incidentes** (~2 weeks). These two are the highest-value sections after movimentações. Petições/Histórico deferred.
3. **All four sections** (~3-4 weeks). Complete §5.1 coverage in one workstream.

Option 1 ships fastest. Option 3 closes Layer 2 inputs in one cycle. Operator picks based on whether the pre-filter rule engine (the next workstream after Phase 2) can usefully consume movimentações-only data, or whether the additional sections unblock specific rules.

## 3. Tested-and-working from Phase 1 — carry-over

Notes from Phase 1 that Phase 2 should keep:

- `#maisDetalhes` is the correct selector for the header-expand "Mais" link. The probe walker also handles `#linkpartes` and `#linkmovimentacoes` for section-level collapsibles — when Phase 2 extracts movimentações, click `#linkmovimentacoes` to expose `tabelaTodasMovimentacoes` rather than only reading `tabelaUltimasMovimentacoes`.
- The viewport fix in `_chrome.py` (1920×1080 + desktop UA + `--disable-blink-features=AutomationControlled`) is what made the desktop layout reliably render. Phase 2 driver code should keep using `configure_chrome_options()` rather than building its own ChromeOptions.
- `derive_from_cnj` returns derived fields even for sealed cases. Phase 2's snapshot row should record those derived fields independently of whether the scrape succeeded — they come from the process number, not from the page.
- BS4's `find(class_=None)` excludes elements with class attributes (despite docs saying otherwise). The Phase 1 fix in `_extract_field` builds `find()` kwargs conditionally. Reuse that pattern when adding new selectors.

## 4. Findings from Phase 2c

### Inventory walker overcounted movimento rows by 6 per case

The inventory probe's per-section row count (e.g. `Movimentações: tables=1 (rows=72)` for case 1033164) is **overinclusive** by a consistent 6 rows per case. Root cause:

- The Movimentações section's `<table>` contains both `<tbody id="tabelaUltimasMovimentacoes">` (the "last 5 movs" subset, always rendered for visual flash-then-hide) AND `<tbody id="tabelaTodasMovimentacoes">` (the full timeline, exposed after `#linkmovimentacoes` is clicked). Both tbodies are in the same `<table>`.
- The inventory walker counts every `<tr>` with `<td>` cells inside the section, which double-counts the "last 5" rows (they appear in both tbodies — same content, two rows each) and includes 1 thead spacer row with empty `<td></td><td></td><td></td>` cells.
- Net overcount per case: **5 (last-5 duplicates) + 1 (thead spacer) = 6**.

**Production parser deduplicates by reading only `#tabelaTodasMovimentacoes`.** See `parse_movimentos` in [poursuite/scraper/movimentos.py](poursuite/scraper/movimentos.py). The parser's count is the canonical "real" movimento count.

**Implication:** when reading the May-10 inventory_report.md, treat Movimentações row counts as N+6. Petições / Apensos / Incidentes / Audiências overcount by **+1** per case from the same root cause: a `<tr class="fundoEscuro" height="2">` thead spacer with empty `<td>` cells that the inventory walker counts as a data row. Smaller magnitude than Movimentações (no second-tbody duplication) but same pattern — the production parsers (`parse_peticoes`, `parse_linked`) skip the thead and are canonical.

### Pagination remains unverified

All 16 cases tested in 2c had <500 movimentos and rendered in a single page. Whether eSAJ paginates movimentos at higher counts is **unproven**. The parser logs a WARNING via `is_full_timeline(soup) → False` when `#tabelaTodasMovimentacoes` isn't present after the expand click; this is the safety net. When production hits a case where this warning fires, treat it as a finding to investigate, not a bug to silence.

### Field-type catalog for the query builder UI

The eventual UI (PLAN.md v3 §6.3, next workstream after Phase 2) needs to decide which operators to offer per field. Storage is all TEXT/INTEGER in SQLite — semantic type is what matters for sane operator choice. Catalog below.

**Legend:**
- *numeric* — comparison ops (`<`, `<=`, `>`, `>=`) compare numerically. Storage is INTEGER.
- *ISO-comparable text* — TEXT, but values are in a fixed-width format where lexicographic ordering equals chronological/natural ordering. Comparison ops are safe.
- *broken comparable text* — TEXT in a format where lexicographic compare gives WRONG results. The UI should restrict to `=`, `!=`, `in`, `not_in` (and `is_null`); comparison ops should be hidden until the value is parsed/normalized.
- *identifier / enum / free text* — TEXT where comparison ops don't make semantic sense. `=`, `!=`, `in`, `not_in` only. `match` for free-text fields with FTS5 backing.

**process_snapshot (header fields, queried via top-level clauses):**

| Field | Type | Ops the UI should offer |
|---|---|---|
| `process_number` | identifier | `=`, `!=`, `in`, `not_in` |
| `snapshot_ts` | ISO-comparable text (ISO 8601 microsecond) | all scalar + `in`/`not_in` |
| `initial_date` | **broken** (raw `DD/MM/YYYY`) | `=`, `!=`, `in`, `not_in`, `is_null` only |
| `class_type` | enum-ish | `=`, `!=`, `in`, `not_in` |
| `subject` | free text | `=`, `!=`, `in`, `not_in` (FTS could be added later) |
| `value` | **broken** (raw `R$ N.NNN,NN`) | `=`, `!=`, `in`, `not_in`, `is_null` only; numeric compare requires normalization |
| `last_movement` | **broken** (raw `DD/MM/YYYY`) | `=`, `!=`, `in`, `not_in`, `is_null` only |
| `status` | enum | `=`, `!=`, `in`, `not_in`, `is_null` |
| `plaintiff` | free text | `=`, `!=`, `in`, `not_in` |
| `defendant` | free text | `=`, `!=`, `in`, `not_in` |
| `other_processes` | numeric | all scalar + `is_null` |
| `foro` | enum-ish (~600 values) | `=`, `!=`, `in`, `not_in` |
| `vara` | free text | `=`, `!=`, `in`, `not_in` |
| `juiz` | free text | `=`, `!=`, `in`, `not_in` |
| `controle` | identifier | `=`, `!=`, `in`, `not_in` |
| `outros_assuntos` | free text | `=`, `!=`, `in`, `not_in`, `is_null` |
| `outros_numeros` | identifier | `=`, `!=`, `in`, `not_in`, `is_null` |
| `local_fisico` | free text | `=`, `!=`, `in`, `not_in`, `is_null` |
| `area` | enum (low cardinality) | `=`, `!=`, `in`, `not_in` |
| `foro_code` | ISO-comparable text (fixed-width 4-digit) | all scalar + `in`/`not_in` |
| `tribunal_code` | ISO-comparable text (fixed-width 2-digit) | all scalar + `in`/`not_in` |
| `distribution_year` | ISO-comparable text (fixed-width 4-digit `YYYY`) | all scalar + `in`/`not_in` |
| `scrape_outcome` | enum (`loaded` / `sealed` / `error` / `not_found`) | `=`, `!=`, `in` |
| `scrape_error` | free text | `=`, `!=`, `in`, `is_null`, `is_not_null` |

**movimento (inside `movimento_any`):**

| Field | Type | Ops |
|---|---|---|
| `ordem` | numeric | all scalar |
| `data_hora` | ISO-comparable text **when parseable** | all scalar (parser normalizes `DD/MM/YYYY` → `YYYY-MM-DD`; unparseable values retain raw, which would break compare — uncommon but possible) |
| `codigo` | numeric (currently NULL — reserved for DataJud enrichment) | all scalar + `is_null` |
| `nome` | free text + **FTS5** | `=`, `!=`, `in`, `not_in`, **`match`** |
| `complementos_text` | free text + **FTS5** | `=`, `!=`, `in`, `not_in`, **`match`** |
| `cd_documento` | identifier | `=`, `!=`, `in`, `is_null`, `is_not_null` |

**linked_process (inside `linked_any`):**

| Field | Type | Ops |
|---|---|---|
| `linked_number` | identifier (CNJ format) | `=`, `!=`, `in`, `not_in` |
| `relationship_type` | enum (`apenso`, `incidente`) | `=`, `!=`, `in`, `not_in` |

**peticao (inside `peticao_any`):**

| Field | Type | Ops |
|---|---|---|
| `ordem` | numeric | all scalar |
| `data` | ISO-comparable text when parseable | all scalar |
| `tipo` | enum-ish | `=`, `!=`, `in`, `not_in` |
| `cd_documento` | identifier (currently always NULL) | `=`, `!=`, `in`, `is_null`, `is_not_null` |

**Aggregate sub-clauses (`movimento_count`, `linked_count`, `peticao_count`):**

- Operand type: **numeric**. Operators: `=`, `!=`, `<`, `<=`, `>`, `>=`. Value must be an integer.

**Followups the UI design might want to surface:**

- `initial_date`, `last_movement`, `peticao.data`, `movimento.data_hora`: production scraper stores these as raw `DD/MM/YYYY` in some paths and normalized `YYYY-MM-DD` in others (the movimentos parser normalizes; the production scraper doesn't normalize `initial_date` from the header). A consistent normalization pass during scrape would unlock comparison ops on all of them.
- `value`: storing as a parsed numeric (cents int? decimal?) would enable `value > 100000` rules. Currently the raw string makes this impossible without re-parsing per query. The CSV-export path also benefits from a normalized value column.
- These are storage-layer cleanups, not query-engine concerns. Queueing as a future workstream.

### `linked_process.relationship_type` granularity is section-level only

The brief listed candidate values `{apenso, incidente, dependente, embargos, ...}` mixing section names and content classifications. In 2d we settled on **section-level only**: `"apenso"` for any row in the Apensos/Entranhados/Unificados section, `"incidente"` for any row in the Incidentes/Recursos/Execuções section. Finer subtypes (entranhado vs unificado, recurso vs execução-de-sentença, embargos vs IDPJ) would require columnar data eSAJ doesn't expose — the Apensos table has columns for Classe of the linked process, Apensamento date, and Motivo, but no column for which of {apenso, entranhado, unificado} applies to *this* relationship. The Classe column tells us what the linked process IS, not what the relationship TO it is. Refinement is feasible later if a query rule wants finer types, but the linkage data needed isn't in eSAJ's current DOM.

### Decisions locked in v3 PLAN.md and shipped in 2a-2c

- Scrape-then-diff (won over append-every-time)
- All four sections in Phase 2 (movimentos shipped in 2c; linked + petições in 2d)
- Snapshot DB at `DB_DIR/esaj_snapshots.db`, separate from DJE corpus
- Hybrid `process_snapshot` schema: 21 promoted Phase-1 columns + `header_json` blob
- `movimento.codigo` is INTEGER NULL (eSAJ doesn't expose; reserved for DataJud enrichment)
- `/extract` keeps in-memory job dict for status polling; writes through to snapshot store for persistence
- `complementos_json` is NULL for 2c; complementos text-only is sufficient for FTS-based "movimento contains X" queries. Structured K:V parsing is a follow-up if a rule wants it.
