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
