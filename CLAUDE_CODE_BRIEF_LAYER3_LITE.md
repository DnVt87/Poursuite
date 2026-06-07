# Brief for Claude Code — Layer 3-lite: DataJud Per-Process Enrichment

## Goal

Enrich the snapshot store with structured DataJud fields for process numbers we already hold, via batched `_msearch`. This is the viable Layer 3 path the re-inventory uncovered — it needs no party/name search. The headline payload is `complementosTabelados` (structured sentença outcomes — a success/failure signal without reading PDFs).

Fields to bring in (use `docs/DATAJUD_CAPABILITY_INVENTORY.md` as the schema source of truth):
- `movimentos.complementosTabelados` — the prize. Nested: movimento code + complemento code/value + names.
- `assuntos` — TPU subject codes + names.
- `grau` — instance level (G1 / G2 / JE / …).
- `orgaoJulgador.codigoMunicipioIBGE` — geographic pivot.
- `dataHoraUltimaAtualizacao` — DataJud freshness marker.

Explicitly **not** sourced from DataJud: `valor`. The re-inventory confirmed it's absent from the index (`exists` returns 0). eSAJ stays the value source. Do not add a DataJud value field.

Runs in parallel with the name-search reconciliation (separate brief). This build does not depend on that verdict — it's per-process enrichment keyed on process numbers, additive either way.

## Constraints from the re-inventory (load-bearing)

- **Batch via `_msearch`.** Per-process enrichment is N=1 round-trip per batch, not N requests. Map our process numbers to `numeroProcesso` queries inside one `_msearch` body.
- **Rate limit is ~0.25 req/sec single-threaded; parallel behavior untested (open Q2).** Design for low request count via batching. Do NOT build concurrent DataJud connections in v1 — sequential batched requests only until Q2 is probed separately.
- **Use `_source` includes** to fetch only the fields above (~90% payload reduction). Don't pull full documents.
- **`size:1000` works; `size:10000` has a sort-shape issue (open Q1).** Not on this critical path — per-process `_msearch` returns ~1 hit each. If a batch ever needs paging, cap at `size:1000`. Note but don't solve Q1 here.

## Storage

Decide and document: new related table vs. typed JSON column on the snapshot store.
- `complementosTabelados` and `assuntos` are arrays/nested — a flat column won't hold them cleanly. Lean toward a related table (e.g. `datajud_enrichment` keyed by process_number with snapshot linkage), or a JSON column only if no in-SQL filtering inside these is expected yet.
- State the query patterns you expect: will the lawyer filter on `complementosTabelados.codigo` in SQL? On `grau`? On IBGE code? Let that drive table-vs-JSON. The `complementosTabelados` success/failure axis is the most likely future query filter — design so it's **filterable**, not buried in opaque JSON.
- Additive v5 migration in `_apply_migration` (`poursuite/db/esaj_snapshots.py`): idempotent / self-guarding DDL, bump `CURRENT_SCHEMA_VERSION` to 5.
- Follow the append-on-change discipline the snapshot store already uses (Track A F2 backfill is the reference). Enrichment is a DataJud-sourced layer; don't overwrite eSAJ-sourced fields.

## Sub-phases (adjust if dependencies warrant)

**L3L-a — Confirm field shapes on real cases.** Pull 5-10 process numbers from `process_snapshot`, query DataJud, dump the exact nested shape of each target field. Confirm against the inventory doc. This drives the schema — don't write the migration before seeing the real shapes. (Inventory before extending.)

**L3L-b — Schema + migration v5.** Per Storage above.

**L3L-c — Enrichment fetcher.** Batched `_msearch`, `_source`-filtered, sequential batches. Per-process error isolation (a bad/missing process number doesn't abort the batch — follow the scraper's per-process error philosophy). This is production code now: lift reusable primitives out of `poursuite/probes/datajud.py` into the package rather than importing from `probes/`.

**L3L-d — Write-through + entry point.** Persist via the append-on-change path. Provide a way to run enrichment over a set of process numbers (a carteira / a query result). Mirror the existing `/extract/start` background-job pattern if a job fits, or a CLI subcommand — whichever matches how the operator runs scrapes today.

**L3L-e — Smoke test.** Run end-to-end over one real carteira (10-20 cases). Confirm `complementosTabelados` lands and is queryable in the form L3L-b chose. Report per-case DataJud hit/miss (some process numbers won't be in DataJud's index — that's a finding; capture the rate).

## What this is NOT

- Not name/party search — that's the parallel reconciliation brief.
- Not concurrent DataJud connections — sequential batched only until Q2 is probed.
- Not a DataJud value source — eSAJ keeps `valor`.
- Not large-result harvesting / `search_after` paging — per-process enrichment doesn't need it.
- Not a UI surface — exposing these fields in the query builder / schema browser is a follow-up once the data lands.

## Definition of done

- v5 migration applies cleanly; storage holds the five fields with `complementosTabelados` filterable.
- Enrichment fetcher runs batched `_msearch`, sequential, `_source`-filtered, per-process error isolation.
- End-to-end smoke over one real carteira; enrichment persisted via append-on-change.
- Per-case DataJud hit/miss rate reported as a finding.
- Estimated effort: 4-7 days.

## When done

Don't commit to main until smoke is clean. Tag per sub-phase for revertability. Report back at each sub-phase, same pattern as Phase 2. Capture the DataJud hit/miss rate and any field-shape surprises as findings.
