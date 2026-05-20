# Brief for Claude Code — Query Builder UI: Implementation Phase (v2.2)

> v2.2 deltas vs. v2.1: normalized columns added (schema v3 → v4 split), `foro_name` promoted to a stored column, `saved_queries` added as the sixth endpoint group.

## Goal

Build the query builder UI based on the design pass (`docs/ui_design/WIREFRAMES.html`, `WORKFLOWS.md`, `UI_DESIGN_NOTES.md`), incorporating five patches from lawyer review.

This is the implementation pass. The wireframes are the visual reference; the workflows are the user journeys; this brief is the technical delta and the build specification.

## The five patches from lawyer review

These supersede the corresponding sections of the design artifacts where they conflict.

### Patch 1 — Carteira is an upload tag, not a workspace

The design pass over-committed to carteira-as-first-class. Lawyer feedback: portfolios are upload provenance, not analytical workspaces. Each case stands alone; the group is just "I uploaded these N processes together and want to reference them later."

**Storage:** a single JSON file at a configurable path (default: `<POURSUITE_SNAPSHOT_DIR>/process_groups.json`). The new env var `POURSUITE_SNAPSHOT_DIR` defaults to the same location where `esaj_snapshots.db` already lives (currently `POURSUITE_DB_DIR`). Naming it `SNAPSHOT_DIR` (not `DATA_DIR`) avoids conflating it with the DJE shard directory's 677 GB role. No database table, no migrations, no foreign keys. Shape:

```json
{
  "carteira_itau_2026_05": {
    "name": "Carteira Itaú · maio/2026",
    "created_at": "2026-05-16T14:32:00Z",
    "process_numbers": ["10180455020158260506", ...]
  },
  "<group_id>": { ... }
}
```

**Backend surface:** four endpoints under `/api/groups`:
- `GET /api/groups` — list all groups with name + created_at + count
- `GET /api/groups/{group_id}` — full group including process_numbers
- `POST /api/groups` — create a group (body: name, process_numbers)
- `DELETE /api/groups/{group_id}` — delete (with soft confirm in UI, no auth wall)

No editing endpoint for v1 — to modify a group, re-upload.

**Query integration:** scoping a query to a group is purely UI-side. The UI fetches the group's process_numbers and adds them as a clause to the query JSON (an `in` operator on `process_number`). The query API stays unchanged; it has no concept of groups.

**Concurrency:** file-level lock on writes. With 2-3 users, races are theoretically possible but practically near-zero. A simple file lock (or even a write-rename pattern) is sufficient.

**Wireframes affected:** Screens 2, 3, 5, 7, 9 use "carteira" terminology. Keep the terminology — it's the right user-facing word — but the implementation is lighter than the wireframes implied. Specifically: there's no "load carteira workspace" state; queries are just queries over the snapshot store, optionally pre-filtered by a process_number list.

### Patch 2 — Aggregates ship in this implementation pass (Phase 2.5)

Design pass flagged Screen 9 (Visões agregadas) as "API futura." Lawyer review: ship now.

**Three aggregate endpoints under `/api/aggregates`:**

- `POST /api/aggregates/group_by` — body: `{group_by: <field>, where?: <query>, snapshot?: "latest"|"any"|{at:ts}}`. Returns `[{value, count}, ...]` sorted by count desc. Supported `group_by` fields: `class_type`, `foro_code`, `foro_name`, `vara`, `juiz`, `distribution_year`, plus the time-bucketed virtual field `last_movement_bucket`. **`last_movement_bucket` source:** the new stored column `process_snapshot.last_movement_iso` (ISO 8601 date, see schema v4 below). Buckets via SQL `julianday('now') - julianday(last_movement_iso)`: `≤30d`, `30-90d`, `90-180d`, `180-365d`, `>365d`. The stored normalized column avoids per-query string-parsing of the raw `DD/MM/YYYY` `last_movement` field.

- `POST /api/aggregates/histogram` — body: `{field: "value", buckets?: <number or array>, where?, snapshot?}`. Returns `[{range_low, range_high, count}, ...]`. **Operates on `value_centavos`** (the new stored integer column, see schema v4 below) — not the raw `R$ 1.234,56` string. Default buckets: `[0, 10000, 50000, 100000, 500000, 1000000, 5000000]` (in BRL units; the endpoint multiplies by 100 internally to match `value_centavos`). The unbounded top bucket emits `{range_low: 5000000, range_high: null, count: N}` — `null` is the unbounded sentinel; the UI renders it as "R$ 5M+". Override via the `buckets` parameter (array of low-edges in BRL).

- `POST /api/aggregates/stats` — body: `{field: "value", where?, snapshot?}`. Returns `{count, sum, mean, median, min, max}` for a numeric field. `value` aliases to `value_centavos` internally; results are returned in BRL units (`value_centavos / 100`).

All three accept the same `where` clause shape as `POST /api/query`, the same `snapshot` modifier, **and the same top-level `flagged_only` / `unflagged_only` filters defined in Patch 4**. "Foro distribution among only flagged cases" is a natural lawyer question after triage; aggregates and queries must accept the same filter surface.

**Prerequisite refactor:** `poursuite/db/esaj_query.py:build_query` returns a full `BuiltQuery` with SELECT/ORDER/LIMIT — there's no primitive for "just the WHERE fragment + params + snapshot predicate." Before building the aggregate endpoints, extract a new function from `build_query` and have both `build_query` and the new aggregate code use it:

```
synth_where_only(body) -> (where_sql, params, joins)
```

- `where_sql`: the user's WHERE tree **already merged with the snapshot predicate** (callers don't need to know about snapshot semantics or how to combine them).
- `params`: bound parameters in the order `where_sql` consumes them.
- `joins`: list of `LEFT JOIN ...` fragments. **In v1 this is always an empty list** — it exists as the hook for future composability if `flagged` ever becomes a regular query field (would join `process_flags`). The aggregate code applies `flagged_only` / `unflagged_only` via a post-hoc `EXISTS (SELECT 1 FROM process_flags pf WHERE pf.process_number = ps.process_number)` (positive) or `NOT EXISTS (...)` (negative) AND'd onto `where_sql` — same as `build_query` will do.

**Drill-down behavior:** when the lawyer clicks a bar in Screen 9, the UI re-runs the equivalent query with an additional clause filtering to that group_by value, lands on Screen 5 (Resultados). No new endpoint needed.

**Performance ceiling:** for v1, aggregate queries scan the snapshot store fully (no precomputation). If a query consistently takes >2 seconds, surface that as a finding; don't auto-cache. We don't know yet which aggregates the lawyer actually runs frequently.

### Patch 3 — Visual builder is primary; "Modo avançado" is a JSON textarea

Design pass mocked Option 1 (visual tree) and Option 2 (text DSL) as parallel alternatives via a toggle. Lawyer review: visual builder is the default, advanced mode is a power-user fallback.

**Important scope clarification for v1:** "Modo avançado" is **not a custom DSL with a parser**. It's a **JSON textarea** showing the same query body that `POST /api/query` accepts. The lawyer pastes/edits JSON directly. No grammar, no parser, no round-trip compilation.

**Why this matters:** writing a real DSL with EBNF grammar and a bidirectional parser is significantly more work than a JSON textarea (multiple weeks vs. a few hours), and adds maintenance burden for a feature only power users touch. The wireframes' Option 2 examples (`class_type = "..." AND value >= 100000 AND movimentos.nome MATCH "..."`) showed the *idea* of a DSL but weren't a v1 commitment. v1 stays with JSON.

**UI changes from the wireframes:**
- Default screen state shows visual builder. No toggle visible by default.
- A small "Modo avançado (JSON)" link in the construtor toolbar reveals the JSON textarea.
- The visual builder and the JSON textarea stay in sync: edits in either mode update the same underlying query body. Switching from JSON back to visual: parse the JSON (`JSON.parse`), reconstruct the visual tree from the parsed object.
- **Sync trigger for the JSON textarea: on blur, validate and apply.** Continuous validation per keystroke is noisy; an explicit "Aplicar" button adds friction. On-blur is the pattern users expect. On focus, snapshot the current value so the lawyer can revert if a mid-edit change turns out wrong.
- If the JSON is malformed when switching back to visual, show "JSON inválido — corrija antes de voltar ao construtor visual." Don't try to recover.
- If the JSON is valid but contains shapes the visual builder doesn't handle (shouldn't happen if Patch 3's parity audit is complete), warn and offer to stay in JSON mode.

**Visual builder's capability requirement is unchanged:** every clause type the API accepts must be expressible visually. The parity audit in UI-2d enforces this. A true custom DSL is deferred to v2 — when there's evidence the lawyer is hitting JSON textarea regularly enough to justify the build.

### Patch 4 — Single-state flag (★)

Design pass mocked a ★ toggle per row. Lawyer review confirms single-state for v1: ★ or not ★. No multi-state (no "parked," no "rejected," no triage stages).

**Storage:** new table `process_flags(process_number TEXT PRIMARY KEY, flagged_at TIMESTAMP)`. Global namespace per design — anyone toggles, everyone sees. No author column.

**Endpoints under `/api/flags`:**
- `GET /api/flags` — list all flagged process_numbers
- `POST /api/flags/{process_number}` — flag (creates row if absent)
- `DELETE /api/flags/{process_number}` — unflag

**Flag filtering in queries:** rather than introducing `flagged` as a virtual field inside the query tree (which would require LEFT JOIN machinery `esaj_query.py` doesn't currently have), add `flagged_only: bool` and `unflagged_only: bool` as **top-level filters** on the query request body. Applied as a final WHERE-clause manipulation after the main tree synthesizes. The aggregate endpoints (`group_by`, `histogram`, `stats`) accept the same two top-level filters — see Patch 2.

Limitation: `flagged` cannot compose with `OR` or `NOT` clauses in v1. That's acceptable — the lawyer's use case is "show me only flagged" or "show me unflagged," not complex composition. If full composability becomes a real need, the proper LEFT JOIN refactor is additive (the `joins` slot in `synth_where_only` is reserved for it).

Add to the schema browser as a separate "Filtros globais" section, not as a field in the regular tables.

If your brother later wants multi-state, it's an additive change: rename `process_flags` to `process_labels`, add a `label TEXT` column, default to "starred." Single-state queries become `WHERE label = 'starred'`. Reversible.

### Patch 4.5 — Saved queries as a sixth endpoint group

Workflow 5 ("saving, browsing, and re-running queries") is in scope per the workflows doc but was missing from v2.1's endpoint list. Restoring it as the sixth endpoint group.

**Storage:** new table `saved_queries` added in the same schema-v3 migration as `process_flags`:

```sql
CREATE TABLE IF NOT EXISTS saved_queries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    description     TEXT,
    query_body      TEXT NOT NULL,          -- JSON-serialized POST /api/query body
    created_at      TEXT NOT NULL,
    last_run_at     TEXT,
    last_run_count  INTEGER
);
CREATE INDEX IF NOT EXISTS idx_saved_queries_last_run_at
    ON saved_queries(last_run_at DESC) WHERE last_run_at IS NOT NULL;
```

Global namespace per the brief's identity model — no author column. `query_body` stores the JSON the UI sends to `POST /api/query` verbatim (including `where`, `select`, `order_by`, `snapshot`, `flagged_only` / `unflagged_only`).

**Endpoints under `/api/saved_queries`:**
- `GET /api/saved_queries` — list all (name, description, created_at, last_run_at, last_run_count). No `query_body` in list view.
- `GET /api/saved_queries/{id}` — full record including `query_body`.
- `POST /api/saved_queries` — body: `{name, description?, query_body}`. Returns `{id, created_at}`.
- `PUT /api/saved_queries/{id}` — body: any of `{name?, description?, query_body?}`. Partial updates.
- `DELETE /api/saved_queries/{id}` — soft confirm in UI.
- `POST /api/saved_queries/{id}/touch` — body: `{result_count: int}`. Updates `last_run_at = now()` and `last_run_count`. UI calls this after a successful `POST /api/query` re-run from the library. Keeps `/api/query` itself stateless.

### Patch 5 — Update UI_DESIGN_NOTES.md in UI-2e

In sub-phase UI-2e (end-to-end smoke), update `docs/ui_design/UI_DESIGN_NOTES.md` with:
- A short addendum explaining the carteira reframe (Patch 1). Two paragraphs is enough — future readers should understand why portfolios are stored as JSON, not as a database table.
- The eight-question resolutions listed below in "What's not changing from the design pass."

Both updates happen at the same time in UI-2e. Don't make two separate doc-update passes.

---

## What's not changing from the design pass

- All ten screens stay (no screens dropped)
- Eight workflows stay
- Cross-cutting deep search affordance stays (and stays "Phase 3 — futuro")
- Global namespace for flags + saved queries stays
- HTTPS-only, sessionStorage-only, single shared API key stays
- PT-BR labels, BRL currency formatting, Foro/Vara/Juiz labels matching eSAJ stay
- The eight Code-surfaced questions in `UI_DESIGN_NOTES.md` are resolved as follows; update the doc to reflect:
  - Q1 — Visual builder primary, DSL as advanced (per Patch 3)
  - Q2 — Single-case deep dive is the more important workflow; bulk triage stays in scope but is not the dominant frame
  - Q3 — Carteira as upload tag (per Patch 1)
  - Q4 — Single-state flag (per Patch 4)
  - Q5 — Keep snapshot temporal modifier; low-traffic but high-value when needed
  - Q6 — Desktop primary; tablet should not break; mobile out of scope
  - Q7 — Aggregates ship in this pass (per Patch 2); a small set of useful aggregates is worth the cost
  - Q8 — Copy-JSON is enough for v1 saved-query sharing; URL-sharing deferred

## Architectural decisions

### Frontend stack

The current SPA at `GET /` is vanilla HTML/CSS/JS in a single file (per `ARCHITECTURE.md`). Continue the same approach — no framework. The wireframes are also vanilla. Reasons:
- Deployment is simpler (no build step)
- The UI is a single-purpose tool, not a general app
- Solo-shop scale doesn't earn React/Vue's complexity
- Lawyers update less frequently than a typical product team; fewer moving parts = less to maintain

If the codebase grows enough that vanilla starts hurting, that's a future refactor. For v1, keep it.

**File organization:**
- `poursuite/api/routes/frontend.py` already serves the existing SPA at `GET /`.
- **Decision: mount the new UI at `/`; move the legacy SPA to `/legacy`.** The legacy SPA was a parameter-form search for local single-user; the new UI does its job and more. Keep `/legacy` available for a transitional month, then delete the route and the embedded HTML/CSS/JS from `frontend.py`. The "flag during implementation" pattern that produced this question is exactly what shouldn't happen again — decide now.

### Backend additions

New routes in `poursuite/api/routes/`:
- `groups.py` — the four `/api/groups` endpoints
- `aggregates.py` — the three `/api/aggregates` endpoints
- `flags.py` — the three `/api/flags` endpoints
- `snapshot_status.py` — `POST /api/snapshot_status` (body: `{process_numbers: [...], max_age_days?: int}`; returns `[{process_number, status: "fresh"|"stale"|"missing", snapshot_ts, age_days}, ...]`). Used by Workflow 2 step 3 ("X have recent snapshot, Y need scraping"). The returned timestamp is `snapshot_ts` (the snapshot's canonical identifier and primary-key component on `process_snapshot`), not a separate `scraped_at` — they're equal at insert time, and `snapshot_ts` matches how snapshots are referenced everywhere else in the API. Freshness cutoff is `max_age_days` (default 7); `null` or omitted means "no age cutoff — every existing snapshot counts as fresh, missing ones remain missing." That handles the UI's "7 / 14 / 30 / never" toggle.
- `saved_queries.py` — the six `/api/saved_queries` endpoints (per Patch 4.5)
- `explain_zero.py` — `POST /api/query/explain_zero` (see "Zero-result explanation" below)

**Schema migrations.** Split across two versions for risk separation: v3 is additive (new tables only), v4 touches `process_snapshot` and requires a data backfill.

**Schema v3 — additive (low risk):** add two new tables.
- `process_flags(process_number TEXT PRIMARY KEY, flagged_at TEXT NOT NULL)` (Patch 4)
- `saved_queries(...)` (Patch 4.5 — full DDL above)

Both use `CREATE TABLE IF NOT EXISTS` for self-guard. Bump `CURRENT_SCHEMA_VERSION` from 2 to 3 in `_apply_migration` in `poursuite/db/esaj_snapshots.py`.

**Schema v4 — normalized columns on `process_snapshot` (requires backfill):**

```sql
ALTER TABLE process_snapshot ADD COLUMN foro_name          TEXT;
ALTER TABLE process_snapshot ADD COLUMN last_movement_iso  TEXT;   -- YYYY-MM-DD
ALTER TABLE process_snapshot ADD COLUMN value_centavos     INTEGER; -- BRL × 100, integer
CREATE INDEX IF NOT EXISTS idx_process_snapshot_foro_name
    ON process_snapshot(foro_name) WHERE foro_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_process_snapshot_last_movement_iso
    ON process_snapshot(last_movement_iso) WHERE last_movement_iso IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_process_snapshot_value_centavos
    ON process_snapshot(value_centavos) WHERE value_centavos IS NOT NULL;
```

Why: aggregates need to operate on real comparable types, not on raw eSAJ display strings (`DD/MM/YYYY`, `R$ N.NNN,NN`). Per `PHASE2_NOTES.md §3`, ordering/range ops were intentionally suppressed on these raw fields. The normalized columns fix the underlying problem rather than papering over it with per-query SQL gymnastics.

Bump `CURRENT_SCHEMA_VERSION` from 3 to 4.

**Scraper change (alongside v4 migration):** `poursuite/scraper/esaj.py` populates the three new fields on write:
- `foro_name` from `derive_from_cnj(process_number)["foro_name"]` — already computed, currently discarded.
- `last_movement_iso` from parsing the raw `DD/MM/YYYY` `last_movement` string; `None` if unparseable.
- `value_centavos` from parsing the raw `R$ N.NNN,NN` `value` string; `None` if unparseable or zero.

`ProcessData` gains these three optional fields. `HEADER_FIELDS` in `esaj_query.py` gains them too — they become queryable like any other column. `foro_name` joins the existing ops; `last_movement_iso` and `value_centavos` unlock the scalar/range ops the raw fields couldn't support.

**Backfill (one-shot, after v4 migration):** new script `scripts/backfill_normalized_columns.py`. Iterates every `(process_number, snapshot_ts)` row where the new columns are NULL, recomputes the three values from the existing raw fields + `derive_from_cnj`, and `UPDATE`s. Idempotent (re-running on a fully populated DB is a no-op). Runs against a configurable DB path; the operator runs it manually after deploying v4. Document in `RUNNING.md`.

New file (not in the DB):
- `process_groups.json` at `POURSUITE_SNAPSHOT_DIR/process_groups.json`. New env var `POURSUITE_SNAPSHOT_DIR` (default: same as `POURSUITE_DB_DIR`, which is where `esaj_snapshots.db` already lives).

### Cancellable queries

`UI_DESIGN_NOTES.md` mentions cancellable in-flight queries. **Implementation scope: UI-side fetch abort only.** When the lawyer clicks "Cancelar," the UI calls `AbortController.abort()` on the in-flight fetch and discards the response. The server continues running the query to completion — true server-side interruption would require out-of-band job state (like `/extract/start` uses) and isn't worth the complexity for queries that typically complete in <1 second.

Wasted server work is bounded: SQLite queries against the snapshot store are fast, and the cancel pattern matters mainly for the lawyer's UX (immediate "stop" feedback), not for server load. If query times grow to where wasted work matters, revisit then.

### Zero-result explanation

Workflow 4 step 11 mocks a "Por que zero?" card. Implementation: when a query returns zero results, run each clause of the top-level AND-tree in isolation and report which clause(s) returned non-zero.

**Recursion depth:** v1 decomposes the top-level AND **plus one level into `*_any` sub-clauses**. Lawyers' queries most commonly fail inside `movimento_any` blocks (FTS5 misses), so explaining only top-level clauses would miss the most common failure. Example: `{and: [a, b, {movimento_any: {and: [c, d]}}]}` decomposes into 4 standalone-tested clauses: `a`, `b`, `movimento_any: c`, `movimento_any: d`. Each runs as a standalone query body, returns its own count. The card highlights the clauses returning 0.

**Out of scope for v1:**
- Trees with top-level OR or NOT — the explanation card says "consulta complexa — explicação automática indisponível, tente simplificar."
- More than one level of `*_any` recursion (e.g., AND inside AND inside `movimento_any` — give up and show the simpler message).

New endpoint: `POST /api/query/explain_zero` — same body as `/api/query`. Returns `{clauses: [{clause: {...}, count_alone: N, decomposition_path: "and[2].movimento_any.and[0]"}, ...]}`. The `decomposition_path` helps the UI highlight the right node in the visual builder.

### Domain + TLS setup

This is the implementation work the design brief deferred.

**Decision: keep Cloudflare Tunnel.** The current cloudflared setup terminates TLS, hides the host IP, provides DDoS protection, and is already working. Point `poursuite.com.br` through Cloudflare DNS at the existing tunnel — no migration to Caddy or Let's Encrypt needed.

This reverses an earlier proposal in this brief (Caddy + Let's Encrypt direct). The earlier proposal was wrong: for a 2-3-user tool served from the operator's machine, exposing the host IP directly to the internet trades real privacy/security value for marginal operational simplicity. Cloudflare Tunnel is the right call.

**Deploy steps:**
1. Cloudflare DNS for `poursuite.com.br` → CNAME to the existing tunnel hostname
2. Cloudflare Access policy on the route, if desired (optional layer 2 — operator decides)
3. Confirm the existing tunnel routes to uvicorn on port 8000

The api/main.py docstring ("Cloudflare Tunnel handles TLS termination — no nginx or certificate management needed") stays accurate. `RUNNING.md` only needs a small addition: how to point the production domain at the tunnel.

## Sequencing within the implementation

Suggested sub-phases. Adjust if the dependencies warrant — the smoke testing pattern from Phase 2 should carry through.

### UI-2a — Backend additions

Build the new endpoints (groups, aggregates, flags, snapshot_status, saved_queries, explain_zero) and the schema migrations that back them. Each endpoint gets a small smoke test. The UI in 2c builds against these.

Order within 2a:
1. Schema v3 migration (`process_flags` + `saved_queries` tables) and the `_apply_migration` v3 branch.
2. Schema v4 migration (normalized columns on `process_snapshot`) + scraper change to populate them + `ProcessData` model update.
3. Backfill script (`scripts/backfill_normalized_columns.py`), run once locally against a copy of the live DB to validate.
4. `synth_where_only` refactor in `esaj_query.py` + expand `HEADER_FIELDS` with `foro_name`, `last_movement_iso`, `value_centavos`.
5. `config.py` — new `SNAPSHOT_DIR` env var.
6. The six new route modules + main.py wiring.

### UI-2b — Domain routing through Cloudflare Tunnel

Independent of the UI work. Can run in parallel with 2a. Validate by hitting `https://poursuite.com.br` and getting routed to the existing tunnel/uvicorn before the UI is even built. No new infrastructure; only DNS configuration.

### UI-2c — UI implementation

Build the 10 screens. Start with the construtor (Screen 4) — it's the headline. Use the wireframes as the visual reference; deviate where the implementation reveals better paths but document deviations.

### UI-2d — Visual-builder parity audit

Per Patch 3: audit every clause type, confirm the visual builder can express it. Fix gaps. This is a checkpoint before committing.

**Deliverable:** `docs/ui_design/BUILDER_PARITY.md` — a concrete checklist tied to the field constants in `esaj_query.py` (`HEADER_FIELDS`, `MOVIMENTO_FIELDS`, `LINKED_FIELDS`, `PETICAO_FIELDS`, `ALL_OPS`, plus the meta-keys `and`/`or`/`not`/`*_any`/`*_count`/`snapshot`/the new top-level flag filters). Each row of the checklist: field/op/meta-key, "expressible in visual builder? Y/N", "if N: why and what's the fix?" Audit complete = every row Y.

### UI-2e — End-to-end smoke + browser test

Walk through each workflow from the workflows doc, against the running deployment. Capture any inconsistencies between wireframes and reality. Update the artifacts if needed.

## What's NOT in this implementation

- Investigation evidence area (Paulo's first suggestion) — separate workstream, possibly Layer 5
- Case management module (Paulo's second suggestion) — out of scope; possibly never
- LLM analysis — Phase 4+, requires ground-truth labels first
- DataJud production module (Layer 3) — after this
- Receita / connection graph (Layer 4) — after this
- Deep search (Phase 3) — the affordance is shown in the UI but the backend doesn't ship in this pass
- Per-user accounts — single shared key only
- Mobile-first responsive design — desktop primary; tablet shouldn't break

## Constraints

- **Don't touch existing routes that work.** The current `/extract/start` flow stays. The new UI uses new endpoints. The legacy SPA moves from `/` to `/legacy` per the routing decision above.
- **Frontend stays vanilla** — no React, no Vue, no build step.
- **Use the existing auth middleware** (`X-API-Key`). Don't invent a new auth layer.
- **All new tables go through the schema migration pattern** established in Phase 2.
- **Reuse `esaj_query.py`'s WHERE synthesis** in the aggregate endpoints. Don't duplicate.
- **Surface unexpected findings as findings.** If during implementation you discover the wireframes pre-committed to something that doesn't work, raise it rather than working around it silently.

## Definition of done

- All six endpoint groups (groups, aggregates, flags, snapshot_status, saved_queries, explain_zero) implemented with smoke tests
- Schema v3 (process_flags, saved_queries) and v4 (normalized columns) migrations land cleanly on a fresh DB and on the existing live DB
- Scraper updated to populate `foro_name`, `last_movement_iso`, `value_centavos` on write
- Backfill script run against the live DB; spot-checks confirm normalized columns match expectations
- `synth_where_only` refactor in `esaj_query.py` complete and used by both `build_query` and the aggregate code
- Domain pointed at the existing Cloudflare Tunnel; `https://poursuite.com.br` serves the UI
- All 10 screens implemented and match the wireframes (with documented deviations where applicable)
- Visual-builder parity audit complete; gaps fixed
- End-to-end smoke walks every workflow successfully
- `RUNNING.md` updated with the deployment steps
- `UI_DESIGN_NOTES.md` updated with carteira reframe addendum (Patch 5) and the eight-question resolutions
- Estimated effort: 3-5 weeks. Frontend builds are harder to predict than backend phases; surface slippage early.

## When done

Don't commit to main until smoke is clean. Tag commits per sub-phase for clean revertability. Report back at each sub-phase, same pattern as Phase 2.
