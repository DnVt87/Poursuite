# Poursuite — UI Design Notes (Design Pass)

> **Addendum — May 2026 (UI Phase 2.5 implementation).** Lawyer review resolved
> the eight Code-surfaced questions below and reframed the carteira concept.
> See section "8. Post-review addendum (UI Phase 2.5)" at the end of this
> document. The questions and their original framing are retained for
> historical context; the addendum is what code shipped against.

This document accompanies `WIREFRAMES.html` and `WORKFLOWS.md`. It captures:

1. The rationale behind the design choices that the wireframes embody.
2. Open questions for the lawyer reviewer — read these first.
3. The per-screen "designs against existing API" vs. "designs against
   not-yet-built API" tagging.
4. The PT-BR display-label mapping for every whitelisted API field.
5. What was deliberately scoped out of v1.
6. Implementation-brief carry-overs (domain/TLS, auth upgrades).

This is throw-away-able. After lawyer feedback, a separate implementation
brief will be written that picks one builder option, drops dead screens,
and scopes the backend additions.

---

## 1. Open questions for the lawyer reviewer

These are listed in priority order. Answer the first one before anything
else — it cuts the implementation work in half.

### Q1. **Option 1 (visual tree) or Option 2 (text DSL) for the query builder?**

The brief's most important open question. Both options appear in Screen 4
of `WIREFRAMES.html` — there's a toggle at the top of the construtor.

- **Option 1 — Visual tree.** Boxes with AND/OR connectors, dropdowns for
  field/operator/value, click-to-add-nested. Friendlier for the first 10
  queries; busier for the 50th. Implementation effort: substantial — every
  validation rule from `esaj_query.py` has to be mirrored in the UI
  (which operators each field allows, when `match` is offered, the
  recursive structure of `*_any` sub-clauses).
- **Option 2 — Text DSL.** A textarea. Server parses, returns results.
  Requires a new server-side parser (not large — the grammar is tiny —
  but new code). Easier for power users once learned; needs a syntax
  reference card always-visible. Could co-exist with the JSON preview
  (the JSON is itself a kind of text mode, just less ergonomic).
- **Hybrid.** Option 1 as primary, with the JSON preview always editable.
  Power users can edit the JSON directly; novices use the tree.

Reviewer: pick one. If Option 2, we skip a *lot* of UI work and the
construtor screen becomes very small.

### Q2. **Is Workflow 6 (single-case deep dive) the dominant workflow?**

The brief speculates that for an investigation-heavy practice, single-case
work may dominate over bulk-triage. If true, Screen 6 (case detail) should
be the most polished screen and Screen 5 (results table) is just a
list-of-links to it. If bulk-triage dominates instead, the construtor +
results pair (Screens 4–5) is the centre of gravity.

Implications:
- If single-case dominant → invest in movimentações filtering, snapshot
  history comparison ("what changed between 2026-04 and 2026-05?"), and
  faster Phase 3 deep-search hookup.
- If bulk-triage dominant → invest in faster query iteration, better
  saved-query management, and CSV export polish.

### Q3. **Carteira is the right unit, or is global query better?**

The wireframes show queries scoped to a carteira (top of Screen 4 says
"Carteira: Itaú · maio/2026 · 847 processos"). The actual API doesn't
have a carteira concept — `process_snapshot` is one global table. The
carteira scope would be implemented as an implicit
`process_number IN (the carteira's list)` clause.

Reviewer: do you think in terms of carteiras, or do you want every query
to be over the global snapshot store with carteira-membership as just
another optional filter?

### Q4. **Flag/star — what does it actually represent?**

The wireframes show ★ as a single-state "interesting / not" toggle. Real
triage often wants three states: starred ("call the seller"), parked
("look later"), rejected ("not worth chasing"). Should we have multiple
flag states, or stick with one and let saved-queries express
"interesting"?

### Q5. **Snapshot history — is the "view as of [date]" actually used?**

The query API supports `snapshot: {"at": "<ISO>"}` and the wireframes
expose this on Screen 4. Is there a real workflow where the lawyer asks
"how did this carteira look on March 15th?" — or is it always "latest"?
If always latest, the "snapshot temporal" control should be hidden behind
an "advanced" toggle.

### Q6. **Mobile / tablet — does it matter?**

The wireframes are desktop-first. The login screen and Screen 2 (Início)
are usable on tablet; Screens 4 (construtor) and 5 (resultados) are
desktop-only as drawn. Brief says "should at least not break on tablet";
this isn't met today.

Reviewer: do you ever use this on tablet? On a phone? If never, we skip
all mobile work.

### Q7. **Aggregate views — useful or theatre?**

Screen 9 (Foro distribution, value histogram, last-movement
distribution). These are easy to build but might not be what a lawyer
actually wants. Concrete question: when you receive a 1000-line CSV,
do you look at *distributions* before drilling into individual cases,
or do you go straight to filtering?

### Q8. **Saved-query sharing — is "copy JSON" enough for v1?**

The wireframes show a 📋 Copiar JSON button on Screen 7. The brief defers
URL-sharing to v2. Is copy-paste actually adequate, or do you need the
queries to have URLs so you can send "go look at this" to a partner?

---

## 2. Per-screen API-backing status

| # | Screen | Status | Notes |
|---|--------|--------|-------|
| 1 | Login | **Existente** | `X-API-Key` mechanism unchanged. Only UI added: HTTPS-warn banner. |
| 2 | Início / Carteiras | **Futura** | Needs "list carteiras" and "list saved queries" endpoints. |
| 3 | Carregar carteira (CSV) | **Futura** | Needs carteira persistence + CSV ingest endpoint + "is recent snapshot?" lookup. The scraping pipeline itself exists (`/extract/start` + polling). |
| 4 | Construtor de consultas (visual) | **Existente** | The entire JSON shape it produces is what `POST /api/query` accepts today. |
| 4 | Construtor de consultas (texto) | **Futura** | Needs a server-side parser that translates the text DSL to the same JSON. |
| 4 | "Explicação de zero resultados" | **Futura** | Needs an endpoint that re-runs each leaf in isolation and reports counts. |
| 5 | Resultados | **Existente** | `POST /api/query` returns the rows. CSV export of arbitrary field selection extends the existing `/search/export` pattern. |
| 5 | Flag/star per row | **Futura** | Needs a global `flag` table and toggle endpoints. |
| 6 | Detalhe do processo (header, movs, linked, petições) | **Existente** | `GET /api/process/{n}/snapshots,movimentos,links,peticoes` all exist. |
| 6 | "Deep search this case" CTA | **Futura — Fase 3** | Brief explicitly defers; we surface the affordance for review only. |
| 7 | Consultas salvas (biblioteca) | **Futura** | Needs saved-query CRUD endpoints. |
| 8 | Esquema (dicionário) | **Mostly existente** | The whitelists in `poursuite/db/esaj_query.py` are the source of truth; the screen can be built as a static rendering of them. Distinct-values discovery ("what does `class_type` actually contain in this carteira?") is **Futura**. |
| 9 | Visões agregadas | **Futura** | Needs aggregate endpoints (group-by Foro/Vara/etc., bucket histograms). |
| 10 | Busca DJE | **Existente** | Re-uses `GET /search` + `/search/export`. "Already in snapshot store?" annotation is **Futura** (small lookup). |

---

## 3. PT-BR display-label mapping (canonical)

The UI displays PT-BR labels matching eSAJ. The API uses snake_case. This
table is the source of truth — every appearance of a field in the UI
should use the Display column.

### `process_snapshot` (header)

| API field | Display (PT-BR) | Notes |
|---|---|---|
| `process_number` | Processo (CNJ) | Always rendered in monospace. |
| `snapshot_ts` | Timestamp do snapshot | ISO 8601 UTC; usually shown as "DD/MM/AAAA HH:MM". |
| `initial_date` | Distribuído em | eSAJ: "Data da distribuição". |
| `class_type` | Classe | eSAJ: "Classe". |
| `subject` | Assunto | eSAJ: "Assunto principal". |
| `value` | Valor da ação | BRL formatting: `R$ 1.234,56`. |
| `last_movement` | Última movimentação | Date, no time. |
| `status` | Situação | eSAJ: "Situação". |
| `plaintiff` | Autor / Exequente | eSAJ uses both depending on class. Display the polo activo label as "Autor / Exequente". |
| `defendant` | Réu / Executado | Symmetric to plaintiff. |
| `other_processes` | Outros processos do réu | Integer count. |
| `foro` | Foro | eSAJ: "Foro". |
| `vara` | Vara | eSAJ: "Vara". |
| `juiz` | Juiz(a) | eSAJ: "Juiz". Use "Juiz(a)" in the UI to be neutral. |
| `controle` | Controle | eSAJ: "Controle". |
| `outros_assuntos` | Outros assuntos | eSAJ: "Outros assuntos". |
| `outros_numeros` | Outros números | eSAJ: "Outros números". |
| `local_fisico` | Local físico | eSAJ: "Local físico". |
| `area` | Área | eSAJ: "Área". |
| `foro_code` | Foro (cód.) | Derived from CNJ. |
| `tribunal_code` | Tribunal (cód.) | Derived from CNJ. |
| `distribution_year` | Ano de distribuição | Derived from CNJ. |
| `scrape_outcome` | Resultado do scrape | Internal; usually only shown in carteira-status screens. |
| `scrape_error` | Erro do scrape | Internal. |

### `movimento`

| API field | Display | Notes |
|---|---|---|
| `ordem` | Ordem | Integer; eSAJ orders newest-first by default. |
| `data_hora` | Data/hora | Mostly date-only in eSAJ. |
| `codigo` | Código TPU | Usually NULL — eSAJ doesn't expose it. |
| `nome` | Movimentação | eSAJ: column label in the timeline. FTS5-eligible. |
| `complementos_text` | Complementos | Long free text. FTS5-eligible. |
| `cd_documento` | Documento (eSAJ) | When present, renders as 📄 link with the doc ID. |

### `linked_process`

| API field | Display | Notes |
|---|---|---|
| `linked_number` | Nº vinculado | CNJ, monospace. |
| `relationship_type` | Tipo de relação | Enum: `apenso`, `incidente`. Display as "Apenso" / "Incidente". |

### `peticao`

| API field | Display | Notes |
|---|---|---|
| `ordem` | Ordem | Integer. |
| `data` | Data | Date, no time. |
| `tipo` | Tipo | Free text. |
| `cd_documento` | Documento (eSAJ) | Usually NULL today; reserved per the dataclass docstring. |

### Operators & modifiers (Q4 schema browser card)

| Symbol / key | PT-BR label | Notes |
|---|---|---|
| `=`, `!=`, `<`, `<=`, `>`, `>=` | "igual a", "diferente de", "menor que", "menor ou igual", "maior que", "maior ou igual" | UI may show the math symbols too. |
| `in`, `not_in` | "está em", "não está em" | Requires a non-empty list. |
| `is_null`, `is_not_null` | "é nulo", "não é nulo" | No value. |
| `match` | "busca FTS5" | Only on `movimento.nome` and `movimento.complementos_text`. |
| `and`, `or`, `not` | "E", "OU", "NÃO" | Composition. |
| `movimento_any`, `linked_any`, `peticao_any` | "Existe movimentação", "Existe processo vinculado", "Existe petição" | EXISTS-style sub-clauses. |
| `movimento_count`, `linked_count`, `peticao_count` | "Quantidade de movimentações", "Quantidade de vinculados", "Quantidade de petições" | Integer comparison. |
| `snapshot: "latest"` | "Mais recente" | Default. |
| `snapshot: "any"` | "Qualquer snapshot" | All snapshots match. |
| `snapshot: {"at": "<ISO>"}` | "A partir de [data]" | Latest snapshot at or before the given timestamp. |

---

## 4. Design rationale (the choices the wireframes make)

### a. Two-tab → fully-routed app

The current SPA (`poursuite/api/routes/frontend.py`) is one page with two
tabs. The new design has ten distinct screens. Reason: a query builder
with saved queries, a portfolio store, and per-case drill-downs has too
much state to live in tab-switches. The sidebar nav is the cheapest way
to communicate "this is a real tool now, not a one-pager."

### b. Carteira as a first-class concept

The brief's Workflow A talks about "a CSV of process numbers" as if it's
a one-shot input. In practice, sellers send carteiras with names ("Itaú
maio 2026"), the lawyer wants to come back to them later, and queries
are scoped to them. Hence Screen 2's "Carteiras recentes" card and the
"compor consulta sobre esta carteira" framing on Screen 4. See Q3 above
— this might be the wrong abstraction.

### c. Side-by-side construtor + JSON preview

The brief requires the JSON preview. It's not just a debug aid; it's the
canonical contract. By keeping it always visible, the lawyer always
knows what the API will actually receive — and a power user who prefers
the JSON can edit it directly. This is also the answer to Q1's "hybrid"
sub-option.

### d. Explain-zero-results card on the construtor

The brief asks for an "explain" affordance. It's mocked-up open on
Screen 4 even when there are no zero-result results, so the reviewer
can see the proposed shape. In production it appears only when the run
returns zero.

### e. Cross-cutting "deep search this case"

The brief specifies this affordance appears in multiple contexts. It's
present on Screen 6 (top CTA strip — primary surface) and inline as a
"🔍 Deep" button on every results table (Screens 5, 9). Identical
styling and identical tooltip wherever it appears.

### f. PT-BR everywhere; eSAJ labels are the source

Field labels in the construtor dropdowns, the schema browser, the case
detail view, and the CSV export field selector all use PT-BR labels
sourced from eSAJ's actual UI. snake_case API names appear *next to*
the PT-BR label in the schema browser, and in the JSON preview — never
as primary copy.

### g. BRL currency formatting

R$ 1.234,56 throughout. Existing `format_currency` in the scraper
already produces this format on the way in.

### h. sessionStorage (not localStorage)

Per brief. The wireframes call this out explicitly on the login screen
("A chave é guardada em sessionStorage e expira ao fechar o navegador.").

### i. Single key → shared workspace mental model

Per the brief's "Identity and shared state" section. Three concrete
choices:
- No "minhas" vs. "do time" tabs in the saved queries list.
- No author column anywhere.
- Soft confirms on destructive operations (deleting a saved query).
- The "Modelo mental" footer hint on Screen 2 makes this explicit.

### j. Skeleton loaders, not spinners

Per brief — for the 200–1000ms cross-internet queries. Screen 5 shows
skeleton rows at the bottom of the results table while data streams in.

### k. Stale indicator

Per brief — Screen 5 header bar shows "Resultados de 47 segundos atrás"
in a warning colour. After 5 minutes it would flip to "⚠ Stale — refresh?"
(mocked at 47s here so the reviewer can see the affordance).

### l. Cancellable execução

Per brief — "Cancelar execução" button next to "▶ Executar" on the
construtor. Active when a query is in flight.

---

## 5. Deliberately scoped out of v1

Listed so the reviewer doesn't have to wonder "did they forget?":

- **AI-assist / auto-suggest.** Brief is explicit: don't add it.
- **Per-user accounts.** Scale is 2–3 lawyers sharing a key. Future, not
  needed now.
- **URL sharing of saved queries.** v2. Copy-JSON is the v1 primitive.
- **Realtime collaboration / presence indicators.** Not at this scale.
- **Full-text search of `complementos_text` from the construtor's value
  field as a regular search box.** Workflow F's schema browser tells you
  what's possible; doing it via natural-language autocomplete is feature
  creep.
- **Phase 3 deep search.** The CTA exists in the UI but does nothing
  (yet). The wireframe shows it for layout / placement review.
- **Mobile/phone breakpoints.** Tablet "shouldn't break"; mobile is not
  a goal. See Q6.
- **Per-snapshot diffing on Screen 6.** "What changed between snapshots
  4 and 3?" — would be nice for monitoring but adds an entire visual
  diff component. Defer.
- **Distinct-values discovery in the schema browser.** "Show me every
  value `class_type` takes in this carteira" needs an endpoint; v2.
- **Excel-style filter chips on the results table.** The construtor is
  the canonical place to refine; results-page filtering would create
  two query truths.
- **Light/dark mode toggle.** Defer until anyone asks.
- **Keyboard shortcuts.** Likely useful for the 50th-query mindset, but
  not part of design pass. Note for implementation.

---

## 6. Carry-over to the implementation brief

These belong to the next document, not this one — flagged here so they
aren't lost.

### Domain / TLS
- `poursuite.com.br` DNS A or CNAME record pointing at the host.
- TLS certificate via Let's Encrypt + certbot, or Caddy as reverse
  proxy with automatic TLS.
- HSTS header (long max-age once stable; staged shorter first).
- The existing cloudflared tunnel can remain as a backup path, or be
  retired once the direct domain is stable.

### Auth upgrades surfaced in the design
- **Key rotation.** Documented on Screen 1's "Sobre o acesso" footer.
  Operator rotates via `POURSUITE_API_KEY` env var; communicates new
  key out-of-band; sessions in flight must re-login.
- **HTTPS-only.** Hard requirement. Login page warns if served over
  HTTP (visible in mock). Server should also redirect HTTP → HTTPS at
  the reverse-proxy layer.
- **No localStorage.** sessionStorage only. Mocked language on login.

### Backend additions needed if all wireframes are kept
Roughly, in implementation-effort order, smallest first:
1. "Is process N in the snapshot store, and how stale?" — single GET.
2. Distinct snapshot list per process — already exists.
3. Saved-query CRUD — new table, four endpoints.
4. Carteira CRUD + CSV ingest + scrape-queue hookup — new table, a
   handful of endpoints, integration with existing extract pipeline.
5. Flag/star CRUD — new table, two endpoints.
6. Aggregate endpoints (group-by, histogram buckets) — new endpoints;
   the SQL is straightforward but the API shape needs design.
7. Explain-zero-results — re-runs each leaf clause in isolation.
8. Option 2 text DSL parser (if Q1 picks Option 2 or hybrid).

The implementation brief written after lawyer review will scope and
sequence these against the lawyer's actual priorities.

---

## 8. Post-review addendum (UI Phase 2.5)

After the design pass, lawyer review (May 2026) resolved every open question
and reframed the carteira concept. This addendum is the authoritative record
of what shipped; when this section disagrees with sections 1–6 above, this
section wins.

### 8.1 Carteira reframe — JSON file, not a database table

The design pass treated carteiras as analytical workspaces — a first-class
concept the user "loads" before querying. Lawyer feedback corrected this:
**a carteira is upload provenance, not a workspace.** The seller hands over
a CSV, the lawyer wants to remember which numbers came together so he can
re-reference them later. Each case stands alone analytically; the group
exists only to label "I uploaded these N together."

What this changed in the implementation:

- **Storage is a single JSON file at `POURSUITE_SNAPSHOT_DIR/process_groups.json`,
  not a database table.** No schema, no migrations, no foreign keys. Per-process
  threading lock plus write-rename atomicity is sufficient for the 2-3-user
  scale. Implementation lives in `poursuite/db/process_groups.py`.
- **The query API has no concept of groups.** Scoping a query to a carteira
  is purely a UI operation: the construtor fetches the group's
  `process_numbers` and injects them as an `IN` clause on `process_number` at
  the top of the visual tree.
- **No "load carteira workspace" state.** The construtor screen is always
  global; the carteira filter is just one clause among many. Removing the
  carteira clause from the visual builder is the same gesture as removing
  any other filter.
- **The user-facing word "carteira" stays** — it's the right shorthand for
  Brazilian portfolio review work. The implementation just doesn't earn the
  weight the wireframes suggested.

This decision is reversible. If a richer carteira concept earns its weight
later (e.g. carteira-level annotations, multi-carteira diff views), promoting
the JSON file to a real table is straightforward — the schema is trivial and
the read paths already centralize through one module.

### 8.2 Resolutions of the eight Code-surfaced questions

The questions in §1 were resolved as follows during lawyer review:

- **Q1 — Visual builder primary; "Modo avançado" is a JSON textarea, not a
  custom DSL.** A real DSL with grammar + parser was scoped out as
  multi-week work for a power-user feature. The JSON view is enough for the
  small fraction of queries the visual builder can't yet express by clicks
  (`select`, `order_by` — see [BUILDER_PARITY.md](BUILDER_PARITY.md)).
- **Q2 — Single-case deep dive is the more important workflow.** Bulk
  triage stays fully supported, but Screen 6 (Detalhe do processo) gets the
  polish budget. The construtor + Resultados pair is the workhorse, not the
  headline.
- **Q3 — Carteira as upload tag (§8.1 above).**
- **Q4 — Single-state flag (★/not-★) for v1.** No "parked", no "rejected".
  Stored in `process_flags` (schema v3). Multi-state is the additive path
  if a real need emerges: rename to `process_labels`, add a `label` column,
  default existing rows to `'starred'`.
- **Q5 — Keep the snapshot temporal modifier.** Low-traffic but high-value;
  hiding it behind an "advanced" toggle would surprise the lawyer when he
  actually needs it. The construtor controls bar exposes `latest` / `any` /
  `at: <date>`.
- **Q6 — Desktop primary; tablet shouldn't break; mobile out of scope.**
  The SPA layout uses CSS grids that collapse on narrow viewports; not
  optimized for touch.
- **Q7 — Aggregates ship in this pass.** A useful trio: `group_by`,
  `histogram`, `stats` — all backed by the snapshot store via
  `synth_where_only`. Drilldowns from group_by bars land back on the
  construtor.
- **Q8 — Copy-JSON is the v1 saved-query sharing primitive.** Real
  URL-sharing deferred. The saved-query record carries the full
  `query_body` JSON, so copy-paste between team members works today via
  📋 in the library.

### 8.3 What schema v3 + v4 actually added

The implementation split the schema migration across two versions for risk
separation:

- **v3 — additive** (new tables only): `process_flags` and `saved_queries`.
  Self-guarded with `CREATE TABLE IF NOT EXISTS`.
- **v4 — column adds on `process_snapshot`** (needs a backfill):
  `foro_name`, `last_movement_iso`, `value_centavos`. Indexed. The
  scraper populates them on write going forward; existing rows are
  backfilled by `scripts/backfill_normalized_columns.py`.

The v4 normalized columns are what makes aggregates and ordering meaningful
on `last_movement` and `value` — the raw eSAJ display strings can't be
compared sensibly.

### 8.4 Filed under "do later" after v1 use

- Promote `select` and `order_by` to first-class UI (currently JSON-only).
- Per-snapshot diff view on Detalhe (would let the lawyer answer "what
  changed since last week?").
- Distinct-values discovery on the schema browser ("show me every
  `class_type` value present in this carteira").
- Server-side query cancellation (today the UI aborts the fetch; the server
  keeps running — fine at current query times).
- A real custom DSL (Q1) if JSON-textarea usage becomes regular.

### 8.5 Layer 3-lite enrichment surfacing (enrichment-ui)

DataJud per-process enrichment (schema v5: `datajud_enrichment` +
`datajud_complemento`) is exposed in the query API (`enrichment` /
`complemento_any` / `complemento_count`), the schema browser (enrichment fields +
a complemento catalog), and the case detail. Two v1 limitations are deliberate:

- **The query universe stays snapshot-based.** Enrichment is free-standing
  (keyed by `process_number`, no FK to `process_snapshot`), but the builder still
  selects from `process_snapshot`. Enrichment augments those rows via EXISTS — it
  filters and displays, but a process that is enriched yet has **no snapshot does
  not appear** in query results. Acceptable: the lawyer works from
  carteiras/snapshots. An "enrichment-only universe" is a future question.
- **Enrichment filters match only enriched processes.** `grau` /
  `complemento_any` / `complemento_count` are EXISTS against the process's
  **current** enrichment, so a process with no current enrichment fails the
  predicate — e.g. `grau != "G1"` excludes un-enriched processes (it is *not*
  "anything that isn't G1"). Compose with `not` at the clause boundary for "not
  enriched".

Also v1: `assuntos` / `codigo_municipio_ibge` / `dataHoraUltimaAtualizacao` are
display-only (Detalhe), not filters; no curated favorable/unfavorable
interpretation of complemento tuples (that mapping is harvested from the lawyer
via the catalog); enrichment runs from the CLI (`python -m poursuite.datajud`),
no UI trigger yet. DataJud complementos render on their **own** movement timeline
in Detalhe (their `movimento_indice`/`data_hora` reference DataJud's movement
array, a distinct source from the eSAJ movimentos), not merged into the eSAJ one.

---

## 9. How to review

1. Open `WIREFRAMES.html` in a desktop browser.
2. Click "Entrar" on the login screen. The sidebar on the left lists all
   10 screens; click any of them.
3. Most cards and buttons within the screens are also clickable and
   navigate to the screen they'd lead to in production.
4. Read this document alongside, especially Section 1 (Open questions).
5. Mark answers / objections / additions on the questions in Section 1
   and send them back to the operator. The implementation brief will
   be written from your feedback.
