# Poursuite — User Workflows (Design Pass)

Each workflow is a numbered sequence of user actions, naming the screen the
user is on at each step. Screens are referenced by the names used in
`WIREFRAMES.html` (Screens 1–10). Mock screenshots / clickable mockups for
each workflow are in `WIREFRAMES.html` — open that file in a browser and
click the left-side nav to follow along.

The brief is explicit that none of these workflows has been declared the
dominant one. The lawyer reviewer should flag which ones he actually does
on a daily basis vs. which ones are nice-to-have. The artifact is
deliberately uncommitted so that scope can shrink after review.

---

## Workflow 1 — Login and orientation

> Every session starts here. The session ends when the browser tab closes
> (no `localStorage`; `sessionStorage` only).

1. Open `https://poursuite.com.br` in a browser.
2. **Screen 1 — Login.** Paste the shared API key into the password field.
   - If the page is served over HTTP, a red banner warns the user (the
     wireframe shows this banner enabled for design review purposes).
   - If the key has been rotated, the operator has previously sent the new
     value via the firm's normal channel; this is documented under
     "Sobre o acesso" on the login card.
3. Click **Entrar**. Land on **Screen 2 — Início**.
4. From Início, the user picks one of: open an existing carteira, start a
   new one, run a saved query, jump to the schema browser, or open the
   DJE search.

---

## Workflow 2 — Portfolio triage (the brief's Workflow A)

> Lawyer receives a CSV from a credit-seller and needs to find the
> juicy cases.

1. **Screen 2 — Início.** Click **+ Nova carteira**.
2. **Screen 3 — Carregar carteira (CSV).**
   - Name the carteira (e.g. "Carteira Itaú · maio/2026").
   - Drop in the CSV. The UI parses one process number per line (or a
     `numero_processo` column).
3. *(System step)* The UI checks the snapshot store for each number. The
   "Verificação de snapshots existentes" card shows how many already have a
   recent snapshot (configurable cutoff: 7 / 14 / 30 days / never) and how
   many need fresh scraping.
4. Lawyer adjusts the freshness cutoff if desired and clicks **Iniciar
   scraping**. Live progress card shows browser count, per-process status,
   and an ETA.
5. When scraping finishes (or in parallel — the carteira is queryable as
   soon as some snapshots are loaded), click **Compor consulta sobre esta
   carteira →**. Land on **Screen 4 — Construtor de consultas**.
6. **Screen 4.** Build the query. The carteira filter is implicit at the
   top of the screen (the construtor is scoped to that carteira's process
   numbers). See Workflow 4 for builder details.
7. Click **▶ Executar**. Land on **Screen 5 — Resultados**.
8. **Screen 5.** Sort, page through, and flag interesting rows (★).
   - The flag is shared state — anyone with the key can see and change it.
9. Refine the query: click **✎ Refinar consulta** → back to Screen 4 →
   adjust → Executar. Loop until the result set is what the lawyer wants.
10. Click **💾 Salvar** to save the query (Workflow 5) — optional.
11. Open **Campos exibidos / a exportar** on Screen 5 and pick the columns
    that the seller needs. Click **⬇ Exportar apenas marcados** to
    download a CSV of only the flagged rows, or **⬇ Exportar CSV** for all
    matches.

> *Backend status:* Steps 1–4 (CSV → carteira persistence + snapshot
> dedup decision) are **API futura**. Steps 6–8 (query + results +
> CSV-export of arbitrary fields) are **API existente** except for flag
> persistence which is future.

---

## Workflow 3 — DJE-driven discovery (the brief's Workflow B)

> Lawyer doesn't have a portfolio yet — he wants to *find* candidate
> processes by searching the 677 GB DJE corpus first.

1. **Screen 2 — Início** → click **Buscar no DJE**.
2. **Screen 10 — Busca DJE.** Compose FTS5 keywords (the existing search
   grammar). Optional date range, exclusion terms, page size.
3. Click **🔎 Buscar no DJE**. Results table lists process numbers and how
   many DJE mentions each has, with an indicator showing whether the
   process is already in the snapshot store and how stale that snapshot
   is.
4. Lawyer ticks the checkboxes for the processes he wants.
5. Two routes:
   - **Promover para scraping →** sends the selected numbers to **Screen 3
     — Carregar carteira**, pre-filled, where the user names a new
     carteira and follows Workflow 2 from step 3 onward.
   - **Tratar como consulta sobre o banco →** treats the selected set as
     an ad-hoc carteira and goes straight to **Screen 4 — Construtor**.
     Useful when all selected processes already have recent snapshots.
6. From Screen 5, the lawyer continues exactly as in Workflow 2 (flag,
   refine, export).

> *Backend status:* DJE search itself is **API existente**. "Promover para
> scraping" reuses the existing extract pipeline. The "already in the
> snapshot store?" annotation is **API futura** (small lookup endpoint).

---

## Workflow 4 — Composing and refining a query (the construtor itself)

> The headline screen. Optimized for the 50th query, not the 1st.
> Two builder approaches are mocked — Option 1 (visual tree) and
> Option 2 (text DSL). Lawyer picks one in review; the other is dropped.

1. **Screen 4 — Construtor de consultas.** Defaults: Option 1 (visual)
   active, snapshot modifier set to "Mais recente (latest)".
2. Lawyer can toggle to Option 2 (textarea DSL) using the toggle at the
   top of the construtor card. JSON preview updates from whichever side
   was last edited.
3. **Building a clause (Option 1):** click **+ Adicionar cláusula**. A new
   leaf appears: field-dropdown (populated from the schema browser),
   operator-dropdown (constrained by the field type — see Screen 8), and
   value input. Operators that the API would reject are *not* shown in
   the dropdown for that field.
4. **Nested logic (Option 1):** click **+ Subconjunto AND/OR** to add a
   nested boolean group. Click the connector chip on a group to flip it
   between AND/OR/NOT.
5. **Child-table sub-clauses (Option 1):** click **+ Existe movimentação**
   / **+ Existe processo vinculado** / **+ Existe petição** to add the
   corresponding `*_any` sub-clause. Inside, the field dropdown is
   constrained to that child table's columns (e.g. inside `movimento_any`
   you can pick `nome`, `complementos_text`, `data_hora`, `cd_documento`,
   `ordem`, `codigo`).
6. **FTS5 (Option 1):** when the user selects a field that supports
   `match` (movimento.nome / movimento.complementos_text), the operator
   dropdown offers "busca FTS5 (match)" and the value input accepts FTS5
   grammar (`"phrase"`, `OR`, `NEAR/n`, prefix `term*`). A help link
   beside the input points to the schema browser's FTS5 section.
7. **Counts (Option 1):** click **+ Contagem** to add a
   `movimento_count` / `linked_count` / `peticao_count` clause with a
   scalar operator and integer value (e.g. "movimento_count ≥ 10").
8. **Snapshot temporal (both options):** the top-right control selects
   `latest` (default), `any`, or `{"at": <date>}`. Choosing the "A partir
   de uma data…" entry reveals a date picker.
9. **JSON preview (both options):** the "Preview do JSON enviado para
   POST /api/query" card updates live as the lawyer edits. Lawyer can
   copy the JSON to the clipboard for sharing or for direct API use.
10. Click **▶ Executar**. Skeleton loaders appear in the results area
    (Screen 5) while the query runs. The button changes to **Cancelar**
    so a long-running query can be aborted in flight.
11. **Zero results → explanation.** If the query returns 0 results, the
    "Por que zero?" card appears (mocked open on Screen 4): each clause
    is re-run in isolation and the one that zeroed everything is
    highlighted. *(This is API futura — the explain endpoint doesn't
    exist yet; the brief flags it.)*
12. Lawyer iterates: edit clauses, re-run, refine. Each iteration the
    JSON preview reflects the current state. When happy, lawyer either
    proceeds to results (Workflow 2 step 8) or saves the query
    (Workflow 5).

> *Backend status:* the construtor is **API existente** (the entire JSON
> shape it produces is what `POST /api/query` accepts today). The
> "explain zero results" affordance is **API futura**.

---

## Workflow 5 — Saving, browsing, and re-running queries

> Queries accumulate. The library is shared across the firm.

1. **From Screen 4 (Construtor):** after a query produces useful results,
   the lawyer names it (textbox in the run/save bar) and clicks
   **💾 Salvar**. The query JSON + the human name + an optional
   description are stored server-side.
2. **From Screen 2 (Início):** the "Consultas salvas (recentes)" card
   lists the most-recently-run named queries. Clicking one re-runs it.
3. **From Screen 7 (Consultas salvas):** the full library.
   - Each row shows name, description, last-run timestamp, last-run
     result count.
   - **▶ Executar** re-runs and lands on Screen 5.
   - **✎ Editar** opens the query in Screen 4's construtor.
   - **📋 Copiar JSON** copies the saved JSON to clipboard (the v1
     sharing primitive — the brief defers URL-sharing to v2).
   - **🗑** asks for confirmation before deleting (shared library;
     anyone can affect anyone else).
4. The "Última execução" column reflects *anyone's* run — there is no
   per-user history because the auth model has no per-user identity.

> *Backend status:* **API futura** (saved-query store doesn't exist
> yet — the implementation brief written after this design pass
> defines the table + endpoints).

---

## Workflow 6 — Single-case deep dive (the brief's Workflow C)

> Lawyer has one specific CNJ number and wants to read the whole story.
> The brief flags this as potentially the dominant flow for an
> investigation-heavy practice — equally valued in this design.

1. **Entry points** (multiple — they all converge):
   - **Screen 2 (Início):** "Abrir processo específico" shortcut → enter
     the CNJ number.
   - **Screen 5 (Resultados):** click the process number in any row.
   - **Screen 9 (Agregados):** drill-down table → click the process number.
   - **Screen 10 (DJE):** click a process number directly (if already in
     the store).
2. **Screen 6 — Detalhe do processo.**
   - "Capa do processo" card: all header fields, plain layout, side by
     side.
   - "Movimentações" timeline: chronological list, newest first, with the
     `nome`, the `data_hora`, and the `complementos_text` for each entry.
     Filter box + date range filter at the top. Document-icon links on
     movimentos that carry a `cd_documento` (Phase 3 hook).
   - "Processos vinculados" table: each row links to that process's
     Screen 6 (recursive navigation).
   - "Petições diversas" table.
3. **Snapshot history.** "Snapshot de [date]" subtitle is a link → opens
   the list of all snapshots for this process (newest first). Clicking
   an older snapshot re-renders Screen 6 against that snapshot.
4. **Deep search this case (cross-cutting affordance).** The blue CTA
   strip at the top — and the **🔍 Deep** button on every results table
   throughout the app — fires a Phase 3 deep-search job for this process
   (ingest its PDFs, make them FTS5-searchable). Flagged as future; the
   button shows a tooltip and a "(Fase 3 — futuro)" badge.
5. Lawyer can navigate sideways into a linked process and treat that as
   the new Screen 6 anchor, or return to the carteira via the breadcrumb.

> *Backend status:* **API existente** for all read paths
> (`GET /api/process/{n}/snapshots`, `/movimentos`, `/links`,
> `/peticoes`). The "deep search this case" CTA is **Phase 3 — future**;
> the wireframe surfaces the affordance now so the lawyer can sanity-
> check where it appears.

---

## Workflow 7 — Cross-case patterns (the brief's Workflow E)

> Not "show me individual cases" — "show me the shape of the whole set".

1. **Entry:** from Screen 2 ("Agregados da carteira atual"), Screen 5
   ("📊 Ver agregados"), or the top nav ("Agregados").
2. **Screen 9 — Visões agregadas.**
   - Base selector: either the full carteira (847 in the mock) or the
     current query's result set (142 in the mock — Penhora SISBAJUD ≥
     R$ 100k).
   - Group-by selector: Foro / Vara / Classe / Juiz / Ano de distribuição
     / Última mov. (mês).
   - Three cards rendered:
     - Distribuição por Foro — horizontal bar chart with counts +
       percentages, click a bar to drill down.
     - Histograma de valores — buckets in BRL ranges, with median /
       mean / total below.
     - Última movimentação — buckets in time ranges (≤30d, 30–90d, …).
3. **Drill-down:** the bottom card mocks what happens when the lawyer
   clicks "Foro Central Cível" — the top-5 by value within that
   subgroup, with a button to "Ver todos os 624 como lista →" which
   feeds back into Screen 5 (Resultados) with the appropriate filter
   pre-applied.
4. The 🔍 Deep button on each drill-down row mirrors Workflow 6's deep-
   search affordance — consistency across contexts is a brief
   requirement.

> *Backend status:* **API futura.** The current API can do the COUNT
> via `count_only`, but per-group aggregates (foro distribution, value
> histogram buckets) need dedicated endpoints.

---

## Workflow 8 — Schema reference / what-can-I-do (the brief's Workflow F)

> While composing a query, the lawyer needs to know "is `outros_assuntos`
> populated for this kind of process?" or "what operator does
> `class_type` support?".

1. **Entry:** top nav → "Esquema". Can also be opened from a contextual
   "?" in the field dropdown of the construtor (future affordance —
   noted in design notes).
2. **Screen 8 — Esquema (dicionário).** Four tables, one per scope:
   - process_snapshot fields (the "header" — 22 fields).
   - movimento fields (timeline rows).
   - linked_process fields.
   - peticao fields.
3. Each row lists: API name (snake_case), UI display label (PT-BR),
   type, the list of **valid operators**, and example values. The
   "valid operators" column is the canonical reference for what the
   query builder dropdowns should permit.
4. A final card lists the meta-operators (and, or, not), the sub-clause
   keys (movimento_any, linked_any, peticao_any), the count keys, and
   the snapshot modifier syntax.
5. The lawyer skims, finds his answer, and returns to the construtor.
   (No interactive "what values does `class_type` actually take" yet —
   that would be a distinct-values endpoint, **API futura** — but the
   "Examples" column hints at the realistic value space.)

> *Backend status:* the static parts of the schema browser are derivable
> from `esaj_query.py` whitelists alone (no new endpoint needed). The
> distinct-values discovery on demand is **API futura**.
