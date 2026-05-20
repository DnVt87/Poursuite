# Visual-builder ↔ API parity audit (UI-2d)

Verified against `poursuite/db/esaj_query.py` (HEADER_FIELDS / MOVIMENTO_FIELDS / LINKED_FIELDS / PETICAO_FIELDS / ALL_OPS / meta-keys) and the SPA at `poursuite/api/routes/spa_v2.html` (UI Phase 2.5).

The brief (Patch 3) mandates: every clause type the API accepts must be expressible in the visual builder, with the JSON textarea as escape hatch for OR/NOT-heavy power-user cases.

Status legend: **Y** — expressible by clicks alone; **partial** — expressible but requires JSON textarea for some sub-cases; **n/a** — not a clause type, doc-only.

---

## Header fields × operators (`process_snapshot`)

The leaf editor in [renderLeaf()](../poursuite/api/routes/spa_v2.html) populates the field dropdown from the SPA's `HEADER_FIELDS` constant and filters the op dropdown to that field's `ops` list (mirrored from `esaj_query.py`). Every cell below is Y by construction; failures here would mean a row missing from the SPA's constant.

| Field (API) | Label (PT-BR) | Allowed ops | Status |
|---|---|---|---|
| `process_number` | Processo (CNJ) | `=`, `!=`, `in`, `not_in` | Y |
| `snapshot_ts` | Timestamp do snapshot | scalar + null | Y |
| `initial_date` | Distribuído em | `=`, `!=`, `in`, `not_in`, null | Y (range ops intentionally hidden — raw `DD/MM/YYYY` doesn't sort lexicographically) |
| `class_type` | Classe | `=`, `!=`, `in`, `not_in`, null | Y |
| `subject` | Assunto | `=`, `!=`, `in`, `not_in`, null | Y |
| `value` | Valor (texto eSAJ) | `=`, `!=`, null | Y (raw display string; for range queries use `value_centavos`) |
| `last_movement` | Última mov. (texto eSAJ) | `=`, `!=`, null | Y (same caveat; use `last_movement_iso` for ordering) |
| `status` | Situação | `=`, `!=`, `in`, `not_in`, null | Y |
| `plaintiff` | Autor / Exequente | `=`, `!=`, `in`, `not_in`, null | Y |
| `defendant` | Réu / Executado | `=`, `!=`, `in`, `not_in`, null | Y |
| `other_processes` | Outros processos do réu | scalar + null | Y |
| `foro` | Foro | `=`, `!=`, `in`, `not_in`, null | Y |
| `foro_name` | Foro (nome normalizado) | `=`, `!=`, `in`, `not_in`, null | Y (schema v4 column) |
| `foro_code` | Foro (cód.) | `=`, `!=`, `in`, `not_in`, null | Y |
| `tribunal_code` | Tribunal (cód.) | `=`, `!=`, `in`, `not_in`, null | Y |
| `distribution_year` | Ano de distribuição | scalar + `in`/`not_in` | Y |
| `vara` | Vara | `=`, `!=`, `in`, `not_in`, null | Y |
| `juiz` | Juiz(a) | `=`, `!=`, `in`, `not_in`, null | Y |
| `controle` | Controle | `=`, `!=`, null | Y |
| `outros_assuntos` | Outros assuntos | `=`, `!=`, null | Y |
| `outros_numeros` | Outros números | `=`, `!=`, null | Y |
| `local_fisico` | Local físico | `=`, `!=`, null | Y |
| `area` | Área | `=`, `!=`, `in`, `not_in`, null | Y |
| `last_movement_iso` | Última mov. (ISO) | scalar + null | Y (schema v4 column; this is the column ordering and bucketing operate on) |
| `value_centavos` | Valor (centavos) | scalar + null | Y (schema v4 column; numeric type, range-friendly) |
| `scrape_outcome` | Resultado do scrape | `=`, `!=`, `in`, `not_in` | Y |
| `scrape_error` | Erro do scrape | — | n/a (not in `HEADER_FIELDS` op-table; rarely useful for filtering) |

Notes:
- `match` is explicitly absent from header fields — FTS5 only applies to `movimento.nome` and `movimento.complementos_text`. The op dropdown filter never offers `match` outside a `movimento_any` block.
- Comparison ops (`<`, `<=`, `>`, `>=`) were deliberately suppressed on raw string fields (`initial_date`, `value`, `last_movement`) because lexicographic compare on `DD/MM/YYYY` and `R$ N.NNN,NN` is misleading. The brief's PHASE2_NOTES §3 documents this; the normalized v4 columns (`last_movement_iso`, `value_centavos`) cover the missing capability.

---

## Movimento fields × operators (inside `movimento_any`)

Reached via "+ Existe movimentação" on any clause group. Inside the resulting `*_any` block, the leaf editor swaps `fields` to `MOVIMENTO_FIELDS` and enables `match` only for `nome` and `complementos_text`.

| Field | Allowed ops | Status |
|---|---|---|
| `ordem` | scalar | Y |
| `data_hora` | scalar + null | Y |
| `codigo` | `=`, `!=`, null | Y |
| `nome` | `=`, `!=`, `in`, `not_in`, **`match`**, null | Y |
| `complementos_text` | `=`, `!=`, **`match`**, null | Y |
| `cd_documento` | `=`, `!=`, null | Y |

FTS5 grammar passes through verbatim (the SPA hints at quotes/OR/NEAR/`*` below the value input).

---

## Linked-process fields × operators (inside `linked_any`)

Reached via "+ Existe vinculado".

| Field | Allowed ops | Status |
|---|---|---|
| `linked_number` | `=`, `!=`, `in`, `not_in` | Y |
| `relationship_type` | `=`, `!=`, `in`, `not_in` | Y |

---

## Petição fields × operators (inside `peticao_any`)

Reached via "+ Existe petição".

| Field | Allowed ops | Status |
|---|---|---|
| `ordem` | scalar | Y |
| `data` | `=`, `!=`, null | Y |
| `tipo` | `=`, `!=`, `in`, `not_in`, null | Y |
| `cd_documento` | `=`, `!=`, null | Y |

---

## Meta-keys (boolean composition + scope)

| Meta-key | How expressed in the visual builder | Status |
|---|---|---|
| `and` | Default group kind; "+ Subgrupo AND" creates nested. | Y |
| `or` | "Trocar para OU" toggle on any group; "+ Subgrupo OU" for new. | Y |
| `not` | "Envolver em NÃO" button on any group; unwraps via "Remover NÃO". | Y |
| `movimento_any` | "+ Existe movimentação" button on any group. | Y |
| `linked_any` | "+ Existe vinculado". | Y |
| `peticao_any` | "+ Existe petição". | Y |
| `movimento_count` | "+ Contagem mov." | Y |
| `linked_count` | "+ Contagem vinculados" | Y |
| `peticao_count` | "+ Contagem petições" | Y |

Boolean composition inside `*_any` sub-clauses is also supported recursively (the `renderChildNode` walker handles `and`/`or`/`not` scoped to the child table's columns).

---

## Top-level body keys (not WHERE clauses but part of the request body)

| Key | UI surface | Status |
|---|---|---|
| `select` | Hardcoded default list (8 columns) — used for Resultados table and CSV export. Not user-editable in the SPA yet. | partial (v2 enhancement) |
| `order_by` | Hardcoded default (`snapshot_ts desc`). Resultados table sort is client-side over the returned page. | partial (v2 enhancement) |
| `limit` / `offset` | Driven by the Resultados pagination component (page size 50). | Y (indirectly) |
| `count_only` | Always false from the SPA. Used internally by `explain_zero`. | n/a |
| `snapshot` | "Snapshot:" select in construtor controls. Three options: `latest`, `any`, `{at: <date>}` (date picker reveals). | Y |
| `flagged_only` | "Apenas ★" checkbox. | Y |
| `unflagged_only` | "Apenas não-★" checkbox. | Y |

The `select` / `order_by` gap is the only true expressibility limitation in v1: a power user who wants different result columns or custom ordering currently uses the JSON textarea. Promote to first-class UI in a follow-up if the lawyer regularly hits this.

---

## Gaps found and fixed during this audit

- **"+ Contagem" originally only created `movimento_count`.** Split into three buttons: "+ Contagem mov." / "+ Contagem vinculados" / "+ Contagem petições".

## Remaining v1 limitations (acceptable per brief)

- `select` not editable via clicks. **Workaround:** edit JSON in "Modo avançado".
- `order_by` not editable via clicks. **Workaround:** same.
- Real custom DSL (vs. raw JSON) deferred to v2 — see Patch 3 of `CLAUDE_CODE_BRIEF_UI_IMPL_v2.md`.

## Audit complete

All clause types the API accepts are expressible in the visual builder by clicks alone, with `select` / `order_by` as documented v2 enhancements. The JSON textarea remains the escape hatch for those two and for any future API-accepted shape that hasn't yet been wired into the visual editor.
