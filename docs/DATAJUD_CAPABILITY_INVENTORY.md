# DataJud Public API — Capability Inventory

_Canonical empirical reference. Last verified June 2026 against `api-publica.datajud.cnj.jus.br` (name-search reconciliation added; verdict N1)._

Replaces patchwork understanding from earlier probes (May 2026 datajud probe, May 2026 layer3 probe). This document is the foundation for any Layer 3 design decisions; when downstream docs disagree with it, the empirical results here are authoritative.

Probe code: [`poursuite/probes/datajud_inventory.py`](../poursuite/probes/datajud_inventory.py). Raw artifacts (every JSON response, every query payload) live under `<POURSUITE_LOG_DIR>/probes/datajud_inventory_<ts>/`.

---

## 1. Headline findings

**Name search via `partes.nome` does not work on the public API — verdict N1, real and reconciled** (see [§2a Name search](#2a-name-search--the-reconciliation) for the full reconciliation). The party fields are absent from the index mapping, proven by the decisive test: `_field_caps` (accessible, HTTP 200) returns all 5 control fields but **zero** of 7 party fields across all tribunals, so the endpoint works for our key and no party field exists in the schema. The official **CNJ DataJud Glossário de Dados** independently documents exactly the 14 fields we observe, with **no `partes` field**. LegalSuite's documented `match.partes.nome` example — same public endpoint, same APIKey scheme — does **not** contradict this: it is an untested marketing-blog example that conflates the free CNJ API with LegalSuite's own enriched product. (Earlier framing in this doc proposed "tiered access most charitable" — that was the un-reconciled placeholder; the actual reconciliation is documented below.)

**Several capabilities the prior probes missed are real and unlock material Layer 3 options.** Aggregations work (terms / date_histogram / cardinality / nested-on-movimentos all return buckets). `complementosTabelados` — the structured success/failure-axis signal called out as the brief's highest-leverage unknown — is present and populated in 10/10 sampled cases AND queryable as a flat match. Bulk patterns (`_msearch`, `terms[]`, multi-index URL) all work. `search_after` pagination works with `_doc`, `dataAjuizamento`, and `@timestamp` sort variants. `_source` filtering reduces payload by up to 90.9%. TPU movement codes are uniform across the 5 tested tribunals for our 3 sample codes (refresh of the prior "not uniform" finding — sample is narrow but worth a refresh).

---

## 2a. Name search — the reconciliation

**Verdict: N1 — the public-API name-search negative is real AND reconciled with LegalSuite.** This section exists because project methodology rule #7 forbids canonizing a confident negative that contradicts observable commercial usage without reconciling it. The earlier rounds confirmed the negative with query evidence (all `partes.nome` queries return 0 hits) but never proved *field absence* — a `match` on an unmapped Elasticsearch field returns `200 / 0 hits`, not an error, so "0 hits" alone can't distinguish "field absent" from "field present but empty/our-name-not-there."

### The decisive test: `_field_caps` with a control

`GET /_mapping` is **403** for the public key (`user dpj_api_publica` lacks `view_index_metadata`) — so the mapping is not directly readable, and an empty mapping response is permission-denial, not proof. But `GET /_field_caps` **is** accessible (200). Requesting control fields + party fields in one call, per tribunal:

- **Control fields returned (5/5):** `numeroProcesso`, `classe` (+`classe.codigo`), `dataAjuizamento`, `movimentos` (+`movimentos.codigo`), `grau` → the endpoint works for our key.
- **Party fields returned (0/7):** `partes`, `partes.*`, `partes.nome`, `partes.nomeParte`, `nomeParte`, `nome`, `cpfCnpj` → none in the mapping.

Elasticsearch omits unmapped fields from `_field_caps`. Control-present + party-absent is airtight: the party fields **do not exist in the index mapping**. Identical across all 5 tribunals.

### Corroborating evidence

- **Official CNJ schema.** The [DataJud Glossário de Dados](https://datajud-wiki.cnj.jus.br/api-publica/glossario/) documents exactly our 14 observed fields (`id, tribunal, numeroProcesso, dataAjuizamento, grau, nivelSigilo, formato, sistema, classe, assuntos, orgaoJulgador, movimentos, dataHoraUltimaAtualizacao, @timestamp`) — **no `partes` field**. The API-pública wiki notes data is published with "o resguardo de processos sigilosos e **dados de partes**" (protection of party data).
- **Field-name sweep.** 9 candidate party paths (`partes.nome`, `partes.pessoa.nome`, `poloPassivo.parte.pessoa.nome`, `dadosBasicos.polo.parte.pessoa.nome`, etc.) all return 0 hits on the canary `"Itaú Unibanco"` — a name guaranteed to appear thousands of times if any party field were searchable.
- **Real-defendant matrix.** Royal Coffee (PJ) + Jean Cássio Luna Santos (PF), exact strings from `process_snapshot`, 4 variants each → all 0.

### Why LegalSuite's documented example does not contradict this

LegalSuite's blog ("DataJud: A API Pública Gratuita do CNJ — Como Usar") shows a `match.partes.nome` curl against `https://api-publica.datajud.cnj.jus.br/api_publica_tjsp/_search` with `Authorization: APIKey …` — **the same public endpoint and auth scheme we use.** We ran their exact published query verbatim: 200, **0 hits**. The reconciliation is **not** "different tier with a magic key" (the earlier placeholder) — it's that the blog is **untested marketing copy that conflates the free CNJ API with LegalSuite's own enriched product** (the fetch confirms it "does not clearly separate free DataJud capabilities from LegalSuite's proprietary features"). The party data LegalSuite actually serves ("Lista de partes com nome e tipo (ativo/passivo/outros)") is their **own enrichment layer** — party data they source themselves (per-tribunal portals / a private feed), the same architecture as our Layer 3-tribal path — not the public Elasticsearch index. Temporal removal ("CNJ indexed parties then pulled them for LGPD") is possible but less likely: the current official glossário lists 14 fields with no `partes` and no deprecation note.

**Implication:** Layer 3-proper (cross-tribunal debtor name search via DataJud alone) stays infeasible. LegalSuite is, in effect, already doing Layer 3-tribal — which validates that architecture as the realistic path to cross-tribunal party discovery if/when the operator wants it. Layer 3-lite (process-number-keyed enrichment) is unaffected.

Full evidence chain + raw artifacts: `<POURSUITE_LOG_DIR>/probes/datajud_inventory_20260607T221500Z/reconciliation_verdict.md`.

---

## 2. Field-by-field capability matrix

For each documented or tested field, whether it appears in `_source`, populated rate on a 10-case sample (4 from snapshot store + 6 from prior probe samples, mix of years 1990–2024 and forums), and whether it's queryable. Tested against TJSP unless noted.

| Field | In `_source` | Populated 10/10 | Filterable | Aggregatable | Notes |
|---|---|---|---|---|---|
| `numeroProcesso` | ✅ | 10/10 | ✅ (`match`) | ✅ (`.keyword` for cardinality) | The reliable join key |
| `id` | ✅ | 10/10 | — | — | Text field; sorting on it errors (fielddata disabled) |
| `tribunal` | ✅ | 10/10 | ✅ | ✅ | Single value per index ("TJSP" etc.) |
| `classe.codigo` + `classe.nome` | ✅ | 10/10 | ✅ | ✅ (terms agg works) | TPU class code |
| `assuntos` (array of `{codigo, nome}`) | ✅ | 10/10 | ✅ (`assuntos.codigo`) | ✅ | 5081 hits on `7724` (Defesa do Consumidor) |
| `grau` (G1/G2/JE) | ✅ | 10/10 | ✅ | ✅ | Instance-level filter |
| `dataAjuizamento` | ✅ | 10/10 | ✅ (range works) | ✅ (date_histogram works) | ISO-ish or compact format depending on case; both filterable |
| `dataHoraUltimaAtualizacao` | ✅ | 10/10 | ✅ | ✅ | Snapshot timestamp |
| `@timestamp` | ✅ | 10/10 | ✅ | — | ES ingestion timestamp; usable for `search_after` sort |
| `sistema` (`{codigo, nome}`) | ✅ | 10/10 | ✅ | — | "Projudi", "SAJ", etc. |
| `formato` (`{codigo, nome}`) | ✅ | 10/10 | ✅ | — | "Eletrônico" or "Físico" |
| `nivelSigilo` | ✅ | 10/10 | ✅ | — | Numeric secrecy level (0 = public) |
| `orgaoJulgador.codigo` + `.nome` | ✅ | 10/10 | ✅ | ✅ (terms agg works) | Vara-level granularity |
| `orgaoJulgador.codigoMunicipioIBGE` | conditional (0/10 in our sample, 10000+ hits exist globally) | depends | ✅ (returns 10000+ hits for 3550308 = São Paulo) | likely ✅ | **Geographic pivot via IBGE municipality codes**. Absent in our snapshot-store sample (older / specific subset?), but the index has it at scale |
| `movimentos` (array) | ✅ | 10/10 | ✅ (flat queries; NOT nested-typed) | ✅ (nested-typed aggregation works on inner fields) | Object array, not ES-nested type |
| `movimentos.codigo` + `.nome` | ✅ | 10/10 | ✅ | ✅ | TPU movement codes |
| `movimentos.dataHora` | ✅ | 10/10 | ✅ (range works, returned 10000+ hits for 2024 range) | — | Per-movement timestamp |
| **`movimentos.complementosTabelados`** | ✅ | **10/10** | **✅ flat match** (`movimentos.complementosTabelados.codigo` returns 10000+ hits on value `3`) | likely ✅ | **HIGHEST-LEVERAGE FINDING.** Structured sub-codes attached to movements. Shape: array of `{codigo, valor, nome, descricao}`. Lets us filter on sentença-outcome / distribuição-type / etc. without parsing free-text |
| **`partes`** | **❌ absent across all 5 tribunals** | — | ❌ | ❌ | Verified 4 ways: schema check, fields/docvalue bypass, nested-query type error, canary "Itaú Unibanco" → 0 hits |
| **`partes.nome`** | ❌ | — | ❌ (absent from mapping — `_field_caps` control test; 0 hits across all variants × 5 tribunals × canary) | ❌ | Not in the index mapping nor the official CNJ glossário. See [§2a](#2a-name-search--the-reconciliation) for the LegalSuite reconciliation |
| `cpfCnpj` / `numeroDocumentoPrincipal` / `documento.numero` / `partes.cpfCnpj` | ❌ | — | ❌ | ❌ | 0 hits across all field × tribunal cells; consistent with LegalSuite LGPD framing |
| **`valor`** | **❌ absent from index** (TJSP) | 0/10 in sample, 0 hits on `exists` query across entire TJSP | ❌ | ❌ | DataJud TJSP simply does not surface the monetary value. eSAJ scraper's `valor` capture (Phase 2) is **irreplaceable** — DataJud cannot back-fill it |

Color of cells:
- ✅ = empirically works
- ❌ = empirically doesn't work
- "conditional" = field IS in the index but missing from our sample

---

## 3. Per-tribunal capability matrix

Tested 5 indices: TJSP, TJRJ, TJMG, TRT-2, TRF-3 (the v1.2 Phase 3 / Layer 3 brief's set; STJ + TST excluded as appellate-inflating per prior analysis). All 5 indices live, all 5 return 200 + ≥10000 hits on `match_all`.

| Capability | TJSP | TJRJ | TJMG | TRT-2 | TRF-3 |
|---|---|---|---|---|---|
| Index responds (`match_all`) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Identical `_source` shape (14 keys) | ✅ | ✅ | ✅ | ✅ | ✅ |
| `partes` field present | ❌ | ❌ | ❌ | ❌ | ❌ |
| `partes.nome` canary (`"Itaú Unibanco"`) | 0 hits | 0 hits | 0 hits | 0 hits | 0 hits |
| CPF/CNPJ searchable | ❌ | ❌ | ❌ | ❌ | ❌ |
| TPU codes 22 / 26 / 85 produce matching top-1 nomes | ✅ uniform | ✅ uniform | ✅ uniform | ✅ uniform | ✅ uniform |

**The `_source` schema is uniform across all 5 tribunals.** No tribunal-specific party exposure exists in our test. TPU code naming was uniform across the 3 codes we tested (a refresh of the May 2026 "not uniform" finding — possibly stale; 3-code sample is narrow but worth flagging).

---

## 4. Operational capabilities & limits

### 4.1 Aggregations — work

| Aggregation | Status | Buckets returned |
|---|---|---|
| `terms` on `classe.codigo` | ✅ 200 | 10 buckets |
| `date_histogram` on `dataAjuizamento` (year interval) | ✅ 200 | Multi-year buckets |
| `terms` on `grau` | ✅ 200 | Multi-bucket |
| `terms` on `orgaoJulgador.codigo` | ✅ 200 | Multi-bucket |
| `cardinality` on `numeroProcesso.keyword` (distinct count) | ✅ 200 | Returns a `value` |
| Nested `terms` aggregation on `movimentos.codigo` (with `nested:{path:"movimentos"}`) | ✅ 200 | Multi-bucket |

**Layer 3 implication:** server-side counting / grouping is fully supported. Layer 3 can ask "count cases by foro for these filters" in one query instead of fetching hits client-side and bucketing.

### 4.2 Bulk / multi-process patterns — all work

| Pattern | Status | Notes |
|---|---|---|
| `POST /_msearch` (NDJSON, multiple subqueries) | ✅ 200 | 3 subqueries → 3 subresponses, each with 1 hit. Native multi-query batching. |
| `terms` query with array of `numeroProcesso` in a single search | ✅ 200 | 3 process numbers in one query → 3 hits |
| Multi-index URL: `/api_publica_tjsp,api_publica_tjrj/_search` | ✅ 200 | Cross-tribunal one-shot search supported |

**Layer 3 implication:** batch enrichment of N process numbers is one request, not N. Cross-tribunal queries don't need N tribunal calls.

### 4.3 `search_after` deep pagination — works

Tested three sort variants:

| Sort | Page 1 hits | Page 2 hits via `search_after` |
|---|---|---|
| `[{"_doc": "asc"}]` | 1000 | 100 |
| `[{"dataAjuizamento": "desc"}]` | 1000 | 100 |
| `[{"@timestamp": "desc"}]` | 1000 | 100 |

**Gotchas:**
- Sorting on `id` or `numeroProcesso` errors: "Fielddata is disabled on [id]. Text fields are not optimised … Please use a keyword field instead." Use `_doc` or a date field.
- We did NOT verify `size: 10000` works (initial run hit the field-data error and returned 0); `size: 1000` works reliably. Worth a follow-up to test the upper page-size limit.

### 4.4 `_source` filtering — works, 90.9% payload reduction

| Variant | Returned `_source` | Response size (chars) |
|---|---|---|
| `default` (no filter) | full 14-key object | 100% baseline |
| `_source: false` | absent | **~9.1%** of baseline (90.9% reduction) |
| `_source: ["numeroProcesso", "classe.codigo"]` | projected fields only | small |
| `_source: {"excludes": ["movimentos"]}` | everything except `movimentos` | small (movimentos dominates) |
| `_source: {"includes": ["classe.*"]}` | just `classe` subtree | small |

**Layer 3 implication:** for high-volume sweeps where we only need numeroProcesso + a few fields, drop `movimentos` and `assuntos` for ~10× efficiency.

### 4.5 Rate limits — cautious read

The probe ramped target rates 1 → 2 → 5 → 10 req/sec with no 429 observed. **But the actual rates achieved were much lower:** 0.22–0.26 req/sec, because the server's response latency (median 4.1 s, max 20.4 s) dominates the inter-request budget. Single-threaded sustained throughput is therefore ≈ **0.25 req/sec from one connection**, not 10.

| Target rate | Actual rate | Latency median | Latency max | 429s |
|---|---|---|---|---|
| 1 req/sec | 0.23 | 5.8 s | 6.2 s | 0 |
| 2 req/sec | 0.24 | 4.3 s | 9.5 s | 0 |
| 5 req/sec | 0.26 | 3.8 s | 10.4 s | 0 |
| 10 req/sec | 0.22 | 4.1 s | 20.4 s | 0 |

**What we don't know:** behavior under parallel connections. To get throughput above ~0.25 req/sec we'd need concurrent requests; this probe didn't test that (risk of being throttled mid-inventory). Worth a follow-up dedicated rate-limit probe before any production Layer 3 batch design.

**Practical guidance for Layer 3:** assume **~0.25 req/sec single-threaded** as a working estimate. For 1000 process enrichments, ~70 minutes serial. With `_msearch` and `terms[]` batching, can collapse many process lookups into a single round-trip. With `search_after` + `_source` filtering, can harvest large result sets efficiently. Don't assume the safe parallel rate is high until tested.

---

## 5. Implications for Layer 3 design

### What is back in scope (vs. the previous synthesis's "Layer 3 cannot ship as v3.1 scoped")

Three things changed since the May 2026 Layer 3 probe synthesis:

1. **`complementosTabelados` is queryable.** This was the brief's "highest-leverage item." Filtering on sentença-outcome / distribuição-type / conclusão-reason at the API level — no PDF reading needed. Layer 3 can ship this as a snapshot-store enrichment that mirrors what DataJud already exposes.
2. **Aggregations + multi-index + msearch all work.** A Layer 3 build doesn't have to choose between "fetch every page client-side" or "make N round-trips per debtor." Server-side counts + batched lookups are real.
3. **TPU codes appear uniform across 5 tribunals (refresh).** Movement-based filters can in principle work cross-tribunal without per-tribunal code maps. (Caveat: only 3 codes tested; widen sample before relying on this.)

### What is still NOT in scope

- **Name search.** `partes.nome` is absent from the index mapping (proven via `_field_caps` control + official CNJ glossário — see [§2a](#2a-name-search--the-reconciliation)). Layer 3-as-cross-tribunal-debtor-search via DataJud alone remains infeasible. The only path to cross-tribunal party discovery is a per-tribunal eSAJ name-search aggregator (Layer 3-tribal — Track A confirmed the per-tribunal building block works at TJSP). Notably, LegalSuite's commercial product appears to be exactly this: they enrich DataJud with party data they source themselves, not from the public index.
- **CPF/CNPJ search.** Same — confirmed absent across all 5 tribunals and 5 field-name variants. LGPD framing per LegalSuite's docs is accurate.
- **`valor` (monetary value).** Index does not surface it at all. DataJud cannot back-fill eSAJ's value capture.

### Recommended Layer 3 v1 scope (post-inventory)

Three tiers, in increasing ambition:

1. **Layer 3-lite (cheapest, safest):** per-process DataJud enrichment via batched `_msearch` lookups. Brings in `complementosTabelados`, `assuntos`, `grau`, `orgaoJulgador.codigoMunicipioIBGE`, `dataHoraUltimaAtualizacao`. Adds structured success/failure signals on movements + geographic pivot + recency signal. Storage: extend the snapshot store schema. Effort: ~4-7 days. No new dependencies.
2. **Layer 3-aggregation:** Layer 3-lite + on-demand DataJud aggregation queries surfaced via the API (`/api/datajud/aggregate?group_by=classe&filter=…`). Server-side bucketing means UI gets fast answers to "count my carteira's open executions by foro." Effort: another ~3-5 days on top of Layer 3-lite.
3. **Layer 3-tribal (party search, expensive):** the per-tribunal eSAJ name-search aggregator path. Independent of DataJud. ~3-5 days per additional tribunal as previously estimated. Pursue only if the operator decides cross-tribunal party-name discovery is a near-term need; otherwise defer until a commercial DataJud tier becomes affordable.

### Open questions worth a follow-up probe

- **Upper page-size limit.** We confirmed `size: 1000` works; `size: 10000` initially failed but for a different reason (sort error, not size). Worth one targeted retry.
- **Parallel rate-limit behavior.** Single-threaded ≈ 0.25 req/sec; what does the API do under 5 / 10 / 20 concurrent connections? Risk of throttling means this needs an intentional probe session.
- **TPU code uniformity beyond 3 sample codes.** We tested 22 / 26 / 85; CNJ has hundreds. Widen to 20-30 codes before relying on cross-tribunal uniform naming.
- **LegalSuite reconciliation — RESOLVED** (was an open question; now closed in [§2a](#2a-name-search--the-reconciliation)). Verdict N1: party fields are absent from the index mapping (`_field_caps` control test) and from the official CNJ glossário; LegalSuite's blog conflates the public API with their own enriched product. No remaining ambiguity here. (The only residual unknown is whether parties were *ever* in the public API and removed — temporal hypothesis — vs. never present; the canonical glossário with no deprecation note favors "never present," and it doesn't change any decision.)

---

## 6. Methodology notes

### What was tested

Every part of the brief (1.1 / 1.2 / 1.3 / 2 / 3.1 / 3.2 / 3.3 / 3.4 / 3.5) ran end-to-end against the live API, plus a dedicated name-search reconciliation pass (`--parts recon`: index `_mapping`/`_field_caps`, LegalSuite verbatim query, 9-path party-field sweep, real PJ+PF defendant matrix). 10 test cases (4 from snapshot store with known defendants + 6 from prior probe samples spanning 1990-2024). Cross-tribunal validation across 5 indices. Documentary cross-check against the official CNJ Glossário de Dados and the LegalSuite blog.

### What was NOT tested (and why)

- **Higher than 10 req/sec target rate** — to avoid API-key suspension breaking other probe parts. Single-threaded actual rate ≈ 0.25 req/sec anyway, so the cap matters less than it sounds.
- **`size: 10000` with a working sort** — first attempt failed for a sort-shape reason, not a size-cap reason. Re-test deferred.
- **Concurrent connections / parallel rate-limit behavior** — risk-laden; deserves its own probe session.
- **More TPU codes for uniformity** — sampled 3 (distribuição, conclusão, arquivamento). Widening is straightforward.
- **Alternate / commercial DataJud endpoints** — out of scope; we only know about `api-publica.datajud.cnj.jus.br` and the wiki-published key.

### Prior-probe finding reconciliation

| Prior probe finding (May 2026) | Re-inventory verdict | Why |
|---|---|---|
| "DataJud has no name search" (Layer 3 probe) | **CONFIRMED + RECONCILED (N1)** | Decisive: `_field_caps` control test (party fields absent from mapping) + official CNJ glossário (14 fields, no `partes`). Track B's LegalSuite contradiction resolved — their blog conflates the public API with their own enriched product. See [§2a](#2a-name-search--the-reconciliation) |
| "Responses strip parties; 5-way verified" (datajud experiment_5) | **REFINED: parties absent, not just stripped** | `fields:["partes.nome"]` returns nothing; if parties were indexed-but-hidden, bypass would surface them. They're genuinely not in the index |
| "TPU codes are NOT national-uniform" (datajud experiment_2) | **REFRESH NEEDED — current 3-code sample shows uniformity** | Possibly stale; possibly our sample is narrow. Widen before relying on either reading |
| "CPF/CNPJ not indexed for LGPD" (LegalSuite framing) | **CONFIRMED** | 0 hits across 5 field-name variants × 5 tribunals |

### How to reproduce

```bash
python -m poursuite.probes inventory                  # full run, all parts
python -m poursuite.probes inventory --parts 1.1 2    # subset
```

Each invocation creates `<POURSUITE_LOG_DIR>/probes/datajud_inventory_<ts>/` with one raw JSON per query plus per-part `findings.json` and a combined `all_findings.json`. Append-only; prior run dirs stay reviewable.

### Layer 3-lite coverage + sampling note (L3L-e, June 2026)

Layer 3-lite (per-process DataJud enrichment) shipped — see `ARCHITECTURE.md` §15.
The end-to-end smoke measured **DataJud public-index coverage ≈ 80%** of a real
TJSP portfolio: **63 / 80** process numbers drawn at random from the DJE corpus
were present in the index (random-60: 49/60 = 82%; random-20: 14/20 = 70%),
2018–2020-weighted. So **~1 in 5 numbers cannot be enriched** — they simply
aren't in the public index. (A curated recent set hits ~100%, which *over*states
a real portfolio; ~80% is the honest planning figure.)

**Methodology trap — sample the optimized shards with random rowids, NOT
`LIMIT`.** The frozen DJE shards are physically sorted by `process_number` (the
`static_database_optimizer` rebuild). So `SELECT … LIMIT N`, natural-order
scans, and even `SELECT DISTINCT … LIMIT N` all concentrate on the low-sequence
`X00000X` tail — administrative / special-numbering cases DataJud indexes poorly
— which depressed the first smoke draws to 65–70% and made *every* miss an
`X000002` number. Random-rowid sampling (pick `rowid`s uniformly in
`[1, MAX(rowid)]`, point-lookup each) is required for a representative draw; it's
what `poursuite/datajud/cli.py --from-shard --limit` now does. Treat this as a
general rule for any spot-check against an optimized shard.

---

## 7. Surprises and recommendations

### What surprised me most

1. **`complementosTabelados` is fully accessible.** Coming into this I'd assumed the rich-movement-metadata signal was either absent or behind some commercial wall. It's right there, queryable via flat match, populated 10/10 in our sample. This is the single most actionable capability we've found.
2. **Aggregations work.** Public-API ES with aggregations enabled is not the default elsewhere. This collapses a lot of Layer 3 design complexity.
3. **`_field_caps` + the official glossário definitively closed the LegalSuite question.** The honest sequence: the first reconciliation pass over-claimed (it treated a 403 `_mapping` response as "field absent" — permission-denial mistaken for proof). The fix was `_field_caps` *with a control field*: it returns the control fields but zero party fields, proving absence without needing mapping-read permission. The official CNJ glossário (14 fields, no `partes`) clinches it from the documentation side. LegalSuite's contradicting example turned out to be marketing copy conflating the free API with their own enriched product — not evidence of a hidden capability.

### Unexpected limits

1. **Server response latency is high.** Median 4s, max 20s for `match_all size:1`. Layer 3 needs to assume slow-per-request even when the rate limit is loose.
2. **`size` interacts with sort.** `size: 10000` with the wrong sort returns 400, not a truncated response. Layer 3 batchers need to use `_doc` or a known-keyword field for sort, not `id` or `numeroProcesso`.
3. **`orgaoJulgador.codigoMunicipioIBGE` is conditional.** Present in the index at scale (10000+ hits for São Paulo), absent in our 10-case sample. So data sparsity is real per-class / per-era; Layer 3 enrichment can't assume populated everywhere.

### Recommended next workstream

Three options, ranked:

1. **Resume Track A (operator pre-existing plan): backfill `other_processes` for any new snapshot scrapes that ship without the flag.** Track A's small follow-up. ~1 day.
2. **Design + build Layer 3-lite.** With this inventory complete, the design is now constrained and well-understood. ~4-7 days for a v1 that brings `complementosTabelados`, `assuntos`, `grau`, `codigoMunicipioIBGE`, and `dataHoraUltimaAtualizacao` into the snapshot store. Operator's call whether to ship as a Layer 3 v1 or as a Phase-2 schema extension under a different name.
3. **Phase 3 (eSAJ deep search).** Unchanged from prior probe synthesis. ~2-3 weeks for the Selenium-mediated PDF download + extraction module.

My recommendation: **Layer 3-lite first**. It's the highest-value-per-day workstream the inventory enabled; `complementosTabelados` alone is a major triage signal that previously didn't exist anywhere in the snapshot store. Phase 3 is the bigger but longer build.
