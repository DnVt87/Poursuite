# Brief for Claude Code — DataJud Capability Re-Inventory

## Goal

Build a canonical, empirically-verified reference for what the DataJud public API can and cannot do. The previous probes (May 2026 + the recent Phase 3 + Layer 3 round) accumulated findings incrementally, and at least one of those findings — "DataJud doesn't support name search" — has been falsified by external evidence (LegalSuite's March 2026 production documentation explicitly publishes a working `partes.nome` query example; see `TRACK_B_FINDINGS.md`).

The methodology failure: we accepted confident negative results without re-verifying when commercial products visibly use the capability we said didn't exist. This re-inventory addresses that systematically.

**Output:** `docs/DATAJUD_CAPABILITY_INVENTORY.md` — the canonical reference, replacing patchwork understanding from prior probes. Becomes the foundation for any future Layer 3 design.

**Budget:** 1.5-2 days, three parts.

**Track A (`other_processes` investigation) is paused** until this re-inventory closes. If DataJud supports name search, the `other_processes` field becomes much less consequential — cross-tribunal parallel-case counts would come from DataJud rather than from the eSAJ scraper's per-defendant secondary search.

## Constraints

- **Use existing infrastructure:** extend `poursuite/probes/datajud.py` rather than starting fresh
- **Test against real cases from the snapshot store**, not synthetic data — using actual scraped cases gives us ground truth to compare against
- **Don't build production code.** This is a probe; findings drive design.
- **Reproducible artifacts:** save all raw JSON responses to disk under `<POURSUITE_LOG_DIR>/probes/datajud_inventory_<ts>/`
- **Document everything.** Every result, every variant tried, every failure mode. The output document is the artifact; the probe code exists to populate it.

---

## Part 1 — Re-verify past negatives

The three things prior probes concluded couldn't be done. Re-test each with the discipline of "what would change my mind?"

### 1.1 Party data accessibility

**Past finding:** "Responses strip parties at the index level — verified across 5 tribunals (May 2026 experiment_5)." Implication assumed: parties not indexed, name search impossible.

**Refined hypothesis:** parties indexed for search, filtered from `_source`. Search works; reading the party from a response doesn't.

**Tests (formerly "Probe B-prime"):**

1. **Pick a real TJSP case from `process_snapshot`.** Read its `defendant` field directly from the database — that's the *exact* string as eSAJ stored it.

2. **Query DataJud `partes.nome` for that defendant** across four variants:
   - **Exact:** `{"match": {"partes.nome": "<exact name>"}}`
   - **Lowercase:** `{"match": {"partes.nome": "<lowercased name>"}}`
   - **Tokenized:** `{"match": {"partes.nome": "<first 2 words of name>"}}`
   - **Phrase:** `{"match_phrase": {"partes.nome": "<exact name>"}}`

3. **Expected outcome (World A):** at least one variant returns ≥1 hit, including the test case itself. This confirms LegalSuite's documentation and falsifies Probe B's original finding.

4. **If 0 hits across all variants (World B/C):** test against TJRJ and TJMG with the same case's defendant. If those work, the failure is TJSP-specific. If they don't, we have a different finding to investigate further.

5. **Test response filtering bypass.** For a variant that DID return hits, try these query modifiers and see if `partes` appears in `_source`:
   - `"_source": true` (default)
   - `"_source": ["partes"]` (explicit include)
   - `"_source": {"includes": ["partes.*"]}` (wildcard include)
   - `"fields": ["partes.nome"]` (Elasticsearch fields API)
   - `"docvalue_fields": ["partes.nome.keyword"]` (doc values, if available)

   This determines whether the filter is absolute or bypassable.

6. **Repeat tests 2-3 with 5-10 different test cases** spanning different classes, dates, and PJ vs PF defendants. One success could be lucky; consistent success across varied inputs is the signal.

### 1.2 CPF/CNPJ search

**Past finding (LegalSuite docs):** CPF/CNPJ not indexed in public API for LGPD reasons.

**Refined hypothesis:** Confirm LegalSuite's framing per-tribunal. Judit called it "inconsistente" — implying it works somewhere.

**Tests:**

1. From a snapshot case with a known PJ defendant, extract the CNPJ if visible (eSAJ surfaces this for some cases). If not, pick a publicly-known major CNPJ (Banco do Brasil: 00.000.000/0001-91).

2. Query `match.cpfCnpj` (and variants like `numeroDocumentoPrincipal`, `documento.numero`) against all 5 tribunal indices we currently use.

3. Document per-tribunal: response code, hit count, any error messages. The expected outcome is uniformly 0 hits or 400 errors — but inconsistent results across tribunals would be a meaningful finding.

### 1.3 Movement code national uniformity

**Past finding (May 2026 experiment_2):** "TPU movement codes are NOT national-uniform — different tribunals use different codes for similar movements."

**Refined hypothesis:** Re-test. CNJ has been pushing TPU standardization; codes may have stabilized.

**Tests:**

1. Define 3 movements with well-known semantics: distribuição (code 22), conclusão ao juiz (code 26), arquivamento (code 85).

2. For each, query each tribunal index for cases with that specific code in `movimentos.codigo`. Tabulate hit counts.

3. From sample hits in each tribunal, read the `movimentos.nome` field for the matched movement. Is the human-readable name consistent across tribunals?

4. Outcome: confirm or refute uniformity. If still non-uniform, document the worst offenders for future planning.

---

## Part 2 — Inventory unprobed fields

For each field LegalSuite documents that we don't currently have in our probe inventory, empirically determine: present in responses? populated for our test cases? queryable as a search target? See LegalSuite's field table:

| Field | Source | Why we care |
|---|---|---|
| `assunto` (array of TPU codes + names) | Documented, we currently take from eSAJ | TPU-coded subject classification; potentially queryable across tribunals |
| `grau` (G1/G2/JE/etc.) | Documented, we don't have it | Instance level — useful filter ("first instance only") |
| `orgaoJulgador.codigoMunicipioIBGE` | Documented, we don't have it | Geographic pivot via IBGE municipality codes |
| `valor` (number) | Documented; eSAJ has the string form | Is DataJud's value normalized? Could replace our raw-string field |
| `movimentos.complementosTabelados` | Documented, we don't capture | **High-value:** sub-codes attached to movements (e.g., movement 12223 "Sentença" + complemento 3 "Procedência" = sentença favorável). Structured success/failure signal without parsing free text. |
| `movimentos.dataHora` | We capture this | Cross-check format consistency |

**For each field, three diagnostics:**

1. **Present in responses?** Pull a sample of 10 cases from the existing snapshot store's CNJ numbers; query DataJud by `numeroProcesso`; inspect the returned `_source`. Document which fields appear, which don't, and at what consistency rate.

2. **Populated for our test cases?** Of cases where the field is present, what proportion are non-null? An always-empty field is functionally absent.

3. **Queryable as a search target?** For each field, try a `match` (or `term` for keyword-typed) query and verify it filters correctly. Specifically test:
   - `grau`: query for `"G1"` and confirm hits return G1 cases
   - `assunto.codigo`: query for a specific subject code and verify
   - `orgaoJulgador.codigoMunicipioIBGE`: query for São Paulo's IBGE code (3550308)
   - `valor`: range query (`"range": {"valor": {"gte": 100000}}`) — does it work as a number?
   - `movimentos.complementosTabelados.codigo`: nested query for sentença complemento — confirms whether structured filtering on the success/failure axis is possible

The `complementosTabelados` test is the highest-leverage item in this whole brief — if structured sentence outcome signals are queryable, that's a near-term filter we should add to the snapshot model regardless of Layer 3 disposition.

---

## Part 3 — Operational capabilities

### 3.1 Rate limits

**Goal:** empirically determine the rate limit for our API key, so we can design Layer 3's batching properly.

**Tests:**

1. Issue queries at increasing rates: 1/sec, 5/sec, 10/sec, 50/sec, 100/sec. Use the same query (e.g., `match_all` size 1 against TJSP) to control variables.

2. Record when (a) the API starts returning 429 (or any error), (b) latency increases significantly, (c) the key is throttled across subsequent requests.

3. Document: requests-per-minute limit, requests-per-hour limit, behavior on exceeding (429 immediately? gradual slowdown? key suspended for a window?), and the "safe" sustained rate for batch operations.

### 3.2 Aggregation queries

**Goal:** determine if Elasticsearch aggregation queries work on the public API. If yes, this changes Layer 3 design substantially — we could ask "count cases by foro for this defendant" in one query instead of fetching all hits and aggregating client-side.

**Tests:**

1. Try a `terms` aggregation: `{"size": 0, "aggs": {"by_classe": {"terms": {"field": "classe.codigo"}}}}` against TJSP. Confirm hits.

2. Try a date histogram: `{"size": 0, "aggs": {"by_year": {"date_histogram": {"field": "dataAjuizamento", "calendar_interval": "year"}}}}`.

3. Try a nested aggregation: by `orgaoJulgador.nome` within a name-search query.

4. Document: which aggregations work, which return errors, and what the response shape looks like.

### 3.3 Bulk/multi-process patterns

**Goal:** can we query multiple processes in one request, instead of N requests for N processes?

**Tests:**

1. Multi-search via `_msearch` endpoint:
   ```
   POST /_msearch
   {"index": "api_publica_tjsp"}
   {"query": {"match": {"numeroProcesso": "1234..."}}}
   {"index": "api_publica_tjsp"}
   {"query": {"match": {"numeroProcesso": "5678..."}}}
   ```

2. `terms` query with multiple process numbers in a single search:
   ```
   {"query": {"terms": {"numeroProcesso": ["1234...", "5678...", "9012..."]}}}
   ```

3. Cross-tribunal in one query — try the multi-index pattern: `/api_publica_tjsp,api_publica_tjrj/_search`.

4. Document what works and the response structure for each.

### 3.4 `search_after` pagination

**Goal:** confirm large-result-set harvesting works. Past observations suggested the 10,000-record limit; `search_after` is the documented workaround.

**Tests:**

1. Issue a sorted query (`sort: [{"dataAjuizamento": "desc"}, {"_id": "asc"}]`) with `size: 10000` against TJSP. Confirm 10000 hits returned.

2. Take the last hit's sort values; issue a follow-up query with `search_after: [<lastDate>, <lastId>]`. Confirm subsequent results.

3. Document: stable across consecutive calls? Any rate-limit interaction with high-volume pagination?

### 3.5 `_source` filtering granularity

**Goal:** if we can limit response payload size (don't fetch fields we don't need), large-batch operations get cheaper.

**Tests:**

1. `{"_source": false}` — confirm metadata-only response works
2. `{"_source": ["numeroProcesso", "classe.codigo"]}` — confirm field-projection works
3. `{"_source": {"excludes": ["movimentos"]}}` — confirm exclude semantics

This may also reveal whether `partes` filtering can be bypassed (test 1.1 step 5).

---

## Output

`docs/DATAJUD_CAPABILITY_INVENTORY.md` — the canonical document. Structure:

1. **Headline findings** (1-2 paragraphs): the major conclusions, including any falsified prior beliefs.
2. **Field-by-field capability matrix:** for every documented and tested field, columns for "in response," "populated rate," "queryable as filter," "queryable as aggregation," "supports range/numeric ops," with empirical values.
3. **Per-tribunal capability matrix:** for TJSP, TJRJ, TJMG, TRT-2, TRF-3, the major capabilities tested (name search, CPF/CNPJ search, movimento codes, aggregation queries).
4. **Operational limits:** rate limit, max page size, `search_after` behavior, aggregation support, multi-search support, response filtering options.
5. **Implications for Layer 3:** with the inventory complete, which Layer 3 designs are now in scope, which are out, and which are unblocked from the prior plan.
6. **Methodology notes:** what was tested, what wasn't (and why), known gaps.

Plus raw artifacts under `<POURSUITE_LOG_DIR>/probes/datajud_inventory_<ts>/` for audit.

---

## Definition of done

- All three parts run end-to-end against the live API
- `docs/DATAJUD_CAPABILITY_INVENTORY.md` exists with all sections populated
- Raw response artifacts saved under the probe directory
- A summary at the end of the work covering: what surprised you most, what unexpected capabilities appeared, what unexpected limits appeared, and explicitly which prior probe findings were confirmed vs. falsified
- Recommended next workstream based on the new inventory (Layer 3 design? Layer 3 build? Resume Track A?)

## When done

Don't commit. Capture findings, report back. The operator reviews the inventory and decides next workstreams.
