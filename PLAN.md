# Poursuite — Strategic Plan (v3)

> Type A strategy document, third revision. Companion to `ARCHITECTURE.md`. When the two disagree, code is authoritative — update both.
>
> Last updated: May 2026. Supersedes v2. The v2 plan was written before Phase 1 (extra eSAJ header fields + CNJ origem derivation) shipped, and before the strategic reframe from "build a filter" to "build a search engine." This rewrite captures both shifts.

---

## 1. North Star (unchanged)

Poursuite buys distressed judicial credit at heavy discounts and recovers what it can. The economic edge is **selection** — picking recoverable cases out of large seller portfolios that nobody has individually analyzed. Two inflows:

- **Seller-provided lists.** A creditor hands over a CSV of process numbers and asks for a price. We surface the recoverable subset.
- **Self-discovered candidates.** Our DJE corpus reveals cases matching patterns indicating recoverability that current creditors haven't noticed.

In both flows, the scarce resource is **expert lawyer attention** (currently your brother). Everything we build multiplies that attention — eliminates cases unworthy of a minute of review, surfaces cases that deserve full focus, captures data he'd otherwise gather manually, and lets him ask new questions of the data without waiting on engineers.

**The system never bids autonomously.** It surfaces, it doesn't decide.

---

## 2. The framing — search engine, not filter (the most important section)

**Earlier versions of this plan framed the goal as "build a filter."** A filter has rules: the system runs them, scores cases, outputs a ranking. Rules are the system's logic; the lawyer is the consumer of the output.

**That framing was wrong.** Every portfolio Poursuite triages is different. A seller list of mortgage cases needs different filters than a list of commercial credit; the rules that work for one batch don't work for the next. Building a filter would have meant either (a) writing rules generic enough to be useless, or (b) re-tuning rules per portfolio and discovering we'd built an inflexible interface around frozen logic.

**The right framing is search engine.** The system holds structured data extracted from public sources. The lawyer composes queries against that data — different queries for different portfolios, different queries as he learns what works. The system is a data substrate plus a query interface; the logic lives in the lawyer's queries, not in the system's code.

What this changes:

- **No built-in filters.** "Creditor is a bank" or "movimento contains 'penhora'" are not default features. They're queries the lawyer composes when he wants them.
- **Curated lists are reference data, not classifiers.** A list of bank names lives in the system so the lawyer can query against it ("creditor IN bank_list"), but the system doesn't auto-classify creditors.
- **The data model is the product.** What fields exist, what they mean, what values they can take. The lawyer's expertise is in knowing what to ask; our job is making sure the data is there to answer it.
- **The interface is a query builder, not a parameter form.** The lawyer needs to compose multi-clause queries, save them, refine them, share them. That's a bigger UI build than v2 assumed.

This framing scales correctly. As Poursuite encounters new portfolio types, new market conditions, new lawyer insights, the system doesn't need code changes — it needs new queries.

Cases your brother handles via deep legal expertise (hidden assets via family connections, technical defects in títulos, mispriced lots) remain his work. The system surfaces candidates; he investigates. The valuable connections happen in his head, informed by what we surface.

---

## 3. The four layers (unchanged from v2)

```
                  ┌─────────────────────────────────────────────┐
                  │  Layer 4 — Connection graph / investigation │
                  │  Receita CNPJ + cross-source linking.       │
                  │  For deep investigation on promoted cases.  │
                  └────────────────────▲────────────────────────┘
                                       │
                  ┌────────────────────┴────────────────────────┐
                  │  Layer 3 — DataJud enrichment               │
                  │  Cross-tribunal debtor history.             │
                  │  National process universe per debtor.      │
                  └────────────────────▲────────────────────────┘
                                       │
                  ┌────────────────────┴────────────────────────┐
                  │  Layer 2 — eSAJ scrape (search substrate)   │
                  │  Per-process structured data + queryable    │
                  │  movimentações timeline + linked processes. │
                  │  The lawyer's primary search target.        │
                  └────────────────────▲────────────────────────┘
                                       │
                  ┌────────────────────┴────────────────────────┐
                  │  Layer 1 — DJE corpus query                 │
                  │  FTS5 over 677 GB of TJSP publications.     │
                  │  Discovery via published acts. Built.       │
                  └─────────────────────────────────────────────┘
```

Each layer takes structured input from below and produces structured output. Layers ship independently.

**The eventual LLM tier is a capability, not a layer.** It plugs in on top of Layer 2's data when (and if) we have ground-truth labels to evaluate it against. It is not on the critical path.

---

## 4. Shallow vs. deep data — a critical distinction

The system holds two kinds of data, and they obey different rules.

**Shallow data** is small, permanent, ingested in bulk:
- Header fields (Classe, Foro, Vara, Juiz, value, parties, etc.)
- Movimentações timeline (dates, codes, names, complements text)
- Linked processes / apensos / incidentes (process numbers, types)
- Petição metadata (date, type, page count — not content)
- Document IDs (`cdDocumento` references, names, types — not document text)

This is the lawyer's primary search target. It's what queries hit. It's what gets stored forever (or until we deliberately archive a case).

**Deep data** is large, ephemeral, fetched on demand:
- Full text of document PDFs (despachos, sentenças, petição content)
- OCR of scanned documents
- Anything that requires per-PDF downloading + extraction

Deep data exists only for cases that earned a lawyer's attention. It is **download-on-demand** ("deep search this case"), **extract-then-discard** (text goes to SQLite, PDF deleted), and **evict-when-cold** (text rows expire after N months of no access; can be re-fetched if the case becomes interesting again).

Storage discipline for deep data:

1. Lawyer requests deep search on a case.
2. System downloads PDFs from the case's `cdDocumento` references (shallow scrape already captured the IDs).
3. Text extracted/OCRed and stored in a `documents` table, compressed.
4. PDFs deleted; only the extracted text remains.
5. `last_accessed_at` timestamp tracked.
6. Periodic eviction: rows untouched for N months are dropped. Document IDs remain in shallow data, so re-fetching is straightforward.

**Why this matters:** PDFs are 100KB–2MB; compressed text is 5–50KB. ~10× savings. More importantly, this discipline keeps the system's footprint bounded as the lawyer deep-searches more cases over time. Without eviction, deep storage grows monotonically; with it, deep storage tracks the lawyer's active working set.

**What this is NOT:** lossy archival. Eviction is reversible — re-running deep search re-populates the text. Nothing is permanently lost.

Cases gated behind `#liberarAutoPorSenha` (per-case password) cannot be deep-searched automatically; the deep-search UI flags these for manual handling by the lawyer.

---

## 5. Where we actually are (May 2026)

Built and verified:

- **Layer 1** — DJE corpus FTS5 search. ~677 GB across time-partitioned shards. Queryable through API and CLI.
- **Layer 2 (partial)** — eSAJ scraper extracts 22 fields per case after Phase 1 (commit `78d8dcb`, May 2026):
  - Original 11 fields (classe, assunto, valor, parties, status, last_movement, distribuição, etc.)
  - 8 inventory-verified header fields added: Foro, Vara, Juiz, Controle, Outros assuntos, Outros números, Local Físico, Área
  - 3 fields derived from the CNJ process number itself: foro_code, tribunal_code, distribution_year
  - Plus `foro_name` resolved via the vendored TJSP origem table (614 codes, official CNJ source)
- **Maintenance orchestrator** — `update_database.py` runs Download → Parse → Split → Optimize → Publish. Idempotent, resumable.
- **Probe infrastructure** — `poursuite/probes/` lives in the repo as a long-term diagnostic.

Verified empirically via probes (not from documentation):

- DataJud's public API returns capa + movimentações for sampled processes including pre-2000 cases.
- DataJud strips party data at index level (5-way verified — no way to retrieve names/CPFs via the public API).
- DataJud's `valorCausa` is empty for TJSP.
- DataJud latency is unbounded — observed 21 days to 616 days behind reality.
- TPU movement codes are not national-uniform. TJSP locally relabels codes; code-based detection is per-tribunal. Text matching on `movimento.nome` is more reliable than code matching.
- eSAJ consulta is a single-document page with `<h2 class='tituloDoBloco'>` blocks, not real tabs.

Not yet built:

- **Movimentações timeline extraction in production.** The shallow scrape currently captures header + parties but not the movement timeline. This is Phase 2.
- **Apensos / Incidentes enumeration.** Currently counted, not enumerated.
- **Petição metadata + document ID capture.** Documents are visible in the inventory but not in the scraper output.
- **Snapshot store.** No persistence of scrape results across runs. Currently each scrape is throwaway.
- **The query interface.** The current API exposes parameter-form search; the search-engine framing wants a query builder. Bigger interface change than v2 assumed.
- **Layer 3 (DataJud), Layer 4 (Receita), LLM tier.** All deferred.

---

## 6. Layer 2 — the search substrate

This is the next workstream and what shipping value depends on. Phase 1 closed half of it.

### 6.1 What the substrate contains (target state)

Per case, after Phase 2 ships:

**Header (already shipped in Phase 1):**
- 22 fields per `ProcessData`, plus snapshot timestamp.

**Movimentações timeline (Phase 2):**
- Every movimento as a structured row: order, dataHora, codigo, nome, complementos (structured + text). Indexed for FTS and date queries.

**Linked structure (Phase 2):**
- Apensos / Incidentes / dependent processes enumerated with their CNJ numbers and relationship type.
- Petição listings with metadata (date, type, count, page count) and `cdDocumento` IDs.

**Reference data:**
- Vendored CNJ origem table (already in Phase 1).
- Curated creditor name lists, class blacklists, etc. — markdown files in the repo, edits via PR.

### 6.2 What the substrate enables the lawyer to query

These are example queries, not built-in features. The interface lets the lawyer compose any of them, save them, refine them:

- "Distribution between 2018-2020 AND value > R$100k AND no movimento containing 'penhora' since 2022" → stalled mid-value cases unlikely to have active recovery
- "Plaintiff IN bank_list AND defendant_other_process_count < 3" → bank-creditor cases with rare defendants
- "Movimento contains 'embargos à execução' AND distribution < 2018" → contested old cases
- "Foro IN [list] AND class = 'Execução de Título Extrajudicial'" → geographic filtering for a specific portfolio
- "Defendant name appears in DJE corpus with frequency > N" → debtors with structural noise in publications

The product isn't these queries. The product is being able to write any of these.

### 6.3 The interface

Bigger interface change than v2 assumed. The current SPA was built around parameter-form search; the search-engine framing wants:

- A query builder UI that exposes the full schema (column names, types, value enumerations, sample values)
- The ability to save named queries with descriptions
- Result sets the lawyer can drill into, sort, re-filter
- Some way to compose queries from saved building blocks
- Eventually: query sharing across team members (low priority for single-user current state)

This is significant frontend work. Probably the long-pole task after Phase 2's data extraction lands.

### 6.4 What Phase 2 needs to do

In rough order:

1. **Extract Movimentações.** Use the eSAJ inventory walker's section-walking logic adapted for production. Capture every movimento with order, dataHora, codigo, nome, structured complements, and raw complement text.
2. **Extract Apensos / Incidentes / linked processes.** Enumerate the CNJ numbers and types.
3. **Extract Petição metadata + `cdDocumento` IDs.** Shallow only — not document content.
4. **Design the snapshot store** (new SQLite, separate from DJE corpus). Schema decisions to make:
   - Append-per-scrape or scrape-then-diff (lean toward diff)
   - Indexing strategy (FTS5 on movimento.nome and complement text; B-tree on dates, values, codes)
   - Schema designed with deep-search-future in mind (a `documents` table that's empty by default)
5. **Wire into the existing API** as queryable endpoints. Defer the query builder UI to a separate workstream.

### 6.5 What Phase 2 explicitly does NOT include

- The query builder UI (separate workstream after data lands)
- Deep search / PDF ingestion (Phase 3)
- Layer 3 DataJud integration
- Any LLM work

Estimated effort: 2-3 weeks for Phase 2 data extraction + snapshot store. Query builder UI is its own multi-week build.

---

## 7. Phase 3 — Deep search

After Phase 2 lands, deep search becomes the next workstream. Per §4:

- Per-case, lawyer-initiated.
- Downloads PDFs from the case's `cdDocumento` references.
- Extracts/OCRs text into the snapshot store's `documents` table (compressed, FTS-indexed).
- Deletes the original PDFs.
- Tracks `last_accessed_at`; periodic eviction of cold rows.
- Flags `#liberarAutoPorSenha`-gated documents for manual lawyer handling.

Once deep search ships, the lawyer's queries can hit document text alongside movimento text. A query like "defendant X AND any document contains 'fraude'" becomes meaningful.

This is its own ~1-2 week workstream and ships only after Phase 2's data is solid.

---

## 8. Layer 3 — DataJud enrichment (unchanged scope)

After Phase 2 and Phase 3, Layer 3 wraps the existing DataJud probe code into a production module. The single use case that matters: cross-tribunal debtor universe — "how many executions does this debtor have nationally?"

The probe already handles authentication, query formation, response parsing, TPU resolution. Production needs persistent storage of DataJud responses, a name-search interface, confidence scoring on name-search hits, and integration with the snapshot store.

Mostly plumbing, not new capability. Estimate: 1-2 weeks once we get there.

---

## 9. Layer 4 — Connection graph (deferred)

For cases promoted from Layer 2/3 search and warranting deep investigation. Receita CNPJ + cross-source linking. Same scope as v2: demoted in urgency because SNIPER coverage handles in-process patrimony and Receita's no-historical-sócios limitation is fundamental.

Ships when promoted cases warrant it. No immediate work.

CNPJ alphanumeric event in July 2026 still applies — any code touching CNPJ must be alphanumeric-ready.

---

## 10. The LLM tier (still a capability, not a layer)

Same disposition as v2. Builds on Layer 2 data + ground-truth labels + skill files. Cost regime decision parked until we have token measurements from real movimentações data.

The search-engine framing reduces the LLM tier's importance somewhat. A good query interface means the lawyer can extract a lot of value without ever invoking an LLM. The LLM becomes a polish for cases where prose interpretation (reading a despacho) adds value beyond what queries can find.

Not on the critical path. Maybe never built. That's fine.

---

## 11. Methodology — how we work (unchanged from v2)

- **Verify before building.** Secondary sources lie. Probes verify.
- **Don't pre-judge what's useful during inventory.** Record everything; filter later.
- **Inventory before extending.** Look at the page, not the docs.
- **Reproducibility costs nothing upfront and a lot to retrofit.** Hash inputs, version everything, cache by content.
- **Lawyer in the loop is structural, not optional.** No layer auto-decides.
- **Documents drift; code is authoritative.** Fix the docs when they're wrong.

The probe + inventory pattern proved itself again in Phase 1. Keep applying.

---

## 12. Sequencing

### Now
**Phase 2 — Movimentações + linked structures + snapshot store.** Extract movimentações timeline, enumerate apensos/incidentes, capture petição metadata + cdDocumento IDs, build snapshot SQLite. Wire as queryable API endpoints. 2-3 weeks.

### Next
**Query builder UI.** The interface change the search-engine framing implies. Multi-week frontend build. Defines the lawyer's primary tool.

### After that
**Phase 3 — Deep search.** Per-case PDF download, text extraction, eviction-managed storage. 1-2 weeks.

### Later
**Layer 3 — DataJud production module.** Cross-tribunal debtor universe wired into the search substrate. 1-2 weeks.

### Eventually
**Layer 4 — Receita / connection graph.** Investigation-tier tool for cases that earn deep attention.

**LLM tier.** If/when ground truth and use case warrant it.

---

## 13. Standing open questions

1. **Ground truth.** Still uncaptured. Less critical now that the system is framed as a search engine the lawyer drives (his queries are the ground truth), but matters if we ever build the LLM tier.
2. **Throughput.** Still uncertain. Less critical too — query-based systems scale differently than rule-based ones.
3. **Storage backup.** Maintenance pipeline outputs are on D:; snapshots will be on D: too. If D: dies, the DJE corpus is rebuildable but snapshots and (eventual) deep-search data are not. Worth a real backup plan before Phase 3 ships.
4. **Multi-tenant.** Currently single-user. Not urgent.
5. **Query interface design.** How expressive does it need to be? Visual builder vs. text query language vs. both? Worth thinking through before the multi-week UI build starts.

---

## 14. Document maintenance

This document updates whenever:
- A workstream completes (e.g., Phase 1 → §5 update in this revision)
- An open question gets answered
- A new constraint or opportunity surfaces
- A probe disproves an assumption baked into the plan
- The strategic framing shifts (search-engine reframe in this revision)

When this document and `ARCHITECTURE.md` disagree, code is authoritative.

When this document and your brother's expertise disagree, your brother is authoritative.
