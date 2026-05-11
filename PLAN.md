# Poursuite — Strategic Plan (v2)

> Type A strategy document, second revision. Companion to `ARCHITECTURE.md` (which describes the system as built). When the two disagree, code is authoritative — update both.
>
> Last updated: May 2026. Supersedes v1 (May 2026, pre-probe). The v1 plan was written before we ran probes against DataJud, before the eSAJ inventory pass, and before the strategic reframe of what "good filtering" actually means for Poursuite. This rewrite captures what we learned.

---

## 1. North Star (unchanged)

Poursuite buys distressed judicial credit at heavy discounts and recovers what it can. The economic edge is **selection** — picking recoverable cases out of large seller portfolios that nobody has individually analyzed. Two inflows:

- **Seller-provided lists.** A creditor hands over a CSV of process numbers and asks for a price. We triage and decide what fraction is worth bidding on.
- **Self-discovered candidates.** Our DJE corpus surfaces cases matching patterns indicating recoverability that current creditors haven't noticed.

In both flows, the scarce resource is **expert lawyer attention** (currently your brother). Everything we build multiplies that attention — eliminates cases unworthy of a minute of review, surfaces cases that deserve full focus, and captures data he'd otherwise gather manually.

**The system never bids autonomously.** It decides what reaches a human's desk and in what order.

---

## 2. The strategic reframe (the most important section)

The v1 plan implicitly framed the goal as "build an AI that triages cases like your brother does." That framing was wrong, and getting it wrong would have wasted months.

**What your brother actually does in his successful cases** — uncovering hidden assets via family connections, spotting technical defects in títulos executivos, recognizing mispriced lots a big fund overlooked, catching procedural irregularities — **is inherently lawyer work.** It requires judgment, legal expertise, and contextual reasoning that we cannot realistically encode. Trying to automate it leads to either (a) brittle rule systems that fail on the edge cases that actually matter, or (b) LLM systems expensive enough to wipe out the arbitrage they're meant to enable.

**What Poursuite actually needs is the layer below that** — a portfolio-level pre-filter that eliminates obvious losers before any human reviews them. Coarse signals, not deep analysis. The point is to turn 5,000 cases into 200 worth a lawyer's attention, ranked. The 200 then get genuine human judgment.

The signals that matter for the pre-filter are **counterparty profile** and **competition density**, not legal analysis:

- **Creditor type.** Is the original creditor a bank (variable, often exploitable), a hedge fund / securitizadora (already optimized, no edge), a specialized recovery firm (won't let go, no opportunity), or a natural person / small company (amateur, opportunity)? This single classification predicts a lot.
- **Debtor's process universe size.** A debtor with 2 active executions is qualitatively different from a debtor with 50. Recoverability per case drops sharply at high counts because competing creditors fight over the same assets.
- **Process age × staleness.** Old + stalled cases have different profiles than active or recent ones.
- **Process value.** Below thresholds where legal cost outweighs recovery; above thresholds where seller diligence already happened.
- **Class and subject.** Some classes recover better — your brother has tacit knowledge of which.

All of these are extractable from what we already have (or are about to add). None requires an LLM. None requires the connection graph. The first product is *much smaller* than v1 implied.

The connection graph and the LLM tier are still on the roadmap — but they're for the cases *that pass the pre-filter*, not for the filter itself. They serve investigation, not triage.

---

## 3. The four layers (renumbered, clarified)

```
                  ┌─────────────────────────────────────────────┐
                  │  Layer 4 — Connection graph / investigation │
                  │  Receita CNPJ + cross-source linking.       │
                  │  For cases promoted from Layer 2/3 that     │
                  │  warrant deeper investigation. Not a filter.│
                  └────────────────────▲────────────────────────┘
                                       │
                  ┌────────────────────┴────────────────────────┐
                  │  Layer 3 — DataJud enrichment               │
                  │  Cross-tribunal debtor history.             │
                  │  "How many executions does this debtor have │
                  │  nationally?" — only practical source.      │
                  └────────────────────▲────────────────────────┘
                                       │
                  ┌────────────────────┴────────────────────────┐
                  │  Layer 2 — eSAJ scrape (the pre-filter)     │
                  │  Per-process structured data. Header fields,│
                  │  parties, value, movimentações, linked      │
                  │  processes. The portfolio pre-filter runs   │
                  │  on this data.                              │
                  └────────────────────▲────────────────────────┘
                                       │
                  ┌────────────────────┴────────────────────────┐
                  │  Layer 1 — DJE corpus query                 │
                  │  FTS5 over 677 GB of TJSP publications.     │
                  │  Candidate discovery via keywords / patterns│
                  │  in published acts. Already exists.         │
                  └─────────────────────────────────────────────┘
```

Each layer takes structured input from below and produces structured output. Layers are independent — each ships and delivers value before the next exists.

**The eventual LLM tier is not a layer.** It's a *capability* that can be added on top of Layer 2's output (with or without Layers 3/4) once we have ground-truth labels to evaluate it against. The v1 plan put it as Layer 3 of three; that was wrong. LLM analysis is the polish on a working pipeline, not a layer of the pipeline itself.

---

## 4. Where we actually are (May 2026)

Built and verified:

- **Layer 1** — DJE corpus FTS5 search. Works. ~677 GB across time-partitioned shards, indexed by paragraph, queryable through the API and CLI.
- **Layer 2 (partial)** — eSAJ scraper. Extracts ~10 header fields plus parties. **The inventory pass identified 8 more header fields available on the same page that we don't extract**, plus 6 structural sections (Partes, Movimentações, Petições, Incidentes, Apensos, Audiências) we either partially capture or don't capture at all. The Movimentações timeline is the biggest gap — fully present on the page, completely unscraped today.
- **Maintenance orchestrator** — single `update_database.py` runs the full Download → Parse → Split → Optimize → Publish pipeline. Idempotent, resumable, with disk-space preflight.
- **Probe infrastructure** — `poursuite/probes/` lives in the repo as a long-term diagnostic. DataJud and eSAJ-inventory probes are wired in. Receita is stubbed.

Verified via probes (not via documentation — these are empirical):

- DataJud's public API returns capa + movimentações for 6/6 sample TJSP processes including the 1990 case.
- DataJud strips party data at the index level (5-way verified — no way to retrieve names/CPFs).
- DataJud's `valorCausa` is empty for TJSP (confirmed). Process value comes from eSAJ.
- DataJud latency is **unbounded** — observed range 21 days to 616 days behind reality.
- TPU movement codes are NOT national-uniform — TJSP locally relabels some codes (11383 nationally documented as "Penhora", TJSP returns "Ato ordinatório"). Code-based detection is per-tribunal, not portable.
- eSAJ consulta is a single-document page with `<h2 class='tituloDoBloco'>` blocks, not real tabs. Same DOM at every viewport.

Not yet built:

- The 8 additional eSAJ header fields. Production extraction of Movimentações timeline. Production extraction of linked processes / apensos / incidentes (currently only counted, not enumerated).
- The pre-filter itself (Layer 2 application).
- DataJud integration (Layer 3).
- Receita ingestion (Layer 4).
- Any LLM tier.

---

## 5. Layer 2 — what the pre-filter looks like

This is the next workstream. The pre-filter is the first real product.

### 5.1 Inputs

Per-process structured data extracted from eSAJ. The current scraper gets the header; the expanded scraper will get everything the inventory pass surfaced. Specifically:

**Header fields to add to extraction** (verified empirically):
- `Foro` — the comarca/forum (e.g., "Foro Central Cível", "Foro de Sorocaba")
- `Vara` — specific division within the foro
- `Juiz` — assigned judge
- `Controle` — internal foro numbering
- `Outros assuntos` — secondary subjects (5/13 cases in inventory had this)
- `Outros números` — alternate process numbers, only on legacy cases (2/13 in inventory)
- `Local Físico` — physical location, only on legacy cases (1/13)
- `Área` — area of law (always "Cível" in our sample, low-value but cheap to capture)

**Plus three fields derivable from the CNJ process number itself** via a lookup table (no scraping needed):
- Foro/origem (deducible from last 4 digits `OOOO`)
- Tribunal (deducible from positions 18-19 `TR`)
- Year of distribution (deducible from positions 14-17 `AAAA`)

**Structural sections to enumerate** (currently uncaptured):
- **Movimentações** — the full procedural timeline. Critical for filtering: presence/absence of citação, penhora events, embargos, suspensions. 70–470 rows per case in our sample.
- **Apensos / Incidentes** — linked process numbers (embargos à execução, IDPJ incidents). Currently we count "other_processes by defendant" but don't enumerate formal linkages.
- **Petições diversas** — petition listings with metadata. Useful for procedural density signal.
- **Histórico de classes** — conditional section showing procedural reclassification history. Rare but high-signal when present (1/13 in inventory). Worth capturing where it exists.

### 5.2 The pre-filter rules

The actual filtering logic. None of these requires an LLM:

**Creditor classifier.** Regex + curated name list. Banks have predictable razão sociais (`BANCO BRADESCO S.A.`, `ITAÚ UNIBANCO`). Hedge funds and securitizadoras have characteristic suffixes (`FIDC`, `SECURITIZADORA DE CRÉDITOS`). Specialized recovery firms are knowable by name (curated by your brother). Natural persons have non-corporate name shapes. Output: one of {bank, hedge_fund, recovery_specialist, natural_person, small_business, unknown}. Trivial to build; ~95% accuracy expected on common cases.

**Debtor process count.** Already captured as `contadorDeProcessos`. Threshold buckets configurable: 1-2 = potentially recoverable, 3-10 = harder, 10+ = trash. Refined later with DataJud cross-tribunal count (Layer 3).

**Value tier.** Already captured as `valorAcaoProcesso`. Configurable thresholds by case class.

**Process age + staleness.** Distribution date and last movement, both already captured. Various rules: very old + stalled = prescrição risk (flag for human review, never auto-discard); very recent = probably still in citação phase.

**Class blacklist.** Some classes (small-claims Juizado Especial under threshold, etc.) → low priority. Curated by your brother.

**Prescrição risk estimate.** Coarse — flag any case where (today − distribuição) > (5 years + suspensão buffer). Refined by Layer 2's Movimentações data once we extract that, because the actual prescrição clock starts from "primeira tentativa infrutífera" which is a movement we'd need to identify (CPC art. 921, Lei 14.195/2021). v0 is the coarse age-based flag; v1 reads the timeline.

**Linked process density.** Once Apensos / Incidentes are extracted, count them. Many embargos / many IDPJ incidents = legally contested case = different recovery profile.

### 5.3 Output

A scored, ranked list:

- Continuous score (0-100, for sorting)
- Bucket (`dead`, `low`, `mid`, `high`, `review_required`)
- Reason codes (`creditor_specialized_fund`, `debtor_50plus_processes`, `value_below_threshold`, `prescription_risk_imminent`, etc.)
- Underlying data backing each reason

The lawyer reviews top-down. The system makes the case for or against; the human decides.

### 5.4 What's needed before this ships

1. **Expand the eSAJ scraper** to extract the additional header fields and the structural sections. Read the inventory output before designing — the prejudgment risk we worked hard to avoid still applies.
2. **Build the snapshot store** so scraped data persists across runs (already partly in scope from the maintenance refactor — extend the schema to cover Movimentações timeline).
3. **Build the rule engine** as a separate module from the scraper. Rules should be loadable and tunable without code deploys (probably YAML for thresholds and curated lists, code for the predicates themselves).
4. **Wire ranking/output** into the existing API + CLI.

### 5.5 Open questions

- **Rule storage format.** Pure code is most powerful but requires deploys. YAML thresholds + code predicates is the obvious compromise. Final choice can wait until we have 3-4 rules built and feel the friction.
- **How seller lists get ingested.** Existing `CSVProcessExtractor` extracts process numbers from arbitrary CSVs — reusable as-is. If sellers provide other metadata (their own value estimates, etc.) we may want to preserve it; not urgent.
- **Curated lists (creditor names, class blacklists) — how to maintain them.** Probably markdown files in the repo, version-controlled. Edits flow through PR review. Your brother is the editor.

---

## 6. Layer 3 — DataJud enrichment

Smaller than v1 implied, but real.

### 6.1 What DataJud gives us

After empirical verification:
- Cross-tribunal coverage of capa + movimentações for any CNJ process number
- All 91 tribunals via one API, one key
- Pre-2000 coverage confirmed (1990 case returned cleanly)
- Hours-to-616-days behind reality — usable for historical context, not real-time

What it does NOT give us:
- Party names, CPFs, CNPJs (stripped at index level — verified five ways)
- Document text
- TJSP `valorCausa` (empty in practice)
- Real-time anything

### 6.2 The single use case that matters

**Cross-tribunal debtor universe.** Once we have the debtor's name from eSAJ (Layer 2), we can search DataJud by name across all 91 tribunals. Returns a count and list of every public process the debtor appears in. This is information no other source provides:

- "Debtor has 3 cases in TJSP, 47 in TJRJ, 12 in TRT-2" → recoverability collapses
- "Debtor has 1 case nationally" → significantly more interesting
- "Most of debtor's cases are tributárias and they're losing" → useful procedural signal

This is also a fundamental input for Layer 4's connection graph (which entities co-occur with our debtor across tribunals?).

### 6.3 Caveats

- **Name search is fuzzy by nature** — common names produce false positives. The Layer 3 module needs to be honest about confidence and surface candidate matches for human disambiguation when needed.
- **TPU codes are per-tribunal.** Any movement-text analysis (e.g., "are any of these national cases at penhora stage?") needs per-tribunal code mappings, not a national table. Adds complexity to any aggregate analysis.
- **Latency means our snapshot is always stale.** Fine for Poursuite's "find quiet cases" use case (a case DataJud hasn't seen in 6 months is exactly the profile we want), but the limitation should be explicit in any Layer 3 output.

### 6.4 What's needed

Wrapping the existing probe code into a production module. The probe already handles authentication, query formation, response parsing, and TPU code resolution. The production version needs:

- Persistent storage of DataJud responses (snapshot per process per fetch date)
- A name-search interface separate from the by-number interface
- Confidence scoring on name-search hits
- Integration with Layer 2 output (debtor names flow in, national counts flow back)

This is mostly plumbing, not new capability.

---

## 7. Layer 4 — Connection graph (deferred, scope clarified)

### 7.1 What Layer 4 is for

The lawyer-grade investigation tooling. Given a debtor that has passed Layer 2's filter and warrants real attention, surface every connection we can find in public data:

- Companies the debtor is a sócio of (current)
- Sócios who appear in those companies alongside the debtor
- Addresses overlapping across multiple CNPJs
- Cross-creditor patterns (same lawyer in many cases on opposing sides)

The valuable cases your brother described — debtor's brother-in-law owns three companies at the same address — live here.

### 7.2 What changed from v1

**Receita CNPJ is demoted in urgency.** Two reasons:

1. **SNIPER coverage of in-process patrimony.** Brazilian courts already query the consolidated debtor patrimony system (SNIPER, BACEN-JUD, SISBAJUD, RENAJUD, etc.) on the judge's behalf. The results appear in the movimentações timeline. So for assets already known to the system, we get them via Layer 2's movimentações extraction — not via Receita.

2. **Receita's no-historical-sócios limitation is fundamental.** The bulk dump shows only current partnerships. The "exited the company three months before bankruptcy" pattern — exactly what investigation cares about — is invisible. Mitigations exist (JUCESP fichas, our own monthly snapshots going forward) but the limitation is real and limits how much value Receita alone delivers.

So Receita stays in the plan, but as a Layer 4 input rather than a near-term ingestion priority. We ship it when we have promoted cases that warrant deep investigation — not before.

### 7.3 The CNPJ alphanumeric event (July 2026)

Still on the critical path. Receita Federal moves CNPJ format to alphanumeric in July 2026. Anything we build that touches CNPJ between now and then must accept letters in positions 1-12. Any database column typed numeric must become text. This applies whenever we touch CNPJ, regardless of layer.

### 7.4 Open questions

Same as v1 — identity resolution is still the hard problem. Address normalization. Storage strategy (SQLite scales further than people think; defer real-graph-database decision until measured).

---

## 8. The LLM tier (capability, not a layer)

Eventually we want LLM analysis on cases that pass Layer 2 — reading despachos, weighing ambiguous signals, producing written justifications. This is real value, but it's a polish on a working pipeline, not a layer of the pipeline itself.

**Preconditions for building this:**
1. Layer 2 movimentações extracted in production
2. Layer 3 wired (cross-tribunal context as LLM input)
3. **Ground-truth labels** — your brother's triage decisions documented case-by-case, ideally retrospectively-labeled with reasoning. Without these, we can't evaluate whether the LLM agrees with him, which means we can't iterate.

**The cost regime decision (Opus everywhere / cheap-then-expensive / self-hosted) is parked.** We don't have token measurements yet — those depend on what movimentações look like in production extraction. Decide when we have real measurements.

**The skill files idea remains right.** Each MD file codifies one decision (`prescricao-intercorrente.md`, `penhora-evaluation.md`, etc.). Versioned in git. Analysis cache keyed on skill SHA + model version + scrape snapshot hash. Disagreements between LLM output and your brother's judgment update the relevant skill file. **This is the training loop.**

Nothing about this changes from v1. It's just genuinely later than v1 implied.

---

## 9. Methodology — how we work

This section is new. The probe and inventory work exposed patterns worth codifying.

**Verify before building.** Secondary sources lie (the TPU mappings disaster cost us a working assumption). Whenever a design rests on a fact about external data, run a probe first. The `poursuite/probes/` infrastructure exists for this.

**Don't pre-judge what's useful during inventory.** The inventory pass discovered `Histórico de classes` — a high-value section we wouldn't have asked for. Whenever we're cataloging an external system, record everything; filter later.

**Inventory before extending.** Before adding fields to a scraper, look at what the page actually exposes (not what docs claim). Apply this consistently going forward.

**Reproducibility costs nothing upfront and a lot to retrofit.** Hash inputs. Version skill files in git. Cache by content hash. Keep cache keys aware of every input version. Start now even if it feels overkill.

**Lawyer in the loop is structural, not optional.** No layer auto-decides. Even "auto-discard" buckets should be auditable — your brother can ask "show me everything you discarded and why" and get a complete answer.

**Documents drift; code is authoritative.** When ARCHITECTURE.md or PLAN.md contradicts code, fix the docs. When PLAN.md and your brother contradict, fix PLAN.md.

---

## 10. Sequencing — what ships when

Rougher than v1 by design. The probe + inventory cycle taught us that detailed multi-month sequencing predicts poorly. So: just the next few moves, each genuinely shippable.

### Now (next workstream)

**Expand the eSAJ scraper.** Add the 8 verified header fields, the foro/origem lookup-table derivations, and the Movimentações timeline extraction. Persist to the snapshot store. This is the foundation of the pre-filter.

### Next

**Build the pre-filter rule engine.** Three or four rules to start — creditor classifier, debtor process count, value tier, age-based prescrição flag. Output ranked buckets. Wire into API + CLI. Iterate with your brother on rule tuning.

### Soon after

**Layer 3 — DataJud production module.** Cross-tribunal debtor universe. Adds one strong signal to the pre-filter.

### Later

**Layer 4 — Receita ingestion + connection graph.** For cases that pass the pre-filter. Drives the lawyer-grade investigation tools.

**LLM tier.** Once we have movimentações in production + ground-truth labels.

Total time to "pre-filter shipping value": probably 4-8 weeks of focused work. Total time to "connection graph + LLM as polish": months — but the system delivers value at every step, not just at the end.

---

## 11. The standing open questions

1. **Ground truth.** Your brother has triaged many cases without documented reasoning. The LLM tier can't be evaluated without labels. Worth deciding when/how to capture them — retrospective labeling on a sample, or going-forward documentation, or both.
2. **Throughput.** Still uncertain. Shapes cost-regime decisions for the eventual LLM tier.
3. **Storage backup.** Maintenance pipeline outputs are stored on D:; if D: dies, the 677 GB DJE corpus is rebuildable but slow. Snapshots and (eventual) graph data accumulate over time and would be lost. Worth a real backup plan before Layer 4 ships.
4. **Multi-tenant.** Currently single-user (one API key). Not urgent.

---

## 12. Document maintenance

This document updates whenever:
- A workstream completes (mark as shipped; capture what we learned)
- An open question gets answered (move it to a decisions section, or just delete if it resolved cleanly)
- A new constraint or opportunity surfaces (add it; revise sequencing if needed)
- A probe disproves an assumption baked into the plan (revise the assumption — this is the most important update trigger)

When this document and `ARCHITECTURE.md` disagree, the code is authoritative; the docs follow.

When this document and your brother's expertise disagree, your brother is authoritative; the docs follow.
