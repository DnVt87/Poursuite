# Track B — DataJud Name Search Re-Investigation

_Research conducted: May 2026 (this conversation), in parallel with Track A._

## TL;DR

**Probe B's negative finding is almost certainly wrong.** DataJud does support name search via the `partes.nome` field — the same field name Probe B tried and got 0 hits on. Multiple independent sources confirm this, including LegalSuite (a commercial product whose entire business is built on the DataJud API) which publishes a working code example in their March 2026 documentation:

```bash
curl -X POST "https://api-publica.datajud.cnj.jus.br/api_publica_tjsp/_search" \
  -H "Authorization: APIKey ..." \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match": {
        "partes.nome": "Empresa Exemplo Ltda"
      }
    },
    "size": 10,
    "sort": [{ "dataAjuizamento": { "order": "desc" } }]
  }'
```

This is exactly Probe B's variant 2 (`match.partes.nome`), which returned 0 hits in the probe run.

The contradiction means one of two things:
- **Probe B made a query-specific mistake** (encoding, test case name not actually indexed, wrong API key, transient API state, response interpretation error)
- **LegalSuite's documentation is wrong** — they published a query example that doesn't work despite their business depending on it

The second is implausible. LegalSuite serves real customers monitoring real processes; broken example queries would generate immediate support tickets.

## Evidence

### Source 1 — LegalSuite (March 31, 2026)

LegalSuite explicitly states (with code examples):

- The `partes` array IS a returned field with `nome` and `tipo` (ativo/passivo/outros) sub-fields
- `partes.nome` is a searchable field via `match` query
- Direct quote on the limitation: "O DataJud armazena nomes das partes, mas não indexa CPF/CNPJ diretamente nos campos pesquisáveis da API pública (por questões de privacidade LGPD). É possível buscar por nome da parte e depois filtrar por outros critérios no resultado."

The limitation is **CPF/CNPJ not indexed for search** — name search works fine.

### Source 2 — Judit (commercial competitor of DataJud, April 2026)

Judit, a paid alternative, describes DataJud's limitations to position their own product. The framing:

- "Suporte inconsistente por CPF/CNPJ. Cobertura incompleta em vários tribunais."

Note the word "inconsistente" — not "ausente." This implies search by CPF/CNPJ exists in some tribunals but is unreliable; nothing similar is said about name search.

### Source 3 — CNJ official documentation

The Portal CNJ and Wiki page both describe protecting "informações das partes envolvidas" — protection, not absence. The Portaria 160/2020 governs what's filtered; party data is restricted but not invisible. Tellingly, the Wiki notes both "capas processuais" and "movimentações" as accessible, where capas processually include parties.

The Wiki's "Glossário de Dados" page would be the canonical schema reference but my fetch returned redirects. Worth Code grabbing it directly from his machine.

## What likely went wrong with Probe B

Several reasonable hypotheses for Probe B's 0-hits result, in rough likelihood order:

1. **Test case name not actually indexed.** The probe used `"BARUK Joias e Presentes Ltda."`. If the name as stored in the index is slightly different (capitalization, punctuation, abbreviation), the `match` query may not score it as a hit. Elasticsearch `match` queries are analyzed — they apply tokenization, lowercasing, and stemming. But edge cases (special characters like `.`, accented characters, exact-text mismatch) can produce 0 hits where a slightly different name would work.

2. **Encoding/special characters.** "BARUK Joias e Presentes Ltda." has a period at the end. Brazilian Portuguese accents (joias, presentes) may interact poorly with the analyzer if the indexing used a different normalization than the query.

3. **Index-side gaps.** TJSP may have inconsistent indexing of `partes.nome` for some classes/dates of cases. The test case existing in TJSP doesn't mean its `partes` field is fully populated.

4. **Test name unique vs. common.** Searching for a very specific PJ name returns 0 hits if the index treats `BARUK Joias` as a separate token sequence than the stored value. A broader common-name test would be more diagnostic.

5. **Stale/transient API state.** Possible but unlikely to affect 5 variants consistently.

## What needs to happen

A new Probe B variant — call it Probe B' — that re-tests name search with the corrected understanding. Specifically:

1. **Pick a TJSP case from the existing snapshot store.** Read its `defendant` field directly from `process_snapshot`. That gives you the *exact* name string as eSAJ stores it.

2. **Query DataJud `partes.nome` with that exact string** plus variants:
   - Exact: `"match": {"partes.nome": "<exact name>"}`
   - Lowercase: `"match": {"partes.nome": "<lowercase name>"}`
   - Tokenized (just first 2 words): `"match": {"partes.nome": "<first 2 words>"}`
   - Wildcard: `"match_phrase": {"partes.nome": "<exact name>"}`

3. **Compare results against what we expect.** For a TJSP case we already have, querying `partes.nome` for its defendant should return ≥1 hit (the case itself). If it doesn't, something fundamental is broken. If it does, we know the API works and Probe B just had a bad test case.

4. **If step 3 returns hits**, run the original Probe B plan with confidence: 20-30 debtor sample, common-name noise floor, false-positive analysis. The expected outcomes change substantially.

5. **If step 3 returns 0 hits even for known-existing cases**, the failure is real but not what Probe B's report said — it's a *specific* failure mode worth understanding. Possibly: TJSP doesn't index `partes` at all, while other tribunals do. Worth testing one or two other tribunals (TJRJ, TJMG) with the same exact-name approach.

## Implications for Layer 3 planning

Two possible worlds, depending on what Probe B' returns:

**World A — Name search works (most likely):** Layer 3 as scoped in PLAN.md v3.1 is back on the table. The cross-tribunal debtor universe IS buildable. The original probe brief's questions (false-positive rate on common names, confidence-scoring heuristics) become the real questions. Layer 3-tribal becomes less attractive because Layer 3 proper now exists.

**World B — Name search broken in TJSP specifically:** Worth understanding *why* and whether it works in other tribunals. If TJRJ or TJMG works, Layer 3 ships cross-tribunal-except-TJSP, with TJSP-specific data coming from the existing eSAJ scraper. Still useful, narrower scope.

**World C — Name search broken nationally:** We're back at Probe B's original finding. Layer 3 as scoped doesn't work. But this seems very unlikely given LegalSuite's documented usage.

## Recommendation

Don't accept Probe B's conclusion as final. Run Probe B' before any Layer 3 disposition decision.

Probe B' is much smaller than the original Probe B: half a day at most, mostly diagnostic. The output is binary — either name search works (proceed with full Layer 3 design) or we have a concrete narrowed-down failure mode (different design call).

Track A (the `other_processes` investigation) proceeds in parallel as already planned. Both close in ~1 day, then the operator has full information to decide Layer 3 disposition.

## What surprised me

The party-stripping finding from the May 2026 probe — "responses strip parties, 5-way verified" — does NOT contradict name search working. The index can index parties for search while filtering them out of returned results. That's a normal Elasticsearch pattern (`_source` exclusion vs. searchability are independent). I was anchored on the assumption that "stripped from response" implied "not indexed at all"; that's wrong.

The right model is: **DataJud knows who the parties are (indexed for search), but won't tell you who they are (filtered from responses).** You can find cases by name; you can't ask "what's the name on this case." For Layer 3's "find all cases where João Silva is a defendant," that's actually fine — you query by name, get process numbers back, then scrape eSAJ for the full party data.

This is materially better than what we thought yesterday.
