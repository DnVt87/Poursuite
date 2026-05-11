# Probe Findings — DataJud & Receita Federal CNPJ

> Reference document. Captures what is currently known about the two highest-leverage external data sources for Poursuite (DataJud + Receita CNPJ) based on official documentation, the open-source community's accumulated experience, and worked examples found on GitHub. **Empirical gaps are flagged explicitly** so the companion exploration tool (`poursuite/probes/`) can fill them.
>
> Last updated: May 2026. Sources: CNJ Datajud Wiki, Receita Federal `cnpj-metadados.pdf`, the `rictom/cnpj-sqlite` and `caiopizzol/cnpj-data-pipeline` open-source projects, public tutorials and worked examples. Where research and code disagree, the code is authoritative.

---

## Part 1 — DataJud (CNJ Public API)

### 1.1 What it is

DataJud is the CNJ's national database of process metadata across all 91 Brazilian tribunals. The public API exposes capa (header) data and movimentações for non-sealed processes. It's what every "monitoramento de processos" SaaS in Brazil is built on top of.

For Poursuite specifically, this is potentially huge: cross-tribunal coverage for any debtor, in a single API, replacing what would otherwise be 91 separate Selenium scrapers.

### 1.2 Access

- **Endpoint pattern**: `https://api-publica.datajud.cnj.jus.br/api_publica_{sigla}/_search`
- **TJSP specifically**: `https://api-publica.datajud.cnj.jus.br/api_publica_tjsp/_search`
- **Method**: POST (the older docs say GET; current practice is POST)
- **Content-Type**: `application/json`
- **Authentication**: header `Authorization: APIKey <key>`
- **Current public key** (as of research date): `cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw==`
- **Key rotation**: The CNJ rotates this without notice. Always pull the current value from the wiki at https://datajud-wiki.cnj.jus.br/api-publica/acesso/ before failing diagnostics.
- **Query language**: Elasticsearch DSL. Familiar `query.match`, `query.bool`, `query.range`, `sort`, pagination via `from`/`size` or `search_after`.
- **Page-size cap**: 10,000 records per query (Elasticsearch default ceiling). For larger pulls, use `search_after`.
- **Rate limits**: documented as "moderate" but exact values are not published in the wiki. Practitioner reports suggest hundreds of requests per minute work; aggressive scanning can get a key suspended temporarily.

### 1.3 Response shape (the published API)

The shape exposed by the public API is **not the same** as the shape DataJud stores internally (the CNJ MTD/MNI model). The public API strips party data per Portaria 160/2020. Below is the actual public-API shape, derived from a worked TJDFT example (TJSP follows the same pattern; tribunal-specific variations noted below).

Top-level Elasticsearch envelope:
```json
{
  "took": 213,
  "timed_out": false,
  "_shards": { ... },
  "hits": {
    "total": { "value": <int>, "relation": "eq"|"gte" },
    "max_score": <float>,
    "hits": [ { "_index": "...", "_id": "...", "_score": ..., "_source": { ... } }, ... ]
  }
}
```

Each hit's `_source` (the real per-process payload) carries:

| Field | Type | Notes |
|---|---|---|
| `numeroProcesso` | string | CNJ format with no separators (e.g. `07223914020178070001`). **TJSP returns it without dots/dashes too.** |
| `classe` | object `{codigo, nome}` | TPU code + human-readable name (e.g. `{1116, "Execução Fiscal"}`) |
| `assuntos` | array of `{codigo, nome}` | TPU subject codes |
| `sistema` | object `{codigo, nome}` | Origin system: PJe, eSAJ, etc. |
| `formato` | object `{codigo, nome}` | Físico vs. Eletrônico |
| `tribunal` | string | E.g. `"TJSP"` |
| `grau` | string | `"G1"`, `"G2"`, `"JE"`, etc. |
| `dataAjuizamento` | ISO datetime | Filing date in UTC |
| `dataHoraUltimaAtualizacao` | ISO datetime | DataJud's last sync — **not** the case's last activity in the tribunal |
| `@timestamp` | ISO datetime | Index-side metadata |
| `orgaoJulgador` | object `{codigo, codigoMunicipioIBGE, nome}` | Vara/comarca |
| `nivelSigilo` | int | 0 = público; higher = sigiloso. Sealed cases may not return at all. |
| `movimentos` | array | The timeline. See below. |

What is NOT in the public API response (despite being in the underlying base):
- `partes` — names, CPF/CNPJ. **Stripped for LGPD compliance.**
- `documentos` — the document tree (titles, signatories, ids). Stripped.
- `valorCausa` — present in the internal MTD model, but routinely empty in TJSP public-API responses (needs empirical verification).
- `justicaGratuita`, `liminar` — present in some tribunals' responses, often missing for TJSP.

### 1.4 The `movimentos` array

This is where most of the recovery-relevant signal lives. Each entry:

```json
{
  "codigo": 11382,
  "nome": "Bloqueio/penhora on line",
  "dataHora": "2022-07-15T10:23:11.000Z",
  "complementosTabelados": [
    { "codigo": 2, "valor": 2, "nome": "sorteio", "descricao": "tipo_de_distribuicao_redistribuicao" }
  ]
}
```

`codigo` indexes into the **Tabela Processual Unificada (TPU)** — the CNJ's national catalog of standardized movement codes. A small selection of codes that matter for Poursuite triage:

| Código | Nome | Why it matters |
|---|---|---|
| 26 | Distribuição | Initial filing |
| 51 | Recebimento | Case received by court |
| 60 | Expedição de documento | Generic admin |
| 11383 | Penhora | Asset attachment — high-value signal |
| 11382 | Bloqueio/penhora on line | BACEN-JUD / SISBAJUD bank attachment |
| 12284 | Citação | Defendant served — required before execution can really proceed |
| 22 | Baixa | Case closed |
| 246 | Arquivamento definitivo | Permanent archive |
| 11373 | Anulação de sentença/acórdão | Higher court overturned ruling |
| 11025 | Suspensão ou Sobrestamento | Stayed — interesting for prescrição analysis |
| 14702 | Incidente ou Cautelar — Procedimento Resolvido | Possibly IDPJ-related |
| 12735 | Extinção da punibilidade | Criminal-only; ignore |

The full TPU table is large (thousands of codes). It's authoritative and downloadable from CNJ. We'll need to vendor it.

**Empirical gaps** (need probe to confirm):
1. Are TJSP execução cases populated with **all** their movimentos via DataJud, or is it truncated/partial?
2. Does `complementosTabelados` consistently carry useful detail, or is it usually empty for TJSP?
3. Are there TJSP-specific movement codes (`local`-defined, not in the national TPU) that DataJud preserves?

### 1.5 Limitations to internalize

**Latency.** DataJud is updated by tribunals on a schedule, not in real time. Practitioner reports range from **hours to weeks** behind reality. TJRS users on the wiki forum report "muitos processos com dados desatualizados". This means:
- DataJud is a **historical metadata source**, not a real-time monitor.
- For real-time penhora detection, eSAJ scraping is still required.
- For "give me everything I missed in the last month across all 91 tribunals," DataJud is unmatched.

**Latency is feature-not-bug for Poursuite specifically.** Worth flagging because it inverts the usual legaltech framing. Poursuite hunts cases that have gone quiet — distressed credit where no creditor is actively pursuing. A case DataJud reports as "last updated 6 months ago" is the profile we *want*. A case DataJud reports as "updated yesterday" likely means an active creditor or lawyer is on it, and the recovery margin is thinner. So DataJud's staleness pre-filters the universe in the right direction for our use case. The downside (missing real-time signals like a fresh penhora) matters less when the cases that interest us aren't moving anyway.

**No CPF/CNPJ search.** Per LGPD/Portaria 160, party identifiers are not indexed in the public API. You can search by:
- `numeroProcesso` (exact or fuzzy match)
- `classe.codigo`, `assuntos.codigo`
- `dataAjuizamento` ranges
- `orgaoJulgador.codigo`

You **cannot** search "give me all processes where CNPJ X is a party." That use case requires either eSAJ (party-name search, then deduplication) or a private commercial source.

**No DJe text.** DataJud has metadata but does not include the full text of publications (intimações, despachos as published). Poursuite's existing 677 GB DJE corpus remains the only practical source for that.

**Sigilo.** Processes with `nivelSigilo > 0` (segredo de justiça) typically return no `_source` at all, or return with most fields nulled out. The existing scraper's behavior of detecting and skipping these maps cleanly: if DataJud doesn't return a process you know exists, it's either sigiloso or hasn't been indexed yet.

**Coverage edges.**
- **Pre-2000 processes** (like `0700505-13.1990.8.26.0547` in the Poursuite sample): coverage is uneven. Some tribunals retroactively migrated old cases to DataJud, others didn't. Empirical question.
- **Recent processes** (last 30 days): may not yet be indexed due to latency.
- **Migration status**: the field `situacaoMigracao` (`NATIVO`, `IMIGRANTE`, `EMIGRANTE`) tells you whether the case was born in PJe/eSAJ or migrated from a legacy system. Migrated cases sometimes have truncated movimentação history.

### 1.6 How DataJud fits into Poursuite's architecture

This is a substantial architectural decision and deserves a short discussion.

DataJud is **a complementary source, not a replacement** for either of Poursuite's existing data flows:

| Need | Best source |
|---|---|
| Full text of DJE publications | Existing 677 GB DJE corpus (FTS5) |
| Real-time process status (last 24h) | eSAJ Selenium scraper |
| Header data for known process numbers | eSAJ scraper (current) — DataJud could replace, but with latency |
| Movimentações timeline for known process numbers | eSAJ deep-scrape (planned) **OR** DataJud — DataJud is faster but less fresh |
| **Cross-tribunal universe of a debtor** | **DataJud (only practical option)** |
| Bulk demographics ("how many execuções fiscais filed in TJSP in 2024 by orgão") | **DataJud (only practical option)** |
| Party/CPF search | None of the above; eSAJ by name with manual dedup |

The clear new-capability unlock from DataJud is **cross-tribunal queries** and **bulk metadata pulls**. A debtor "JOÃO DA SILVA" might have 3 processes in TJSP, 1 in TJRJ, 2 in TRT-2 — DataJud can find all of these in one query (subject to the name-search caveats and false-positive risk for common names). Today Poursuite cannot see beyond TJSP at all.

The secondary unlock is **cheap movimentação ingestion**: instead of building Selenium scraping for the eSAJ Movimentações tab from scratch, we can pull the same data (with latency caveats) from DataJud for free. This may turn out to be the cheaper path for Layer 1 triage even before considering cross-tribunal use.

### 1.7 Open empirical questions for the probe

Listed in priority order. Each one needs to be answered against the actual sample provided by the user, not from documentation.

1. **Coverage of the 6 sample numbers.** Does the public API return data for all 6? Especially the 1990 case and the 2024 case (oldest and newest extremes).
2. **Per-process field completeness.** For each of the 6, which fields in §1.3 are populated vs. null/missing? Is `valorCausa` ever populated for TJSP? `orgaoJulgador.codigo` consistent?
3. **Movimentações depth.** How many movimentos per process? What's the date range from first to last movimento? Does it match the eSAJ-visible timeline length?
4. **Latency.** For the most recent case (1002273-51.2024.8.26.0629), what's the gap between DataJud's `dataHoraUltimaAtualizacao` and today? Compare to eSAJ's last-movement date.
5. **Penhora detection.** Do any of the 6 cases have `codigo` 11382 or 11383 in their movimentos? If so, can we read enough from `complementosTabelados` to know what was attached?
6. **Sealed handling.** If any sample is sealed, what does the response look like? Empty hits, or partial response with nulled fields?
7. **TPU table currency.** Is the TPU table on cnj.jus.br stable enough to vendor, or do new codes appear month-over-month?

---

## Part 2 — Receita Federal CNPJ (Dados Públicos)

### 2.1 What it is

Receita Federal publishes **the entire active-CNPJ database of Brazil** — roughly 60 million records — as bulk monthly downloads. Includes razão social, sócios, addresses, CNAE, capital social, situação cadastral, partner qualifications, Simples Nacional status. This is foundational infrastructure for any corporate-graph analysis in Brazil.

For Poursuite: this is the data that makes the Layer 2 connection graph possible. Without it, "find all companies where this debtor is a partner" is unanswerable from public data alone.

### 2.2 Access (post-Jan 2026 layout change)

**Critical note:** RFB migrated the data publication infrastructure in January 2026. The old fixed URL pattern at `arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/` is deprecated. The current setup is:

- Files now hosted on a **Nextcloud** instance, accessed via **WebDAV**.
- Direct paths are not stable; the canonical entry point is the dataset page at `https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj`.
- Primary catalog page: `https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj`.
- Reference shortlink (Nextcloud): `https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9`.
- Update cadence: **monthly**, typically released between days 12-15 of each month covering the prior month's snapshot.

The post-Jan-2026 download mechanism is sufficiently new that we should not roll our own download code. The community reference is `caiopizzol/cnpj-data-pipeline` — actively maintained, supports the new WebDAV flow, and offers a Parquet output mode that doesn't require Postgres. The `rictom/cnpj-sqlite` tool was updated in March 2026 with the same fix and is the SQLite-targeted alternative.

**Recommendation: don't reinvent the download.** Use one of these two as the foundation.

### 2.3 File partitions and sizes

The dataset is split into multiple zip files by category:

| Category | Files | Compressed total | Notes |
|---|---|---|---|
| Empresas | 10 partitions (Empresas0.zip ... Empresas9.zip) | ~700 MB | Matriz-level data (razão social, capital, natureza jurídica) |
| Estabelecimentos | 10 partitions | ~3.5 GB | Per-establishment data (endereço, CNAE, telefone, situação) — the bulk of the dataset |
| Sócios | 10 partitions | ~700 MB | Partner relationships |
| Simples | 1 file | ~250 MB | Simples Nacional / MEI status |
| CNAEs | 1 small file | <1 MB | Reference table |
| Motivos | 1 small file | <1 MB | Cancellation reasons |
| Municípios | 1 small file | <1 MB | IBGE codes |
| Naturezas Jurídicas | 1 small file | <1 MB | Legal forms |
| Países | 1 small file | <1 MB | Country codes |
| Qualificações de Sócios | 1 small file | <1 MB | Partner role codes |

**Total**: ~5 GB compressed, ~30 GB SQLite after processing (per `rictom/cnpj-sqlite` README; tested on 2024 data).

### 2.4 Schema (key tables)

From the official `cnpj-metadados.pdf` and the open-source community's tested loaders:

**`empresas`** — one row per *matriz* CNPJ root (first 8 digits)
- `cnpj_basico` (8 chars) — primary join key
- `razao_social`
- `natureza_juridica` (FK → `naturezas_juridicas`)
- `qualificacao_responsavel`
- `capital_social` (numeric, BRL)
- `porte_empresa` (1=Não informado, 3=ME, 5=EPP, 0=Demais)
- `ente_federativo_responsavel` (only populated for órgãos públicos)

**`estabelecimentos`** — one row per individual establishment (matriz or filial). This is the **biggest table** and the one with the richest data:
- `cnpj_basico`, `cnpj_ordem` (4 chars), `cnpj_dv` (2 chars) — together = full 14-digit CNPJ
- `identificador_matriz_filial` (1=matriz, 2=filial)
- `nome_fantasia`
- `situacao_cadastral` (1=Nula, 2=Ativa, 3=Suspensa, 4=Inapta, 8=Baixada)
- `data_situacao_cadastral`
- `motivo_situacao_cadastral` (FK → `motivos`)
- `nome_cidade_exterior`
- `pais` (FK → `paises`)
- `data_inicio_atividade`
- `cnae_fiscal_principal` (FK → `cnaes`)
- `cnae_fiscal_secundaria` (CSV-encoded list)
- `tipo_logradouro`, `logradouro`, `numero`, `complemento`, `bairro`, `cep`, `uf`, `municipio` (FK → `municipios`)
- `ddd_1`, `telefone_1`, `ddd_2`, `telefone_2`, `ddd_fax`, `fax`
- `correio_eletronico` (email)
- `situacao_especial`, `data_situacao_especial`

**`socios`** — partner relationships, ~20M rows
- `cnpj_basico` — links to `empresas`
- `identificador_socio` (1=PJ, 2=PF, 3=Estrangeiro)
- `nome_socio` (or `razao_social` if PJ)
- `cnpj_cpf_socio` — **CPF is masked**: only middle 6 digits visible (e.g. `***123456**`). CNPJs of PJ-sócios are full.
- `qualificacao_socio` (FK → `qualificacoes_socios`)
- `data_entrada_sociedade`
- `pais` (when estrangeiro)
- `cpf_representante_legal` (also masked)
- `nome_representante_legal`
- `qualificacao_representante_legal`
- `faixa_etaria` (1-9 age band; helps identify adults vs. minors as fronts)

**`dados_simples`** — Simples Nacional / MEI
- `cnpj_basico`, `opcao_simples` (S/N), `data_opcao_simples`, `data_exclusao_simples`, `opcao_mei`, `data_opcao_mei`, `data_exclusao_mei`

### 2.5 Critical limitations

**1. No historical sócios.** This is the single biggest data limitation. The bulk dataset shows **only the current state of partnerships**. If Person X was a sócio of Company Y from 2018-2022 and exited just before the execução was filed, they are **invisible** in the Receita bulk data — the row simply isn't there.

For historical sócio data, the only public sources are:
- **JUCESP Online** (and other Junta Comercial systems), which do show historical changes via Ficha Cadastral Completa. Requires authentication and produces PDF output.
- **Some commercial aggregators** (Casa dos Dados, Cnpj.biz, Receitaws) sometimes preserve historical snapshots, but their TOS often forbid bulk consumption.

This limitation directly affects Poursuite's investigation use case: "X declared bankruptcy and his wife immediately registered a new company" requires historical sócios data that Receita bulk cannot provide.

**2. CPFs are masked.** Only the middle 6 digits of any CPF are exposed (`***123456**`). This is sufficient to match a known CPF (you need all 11 digits to verify a hit), but not to look up unknowns.

CNPJs are fully exposed.

**3. The "Identificador de Sócio Estrangeiro" column.** Estrangeiros (foreign nationals) appear in `socios` with `identificador_socio = 3` and no CPF — only `nome_socio` and `pais`. Identity resolution for these is harder; common in companies with foreign capital.

**4. Address normalization.** Addresses in `estabelecimentos` are reasonably clean but not normalized for graph use. Same physical building shows up as multiple distinct addresses due to:
- Different `complemento` values ("SALA 1", "SALA 01", "SL 1")
- `tipo_logradouro` variations (RUA vs. R)
- CEP changes over time
- Typos

For the connection graph's "same address" edges, the canonical practice (per the `cnpj_links_ete.db` derivative in `rede-cnpj`) is to key on `(cep, numero)` — or just `cep` for very large buildings — rather than the full address string. **This is identity resolution, and it's hard.** Plan for it accordingly.

**5. Update lag.** Monthly cadence means Receita data is up to 30 days stale on the day it's published, and up to 60 days stale by month-end. For freshness-critical lookups, a separate query against the live "consulta CNPJ" page on receita.gov.br (paywalled or rate-limited; not bulk) would be needed.

**6. Data quality.** The community's accumulated experience is that the data is reasonably clean compared to other Brazilian gov datasets but has known issues:
- Addresses with typos
- A small number of records with corrupted encoding or bad delimiters
- Occasional truncation on very long razão social fields
- Estabelecimento `situacao_cadastral` lag — companies marked "Ativa" that have been informally inactive for years

### 2.6 The CNPJ-changes-format event (July 2026)

**This deserves prominent flagging.** Receita Federal is changing the CNPJ identifier format itself:

- **Before**: 14 numeric digits (`12.345.678/0001-90`)
- **From July 2026**: alphanumeric (mix of letters and numbers in the first 12 positions; check digits remain numeric)
- **Existing CNPJs are NOT changed** — only newly-issued ones use the new format.
- **All systems handling CNPJ input/storage must be updated** to accept letters in those positions.

The impact for Poursuite:
- Any CNPJ-validation regex must be loosened (currently "14 digits" → "12 alphanumerics + 2 digits")
- Any database column typed as numeric must become text
- Any join logic on CNPJ assumes string equality, not numeric — verify
- Receita's bulk data layout *may* change again to accommodate this; we should expect another schema event in mid-2026

The 2-month timeline (May → July 2026) means **anything we build now needs to be alphanumeric-ready from day one**. Don't ship a system in June 2026 that breaks on day one of July.

### 2.7 Recommended ingestion approach

Based on the community state-of-the-art, our path of least resistance is:

1. **Use `caiopizzol/cnpj-data-pipeline`** as the download + initial-processing layer. Run it monthly via Docker; output to Parquet (no Postgres dependency).
2. **Write our own loader** from Parquet into the Poursuite SQLite shards (or alongside them). This loader is the place to:
   - Apply CNPJ alphanumeric-readiness from the start
   - Index the columns we'll actually query (cnpj_basico, nome_socio normalized, cep, etc.)
   - Add Poursuite-specific derived columns if needed
3. **Don't try to re-download every month from scratch.** The pipeline supports incremental loads; honor that.
4. **Consider a "snapshot history" approach** for sócios specifically — preserve each month's `socios` table separately so we can detect "Person X was sócio in March, no longer in April". This partially mitigates limitation #1 above (no historical sócios), but only going forward.

The third-party tool `rede-cnpj` produces a useful derivative `cnpj_links_ete.db` (links by endereço, telefone, email). Worth either using directly or borrowing the SQL that builds it.

### 2.8 Open empirical questions for the probe

1. **Verify current download URLs.** The Nextcloud paths are stable in pattern but need confirmation each run. The probe should fetch the latest catalog and emit the resolved file URLs.
2. **Sample row content per partition.** Pull a small slice (say 1000 rows) from each of the 4 main tables and confirm the field layouts match this document. Especially after the next monthly publication, since the Jan 2026 layout change may not yet be fully stable.
3. **CPF masking format.** Confirm the exact mask pattern (`***123456**` vs. `12.***.***-90` vs. something else). Critical for matching code.
4. **Address quality on a sample.** Pull 100 random rows from `estabelecimentos`, eyeball the address fields. How often is `complemento` populated? `cep` valid format? `numero` numeric or text?
5. **Cross-table integrity.** Pick 10 random `cnpj_basico` from `empresas`, verify they have at least one row in `estabelecimentos`. Are there orphans?
6. **Sócios masking on PF entries.** Confirm CPF is masked but `nome_socio` is full. Check `cpf_representante_legal` masking too.
7. **Encoding sanity.** Sample BRL accent-heavy fields (razão social, nome do sócio) — UTF-8 clean, or any latin-1 leakage?
8. **Active vs. inactive density.** What fraction of `estabelecimentos` have `situacao_cadastral=2` (active)? Helps size the data we actually care about.

---

## Part 3 — How These Fit Together for Poursuite

A short orientation on how the pieces compose, since the rest of the project planning depends on this.

### 3.1 The data layers we'll have once both are ingested

```
                    ┌──────────────────────────────────────┐
                    │  DJE Corpus (existing)               │
                    │  Full text of TJSP publications      │
                    │  from ~2013 to present, ~677 GB      │
                    │  FTS5 indexed, paragraph-grained     │
                    └──────────────────────────────────────┘

                    ┌──────────────────────────────────────┐
                    │  DataJud snapshots (planned)         │
                    │  Capa + movimentações for any        │
                    │  CNJ process across 91 tribunals     │
                    │  No party data. Hours-to-weeks lag.  │
                    └──────────────────────────────────────┘

                    ┌──────────────────────────────────────┐
                    │  eSAJ deep-scrape (planned)          │
                    │  Real-time party data, full          │
                    │  movimentações, linked processes,    │
                    │  document tree. TJSP only.           │
                    └──────────────────────────────────────┘

                    ┌──────────────────────────────────────┐
                    │  Receita CNPJ (planned)              │
                    │  Empresas, sócios (current only),    │
                    │  endereços, partner graph.           │
                    │  Monthly snapshots. ~30 GB.          │
                    └──────────────────────────────────────┘
```

### 3.2 What changes for the project plan

Three updates to PLAN.md will be needed once we've run the probe:

1. **Movimentações for triage may not require eSAJ deep-scraping.** If DataJud's TJSP movimentações coverage proves complete enough, we can use it as the Layer 1 input — much cheaper and broader-tribunal — and reserve eSAJ for the freshness-critical and party-data needs.
2. **Cross-tribunal coverage becomes a Layer 1 feature.** "How many other processes does this debtor have, anywhere in Brazil?" is now answerable as a name-based DataJud query. This is genuinely new capability.
3. **Receita CNPJ should ship before historical-sócio sources.** Limitation 2.5/#1 (no historical sócios) makes Receita a partial solution for Layer 2 — but it's the partial solution that buys us 80% of the value at 20% of the effort, and the right thing to ship first.

### 3.3 The CNPJ alphanumeric event is on the critical path

July 2026 affects every system that touches CNPJ. Anything we ship in May/June must be alphanumeric-ready. Anything we postpone past July 2026 is shipped into a different data world. Plan accordingly.

---

## Part 4 — What the Probe Tool Should Do

The companion tool (separate brief, to be built under `poursuite/probes/`) should:

1. **For DataJud**: take a list of process numbers, query each, dump the full response, and produce a structured summary report (per-process: hits/no-hits, fields populated, movimentos count and date range, presence of penhora codes). Run against the user's 6 sample numbers + a small generated set covering edge cases.

2. **For Receita CNPJ**: download the current monthly partition slice (one of each main file), load into a probe-only SQLite, run sample queries verifying the schema matches §2.4, and report any drift.

3. **Be reusable**: live under `poursuite/probes/` as a maintained diagnostic tool, not a throwaway script. When DataJud changes its key, when Receita's monthly format drifts, when we want to debug "why is this case showing strange data," the probe is what we re-run.

The probe brief is in `CLAUDE_CODE_BRIEF_PROBE.md`.
