# Brief for Claude Code — eSAJ Full Inventory Pass

## Goal

Catalog **everything** that TJSP's eSAJ "Consulta de Processo" page exposes for a process — every field, every tab, every link, every conditional render, every clickable element that leads somewhere. Produce a comprehensive inventory. **Do not decide what's useful.** That decision belongs to the operator after reviewing the inventory.

The current `poursuite/scraper/esaj.py` extracts ~10 header fields. We suspect significant signal is being left on the table, but we don't yet know what's there or what shape it takes. This pass answers that.

## Methodology — discover, don't assume

This is critical. Common failure mode: enter the inventory with a mental list of "fields that matter" and ignore everything else. **Do not do this.**

Instead:
- Walk the page systematically, top to bottom, tab by tab.
- For every visible element, record it: header field, tab content, expandable section, popup, modal, tooltip, link target.
- Record content even when meaning is unclear. "Field labeled `Indicador de Cumprimento de Sentença` with value `Não`" with no interpretation is more useful than skipping it because it seemed irrelevant.
- Record clickable elements as **sub-scraper candidates**: where they go, what they expose. Even if we don't follow them now, the catalog of "this page links to N other things" is itself a finding.
- Record conditional behavior: things that appear in some cases but not others. The diversity in the test set (below) is designed to surface these.
- Record visual/UI elements that *imply* structured data: badges, colored tags, icons next to fields. These usually correspond to flags/booleans in the underlying system.

## Constraints

- **Don't touch the runtime package** (`poursuite/api/`, `poursuite/db/`, `poursuite/scraper/`, etc.). This is investigation. The output is a document, not code changes.
- **Don't try to build the better scraper.** That comes later, after operator review.
- **Don't filter findings by perceived relevance.** Everything goes in the inventory. If you have an opinion about utility, put it in a separate "candidate signal" section — don't let it shape what gets recorded.
- **Reuse existing infrastructure.** Selenium driver setup from `poursuite/scraper/esaj.py` (extracted as a helper if needed). Logging via `setup_logging` from `poursuite/utils.py`.

## Test set — 10 cases provided + archetype mapping

The operator has provided the following 10 process numbers for the inventory pass:

```
1033164-10.2022.8.26.0602
1063559-02.2023.8.26.0100
1002177-13.2020.8.26.0100
1185092-25.2023.8.26.0100
1005405-88.2023.8.26.0100
1004813-32.2019.8.26.0020
1105144-39.2020.8.26.0100
1121786-92.2017.8.26.0100
1045075-07.2021.8.26.0100
1014358-56.2020.8.26.0032
```

Process numbers should be saved as `poursuite/probes/esaj_inventory_samples.yaml` for re-runnability.

These cover Poursuite's actual case mix. After running the inventory on all 10, classify each case by its observable archetype (execução de título extrajudicial, cumprimento de sentença, execução fiscal, multi-party, etc.) and note which of the conceptual archetypes from the original brief are *not* represented. The diversity-coverage list to check against:

1. Execução de título extrajudicial
2. Cumprimento de sentença
3. Execução fiscal
4. Multi-party (more than 2 polos)
5. Sealed (segredo de justiça)
6. Migrated/legacy (pre-2010)
7. Recent case still in citação phase
8. Case with active penhora movimento
9. Case with embargos à execução / linked processes
10. High-volume movimentações (>200 movimentos)

For any conceptual archetype not represented in the operator's 10, **pick a public TJSP case to fill the gap** — note explicitly that this is a gap-filler, not a Poursuite case. Sealed and pre-2010 are the most likely gaps.

The goal is to surface every conditional rendering and every distinct field shape across the full diversity of TJSP execution cases, not just Poursuite's typical mix.

## Module placement

Add as a new probe subcommand:

```
python -m poursuite.probes esaj --inventory --processes <yaml>
python -m poursuite.probes esaj --inventory --process <single>
```

New file: `poursuite/probes/esaj_inventory.py`. CLI tied into existing `poursuite/probes/cli.py`.

Output goes to `<POURSUITE_LOG_DIR>/probes/esaj_inventory_<timestamp>/`:
- `raw_html/` — saved page HTML for each case (one file per tab/sub-page)
- `screenshots/` — screenshots of each tab for visual reference
- `structured/<process_number>.json` — every captured field, hierarchically organized
- `inventory_report.md` — the human-readable consolidated inventory

## What "inventory" means concretely

For each of the 10 cases, capture:

### Page-level catalog
- URL of the consulta page
- HTTP status if not 200
- Whether the case appears (or fails for sealed/missing reasons)
- Any banners, alerts, or system messages on the page

### Header tab — every field
- The current scraper hits this tab. Compare what it gets vs. what's actually there.
- Walk every label-value pair in the panel. Don't stop at "the ones we know about."
- Record the exact label as displayed, the value, and the DOM selector that found it.
- Note conditional fields (present in some cases, absent in others).

### All other tabs / sections
For each tab visible at the top of the page (Partes, Movimentações, Petições, Documentos, Apensos, Incidentes, Audiências, etc. — list all that appear):
- Tab label as shown
- Whether it's clickable / loads new content vs. inline
- For each tab: walk its content, extract structure
- For tabs with paginated content (movimentos especially): document pagination mechanism, total count if visible
- For tabs with filters/search inputs: document them

### Clickable elements that lead elsewhere
This is the "sub-scrapers" part of the brief. Catalog:
- Process number links (linked processes — apensos, dependentes, conexos, embargos)
- OAB number links (lawyer profile pages)
- Document links (PDFs of petições, despachos)
- Party name links (some eSAJ pages link to party listings)
- Any other clickable that navigates

For each: where does it go? What's on the destination page? Don't deeply scrape destination pages — one or two examples is enough to characterize what's there.

### Visual signals (badges, tags, icons)
- Any colored tag or badge — capture text content and visual style
- Status icons next to fields (warning triangles, lock icons, etc.)
- Sealed-case rendering specifically

### Conditional behavior across the 10 cases
After all 10 are captured, produce a comparison matrix:
- Field/section name on rows
- Each case as a column
- Cell values: "present", "absent", "different (note value)", "rendered differently"

This matrix is the most valuable single output. It surfaces conditional fields that single-case inspection would miss.

## Output document structure

`inventory_report.md` should have:

1. **Methodology section.** Brief — what was scraped, what wasn't (e.g., didn't follow linked processes recursively).
2. **Per-archetype findings.** One section per case, summarizing what was distinctive about it.
3. **Comprehensive field catalog.** Every distinct field discovered across all 10 cases, with: label, exact location (tab/section), example value, presence-by-case (matrix), notes on rendering.
4. **Sub-scraper candidates.** Catalog of clickable elements that lead to additional data. For each: where it leads, what's there, what data it would unlock. NO recommendation about whether to follow it — just the catalog.
5. **Comparison matrix.** As described above.
6. **What the current scraper extracts.** Read `poursuite/scraper/esaj.py` and produce a definitive list of fields it currently captures, not from ARCHITECTURE.md but from code.
7. **Gap section.** What's in the page but not in the current scraper. **Just the list. No prioritization. No recommendations.**
8. **Anomalies.** Anything weird, unexpected, broken, or that didn't fit the framework. Surprises matter — they're often the most valuable findings.

The operator will review this report and decide what to add to the scraper. Code does not make that call.

## What NOT to do

- Don't recommend changes to `poursuite/scraper/esaj.py`.
- Don't auto-add fields to the scraper based on the inventory.
- Don't filter findings by perceived utility.
- Don't go deep on sub-pages (linked processes, OAB profiles, etc.) — characterize what's there with 1-2 examples each, then stop.
- Don't try to interpret legal meaning of unfamiliar fields — record what's displayed, let the operator interpret.
- Don't skip sealed cases. We want to know what the seal-block rendering looks like even though our scraper currently bails on these.

## Definition of done

- `python -m poursuite.probes esaj --inventory` runs against the 10 archetype cases
- Raw HTML and screenshots saved per case
- Structured JSON output per case
- `inventory_report.md` produced with all 8 sections above
- Comparison matrix included
- No code changes to `poursuite/scraper/esaj.py` or any runtime module
- Brief summary of "what surprised you during the inventory" at the end

## When done

Don't commit. Capture the findings, save the artifacts, and report back. The operator will review the inventory and decide which fields are worth extracting in production.

## A note on judgment

The point of this brief is to push back against the natural tendency to filter information based on assumed relevance. That tendency has cost us before in this project (community-sourced TPU mappings turned out to be wrong because we didn't verify; party-stripping turned out to be a hard wall only because the probe explicitly tested it). The discipline this time: **record everything, judge nothing, let the data speak.**
