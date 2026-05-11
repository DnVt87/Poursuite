# Brief for Claude Code — eSAJ Inventory: Viewport Fix + Gap-Filler Re-run

## Background

The first inventory run (`esaj_inventory_20260510T112631Z`) completed in under a minute and produced what looked like complete output. **It wasn't.** Operator review of the screenshots revealed the walker rendered the mobile/responsive view of eSAJ, not the desktop view. Specifically: the screenshots show the narrow vertical layout with hamburger menu, one field per line, and stacked sections — not the wider desktop panel layout the production scraper sees.

This invalidates a chunk of the previous run's conclusions:

- Section content row counts and text lengths reflect mobile rendering, not real data depth
- The "no tabs, just `<h2 class='tituloDoBloco'>` blocks" structural finding may be a mobile-view artifact
- Sub-scraper link counts (1,062 internal_anchors etc.) likely reflect mobile-view link multiplication

Header field set, existence of the six structural sections, and the `#linkmovimentacoes` collapsible mechanics remain trustworthy — those exist in both views, just laid out differently. The rest needs to be re-measured.

## Goal

1. Diagnose and fix the mobile-view rendering issue.
2. Re-run a one-case headed sanity check, side-by-side with a manually-opened browser at the same URL, to confirm parity.
3. Once confirmed, re-run the full inventory with gap-fillers added for the unfilled archetypes.

## Part 1 — Viewport / rendering fix

Likely causes (investigate in order):

1. **Viewport dimensions.** Selenium's default window may be narrow enough to trigger eSAJ's mobile breakpoint. Check `_chrome.py` and the inventory walker — does either explicitly set window size? If not, set explicit `1920×1080` (or similar wide desktop) before navigation.
2. **User-agent.** eSAJ may serve different responses based on UA. Compare what the production scraper at `poursuite/scraper/esaj.py` uses vs. what the inventory walker uses. If the production scraper has a desktop UA and the walker doesn't, align.
3. **Headless mode side effects.** Some sites detect headless Chrome and serve degraded content. The earlier headed sanity check may have *also* been mobile because the same window size was inherited. Don't assume `--headed` alone fixes this.

Fix all three to be safe. They're cheap to address together.

After the fix: the walker's screenshots should look like the production desktop view — wider layout, side-by-side panels, all section headers and content visible without scrolling tricks.

## Part 2 — Verify before re-running

Before kicking off the full inventory:

1. Run the walker in headed mode against ONE case from the existing sample list (suggest `1033164-10.2022.8.26.0602` for continuity).
2. Manually open the same case URL in a fresh Chrome window at the operator's standard window size.
3. Side-by-side compare:
   - Screenshot dimensions and layout (the walker's screenshot should match the manual browser's appearance)
   - Header field count (should be ≥10, matching the production scraper)
   - Section structure (the desktop view may have tab-like UI the mobile view collapsed into stacked blocks — capture whatever's there)
   - Any sub-elements that didn't appear in the previous mobile-view run

If parity is achieved → proceed to Part 3. If not → diagnose further before continuing.

## Part 3 — Add gap-fillers and re-run

The previous run's `archetype_coverage.json` flagged 6 unfilled archetypes:

1. Cumprimento de sentença
2. Execução fiscal
3. Multi-party (>2 polos)
4. Sealed (segredo de justiça)
5. Recent case still in citação phase
6. Case with active penhora movimento

For each, find a representative public TJSP case and add it to `poursuite/probes/esaj_inventory_samples.yaml` with `(gap-filler)` annotation and target archetype. **Verify each candidate actually fits the archetype before committing it** — load the page once, confirm visually, then add.

Sourcing strategy:

- **Cumprimento de sentença**: any TJSP case where the consulta page shows class = "Cumprimento de Sentença". Common in capital comarcas.
- **Execução fiscal**: usually has Fazenda Pública or similar as polo ativo. Try Foro das Execuções Fiscais Estaduais or Municipais.
- **Multi-party (>2 polos)**: cases with consórcios, solidary co-defendants, or multiple plaintiffs. Construction-industry executions often have these.
- **Sealed**: try execuções that show "Segredo de Justiça" — common in family-law adjacent execuções.
- **Recent in citação phase**: a 2025–2026 case where movimentos contain "Citação" but not "Penhora" or "Bloqueio."
- **Active penhora movimento**: any case where movimentos contain "Penhora" or "Bloqueio". Easier to find in older cases.

If any archetype is hard to source publicly, document the difficulty and proceed with what you have. Don't block on completeness — partial gap-filling is still better than the previous run's coverage.

After re-running, the new `inventory_report.md` should show the corrected:

- Section 5 comparison matrix with meaningful `≠` semantics (don't mark unique-by-design fields like Distribuição or Controle as "differs" — that produces noise; restrict `≠` to fields where similarity across cases is informative)
- Section 7 reconciled gap list — compare walker output against `FIELD_MAPPINGS` in `poursuite/scraper/esaj.py` and produce ONLY the fields the walker found that the production scraper doesn't extract. Below is the mapping the operator already worked through; verify and use as ground truth:

| eSAJ raw label | Production scraper field | Status |
|---|---|---|
| `Classe` | `class_type` | already extracted |
| `Assunto` | `subject` | already extracted |
| `Distribuição` | `initial_date` | already extracted |
| `Valor da ação` | `value` | already extracted |
| `Foro` | — | gap |
| `Vara` | — | gap |
| `Juiz` | — | gap |
| `Controle` | — | gap |
| `Outros assuntos` | — | gap |
| `Outros números` | — | gap (rare) |
| `Local Físico` | — | gap (rare) |
| `Área` | — | gap (always "Cível" in sample, possibly low-value) |

Also note: production scraper extracts `last_movement` and `status` from `#maisDetalhes`. Confirm the walker captures these (it should, after the viewport fix exposes the full panel).

## Constraints

- **Don't touch the runtime package** (`poursuite/api/`, `poursuite/db/`, `poursuite/cli.py`) for anything other than the `_chrome.py` viewport fix if needed. The shared `_chrome.py` helper is fair game for adjustments since it serves both probe and runtime.
- **Don't filter by perceived utility.** If the desktop view exposes additional fields the mobile view didn't, record them all — even if they look low-value.
- **Don't auto-add discovered fields to the production scraper.** Inventory work only.
- The archetype-classifier in the previous run was unreliable (tagged everything as having embargos/linked processes). Either fix the classification logic or just record raw observations and skip the auto-tagging.

## Definition of done

- Mobile-vs-desktop diagnosis written up with root cause and fix
- Single-case headed sanity check run with manual browser parity confirmed
- 6 gap-filler cases added to `esaj_inventory_samples.yaml`, each verified to fit its archetype
- Full inventory re-run on all 17 cases (11 original + 6 gap-fillers)
- New `inventory_report.md` with:
  - Corrected Section 5 comparison matrix (no noise from unique-by-design fields)
  - Corrected Section 7 gap list (using the table above as ground truth)
  - Honest archetype coverage check
- Brief written summary at the end: what changed between the two runs, what the desktop view exposed that mobile didn't, any anomalies in the gap-filler cases

## Future, not now (queued for later)

- **Foro/origem lookup table.** Three fields (Foro, Área, year-of-Distribuição) are partially deducible from the CNJ process number itself: `OOOO` (last 4 digits) = origem code → maps to foro/comarca via a CNJ-published lookup; `J` = segmento da justiça → constrains área; `AAAA` = year. Worth a small lookup table in the eventual production scraper to derive these without scraping. Operator confirmed this is wanted but it's not part of this brief — record as a future task.

## When done

Don't commit. Capture artifacts, report back. We'll review the corrected inventory and decide what production-scraper changes are warranted.
