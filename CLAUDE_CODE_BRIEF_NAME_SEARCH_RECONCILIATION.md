# Brief for Claude Code — DataJud Name-Search Reconciliation

## Context

The re-inventory concluded `partes.nome` name search "doesn't work" with 4 lines of evidence. This directly contradicts `TRACK_B_FINDINGS.md`: LegalSuite — a commercial product whose entire business is built on the DataJud public API — publishes a working `partes.nome` query example in its March 2026 production documentation, using the same field name and query shape the re-inventory tested.

Project methodology rule #7: don't accept confident negatives that contradict observable commercial usage without reconciling them. The re-inventory was meant to apply that discipline and instead came back confirming the negative. Before that negative becomes canonical in `docs/DATAJUD_CAPABILITY_INVENTORY.md`, the contradiction with LegalSuite must be reconciled explicitly — not left as your open question #3.

This is a small diagnostic pass, not a build. It runs in parallel with the Layer 3-lite build (separate brief). It does not block that build.

## Goal

Determine whether the public-API name-search negative is (a) genuinely real **and reconciled** with LegalSuite's documented usage, or (b) an artifact of the test that needs correcting. Update the inventory doc's name-search section to reflect the answer.

## Tasks

### 1 — Surface the existing evidence

Write out the 4 lines of evidence verbatim. For each: the exact query body sent, the index, the HTTP response, the hit count. Raw queries and raw responses, not a summary.

### 2 — Audit against the re-inventory brief's actual test design (Part 1.1)

Confirm, step by step, which of the specified protocol actually ran:

- Did it use a **real defendant string read directly from `process_snapshot`** — the exact name as eSAJ stored it (casing, punctuation, accents) — or a hand-typed / synthetic name? Track A gives known-good cases with real defendant strings: Royal Coffee, Crb Imóveis.
- Were all four variants tried (exact / lowercase / first-2-words / `match_phrase`)?
- Was the `_source` bypass tested (`_source: ["partes"]`, wildcard include, `fields`, `docvalue_fields`)? Note: search-indexability and `_source` exclusion are independent. The real test is whether `match` returns hits; a stripped `partes` in the response is expected and is not evidence either way.
- On a 0-hit at TJSP, was the **same exact name retried against TJRJ and TJMG** to localize? This is the step that separates "TJSP-specific indexing gap" from "national absence."

Report which steps ran and which didn't.

### 3 — Fill the gaps

Run every step from §2 that wasn't run, against a real snapshot defendant string. Use at least two test cases (one PJ, one PF) so a single bad name can't drive the conclusion.

### 4 — Resolve open question #3 (the actual reconciliation)

Hypothesis: LegalSuite queries an authenticated / paid / different endpoint that indexes parties, while the public `api-publica.datajud.cnj.jus.br` endpoint does not.

- Re-read LegalSuite's March 2026 documented example. What exact endpoint URL, auth header, and host does it hit? Is it `api-publica.datajud.cnj.jus.br` (the same public endpoint we use) or a different/wrapped one?
- If it's the **same public endpoint** with the same API-key scheme → their query should work on our key. Run their published example verbatim (their exact name string, their exact body) against our key. Hits → our negative is a test artifact and name search works. 0 hits → hard contradiction; escalate.
- If it's a **different endpoint / their own enriched layer** → the public-API negative is real and reconciled. Document the distinction.

### 5 — Verdict

Classify:
- **N1 — Negative is real and reconciled.** Public API doesn't index parties; LegalSuite uses a different tier. Cross-tribunal name search via the public API stays infeasible.
- **N2 — Name search works after a corrected test.** The re-inventory test was flawed (synthetic name, wrong tribunal, encoding). Document the corrected working query. Reopens Layer 3 proper.
- **N3 — Localized.** Works in some tribunals, not others. Document the per-tribunal map.

## Output

- Update the name-search section of `docs/DATAJUD_CAPABILITY_INVENTORY.md` to reflect the verdict and the LegalSuite reconciliation explicitly. If the verdict is still N1, the section must state *why* LegalSuite's documented usage doesn't contradict it (the tier distinction) — not just assert the negative.
- Raw queries + responses under the existing probe artifacts directory.
- A short written verdict (N1/N2/N3) with the evidence chain.

## Commit guidance (for the already-done re-inventory work)

- **Commit now:** `poursuite/probes/datajud_inventory.py` and the `cli.py` `inventory` subcommand wiring — reusable diagnostics, commit-worthy regardless of the verdict.
- **Hold the doc** until this reconciliation closes. Commit `DATAJUD_CAPABILITY_INVENTORY.md` only once the name-search section reflects the verdict + reconciliation. Don't canonize an unreconciled negative.

## What this is NOT

- Not a Layer 3 build (parallel brief).
- Not a re-run of the whole re-inventory — only the name-search loop and its LegalSuite reconciliation.
- Not entity resolution or false-positive analysis — downstream, and only relevant if the verdict is N2/N3.

## When done

Don't commit the doc until the verdict is in (probe infra + CLI may commit now per above). Capture findings, report back. Operator reviews the verdict alongside Layer 3-lite progress.
