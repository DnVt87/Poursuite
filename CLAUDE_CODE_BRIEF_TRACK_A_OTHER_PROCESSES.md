# Brief for Claude Code — Track A: `other_processes` Investigation

## Goal

The Phase 3 + Layer 3 probe surfaced that `process_snapshot.other_processes > 1` in 0 of 4 loaded cases. This is either:

- **(a) True:** all 4 cases happen to be single-defendant single-case executions with no parallel cases. Possible but suspicious.
- **(b) False:** the secondary search-by-name step in the scraper is silently failing, returning 0 (or `None`), and the field is being persisted as broken/empty everywhere.

This investigation determines which. If (b), we fix the bug — material because `other_processes` is one of the most important triage fields for Poursuite (debtor with many parallel cases = low recoverability) and a broken capture means every snapshot in production has incorrect data on a load-bearing filter signal.

The fix may also redirect Layer 3 scope (separate Track B work). If the existing scraper's per-defendant name search is functionally TJSP-only Layer 3, extending it cross-tribunal becomes a "Layer 3-tribal" path with most infrastructure already in place.

## Investigation plan

Four diagnostic steps. Each is ~30 minutes; total budget half a day.

### Step 1 — Read the existing code path

Trace the data flow for `other_processes` end-to-end. Specifically:

1. **`poursuite/scraper/esaj.py`** — find the secondary search-by-name step (around line 439 per recent reference; verify). Read carefully:
   - When does it run? Always, or conditionally?
   - What inputs does it take (defendant name? CPF/CNPJ? both?)
   - What does it return?
   - What happens on error — exception propagates or silently returns 0/None?

2. **`poursuite/models.py`** — confirm `ProcessData.other_processes` is in the model and check what the secondary search step writes into it.

3. **`poursuite/scraper/esaj.py` — `process_batch_records()`** — does this new code path (added in Phase 2c per commit `7ef67b1`) preserve the secondary search step? Or did refactoring it accidentally bypass that logic?

4. **`poursuite/db/esaj_snapshots.py`** — is `other_processes` actually persisted on snapshot write? Was it added to the schema correctly in Phase 1 or Phase 2?

Output a written trace: "input X → step Y → output Z → persisted as W."

### Step 2 — Inspect the live snapshot store

```python
import sqlite3
conn = sqlite3.connect(r'D:\Poursuite\Databases\esaj_snapshots.db')
cur = conn.cursor()

# What's actually in process_snapshot for other_processes?
cur.execute('SELECT process_number, other_processes, defendant, scrape_outcome FROM process_snapshot')
for row in cur.fetchall():
    print(row)

# Are there any non-NULL non-zero values anywhere?
cur.execute('SELECT COUNT(*) FROM process_snapshot WHERE other_processes IS NOT NULL AND other_processes > 0')
print('non-zero count:', cur.fetchone()[0])
```

If literally every row is NULL or 0, the field is dead in production. If some are populated, the field works *sometimes* — also a finding worth understanding.

### Step 3 — Construct a known-good test case

Find a TJSP execution where the defendant is a major bank — any process where `Banco Bradesco`, `Itaú Unibanco`, `Banco do Brasil`, `Santander`, etc. appears as polo passivo. Such defendants are in tens or hundreds of TJSP executions, so `other_processes` should return ≥5 if working.

How to find one:
- Search the existing DJE corpus for paragraphs containing both a CNJ process number and "Bradesco" (or another major bank): `SELECT process_number FROM paragraphs WHERE content LIKE '%BRADESCO%' LIMIT 5` against any shard. Pick one.
- Or, ask the operator for a known-good case if quicker.

Sanity-check the chosen case manually first: visit it on eSAJ in a browser, scroll to the polo passivo, confirm the defendant is the bank. Then check `contadorDeProcessos` is exposed on the page (the secondary-search target).

### Step 4 — Run the production code path against the test case

Run the new carteira-scrape flow (post-UI-2) against the single test case. Specifically use the `/extract/start` endpoint or the underlying `process_batch_records()` directly — whichever is the production path.

After scrape completes, check:
- Did `other_processes` get populated? With what value?
- If null/0, did the scraper's secondary search step actually fire? (Check `tjsp_scraper.log` for evidence — search-by-name should leave a trace.)
- If the value differs from what's visible in a browser (e.g., browser shows "147 processos" but scraper persists 0), the bug is in the parser, not the orchestration.

### Step 5 — Diagnosis

Based on Steps 1-4, classify the outcome as one of:

- **A — Field works correctly.** Probe sample just happened to have single-case defendants. Move on. (Unlikely given Bradesco test should produce ≥5.)
- **B — Field is silently broken.** Identify which layer: orchestration (step never runs), parser (step runs, returns wrong value), or persistence (step returns value, fails to write).
- **C — Field works but is unreliable.** Some scrapes populate it correctly, some don't. Worth understanding why.

For B and C, propose a concrete fix and what tests would validate it.

## Output

`<POURSUITE_LOG_DIR>/probes/other_processes_investigation_<ts>/findings.md` covering:

1. The data-flow trace from Step 1 (where `other_processes` lives in code)
2. The live-snapshot inspection from Step 2 (what's actually in production)
3. The test case used and the scrape result
4. The diagnosis (A / B / C) with evidence
5. If B or C: proposed fix + validation plan
6. Implications for Layer 3 — if the per-defendant secondary search works, what would it take to extend it to other tribunals?

## What this is NOT

- **Not a fix yet.** Investigation only. After the operator reviews findings, we decide what to fix and when.
- **Not a Phase 3 prerequisite.** Phase 3 doesn't depend on `other_processes`. This investigation can complete in parallel with anything else.
- **Not a Layer 3 design pass.** The Layer 3 disposition decision is downstream of both this investigation and Track B's parallel research.

## When done

Don't commit. Capture findings, report back. The operator reviews this alongside Track B's DataJud research, then decides next workstreams.
