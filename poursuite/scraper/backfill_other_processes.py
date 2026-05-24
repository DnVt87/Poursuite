"""Backfill `other_processes` for already-loaded snapshots.

Targets: process_numbers whose latest snapshot has `scrape_outcome='loaded'`
and `other_processes IS NULL`. Re-scrapes each with
`include_other_processes=True`; persistence is the existing
`SnapshotStore.save_snapshot` append-on-change path, so a new snapshot row is
appended (the old row is kept).

Track A finding (2026-05): 11/11 production snapshots had `other_processes`
NULL because the API + UI default to False. This utility addresses the
already-persisted snapshots; the F1 default-flip handles future scrapes.

Usage::

    python -m poursuite.scraper.backfill_other_processes              # backfill all
    python -m poursuite.scraper.backfill_other_processes --dry-run    # list only
    python -m poursuite.scraper.backfill_other_processes --max-cases 10
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import List, Optional

from poursuite.db.esaj_snapshots import DEFAULT_DB_PATH, SnapshotStore
from poursuite.scraper.esaj import ProcessValueScraper
from poursuite.utils import setup_logging


def find_candidates(db_path: Path) -> List[str]:
    """Return process_numbers whose latest snapshot is `loaded` but missing
    `other_processes`. Ordered for determinism."""
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(
            """
            WITH latest AS (
                SELECT process_number, MAX(snapshot_ts) AS ts
                FROM process_snapshot
                GROUP BY process_number
            )
            SELECT p.process_number
            FROM process_snapshot p
            JOIN latest l
              ON l.process_number = p.process_number
             AND l.ts = p.snapshot_ts
            WHERE p.scrape_outcome = 'loaded'
              AND p.other_processes IS NULL
            ORDER BY p.process_number
            """
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m poursuite.scraper.backfill_other_processes",
        description="Backfill other_processes for loaded snapshots where it's NULL.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="List candidates without scraping.",
    )
    parser.add_argument(
        "--max-cases", type=int, default=0,
        help="Cap on cases to backfill (0 = no cap).",
    )
    parser.add_argument(
        "--concurrent", type=int, default=1,
        help="Concurrent browsers (default 1 — backfill is rarely time-critical).",
    )
    parser.add_argument(
        "--db-path", type=Path, default=DEFAULT_DB_PATH,
        help="Snapshot DB path.",
    )
    args = parser.parse_args(argv)

    logger = setup_logging("poursuite.scraper.backfill_other_processes")
    logger.info("Backfill other_processes — db=%s", args.db_path)

    candidates = find_candidates(args.db_path)
    logger.info("Found %d candidate(s) with other_processes IS NULL", len(candidates))
    for pn in candidates:
        logger.info("  - %s", pn)

    if args.max_cases:
        candidates = candidates[: args.max_cases]
        logger.info("Capped to %d candidate(s)", len(candidates))

    if args.dry_run:
        logger.info("Dry run — exiting.")
        return 0

    if not candidates:
        logger.info("Nothing to do.")
        return 0

    snapshot_store = SnapshotStore(db_path=args.db_path, logger=logger)
    scraper = ProcessValueScraper(max_concurrent_browsers=args.concurrent)
    ok_count = err_count = written = 0
    try:
        results = scraper.process_batch_records(
            candidates,
            include_other_processes=True,
            include_movimentos=True,
        )
        for r in results:
            pn = r.process_data.number
            if r.process_data.error:
                err_count += 1
                logger.warning("  %s: error=%s", pn,
                               (r.process_data.error or "")[:200])
                continue
            ok_count += 1
            save_res = snapshot_store.save_snapshot(
                process_data=r.process_data,
                movimentos=[m.to_dict() for m in r.movimentos],
                linked=[lp.to_dict() for lp in r.linked_processes],
                peticoes=[p.to_dict() for p in r.peticoes],
            )
            if save_res.get("inserted"):
                written += 1
            logger.info(
                "  %s: other_processes=%s, snapshot_inserted=%s",
                pn, r.process_data.other_processes, save_res.get("inserted"),
            )
    finally:
        del scraper

    logger.info("Done. ok=%d err=%d new_snapshots=%d",
                ok_count, err_count, written)
    return 0 if err_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
