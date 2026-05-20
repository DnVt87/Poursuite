"""Delete process_snapshot rows where scrape_outcome='error'.

Used after the UI-2 carteira-scrape regression (digit-only process numbers
rejected by the strict CNJ validator) to clear bad rows before re-scraping.
Child tables cascade via the existing FK.

Run:
    .venv/Scripts/python.exe scripts/purge_error_snapshots.py --dry-run
    .venv/Scripts/python.exe scripts/purge_error_snapshots.py

Optional filter to a specific error message substring:
    --error "Invalid process number"
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from poursuite.config import SNAPSHOT_DIR


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path, default=SNAPSHOT_DIR / "esaj_snapshots.db")
    p.add_argument("--error", help="Only delete rows whose scrape_error contains this substring.")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    if not args.db.exists():
        print(f"ERROR: DB not found at {args.db}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(str(args.db))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    where = "scrape_outcome = 'error'"
    params: list = []
    if args.error:
        where += " AND scrape_error LIKE ?"
        params.append(f"%{args.error}%")

    cur.execute(f"SELECT process_number, snapshot_ts, scrape_error FROM process_snapshot WHERE {where}", params)
    rows = cur.fetchall()
    print(f"Matched {len(rows)} error rows")
    for r in rows[:20]:
        print(f"  {r['process_number']} @ {r['snapshot_ts']}: {r['scrape_error']}")
    if len(rows) > 20:
        print(f"  … and {len(rows) - 20} more")

    if args.dry_run or not rows:
        conn.close()
        return 0

    cur.execute(f"DELETE FROM process_snapshot WHERE {where}", params)
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    print(f"Deleted {deleted} row(s). Child tables cascaded via FK.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
