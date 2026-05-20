"""One-shot backfill for the schema-v4 normalized columns on process_snapshot.

After the v4 migration adds `foro_name`, `last_movement_iso`, `value_centavos`,
existing rows have NULLs there. This script computes each from the row's raw
fields (no scrape required) and UPDATEs in-place. Idempotent: re-running on a
fully populated DB updates nothing.

Run:
    .venv/Scripts/python.exe scripts/backfill_normalized_columns.py

Or against an alternate DB:
    .venv/Scripts/python.exe scripts/backfill_normalized_columns.py --db D:/Poursuite/Databases/esaj_snapshots.db

Add --dry-run to see counts without writing.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

# Allow running from the repo root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from poursuite.config import DB_DIR
from poursuite.scraper.cnj_origem import derive_from_cnj
from poursuite.utils import parse_brazilian_date_to_iso, parse_brl_to_centavos


def backfill(db_path: Path, dry_run: bool = False) -> dict:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        """
        SELECT process_number, snapshot_ts, last_movement, value,
               foro_name, last_movement_iso, value_centavos
        FROM process_snapshot
        WHERE foro_name IS NULL
           OR (last_movement IS NOT NULL AND last_movement_iso IS NULL)
           OR (value IS NOT NULL AND value_centavos IS NULL)
        """
    )
    rows = cur.fetchall()

    counts = {"scanned": len(rows), "updated": 0, "foro_name": 0,
              "last_movement_iso": 0, "value_centavos": 0}
    updates = []
    for row in rows:
        new_foro_name = row["foro_name"]
        if new_foro_name is None:
            new_foro_name = derive_from_cnj(row["process_number"]).get("foro_name")
            if new_foro_name is not None:
                counts["foro_name"] += 1

        new_lm_iso = row["last_movement_iso"]
        if new_lm_iso is None and row["last_movement"]:
            new_lm_iso = parse_brazilian_date_to_iso(row["last_movement"])
            if new_lm_iso is not None:
                counts["last_movement_iso"] += 1

        new_cents = row["value_centavos"]
        if new_cents is None and row["value"]:
            new_cents = parse_brl_to_centavos(row["value"])
            if new_cents is not None:
                counts["value_centavos"] += 1

        if (new_foro_name, new_lm_iso, new_cents) == (
            row["foro_name"], row["last_movement_iso"], row["value_centavos"]
        ):
            continue
        updates.append((new_foro_name, new_lm_iso, new_cents,
                        row["process_number"], row["snapshot_ts"]))

    counts["updated"] = len(updates)

    if dry_run or not updates:
        conn.close()
        return counts

    cur.executemany(
        """
        UPDATE process_snapshot
        SET foro_name = ?, last_movement_iso = ?, value_centavos = ?
        WHERE process_number = ? AND snapshot_ts = ?
        """,
        updates,
    )
    conn.commit()
    conn.close()
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=DB_DIR / "esaj_snapshots.db",
        help="Path to esaj_snapshots.db (default: $POURSUITE_DB_DIR/esaj_snapshots.db)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without writing.",
    )
    args = parser.parse_args()

    if not args.db.exists():
        print(f"ERROR: DB not found at {args.db}", file=sys.stderr)
        return 2

    counts = backfill(args.db, dry_run=args.dry_run)
    verb = "would update" if args.dry_run else "updated"
    print(
        f"Scanned {counts['scanned']} candidate rows; "
        f"{verb} {counts['updated']} "
        f"(foro_name: {counts['foro_name']}, "
        f"last_movement_iso: {counts['last_movement_iso']}, "
        f"value_centavos: {counts['value_centavos']})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
