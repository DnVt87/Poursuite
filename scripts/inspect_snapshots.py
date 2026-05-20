"""Diagnostic dump of process_snapshot state."""
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from poursuite.config import SNAPSHOT_DIR

db = SNAPSHOT_DIR / "esaj_snapshots.db"
conn = sqlite3.connect(str(db))
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=== counts by scrape_outcome ===")
for r in cur.execute("SELECT scrape_outcome, COUNT(*) AS n FROM process_snapshot GROUP BY scrape_outcome"):
    print(f"  {r['scrape_outcome']}: {r['n']}")

print()
print("=== latest 5 rows ===")
for r in cur.execute(
    "SELECT process_number, scrape_outcome, scrape_error, "
    "class_type, foro, foro_name, value, value_centavos, last_movement, last_movement_iso "
    "FROM process_snapshot ORDER BY snapshot_ts DESC LIMIT 5"
):
    print(dict(r))

print()
print("=== child table counts ===")
for tbl in ("movimento", "linked_process", "peticao"):
    cur.execute(f"SELECT COUNT(*) FROM {tbl}")
    print(f"  {tbl}: {cur.fetchone()[0]}")
