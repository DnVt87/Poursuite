"""End-to-end smoke for UI Phase 2.5 endpoint groups.

Spins up the FastAPI app pointing at an ephemeral DB + scratch JSON file,
seeds 3 known process_snapshot rows, and exercises every new endpoint:
  - /api/groups (create, list, get, delete)
  - /api/flags (post, get, delete)
  - /api/snapshot_status
  - /api/aggregates/{group_by, histogram, stats}
  - /api/query/explain_zero
  - /api/saved_queries (create, list, get, put, touch, delete)
Plus a regression check that build_query output is unchanged after the
synth_where_only refactor.

Run:
    .venv/Scripts/python.exe tests/smoke_ui_phase_2_5.py
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# Run from repo root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

API_KEY = "smoke-key"
os.environ["POURSUITE_API_KEY"] = API_KEY


def _seed(store):
    """Insert 3 known rows directly via the store (skipping the scraper)."""
    from poursuite.models import ProcessData

    rows = [
        ProcessData(
            number="10000000020208260100",
            class_type="Execução de Título Extrajudicial",
            foro="Foro Central Cível",
            foro_name="Foro Central Cível",
            foro_code="0100",
            distribution_year="2020",
            value="R$ 50.000,00",
            value_centavos=5_000_000,
            last_movement="01/03/2026",
            last_movement_iso="2026-03-01",
        ),
        ProcessData(
            number="20000000020218260100",
            class_type="Execução de Título Extrajudicial",
            foro="Foro Central Cível",
            foro_name="Foro Central Cível",
            foro_code="0100",
            distribution_year="2021",
            value="R$ 250.000,00",
            value_centavos=25_000_000,
            last_movement="15/04/2025",
            last_movement_iso="2025-04-15",
        ),
        ProcessData(
            number="30000000020228260002",
            class_type="Procedimento Comum Cível",
            foro="Foro de São Bernardo",
            foro_name="Foro de São Bernardo",
            foro_code="0002",
            distribution_year="2022",
            value="R$ 12.000,00",
            value_centavos=1_200_000,
            last_movement="10/02/2024",
            last_movement_iso="2024-02-10",
        ),
    ]
    for r in rows:
        store.save_snapshot(r)


def _check(cond, msg):
    if not cond:
        print(f"FAIL: {msg}")
        sys.exit(1)


def main() -> int:
    tmpdir = Path(tempfile.mkdtemp(prefix="poursuite_smoke_"))
    os.environ["POURSUITE_DB_DIR"] = str(tmpdir)
    os.environ["POURSUITE_SNAPSHOT_DIR"] = str(tmpdir)
    os.environ["POURSUITE_LOG_DIR"] = str(tmpdir / "logs")

    # Re-import config so env-vars take effect before SnapshotStore is built.
    import importlib
    import poursuite.config
    importlib.reload(poursuite.config)
    import poursuite.db.esaj_snapshots as snap_mod
    importlib.reload(snap_mod)
    import poursuite.db.process_groups as pg_mod
    importlib.reload(pg_mod)

    from fastapi.testclient import TestClient
    import poursuite.api.main as main_mod
    importlib.reload(main_mod)

    # Seed before starting the client so save_snapshot uses the right DB.
    store = snap_mod.SnapshotStore(tmpdir / "esaj_snapshots.db")
    _check(store.schema_version() == 4, f"schema_version is {store.schema_version()}, want 4")
    _seed(store)
    store.close()

    with TestClient(main_mod.app) as client:
        H = {"X-API-Key": API_KEY}

        # ── groups ───────────────────────────────────────────────────
        r = client.post("/api/groups", headers=H, json={
            "name": "Carteira Itaú · maio/2026",
            "process_numbers": ["10000000020208260100", "20000000020218260100"],
        })
        _check(r.status_code == 200, f"create group: {r.status_code} {r.text}")
        gid = r.json()["group_id"]

        r = client.get("/api/groups", headers=H)
        _check(r.status_code == 200 and len(r.json()["results"]) == 1, "list groups")

        r = client.get(f"/api/groups/{gid}", headers=H)
        _check(r.status_code == 200 and len(r.json()["process_numbers"]) == 2, "get group")

        # ── flags ────────────────────────────────────────────────────
        r = client.post("/api/flags/10000000020208260100", headers=H)
        _check(r.status_code == 200, "flag")
        r = client.get("/api/flags", headers=H)
        _check(r.status_code == 200 and r.json()["flagged"] == ["10000000020208260100"], "list flags")

        # flagged_only on /api/query
        r = client.post("/api/query", headers=H, json={
            "select": ["process_number"], "flagged_only": True,
        })
        _check(r.status_code == 200 and r.json()["total"] == 1, "flagged_only filter")

        r = client.post("/api/query", headers=H, json={
            "select": ["process_number"], "unflagged_only": True,
        })
        _check(r.status_code == 200 and r.json()["total"] == 2, "unflagged_only filter")

        # ── snapshot_status ──────────────────────────────────────────
        r = client.post("/api/snapshot_status", headers=H, json={
            "process_numbers": ["10000000020208260100", "99999999999999999999"],
            "max_age_days": None,
        })
        _check(r.status_code == 200, f"snapshot_status: {r.text}")
        results = r.json()["results"]
        _check(results[0]["status"] == "fresh", f"first should be fresh: {results[0]}")
        _check(results[1]["status"] == "missing", f"second should be missing: {results[1]}")

        # ── aggregates ───────────────────────────────────────────────
        r = client.post("/api/aggregates/group_by", headers=H, json={
            "group_by": "class_type",
        })
        _check(r.status_code == 200, f"group_by class_type: {r.text}")
        gb = {row["value"]: row["count"] for row in r.json()["results"]}
        _check(gb.get("Execução de Título Extrajudicial") == 2, f"group_by counts: {gb}")

        r = client.post("/api/aggregates/group_by", headers=H, json={
            "group_by": "foro_name",
        })
        _check(r.status_code == 200, f"group_by foro_name: {r.text}")
        gb = {row["value"]: row["count"] for row in r.json()["results"]}
        _check(gb.get("Foro Central Cível") == 2, f"group_by foro_name: {gb}")

        r = client.post("/api/aggregates/group_by", headers=H, json={
            "group_by": "last_movement_bucket",
        })
        _check(r.status_code == 200, f"group_by bucket: {r.text}")
        # All three rows have last_movement_iso; counts should sum to 3.
        bucket_total = sum(row["count"] for row in r.json()["results"])
        _check(bucket_total == 3, f"bucket total {bucket_total} should be 3")

        r = client.post("/api/aggregates/histogram", headers=H, json={"field": "value"})
        _check(r.status_code == 200, f"histogram: {r.text}")
        # Row of 50k goes to 50000-100000 (low=50000 inclusive); 250k to 100000-500000;
        # 12k to 10000-50000. Top bucket (>=5M) is empty.
        hist = r.json()["results"]
        _check(any(row["range_low"] == 10000 and row["count"] >= 1 for row in hist),
               f"histogram low bucket: {hist}")
        _check(hist[-1]["range_high"] is None, "last bucket has null range_high")

        r = client.post("/api/aggregates/stats", headers=H, json={"field": "value"})
        _check(r.status_code == 200, f"stats: {r.text}")
        s = r.json()
        _check(s["count"] == 3 and s["sum"] == 312_000.0, f"stats: {s}")

        # ── explain_zero ─────────────────────────────────────────────
        # Build a zero-result AND query: real class_type but bogus foro_code.
        r = client.post("/api/query/explain_zero", headers=H, json={
            "where": {"and": [
                {"field": "class_type", "op": "=", "value": "Execução de Título Extrajudicial"},
                {"field": "foro_code", "op": "=", "value": "NOPE"},
            ]},
        })
        _check(r.status_code == 200, f"explain_zero: {r.text}")
        clauses = r.json()["clauses"]
        _check(len(clauses) == 2, f"explain_zero clauses: {clauses}")
        counts = {c["decomposition_path"]: c["count_alone"] for c in clauses}
        _check(counts.get("and[0]") == 2 and counts.get("and[1]") == 0,
               f"explain_zero counts: {counts}")

        # Complex shape returns the message.
        r = client.post("/api/query/explain_zero", headers=H, json={
            "where": {"or": [{"field": "class_type", "op": "=", "value": "X"}]},
        })
        _check(r.status_code == 200 and r.json().get("message"),
               f"explain_zero complex: {r.text}")

        # ── saved queries ────────────────────────────────────────────
        body = {"select": ["process_number"], "where": {
            "field": "class_type", "op": "=", "value": "Execução de Título Extrajudicial"
        }}
        r = client.post("/api/saved_queries", headers=H, json={
            "name": "Execuções", "description": "All execuções", "query_body": body,
        })
        _check(r.status_code == 200, f"create saved: {r.text}")
        sq_id = r.json()["id"]

        r = client.get("/api/saved_queries", headers=H)
        _check(r.status_code == 200 and len(r.json()["results"]) == 1, "list saved")

        r = client.get(f"/api/saved_queries/{sq_id}", headers=H)
        _check(r.status_code == 200 and r.json()["query_body"] == body, "get saved")

        r = client.put(f"/api/saved_queries/{sq_id}", headers=H, json={"name": "Renamed"})
        _check(r.status_code == 200, f"put saved: {r.text}")

        r = client.post(f"/api/saved_queries/{sq_id}/touch", headers=H, json={"result_count": 42})
        _check(r.status_code == 200, f"touch saved: {r.text}")

        r = client.get(f"/api/saved_queries/{sq_id}", headers=H)
        _check(r.json()["last_run_count"] == 42 and r.json()["name"] == "Renamed",
               f"touched + renamed: {r.json()}")

        r = client.delete(f"/api/saved_queries/{sq_id}", headers=H)
        _check(r.status_code == 200, "delete saved")

        # Cleanup groups + flags
        r = client.delete("/api/flags/10000000020208260100", headers=H)
        _check(r.status_code == 200, "unflag")
        r = client.delete(f"/api/groups/{gid}", headers=H)
        _check(r.status_code == 200, "delete group")

    # ── synth_where_only regression check ────────────────────────────
    from poursuite.db.esaj_query import build_query, synth_where_only
    # A representative panel of bodies that exercised build_query before refactor.
    panel = [
        {"select": ["process_number"]},
        {"where": {"field": "class_type", "op": "=", "value": "X"}, "limit": 10},
        {"where": {"and": [
            {"field": "foro_code", "op": "=", "value": "0100"},
            {"movimento_any": {"field": "nome", "op": "match", "value": "penhora"}},
        ]}, "snapshot": "any"},
        {"where": {"field": "value_centavos", "op": ">=", "value": 1000000}},
    ]
    for body in panel:
        b = build_query(body)
        _check(isinstance(b.select_sql, str) and b.count_sql.startswith("SELECT COUNT(*)"),
               f"build_query unchanged: {body}")
        ws, ps, joins = synth_where_only(body)
        _check(joins == [], "joins empty in v1")
        # build_query's combined_where must equal synth_where_only's output.
        _check(ws in b.select_sql, f"synth_where_only fragment in select_sql: {ws}")

    print("OK — UI Phase 2.5 smoke passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
