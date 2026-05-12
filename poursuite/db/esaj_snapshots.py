"""eSAJ snapshot store — durable, queryable per-process scrape data.

Single SQLite file at `DB_DIR/esaj_snapshots.db`. Append-on-change semantics:
each scrape computes a SHA-256 hash over (header + movimentos + linked
+ peticoes); when the hash matches the most-recent stored snapshot for the
process, the scrape is a no-op. Otherwise a new (process_number, snapshot_ts)
row is inserted plus child rows.

Schema is in poursuite/db/esaj_schema.sql. Initialization is idempotent via
the `schema_version` table; future migrations apply sequentially from the
current version.

Phase 2a — this module ships the store, the diff-aware save path for header
data, and retrieval helpers. Movimentações, linked processes, and petições
parameters are accepted by save_snapshot but currently must be empty (will
be populated by phases 2c/2d).
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
from dataclasses import asdict, fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from poursuite.config import DB_DIR
from poursuite.models import ProcessData
from poursuite.utils import setup_logging

SCHEMA_PATH = Path(__file__).parent / "esaj_schema.sql"
DEFAULT_DB_PATH: Path = DB_DIR / "esaj_snapshots.db"
CURRENT_SCHEMA_VERSION = 2

# Field names from ProcessData that map 1:1 to dedicated columns on
# process_snapshot. Excludes `number` (becomes process_number) and
# `error` (becomes scrape_error). Built from the dataclass to stay
# in sync if ProcessData grows.
def _promoted_field_names() -> List[str]:
    skip = {"number", "error"}
    return [f.name for f in fields(ProcessData) if f.name not in skip]


_PROMOTED_FIELDS: List[str] = _promoted_field_names()


def _canonical_json(payload: Any) -> str:
    """Deterministic JSON for hashing — sorted keys, no whitespace, UTF-8."""
    return json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _utc_now_iso() -> str:
    """ISO 8601 UTC with microsecond precision, suitable for snapshot_ts."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


def _bump_microsecond(ts_iso: str) -> str:
    """Add 1µs to an ISO 8601 UTC timestamp produced by `_utc_now_iso`.

    Used by save_snapshot to break ties when the generated snapshot_ts
    equals the latest stored one — clock jitter or pathological retry
    loops only. Format compatible with the timestamps the rest of the
    store produces.
    """
    from datetime import timedelta
    dt = datetime.strptime(ts_iso, "%Y-%m-%dT%H:%M:%S.%f+00:00").replace(tzinfo=timezone.utc)
    return (dt + timedelta(microseconds=1)).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


def compute_snapshot_hash(
    process_data: ProcessData,
    movimentos: Optional[Sequence[Dict[str, Any]]] = None,
    linked: Optional[Sequence[Dict[str, Any]]] = None,
    peticoes: Optional[Sequence[Dict[str, Any]]] = None,
) -> str:
    """Compute the canonical SHA-256 hash of a snapshot's contents.

    The hash is the diff key. Two scrapes producing identical content yield
    the same hash regardless of wire order (the second is then a no-op
    against the store).

    Sort + exclusion strategy:
      - Movimentos: sort and serialize by content keys only — (data_hora,
        nome, complementos_text). `ordem` is wire-order metadata and is
        EXCLUDED from the hashed payload so wire-order shuffles between
        scrapes don't trigger spurious diffs.
      - Linked: sort by (linked_number, relationship_type) — both content.
      - Peticoes: sort by (data, tipo, cd_documento). `ordem` excluded
        for the same reason as movimentos.
    """
    header = {f.name: getattr(process_data, f.name) for f in fields(ProcessData)}

    def _strip_ordem(rows: Optional[Sequence[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        return [{k: v for k, v in dict(r).items() if k != "ordem"} for r in (rows or [])]

    movs_no_ordem = _strip_ordem(movimentos)
    movs_canonical = sorted(
        movs_no_ordem,
        key=lambda m: (
            m.get("data_hora") or "",
            m.get("nome") or "",
            (m.get("complementos_text") or "")[:200],
        ),
    )
    linked_canonical = sorted(
        (dict(li) for li in (linked or [])),
        key=lambda li: (li.get("linked_number") or "", li.get("relationship_type") or ""),
    )
    peti_no_ordem = _strip_ordem(peticoes)
    peti_canonical = sorted(
        peti_no_ordem,
        key=lambda p: (p.get("data") or "", p.get("tipo") or "", p.get("cd_documento") or ""),
    )
    payload = {
        "header": header,
        "movimentos": movs_canonical,
        "linked": linked_canonical,
        "peticoes": peti_canonical,
    }
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _outcome_from(process_data: ProcessData) -> str:
    """Derive scrape_outcome from a ProcessData."""
    if process_data.error == "Segredo de justiça":
        return "sealed"
    if process_data.error:
        return "error"
    if process_data.class_type:
        return "loaded"
    return "not_found"


class SnapshotStore:
    """Single-file SQLite store for eSAJ per-process snapshots.

    Thread-safe via a single internal lock; assumes one process owns the DB
    (no cross-process locking). The FastAPI app holds one instance for its
    lifetime via lifespan; tests construct ephemeral stores against temp paths.
    """

    def __init__(
        self,
        db_path: Optional[Path] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.db_path = Path(db_path) if db_path is not None else DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger = logger or setup_logging("esaj_snapshots")
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._init_schema()

    # ─────────────────────────────────────────────────────────────────
    # Schema init / version
    # ─────────────────────────────────────────────────────────────────

    def _init_schema(self) -> None:
        """Initialize or migrate the snapshot store schema.

        Fresh DB (version=0): apply the bundled esaj_schema.sql (the v1
        baseline) and then run any v2+ migrations.

        Existing v1 DB: skip the baseline and run v2+ migrations only.

        Migrations live in `_apply_migration`. The pattern scales to
        future versions: bump CURRENT_SCHEMA_VERSION and add a branch.
        """
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_version (
                    version    INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
                """
            )
            cur.execute("SELECT COALESCE(MAX(version), 0) FROM schema_version")
            current = cur.fetchone()[0]
            if current >= CURRENT_SCHEMA_VERSION:
                return

            if current == 0:
                # Fresh DB: apply the v1 baseline from schema.sql.
                schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
                cur.executescript(schema_sql)
                cur.execute(
                    "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
                    (1, _utc_now_iso()),
                )
                current = 1
                self.logger.info(
                    "Initialized esaj snapshot store schema v1 at %s", self.db_path,
                )

            # Apply each missing migration in sequence.
            while current < CURRENT_SCHEMA_VERSION:
                next_v = current + 1
                self._apply_migration(cur, next_v)
                cur.execute(
                    "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
                    (next_v, _utc_now_iso()),
                )
                self.logger.info(
                    "Migrated esaj snapshot store schema v%d -> v%d", current, next_v,
                )
                current = next_v
            self._conn.commit()

    def _apply_migration(self, cur: sqlite3.Cursor, version: int) -> None:
        """Apply the migration that brings the schema from version-1 to `version`.

        Each migration must be idempotent or guard against re-application,
        since SQLite has no transactional DDL — a half-applied migration on
        crash recovery would need manual cleanup.
        """
        if version == 2:
            # Phase 2d: cd_documento column on movimento. Carries the eSAJ
            # document ID (from <a class="linkMovVincProc" href="...?cdDocumento=...">)
            # so Phase 3 deep-search can fetch the PDFs directly.
            cur.execute("ALTER TABLE movimento ADD COLUMN cd_documento TEXT")
            return
        raise RuntimeError(f"Unknown migration version: {version}")

    def schema_version(self) -> int:
        cur = self._conn.cursor()
        cur.execute("SELECT COALESCE(MAX(version), 0) FROM schema_version")
        return int(cur.fetchone()[0])

    # ─────────────────────────────────────────────────────────────────
    # Save (diff-aware)
    # ─────────────────────────────────────────────────────────────────

    def save_snapshot(
        self,
        process_data: ProcessData,
        movimentos: Optional[Sequence[Dict[str, Any]]] = None,
        linked: Optional[Sequence[Dict[str, Any]]] = None,
        peticoes: Optional[Sequence[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Save a snapshot if the payload differs from the latest stored one.

        Returns a dict:
          {"inserted": bool, "snapshot_ts": str | None, "snapshot_hash": str,
           "reason": "first_snapshot" | "changed" | "unchanged"}

        On first scrape of a process: always inserts. On subsequent scrapes:
        inserts only if hash differs from latest. Child rows (movimentos,
        linked, peticoes) are inserted atomically with the snapshot row.
        """
        new_hash = compute_snapshot_hash(process_data, movimentos, linked, peticoes)
        outcome = _outcome_from(process_data)
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                SELECT snapshot_ts, snapshot_hash
                FROM process_snapshot
                WHERE process_number = ?
                ORDER BY snapshot_ts DESC
                LIMIT 1
                """,
                (process_data.number,),
            )
            latest = cur.fetchone()
            if latest is not None and latest["snapshot_hash"] == new_hash:
                self.logger.debug(
                    "save_snapshot: unchanged for %s (hash %s)",
                    process_data.number, new_hash[:12],
                )
                return {
                    "inserted": False,
                    "snapshot_ts": latest["snapshot_ts"],
                    "snapshot_hash": new_hash,
                    "reason": "unchanged",
                }

            # snapshot_ts uses microsecond-precision UTC. The lock above
            # serializes writes within this process, so consecutive snapshots
            # to the same process get distinct timestamps under normal clock
            # behavior. Bump-by-1µs handles the pathological case where the
            # generated ts equals the latest stored ts (system-clock jitter,
            # or extremely tight retry loops in the future).
            snapshot_ts = _utc_now_iso()
            if latest is not None and snapshot_ts <= latest["snapshot_ts"]:
                snapshot_ts = _bump_microsecond(latest["snapshot_ts"])
                self.logger.warning(
                    "save_snapshot: bumped snapshot_ts past latest for %s "
                    "(latest=%s, bumped_to=%s)",
                    process_data.number, latest["snapshot_ts"], snapshot_ts,
                )
            header_dict = asdict(process_data)
            header_json = _canonical_json(header_dict)
            cur.execute("BEGIN")
            try:
                cur.execute(
                    f"""
                    INSERT INTO process_snapshot (
                        process_number, snapshot_ts, snapshot_hash, scraped_at,
                        scrape_outcome,
                        {", ".join(_PROMOTED_FIELDS)},
                        scrape_error,
                        header_json
                    ) VALUES (?, ?, ?, ?, ?, {", ".join("?" for _ in _PROMOTED_FIELDS)}, ?, ?)
                    """,
                    (
                        process_data.number,
                        snapshot_ts,
                        new_hash,
                        snapshot_ts,
                        outcome,
                        *(getattr(process_data, f) for f in _PROMOTED_FIELDS),
                        process_data.error,
                        header_json,
                    ),
                )

                if movimentos:
                    cur.executemany(
                        """
                        INSERT INTO movimento (
                            process_number, snapshot_ts, ordem, data_hora,
                            codigo, nome, complementos_json, complementos_text,
                            cd_documento
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            (
                                process_data.number,
                                snapshot_ts,
                                m.get("ordem"),
                                m.get("data_hora"),
                                m.get("codigo"),
                                m.get("nome"),
                                m.get("complementos_json"),
                                m.get("complementos_text"),
                                m.get("cd_documento"),
                            )
                            for m in movimentos
                        ],
                    )
                if linked:
                    cur.executemany(
                        """
                        INSERT INTO linked_process (
                            process_number, snapshot_ts, linked_number, relationship_type
                        ) VALUES (?, ?, ?, ?)
                        """,
                        [
                            (
                                process_data.number,
                                snapshot_ts,
                                li.get("linked_number"),
                                li.get("relationship_type"),
                            )
                            for li in linked
                        ],
                    )
                if peticoes:
                    cur.executemany(
                        """
                        INSERT INTO peticao (
                            process_number, snapshot_ts, ordem, data, tipo, cd_documento
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        [
                            (
                                process_data.number,
                                snapshot_ts,
                                p.get("ordem"),
                                p.get("data"),
                                p.get("tipo"),
                                p.get("cd_documento"),
                            )
                            for p in peticoes
                        ],
                    )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

        return {
            "inserted": True,
            "snapshot_ts": snapshot_ts,
            "snapshot_hash": new_hash,
            "reason": "first_snapshot" if latest is None else "changed",
        }

    # ─────────────────────────────────────────────────────────────────
    # Retrieval
    # ─────────────────────────────────────────────────────────────────

    def get_latest(self, process_number: str) -> Optional[Dict[str, Any]]:
        """Return the most recent snapshot for a process, or None."""
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT * FROM process_snapshot
            WHERE process_number = ?
            ORDER BY snapshot_ts DESC
            LIMIT 1
            """,
            (process_number,),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def list_snapshots(self, process_number: str) -> List[Dict[str, Any]]:
        """Return all snapshots for a process, newest first."""
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT snapshot_ts, snapshot_hash, scraped_at, scrape_outcome, scrape_error
            FROM process_snapshot
            WHERE process_number = ?
            ORDER BY snapshot_ts DESC
            """,
            (process_number,),
        )
        return [dict(r) for r in cur.fetchall()]

    def count_snapshots(self, process_number: Optional[str] = None) -> int:
        cur = self._conn.cursor()
        if process_number is None:
            cur.execute("SELECT COUNT(*) FROM process_snapshot")
        else:
            cur.execute(
                "SELECT COUNT(*) FROM process_snapshot WHERE process_number = ?",
                (process_number,),
            )
        return int(cur.fetchone()[0])

    # ─────────────────────────────────────────────────────────────────
    # Child-table retrieval (for the 4 GET endpoints)
    # ─────────────────────────────────────────────────────────────────

    def _latest_snapshot_ts(self, process_number: str) -> Optional[str]:
        cur = self._conn.cursor()
        cur.execute(
            "SELECT MAX(snapshot_ts) FROM process_snapshot WHERE process_number = ?",
            (process_number,),
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else None

    def get_movimentos(
        self,
        process_number: str,
        snapshot_ts: Optional[str] = None,
        since: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Movimentos for the given snapshot (latest if not specified).

        `since` filters by data_hora >= since (ISO 8601 date or full ts).
        """
        ts = snapshot_ts or self._latest_snapshot_ts(process_number)
        if ts is None:
            return []
        cur = self._conn.cursor()
        if since:
            cur.execute(
                "SELECT * FROM movimento "
                "WHERE process_number = ? AND snapshot_ts = ? "
                "AND data_hora IS NOT NULL AND data_hora >= ? "
                "ORDER BY ordem",
                (process_number, ts, since),
            )
        else:
            cur.execute(
                "SELECT * FROM movimento "
                "WHERE process_number = ? AND snapshot_ts = ? "
                "ORDER BY ordem",
                (process_number, ts),
            )
        return [dict(r) for r in cur.fetchall()]

    def get_linked(
        self,
        process_number: str,
        snapshot_ts: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        ts = snapshot_ts or self._latest_snapshot_ts(process_number)
        if ts is None:
            return []
        cur = self._conn.cursor()
        cur.execute(
            "SELECT * FROM linked_process "
            "WHERE process_number = ? AND snapshot_ts = ? "
            "ORDER BY relationship_type, linked_number",
            (process_number, ts),
        )
        return [dict(r) for r in cur.fetchall()]

    def get_peticoes(
        self,
        process_number: str,
        snapshot_ts: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        ts = snapshot_ts or self._latest_snapshot_ts(process_number)
        if ts is None:
            return []
        cur = self._conn.cursor()
        cur.execute(
            "SELECT * FROM peticao "
            "WHERE process_number = ? AND snapshot_ts = ? "
            "ORDER BY ordem",
            (process_number, ts),
        )
        return [dict(r) for r in cur.fetchall()]

    # ─────────────────────────────────────────────────────────────────
    # Query (POST /api/query)
    # ─────────────────────────────────────────────────────────────────

    def query(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """Run a structured query body against the snapshot store.

        See poursuite/db/esaj_query.py for the body shape. On malformed
        bodies, raises QueryError — caller should catch and return 400.
        On FTS5 syntax errors in `match` values, the underlying SQLite
        raises an OperationalError; we wrap it as QueryError too.
        """
        from poursuite.db.esaj_query import QueryError, build_query

        built = build_query(body)

        cur = self._conn.cursor()
        try:
            cur.execute(built.count_sql, built.count_params)
        except sqlite3.OperationalError as e:
            raise QueryError(f"SQL error (likely FTS5 syntax): {e}") from e
        total = int(cur.fetchone()[0])

        results: List[Dict[str, Any]] = []
        if not built.count_only and built.limit > 0:
            try:
                cur.execute(built.select_sql, built.select_params)
            except sqlite3.OperationalError as e:
                raise QueryError(f"SQL error (likely FTS5 syntax): {e}") from e
            rows = cur.fetchall()
            for row in rows:
                results.append({
                    "process_number": row["process_number"],
                    "snapshot_ts": row["snapshot_ts"],
                    "fields": {
                        f: row[f]
                        for f in built.fields
                        if f not in ("process_number",) and f in row.keys()
                    },
                })
        return {
            "total": total,
            "limit": built.limit,
            "offset": built.offset,
            "count_only": built.count_only,
            "results": results,
        }

    # ─────────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────────

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass
