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
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

from poursuite.config import SNAPSHOT_DIR
from poursuite.models import ProcessData
from poursuite.utils import setup_logging

if TYPE_CHECKING:
    from poursuite.datajud.enrichment import EnrichmentRecord

SCHEMA_PATH = Path(__file__).parent / "esaj_schema.sql"
DEFAULT_DB_PATH: Path = SNAPSHOT_DIR / "esaj_snapshots.db"
CURRENT_SCHEMA_VERSION = 5

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


def compute_enrichment_hash(payload: Any) -> str:
    """SHA-256 over a canonical DataJud-enrichment payload — the diff key for
    append-on-change. `payload` is the JSON-able structure returned by
    `EnrichmentRecord.canonical_payload()`; canonicalization (sorted keys, no
    whitespace) lives here so the store owns the one hashing convention."""
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
        if version == 3:
            # UI Phase 2.5: additive — new tables only. Safe to re-run.
            # process_flags: single-state ★ per process_number. Global namespace
            # per the brief's identity model — no author column.
            # If multi-state becomes a real need, the additive path is to add
            # a label TEXT column with default 'starred'; v1 queries become
            # WHERE label = 'starred'.
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS process_flags (
                    process_number TEXT PRIMARY KEY,
                    flagged_at     TEXT NOT NULL
                )
                """
            )
            # saved_queries: shared library backing Workflow 5.
            # query_body is the JSON body POST /api/query accepts, verbatim
            # (so re-running a saved query is just sending its query_body back).
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS saved_queries (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    name            TEXT NOT NULL,
                    description     TEXT,
                    query_body      TEXT NOT NULL,
                    created_at      TEXT NOT NULL,
                    last_run_at     TEXT,
                    last_run_count  INTEGER
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_saved_queries_last_run_at "
                "ON saved_queries(last_run_at DESC) WHERE last_run_at IS NOT NULL"
            )
            return
        if version == 4:
            # Normalized columns on process_snapshot for aggregate-friendly
            # ordering and range queries. The raw eSAJ fields (last_movement
            # DD/MM/YYYY, value 'R$ N.NNN,NN') stay populated; these columns
            # are derivative and indexed.
            #
            # SQLite ALTER TABLE ADD COLUMN is idempotent only via a guard;
            # the table_info pragma check below makes re-runs no-op.
            cur.execute("PRAGMA table_info(process_snapshot)")
            existing_cols = {row[1] for row in cur.fetchall()}
            if "foro_name" not in existing_cols:
                cur.execute("ALTER TABLE process_snapshot ADD COLUMN foro_name TEXT")
            if "last_movement_iso" not in existing_cols:
                cur.execute("ALTER TABLE process_snapshot ADD COLUMN last_movement_iso TEXT")
            if "value_centavos" not in existing_cols:
                cur.execute("ALTER TABLE process_snapshot ADD COLUMN value_centavos INTEGER")
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_process_snapshot_foro_name "
                "ON process_snapshot(foro_name) WHERE foro_name IS NOT NULL"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_process_snapshot_last_movement_iso "
                "ON process_snapshot(last_movement_iso) WHERE last_movement_iso IS NOT NULL"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_process_snapshot_value_centavos "
                "ON process_snapshot(value_centavos) WHERE value_centavos IS NOT NULL"
            )
            return
        if version == 5:
            # Layer 3-lite: DataJud per-process enrichment. Additive — new
            # tables only, a separate DataJud-sourced layer that runs on its
            # own cadence and never touches eSAJ-sourced columns. Append-on-
            # change keyed by (process_number, fetched_at), mirroring the
            # (process_number, snapshot_ts) discipline on process_snapshot.
            # Safe to re-run (IF NOT EXISTS guards; SQLite has no transactional
            # DDL, so each statement is independently idempotent).
            #
            # complementosTabelados is stored in FULL — every complemento, not
            # just outcome-bearing ones (the L3L design fork). Filtering is a
            # query-time concern, so (complemento_codigo, complemento_valor) is
            # indexed for the success/failure axis.
            #
            # Shapes confirmed empirically in L3L-a (8/8 indexed cases):
            #   complementosTabelados {codigo:int, valor:int, nome:str,
            #     descricao:str}, 100% populated, 1221 objects sampled;
            #   assuntos {codigo:int, nome:str}; grau scalar str ("G1" in
            #   sample); codigoMunicipioIBGE sparse int (0/8 populated);
            #   dataHoraUltimaAtualizacao ISO-8601 str, variable fractional
            #   precision (.fff vs .ffffff) — parsed into the _iso column.
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS datajud_enrichment (
                    process_number                   TEXT NOT NULL,
                    fetched_at                       TEXT NOT NULL,
                    enrichment_hash                  TEXT NOT NULL,
                    datajud_found                    INTEGER NOT NULL,
                    tribunal                         TEXT,
                    grau                             TEXT,
                    assuntos_json                    TEXT,
                    codigo_municipio_ibge            INTEGER,
                    data_hora_ultima_atualizacao     TEXT,
                    data_hora_ultima_atualizacao_iso TEXT,
                    movimentos_count                 INTEGER,
                    complementos_count               INTEGER,
                    raw_source_json                  TEXT,
                    PRIMARY KEY (process_number, fetched_at)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS datajud_complemento (
                    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                    process_number        TEXT NOT NULL,
                    fetched_at            TEXT NOT NULL,
                    movimento_indice      INTEGER,
                    movimento_data_hora   TEXT,
                    movimento_codigo      INTEGER,
                    movimento_nome        TEXT,
                    complemento_codigo    INTEGER,
                    complemento_valor     INTEGER,
                    complemento_nome      TEXT,
                    complemento_descricao TEXT,
                    FOREIGN KEY (process_number, fetched_at)
                        REFERENCES datajud_enrichment (process_number, fetched_at)
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_datajud_complemento_proc "
                "ON datajud_complemento(process_number, fetched_at)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_datajud_complemento_codigo_valor "
                "ON datajud_complemento(complemento_codigo, complemento_valor)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_datajud_complemento_mov_codigo "
                "ON datajud_complemento(movimento_codigo)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_datajud_enrichment_grau "
                "ON datajud_enrichment(grau) WHERE grau IS NOT NULL"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_datajud_enrichment_ibge "
                "ON datajud_enrichment(codigo_municipio_ibge) "
                "WHERE codigo_municipio_ibge IS NOT NULL"
            )
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
    # Flags (UI Phase 2.5)
    # ─────────────────────────────────────────────────────────────────

    def list_flagged(self) -> List[str]:
        cur = self._conn.cursor()
        cur.execute("SELECT process_number FROM process_flags ORDER BY flagged_at DESC")
        return [r[0] for r in cur.fetchall()]

    def flag(self, process_number: str) -> Dict[str, Any]:
        """Mark a process as flagged. Idempotent — re-flagging keeps the
        original flagged_at (per INSERT OR IGNORE)."""
        ts = _utc_now_iso()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "INSERT OR IGNORE INTO process_flags (process_number, flagged_at) VALUES (?, ?)",
                (process_number, ts),
            )
            self._conn.commit()
            cur.execute(
                "SELECT flagged_at FROM process_flags WHERE process_number = ?",
                (process_number,),
            )
            row = cur.fetchone()
        return {"process_number": process_number, "flagged_at": row["flagged_at"] if row else ts}

    def unflag(self, process_number: str) -> Dict[str, Any]:
        """Remove a flag. Idempotent — returns deleted=False if the row was absent."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "DELETE FROM process_flags WHERE process_number = ?",
                (process_number,),
            )
            deleted = cur.rowcount > 0
            self._conn.commit()
        return {"process_number": process_number, "deleted": deleted}

    # ─────────────────────────────────────────────────────────────────
    # Saved queries (UI Phase 2.5)
    # ─────────────────────────────────────────────────────────────────

    def list_saved_queries(self) -> List[Dict[str, Any]]:
        cur = self._conn.cursor()
        cur.execute(
            "SELECT id, name, description, created_at, last_run_at, last_run_count "
            "FROM saved_queries ORDER BY COALESCE(last_run_at, created_at) DESC"
        )
        return [dict(r) for r in cur.fetchall()]

    def get_saved_query(self, qid: int) -> Optional[Dict[str, Any]]:
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM saved_queries WHERE id = ?", (qid,))
        row = cur.fetchone()
        return dict(row) if row else None

    def create_saved_query(
        self, name: str, query_body: str, description: Optional[str] = None
    ) -> Dict[str, Any]:
        created_at = _utc_now_iso()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "INSERT INTO saved_queries (name, description, query_body, created_at) "
                "VALUES (?, ?, ?, ?)",
                (name, description, query_body, created_at),
            )
            qid = cur.lastrowid
            self._conn.commit()
        return {"id": qid, "created_at": created_at}

    def update_saved_query(
        self,
        qid: int,
        name: Optional[str] = None,
        description: Optional[str] = None,
        query_body: Optional[str] = None,
    ) -> bool:
        """Partial update. Returns True if the row existed and was updated."""
        sets: List[str] = []
        params: List[Any] = []
        if name is not None:
            sets.append("name = ?")
            params.append(name)
        if description is not None:
            sets.append("description = ?")
            params.append(description)
        if query_body is not None:
            sets.append("query_body = ?")
            params.append(query_body)
        if not sets:
            return self.get_saved_query(qid) is not None
        params.append(qid)
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(f"UPDATE saved_queries SET {', '.join(sets)} WHERE id = ?", params)
            updated = cur.rowcount > 0
            self._conn.commit()
        return updated

    def delete_saved_query(self, qid: int) -> bool:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("DELETE FROM saved_queries WHERE id = ?", (qid,))
            deleted = cur.rowcount > 0
            self._conn.commit()
        return deleted

    def touch_saved_query(self, qid: int, result_count: int) -> bool:
        """Record a re-run: update last_run_at and last_run_count."""
        ts = _utc_now_iso()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "UPDATE saved_queries SET last_run_at = ?, last_run_count = ? WHERE id = ?",
                (ts, result_count, qid),
            )
            updated = cur.rowcount > 0
            self._conn.commit()
        return updated

    # ─────────────────────────────────────────────────────────────────
    # Snapshot status (UI Phase 2.5)
    # ─────────────────────────────────────────────────────────────────

    def snapshot_status(
        self, process_numbers: List[str], max_age_days: Optional[int] = 7
    ) -> List[Dict[str, Any]]:
        """For each process_number, classify as 'fresh' / 'stale' / 'missing'.

        Lookup is one chunked IN (...) per 1000 numbers. max_age_days=None
        means "no age cutoff": every existing snapshot is fresh, missing ones
        still missing.
        """
        from datetime import timedelta

        if not process_numbers:
            return []
        now = datetime.now(timezone.utc)
        latest: Dict[str, str] = {}
        chunk = 1000
        cur = self._conn.cursor()
        for i in range(0, len(process_numbers), chunk):
            sub = process_numbers[i : i + chunk]
            placeholders = ",".join("?" * len(sub))
            cur.execute(
                f"SELECT process_number, MAX(snapshot_ts) AS ts "
                f"FROM process_snapshot WHERE process_number IN ({placeholders}) "
                f"GROUP BY process_number",
                sub,
            )
            for row in cur.fetchall():
                latest[row["process_number"]] = row["ts"]

        results: List[Dict[str, Any]] = []
        for pn in process_numbers:
            ts = latest.get(pn)
            if ts is None:
                results.append({
                    "process_number": pn, "status": "missing",
                    "snapshot_ts": None, "age_days": None,
                })
                continue
            try:
                ts_dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f+00:00").replace(
                    tzinfo=timezone.utc
                )
                age_days = (now - ts_dt).days
            except ValueError:
                age_days = None
            if max_age_days is None or (age_days is not None and age_days <= max_age_days):
                status = "fresh"
            else:
                status = "stale"
            results.append({
                "process_number": pn, "status": status,
                "snapshot_ts": ts, "age_days": age_days,
            })
        return results

    # ─────────────────────────────────────────────────────────────────
    # DataJud enrichment (Layer 3-lite)
    # ─────────────────────────────────────────────────────────────────

    def save_datajud_enrichment(self, record: "EnrichmentRecord") -> Dict[str, Any]:
        """Append-on-change persist of a DataJud enrichment for one process.

        Mirrors `save_snapshot`: hashes the enrichment payload; if it equals
        the latest stored enrichment for the process, it's a no-op. Otherwise
        a new (process_number, fetched_at) row is inserted plus its complemento
        children, atomically. This is a DataJud-sourced layer — it never reads
        or writes eSAJ-sourced columns.

        Returns: {"inserted": bool, "fetched_at": str, "enrichment_hash": str,
                  "reason": "first_enrichment" | "changed" | "unchanged"}
        """
        new_hash = compute_enrichment_hash(record.canonical_payload())
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT fetched_at, enrichment_hash FROM datajud_enrichment "
                "WHERE process_number = ? ORDER BY fetched_at DESC LIMIT 1",
                (record.process_number,),
            )
            latest = cur.fetchone()
            if latest is not None and latest["enrichment_hash"] == new_hash:
                self.logger.debug(
                    "save_datajud_enrichment: unchanged for %s (hash %s)",
                    record.process_number, new_hash[:12],
                )
                return {
                    "inserted": False,
                    "fetched_at": latest["fetched_at"],
                    "enrichment_hash": new_hash,
                    "reason": "unchanged",
                }

            fetched_at = _utc_now_iso()
            if latest is not None and fetched_at <= latest["fetched_at"]:
                fetched_at = _bump_microsecond(latest["fetched_at"])
            assuntos_json = (
                _canonical_json(record.assuntos)
                if record.assuntos is not None else None
            )
            cur.execute("BEGIN")
            try:
                cur.execute(
                    """
                    INSERT INTO datajud_enrichment (
                        process_number, fetched_at, enrichment_hash, datajud_found,
                        tribunal, grau, assuntos_json, codigo_municipio_ibge,
                        data_hora_ultima_atualizacao, data_hora_ultima_atualizacao_iso,
                        movimentos_count, complementos_count, raw_source_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.process_number,
                        fetched_at,
                        new_hash,
                        1 if record.datajud_found else 0,
                        record.tribunal,
                        record.grau,
                        assuntos_json,
                        record.codigo_municipio_ibge,
                        record.data_hora_ultima_atualizacao,
                        record.data_hora_ultima_atualizacao_iso,
                        record.movimentos_count,
                        len(record.complementos),
                        record.raw_source_json,
                    ),
                )
                if record.complementos:
                    cur.executemany(
                        """
                        INSERT INTO datajud_complemento (
                            process_number, fetched_at, movimento_indice,
                            movimento_data_hora, movimento_codigo, movimento_nome,
                            complemento_codigo, complemento_valor,
                            complemento_nome, complemento_descricao
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            (
                                record.process_number,
                                fetched_at,
                                c.movimento_indice,
                                c.movimento_data_hora,
                                c.movimento_codigo,
                                c.movimento_nome,
                                c.codigo,
                                c.valor,
                                c.nome,
                                c.descricao,
                            )
                            for c in record.complementos
                        ],
                    )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

        return {
            "inserted": True,
            "fetched_at": fetched_at,
            "enrichment_hash": new_hash,
            "reason": "first_enrichment" if latest is None else "changed",
        }

    def get_latest_enrichment(self, process_number: str) -> Optional[Dict[str, Any]]:
        """Most recent DataJud enrichment row for a process, or None."""
        cur = self._conn.cursor()
        cur.execute(
            "SELECT * FROM datajud_enrichment WHERE process_number = ? "
            "ORDER BY fetched_at DESC LIMIT 1",
            (process_number,),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def get_complementos(
        self, process_number: str, fetched_at: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """complementosTabelados rows for the given enrichment (latest if not
        specified). Returns [] if the process has no enrichment."""
        ts = fetched_at
        if ts is None:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT MAX(fetched_at) FROM datajud_enrichment WHERE process_number = ?",
                (process_number,),
            )
            row = cur.fetchone()
            ts = row[0] if row and row[0] else None
        if ts is None:
            return []
        cur = self._conn.cursor()
        cur.execute(
            "SELECT * FROM datajud_complemento "
            "WHERE process_number = ? AND fetched_at = ? "
            "ORDER BY movimento_codigo, complemento_codigo, complemento_valor",
            (process_number, ts),
        )
        return [dict(r) for r in cur.fetchall()]

    def count_enrichments(self, process_number: Optional[str] = None) -> int:
        cur = self._conn.cursor()
        if process_number is None:
            cur.execute("SELECT COUNT(*) FROM datajud_enrichment")
        else:
            cur.execute(
                "SELECT COUNT(*) FROM datajud_enrichment WHERE process_number = ?",
                (process_number,),
            )
        return int(cur.fetchone()[0])

    def list_unenriched_process_numbers(self, limit: Optional[int] = None) -> List[str]:
        """Distinct loaded process numbers that have no DataJud enrichment yet —
        the backfill universe for the Layer 3-lite CLI's --backfill-all mode.
        Mirrors backfill_other_processes' 'loaded and missing' selection."""
        sql = (
            "SELECT DISTINCT process_number FROM process_snapshot "
            "WHERE scrape_outcome = 'loaded' "
            "AND process_number NOT IN (SELECT process_number FROM datajud_enrichment) "
            "ORDER BY process_number"
        )
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        cur = self._conn.cursor()
        cur.execute(sql)
        return [r[0] for r in cur.fetchall()]

    def complemento_catalog(self) -> List[Dict[str, Any]]:
        """Distinct complemento tuples present across all CURRENT enrichments,
        with the parent movement code/name and occurrence/process counts.

        This is the EU-b catalog: what outcome codes actually exist, grouped by
        movement, so the operator can tell us which `(movimento_codigo,
        complemento_codigo, complemento_valor)` tuples mean what. Scoped to each
        process's latest fetched_at (superseded enrichments excluded)."""
        cur = self._conn.cursor()
        cur.execute(
            """
            WITH current AS (
              SELECT process_number, MAX(fetched_at) AS fetched_at
              FROM datajud_enrichment GROUP BY process_number
            )
            SELECT dc.movimento_codigo, dc.movimento_nome,
                   dc.complemento_codigo, dc.complemento_valor,
                   dc.complemento_nome, dc.complemento_descricao,
                   COUNT(*) AS occurrences,
                   COUNT(DISTINCT dc.process_number) AS process_count
            FROM datajud_complemento dc
            JOIN current c
              ON c.process_number = dc.process_number
             AND c.fetched_at = dc.fetched_at
            GROUP BY dc.movimento_codigo, dc.movimento_nome,
                     dc.complemento_codigo, dc.complemento_valor,
                     dc.complemento_nome, dc.complemento_descricao
            ORDER BY dc.movimento_codigo, dc.complemento_codigo, dc.complemento_valor
            """
        )
        return [dict(r) for r in cur.fetchall()]

    def enrichment_status(self, process_numbers: List[str]) -> List[Dict[str, Any]]:
        """Bulk "which of these have a current DataJud enrichment" — the
        Resultados "enriquecido" indicator. Chunked `IN` like snapshot_status.

        Per process: `enriched` (an enrichment row exists), `datajud_found` (the
        case was actually in DataJud's index — a stored row with found=0 means
        "checked, absent"), and the freshness `fetched_at`."""
        if not process_numbers:
            return []
        latest: Dict[str, Dict[str, Any]] = {}
        chunk = 1000
        cur = self._conn.cursor()
        for i in range(0, len(process_numbers), chunk):
            sub = process_numbers[i : i + chunk]
            placeholders = ",".join("?" * len(sub))
            cur.execute(
                f"""
                SELECT e.process_number, e.fetched_at, e.datajud_found
                FROM datajud_enrichment e
                JOIN (
                  SELECT process_number, MAX(fetched_at) AS ts
                  FROM datajud_enrichment
                  WHERE process_number IN ({placeholders})
                  GROUP BY process_number
                ) m ON m.process_number = e.process_number AND m.ts = e.fetched_at
                """,
                sub,
            )
            for row in cur.fetchall():
                latest[row["process_number"]] = {
                    "fetched_at": row["fetched_at"],
                    "datajud_found": bool(row["datajud_found"]),
                }
        out: List[Dict[str, Any]] = []
        for pn in process_numbers:
            row = latest.get(pn)
            out.append({
                "process_number": pn,
                "enriched": row is not None,
                "datajud_found": row["datajud_found"] if row else False,
                "fetched_at": row["fetched_at"] if row else None,
            })
        return out

    # ─────────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────────

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass
