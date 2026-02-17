import logging
import os
import sqlite3
import threading
from pathlib import Path
from typing import Dict, Optional

from poursuite.config import DB_DIR
from poursuite.models import DatabaseInfo
from poursuite.utils import setup_logging


class DatabaseManager:
    """Handles discovery and lifecycle of all SQLite database connections."""

    def __init__(self, db_dir: Path = DB_DIR) -> None:
        self.db_dir = db_dir
        self.logger: logging.Logger = setup_logging("search_engine")
        self._db_cache: Dict[str, sqlite3.Connection] = {}
        self._cache_lock = threading.Lock()
        self.db_info: Dict[str, DatabaseInfo] = self._discover_databases()

    def _discover_databases(self) -> Dict[str, DatabaseInfo]:
        """Discover and validate all database files in db_dir."""
        databases = {}

        if not self.db_dir.exists():
            self.logger.warning(f"Database directory {self.db_dir} not found")
            return databases

        for db_path in sorted(self.db_dir.glob('*.db')):
            try:
                db_id = db_path.stem

                with sqlite3.connect(str(db_path)) as conn:
                    cursor = conn.cursor()

                    cursor.execute("""
                        SELECT name FROM sqlite_master
                        WHERE type='table' AND name='paragraphs'
                    """)
                    if not cursor.fetchone():
                        self.logger.warning(f"Database {db_path} missing paragraphs table, skipping")
                        continue

                    cursor.execute("""
                        SELECT MIN(document_date), MAX(document_date)
                        FROM paragraphs
                    """)
                    start_date, end_date = cursor.fetchone()

                    size_mb = db_path.stat().st_size / (1024 * 1024)

                    databases[db_id] = DatabaseInfo(
                        path=db_path,
                        start_date=start_date,
                        end_date=end_date,
                        size_mb=size_mb
                    )

                    self.logger.info(f"Found database {db_id}: {start_date} to {end_date}, {size_mb:.2f} MB")

            except Exception as e:
                self.logger.error(f"Error validating database {db_path}: {e}")

        self.logger.info(f"Discovered {len(databases)} valid databases")
        return databases

    def get_connection(self, db_id: str) -> Optional[sqlite3.Connection]:
        """Get a cached connection to a database. Thread-safe."""
        if db_id not in self.db_info:
            return None

        with self._cache_lock:
            if db_id not in self._db_cache:
                try:
                    path = self.db_info[db_id].path
                    conn = sqlite3.connect(str(path), check_same_thread=False)
                    conn.row_factory = sqlite3.Row
                    self._db_cache[db_id] = conn
                except Exception as e:
                    self.logger.error(f"Error connecting to database {db_id}: {e}")
                    return None
            return self._db_cache[db_id]

    def close_connections(self) -> None:
        """Close all open database connections. Call only at application shutdown."""
        with self._cache_lock:
            for db_id, conn in self._db_cache.items():
                try:
                    conn.close()
                except Exception:
                    pass
            self._db_cache = {}

    def get_database_stats(self) -> Dict:
        """
        Return metadata about all available databases.
        Lazy: no COUNT(*) queries — only information collected during discovery.
        """
        stats = {
            'total_databases': len(self.db_info),
            'total_size_mb': sum(info.size_mb for info in self.db_info.values()),
            'date_range': {'earliest': None, 'latest': None},
            'databases': {},
        }

        all_start_dates = [info.start_date for info in self.db_info.values() if info.start_date]
        all_end_dates = [info.end_date for info in self.db_info.values() if info.end_date]

        if all_start_dates:
            stats['date_range']['earliest'] = min(all_start_dates)
        if all_end_dates:
            stats['date_range']['latest'] = max(all_end_dates)

        for db_id, info in self.db_info.items():
            stats['databases'][db_id] = {
                'size_mb': info.size_mb,
                'date_range': f"{info.start_date} to {info.end_date}" if info.start_date else "Unknown",
            }

        return stats
