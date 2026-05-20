"""Process-group store — a JSON file at SNAPSHOT_DIR/process_groups.json.

Carteiras are upload provenance, not analytical workspaces (per UI Phase 2.5
brief, Patch 1). A single JSON file is sufficient: 2-3 users, write-rename
atomicity, in-process threading lock. No database table, no migrations.

Shape on disk:

    {
      "carteira_itau_2026_05": {
        "name": "Carteira Itaú · maio/2026",
        "created_at": "2026-05-16T14:32:00Z",
        "process_numbers": ["10180455020158260506", ...]
      },
      ...
    }
"""
from __future__ import annotations

import json
import os
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from poursuite.config import SNAPSHOT_DIR

DEFAULT_PATH: Path = SNAPSHOT_DIR / "process_groups.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def _slugify(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower())
    s = s.strip("_")
    return s or "carteira"


class ProcessGroupStore:
    """JSON-backed store with write-rename atomicity and a per-instance lock.

    Single-writer assumption: uvicorn runs with --workers 1, so the lock is
    sufficient. The write-rename pattern means a crash mid-write leaves the
    previous valid file intact (atomic at the filesystem level on Windows
    via os.replace).
    """

    def __init__(self, path: Optional[Path] = None) -> None:
        self.path = Path(path) if path is not None else DEFAULT_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    # ─── persistence ─────────────────────────────────────────────────

    def _load(self) -> Dict[str, Dict[str, Any]]:
        if not self.path.exists():
            return {}
        with self.path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _save(self, payload: Dict[str, Dict[str, Any]]) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, self.path)

    # ─── public API ──────────────────────────────────────────────────

    def list_groups(self) -> List[Dict[str, Any]]:
        """Return groups without their process_numbers list (lightweight)."""
        with self._lock:
            data = self._load()
        return [
            {
                "group_id": gid,
                "name": g["name"],
                "created_at": g["created_at"],
                "count": len(g.get("process_numbers", [])),
            }
            for gid, g in data.items()
        ]

    def get_group(self, group_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            data = self._load()
        g = data.get(group_id)
        if g is None:
            return None
        return {
            "group_id": group_id,
            "name": g["name"],
            "created_at": g["created_at"],
            "process_numbers": list(g.get("process_numbers", [])),
        }

    def create_group(self, name: str, process_numbers: List[str]) -> Dict[str, Any]:
        if not name or not isinstance(name, str):
            raise ValueError("name is required")
        if not isinstance(process_numbers, list) or not process_numbers:
            raise ValueError("process_numbers must be a non-empty list")
        with self._lock:
            data = self._load()
            base = _slugify(name)
            gid = base
            suffix = 2
            while gid in data:
                gid = f"{base}_{suffix}"
                suffix += 1
            data[gid] = {
                "name": name,
                "created_at": _utc_now_iso(),
                "process_numbers": list(dict.fromkeys(process_numbers)),  # dedup, preserve order
            }
            self._save(data)
        return self.get_group(gid)  # type: ignore[return-value]

    def delete_group(self, group_id: str) -> bool:
        with self._lock:
            data = self._load()
            if group_id not in data:
                return False
            del data[group_id]
            self._save(data)
        return True
