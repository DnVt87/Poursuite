"""poursuite.probes — diagnostic probes for DataJud and Receita CNPJ.

Read PROBE_FINDINGS.md (project root) for the empirical questions these
probes try to answer. This package is investigation-only: nothing here is
imported by poursuite.api, poursuite.db, or poursuite.scraper.
"""
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from poursuite.config import LOG_DIR

PROBES_ROOT: Path = LOG_DIR / "probes"


def make_run_dir(prefix: str) -> Path:
    """Create and return a fresh timestamped run dir under PROBES_ROOT."""
    PROBES_ROOT.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = PROBES_ROOT / f"{prefix}_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def latest_run_dir(prefix: str) -> Optional[Path]:
    """Return the most recent run dir for a given probe prefix, or None."""
    if not PROBES_ROOT.exists():
        return None
    candidates = sorted(
        p for p in PROBES_ROOT.iterdir()
        if p.is_dir() and p.name.startswith(f"{prefix}_")
    )
    return candidates[-1] if candidates else None


def write_json(path: Path, data: object) -> None:
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )


def read_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))
