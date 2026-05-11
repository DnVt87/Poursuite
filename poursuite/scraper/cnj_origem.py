"""Derive foro/tribunal/year from a CNJ-formatted process number.

CNJ Resolução 65/2008 format: `NNNNNNN-DD.AAAA.J.TR.OOOO`
  - NNNNNNN : 7-digit sequential
  - DD      : 2-digit check
  - AAAA    : 4-digit year
  - J       : 1-digit segmento da justiça (8 = Justiça Estadual)
  - TR      : 2-digit tribunal (26 = TJSP)
  - OOOO    : 4-digit origem/foro code

The origem code maps to a foro name via a vendored lookup table —
see cnj_origem_table.json (sourced from the TJSP official PDF).
Currently covers segment 8, tribunal 26 (TJSP) only. The table
exposes its provenance via a `_metadata` block which the loader
strips before lookup.
"""
import json
import re
from pathlib import Path
from typing import Dict, Optional

_TABLE_PATH = Path(__file__).parent / "cnj_origem_table.json"

# Strict CNJ format. Groups: sequential, check, year, segment, tribunal, origem.
_CNJ_RE = re.compile(r"^(\d{7})-(\d{2})\.(\d{4})\.(\d)\.(\d{2})\.(\d{4})$")


def _load_table() -> Dict[str, str]:
    raw = json.loads(_TABLE_PATH.read_text(encoding="utf-8"))
    # Strip metadata entries — keys starting with '_' are not foro codes.
    return {k: v for k, v in raw.items() if not k.startswith("_")}


_ORIGEM_TABLE: Dict[str, str] = _load_table()


def derive_from_cnj(process_number: str) -> Dict[str, Optional[str]]:
    """Parse a CNJ process number and return its component codes + foro name.

    On malformed input or empty string, returns all-None values rather than
    raising. The caller is the scraper which never wants a derivation
    failure to abort the wider scrape.

    `foro_name` is None when the origem code isn't in the vendored table —
    that's possible for codes added after the table snapshot, and should
    be treated as a soft signal to refresh the table, not as an error.
    """
    if not process_number:
        return _empty_result()
    m = _CNJ_RE.match(process_number.strip())
    if not m:
        return _empty_result()
    _, _, year, segment, tribunal, origem = m.groups()
    return {
        "foro_code": origem,
        "tribunal_code": tribunal,
        "segment_code": segment,
        "distribution_year": year,
        "foro_name": _ORIGEM_TABLE.get(origem),
    }


def _empty_result() -> Dict[str, None]:
    return {
        "foro_code": None,
        "tribunal_code": None,
        "segment_code": None,
        "distribution_year": None,
        "foro_name": None,
    }
