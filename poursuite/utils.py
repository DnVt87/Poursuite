import logging
import re
import zlib
from pathlib import Path
from typing import Optional

from poursuite.config import LOG_DIR


def setup_logging(name: str, log_file: Optional[Path] = None) -> logging.Logger:
    """
    Configure and return a named logger with file + console handlers.
    If log_file is None, defaults to LOG_DIR / f"{name}.log".
    Guard against duplicate handlers so multiple imports don't stack them.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    if log_file is None:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_file = LOG_DIR / f"{name}.log"

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    fh = logging.FileHandler(str(log_file))
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


def decompress_content(content) -> str:
    """
    Decompress zlib-compressed content bytes; pass through plain strings unchanged.
    """
    if isinstance(content, bytes):
        try:
            return zlib.decompress(content).decode('utf-8')
        except zlib.error:
            return content.decode('utf-8', errors='replace')
    return content


def format_currency(value: str) -> Optional[str]:
    """
    Format a currency string to ensure a single space after 'R$'.
    """
    if not value:
        return None
    value = re.sub(r'\s+', '', value)
    if value.startswith('R$'):
        value = 'R$ ' + value[2:]
    return value


def parse_brl_to_centavos(value: Optional[str]) -> Optional[int]:
    """Parse a BRL string like 'R$ 1.234,56' or '1234,56' to integer centavos.

    Returns None on missing/unparseable input. Zero is treated as missing data
    (eSAJ frequently emits 'R$ 0,00' for cases with no declared valor) so that
    histograms and stats don't count it as a real R$ 0 case.
    """
    if not value or not isinstance(value, str):
        return None
    cleaned = re.sub(r'[^\d,]', '', value)
    if not cleaned:
        return None
    if ',' in cleaned:
        whole, _, frac = cleaned.partition(',')
        frac = (frac + '00')[:2]
    else:
        whole, frac = cleaned, '00'
    if not whole:
        return None
    try:
        cents = int(whole) * 100 + int(frac)
    except ValueError:
        return None
    return cents if cents > 0 else None


def parse_brazilian_date_to_iso(value: Optional[str]) -> Optional[str]:
    """Parse 'DD/MM/YYYY' (optionally followed by HH:MM[:SS]) to ISO 'YYYY-MM-DD'.

    Returns None on missing/unparseable input. Only checks structural validity
    (digit counts and ranges) — does not validate month-day combinations like
    31/02. That's good enough for SQL ordering and bucket math.
    """
    if not value or not isinstance(value, str):
        return None
    m = re.match(r'\s*(\d{2})/(\d{2})/(\d{4})', value)
    if not m:
        return None
    dd, mm, yyyy = m.groups()
    try:
        d, mo, y = int(dd), int(mm), int(yyyy)
    except ValueError:
        return None
    if not (1 <= d <= 31 and 1 <= mo <= 12 and 1900 <= y <= 2100):
        return None
    return f"{yyyy}-{mm}-{dd}"


# Characters that are genuinely dangerous in FTS5 (cause syntax errors) but are NOT
# part of valid query syntax. Parentheses, AND/OR/NOT, quotes, and * are valid FTS5
# syntax and must NOT be escaped.
_FTS_UNSAFE = re.compile(r'([\\^])')


def sanitize_fts_query(query: str) -> str:
    """
    Minimally sanitize a user-supplied FTS5 query string.

    Preserved as-is:
      - Boolean operators: AND, OR, NOT
      - Quoted phrases: "some phrase"
      - Grouping parentheses: (SISBAJUD OR BACENJUD)
      - Prefix wildcards: word*

    Escaped (genuinely break SQLite FTS5):
      - Backslash
      - Caret
    """
    tokens = re.findall(r'(?:"[^"]*"|\S)+', query)
    sanitized = []
    for token in tokens:
        if token.upper() in ('AND', 'OR', 'NOT'):
            sanitized.append(token.upper())
        elif token.startswith('"') and token.endswith('"'):
            sanitized.append(token)
        else:
            sanitized.append(_FTS_UNSAFE.sub(r'\\\1', token))
    return ' '.join(sanitized)
