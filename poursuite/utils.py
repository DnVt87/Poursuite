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
