"""eSAJ movimentações timeline parser.

Each movimento on the consulta page is one `<tr class="containerMovimentacao">`
inside `<tbody id="tabelaTodasMovimentacoes">` (after the `#linkmovimentacoes`
expand) or `<tbody id="tabelaUltimasMovimentacoes">` (the default visible
subset before expand).

Row anatomy:
    <tr class="containerMovimentacao">
        <td class="dataMovimentacao">DD/MM/YYYY</td>
        <td>&nbsp;</td>  -- spacer
        <td class="descricaoMovimentacao">
            {nome text}
            <br>
            <span style="font-style: italic;">{complementos free text}</span>
        </td>
    </tr>

eSAJ exposes nome (human label) and the italic complementos span. It does NOT
expose the TPU `codigo` — production code that needs the code path through
DataJud (Layer 3) for enrichment.
"""
from __future__ import annotations

import re
from typing import List, Optional
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup, Tag

from poursuite.models import Movimento


_DATE_RE = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")
_WS_RE = re.compile(r"\s+")
_CD_DOC_RE = re.compile(r"[?&]cdDocumento=(\d+)")


def _normalize_date(raw: str) -> Optional[str]:
    """DD/MM/YYYY → YYYY-MM-DD when parseable; raw string otherwise; None if empty."""
    if not raw:
        return None
    s = raw.strip()
    if not s:
        return None
    m = _DATE_RE.match(s)
    if not m:
        return s
    d, mo, y = m.groups()
    return f"{y}-{mo}-{d}"


def _collapse_ws(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    cleaned = _WS_RE.sub(" ", s).strip()
    return cleaned or None


def _find_complementos_span(desc_td: Tag) -> Optional[Tag]:
    """Locate the italic complementos span inside a descricaoMovimentacao cell."""
    return desc_td.find(
        "span",
        style=lambda v: bool(v) and "italic" in v.lower(),
    )


def _extract_cd_documento(tr: Tag) -> Optional[str]:
    """Pull the cdDocumento query-string value from any linkMovVincProc anchor
    inside this movimento row. Returns the first match (eSAJ duplicates the
    same id across icon + text anchors). None if the movimento has no doc."""
    anchor = tr.find("a", class_="linkMovVincProc")
    if anchor is None:
        return None
    href = anchor.get("href") or ""
    m = _CD_DOC_RE.search(href)
    return m.group(1) if m else None


def _extract_movimento_row(tr: Tag, ordem: int) -> Movimento:
    date_td = tr.find("td", class_="dataMovimentacao")
    data_hora = _normalize_date(date_td.get_text(strip=True) if date_td else "")

    desc_td = tr.find("td", class_="descricaoMovimentacao")
    if desc_td is None:
        return Movimento(ordem=ordem, data_hora=data_hora, nome="")

    comp_span = _find_complementos_span(desc_td)
    complementos_text = (
        _collapse_ws(comp_span.get_text(" ", strip=True)) if comp_span else None
    )

    # Nome is the descricaoMovimentacao text MINUS the complementos span text.
    # Approach: pull the cell's full text, then strip the complementos suffix.
    # Falls back to the full text if the complementos isn't a clean suffix
    # (rare; happens if eSAJ ever puts text after the italic span).
    full_text = _collapse_ws(desc_td.get_text(" ", strip=True)) or ""
    if complementos_text and full_text.endswith(complementos_text):
        nome = full_text[: -len(complementos_text)].strip()
    else:
        nome = full_text

    return Movimento(
        ordem=ordem,
        data_hora=data_hora,
        codigo=None,            # eSAJ doesn't expose TPU code in this DOM
        nome=nome,
        complementos_text=complementos_text,
        complementos_json=None,  # structured complementos parsing deferred
        cd_documento=_extract_cd_documento(tr),
    )


def parse_movimentos(soup: BeautifulSoup) -> List[Movimento]:
    """Parse the full movimentos timeline from a post-expand page soup.

    Prefers `#tabelaTodasMovimentacoes` (visible only after `#linkmovimentacoes`
    has been clicked). Falls back to `#tabelaUltimasMovimentacoes` — that
    fallback only returns the few most recent rows and indicates the expand
    click failed; the caller should treat a fallback-only timeline as partial
    and surface an anomaly.

    Returns movimentos in eSAJ wire order (newest first), with `ordem` =
    position in the list (0-based). `ordem` is wire-order metadata only;
    do NOT include it in any hash that's meant to be stable across scrapes.
    """
    tbody = soup.find("tbody", id="tabelaTodasMovimentacoes")
    if tbody is None:
        tbody = soup.find("tbody", id="tabelaUltimasMovimentacoes")
    if tbody is None:
        return []

    rows = tbody.find_all("tr", class_="containerMovimentacao")
    return [_extract_movimento_row(tr, ordem) for ordem, tr in enumerate(rows)]


def is_full_timeline(soup: BeautifulSoup) -> bool:
    """True iff `#tabelaTodasMovimentacoes` is present (i.e., expand succeeded).

    Production scrapers should check this after clicking `#linkmovimentacoes`
    and log an anomaly when False so partial-timeline cases are visible.
    """
    return soup.find("tbody", id="tabelaTodasMovimentacoes") is not None
