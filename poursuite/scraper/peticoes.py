"""eSAJ petições parser — the Petições diversas section.

Section DOM (after the h2.tituloDoBloco "Petições diversas"):
    <table>
      <thead><tr class="label"><th>Data</th><th>Tipo</th></tr> ...</thead>
      <tbody>
        <tr class="fundoClaro">
          <td>DD/MM/YYYY</td>
          <td>{Tipo} <br></td>
        </tr>
        ...
      </tbody>
    </table>

eSAJ does NOT expose `cd_documento` on petição rows in this section. The
schema's `peticao.cd_documento` column is kept NULL for forward compat —
if eSAJ ever surfaces document IDs on petições, the parser can populate
without a schema change. Document IDs DO appear on movimento rows; see
poursuite/scraper/movimentos.py.
"""
from __future__ import annotations

import re
from typing import List

from bs4 import BeautifulSoup, Tag

from poursuite.models import Peticao

_DATE_RE = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")
_WS_RE = re.compile(r"\s+")


def _normalize_date(raw: str) -> "str | None":
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


def _collapse_ws(s: "str | None") -> "str | None":
    if not s:
        return None
    cleaned = _WS_RE.sub(" ", s).strip()
    return cleaned or None


def _find_petições_section(soup: BeautifulSoup) -> "Tag | None":
    for h2 in soup.find_all("h2", class_="tituloDoBloco"):
        text = h2.get_text(" ", strip=True).lower()
        if "petições diversas" in text or "peticoes diversas" in text:
            return h2
    return None


def _slice_section_after(h2: Tag) -> List[Tag]:
    wrapper = h2.parent if h2.parent is not None else h2
    out: List[Tag] = []
    sib = wrapper.next_sibling
    while sib is not None:
        if hasattr(sib, "name") and sib.name:
            inner_h2 = (sib.find("h2", class_="tituloDoBloco")
                        if hasattr(sib, "find") else None)
            if inner_h2 is not None:
                break
            out.append(sib)
        sib = sib.next_sibling
    return out


def parse_peticoes(soup: BeautifulSoup) -> List[Peticao]:
    """Parse the Petições diversas section into a list of Peticao rows.

    Returns [] when the section is absent or empty. Order matches eSAJ's
    display order (typically chronological-ascending, oldest first).
    """
    h2 = _find_petições_section(soup)
    if h2 is None:
        return []
    root_tags = _slice_section_after(h2)

    # Find the petições table — the first table in the section that has
    # a Data column. The section sometimes has multiple wrapper divs.
    candidate_tables: List[Tag] = []
    for root in root_tags:
        if not hasattr(root, "find_all"):
            continue
        if root.name == "table":
            candidate_tables.append(root)
        candidate_tables.extend(root.find_all("table"))

    out: List[Peticao] = []
    ordem = 0
    seen_tables: set = set()
    for table in candidate_tables:
        tid = id(table)
        if tid in seen_tables:
            continue
        seen_tables.add(tid)

        # Header sanity: must contain "Data" and "Tipo" cells. Cheap filter
        # so we don't accidentally pick up a non-petições table that landed
        # in this section's slice.
        headers = [th.get_text(" ", strip=True).lower()
                   for th in table.find_all("th")]
        if not ("data" in headers and "tipo" in headers):
            continue

        tbody = table.find("tbody")
        if tbody is None:
            continue
        for tr in tbody.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue
            data = _normalize_date(tds[0].get_text(strip=True))
            tipo = _collapse_ws(tds[1].get_text(" ", strip=True))
            if not data and not tipo:
                continue
            out.append(Peticao(
                ordem=ordem,
                data=data,
                tipo=tipo,
                cd_documento=None,
            ))
            ordem += 1
        # Petições section typically has one table; once we've parsed it, stop.
        break
    return out
