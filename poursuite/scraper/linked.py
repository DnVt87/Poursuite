"""eSAJ linked-process parsers — Apensos and Incidentes sections.

Two sections of the consulta page enumerate linked processes:

  - "Apensos, Entranhados e Unificados" — populated DOM:
        <tbody id="dadosApenso">
          <tr>
            <td><a class="processoApensado" href="...">CNJ_NUMBER</a></td>
            <td>{Classe of linked process}</td>
            <td>{Apensamento date}</td>
            <td>{Motivo}</td>
          </tr>
          ...
        </tbody>
    Empty marker: <tbody id="dadosApensosNaoDisponiveis">

  - "Incidentes, ações incidentais, recursos e execuções de sentenças" —
    empty marker observed: <td id="processoSemIncidentes">
    Populated DOM not observed in the 13-case inventory; based on eSAJ
    JavaScript hooks (`.incidente` class), populated rows likely follow a
    similar table-of-anchors pattern. Parser scans for `<a class="incidente">`
    href anchors as a permissive fallback. When eSAJ surfaces a real
    populated incidente in production, the result should be validated and
    this parser refined.

`relationship_type` is the section-level type:
  - 'apenso'    — from the Apensos section
  - 'incidente' — from the Incidentes section

Finer subtypes (entranhado, unificado, recurso, execucao_sentenca) require
column data eSAJ doesn't expose in the table headers; deferred.
"""
from __future__ import annotations

import re
from typing import List

from bs4 import BeautifulSoup, Tag

from poursuite.models import LinkedProcess

# CNJ format pattern. Used for permissive extraction when the relationship
# type isn't obvious from the section header (e.g. malformed structure).
_CNJ_RE = re.compile(r"\b(\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4})\b")


def _find_section_h2(soup: BeautifulSoup, keyword: str) -> "Tag | None":
    """Find the h2.tituloDoBloco whose label contains `keyword` (case-insensitive)."""
    for h2 in soup.find_all("h2", class_="tituloDoBloco"):
        if keyword.lower() in h2.get_text(" ", strip=True).lower():
            return h2
    return None


def _slice_section_after(h2: Tag) -> List[Tag]:
    """Return root sibling tags that follow this h2's wrapper, up to the next h2."""
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


def parse_apensos(soup: BeautifulSoup) -> List[LinkedProcess]:
    """Parse the Apensos, Entranhados e Unificados section. Empty when no apensos."""
    h2 = _find_section_h2(soup, "Apensos")
    if h2 is None:
        return []
    root_tags = _slice_section_after(h2)
    # Look for the populated tbody first; bail if only the empty marker is present.
    for root in root_tags:
        if not hasattr(root, "find"):
            continue
        if root.find("tbody", id="dadosApensosNaoDisponiveis") is not None:
            return []
        tbody = root.find("tbody", id="dadosApenso")
        if tbody is None and root.name == "tbody" and root.get("id") == "dadosApenso":
            tbody = root
        if tbody is not None:
            return _extract_apenso_rows(tbody)
    # Fallback: no tbody found in section. Treat as empty rather than guess.
    return []


def _extract_apenso_rows(tbody: Tag) -> List[LinkedProcess]:
    out: List[LinkedProcess] = []
    for tr in tbody.find_all("tr"):
        anchor = tr.find("a", class_="processoApensado")
        if anchor is None:
            # Defensive: not every row has the expected anchor structure.
            text = tr.get_text(" ", strip=True)
            match = _CNJ_RE.search(text)
            if match:
                out.append(LinkedProcess(
                    linked_number=match.group(1),
                    relationship_type="apenso",
                ))
            continue
        number = anchor.get_text(strip=True)
        if not _CNJ_RE.fullmatch(number):
            # eSAJ surfaces something else in the anchor text. Try the href.
            m = _CNJ_RE.search(anchor.get("href") or "")
            if m:
                number = m.group(1)
            else:
                continue
        out.append(LinkedProcess(
            linked_number=number,
            relationship_type="apenso",
        ))
    return out


def parse_incidentes(soup: BeautifulSoup) -> List[LinkedProcess]:
    """Parse the Incidentes section.

    Inventory data only included the empty-state DOM (`<td id="processoSemIncidentes">`).
    Populated structure is inferred from eSAJ JavaScript (`.incidente` class).
    Returns [] when the empty marker is present; otherwise scans the section
    for anchors with class `incidente` or `processoApensado` (eSAJ sometimes
    reuses the apensado class across linked-process tables) and for CNJ-shaped
    text as a permissive fallback.
    """
    h2 = _find_section_h2(soup, "Incidentes")
    if h2 is None:
        return []
    root_tags = _slice_section_after(h2)

    # Check for empty marker first.
    for root in root_tags:
        if hasattr(root, "find") and root.find(id="processoSemIncidentes") is not None:
            return []

    out: List[LinkedProcess] = []
    seen: set = set()
    for root in root_tags:
        if not hasattr(root, "find_all"):
            continue
        anchors = []
        anchors.extend(root.find_all("a", class_="incidente"))
        anchors.extend(root.find_all("a", class_="processoApensado"))
        for a in anchors:
            number = a.get_text(strip=True)
            if not _CNJ_RE.fullmatch(number):
                m = _CNJ_RE.search(a.get("href") or "")
                if not m:
                    continue
                number = m.group(1)
            if number in seen:
                continue
            seen.add(number)
            out.append(LinkedProcess(
                linked_number=number,
                relationship_type="incidente",
            ))
    return out


def parse_linked(soup: BeautifulSoup) -> List[LinkedProcess]:
    """Return all linked processes from both Apensos and Incidentes sections."""
    return parse_apensos(soup) + parse_incidentes(soup)
