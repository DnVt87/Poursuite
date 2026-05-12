"""Shared eSAJ section-walking primitives.

eSAJ's consulta page is a single long document. Sections are delimited by
`<h2 class="tituloDoBloco">` headers (Partes do processo, Movimentações,
Petições diversas, Incidentes..., Apensos..., Audiências, and occasionally
Histórico de classes). Section content is rendered as the h2 wrapper's
following siblings until the next h2.tituloDoBloco.

Each section has its own inline expand-collapse anchor that toggles between
"shown" and "all" tables. The two anchors with stable IDs are:
  - linkpartes:         tablePartesPrincipais  ↔ tableTodasPartes
  - linkmovimentacoes:  tabelaUltimasMovimentacoes ↔ tabelaTodasMovimentacoes

This module ships three primitives used by both the inventory probe and the
production scraper:

  expand_section_collapsibles(driver, ids=...)
    Click each given section-level expand link. Returns the ids actually
    clicked. Idempotent and tolerant of missing/non-clickable links.

  enumerate_sections(soup) -> [{label, h2_id, wrapper_classes, root_tags}, ...]
    Find all h2.tituloDoBloco and slice the DOM into per-section root-tag
    lists. Returns the structured slices without aggregating content.

  walk_section(section) -> {label, text_*, tables, list_counts, ...}
    Aggregate stats from a section's root tags — text length / preview,
    table summaries, list counts, anchor count, filter inputs, pagination
    hints. Used by the inventory probe; production code typically passes
    `root_tags` to its own section-specific parsers instead.
"""
from __future__ import annotations

import re
import time
from typing import Any, Dict, Iterable, List, Tuple

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


SECTION_HEADER_CLASS = "tituloDoBloco"

# Stable IDs of inline expand-collapse anchors inside section blocks.
# Order intentionally lists `linkmovimentacoes` last because the movimentos
# table is larger; clicking it last keeps the page reflow predictable.
SECTION_COLLAPSIBLE_IDS: Tuple[str, ...] = (
    "linkpartes",
    "linkmovimentacoes",
)


def expand_section_collapsibles(
    driver: webdriver.Chrome,
    ids: Iterable[str] = SECTION_COLLAPSIBLE_IDS,
    timeout_per_id: int = 3,
    settle_seconds: float = 0.4,
) -> List[str]:
    """Click each named section-level expand link. Returns the ids actually
    clicked. Tolerates missing/non-clickable links — callers should treat the
    absence of an id from the return list as informational, not as failure.
    """
    clicked: List[str] = []
    for link_id in ids:
        try:
            link = WebDriverWait(driver, timeout_per_id).until(
                EC.element_to_be_clickable((By.ID, link_id))
            )
            driver.execute_script("arguments[0].click();", link)
            clicked.append(link_id)
            time.sleep(settle_seconds)
        except (TimeoutException, NoSuchElementException, WebDriverException):
            continue
    return clicked


def enumerate_sections(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """Find h2.tituloDoBloco headers and slice the DOM into per-section root tags.

    Each section's `root_tags` is the list of h2-wrapper's following siblings
    (in document order) up to but not including the wrapper containing the
    next h2.tituloDoBloco. Stripping is intentionally minimal — callers do
    their own parsing on the returned tag list.
    """
    sections: List[Dict[str, Any]] = []
    for h2 in soup.find_all("h2", class_=SECTION_HEADER_CLASS):
        label = re.sub(r"\s+", " ", h2.get_text(" ", strip=True)).strip()
        wrapper = h2.parent if h2.parent is not None else h2
        root_tags: List[Any] = []
        sib = wrapper.next_sibling
        while sib is not None:
            if hasattr(sib, "name") and sib.name:
                inner_h2 = (sib.find("h2", class_=SECTION_HEADER_CLASS)
                            if hasattr(sib, "find") else None)
                if inner_h2 is not None:
                    break
                root_tags.append(sib)
            sib = sib.next_sibling
        sections.append({
            "label": label or "(unnamed section)",
            "h2_id": h2.get("id"),
            "wrapper_classes": (
                " ".join(wrapper.get("class") or [])
                if hasattr(wrapper, "get") else ""
            ),
            "root_tags": root_tags,
        })
    return sections


def walk_section(section: Dict[str, Any]) -> Dict[str, Any]:
    """Aggregate stats from a section's list of root sibling tags.

    Captures: text length / preview, table summaries, list counts, anchor
    counts, filter inputs, pagination hints. Operates on the pre-fetched
    BeautifulSoup tags — no driver interaction.
    """
    label = section["label"]
    root_tags = section["root_tags"]
    text_parts: List[str] = []
    table_summaries: List[Dict[str, Any]] = []
    list_counts = {"ul": 0, "ol": 0, "li": 0}
    filters: List[Dict[str, str]] = []
    pagination: Dict[str, Any] = {"text_hits": [], "has_pagination_class": False}
    anchor_count = 0
    pag_classes = ("paginacao", "pagination", "pagina")

    def _summarize_table(table) -> Dict[str, Any]:
        headers = [th.get_text(" ", strip=True) for th in table.find_all("th")]
        rows = table.find_all("tr")
        sample_row = None
        for r in rows:
            cells = r.find_all("td")
            if cells:
                sample_row = [c.get_text(" ", strip=True) for c in cells]
                break
        return {
            "id": table.get("id"),
            "classes": " ".join(table.get("class") or []),
            "header_count": len(headers),
            "headers": headers[:30],
            "row_count": sum(1 for r in rows if r.find_all("td")),
            "sample_row": sample_row,
        }

    seen_table_ids: set = set()
    for root in root_tags:
        if not hasattr(root, "name") or not root.name:
            continue
        try:
            text_parts.append(root.get_text(" ", strip=True))
        except Exception:
            pass

        candidate_tables: List[Any] = []
        if root.name == "table":
            candidate_tables.append(root)
        candidate_tables.extend(root.find_all("table"))
        for table in candidate_tables:
            tid = id(table)
            if tid in seen_table_ids:
                continue
            seen_table_ids.add(tid)
            table_summaries.append(_summarize_table(table))

        anchor_count += sum(1 for _ in root.find_all("a"))
        if root.name == "a":
            anchor_count += 1

        list_counts["ul"] += sum(1 for _ in root.find_all("ul"))
        list_counts["ol"] += sum(1 for _ in root.find_all("ol"))
        list_counts["li"] += sum(1 for _ in root.find_all("li"))

        candidate_inputs: List[Any] = []
        if root.name in ("input", "select", "textarea"):
            candidate_inputs.append(root)
        candidate_inputs.extend(root.find_all(["input", "select", "textarea"]))
        for inp in candidate_inputs:
            if inp.get("type") == "hidden":
                continue
            filters.append({
                "tag": inp.name,
                "type": inp.get("type") or "",
                "name": inp.get("name") or "",
                "id": inp.get("id") or "",
                "placeholder": inp.get("placeholder") or "",
            })

        for el in root.find_all(class_=lambda c: bool(c) and any(m in c for m in pag_classes)):
            pagination["has_pagination_class"] = True
            pagination["text_hits"].append(el.get_text(" ", strip=True)[:200])

    text = " ".join(t for t in text_parts if t)
    m = re.search(r"P[áa]gina\s+\d+\s+de\s+\d+", text, flags=re.IGNORECASE)
    if m:
        pagination["text_hits"].append(m.group(0))
    m = re.search(r"(\d+)\s+movimentos?", text, flags=re.IGNORECASE)
    if m:
        pagination["movimentos_total_hint"] = int(m.group(1))

    return {
        "label": label,
        "h2_id": section.get("h2_id"),
        "root_tag_count": len(root_tags),
        "text_length": len(text),
        "text_preview": text[:1000],
        "tables": table_summaries[:30],
        "table_count_total": len(table_summaries),
        "list_counts": list_counts,
        "anchors_in_section": anchor_count,
        "filters": filters,
        "pagination": pagination,
    }
