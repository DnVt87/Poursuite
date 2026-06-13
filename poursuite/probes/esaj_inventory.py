"""eSAJ Full Inventory probe.

Walks every TJSP "Consulta de Processo" page top-to-bottom for a sample of
process numbers and records EVERYTHING — header fields, sections, clickables,
badges, conditional renderings — without filtering by perceived utility.

The goal is a comprehensive catalog the operator can review to decide what
to add to the production scraper. This module makes no recommendations.

Reference: eSAJ full-inventory pass (see ARCHITECTURE.md §10.4).
"""
import json
import logging
import re
import time
import traceback
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qs

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

from poursuite.config import (
    ESAJ_SEALED_ELEMENT_ID,
    ESAJ_SEALED_TEXT,
    ESAJ_URL,
    PROCESS_NUMBER_PATTERN_STRICT,
)
from poursuite.probes import write_json
from poursuite.scraper._chrome import configure_chrome_options
from poursuite.scraper.sections import (
    SECTION_COLLAPSIBLE_IDS as _SECTION_COLLAPSIBLE_IDS,
    enumerate_sections as _enumerate_sections,
    expand_section_collapsibles as _expand_section_collapsibles,
    walk_section as _walk_section,
)


# Authoritative mapping from the eSAJ inventory viewport re-run pass,
# "Section 7 reconciled gap list". Maps eSAJ raw header label → production
# scraper field name. Labels NOT in this map are gaps (the production scraper
# does not extract them). The brief lists this as ground truth — use it
# directly rather than re-deriving from FIELD_MAPPINGS via heuristic match.
PRODUCTION_SCRAPER_LABEL_MAP = {
    "Classe": "class_type",
    "Assunto": "subject",
    "Distribuição": "initial_date",
    "Valor da ação": "value",
}

# Production scraper also extracts these via non-label DOM (movimentações
# table, status badge, parties cells). They aren't header label-value pairs,
# so the walker won't surface them in the field catalog — listed here so the
# report can note they're already covered.
PRODUCTION_SCRAPER_NON_LABEL_FIELDS = (
    "last_movement (via td.dataMovimentacao)",
    "status (via #labelSituacaoProcesso.unj-tag)",
    "plaintiff (first td.nomeParteEAdvogado)",
    "defendant (second td.nomeParteEAdvogado)",
    "other_processes (separate name search)",
)


# Conceptual archetypes the operator's brief asks us to cover.
ARCHETYPES = [
    "Execução de título extrajudicial",
    "Cumprimento de sentença",
    "Execução fiscal",
    "Multi-party (>2 polos)",
    "Sealed (segredo de justiça)",
    "Migrated/legacy (pre-2010)",
    "Recent case still in citação phase",
    "Case with active penhora movimento",
    "Case with embargos à execução / linked processes",
    "High-volume movimentações (>200 movimentos)",
]


# ---------------------------------------------------------------------------
# Sample loader — minimal YAML for `samples: [- key: value, ...]` shape
# ---------------------------------------------------------------------------

def load_samples(path: Path) -> List[Dict[str, str]]:
    text = path.read_text(encoding="utf-8")
    try:
        data = json.loads(text)
        if isinstance(data, dict) and "samples" in data:
            return [_coerce_case(x) for x in data["samples"]]
        if isinstance(data, list):
            return [_coerce_case(x) for x in data]
    except json.JSONDecodeError:
        pass
    return _parse_dict_yaml_samples(text)


def _coerce_case(x: Any) -> Dict[str, str]:
    if isinstance(x, str):
        return {"number": x.strip(), "source": "operator", "target_archetype": "unclassified"}
    if isinstance(x, dict):
        return {
            "number": str(x.get("number", "")).strip(),
            "source": str(x.get("source", "operator")).strip(),
            "target_archetype": str(x.get("target_archetype", "unclassified")).strip(),
        }
    return {"number": str(x).strip(), "source": "operator", "target_archetype": "unclassified"}


def _parse_dict_yaml_samples(text: str) -> List[Dict[str, str]]:
    samples: List[Dict[str, str]] = []
    in_block = False
    current: Optional[Dict[str, str]] = None
    for raw in text.splitlines():
        line = raw.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        is_top_level = not line.startswith((" ", "\t"))
        if is_top_level:
            if current is not None:
                samples.append(current)
                current = None
            in_block = stripped.startswith("samples:")
            continue
        if not in_block:
            continue
        if stripped.startswith("- "):
            if current is not None:
                samples.append(current)
            current = {}
            kv = stripped[2:].strip()
            if ":" in kv:
                k, _, v = kv.partition(":")
                current[k.strip()] = _strip_scalar(v)
        elif current is not None and ":" in stripped:
            k, _, v = stripped.partition(":")
            current[k.strip()] = _strip_scalar(v)
    if current is not None:
        samples.append(current)
    return [_coerce_case(s) for s in samples if s.get("number")]


def _strip_scalar(v: str) -> str:
    v = v.strip()
    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        return v[1:-1]
    return v.split("#", 1)[0].strip()


# ---------------------------------------------------------------------------
# Filename / process-number helpers
# ---------------------------------------------------------------------------

def _digits_only(process: str) -> str:
    return re.sub(r"\D", "", process)


def _slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_").lower() or "x"


def _safe_name(process_number: str) -> str:
    return _digits_only(process_number)


# ---------------------------------------------------------------------------
# Driver lifecycle
# ---------------------------------------------------------------------------

def _new_driver(headless: bool, logger: logging.Logger) -> webdriver.Chrome:
    options = configure_chrome_options(headless=headless)
    logger.info("Starting Chrome (headless=%s, window=1920x1080)", headless)
    driver = webdriver.Chrome(options=options)
    # Defensive: the --window-size arg is the primary control, but explicitly
    # setting the size post-init guards against any environment where the arg
    # isn't honored. eSAJ serves a stacked mobile layout below ~1024px width.
    try:
        driver.set_window_size(1920, 1080)
    except WebDriverException as e:
        logger.warning("set_window_size failed: %s", e)
    return driver


# ---------------------------------------------------------------------------
# Page interaction (mirrors poursuite/scraper/esaj.py patterns; copied to
# keep the probe self-contained per the inventory brief's reuse note).
# ---------------------------------------------------------------------------

def _fill_form(driver: webdriver.Chrome, process_number: str) -> None:
    field = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "numeroDigitoAnoUnificado"))
    )
    field.clear()
    field.send_keys(process_number[:15])

    field = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "foroNumeroUnificado"))
    )
    field.clear()
    field.send_keys(process_number[-4:])

    btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "botaoConsultarProcessos"))
    )
    btn.click()


def _wait_for_outcome(driver: webdriver.Chrome) -> None:
    """Wait for either the process header, sealed marker, or 'not found' page."""
    try:
        WebDriverWait(driver, 15).until(
            lambda d: d.find_elements(By.ID, "classeProcesso")
            or d.find_elements(By.ID, ESAJ_SEALED_ELEMENT_ID)
            or d.find_elements(By.ID, "mensagemRetorno")
            or d.find_elements(By.CLASS_NAME, "nuncaExecutado")
        )
    except TimeoutException:
        pass


def _expand_header(driver: webdriver.Chrome) -> bool:
    """Click the header 'Mais' (`a[href='#maisDetalhes']`) to expose the secondary
    metadata block (filing date, valor, status, distribuição, etc.).
    Returns True on click, False if the link wasn't found.
    """
    try:
        link = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href='#maisDetalhes']"))
        )
        driver.execute_script("arguments[0].click();", link)
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "dataHoraDistribuicaoProcesso"))
            )
        except TimeoutException:
            pass
        return True
    except TimeoutException:
        return False


# ---------------------------------------------------------------------------
# Outcome detection
# ---------------------------------------------------------------------------

def _is_sealed(soup: BeautifulSoup) -> bool:
    el = soup.find("span", id=ESAJ_SEALED_ELEMENT_ID)
    return el is not None and ESAJ_SEALED_TEXT.lower() in el.get_text(strip=True).lower()


def _has_process_loaded(soup: BeautifulSoup) -> bool:
    return soup.find(id="classeProcesso") is not None


def _capture_alerts(soup: BeautifulSoup) -> List[Dict[str, str]]:
    """Banners, error messages, system notices visible at page level."""
    alerts: List[Dict[str, str]] = []
    seen_texts: set = set()

    selectors = [
        ("div", "alert"),
        ("div", "mensagemRetorno"),
        ("div", "msgPaginaSuperior"),
        ("div", "msgRetorno"),
        ("span", "erro"),
        ("div", "erro"),
        ("div", "popup-error"),
    ]
    for tag, cls in selectors:
        for el in soup.find_all(tag, class_=cls):
            txt = el.get_text(" ", strip=True)
            if txt and txt not in seen_texts:
                seen_texts.add(txt)
                alerts.append({"selector": f"{tag}.{cls}", "text": txt[:500]})
    for el in soup.find_all(id="mensagemRetorno"):
        txt = el.get_text(" ", strip=True)
        if txt and txt not in seen_texts:
            seen_texts.add(txt)
            alerts.append({"selector": "#mensagemRetorno", "text": txt[:500]})
    return alerts


# ---------------------------------------------------------------------------
# Header walker — capture EVERY label-value pair, not just FIELD_MAPPINGS
# ---------------------------------------------------------------------------

# Container IDs / class names worth preferring as the "header region" root.
# `unj-entity-header` is preferred because it encloses BOTH the primary
# fields (in `#containerDadosPrincipaisProcesso`) AND the secondary fields
# behind the `Mais` collapse (in `#maisDetalhes`). Narrower roots miss
# the secondary block entirely.
_HEADER_CANDIDATE_CLASSES = (
    "unj-entity-header",
    "secaoFormBody",
    "unj-card",
)
_HEADER_CANDIDATE_IDS = (
    "containerDadosPrincipaisProcesso",
    "dadosProcesso",
    "modalIncidentes",  # fallback; not always present
)


def _header_root(soup: BeautifulSoup):
    for cls in _HEADER_CANDIDATE_CLASSES:
        el = soup.find(class_=cls)
        if el:
            return el
    for hid in _HEADER_CANDIDATE_IDS:
        el = soup.find(id=hid)
        if el:
            return el
    return soup


def _walk_header(soup: BeautifulSoup) -> Dict[str, Any]:
    """Capture every label-value pair findable in the header region.

    Records label text (as displayed), value text, and a DOM selector hint
    so the operator can map findings back to source. NO filtering by
    FIELD_MAPPINGS — record everything, judge nothing.
    """
    root = _header_root(soup)
    fields: List[Dict[str, str]] = []
    seen: set = set()

    def _record(label: str, value: str, selector: str) -> None:
        label_clean = re.sub(r"\s+", " ", (label or "").strip()).rstrip(":").strip()
        value_clean = re.sub(r"\s+", " ", (value or "").strip())
        if not label_clean or not value_clean:
            return
        # Dedup on (label, value) alone — two patterns finding the same field
        # via different selectors are the same finding.
        key = (label_clean.lower(), value_clean[:200])
        if key in seen:
            return
        seen.add(key)
        fields.append({
            "label": label_clean,
            "value": value_clean,
            "selector": selector,
        })

    # Pattern 1: explicit unj-label / unj-value siblings.
    for label_el in root.find_all(class_="unj-label"):
        label = label_el.get_text(" ", strip=True)
        sib = label_el.find_next_sibling()
        if sib:
            _record(label, sib.get_text(" ", strip=True), ".unj-label + sibling")

    # Pattern 2: any element with id, where there's a nearby label-like sibling
    # (label ends in ':' or carries .unj-label / .col-form-label).
    for el in root.find_all(id=True):
        eid = el.get("id", "")
        if not eid or not isinstance(eid, str):
            continue
        # Skip layout-only ids
        if eid in ("Mais", "Menos") or eid.startswith("modal"):
            continue
        # Skip elements that ARE labels — Pattern 1 already pairs them with values.
        classes = el.get("class") or []
        if "unj-label" in classes or eid.startswith("label"):
            continue
        # Skip <a> elements — clickables, cataloged separately.
        if el.name == "a":
            continue
        # Skip wrapper/container elements that contain other id-bearing elements.
        # Their .get_text() is the concatenation of their leaf-field descendants,
        # which we capture individually. Recording the wrapper too is redundant.
        if el.find(id=True) is not None:
            continue
        value = el.get_text(" ", strip=True)
        if not value:
            continue
        label = _nearest_label(el)
        if label:
            _record(label, value, f"#{eid} (label via heuristic)")
        else:
            # Record by id alone — label may be implicit (e.g. tag spans).
            _record(eid, value, f"#{eid} (no label found)")

    # Pattern 3: <tr><th>Label</th><td>Value</td></tr>
    for tr in root.find_all("tr"):
        ths = tr.find_all("th")
        tds = tr.find_all("td")
        if ths and tds:
            for th, td in zip(ths, tds):
                _record(th.get_text(" ", strip=True),
                        td.get_text(" ", strip=True),
                        "tr > th + td")

    # Pattern 4: <dt>Label</dt><dd>Value</dd>
    for dt in root.find_all("dt"):
        dd = dt.find_next_sibling("dd")
        if dd:
            _record(dt.get_text(" ", strip=True),
                    dd.get_text(" ", strip=True),
                    "dt + dd")

    # Pattern 5: any element whose text ends with ':' followed by a sibling.
    for el in root.find_all(["span", "div", "label", "td", "th", "strong", "b"]):
        txt = el.get_text(" ", strip=True)
        if not txt or not txt.endswith(":") or len(txt) > 80:
            continue
        sib = el.find_next_sibling()
        if sib:
            sib_txt = sib.get_text(" ", strip=True)
            if sib_txt and len(sib_txt) <= 1000:
                _record(txt, sib_txt, f"{el.name}[ends-with-colon] + sibling")

    return {
        "field_count": len(fields),
        "fields": fields,
    }


def _nearest_label(el) -> Optional[str]:
    """Find a plausible label for `el` by inspecting siblings and ancestors.

    Accepts labels that:
      - end with ':' (the classic CSS-free marker), or
      - carry class `unj-label` (eSAJ's own convention — no trailing colon), or
      - are a <label> element.
    """

    def _label_text_if_match(node) -> Optional[str]:
        if node is None or not hasattr(node, "name") or not node.name:
            return None
        classes = node.get("class") or []
        text = node.get_text(" ", strip=True)
        if not text or len(text) > 100:
            return None
        if text.endswith(":"):
            return text
        if "unj-label" in classes or node.name == "label":
            return text
        # Some labels are wrapped: <div><span class="unj-label">...</span></div>.
        inner = node.find(class_="unj-label") if hasattr(node, "find") else None
        if inner is not None:
            inner_text = inner.get_text(" ", strip=True)
            if inner_text and len(inner_text) <= 100:
                return inner_text
        return None

    found = _label_text_if_match(el.find_previous_sibling())
    if found:
        return found
    parent = el.parent
    for _ in range(3):
        if parent is None:
            break
        found = _label_text_if_match(parent.find_previous_sibling())
        if found:
            return found
        parent = parent.parent
    return None


# ---------------------------------------------------------------------------
# Badge / tag / icon capture
# ---------------------------------------------------------------------------

def _capture_badges(soup: BeautifulSoup) -> List[Dict[str, str]]:
    badges: List[Dict[str, str]] = []
    seen: set = set()
    for el in soup.find_all(class_=lambda c: bool(c) and any(
        marker in c for marker in ("unj-tag", "tag", "badge", "label-")
    )):
        txt = el.get_text(" ", strip=True)
        if not txt:
            continue
        cls = " ".join(el.get("class") or [])
        key = (txt[:200], cls)
        if key in seen:
            continue
        seen.add(key)
        badges.append({"text": txt, "classes": cls, "tag": el.name})
    return badges


# Section enumeration & per-section walking primitives live in
# poursuite/scraper/sections.py so the production scraper (Phase 2c) can
# reuse them. They're imported above with underscore aliases to preserve
# this module's pre-2b call-sites verbatim.


# ---------------------------------------------------------------------------
# Clickables catalog — type-deep, not instance-deep
# ---------------------------------------------------------------------------

# Each rule: (type_label, predicate(href:str, anchor) -> bool)
_LINK_TYPES: List[Tuple[str, Any]] = [
    ("process_number_link",
     lambda h, a: bool(h) and ("processo.numero" in h or "numeroProcesso=" in h
                                or "/cpopg/show.do" in h)),
    ("oab_link",
     lambda h, a: bool(h) and ("oab" in h.lower() or "/sajcas/" in h
                                or "consultaOAB" in h)),
    ("document_pdf",
     lambda h, a: bool(h) and (h.lower().endswith(".pdf") or "getDocument" in h
                                or "abrirDocumentoVinculado" in h)),
    ("party_listing",
     lambda h, a: bool(h) and "campo_NMPARTE" in h),
    ("internal_anchor",
     lambda h, a: bool(h) and h.startswith("#")),
    ("javascript_void",
     lambda h, a: bool(h) and h.lower().startswith("javascript:")),
    ("external_link",
     lambda h, a: bool(h) and h.startswith("http") and "tjsp.jus.br" not in h),
]


def _catalog_clickables(soup: BeautifulSoup, page_url: str) -> Dict[str, Any]:
    catalog: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"count": 0, "examples": []})
    other_examples: List[Dict[str, str]] = []
    other_count = 0

    for a in soup.find_all("a"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full_url = urljoin(page_url, href) if not href.startswith(("javascript:", "#")) else href
        text = a.get_text(" ", strip=True)
        matched = False
        for type_label, pred in _LINK_TYPES:
            try:
                if pred(href, a):
                    bucket = catalog[type_label]
                    bucket["count"] += 1
                    if len(bucket["examples"]) < 2:
                        bucket["examples"].append({"text": text[:200], "href": full_url[:500]})
                    matched = True
                    break
            except Exception:
                continue
        if not matched:
            other_count += 1
            if len(other_examples) < 5:
                other_examples.append({"text": text[:200], "href": full_url[:500]})

    out: Dict[str, Any] = {
        "by_type": {k: dict(v) for k, v in catalog.items()},
        "other": {"count": other_count, "examples": other_examples},
    }
    return out


# ---------------------------------------------------------------------------
# Per-case orchestrator
# ---------------------------------------------------------------------------

def _validate_process_number(process_number: str) -> bool:
    return bool(re.match(PROCESS_NUMBER_PATTERN_STRICT, process_number))


def probe_one_case(driver: webdriver.Chrome, case: Dict[str, str],
                   run_dir: Path, logger: logging.Logger) -> Dict[str, Any]:
    process_number = case["number"]
    name = _safe_name(process_number)
    raw_html_dir = run_dir / "raw_html"
    screenshots_dir = run_dir / "screenshots"
    structured_dir = run_dir / "structured"
    raw_html_dir.mkdir(parents=True, exist_ok=True)
    screenshots_dir.mkdir(parents=True, exist_ok=True)
    structured_dir.mkdir(parents=True, exist_ok=True)

    result: Dict[str, Any] = {
        "input": case,
        "url_consulta": ESAJ_URL,
        "outcome": None,
        "outcome_detail": None,
        "page_url_after_submit": None,
        "page_alerts": [],
        "header": {"field_count": 0, "fields": []},
        "sections": {},
        "sections_discovered": [],
        "section_collapsibles_clicked": [],
        "links_by_type": {},
        "badges": [],
        "anomalies": [],
        "errors": [],
        "timing_ms": {},
    }

    if not _validate_process_number(process_number):
        result["outcome"] = "invalid_process_number"
        result["errors"].append(f"does not match {PROCESS_NUMBER_PATTERN_STRICT}")
        return result

    def _save_artifacts(label: str) -> None:
        try:
            (raw_html_dir / f"{name}__{label}.html").write_text(
                driver.page_source, encoding="utf-8")
        except Exception as e:
            result["anomalies"].append(f"save html '{label}': {e}")
        try:
            driver.save_screenshot(str(screenshots_dir / f"{name}__{label}.png"))
        except Exception as e:
            result["anomalies"].append(f"save screenshot '{label}': {e}")

    t0 = time.perf_counter()
    try:
        driver.get(ESAJ_URL)
        _fill_form(driver, process_number)
        _wait_for_outcome(driver)
        result["page_url_after_submit"] = driver.current_url
    except Exception as e:
        result["outcome"] = "error"
        result["outcome_detail"] = "form submission failed"
        result["errors"].append(f"{type(e).__name__}: {e}")
        result["errors"].append(traceback.format_exc()[-1000:])
        _save_artifacts("error_after_submit")
        write_json(structured_dir / f"{name}.json", result)
        return result
    result["timing_ms"]["submit"] = int((time.perf_counter() - t0) * 1000)

    _save_artifacts("after_submit")
    soup = BeautifulSoup(driver.page_source, "html.parser")
    result["page_alerts"] = _capture_alerts(soup)

    if _is_sealed(soup):
        result["outcome"] = "sealed"
        result["outcome_detail"] = ESAJ_SEALED_TEXT
        result["header"] = _walk_header(soup)
        result["badges"] = _capture_badges(soup)
        result["links_by_type"] = _catalog_clickables(soup, driver.current_url)
        write_json(structured_dir / f"{name}.json", result)
        logger.info("[%s] outcome=sealed; %d header fields captured",
                    process_number, result["header"]["field_count"])
        return result

    if not _has_process_loaded(soup):
        result["outcome"] = "not_found_or_other"
        result["outcome_detail"] = "no #classeProcesso element on page"
        result["links_by_type"] = _catalog_clickables(soup, driver.current_url)
        write_json(structured_dir / f"{name}.json", result)
        logger.info("[%s] outcome=not_found_or_other", process_number)
        return result

    t1 = time.perf_counter()
    header_expanded = _expand_header(driver)
    result["timing_ms"]["header_expand"] = int((time.perf_counter() - t1) * 1000)
    if not header_expanded:
        result["anomalies"].append(
            "header 'Mais' (a[href='#maisDetalhes']) not found or click failed"
        )

    t2 = time.perf_counter()
    clicked_collapsibles = _expand_section_collapsibles(driver)
    result["timing_ms"]["section_collapsibles_expand"] = int((time.perf_counter() - t2) * 1000)
    result["section_collapsibles_clicked"] = clicked_collapsibles
    not_clicked = [c for c in _SECTION_COLLAPSIBLE_IDS if c not in clicked_collapsibles]
    for nc in not_clicked:
        result["anomalies"].append(
            f"section collapsible #{nc} not present or not clickable"
        )

    _save_artifacts("after_expand")
    soup = BeautifulSoup(driver.page_source, "html.parser")

    result["outcome"] = "loaded"
    result["header"] = _walk_header(soup)
    result["badges"] = _capture_badges(soup)
    logger.info("[%s] outcome=loaded; %d header fields, %d badges",
                process_number, result["header"]["field_count"], len(result["badges"]))

    sections = _enumerate_sections(soup)
    section_labels = [s["label"] for s in sections]
    result["sections_discovered"] = section_labels
    logger.info("[%s] sections discovered: %s",
                process_number,
                ", ".join(section_labels) or "(none)")

    for section in sections:
        label = section["label"]
        try:
            section_data = _walk_section(section)
            result["sections"][label] = section_data
        except Exception as e:
            result["sections"][label] = {"error": f"{type(e).__name__}: {e}"}
            result["anomalies"].append(f"section '{label}' walk error: {e}")

    # Single clickable catalog over the fully-expanded DOM. (No tab walking,
    # so no risk of lazy-loaded anchors being missed.)
    result["links_by_type"] = _catalog_clickables(soup, driver.current_url)

    write_json(structured_dir / f"{name}.json", result)
    return result


# ---------------------------------------------------------------------------
# Cross-case aggregation
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Empirical observations (NOT auto-classification)
#
# The previous run's auto-classifier over-tagged: empty Apensos sections were
# read as "linked processes" because eSAJ renders a placeholder row when the
# list is empty. Per the viewport re-run findings, we now record
# raw observations and let the operator interpret. The archetype coverage
# check uses DECLARED archetypes from the YAML, not inferred ones.
# ---------------------------------------------------------------------------

_PLACEHOLDER_PHRASES = ("não há", "nao ha", "não existem", "nao existem")


def _section_is_empty_placeholder(section_data: Dict[str, Any]) -> bool:
    """Detect eSAJ's 'Não há ...' empty-state row that some sections render."""
    text = (section_data.get("text_preview") or "").strip().lower()
    if not text:
        return True
    if any(p in text for p in _PLACEHOLDER_PHRASES):
        # Short text + placeholder phrase = empty section.
        return len(text) < 200
    return False


def _empirical_observations(case: Dict[str, str], result: Dict[str, Any]) -> List[str]:
    """Conservative, neutral observations from a case's captured content.

    These do NOT classify the case. They surface signals the operator can use
    to verify or refute a target archetype. Recorded as plain English so the
    operator reads them in the report exactly as they were inferred.
    """
    obs: List[str] = []

    if result.get("outcome") == "sealed":
        obs.append("sealed-case rendering observed")

    process = case.get("number", "")
    year_match = re.search(r"\.(\d{4})\.", process)
    year = int(year_match.group(1)) if year_match else 0
    if year and year < 2010:
        obs.append(f"process number indicates pre-2010 case ({year})")

    header_fields = {f.get("label", ""): f.get("value", "")
                     for f in (result.get("header") or {}).get("fields") or []
                     if isinstance(f, dict)}
    classe = (header_fields.get("Classe") or "").lower()
    if classe:
        obs.append(f"Classe = {header_fields.get('Classe')}")

    sections = result.get("sections") or {}

    # Movimentações observations — penhora-family terms, citação, volume.
    mov = sections.get("Movimentações") or sections.get("Movimentacoes")
    if isinstance(mov, dict):
        text = (mov.get("text_preview") or "").lower()
        penhora_terms = [t for t in ("penhora", "bloqueio", "sisbajud", "bacenjud")
                         if t in text]
        if penhora_terms:
            obs.append("penhora-family terms in Movimentações: "
                       + ", ".join(penhora_terms))
        if "citação" in text or "citacao" in text:
            obs.append("citação term in Movimentações")
        total_rows = sum((t.get("row_count") or 0)
                         for t in mov.get("tables") or []
                         if isinstance(t, dict))
        if total_rows:
            obs.append(f"Movimentações row count: {total_rows}")

    # Apensos / Incidentes — only flag when non-empty.
    for label, sd in sections.items():
        if not isinstance(sd, dict):
            continue
        if not any(k in label.lower() for k in ("apens", "incident")):
            continue
        if not _section_is_empty_placeholder(sd):
            obs.append(f"non-empty section: {label}")

    # Multi-party hint — Partes section anchor count.
    partes = sections.get("Partes do processo")
    if isinstance(partes, dict):
        anchors = partes.get("anchors_in_section") or 0
        if anchors >= 6:
            obs.append(f"high anchor count in Partes ({anchors}) — possible multi-party")

    return obs


def _build_comparison_matrix(case_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Field × case matrix. Cell values: present / absent / value-different."""
    case_ids = [r["input"]["number"] for r in case_results]

    field_to_values: Dict[str, Dict[str, str]] = defaultdict(dict)
    for r in case_results:
        cid = r["input"]["number"]
        for f in (r.get("header") or {}).get("fields", []):
            label = f.get("label", "")
            value = f.get("value", "")
            if not label:
                continue
            # If the same label has been recorded multiple ways for one case,
            # keep the longest value (the more informative duplicate).
            existing = field_to_values[label].get(cid)
            if existing is None or len(value) > len(existing):
                field_to_values[label][cid] = value

    matrix: List[Dict[str, Any]] = []
    for label, by_case in sorted(field_to_values.items(), key=lambda x: x[0].lower()):
        row = {"field": label, "cells": {}}
        distinct_values = set()
        for cid in case_ids:
            v = by_case.get(cid)
            if v is None:
                row["cells"][cid] = {"state": "absent", "value": None}
            else:
                row["cells"][cid] = {"state": "present", "value": v}
                distinct_values.add(v)
        row["distinct_values"] = len(distinct_values)
        row["present_count"] = sum(1 for c in row["cells"].values() if c["state"] == "present")
        matrix.append(row)
    return {"case_ids": case_ids, "rows": matrix}


def _collect_subscraper_candidates(case_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Type-deep catalog of clickables across all cases."""
    by_type: Dict[str, Dict[str, Any]] = {}
    for r in case_results:
        for type_label, info in (r.get("links_by_type") or {}).get("by_type", {}).items():
            entry = by_type.setdefault(type_label, {"total_count": 0, "examples": []})
            entry["total_count"] += int(info.get("count", 0) or 0)
            for ex in info.get("examples", []):
                if len(entry["examples"]) < 4 and ex not in entry["examples"]:
                    entry["examples"].append(ex)
    return by_type


def _archetype_coverage(case_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Coverage check based on DECLARED archetypes only (from the YAML).

    No inference. Empirical observations are recorded separately per case
    and surfaced in the report so the operator can decide whether each case
    actually matches its declared archetype.
    """
    declared_per_case: Dict[str, str] = {}
    observations_per_case: Dict[str, List[str]] = {}
    coverage: Dict[str, List[str]] = {a: [] for a in ARCHETYPES}
    for r in case_results:
        cid = r["input"]["number"]
        declared = (r["input"].get("target_archetype") or "unclassified").strip()
        declared_per_case[cid] = declared
        observations_per_case[cid] = _empirical_observations(r["input"], r)
        if declared in coverage:
            coverage[declared].append(cid)
    unfilled = [a for a, cases in coverage.items() if not cases]
    return {
        "declared_per_case": declared_per_case,
        "observations_per_case": observations_per_case,
        "coverage_by_declared_archetype": coverage,
        "unfilled_archetypes": unfilled,
    }


# ---------------------------------------------------------------------------
# Report writer — 8 sections per the brief
# ---------------------------------------------------------------------------

def write_inventory_report(run_dir: Path, case_results: List[Dict[str, Any]],
                           current_scraper_fields: List[str],
                           archetype_info: Dict[str, Any],
                           matrix: Dict[str, Any],
                           subscraper_candidates: Dict[str, Any],
                           logger: logging.Logger) -> Path:
    md: List[str] = []
    md.append("# eSAJ Full Inventory Report")
    md.append(f"_Run: `{run_dir.name}`  |  Generated: {datetime.now(timezone.utc).isoformat()}_")
    md.append("")

    # 1. Methodology
    md.append("## 1. Methodology")
    md.append("")
    md.append(f"- Cases probed: {len(case_results)}")
    md.append(f"- Authentication: none (per brief — eSAJ unauthenticated consulta pública only).")
    md.append(f"- Headed/headless: see run log.")
    md.append("- Per case: opened the consulta page, submitted the form, expanded the "
              "header `Mais` (`a[href='#maisDetalhes']`) plus the inline section "
              "expanders (`#linkpartes`, `#linkmovimentacoes`), walked the header for "
              "**every** label-value pair (not just the production scraper subset), "
              "enumerated all section blocks delimited by `<h2 class='tituloDoBloco'>`, "
              "and parsed each section's content (tables, lists, anchors, pagination, "
              "filter inputs). The page is a single long document — there are no tabs.")
    md.append("- Linked processes (apensos, dependentes), OAB profiles, and document PDFs "
              "are cataloged **by type** (with 1–2 examples each) — not followed.")
    md.append("- All findings recorded; no filtering by perceived utility.")
    md.append("")
    md.append("**Out of scope (not investigated this pass):**")
    md.append("- Recursive descent into linked process pages")
    md.append("- OAB profile pages beyond the link itself")
    md.append("- Document PDF contents")
    md.append("- Authenticated views (login wall recorded as finding only)")
    md.append("")

    # 2. Per-case findings
    md.append("## 2. Per-case findings")
    md.append("")
    observations_per_case = archetype_info.get("observations_per_case", {})
    for r in case_results:
        c = r["input"]
        md.append(f"### `{c['number']}`  ({c.get('source','operator')})")
        md.append(f"- **target archetype (declared in YAML):** {c.get('target_archetype','unclassified')}")
        md.append(f"- **outcome:** `{r.get('outcome')}`"
                  + (f" — {r.get('outcome_detail')}" if r.get('outcome_detail') else ""))
        md.append(f"- **header fields captured:** {(r.get('header') or {}).get('field_count', 0)}")
        md.append(f"- **badges/tags captured:** {len(r.get('badges') or [])}")
        md.append(f"- **sections discovered:** "
                  + (", ".join(r.get('sections_discovered') or []) or "(none)"))
        sd = r.get("sections") or {}
        section_facts: List[str] = []
        for slabel, sinfo in sd.items():
            if not isinstance(sinfo, dict):
                continue
            tcount = sinfo.get("table_count_total") or 0
            anchors = sinfo.get("anchors_in_section") or 0
            row_total = sum((t.get("row_count") or 0) for t in (sinfo.get("tables") or [])
                            if isinstance(t, dict))
            section_facts.append(
                f"  - `{slabel}`: tables={tcount} (rows={row_total}), "
                f"anchors={anchors}, text_len={sinfo.get('text_length', 0)}"
            )
        if section_facts:
            md.append("- **section content:**")
            md.extend(section_facts)
        md.append(f"- **section collapsibles clicked:** "
                  + (", ".join(r.get('section_collapsibles_clicked') or []) or "(none)"))
        observations = observations_per_case.get(c["number"], [])
        if observations:
            md.append("- **empirical observations** _(neutral signals — operator interprets):_")
            for o in observations:
                md.append(f"  - {o}")
        anomalies = r.get("anomalies") or []
        if anomalies:
            md.append(f"- **anomalies during walk:** {len(anomalies)}")
            for a in anomalies[:5]:
                md.append(f"  - {a}")
        errors = r.get("errors") or []
        if errors:
            md.append(f"- **errors:** {len(errors)}")
            for e in errors[:3]:
                md.append(f"  - `{e}`")
        md.append("")

    # 3. Comprehensive field catalog
    md.append("## 3. Comprehensive field catalog")
    md.append("")
    md.append("Every distinct header label/value seen across all cases. "
              "`presence` is the count of cases where the field appeared.")
    md.append("")
    md.append("| Field | Presence | Distinct values | Example value |")
    md.append("|---|---:|---:|---|")
    rows = matrix.get("rows", [])
    for row in rows:
        any_present_value = next(
            (c["value"] for c in row["cells"].values() if c["state"] == "present"),
            "",
        )
        example = (any_present_value or "")[:120]
        md.append(f"| `{row['field']}` | {row['present_count']}/{len(matrix['case_ids'])} "
                  f"| {row['distinct_values']} | {example} |")
    md.append("")

    # 4. Sub-scraper candidates
    md.append("## 4. Sub-scraper candidates (clickables that lead elsewhere)")
    md.append("")
    md.append("Type-deep catalog. **No recommendation about whether to follow.** "
              "Operator decides.")
    md.append("")
    if not subscraper_candidates:
        md.append("_No clickables cataloged._")
    for type_label, info in sorted(subscraper_candidates.items(),
                                   key=lambda x: -x[1]["total_count"]):
        md.append(f"### `{type_label}`")
        md.append(f"- total occurrences across all cases: {info['total_count']}")
        if info["examples"]:
            md.append("- examples:")
            for ex in info["examples"]:
                md.append(f"  - `{(ex.get('text') or '').strip()[:80]}` → `{ex.get('href','')}`")
        md.append("")

    # 5. Comparison matrix
    md.append("## 5. Comparison matrix (field × case)")
    md.append("")
    md.append("Cells: `✓` present, `·` absent. The matrix answers \"does this "
              "field appear on this case?\" — not value-equality. Per-field "
              "value variation is in Section 3's `Distinct values` column.")
    md.append("")
    case_ids = matrix.get("case_ids", [])
    if case_ids and rows:
        # Compact column headers — use 1-based indices, list legend below
        header_cells = [f"C{i+1}" for i in range(len(case_ids))]
        md.append("| Field | " + " | ".join(header_cells) + " |")
        md.append("|---" + "|---" * len(case_ids) + "|")
        for row in rows:
            cells = []
            for cid in case_ids:
                cell = row["cells"].get(cid, {"state": "absent"})
                cells.append("✓" if cell["state"] == "present" else "·")
            md.append(f"| `{row['field']}` | " + " | ".join(cells) + " |")
        md.append("")
        md.append("**Legend:**")
        for i, cid in enumerate(case_ids):
            md.append(f"- C{i+1} = `{cid}`")
    md.append("")

    # 6. What the current scraper extracts
    md.append("## 6. What the current scraper extracts")
    md.append("")
    md.append("From [poursuite/scraper/esaj.py](poursuite/scraper/esaj.py)'s "
              "`FIELD_MAPPINGS` and `_extract_parties` / `_get_other_processes_count`:")
    for f in current_scraper_fields:
        md.append(f"- `{f}`")
    md.append("")
    md.append("**Mapping from production field → eSAJ header label** "
              "(per the eSAJ inventory viewport re-run pass):")
    for label, field in PRODUCTION_SCRAPER_LABEL_MAP.items():
        md.append(f"- `{field}` ← `{label}`")
    md.append("")
    md.append("**Production scraper fields NOT sourced from header label-value pairs:**")
    for f in PRODUCTION_SCRAPER_NON_LABEL_FIELDS:
        md.append(f"- `{f}`")
    md.append("")

    # 7. Gap section
    md.append("## 7. Gap — header fields present on the page but NOT extracted")
    md.append("")
    md.append("Walker-discovered header labels that the production scraper does "
              "not extract, per the brief's ground-truth mapping. Just the list. "
              "No prioritization, no recommendation.")
    md.append("")
    extracted_labels = set(PRODUCTION_SCRAPER_LABEL_MAP.keys())
    discovered_labels: List[str] = [row["field"] for row in rows]
    gaps = [lbl for lbl in discovered_labels if lbl not in extracted_labels]
    # `numeroProcesso` is the case identifier itself, not a discovered field —
    # it's the input. Don't list it as a gap.
    gaps = [g for g in gaps if g != "numeroProcesso"]
    if not gaps:
        md.append("_No header-field gaps detected._")
    for g in gaps:
        md.append(f"- `{g}`")
    md.append("")
    # Sections the current scraper doesn't visit at all
    distinct_sections: set = set()
    for r in case_results:
        for s in r.get("sections_discovered") or []:
            if s:
                distinct_sections.add(s)
    md.append("**Sections not visited by the current scraper (it only reads the header):**")
    for s in sorted(distinct_sections):
        md.append(f"- `{s}`")
    md.append("")

    # 8. Anomalies
    md.append("## 8. Anomalies")
    md.append("")
    md.append("Anything weird, unexpected, broken, or that didn't fit the framework.")
    md.append("")
    any_anom = False
    for r in case_results:
        if r.get("anomalies"):
            any_anom = True
            md.append(f"**`{r['input']['number']}`:**")
            for a in r["anomalies"]:
                md.append(f"- {a}")
            md.append("")
        if r.get("errors"):
            any_anom = True
            md.append(f"**`{r['input']['number']}` (errors):**")
            for e in r["errors"][:5]:
                md.append(f"- `{e}`")
            md.append("")
    if not any_anom:
        md.append("_None recorded._")
        md.append("")

    # Archetype coverage check (declared, not inferred)
    md.append("## Archetype coverage check")
    md.append("")
    md.append("Coverage is by **declared** target archetype (from "
              "`esaj_inventory_samples.yaml`). The previous run's auto-classifier "
              "was unreliable. "
              "Each case's empirical observations are listed in Section 2 so "
              "the operator can verify whether a case actually fits the archetype "
              "it was declared as.")
    md.append("")
    coverage = archetype_info.get("coverage_by_declared_archetype", {})
    md.append("| Archetype | Cases declared as |")
    md.append("|---|---|")
    for a in ARCHETYPES:
        cases = coverage.get(a, [])
        cell = ", ".join(f"`{c}`" for c in cases) if cases else "_(unfilled)_"
        md.append(f"| {a} | {cell} |")
    md.append("")
    unfilled = archetype_info.get("unfilled_archetypes", [])
    if unfilled:
        md.append(f"**Unfilled archetypes ({len(unfilled)}):** "
                  + ", ".join(unfilled))
        md.append("")
        md.append("These represent gaps the operator should review — either by "
                  "adding gap-filler cases to `esaj_inventory_samples.yaml` and "
                  "re-running, or by accepting the gap with rationale.")
        md.append("")

    out_path = run_dir / "inventory_report.md"
    out_path.write_text("\n".join(md), encoding="utf-8")
    logger.info("Wrote %s", out_path)
    return out_path


# ---------------------------------------------------------------------------
# Current-scraper field extraction (read from code, not from docs)
# ---------------------------------------------------------------------------

def _extract_current_scraper_fields() -> List[str]:
    """Read poursuite/scraper/esaj.py and list the fields it captures.

    Looks for the FIELD_MAPPINGS keys + the parties extraction + other_processes.
    Read-only; we do not import or execute scraper code from this probe.
    """
    scraper_path = Path(__file__).resolve().parent.parent / "scraper" / "esaj.py"
    fields: List[str] = []
    if not scraper_path.exists():
        return fields
    text = scraper_path.read_text(encoding="utf-8")
    m = re.search(r"FIELD_MAPPINGS\s*=\s*\{(.+?)\n\s*\}", text, flags=re.DOTALL)
    if m:
        for k in re.findall(r'"([^"]+)"\s*:\s*\{', m.group(1)):
            fields.append(k)
    if "_extract_parties" in text:
        fields.extend(["plaintiff (first nomeParteEAdvogado)",
                       "defendant (second nomeParteEAdvogado)"])
    if "_get_other_processes_count" in text:
        fields.append("other_processes (count of cases under defendant name)")
    return fields


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------

def run(samples: List[Dict[str, str]], run_dir: Path, logger: logging.Logger,
        *, headed: bool = False) -> Dict[str, Any]:
    logger.info("eSAJ inventory: %d sample(s); headed=%s; run_dir=%s",
                len(samples), headed, run_dir)

    driver = _new_driver(headless=not headed, logger=logger)
    case_results: List[Dict[str, Any]] = []
    try:
        for i, case in enumerate(samples):
            logger.info("==> sample %d/%d  %s  (source=%s, target=%s)",
                        i + 1, len(samples),
                        case.get("number"), case.get("source"),
                        case.get("target_archetype"))
            try:
                r = probe_one_case(driver, case, run_dir, logger)
            except Exception as e:
                logger.exception("Unhandled error on %s", case.get("number"))
                r = {
                    "input": case,
                    "outcome": "error",
                    "errors": [f"{type(e).__name__}: {e}",
                               traceback.format_exc()[-1000:]],
                    "header": {"field_count": 0, "fields": []},
                    "sections": {}, "sections_discovered": [],
                    "section_collapsibles_clicked": [],
                    "links_by_type": {}, "badges": [], "anomalies": [],
                }
            case_results.append(r)
            try:
                driver.delete_all_cookies()
            except Exception:
                pass
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    write_json(run_dir / "all_cases_summary.json", case_results)

    matrix = _build_comparison_matrix(case_results)
    write_json(run_dir / "comparison_matrix.json", matrix)

    subscraper = _collect_subscraper_candidates(case_results)
    write_json(run_dir / "subscraper_candidates.json", subscraper)

    archetype_info = _archetype_coverage(case_results)
    write_json(run_dir / "archetype_coverage.json", archetype_info)

    current_fields = _extract_current_scraper_fields()
    write_inventory_report(
        run_dir, case_results, current_fields,
        archetype_info, matrix, subscraper, logger,
    )

    summary = {
        "ok": all(r.get("outcome") not in (None, "error") for r in case_results),
        "case_count": len(case_results),
        "outcomes": Counter(r.get("outcome") for r in case_results),
        "unfilled_archetypes": archetype_info.get("unfilled_archetypes", []),
    }
    summary["outcomes"] = dict(summary["outcomes"])
    return summary
