"""Phase 3 probe — eSAJ document download + extraction quality.

See CLAUDE_CODE_BRIEF_PHASE3_LAYER3_PROBE.md (v1.2). Investigates:

- Can we download eSAJ documents from `cd_documento` references in the
  snapshot store? Authenticated vs. unauthenticated.
- What URL form (`abrirDocumentoVinculadoMovimentacao.do?cdDocumento=...`,
  with or without `cdProcesso`) actually works?
- For successful downloads: file size, page count, text-PDF vs. image-PDF.
- For text-PDFs: extraction quality, encoding, structural noise.

Methodology disciplines (PLAN.md §11): record everything, defensive HTTP,
surface unexpected findings. Outputs to
`<POURSUITE_LOG_DIR>/probes/phase3_probe_<ts>/`.
"""
from __future__ import annotations

import json
import logging
import re
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Optional, Tuple

from poursuite.config import SNAPSHOT_DIR
from poursuite.probes import write_json

ESAJ_DOC_URL = "https://esaj.tjsp.jus.br/cpopg/abrirDocumentoVinculadoMovimentacao.do"
ESAJ_CASE_URL = "https://esaj.tjsp.jus.br/cpopg/open.do"
DEFAULT_DB_PATH = SNAPSHOT_DIR / "esaj_snapshots.db"

DESKTOP_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Per-case cap from brief §3.2.
PER_CASE_DOC_CAP = 20

# Brief v1.2: the 4 currently-loaded cases. Probe report flags case-mix-unmet.
LOADED_CASES = (
    "1002177-13.2020.8.26.0100",
    "1063559-02.2023.8.26.0100",
    "1185092-25.2023.8.26.0100",
    "1033164-10.2022.8.26.0602",
)


# ---------------------------------------------------------------------------
# Snapshot store enumeration
# ---------------------------------------------------------------------------

def enumerate_documents(db_path: Path) -> Dict[str, List[Dict[str, Any]]]:
    """Return {process_number: [{ordem, data_hora, nome, cd_documento}, ...]}
    for the latest snapshot of each loaded case in LOADED_CASES.
    Caps at PER_CASE_DOC_CAP documents per case (ordered by ordem)."""
    docs: Dict[str, List[Dict[str, Any]]] = {}
    conn = sqlite3.connect(str(db_path))
    try:
        for process_number in LOADED_CASES:
            row = conn.execute(
                "SELECT MAX(snapshot_ts) FROM process_snapshot "
                "WHERE process_number = ? AND scrape_outcome = 'loaded'",
                (process_number,),
            ).fetchone()
            if not row or not row[0]:
                docs[process_number] = []
                continue
            latest_ts = row[0]
            cur = conn.execute(
                "SELECT ordem, data_hora, nome, cd_documento "
                "FROM movimento "
                "WHERE process_number = ? AND snapshot_ts = ? "
                "  AND cd_documento IS NOT NULL "
                "ORDER BY ordem "
                "LIMIT ?",
                (process_number, latest_ts, PER_CASE_DOC_CAP),
            )
            docs[process_number] = [
                {"ordem": r[0], "data_hora": r[1], "nome": r[2], "cd_documento": r[3]}
                for r in cur.fetchall()
            ]
    finally:
        conn.close()
    return docs


# ---------------------------------------------------------------------------
# HTTP — defensive download
# ---------------------------------------------------------------------------

def _http_get(url: str, *, cookies: Optional[Dict[str, str]] = None,
              referer: Optional[str] = None, timeout: int = 30,
              logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
    """GET with realistic headers; never raises. Returns dict with status,
    bytes (capped sample for non-PDF), final_url, content_type, and timing."""
    headers = {
        "User-Agent": DESKTOP_USER_AGENT,
        "Accept": "application/pdf,application/octet-stream,*/*",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    }
    if referer:
        headers["Referer"] = referer
    if cookies:
        headers["Cookie"] = "; ".join(f"{k}={v}" for k, v in cookies.items())
    req = urllib.request.Request(url, headers=headers, method="GET")
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            latency_ms = int((time.perf_counter() - t0) * 1000)
            ctype = resp.headers.get("Content-Type", "") or ""
            return {
                "ok": True,
                "status": resp.status,
                "final_url": resp.geturl(),
                "content_type": ctype,
                "size": len(body),
                "body": body,
                "is_pdf": body.startswith(b"%PDF"),
                "html_sample": (body[:2048].decode("utf-8", errors="replace")
                                if not body.startswith(b"%PDF") else None),
                "latency_ms": latency_ms,
                "error": None,
            }
    except urllib.error.HTTPError as e:
        latency_ms = int((time.perf_counter() - t0) * 1000)
        try:
            err_body = e.read()
        except Exception:
            err_body = b""
        if logger:
            logger.warning("HTTP %s for %s: %s", e.code, url, err_body[:200])
        return {
            "ok": False, "status": e.code, "final_url": url,
            "content_type": e.headers.get("Content-Type", "") if e.headers else "",
            "size": len(err_body),
            "body": err_body,
            "is_pdf": False,
            "html_sample": err_body[:2048].decode("utf-8", errors="replace"),
            "latency_ms": latency_ms,
            "error": f"HTTPError {e.code} {e.reason}",
        }
    except urllib.error.URLError as e:
        latency_ms = int((time.perf_counter() - t0) * 1000)
        if logger:
            logger.error("URLError for %s: %s", url, e.reason)
        return {
            "ok": False, "status": None, "final_url": url,
            "content_type": "", "size": 0, "body": b"",
            "is_pdf": False, "html_sample": None,
            "latency_ms": latency_ms,
            "error": f"URLError {e.reason}",
        }
    except Exception as e:  # noqa: BLE001
        latency_ms = int((time.perf_counter() - t0) * 1000)
        if logger:
            logger.error("Unexpected error for %s: %s", url, e, exc_info=True)
        return {
            "ok": False, "status": None, "final_url": url,
            "content_type": "", "size": 0, "body": b"",
            "is_pdf": False, "html_sample": None,
            "latency_ms": latency_ms,
            "error": f"{type(e).__name__}: {e}",
        }


# ---------------------------------------------------------------------------
# Step 0 — auth/URL discovery
# ---------------------------------------------------------------------------

def discover_download_approach(sample_cd_documento: str, sample_process_number: str,
                               run_dir: Path,
                               logger: logging.Logger) -> Dict[str, Any]:
    """Try several URL forms unauthenticated against a known cdDocumento.

    Records what happens for each variant. The first one returning a PDF body
    wins. If none do, the report flags that Selenium-session fallback is
    required (we do NOT implement that fallback automatically — operator
    decides whether to invest in it after seeing the probe findings)."""

    variants: List[Tuple[str, str]] = [
        ("cdDocumento_only",
         f"{ESAJ_DOC_URL}?cdDocumento={sample_cd_documento}"),
        ("cdDocumento_with_processo_qs",
         f"{ESAJ_DOC_URL}?cdDocumento={sample_cd_documento}"
         f"&processo.numero={urllib.parse.quote(sample_process_number)}"),
        ("cdDocumento_with_referer",
         f"{ESAJ_DOC_URL}?cdDocumento={sample_cd_documento}"),
    ]

    results: List[Dict[str, Any]] = []
    working_variant: Optional[str] = None
    for name, url in variants:
        referer = (f"{ESAJ_CASE_URL}?gateway=true&processo.numero="
                   f"{urllib.parse.quote(sample_process_number)}"
                   if name == "cdDocumento_with_referer" else None)
        logger.info("auth-discovery: trying %s", name)
        result = _http_get(url, referer=referer, logger=logger)
        is_pdf = result.get("is_pdf", False)
        html_clue: Optional[str] = None
        if not is_pdf and result.get("html_sample"):
            sample = result["html_sample"].lower()
            if "login" in sample or "senha" in sample:
                html_clue = "html-login-page"
            elif "<html" in sample:
                html_clue = "html-other"
        record = {
            "variant": name,
            "url": url,
            "referer": referer,
            "status": result.get("status"),
            "content_type": result.get("content_type"),
            "size": result.get("size"),
            "is_pdf": is_pdf,
            "latency_ms": result.get("latency_ms"),
            "html_clue": html_clue,
            "html_sample_first_500": (result.get("html_sample") or "")[:500]
                                     if result.get("html_sample") else None,
            "error": result.get("error"),
        }
        results.append(record)
        if is_pdf and working_variant is None:
            working_variant = name
            sample_path = run_dir / "auth_discovery_sample.pdf"
            sample_path.write_bytes(result["body"])
            record["sample_pdf_saved"] = str(sample_path.name)

    out = {
        "sample_cd_documento": sample_cd_documento,
        "sample_process_number": sample_process_number,
        "variants_tested": results,
        "working_variant": working_variant,
        "selenium_fallback": None,
    }

    if working_variant is None:
        logger.info("auth-discovery: no unauthenticated variant; "
                    "trying Selenium-cookie fallback")
        sel = _try_selenium_cookie_fallback(
            sample_cd_documento, sample_process_number, run_dir, logger,
        )
        out["selenium_fallback"] = sel
        if sel.get("is_pdf"):
            working_variant = "selenium_cookies"
            out["working_variant"] = working_variant

    if working_variant == "selenium_cookies":
        out["conclusion"] = (
            "Plain unauthenticated HTTP rejected by eSAJ "
            "(\"Não foi possível validar o seu acesso a esse recurso\"). "
            "The doc URL pattern returns an HTML viewer (Pasta Digital), "
            "**not** the PDF — the PDF is loaded by JavaScript into an "
            "iframe whose src points to `/pastadigital/getPDF.do?<params>` "
            "after a ticketed-session hop through `abrirDocumentoEdt.do`. "
            "**The working flow:** Selenium loads the case page → clicks "
            "the doc anchor (new window opens with `?ticket=<encrypted>`) "
            "→ waits for the viewer's `<iframe id=documento>` src to be "
            "set → decodes the `file=` query param → urllib-fetches that "
            "URL with the current Selenium session cookies. This is the "
            "Phase 3 download path."
        )
    elif working_variant:
        out["conclusion"] = (
            f"Unauthenticated download works via `{working_variant}`."
        )
    else:
        out["conclusion"] = (
            "Neither unauthenticated HTTP nor the Selenium-mediated viewer "
            "flow produced a PDF body. Phase 3 needs deeper investigation."
        )

    write_json(run_dir / "step0_auth_discovery.json", out)
    return out


def _open_case_page_via_form(driver, process_number: str) -> str:
    """Mirror the production scraper flow: load the form, fill it, submit,
    wait for the case detail page. Returns the final URL (the post-submit
    case page URL, which is what eSAJ uses as the session-establishing context)."""
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    from poursuite.config import ESAJ_SEALED_ELEMENT_ID

    driver.get(ESAJ_CASE_URL)
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
    WebDriverWait(driver, 15).until(
        lambda d: d.find_elements(By.ID, "classeProcesso")
        or d.find_elements(By.ID, ESAJ_SEALED_ELEMENT_ID)
    )
    return driver.current_url


def _extract_pdf_via_viewer(driver, cd_documento: str,
                            logger: logging.Logger) -> Dict[str, Any]:
    """In the current driver session (which has already loaded a case page
    and clicked through to a viewer window), wait for the PDF viewer iframe
    and extract the actual /pastadigital/getPDF.do URL. Then fetch it with
    cookies harvested from the driver. Returns dict with PDF bytes or error."""
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By

    def _has_real_src(d):
        try:
            iframe = d.find_element(By.ID, "documento")
        except Exception:
            return False
        src = iframe.get_attribute("src") or ""
        return ("viewer.html" in src or "getPDF" in src) \
               and "processando.html" not in src

    try:
        WebDriverWait(driver, 30).until(_has_real_src)
    except Exception as e:
        return {"ok": False, "error": f"viewer iframe never loaded: {e}"}

    iframe = driver.find_element(By.ID, "documento")
    iframe_src = iframe.get_attribute("src") or ""
    m = re.search(r"file=([^&#]+)", iframe_src)
    if not m:
        return {"ok": False,
                "error": f"no file= param in iframe.src: {iframe_src[:200]}",
                "iframe_src": iframe_src}
    raw_pdf_url = urllib.parse.unquote(m.group(1))
    if raw_pdf_url.startswith("/"):
        raw_pdf_url = "https://esaj.tjsp.jus.br" + raw_pdf_url

    cookies = {c["name"]: c["value"] for c in driver.get_cookies()}
    result = _http_get(raw_pdf_url, cookies=cookies, referer=iframe_src,
                       logger=logger)
    return {
        "ok": result.get("is_pdf", False),
        "iframe_src": iframe_src,
        "pdf_url": raw_pdf_url,
        "status": result.get("status"),
        "content_type": result.get("content_type"),
        "size": result.get("size"),
        "is_pdf": result.get("is_pdf", False),
        "body": result.get("body") if result.get("is_pdf") else None,
        "latency_ms": result.get("latency_ms"),
        "html_sample_first_500": (result.get("html_sample") or "")[:500]
                                 if result.get("html_sample") else None,
        "error": None if result.get("is_pdf") else "non-PDF response",
    }


def _try_selenium_cookie_fallback(cd_documento: str, process_number: str,
                                  run_dir: Path,
                                  logger: logging.Logger) -> Dict[str, Any]:
    """Full Selenium-mediated discovery: load case via form-submit, click the
    doc anchor (opens viewer window), extract real PDF URL from iframe, fetch."""
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from poursuite.scraper._chrome import configure_chrome_options
    except ImportError as e:
        return {"ok": False, "error": f"selenium not available: {e}"}

    driver = None
    try:
        options = configure_chrome_options(headless=True)
        driver = webdriver.Chrome(options=options)
        logger.info("Selenium: opening case page for %s via form-submit",
                    process_number)
        _open_case_page_via_form(driver, process_number)
        main_handle = driver.current_window_handle

        # Expand the movements section if needed.
        try:
            mais = driver.find_elements(By.ID, "mostrarMaisMovimentacoes")
            if mais:
                driver.execute_script("arguments[0].click();", mais[0])
                WebDriverWait(driver, 5).until(
                    lambda d: len(d.find_elements(By.CSS_SELECTOR,
                                                  "a.linkMovVincProc")) > 0
                )
        except Exception:
            pass

        anchors = driver.find_elements(By.CSS_SELECTOR, "a.linkMovVincProc")
        target = None
        for a in anchors:
            href = a.get_attribute("href") or ""
            if f"cdDocumento={cd_documento}" in href:
                target = a
                break
        if target is None and anchors:
            target = anchors[0]
        if target is None:
            return {"ok": False,
                    "error": "no linkMovVincProc anchor found on case page"}

        anchor_href = target.get_attribute("href")
        driver.execute_script("arguments[0].click();", target)

        # Switch to the new (viewer) window.
        WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) > 1)
        for h in driver.window_handles:
            if h != main_handle:
                driver.switch_to.window(h)
                break

        extracted = _extract_pdf_via_viewer(driver, cd_documento, logger)
        record = {
            "ok": extracted.get("is_pdf", False),
            "anchor_href": anchor_href,
            "iframe_src": extracted.get("iframe_src"),
            "pdf_url": extracted.get("pdf_url"),
            "status": extracted.get("status"),
            "content_type": extracted.get("content_type"),
            "size": extracted.get("size"),
            "is_pdf": extracted.get("is_pdf"),
            "cookies_count": len(driver.get_cookies()),
            "cookie_names": sorted(c["name"] for c in driver.get_cookies()),
            "latency_ms": extracted.get("latency_ms"),
            "error": extracted.get("error"),
        }
        if extracted.get("is_pdf") and extracted.get("body"):
            sample_path = run_dir / "auth_discovery_sample.pdf"
            sample_path.write_bytes(extracted["body"])
            record["sample_pdf_saved"] = sample_path.name
        return record
    except Exception as e:  # noqa: BLE001
        logger.exception("Selenium fallback failed")
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# PDF analysis
# ---------------------------------------------------------------------------

def analyze_pdf(pdf_bytes: bytes, extract_text: bool = True) -> Dict[str, Any]:
    """PyMuPDF analysis. Returns page_count, text/image classification,
    extracted text + quality signals. Never raises."""
    try:
        import pymupdf  # type: ignore
    except ImportError:
        return {"ok": False, "error": "pymupdf not installed"}

    try:
        doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"open failed: {type(e).__name__}: {e}"}

    try:
        page_count = doc.page_count
        total_chars = 0
        non_ws_chars = 0
        page_text_chars: List[int] = []
        page_image_counts: List[int] = []
        text_parts: List[str] = []
        for page in doc:
            page_text = page.get_text("text") or ""
            page_text_chars.append(len(page_text))
            non_ws_chars += sum(1 for c in page_text if not c.isspace())
            total_chars += len(page_text)
            text_parts.append(page_text)
            try:
                imgs = page.get_images(full=True)
                page_image_counts.append(len(imgs))
            except Exception:
                page_image_counts.append(0)
        full_text = "\n".join(text_parts) if extract_text else None

        # Classification rule: < 50 non-whitespace chars per page on average
        # AND at least one image per page on average → image-PDF.
        avg_text = non_ws_chars / page_count if page_count else 0
        avg_imgs = (sum(page_image_counts) / page_count) if page_count else 0
        if avg_text < 50 and avg_imgs >= 1:
            classification = "image-pdf"
        elif avg_text < 50:
            classification = "empty-or-unknown"
        else:
            classification = "text-pdf"

        # Encoding heuristic: count multi-char mojibake sequences (specific
        # double-encoded patterns). NOT bare "Ã" — that's a legitimate
        # Portuguese character (São, ação, etc.) and over-counts.
        mojibake_markers = ("â€", "Ã©", "Ã¡", "Ã£", "Ã§", "â€™", "â€œ", "Ã³")
        mojibake_hits = sum(full_text.count(m) for m in mojibake_markers) if full_text else 0

        # Structural noise heuristic: identical lines repeating across pages
        # (header/footer noise).
        repeated_lines = 0
        if full_text:
            line_counter: Counter[str] = Counter()
            for part in text_parts:
                for raw in part.splitlines():
                    line = raw.strip()
                    if line and 4 <= len(line) <= 120:
                        line_counter[line] += 1
            repeated_lines = sum(c for c in line_counter.values()
                                 if c >= max(3, page_count // 2 + 1))

        return {
            "ok": True,
            "page_count": page_count,
            "classification": classification,
            "total_chars": total_chars,
            "non_ws_chars": non_ws_chars,
            "avg_text_per_page": round(avg_text, 1),
            "avg_images_per_page": round(avg_imgs, 1),
            "page_text_chars": page_text_chars,
            "page_image_counts": page_image_counts,
            "mojibake_marker_hits": mojibake_hits,
            "repeated_line_chars": repeated_lines,
            "full_text": full_text,
        }
    finally:
        doc.close()


# ---------------------------------------------------------------------------
# Per-document download + analyze
# ---------------------------------------------------------------------------

def download_doc_via_selenium(driver, main_handle: str,
                              process_number: str, doc_info: Dict[str, Any],
                              run_dir: Path,
                              logger: logging.Logger) -> Dict[str, Any]:
    """Drive the Selenium session already on the case page: locate the doc
    anchor, click it (opens viewer window), extract iframe src, decode PDF
    URL, fetch via urllib with current cookies."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait

    cd_documento = doc_info["cd_documento"]
    record: Dict[str, Any] = {
        "process_number": process_number,
        "ordem": doc_info["ordem"],
        "data_hora": doc_info["data_hora"],
        "movimento_nome": doc_info["nome"],
        "cd_documento": cd_documento,
        "is_pdf": False,
        "download_latency_ms": None,
        "error": None,
        "gated": False,
        "html_clue": None,
        "pdf_analysis": None,
    }

    t0 = time.perf_counter()
    try:
        # Make sure we are on the main (case) window for the anchor lookup.
        if driver.current_window_handle != main_handle:
            driver.switch_to.window(main_handle)

        anchors = driver.find_elements(By.CSS_SELECTOR, "a.linkMovVincProc")
        target = None
        for a in anchors:
            href = a.get_attribute("href") or ""
            if f"cdDocumento={cd_documento}" in href:
                target = a
                break
        if target is None:
            record["error"] = "anchor not found on case page"
            record["download_latency_ms"] = int((time.perf_counter() - t0) * 1000)
            return record

        record["anchor_href"] = target.get_attribute("href")

        existing_handles = set(driver.window_handles)
        driver.execute_script("arguments[0].click();", target)
        try:
            WebDriverWait(driver, 10).until(
                lambda d: len(d.window_handles) > len(existing_handles)
            )
        except Exception:
            record["error"] = "viewer window never opened"
            record["download_latency_ms"] = int((time.perf_counter() - t0) * 1000)
            return record
        new_handle = next((h for h in driver.window_handles
                           if h not in existing_handles), None)
        if not new_handle:
            record["error"] = "could not identify new window"
            record["download_latency_ms"] = int((time.perf_counter() - t0) * 1000)
            return record
        driver.switch_to.window(new_handle)

        # If eSAJ instead presented a "senha" prompt for this document
        # (#liberarAutoPorSenha), the viewer window's URL/title hints at it.
        try:
            page_text = driver.find_element(By.TAG_NAME, "body").text.lower()
            if "senha do processo" in page_text or "liberarautoporsenha" in page_text:
                record["gated"] = True
                record["html_clue"] = "liberarAutoPorSenha"
                driver.close()
                driver.switch_to.window(main_handle)
                record["download_latency_ms"] = int((time.perf_counter() - t0) * 1000)
                return record
        except Exception:
            pass

        extracted = _extract_pdf_via_viewer(driver, cd_documento, logger)
        record["iframe_src"] = extracted.get("iframe_src")
        record["pdf_url"] = extracted.get("pdf_url")
        record["http_status"] = extracted.get("status")
        record["content_type"] = extracted.get("content_type")
        record["size_bytes"] = extracted.get("size")
        record["is_pdf"] = extracted.get("is_pdf", False)
        record["error"] = extracted.get("error")

        if extracted.get("is_pdf") and extracted.get("body"):
            pdf_dir = run_dir / "raw_pdfs"
            pdf_dir.mkdir(parents=True, exist_ok=True)
            fname = f"{process_number}_{doc_info['ordem']:04d}_{cd_documento}.pdf"
            pdf_path = pdf_dir / fname
            pdf_path.write_bytes(extracted["body"])
            record["pdf_path"] = pdf_path.name

            analysis = analyze_pdf(extracted["body"], extract_text=True)
            record["pdf_analysis"] = {k: v for k, v in analysis.items()
                                      if k != "full_text"}
            if analysis.get("ok") and analysis.get("classification") == "text-pdf":
                text_dir = run_dir / "extracted_text"
                text_dir.mkdir(parents=True, exist_ok=True)
                text_path = text_dir / (pdf_path.stem + ".txt")
                text_path.write_text(analysis.get("full_text") or "", encoding="utf-8")
                record["text_path"] = text_path.name

        driver.close()
        driver.switch_to.window(main_handle)
    except Exception as e:  # noqa: BLE001
        logger.exception("download_doc_via_selenium failed for %s/%s",
                         process_number, cd_documento)
        record["error"] = f"{type(e).__name__}: {e}"
        # Try to recover the main window.
        try:
            if driver.current_window_handle != main_handle:
                driver.close()
                driver.switch_to.window(main_handle)
        except Exception:
            pass

    record["download_latency_ms"] = int((time.perf_counter() - t0) * 1000)
    return record


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def write_report(run_dir: Path, step0: Dict[str, Any],
                 per_case: Dict[str, List[Dict[str, Any]]],
                 dep_change: Dict[str, Any]) -> Path:
    md: List[str] = []
    md.append("# Phase 3 Probe — eSAJ Document Download + Extraction")
    md.append(f"_Run: `{run_dir.name}`  |  "
              f"Generated: {datetime.now(timezone.utc).isoformat()}_")
    md.append("")
    md.append("Reference brief: `CLAUDE_CODE_BRIEF_PHASE3_LAYER3_PROBE.md` (v1.2)")
    md.append("")

    md.append("## Dependency change")
    md.append("")
    md.append(f"- PyMuPDF added to `pyproject.toml`: "
              f"`{dep_change.get('pyproject_line', 'pymupdf>=1.24')}`")
    md.append(f"- Pre-existing in pyproject.toml: "
              f"{'yes' if dep_change.get('was_present') else 'no'}")
    md.append("")

    md.append("## Step 0 — auth/URL discovery")
    md.append("")
    md.append(f"Working variant: **{step0.get('working_variant') or 'NONE'}**")
    md.append("")
    md.append(step0.get("conclusion", ""))
    md.append("")
    md.append("Variants tested:")
    md.append("")
    md.append("| Variant | Status | Content-Type | Size | is_pdf | latency_ms | html_clue |")
    md.append("|---|---|---|---|---|---|---|")
    for v in step0.get("variants_tested", []):
        md.append(f"| {v['variant']} | {v.get('status')} | "
                  f"{(v.get('content_type') or '')[:30]} | "
                  f"{v.get('size')} | {v.get('is_pdf')} | "
                  f"{v.get('latency_ms')} | {v.get('html_clue') or '—'} |")
    sel = step0.get("selenium_fallback") or {}
    if sel:
        md.append(f"| selenium_cookies | {sel.get('status')} | "
                  f"{(sel.get('content_type') or '')[:30]} | "
                  f"{sel.get('size')} | {sel.get('is_pdf')} | "
                  f"{sel.get('latency_ms')} | "
                  f"cookies={sel.get('cookies_count', '—')} |")
    md.append("")
    if sel and sel.get("is_pdf"):
        md.append(f"Selenium harvested {sel.get('cookies_count')} cookies "
                  f"({', '.join(sel.get('cookie_names') or [])}) from the case "
                  "page before the doc GET succeeded. **Per-case cookie acquisition "
                  "is required** — the probe re-acquires cookies for each of the "
                  "4 cases (not once globally), since eSAJ session state appears "
                  "to be per-case.")
        md.append("")

    # If no working variant, the rest is mostly empty — say so.
    if not step0.get("working_variant"):
        md.append("## Downloads")
        md.append("")
        md.append("**Bulk download skipped — no unauthenticated variant worked.** "
                  "Production deep search would need a Selenium session "
                  "(open case page → harvest cookies → reuse on doc GET). "
                  "Out of scope for v1.2 of this probe.")
        md.append("")
        md.append("## Findings — sequencing implications for Phase 3")
        md.append("")
        md.append("- Phase 3 cannot rely on plain HTTP. Either:")
        md.append("  - (a) build a Selenium-driven download path (slower, "
                  "but matches the rest of the scraper);")
        md.append("  - (b) port the session/cookie acquisition to a stdlib "
                  "approach (requests.Session-style, login if needed).")
        md.append("- Per-case runtime estimate (download + extract) cannot "
                  "be measured in this probe run.")
        md.append("")
        out_path = run_dir / "phase3_findings.md"
        out_path.write_text("\n".join(md), encoding="utf-8")
        return out_path

    # Aggregate stats
    all_docs = [d for docs in per_case.values() for d in docs]
    total = len(all_docs)
    downloaded = sum(1 for d in all_docs if d.get("is_pdf"))
    gated = sum(1 for d in all_docs if d.get("gated"))
    errors = sum(1 for d in all_docs if d.get("error"))
    text_pdfs = sum(1 for d in all_docs
                    if d.get("pdf_analysis", {}).get("classification") == "text-pdf")
    image_pdfs = sum(1 for d in all_docs
                     if d.get("pdf_analysis", {}).get("classification") == "image-pdf")
    empty_pdfs = sum(1 for d in all_docs
                     if d.get("pdf_analysis", {}).get("classification") == "empty-or-unknown")

    md.append("## Aggregate stats")
    md.append("")
    md.append(f"- Documents attempted: **{total}** across {len(per_case)} cases")
    md.append(f"- Successfully downloaded as PDF: **{downloaded}**")
    md.append(f"- Gated (`#liberarAutoPorSenha`): **{gated}**")
    md.append(f"- HTTP errors / non-PDF responses: **{errors + (total - downloaded - gated)}**")
    md.append("")
    if downloaded:
        md.append(f"- Text-PDFs: **{text_pdfs}**")
        md.append(f"- Image-PDFs (would need OCR — counted, not processed): **{image_pdfs}**")
        md.append(f"- Empty / unknown classification: **{empty_pdfs}**")
        md.append("")

    md.append("## Per-case breakdown")
    md.append("")
    md.append("| Case | Docs attempted | Downloaded | Text-PDFs | Image-PDFs | Gated |")
    md.append("|---|---|---|---|---|---|")
    for proc, docs in per_case.items():
        n_total = len(docs)
        n_dl = sum(1 for d in docs if d.get("is_pdf"))
        n_text = sum(1 for d in docs
                     if d.get("pdf_analysis", {}).get("classification") == "text-pdf")
        n_image = sum(1 for d in docs
                      if d.get("pdf_analysis", {}).get("classification") == "image-pdf")
        n_gated = sum(1 for d in docs if d.get("gated"))
        md.append(f"| `{proc}` | {n_total} | {n_dl} | {n_text} | {n_image} | {n_gated} |")
    md.append("")

    # Per-case runtime as (min, median, max)
    md.append("## Per-case download runtime")
    md.append("")
    md.append("Reported as **(min, median, max) ms** over all download attempts "
              "in a case. Small sample (N=4 cases, ≤20 docs each), network-bound — "
              "treat as order-of-magnitude, not precise.")
    md.append("")
    md.append("| Case | Attempts | min | median | max | total |")
    md.append("|---|---|---|---|---|---|")
    for proc, docs in per_case.items():
        latencies = [d.get("download_latency_ms") for d in docs
                     if d.get("download_latency_ms") is not None]
        if not latencies:
            md.append(f"| `{proc}` | 0 | — | — | — | — |")
            continue
        md.append(f"| `{proc}` | {len(latencies)} | "
                  f"{min(latencies)} | {int(median(latencies))} | "
                  f"{max(latencies)} | {sum(latencies)} |")
    md.append("")

    # Extraction quality samples
    md.append("## Extraction quality — text-PDF samples")
    md.append("")
    samples_shown = 0
    for proc, docs in per_case.items():
        for d in docs:
            if samples_shown >= 3:
                break
            pa = d.get("pdf_analysis", {})
            if pa.get("classification") != "text-pdf":
                continue
            md.append(f"### `{proc}` ordem {d['ordem']} — cd {d['cd_documento']}")
            md.append("")
            md.append(f"- pages: {pa.get('page_count')}; "
                      f"avg text/page: {pa.get('avg_text_per_page')}; "
                      f"avg images/page: {pa.get('avg_images_per_page')}")
            md.append(f"- mojibake markers: {pa.get('mojibake_marker_hits')}; "
                      f"repeated header/footer line chars: "
                      f"{pa.get('repeated_line_chars')}")
            text_path = d.get("text_path")
            if text_path:
                full = (run_dir / "extracted_text" / text_path).read_text(encoding="utf-8")
                preview = full[:600].replace("\n", "\n> ")
                md.append("")
                md.append("Text preview (first 600 chars):")
                md.append("")
                md.append(f"> {preview}")
                md.append("")
            samples_shown += 1
        if samples_shown >= 3:
            break

    # Recommendations
    md.append("## Recommendations for Phase 3 build")
    md.append("")
    md.append("Based on the empirical run above:")
    md.append("")
    if downloaded == 0:
        md.append("- No successful downloads. Re-investigate the auth approach "
                  "before considering Phase 3 sizing.")
    else:
        if step0.get("working_variant") == "selenium_cookies":
            md.append("- Download path **works via Selenium-mediated viewer flow** "
                      "(see Step 0 conclusion). Production Phase 3 cannot use plain HTTP — "
                      "the doc URL returns an HTML viewer, and the PDF lives behind a "
                      "ticketed `abrirDocumentoEdt.do` hop only reachable from a "
                      "JavaScript-driven click. **Implication for the 1-2 week estimate:** "
                      "the downloader is heavier than originally assumed (per-doc Selenium "
                      "round-trip) — runtime budget below.")
        else:
            md.append(f"- Download path **works** via `{step0.get('working_variant')}`. "
                      "Production Phase 3 can use plain HTTP.")
        if image_pdfs > 0:
            md.append(f"- {image_pdfs} image-PDF(s) seen — out of v1 scope. "
                      "Phase 3 v1 ships text-only; image-PDF queue is a v2 problem.")
        if text_pdfs > 0:
            text_sizes = [d["pdf_analysis"]["total_chars"] for d in all_docs
                          if d.get("pdf_analysis", {}).get("classification") == "text-pdf"]
            if text_sizes:
                md.append(f"- Text-PDF size distribution: median "
                          f"{int(median(text_sizes))} chars, "
                          f"min {min(text_sizes)}, max {max(text_sizes)}. "
                          "FTS5 indexing strategy: index per-document; consider "
                          "chunking only if max sizes routinely exceed ~500k chars.")
        all_latencies = [d.get("download_latency_ms") for d in all_docs
                         if d.get("download_latency_ms") is not None]
        if all_latencies:
            md.append(f"- Per-doc download latency: median "
                      f"{int(median(all_latencies))} ms across {len(all_latencies)} "
                      f"successful HTTP rounds. A 20-doc case ≈ "
                      f"{int(median(all_latencies)) * 20 / 1000:.1f}s download time "
                      f"(serial) — small-sample estimate, network-bound.")
    md.append("")
    md.append("## Petição-document gap (v1.2 scope note)")
    md.append("")
    md.append("Probe A is movimento-only. `peticao.cd_documento` is always NULL "
              "in the snapshot store today (eSAJ does not expose IDs in the "
              "Petições diversas section — see `poursuite/scraper/peticoes.py`). "
              "Phase 3 will need a separate inventory before petição documents "
              "can be included.")
    md.append("")
    md.append("## Case-mix unmet (v1.2 scope note)")
    md.append("")
    md.append("The brief originally targeted 5 cases of varied shapes including "
              "the 1990 legacy case and a 2015-2020 mid-period case. Neither "
              "exists in the snapshot store today. Ran on the 4 currently-"
              "loaded cases (2020 / 2022 / 2023×2). Findings are representative "
              "of recent post-digital-era cases only.")
    md.append("")

    out_path = run_dir / "phase3_findings.md"
    out_path.write_text("\n".join(md), encoding="utf-8")
    return out_path


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run(run_dir: Path, logger: logging.Logger,
        db_path: Path = DEFAULT_DB_PATH) -> Dict[str, Any]:
    logger.info("Phase 3 probe — run dir: %s", run_dir)

    # Dep change record
    pyproject = Path("pyproject.toml")
    was_present = False
    if pyproject.exists():
        was_present = "pymupdf" in pyproject.read_text(encoding="utf-8").lower()
    dep_change = {"was_present": was_present, "pyproject_line": "pymupdf>=1.24"}
    write_json(run_dir / "dep_change.json", dep_change)

    # 1. Enumerate
    if not db_path.exists():
        logger.error("Snapshot DB not found: %s", db_path)
        return {"ok": False, "error": f"snapshot db not found: {db_path}"}
    docs = enumerate_documents(db_path)
    write_json(run_dir / "enumerated_docs.json", docs)
    total_docs = sum(len(d) for d in docs.values())
    logger.info("Enumerated %d documents across %d cases", total_docs, len(docs))

    # 2. Step 0 — auth/URL discovery against first available doc
    sample_proc = next((p for p, d in docs.items() if d), None)
    if not sample_proc:
        logger.error("No documents to test against.")
        return {"ok": False, "error": "no documents in snapshot store"}
    sample_doc = docs[sample_proc][0]
    step0 = discover_download_approach(
        sample_doc["cd_documento"], sample_proc, run_dir, logger,
    )
    working_variant = step0.get("working_variant")

    # 3. Bulk download via Selenium-mediated viewer flow (the only working path
    #    per step 0). One driver per case; navigate to case page once, then
    #    drive through each doc.
    per_case: Dict[str, List[Dict[str, Any]]] = {p: [] for p in docs}
    if working_variant == "selenium_cookies":
        try:
            from selenium import webdriver
            from poursuite.scraper._chrome import configure_chrome_options
        except ImportError as e:
            logger.error("Selenium not available for bulk download: %s", e)
            working_variant = None

    if working_variant == "selenium_cookies":
        for proc, doc_list in docs.items():
            if not doc_list:
                continue
            logger.info("Opening Selenium session for %s (%d docs)",
                        proc, len(doc_list))
            driver = None
            try:
                driver = webdriver.Chrome(
                    options=configure_chrome_options(headless=True)
                )
                _open_case_page_via_form(driver, proc)
                main_handle = driver.current_window_handle

                # Expand "mais" if present so all anchors render.
                try:
                    from selenium.webdriver.common.by import By
                    from selenium.webdriver.support.ui import WebDriverWait
                    mais = driver.find_elements(By.ID, "mostrarMaisMovimentacoes")
                    if mais:
                        driver.execute_script("arguments[0].click();", mais[0])
                        WebDriverWait(driver, 5).until(
                            lambda d: len(d.find_elements(
                                By.CSS_SELECTOR, "a.linkMovVincProc")) > 0
                        )
                except Exception:
                    pass

                for doc_info in doc_list:
                    rec = download_doc_via_selenium(
                        driver, main_handle, proc, doc_info, run_dir, logger,
                    )
                    per_case[proc].append(rec)
                    logger.info("  %s ordem=%d cd=%s: pdf=%s latency=%dms",
                                proc, doc_info["ordem"], doc_info["cd_documento"],
                                rec.get("is_pdf"),
                                rec.get("download_latency_ms") or 0)
            except Exception as e:  # noqa: BLE001
                logger.exception("Driver-level failure for %s", proc)
                per_case[proc].append({
                    "process_number": proc, "error": f"{type(e).__name__}: {e}",
                    "is_pdf": False,
                })
            finally:
                if driver is not None:
                    try:
                        driver.quit()
                    except Exception:
                        pass
            write_json(run_dir / f"case_{proc.replace('/', '_')}.json",
                       per_case[proc])
    else:
        logger.warning("No working variant — skipping bulk download.")

    # 4. Report
    report_path = write_report(run_dir, step0, per_case, dep_change)
    logger.info("Wrote %s", report_path)

    return {
        "ok": True,
        "working_variant": working_variant,
        "total_docs_attempted": total_docs,
        "report_path": str(report_path),
    }
