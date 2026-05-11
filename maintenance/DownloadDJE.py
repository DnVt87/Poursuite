"""TJSP DJE downloader.

The DJE web form (https://dje.tjsp.jus.br/cdje) deliberately disables both the
caderno <select> and the Download button on initial page load; the only
documented re-enable path is clicking through the JsDatePick calendar widget,
which is not reliably driveable from Selenium (the date input is `readonly` and
the calendar's enable logic does not fire from a programmatic value set or a
plain `change` dispatch).

The download endpoint itself is a simple GET that requires only the session
cookies issued by loading /cdje. We bypass the form and hit it directly:

    /cdje/downloadCaderno.do?dtDiario=DD/MM/YYYY&cdCaderno=NN&tpDownload=D

A successful response is `Content-Type: application/octet-stream` with the PDF
body. If the date has no publication (weekend, holiday, recesso forense), the
endpoint returns a small `text/html` error page instead — we detect that and
record the date as skipped.
"""
from __future__ import annotations

import http.cookiejar
import logging
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from poursuite.config import COURT_DOCS_DIR
from poursuite.utils import setup_logging


@dataclass
class Caderno:
    """Configuration for each document type to download."""
    value: str
    name: str


class TJSPScraper:
    """Downloads DJE PDFs by hitting the download endpoint directly."""

    BASE_URL = "https://dje.tjsp.jus.br/cdje"
    DOWNLOAD_URL = BASE_URL + "/downloadCaderno.do"
    BASE_DIR = COURT_DOCS_DIR

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
    REQUEST_TIMEOUT_SECONDS = 60
    PDF_CONTENT_TYPE_PREFIX = "application/octet-stream"

    CADERNOS = [
        Caderno("12", "Judicial_Capital_1"),
        Caderno("20", "Judicial_Capital_2"),
        Caderno("18", "Judicial_Interior_1"),
        Caderno("13", "Judicial_Interior_2"),
        Caderno("15", "Judicial_Interior_3"),
    ]

    def __init__(self):
        self.logger = setup_logging("tjsp_scraper")
        self.BASE_DIR.mkdir(parents=True, exist_ok=True)
        self._opener: Optional[urllib.request.OpenerDirector] = None

    def _get_opener(self) -> urllib.request.OpenerDirector:
        """Build a session-bearing opener; primes JSESSIONID by hitting the index page."""
        if self._opener is not None:
            return self._opener
        cj = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
        opener.addheaders = [("User-Agent", self.USER_AGENT)]
        try:
            with opener.open(self.BASE_URL, timeout=self.REQUEST_TIMEOUT_SECONDS) as r:
                r.read(0)  # discard body; we only want cookies
            cookie_names = sorted(c.name for c in cj)
            self.logger.info(f"Session primed; cookies: {cookie_names}")
        except Exception as e:
            self.logger.error(f"Failed to prime session: {e}")
            raise
        self._opener = opener
        return opener

    @staticmethod
    def _is_valid_date(d: datetime, today: datetime) -> bool:
        """Pre-filter dates the endpoint will refuse anyway (future + weekends)."""
        if d > today:
            return False
        if d.weekday() >= 5:  # Saturday=5, Sunday=6
            return False
        return True

    def _date_directory(self, d: datetime) -> Path:
        """Final on-disk location: BASE_DIR/<year>/<MM>/."""
        out = self.BASE_DIR / str(d.year) / f"{d.month:02d}"
        out.mkdir(parents=True, exist_ok=True)
        return out

    def _download_one(self, caderno: Caderno, d: datetime) -> bool:
        """Download a single caderno PDF for a given date. True on success/already-present."""
        date_label = d.strftime("%d/%m/%Y")
        date_stem = d.strftime("%Y%m%d")
        out_path = self._date_directory(d) / f"{date_stem}_{caderno.name}.pdf"

        if out_path.exists():
            self.logger.debug(f"Already on disk: {out_path.name}")
            return True

        url = (
            f"{self.DOWNLOAD_URL}"
            f"?dtDiario={date_label.replace('/', '%2F')}"
            f"&cdCaderno={caderno.value}"
            f"&tpDownload=D"
        )

        opener = self._get_opener()
        try:
            with opener.open(url, timeout=self.REQUEST_TIMEOUT_SECONDS) as resp:
                ct = (resp.headers.get("Content-Type") or "").lower()
                if not ct.startswith(self.PDF_CONTENT_TYPE_PREFIX):
                    # The endpoint returns a small text/html page when there's no
                    # publication for the date (weekend caught past our pre-filter,
                    # holiday, recesso forense). Treat as a non-fatal skip.
                    body_preview = resp.read(200)
                    self.logger.info(
                        f"No PDF for {caderno.name} on {date_label} "
                        f"(Content-Type={ct!r}); preview={body_preview[:80]!r}"
                    )
                    return False

                tmp_path = out_path.with_suffix(out_path.suffix + ".part")
                with open(tmp_path, "wb") as f:
                    while True:
                        chunk = resp.read(64 * 1024)
                        if not chunk:
                            break
                        f.write(chunk)
                tmp_path.replace(out_path)
                self.logger.info(f"Downloaded {out_path.name}")
                return True
        except urllib.error.URLError as e:
            self.logger.error(f"Network error fetching {caderno.name} for {date_label}: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error fetching {caderno.name} for {date_label}: {e}")
            return False

    def download_documents(self, start_date: str, end_date: Optional[str] = None) -> Dict[str, List[str]]:
        """Download every caderno for every business day in [start_date, end_date]."""
        results: Dict[str, List[str]] = {"successful": [], "failed": []}
        try:
            start = datetime.strptime(start_date, "%d/%m/%Y")
            end = datetime.strptime(end_date, "%d/%m/%Y") if end_date else start
        except ValueError as e:
            self.logger.error(f"Invalid date format (expected DD/MM/YYYY): {e}")
            return results

        today = datetime.now()
        current = start
        while current <= end:
            label = current.strftime("%d/%m/%Y")
            if not self._is_valid_date(current, today):
                self.logger.info(f"Skipping {label} (weekend or future)")
                current += timedelta(days=1)
                continue
            for caderno in self.CADERNOS:
                key = f"{caderno.name}_{label}"
                if self._download_one(caderno, current):
                    results["successful"].append(key)
                else:
                    results["failed"].append(key)
                # Light pacing to avoid hammering the server
                time.sleep(0.2)
            current += timedelta(days=1)

        self.logger.info(
            f"Done. {len(results['successful'])} successful, {len(results['failed'])} failed/skipped."
        )
        return results


if __name__ == "__main__":
    scraper = TJSPScraper()
    out = scraper.download_documents("16/05/2024")
    print(f"Successful: {len(out['successful'])}; failed/skipped: {len(out['failed'])}")
