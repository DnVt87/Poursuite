import logging
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, List, Optional

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from poursuite.config import (
    ESAJ_OUTPUT_DIR,
    ESAJ_SEALED_ELEMENT_ID,
    ESAJ_SEALED_TEXT,
    ESAJ_URL,
    PROCESS_NUMBER_PATTERN_STRICT,
)
from poursuite.models import Movimento, ProcessData, ScrapeResult
from poursuite.scraper._chrome import configure_chrome_options
from poursuite.scraper.cnj_origem import derive_from_cnj
from poursuite.scraper.movimentos import is_full_timeline, parse_movimentos
from poursuite.scraper.sections import expand_section_collapsibles
from poursuite.utils import format_currency, setup_logging

logger = setup_logging("tjsp_scraper")


class ProcessValueScraper:
    """Scrapes process data from the eSAJ system (tjsp.jus.br)."""

    FIELD_MAPPINGS = {
        "initial_date": {"type": "div", "id": "dataHoraDistribuicaoProcesso", "slice": slice(0, 10)},
        "class_type": {"type": "span", "id": "classeProcesso"},
        "subject": {"type": "span", "id": "assuntoProcesso"},
        "value": {"type": "div", "id": "valorAcaoProcesso"},
        "last_movement": {"type": "td", "class_": "dataMovimentacao"},
        "status": {"type": "span", "id": "labelSituacaoProcesso", "class_": "unj-tag"},
        # Phase 1: additional header fields with stable IDs (foro/vara/juiz in
        # the primary panel; controle/area inside #maisDetalhes).
        "foro": {"type": "span", "id": "foroProcesso"},
        "vara": {"type": "span", "id": "varaProcesso"},
        "juiz": {"type": "span", "id": "juizProcesso"},
        "controle": {"type": "div", "id": "numeroControleProcesso"},
        "area": {"type": "div", "id": "areaProcesso"},
    }

    # Phase 1: header fields that have NO id on the value div — only a
    # preceding <span class="unj-label">{label}</span> sibling. Extracted
    # by label-text match. Rare fields, conditionally rendered.
    LABEL_FIELDS = {
        "outros_assuntos": "Outros assuntos",
        "outros_numeros": "Outros números",
        "local_fisico": "Local Físico",
    }

    def __init__(self, max_concurrent_browsers: int = 4) -> None:
        self.max_concurrent_browsers = max_concurrent_browsers
        self.options = configure_chrome_options()
        ESAJ_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        self._drivers: Dict[int, webdriver.Chrome] = {}
        self._driver_lock = threading.Lock()

    def __del__(self) -> None:
        self._cleanup_all_drivers()

    # ------------------------------------------------------------------
    # Driver lifecycle
    # ------------------------------------------------------------------

    def _get_driver(self) -> webdriver.Chrome:
        """Return (or create) the Chrome instance for the current thread."""
        tid = threading.get_ident()
        with self._driver_lock:
            if tid not in self._drivers:
                self._drivers[tid] = webdriver.Chrome(options=self.options)
        return self._drivers[tid]

    def _cleanup_thread_driver(self) -> None:
        """Quit and remove the Chrome instance for the current thread."""
        tid = threading.get_ident()
        with self._driver_lock:
            driver = self._drivers.pop(tid, None)
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    def _cleanup_all_drivers(self) -> None:
        with self._driver_lock:
            drivers = list(self._drivers.values())
            self._drivers.clear()
        for driver in drivers:
            try:
                driver.quit()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_process_number(process_number: str) -> None:
        if not re.match(PROCESS_NUMBER_PATTERN_STRICT, process_number):
            raise ValueError(
                f"Invalid process number format: {process_number}. "
                "Expected: NNNNNNN-DD.AAAA.J.TR.OOOO"
            )

    # ------------------------------------------------------------------
    # Page interaction
    # ------------------------------------------------------------------

    @staticmethod
    def _fill_process_form(driver: webdriver.Chrome, process_number: str) -> None:
        """Fill and submit the process search form. Waits for each element."""
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

    @staticmethod
    def _wait_for_results(driver: webdriver.Chrome) -> None:
        """Wait for the results page to load after form submission."""
        try:
            WebDriverWait(driver, 15).until(
                lambda d: d.find_elements(By.ID, "classeProcesso")
                or d.find_elements(By.ID, ESAJ_SEALED_ELEMENT_ID)
            )
        except TimeoutException:
            pass  # Extraction will handle missing elements gracefully

    # ------------------------------------------------------------------
    # Data extraction
    # ------------------------------------------------------------------

    @staticmethod
    def _is_sealed_case(soup: BeautifulSoup) -> bool:
        element = soup.find("span", id=ESAJ_SEALED_ELEMENT_ID)
        return element is not None and ESAJ_SEALED_TEXT.lower() in element.text.lower()

    def _extract_field(self, soup: BeautifulSoup, config: dict) -> Optional[str]:
        # Build find() kwargs explicitly — passing class_=None excludes
        # elements with any class attribute (contrary to BS4 docs).
        find_kwargs = {}
        if "id" in config:
            find_kwargs["id"] = config["id"]
        if "class_" in config:
            find_kwargs["class_"] = config["class_"]
        element = soup.find(config["type"], **find_kwargs)
        if not element:
            return None
        value = element.text.strip()
        if config.get("id") == "valorAcaoProcesso":
            return format_currency(value)
        if "slice" in config:
            value = value[config["slice"]]
        return value

    @staticmethod
    def _extract_field_by_label(soup: BeautifulSoup, label_text: str) -> Optional[str]:
        """Find a `<span class="unj-label">{label_text}</span>` and return the
        text of the immediately-following sibling element.

        Used for header fields that eSAJ renders without an id on the value
        div (Outros assuntos, Outros números, Local Físico). Returns None
        when the label isn't present — these fields are conditionally
        rendered (1-5 cases out of 13 in the inventory pass).
        """
        label_el = soup.find(
            "span",
            class_="unj-label",
            string=lambda s: s is not None and s.strip() == label_text,
        )
        if label_el is None:
            return None
        sibling = label_el.find_next_sibling()
        if sibling is None:
            return None
        text = sibling.get_text(" ", strip=True)
        return text or None

    @staticmethod
    def _extract_parties(soup: BeautifulSoup):
        parties = soup.find_all("td", class_="nomeParteEAdvogado")
        if len(parties) < 2:
            return None, None
        return (
            parties[0].text.strip().partition("\n")[0],
            parties[1].text.strip().partition("\n")[0],
        )

    def _extract_process_data(self, soup: BeautifulSoup, process_number: str) -> ProcessData:
        # Check for sealed case before attempting field extraction.
        # Note: derived fields (foro_code, tribunal_code, distribution_year)
        # are populated even for sealed cases — they come from the process
        # number itself, not from the page.
        derived = derive_from_cnj(process_number)
        if self._is_sealed_case(soup):
            return ProcessData(
                number=process_number,
                error="Segredo de justiça",
                foro_code=derived["foro_code"],
                tribunal_code=derived["tribunal_code"],
                distribution_year=derived["distribution_year"],
            )
        try:
            data = {
                field: self._extract_field(soup, config)
                for field, config in self.FIELD_MAPPINGS.items()
            }
            label_data = {
                field: self._extract_field_by_label(soup, label)
                for field, label in self.LABEL_FIELDS.items()
            }
            plaintiff, defendant = self._extract_parties(soup)
            return ProcessData(
                number=process_number,
                initial_date=data["initial_date"],
                class_type=data["class_type"],
                subject=data["subject"],
                value=data["value"],
                last_movement=data["last_movement"],
                status=data["status"],
                plaintiff=plaintiff,
                defendant=defendant,
                other_processes=None,
                foro=data["foro"],
                vara=data["vara"],
                juiz=data["juiz"],
                controle=data["controle"],
                outros_assuntos=label_data["outros_assuntos"],
                outros_numeros=label_data["outros_numeros"],
                local_fisico=label_data["local_fisico"],
                area=data["area"],
                foro_code=derived["foro_code"],
                tribunal_code=derived["tribunal_code"],
                distribution_year=derived["distribution_year"],
                error=None,
            )
        except Exception as e:
            return ProcessData(number=process_number, error=f"Extraction error: {e}")

    # ------------------------------------------------------------------
    # Single-process scraping
    # ------------------------------------------------------------------

    def get_process_data(
        self, process_number: str, include_other_processes: bool = True
    ) -> ProcessData:
        """Scrape data for a single process number."""
        try:
            self._validate_process_number(process_number)
            driver = self._get_driver()

            driver.get(ESAJ_URL)
            self._fill_process_form(driver, process_number)
            self._wait_for_results(driver)

            try:
                # Target the header expand-collapse anchor specifically. The
                # page also renders "Mais" links for `#linkpartes` and
                # `#linkmovimentacoes` — the previous LINK_TEXT match was
                # ambiguous. See poursuite/probes/esaj_inventory.py for the
                # full enumeration.
                mais = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable(
                        (By.CSS_SELECTOR, "a[href='#maisDetalhes']")
                    )
                )
                driver.execute_script("arguments[0].click();", mais)
                try:
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.ID, "dataHoraDistribuicaoProcesso"))
                    )
                except TimeoutException:
                    pass
            except TimeoutException:
                pass

            soup = BeautifulSoup(driver.page_source, "html.parser")
            process_data = self._extract_process_data(soup, process_number)

            if include_other_processes and process_data.defendant and not process_data.error:
                process_data.other_processes = self._get_other_processes_count(
                    driver, process_data.defendant
                )

            return process_data

        except Exception as e:
            return ProcessData(number=process_number, error=str(e))

    def get_process_record(
        self,
        process_number: str,
        include_other_processes: bool = True,
        include_movimentos: bool = True,
    ) -> ScrapeResult:
        """Scrape header + movimentos in one driver session.

        Returns a ScrapeResult containing the ProcessData plus the
        movimentos timeline. The `linkmovimentacoes` expand click is
        attempted before parsing; if it fails, parse_movimentos falls
        back to the visible-by-default ultimas table and an anomaly is
        logged (caller sees a partial timeline rather than nothing).
        """
        try:
            self._validate_process_number(process_number)
            driver = self._get_driver()

            driver.get(ESAJ_URL)
            self._fill_process_form(driver, process_number)
            self._wait_for_results(driver)

            # Expand the header `Mais` first — same logic as get_process_data.
            try:
                mais = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable(
                        (By.CSS_SELECTOR, "a[href='#maisDetalhes']")
                    )
                )
                driver.execute_script("arguments[0].click();", mais)
                try:
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.ID, "dataHoraDistribuicaoProcesso"))
                    )
                except TimeoutException:
                    pass
            except TimeoutException:
                pass

            soup = BeautifulSoup(driver.page_source, "html.parser")
            process_data = self._extract_process_data(soup, process_number)

            movimentos: List[Movimento] = []
            if include_movimentos and not process_data.error:
                # Expose tabelaTodasMovimentacoes by clicking the section toggle.
                clicked = expand_section_collapsibles(driver, ids=("linkmovimentacoes",))
                # Re-parse the page after the expand.
                movs_soup = BeautifulSoup(driver.page_source, "html.parser")
                if not is_full_timeline(movs_soup):
                    logger.warning(
                        "movimentos: full timeline not loaded for %s "
                        "(linkmovimentacoes expand: %s) — parsing visible subset",
                        process_number, clicked,
                    )
                movimentos = parse_movimentos(movs_soup)

            if include_other_processes and process_data.defendant and not process_data.error:
                process_data.other_processes = self._get_other_processes_count(
                    driver, process_data.defendant
                )

            return ScrapeResult(process_data=process_data, movimentos=movimentos)

        except Exception as e:
            return ScrapeResult(
                process_data=ProcessData(number=process_number, error=str(e)),
                movimentos=[],
            )

    def _get_other_processes_count(
        self, driver: webdriver.Chrome, defendant_name: str
    ) -> Optional[int]:
        """Search eSAJ by defendant name and return total process count."""
        try:
            driver.get(ESAJ_URL)

            select = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "cbPesquisa"))
            )
            select.send_keys("NMPARTE")

            checkbox = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "pesquisarPorNomeCompleto"))
            )
            driver.execute_script("arguments[0].click();", checkbox)

            name_field = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "campo_NMPARTE"))
            )
            name_field.clear()
            name_field.send_keys(defendant_name)

            btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.ID, "botaoConsultarProcessos"))
            )
            btn.click()

            try:
                count_el = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.ID, "contadorDeProcessos"))
                )
                return int(count_el.text.strip().split()[0])
            except TimeoutException:
                return 0
            except (IndexError, ValueError):
                return 0

        except Exception as e:
            logger.error(f"Error getting process count: {e}")
            return 0
        finally:
            driver.delete_all_cookies()

    # ------------------------------------------------------------------
    # Batch processing
    # ------------------------------------------------------------------

    def process_batch(
        self,
        process_numbers: List[str],
        include_other_processes: bool = False,
        progress_callback: Optional[Callable[[ProcessData], None]] = None,
    ) -> List[ProcessData]:
        """Scrape data for multiple process numbers using a thread pool.

        Results are delivered to progress_callback in completion order as they
        arrive. The return value restores the original input order.
        """
        total = len(process_numbers)
        logger.info(
            f"Processing {total} processes with {self.max_concurrent_browsers} concurrent browsers"
        )

        results: List[ProcessData] = []

        def scrape_one(pn: str) -> ProcessData:
            try:
                return self.get_process_data(pn, include_other_processes=include_other_processes)
            except Exception as e:
                return ProcessData(number=pn, error=f"Worker error: {e}")
            finally:
                self._cleanup_thread_driver()

        with ThreadPoolExecutor(max_workers=self.max_concurrent_browsers) as executor:
            futures = {executor.submit(scrape_one, pn): pn for pn in process_numbers}
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                logger.info(f"Progress: {len(results)}/{total} — {result.number}")
                if progress_callback:
                    progress_callback(result)

        # Restore original input order
        pn_to_result = {r.number: r for r in results}
        return [
            pn_to_result.get(pn, ProcessData(number=pn, error="No result returned"))
            for pn in process_numbers
        ]

    def process_batch_records(
        self,
        process_numbers: List[str],
        include_other_processes: bool = False,
        include_movimentos: bool = True,
        progress_callback: Optional[Callable[[ScrapeResult], None]] = None,
    ) -> List[ScrapeResult]:
        """Scrape multiple process numbers with full record (header + movimentos).

        Same threading/ordering semantics as process_batch. The progress
        callback receives ScrapeResult objects in completion order; the
        return value restores input order.
        """
        total = len(process_numbers)
        logger.info(
            f"Processing {total} processes (with movimentos) "
            f"with {self.max_concurrent_browsers} concurrent browsers"
        )

        results: List[ScrapeResult] = []

        def scrape_one(pn: str) -> ScrapeResult:
            try:
                return self.get_process_record(
                    pn,
                    include_other_processes=include_other_processes,
                    include_movimentos=include_movimentos,
                )
            except Exception as e:
                return ScrapeResult(
                    process_data=ProcessData(number=pn, error=f"Worker error: {e}"),
                    movimentos=[],
                )
            finally:
                self._cleanup_thread_driver()

        with ThreadPoolExecutor(max_workers=self.max_concurrent_browsers) as executor:
            futures = {executor.submit(scrape_one, pn): pn for pn in process_numbers}
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                pn = result.process_data.number
                logger.info(
                    f"Progress: {len(results)}/{total} — {pn} "
                    f"({len(result.movimentos)} movs)"
                )
                if progress_callback:
                    progress_callback(result)

        # Restore original input order
        pn_to_result = {r.process_data.number: r for r in results}
        return [
            pn_to_result.get(
                pn,
                ScrapeResult(
                    process_data=ProcessData(number=pn, error="No result returned"),
                ),
            )
            for pn in process_numbers
        ]
