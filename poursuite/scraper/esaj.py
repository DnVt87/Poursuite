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
from poursuite.models import ProcessData
from poursuite.utils import format_currency, setup_logging

logger = setup_logging("tjsp_scraper")


def _configure_chrome_options() -> webdriver.ChromeOptions:
    """Configure headless Chrome options."""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--log-level=3")          # suppress INFO/WARNING/ERROR logs
    options.add_argument("--disable-logging")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    return options


class ProcessValueScraper:
    """Scrapes process data from the eSAJ system (tjsp.jus.br)."""

    FIELD_MAPPINGS = {
        "initial_date": {"type": "div", "id": "dataHoraDistribuicaoProcesso", "slice": slice(0, 10)},
        "class_type": {"type": "span", "id": "classeProcesso"},
        "subject": {"type": "span", "id": "assuntoProcesso"},
        "value": {"type": "div", "id": "valorAcaoProcesso"},
        "last_movement": {"type": "td", "class_": "dataMovimentacao"},
        "status": {"type": "span", "id": "labelSituacaoProcesso", "class_": "unj-tag"},
    }

    def __init__(self, max_concurrent_browsers: int = 4) -> None:
        self.max_concurrent_browsers = max_concurrent_browsers
        self.options = _configure_chrome_options()
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
        element = soup.find(config["type"], id=config.get("id"), class_=config.get("class_"))
        if not element:
            return None
        value = element.text.strip()
        if config.get("id") == "valorAcaoProcesso":
            return format_currency(value)
        if "slice" in config:
            value = value[config["slice"]]
        return value

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
        # Check for sealed case before attempting field extraction
        if self._is_sealed_case(soup):
            return ProcessData(number=process_number, error="Segredo de justiça")
        try:
            data = {
                field: self._extract_field(soup, config)
                for field, config in self.FIELD_MAPPINGS.items()
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
                mais = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.LINK_TEXT, "Mais"))
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
