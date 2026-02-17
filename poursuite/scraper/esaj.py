import logging
import queue
import re
import threading
import time
from dataclasses import fields
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
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
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    return options


class ProcessValueScraper:
    """Scrapes process data from the eSAJ system (tjsp.jus.br)."""

    FIELD_MAPPINGS = {
        'initial_date': {'type': 'div', 'id': 'dataHoraDistribuicaoProcesso', 'slice': slice(0, 10)},
        'class_type': {'type': 'span', 'id': 'classeProcesso'},
        'subject': {'type': 'span', 'id': 'assuntoProcesso'},
        'value': {'type': 'div', 'id': 'valorAcaoProcesso'},
        'last_movement': {'type': 'td', 'class_': 'dataMovimentacao'},
        'status': {'type': 'span', 'id': 'labelSituacaoProcesso', 'class_': 'unj-tag'},
    }

    def __init__(self, max_concurrent_browsers: int = 4) -> None:
        self.max_concurrent_browsers = max_concurrent_browsers
        self.options = _configure_chrome_options()
        ESAJ_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        self.drivers: Dict[int, webdriver.Chrome] = {}
        self.driver_lock = threading.Lock()
        self.results_queue: queue.Queue = queue.Queue()

    def __del__(self):
        self._cleanup_all_drivers()

    # ------------------------------------------------------------------
    # Driver lifecycle
    # ------------------------------------------------------------------

    def _setup_webdriver(self) -> webdriver.Chrome:
        thread_id = threading.get_ident()
        with self.driver_lock:
            if thread_id not in self.drivers:
                try:
                    driver = webdriver.Chrome(options=self.options)
                    self.drivers[thread_id] = driver
                except Exception as e:
                    logger.error(f"Error creating webdriver: {e}")
                    raise
        return self.drivers[thread_id]

    def _cleanup_thread_driver(self) -> None:
        thread_id = threading.get_ident()
        with self.driver_lock:
            if thread_id in self.drivers:
                try:
                    self.drivers[thread_id].quit()
                except Exception:
                    pass
                finally:
                    del self.drivers[thread_id]

    def _cleanup_all_drivers(self) -> None:
        with self.driver_lock:
            for driver in self.drivers.values():
                try:
                    driver.quit()
                except Exception:
                    pass
            self.drivers.clear()

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_process_number(self, process_number: str) -> bool:
        if not re.match(PROCESS_NUMBER_PATTERN_STRICT, process_number):
            raise ValueError(
                f"Invalid process number format: {process_number}. "
                "Please use: NNNNNNN-DD.AAAA.J.TR.OOOO"
            )
        return True

    # ------------------------------------------------------------------
    # Page interaction
    # ------------------------------------------------------------------

    @staticmethod
    def _fill_process_form(driver: webdriver.Chrome, process_number: str) -> None:
        try:
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
        except Exception as e:
            raise Exception(f"Error filling form: {e}")

    # ------------------------------------------------------------------
    # Data extraction
    # ------------------------------------------------------------------

    @staticmethod
    def _is_sealed_case(soup: BeautifulSoup) -> bool:
        """Return True if the page shows a sealed case (Segredo de Justiça)."""
        element = soup.find('span', id=ESAJ_SEALED_ELEMENT_ID)
        return element is not None and ESAJ_SEALED_TEXT.lower() in element.text.lower()

    def _extract_field(self, soup: BeautifulSoup, config: dict) -> Optional[str]:
        element = soup.find(config['type'], id=config.get('id'), class_=config.get('class_'))
        if not element:
            return None

        value = element.text.strip()

        if config.get('id') == 'valorAcaoProcesso':
            return format_currency(value)

        if 'slice' in config:
            value = value[config['slice']]

        return value

    @staticmethod
    def _extract_parties(soup: BeautifulSoup):
        parties = soup.find_all('td', class_='nomeParteEAdvogado')
        if len(parties) < 2:
            return None, None
        return (
            parties[0].text.strip().partition("\n")[0],
            parties[1].text.strip().partition("\n")[0],
        )

    def _extract_process_data(self, soup: BeautifulSoup, process_number: str) -> ProcessData:
        try:
            data = {
                field: self._extract_field(soup, config)
                for field, config in self.FIELD_MAPPINGS.items()
            }
            plaintiff, defendant = self._extract_parties(soup)
            return ProcessData(
                number=process_number,
                initial_date=data['initial_date'],
                class_type=data['class_type'],
                subject=data['subject'],
                value=data['value'],
                last_movement=data['last_movement'],
                status=data['status'],
                plaintiff=plaintiff,
                defendant=defendant,
                other_processes=None,
                error=None,
            )
        except Exception as e:
            if self._is_sealed_case(soup):
                return ProcessData(number=process_number, error="Segredo de justiça")
            return ProcessData(number=process_number, error=f"Scraping error: {e}")

    # ------------------------------------------------------------------
    # Single-process scraping
    # ------------------------------------------------------------------

    def get_process_data(self, process_number: str) -> ProcessData:
        """Scrape data for a single process number."""
        soup = None
        try:
            self._validate_process_number(process_number)
            driver = self._setup_webdriver()

            driver.get(ESAJ_URL)
            time.sleep(1)

            self._fill_process_form(driver, process_number)
            time.sleep(2)

            try:
                mais = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.LINK_TEXT, "Mais"))
                )
                mais.click()
                time.sleep(2)
            except TimeoutException:
                pass

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            process_data = self._extract_process_data(soup, process_number)

            if process_data.defendant:
                process_data.other_processes = self._get_other_processes_count(
                    driver, process_data.defendant
                )

            return process_data

        except Exception as e:
            if soup is not None and self._is_sealed_case(soup):
                return ProcessData(number=process_number, error="Segredo de justiça")
            return ProcessData(number=process_number, error=str(e))

    def _get_other_processes_count(
        self, driver: webdriver.Chrome, defendant_name: str
    ) -> Optional[int]:
        try:
            driver.get(ESAJ_URL)
            time.sleep(1)

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

    def _scrape_process_worker(self, process_number: str) -> None:
        """Thread worker: scrape one process and put result in queue."""
        try:
            result = self.get_process_data(process_number)
            self.results_queue.put((process_number, result))
        except Exception as e:
            error_result = ProcessData(number=process_number, error=f"Worker error: {e}")
            self.results_queue.put((process_number, error_result))
        finally:
            self._cleanup_thread_driver()

    def _process_batch_parallel(self, batch: List[str]) -> List[ProcessData]:
        """Process a batch of process numbers in parallel with concurrency limit."""
        while not self.results_queue.empty():
            self.results_queue.get()

        threads = [
            threading.Thread(target=self._scrape_process_worker, args=(pn,))
            for pn in batch
        ]

        active_threads = []
        results = []

        for thread in threads:
            while len(active_threads) >= self.max_concurrent_browsers:
                for t in active_threads[:]:
                    if not t.is_alive():
                        active_threads.remove(t)
                        while not self.results_queue.empty():
                            process_number, result = self.results_queue.get()
                            results.append((process_number, result))
                if len(active_threads) >= self.max_concurrent_browsers:
                    time.sleep(0.5)

            thread.start()
            active_threads.append(thread)

        for thread in threads:
            thread.join()

        while not self.results_queue.empty():
            process_number, result = self.results_queue.get()
            results.append((process_number, result))

        pn_to_result = {pn: r for pn, r in results}
        return [
            pn_to_result.get(pn, ProcessData(number=pn, error="No result returned"))
            for pn in batch
        ]

    def process_batch(self, process_numbers: List[str], batch_size: int = 50) -> List[ProcessData]:
        """Process multiple process numbers and return results."""
        results = []
        total = len(process_numbers)

        logger.info(f"Processing {total} process numbers in batches of {batch_size}")
        logger.info(f"Using up to {self.max_concurrent_browsers} concurrent browser instances")

        for i in range(0, total, batch_size):
            batch = process_numbers[i: i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total + batch_size - 1) // batch_size
            logger.info(f"Processing batch {batch_num}/{total_batches} ({i + 1}-{min(i + batch_size, total)} of {total})")

            batch_results = self._process_batch_parallel(batch)
            results.extend(batch_results)

            logger.info(f"Completed batch {batch_num} ({len(batch_results)} processes)")

            if batch_size > 100:
                self._save_intermediate_results(results, i + len(batch), is_batch=True)

        return results

    def _save_intermediate_results(
        self, results: List[ProcessData], processed_count: int, is_batch: bool = False
    ) -> None:
        """Save intermediate results to avoid data loss on large batches."""
        if not results:
            return

        df = pd.DataFrame([r.to_dict() for r in results])
        timestamp = datetime.now().strftime('%Y_%m_%d_%H%M%S')
        prefix = "eSAJ_batch" if is_batch else "eSAJ_intermediate"
        filepath = ESAJ_OUTPUT_DIR / f"{prefix}_{timestamp}_{processed_count}.csv"
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"Intermediate results saved to: {filepath}")
