from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup
from dataclasses import dataclass, fields, asdict
from typing import Optional, List, Dict, Set, Tuple
from datetime import datetime
import pandas as pd
from pathlib import Path
import re
import time
import csv
import os
import threading
import queue
import logging
import json
import psutil
import sys


@dataclass
class ProcessData:
    """Data class to store process information"""
    number: str
    initial_date: Optional[str] = None
    class_type: Optional[str] = None
    subject: Optional[str] = None
    value: Optional[str] = None
    last_movement: Optional[str] = None
    status: Optional[str] = None
    plaintiff: Optional[str] = None
    defendant: Optional[str] = None
    other_processes: Optional[int] = None
    error: Optional[str] = None
    error_type: Optional[str] = None

    @classmethod
    def get_headers(cls) -> List[str]:
        """Get formatted headers for display and CSV"""
        return [field.name.replace('_', ' ').title() for field in fields(cls)]

    def to_dict(self) -> Dict:
        """Convert to dictionary for DataFrame creation"""
        return asdict(self)


class ProcessValueScraper:
    URL = "https://esaj.tjsp.jus.br/cpopg/open.do"
    PROCESS_NUMBER_PATTERN = r'^\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}$'
    OUTPUT_DIR = Path("C:/Poursuite/eSAJ")

    FIELD_MAPPINGS = {
        'initial_date': {'type': 'div', 'id': 'dataHoraDistribuicaoProcesso', 'slice': slice(0, 10)},
        'class_type': {'type': 'span', 'id': 'classeProcesso'},
        'subject': {'type': 'span', 'id': 'assuntoProcesso'},
        'value': {'type': 'div', 'id': 'valorAcaoProcesso'},
        'last_movement': {'type': 'td', 'class_': 'dataMovimentacao'},
        'status': {'type': 'span', 'id': 'labelSituacaoProcesso', 'class_': 'unj-tag'},
    }

    def __init__(self, max_concurrent_browsers=4):
        """Initialize with support for multiple browser instances"""
        self.max_concurrent_browsers = max_concurrent_browsers
        self.options = self._configure_chrome_options()
        self._ensure_output_directory()
        self.drivers = {}
        self.driver_lock = threading.Lock()
        self.results_queue = queue.Queue()

        # Setup logging
        self._setup_logging()
        self.logger = logging.getLogger(__name__)

        # Progress tracking
        self.progress_file = self.OUTPUT_DIR / "scraping_progress.json"
        self.processed_count = 0
        self.total_count = 0

        # Performance monitoring
        self.start_time = None
        self.batch_stats = []

    def __del__(self):
        """Cleanup method to ensure all browser instances are closed"""
        self._cleanup_all_drivers()

    def _setup_logging(self):
        """Setup comprehensive logging"""
        log_file = self.OUTPUT_DIR / f"scraper_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )

    def _setup_webdriver(self) -> webdriver.Chrome:
        """Thread-safe webdriver setup with proper error handling"""
        thread_id = threading.get_ident()

        with self.driver_lock:
            if thread_id not in self.drivers:
                try:
                    driver = webdriver.Chrome(options=self.options)
                    driver.set_page_load_timeout(30)
                    driver.implicitly_wait(10)
                    self.drivers[thread_id] = driver
                    self.logger.info(f"Created driver for thread {thread_id}")
                except Exception as e:
                    self.logger.error(f"Failed to create driver for thread {thread_id}: {e}")
                    raise

            return self.drivers[thread_id]

    def _cleanup_thread_driver(self):
        """Improved cleanup with better error handling"""
        thread_id = threading.get_ident()

        with self.driver_lock:
            if thread_id in self.drivers:
                try:
                    driver = self.drivers[thread_id]
                    driver.delete_all_cookies()
                    try:
                        driver.execute_script("window.localStorage.clear();")
                        driver.execute_script("window.sessionStorage.clear();")
                    except:
                        pass
                    driver.quit()
                    self.logger.info(f"Cleaned up driver for thread {thread_id}")
                except Exception as e:
                    self.logger.warning(f"Error during cleanup for thread {thread_id}: {e}")
                finally:
                    del self.drivers[thread_id]

    def _cleanup_all_drivers(self):
        """Clean up all driver instances"""
        with self.driver_lock:
            for thread_id, driver in list(self.drivers.items()):
                try:
                    driver.quit()
                except Exception:
                    pass
            self.drivers.clear()

    @staticmethod
    def _configure_chrome_options():
        """Enhanced Chrome options for better stability"""
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-plugins')
        options.add_argument('--disable-images')
        options.add_argument('--disable-web-security')
        options.add_argument('--memory-pressure-off')
        options.add_argument('--max_old_space_size=4096')
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-backgrounding-occluded-windows')
        options.add_argument('--disable-renderer-backgrounding')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-features=VizDisplayCompositor')
        options.add_argument(
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        return options

    def _ensure_output_directory(self):
        """Ensure output directory exists"""
        self.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    def _validate_process_number(self, process_number: str) -> bool:
        """Validate the process number format"""
        if not re.match(self.PROCESS_NUMBER_PATTERN, process_number):
            raise ValueError(f"Invalid process number format: {process_number}. Please use: NNNNNNN-DD.AAAA.J.TR.OOOO")
        return True

    @staticmethod
    def _fill_process_form(driver: webdriver.Chrome, process_number: str):
        """Fill and submit the process search form with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                input_field = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.ID, "numeroDigitoAnoUnificado"))
                )
                input_field.clear()
                input_field.send_keys(process_number[:15])

                input_field = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "foroNumeroUnificado"))
                )
                input_field.clear()
                input_field.send_keys(process_number[-4:])

                search_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "botaoConsultarProcessos"))
                )
                search_button.click()
                return

            except Exception as e:
                if attempt == max_retries - 1:
                    raise Exception(f"Error filling form after {max_retries} attempts: {str(e)}")
                time.sleep(2 ** attempt)

    @staticmethod
    def _format_currency_value(value: str) -> Optional[str]:
        """Format currency value to maintain one space after R$"""
        if not value:
            return None

        value = re.sub(r'\s+', '', value)

        if value.startswith('R$'):
            value = 'R$ ' + value[2:]

        return value

    def _extract_field(self, soup: BeautifulSoup, config: dict) -> Optional[str]:
        """Extract a field from the page using the provided configuration"""
        element = soup.find(config['type'], id=config.get('id'), class_=config.get('class_'))
        if not element:
            return None

        value = element.text.strip()

        if config.get('id') == 'valorAcaoProcesso':
            return self._format_currency_value(value)

        if 'slice' in config:
            value = value[config['slice']]

        return value

    @staticmethod
    def _extract_parties(soup: BeautifulSoup) -> tuple[Optional[str], Optional[str]]:
        """Extract plaintiff and defendant party information"""
        parties = soup.find_all('td', class_='nomeParteEAdvogado')
        if len(parties) < 2:
            return None, None

        return (
            parties[0].text.strip().partition("\n")[0],
            parties[1].text.strip().partition("\n")[0]
        )

    def _extract_process_data(self, soup: BeautifulSoup, process_number: str) -> ProcessData:
        """Extract all process data from the page"""
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
                error_type=None
            )
        except Exception as e:
            return ProcessData(
                number=process_number,
                error=f"Data extraction error: {str(e)[:100]}",
                error_type="extraction_error"
            )

    def _scrape_process_worker(self, process_number: str):
        """Improved worker with better error categorization"""
        thread_id = threading.get_ident()

        try:
            self.logger.info(f"Thread {thread_id} processing {process_number}")
            result = self.get_process_data(process_number)
            self.results_queue.put((process_number, result, None))

        except ValueError as e:
            error_result = ProcessData(
                number=process_number,
                error=f"Format error: {str(e)}",
                error_type="format_error"
            )
            self.results_queue.put((process_number, error_result, "format_error"))

        except TimeoutException as e:
            error_result = ProcessData(
                number=process_number,
                error="Timeout - page load too slow",
                error_type="timeout"
            )
            self.results_queue.put((process_number, error_result, "timeout"))

        except WebDriverException as e:
            error_msg = str(e).lower()
            if "click intercepted" in error_msg:
                error_result = ProcessData(
                    number=process_number,
                    error="Elemento interceptado (problema de UI)",
                    error_type="ui_interception"
                )
                self.results_queue.put((process_number, error_result, "ui_interception"))
            else:
                error_result = ProcessData(
                    number=process_number,
                    error=f"Browser error: {str(e)[:100]}",
                    error_type="browser_error"
                )
                self.results_queue.put((process_number, error_result, "browser_error"))

        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in
                   ['segredo', 'justiÃ§a', 'sigiloso', 'confidencial', 'nÃ£o localizado']):
                error_result = ProcessData(
                    number=process_number,
                    error="Segredo de justiÃ§a",
                    error_type="confidential"
                )
                self.results_queue.put((process_number, error_result, "confidential"))
            else:
                error_result = ProcessData(
                    number=process_number,
                    error=f"Unknown error: {str(e)[:100]}",
                    error_type="unknown"
                )
                self.results_queue.put((process_number, error_result, "unknown"))
                self.logger.error(f"Unexpected error for {process_number}: {e}")

        finally:
            self._cleanup_thread_driver()

    def get_process_data(self, process_number: str) -> ProcessData:
        """Enhanced data extraction with better error handling"""
        try:
            self._validate_process_number(process_number)
            driver = self._setup_webdriver()

            max_retries = 2
            for attempt in range(max_retries + 1):
                try:
                    driver.get(self.URL)
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.ID, "numeroDigitoAnoUnificado"))
                    )
                    break
                except TimeoutException:
                    if attempt == max_retries:
                        raise
                    self.logger.warning(f"Attempt {attempt + 1} failed for {process_number}, retrying...")
                    time.sleep(2)

            self._fill_process_form(driver, process_number)

            WebDriverWait(driver, 15).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )

            page_source = driver.page_source.lower()

            if any(keyword in page_source for keyword in
                   ['segredo de justiÃ§a', 'acesso negado', 'processo sigiloso', 'nÃ£o localizado',
                    'nÃ£o foi possÃ­vel localizar', 'processo inexistente', 'acesso restrito']):
                return ProcessData(
                    number=process_number,
                    error="Segredo de justiÃ§a",
                    error_type="confidential"
                )

            if any(keyword in page_source for keyword in
                   ['processo nÃ£o encontrado', 'nÃ£o encontrado', 'nÃ£o existe', 'inexistente']):
                return ProcessData(
                    number=process_number,
                    error="Processo nÃ£o encontrado",
                    error_type="not_found"
                )

            if any(keyword in page_source for keyword in
                   ['erro interno', 'sistema indisponÃ­vel', 'erro no servidor', 'erro inesperado']):
                return ProcessData(
                    number=process_number,
                    error="Erro do sistema",
                    error_type="system_error"
                )

            # Try to click "Mais" button with multiple strategies
            try:
                mais_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.LINK_TEXT, "Mais"))
                )

                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", mais_button)
                time.sleep(1)

                try:
                    mais_button.click()
                except Exception as e:
                    if "click intercepted" in str(e).lower():
                        self.logger.info(f"Normal click intercepted for {process_number}, trying JavaScript click")
                        driver.execute_script("arguments[0].click();", mais_button)
                    else:
                        raise e

                time.sleep(2)

            except TimeoutException:
                try:
                    mais_button = driver.find_element(By.CSS_SELECTOR, "a[href='#maisDetalhes']")
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", mais_button)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", mais_button)
                    time.sleep(2)
                except:
                    try:
                        mais_button = driver.find_element(By.CLASS_NAME, "unj-link-collapse")
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", mais_button)
                        time.sleep(1)
                        driver.execute_script("arguments[0].click();", mais_button)
                        time.sleep(2)
                    except:
                        self.logger.info(f"Could not click 'Mais' button for {process_number}, continuing without it")
                        pass

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            process_data = self._extract_process_data(soup, process_number)

            if (not process_data.class_type and not process_data.subject and
                    not process_data.plaintiff and not process_data.defendant):
                return ProcessData(
                    number=process_number,
                    error="Nenhum dado encontrado na pÃ¡gina",
                    error_type="no_data"
                )

            if process_data.defendant and not process_data.error:
                try:
                    other_processes = self._get_other_processes_count(driver, process_data.defendant)
                    process_data.other_processes = other_processes
                except Exception as e:
                    self.logger.warning(f"Failed to get other processes count for {process_number}: {e}")

            return process_data

        except Exception as e:
            error_msg = str(e).lower()

            if any(keyword in error_msg for keyword in ['timeout', 'timed out']):
                raise TimeoutException(f"Timeout error for {process_number}: {str(e)}")
            elif any(keyword in error_msg for keyword in ['connection', 'network', 'dns']):
                raise WebDriverException(f"Network error for {process_number}: {str(e)}")
            elif any(keyword in error_msg for keyword in ['segredo', 'justiÃ§a', 'sigiloso', 'confidencial']):
                return ProcessData(
                    number=process_number,
                    error="Segredo de justiÃ§a",
                    error_type="confidential"
                )
            else:
                raise Exception(f"Failed to extract data for {process_number}: {str(e)}")

    def _get_other_processes_count(self, driver: webdriver.Chrome, defendant_name: str) -> Optional[int]:
        """Get the count of other processes for the defendant party with better error handling"""
        try:
            driver.get(self.URL)
            time.sleep(1)

            select_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "cbPesquisa"))
            )
            select_element.send_keys("NMPARTE")

            checkbox = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "pesquisarPorNomeCompleto"))
            )
            driver.execute_script("arguments[0].click();", checkbox)

            name_field = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "campo_NMPARTE"))
            )
            name_field.clear()
            name_field.send_keys(defendant_name)

            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "botaoConsultarProcessos"))
            )
            search_button.click()

            try:
                count_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "contadorDeProcessos"))
                )

                count_text = count_element.text.strip().split()[0]
                return int(count_text)

            except TimeoutException:
                return 0
            except (IndexError, ValueError):
                return 0

        except Exception as e:
            self.logger.warning(f"Error getting process count for {defendant_name}: {str(e)}")
            return 0
        finally:
            try:
                driver.delete_all_cookies()
            except:
                pass

    def _collect_available_results(self, results_dict: dict, error_stats: dict):
        """Helper method to collect results from queue"""
        while not self.results_queue.empty():
            try:
                process_number, result, error_type = self.results_queue.get_nowait()
                results_dict[process_number] = result

                if error_type:
                    error_stats[error_type] += 1
                else:
                    error_stats["success"] += 1

            except queue.Empty:
                break

    def _process_batch_parallel(self, batch: List[str]) -> List[ProcessData]:
        """Improved batch processing with better synchronization"""
        batch_start_time = time.time()

        while not self.results_queue.empty():
            try:
                self.results_queue.get_nowait()
            except queue.Empty:
                break

        results_dict = {}
        error_stats = {"format_error": 0, "timeout": 0, "browser_error": 0,
                       "confidential": 0, "unknown": 0, "success": 0, "extraction_error": 0,
                       "not_found": 0, "system_error": 0, "no_data": 0, "ui_interception": 0}

        initial_memory = psutil.virtual_memory().percent
        self.logger.info(f"Starting batch with {len(batch)} processes. Initial memory usage: {initial_memory:.1f}%")

        threads = []
        for process_number in batch:
            thread = threading.Thread(
                target=self._scrape_process_worker,
                args=(process_number,),
                name=f"Worker-{process_number}"
            )
            threads.append((process_number, thread))

        active_threads = []

        for process_number, thread in threads:
            while len(active_threads) >= self.max_concurrent_browsers:
                for active_pn, active_thread in active_threads[:]:
                    if not active_thread.is_alive():
                        active_threads.remove((active_pn, active_thread))
                        self._collect_available_results(results_dict, error_stats)

                if len(active_threads) >= self.max_concurrent_browsers:
                    time.sleep(0.1)

            thread.start()
            active_threads.append((process_number, thread))

        for process_number, thread in threads:
            thread.join(timeout=120)
            if thread.is_alive():
                self.logger.warning(f"Thread for {process_number} timed out")

        self._collect_available_results(results_dict, error_stats)

        batch_time = time.time() - batch_start_time
        final_memory = psutil.virtual_memory().percent
        memory_increase = final_memory - initial_memory

        batch_stat = {
            'batch_size': len(batch),
            'processing_time': batch_time,
            'memory_increase': memory_increase,
            'error_stats': error_stats.copy()
        }
        self.batch_stats.append(batch_stat)

        self.logger.info(
            f"Batch completed in {batch_time:.1f}s. Memory increase: {memory_increase:.1f}%. Stats: {error_stats}")

        ordered_results = []
        for process_number in batch:
            if process_number in results_dict:
                ordered_results.append(results_dict[process_number])
            else:
                missing_result = ProcessData(
                    number=process_number,
                    error="Thread failed to complete",
                    error_type="thread_failure"
                )
                ordered_results.append(missing_result)
                self.logger.warning(f"No result returned for {process_number}")

        return ordered_results

    def _save_progress(self, processed_numbers: List[str], results: List[ProcessData]):
        """Save progress to resume later if interrupted"""
        progress_data = {
            'timestamp': datetime.now().isoformat(),
            'processed_count': len(processed_numbers),
            'total_count': self.total_count,
            'processed_numbers': processed_numbers,
            'last_batch_results': [result.to_dict() for result in results[-50:]]
        }

        try:
            with open(self.progress_file, 'w') as f:
                json.dump(progress_data, f, indent=2)
        except Exception as e:
            self.logger.warning(f"Failed to save progress: {e}")

    def _load_progress(self) -> Tuple[List[str], int]:
        """Load previous progress if available"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    progress_data = json.load(f)

                processed_numbers = progress_data.get('processed_numbers', [])
                self.logger.info(f"Found previous progress: {len(processed_numbers)} processes already completed")
                return processed_numbers, progress_data.get('total_count', 0)
            except Exception as e:
                self.logger.warning(f"Failed to load progress: {e}")

        return [], 0

    def process_batch(self, process_numbers: List[str], batch_size: int = 50, resume: bool = True) -> List[ProcessData]:
        """Process multiple process numbers with resume capability"""
        self.start_time = time.time()
        self.total_count = len(process_numbers)

        processed_numbers, prev_total = [], 0
        if resume:
            processed_numbers, prev_total = self._load_progress()

        if processed_numbers:
            remaining_numbers = [pn for pn in process_numbers if pn not in processed_numbers]
            self.logger.info(f"Resuming: {len(remaining_numbers)} processes remaining out of {len(process_numbers)}")
            process_numbers = remaining_numbers

        if not process_numbers:
            self.logger.info("All processes already completed!")
            return []

        results = []
        total = len(process_numbers)
        all_processed_numbers = processed_numbers.copy()

        print(f"\nProcessing {total} process numbers in batches of {batch_size}...")
        print(f"Using up to {self.max_concurrent_browsers} concurrent browser instances")

        for i in range(0, total, batch_size):
            batch = process_numbers[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total + batch_size - 1) // batch_size

            print(f"\nProcessing batch {batch_num}/{total_batches} ({i + 1}-{min(i + batch_size, total)} of {total}):")

            try:
                batch_results = self._process_batch_parallel(batch)
                results.extend(batch_results)

                all_processed_numbers.extend([result.number for result in batch_results])

                self._save_progress(all_processed_numbers, results)

                print(f"  Completed batch {batch_num} ({len(batch_results)} processes)")

                if batch_size >= 50 or batch_num % 5 == 0:
                    self._save_intermediate_results(results, len(all_processed_numbers), is_batch=True)

                if batch_num % 3 == 0:
                    self._cleanup_all_drivers()
                    self.logger.info("Performed memory cleanup between batches")

            except Exception as e:
                self.logger.error(f"Batch {batch_num} failed: {e}")
                self._save_progress(all_processed_numbers, results)
                raise

        self._cleanup_all_drivers()
        self._save_final_statistics()

        return results

    def _save_final_statistics(self):
        """Save final processing statistics"""
        if not self.batch_stats:
            return

        total_time = time.time() - self.start_time if self.start_time else 0

        stats = {
            'total_processing_time': total_time,
            'total_processes': self.total_count,
            'avg_time_per_process': total_time / max(self.total_count, 1),
            'batch_statistics': self.batch_stats,
            'concurrent_browsers': self.max_concurrent_browsers
        }

        stats_file = self.OUTPUT_DIR / f"processing_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            with open(stats_file, 'w') as f:
                json.dump(stats, f, indent=2)
            self.logger.info(f"Processing statistics saved to {stats_file}")
        except Exception as e:
            self.logger.warning(f"Failed to save statistics: {e}")

    def _save_intermediate_results(self, results: List[ProcessData], processed_count: int, is_batch: bool = False):
        """Save intermediate results to avoid data loss"""
        if not results:
            return

        df = pd.DataFrame([result.to_dict() for result in results])

        timestamp = datetime.now().strftime('%Y_%m_%d_%H%M%S')
        filename_prefix = "eSAJ_batch" if is_batch else "eSAJ_intermediate"
        filename = f"{filename_prefix}_{timestamp}_{processed_count}.csv"
        filepath = self.OUTPUT_DIR / filename

        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"    Intermediate results saved to: {filepath}")

    def display_results(self, results: List[ProcessData]):
        """Display results summary and limited table with enhanced statistics"""
        if not results:
            print("No results to display.")
            return

        total = len(results)
        successful = len([r for r in results if not r.error])
        errors = len([r for r in results if r.error])

        error_types = {}
        for result in results:
            if result.error_type:
                error_types[result.error_type] = error_types.get(result.error_type, 0) + 1

        print(f"\n{'=' * 60}")
        print(f"PROCESSING RESULTS SUMMARY")
        print(f"{'=' * 60}")
        print(f"Total processes processed: {total}")
        print(f"Successful extractions: {successful} ({successful / total * 100:.1f}%)")
        print(f"Errors: {errors} ({errors / total * 100:.1f}%)")

        if error_types:
            print(f"\nError breakdown:")
            for error_type, count in sorted(error_types.items()):
                print(f"  {error_type.replace('_', ' ').title()}: {count}")

        if self.batch_stats:
            total_time = sum(stat['processing_time'] for stat in self.batch_stats)
            avg_time_per_process = total_time / max(total, 1)
            print(f"\nPerformance statistics:")
            print(f"  Total processing time: {total_time:.1f} seconds ({total_time / 60:.1f} minutes)")
            print(f"  Average time per process: {avg_time_per_process:.2f} seconds")
            print(f"  Processes per minute: {60 / avg_time_per_process:.1f}")

        display_count = min(5, len(results))
        headers = ProcessData.get_headers()
        table_data = []

        for result in results[:display_count]:
            row = []
            for field in fields(ProcessData):
                value = getattr(result, field.name)
                if isinstance(value, str) and len(value) > 30:
                    value = value[:27] + "..."
                row.append(value)
            table_data.append(row)

        print(f"\nShowing first {display_count} results:")
        print(tabulate(table_data, headers=headers, tablefmt="grid"))

        if len(results) > display_count:
            print(f"\n... and {len(results) - display_count} more results not shown.")

    def save_results(self, results: List[ProcessData], force_save: bool = False):
        """Save results to CSV with enhanced options"""
        if not results:
            print("No results to save.")
            return

        save_to_csv = force_save
        if not force_save:
            response = input("\nSave results to CSV? (y/n): ").strip().lower()
            save_to_csv = response.startswith('y')

        if save_to_csv:
            df = pd.DataFrame([result.to_dict() for result in results])

            timestamp = datetime.now().strftime('%Y_%m_%d_%H%M%S')
            filename = f"eSAJ_final_{timestamp}.csv"
            filepath = self.OUTPUT_DIR / filename

            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"\nResults saved to: {filepath}")

            self._save_summary_report(results, filepath.parent / f"summary_{timestamp}.txt")
        else:
            print("\nResults not saved to CSV.")

    def _save_summary_report(self, results: List[ProcessData], filepath: Path):
        """Save a summary report of the processing"""
        try:
            total = len(results)
            successful = len([r for r in results if not r.error])
            errors = len([r for r in results if r.error])

            error_types = {}
            for result in results:
                if result.error_type:
                    error_types[result.error_type] = error_types.get(result.error_type, 0) + 1

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write("eSAJ PROCESSING SUMMARY REPORT\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total processes: {total}\n")
                f.write(f"Successful: {successful} ({successful / total * 100:.1f}%)\n")
                f.write(f"Errors: {errors} ({errors / total * 100:.1f}%)\n\n")

                if error_types:
                    f.write("ERROR BREAKDOWN:\n")
                    f.write("-" * 20 + "\n")
                    for error_type, count in sorted(error_types.items()):
                        f.write(f"{error_type.replace('_', ' ').title()}: {count}\n")
                    f.write("\n")

                if self.batch_stats:
                    total_time = sum(stat['processing_time'] for stat in self.batch_stats)
                    avg_time = total_time / max(total, 1)
                    f.write("PERFORMANCE STATISTICS:\n")
                    f.write("-" * 25 + "\n")
                    f.write(f"Total processing time: {total_time:.1f} seconds\n")
                    f.write(f"Average time per process: {avg_time:.2f} seconds\n")
                    f.write(f"Processes per minute: {60 / avg_time:.1f}\n")
                    f.write(f"Concurrent browsers used: {self.max_concurrent_browsers}\n")

            print(f"Summary report saved to: {filepath}")
        except Exception as e:
            self.logger.warning(f"Failed to save summary report: {e}")


class CSVProcessExtractor:
    """Extracts process numbers from CSV files generated by NewSearchEngine.py"""

    def __init__(self):
        self.process_number_pattern = r'\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}'

    def extract_from_csv(self, csv_path: str) -> Set[str]:
        """Extract unique process numbers from a CSV file with improved error handling"""
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        print(f"Extracting process numbers from: {csv_path}")
        process_numbers = set()

        try:
            csv.field_size_limit(2000000000)
            with open(csv_path, 'r', encoding='utf-8-sig', errors='replace') as file:
                header_line_idx = 0
                for idx, line in enumerate(file):
                    if 'Process Number' in line:
                        header_line_idx = idx
                        break

            with open(csv_path, 'r', encoding='utf-8-sig', errors='replace') as file:
                for _ in range(header_line_idx):
                    next(file)

                reader = csv.reader(file)
                header = next(reader)

                process_col_idx = None
                for idx, col_name in enumerate(header):
                    if 'Process Number' in col_name:
                        process_col_idx = idx
                        break

                if process_col_idx is None:
                    raise ValueError("Could not find 'Process Number' column in the CSV file")

                row_count = 0
                for row in reader:
                    row_count += 1
                    if len(row) > process_col_idx:
                        cell_value = row[process_col_idx]
                        matches = re.findall(self.process_number_pattern, cell_value)
                        if matches:
                            for match in matches:
                                process_numbers.add(match)

                print(f"Processed {row_count} rows, found {len(process_numbers)} unique process numbers")

        except Exception as e:
            print(f"Error extracting process numbers: {str(e)}")
            self._extract_with_fallback(csv_path, process_numbers)

        return process_numbers

    def _extract_with_fallback(self, csv_path: str, process_numbers: Set[str]):
        """Fallback method to extract process numbers by searching the entire file content"""
        print("Using fallback extraction method...")
        try:
            with open(csv_path, 'r', encoding='utf-8-sig', errors='replace') as file:
                content = file.read()
                matches = re.findall(self.process_number_pattern, content)
                for match in matches:
                    process_numbers.add(match)

            print(f"Fallback method found {len(process_numbers)} unique process numbers")
        except Exception as e:
            print(f"Fallback extraction also failed: {str(e)}")


def show_help():
    """Display help information"""
    help_text = """
    ENHANCED eSAJ PROCESS DATA EXTRACTION TOOL
    ==========================================

    This tool extracts judicial process data from the eSAJ system with the following features:

    FEATURES:
    - Multi-threaded processing for faster extraction
    - Automatic progress saving and resume capability
    - Enhanced error handling and categorization
    - Memory management and resource cleanup
    - Detailed logging and statistics
    - Batch processing for large datasets

    REQUIREMENTS:
    - Chrome browser installed
    - Python packages: selenium, beautifulsoup4, pandas, psutil, tabulate
    - ChromeDriver (automatically managed by selenium)

    USAGE TIPS:
    - Start with smaller batch sizes (20-50) for testing
    - Use 2-4 concurrent browsers for optimal performance
    - Monitor system resources during large extractions
    - Results are automatically saved with timestamps
    - Check log files for detailed error information

    OUTPUT FILES:
    - Final results: eSAJ_final_TIMESTAMP.csv
    - Intermediate backups: eSAJ_batch_TIMESTAMP.csv
    - Processing logs: scraper_log_TIMESTAMP.log
    - Statistics: processing_stats_TIMESTAMP.json
    - Progress file: scraping_progress.json (for resume)

    PROCESS NUMBER FORMAT:
    NNNNNNN-DD.AAAA.J.TR.OOOO
    Example: 1234567-89.2023.8.26.0001
    """
    print(help_text)


def main():
    extractor = None
    scraper = None

    try:
        print("\n" + "=" * 60)
        print("ENHANCED PROCESS DATA EXTRACTION TOOL")
        print("=" * 60)
        print("This tool extracts process data from the eSAJ system")
        print("Features: Multi-threading, Progress resumption, Enhanced error handling")

        while True:
            print(f"\n{'-' * 40}")
            print("OPTIONS:")
            print("1. Extract data from CSV file")
            print("2. Extract data from manual process number entry")
            # print("3. Resume previous extraction")
            print("3. View system status")
            print("4. Exit")
            print(f"{'-' * 40}")

            choice = input("\nSelect an option (1-4): ").strip()

            if choice == "4":
                print("Exiting program...")
                break

            elif choice == "3":
                memory = psutil.virtual_memory()
                cpu = psutil.cpu_percent(interval=1)
                print(f"\nSYSTEM STATUS:")
                print(
                    f"Memory usage: {memory.percent:.1f}% ({memory.used // (1024 ** 3):.1f}GB / {memory.total // (1024 ** 3):.1f}GB)")
                print(f"CPU usage: {cpu:.1f}%")
                print(f"Available memory: {memory.available // (1024 ** 3):.1f}GB")

                progress_file = Path("C:/Poursuite/eSAJ/scraping_progress.json")
                if progress_file.exists():
                    try:
                        with open(progress_file, 'r') as f:
                            progress = json.load(f)
                        print(f"\nPREVIOUS SESSION FOUND:")
                        print(f"Last run: {progress.get('timestamp', 'Unknown')}")
                        print(
                            f"Processed: {progress.get('processed_count', 0)} / {progress.get('total_count', 0)} processes")
                    except:
                        print("\nNo valid previous session found")
                else:
                    print("\nNo previous session found")
                continue

            # elif choice == "3":
            #     progress_file = Path("C:/Poursuite/eSAJ/scraping_progress.json")
            #     if not progress_file.exists():
            #         print("No previous session found to resume.")
            #         continue
            #
            #     try:
            #         with open(progress_file, 'r') as f:
            #             progress = json.load(f)
            #
            #         processed_count = progress.get('processed_count', 0)
            #         total_count = progress.get('total_count', 0)
            #
            #         if processed_count >= total_count:
            #             print("Previous session appears to be complete.")
            #             continue
            #
            #         print(f"\nFound previous session:")
            #         print(f"Progress: {processed_count} / {total_count} processes")
            #         print(f"Remaining: {total_count - processed_count} processes")
            #
            #         confirm = input("Resume this session? (y/n): ").strip().lower()
            #         if confirm != 'y':
            #             continue
            #
            #         print("Note: You'll need to provide the original CSV file or process list to resume.")
            #         print("The resume feature will skip already processed numbers.")
            #
            #     except Exception as e:
            #         print(f"Error reading progress file: {e}")
            #         continue

            elif choice == "1":
                csv_path = input("\nEnter the path to the CSV file: ").strip().strip('"')
                if not csv_path:
                    print("No file path provided.")
                    continue

                if not os.path.exists(csv_path):
                    print(f"File not found: {csv_path}")
                    continue

                print("Extracting process numbers from CSV...")
                extractor = CSVProcessExtractor()
                try:
                    process_numbers = list(extractor.extract_from_csv(csv_path))
                except Exception as e:
                    print(f"Error extracting from CSV: {e}")
                    continue

                if not process_numbers:
                    print("No process numbers found in the CSV file.")
                    continue

                print(f"\nFound {len(process_numbers)} process numbers.")
                max_display = min(10, len(process_numbers))
                print(f"Sample: {', '.join(process_numbers[:max_display])}" +
                      (f" (and {len(process_numbers) - max_display} more...)" if len(
                          process_numbers) > max_display else ""))

                confirm = input(
                    f"\nProceed with extracting data for these {len(process_numbers)} processes? (y/n): ").strip().lower()
                if confirm != 'y':
                    print("Operation cancelled.")
                    continue

                print(f"\n{'-' * 30}")
                print("CONFIGURATION OPTIONS:")
                print(f"{'-' * 30}")

                default_batch_size = min(50, len(process_numbers))
                batch_size_input = input(f"Batch size (default: {default_batch_size}): ").strip()
                try:
                    batch_size = int(batch_size_input) if batch_size_input else default_batch_size
                    batch_size = max(1, min(batch_size, 200))
                except ValueError:
                    batch_size = default_batch_size
                    print(f"Invalid batch size, using default: {default_batch_size}")

                default_browser_count = min(4, max(1, psutil.cpu_count() // 2))
                browser_count_input = input(
                    f"Concurrent browsers (default: {default_browser_count}): ").strip()
                try:
                    browser_count = int(browser_count_input) if browser_count_input else default_browser_count
                    browser_count = max(1, min(browser_count, 8))

                    if browser_count > 6:
                        print("Warning: Using many browsers may cause system instability.")
                        if input("Continue? (y/n): ").strip().lower() != 'y':
                            browser_count = default_browser_count
                except ValueError:
                    browser_count = default_browser_count
                    print(f"Invalid browser count, using default: {default_browser_count}")

                # resume = input("Enable resume capability? (y/n, default: y): ").strip().lower()
                resume = 'y'
                resume = resume != 'n'

                print(f"\n{'-' * 40}")
                print("STARTING EXTRACTION:")
                print(f"Total processes: {len(process_numbers)}")
                print(f"Batch size: {batch_size}")
                print(f"Concurrent browsers: {browser_count}")
                print(f"Resume enabled: {resume}")
                print(f"{'-' * 40}")

                try:
                    scraper = ProcessValueScraper(max_concurrent_browsers=browser_count)
                    results = scraper.process_batch(process_numbers, batch_size=batch_size, resume=resume)

                    if results:
                        scraper.display_results(results)
                        scraper.save_results(results)
                    else:
                        print("No new results to process (may have been resumed and completed).")

                except KeyboardInterrupt:
                    print("\n\nExtraction interrupted by user.")
                    print("Progress has been saved and can be resumed later.")
                    break
                except Exception as e:
                    print(f"\nExtraction failed: {e}")
                    print("Check the log files for detailed error information.")

            elif choice == "2":
                print(f"\n{'-' * 40}")
                print("MANUAL PROCESS NUMBER ENTRY:")
                print("Enter process numbers (one per line)")
                print("Empty line to finish")
                print("Format: NNNNNNN-DD.AAAA.J.TR.OOOO")
                print(f"{'-' * 40}")

                process_numbers = []
                line_count = 0

                while True:
                    line_count += 1
                    try:
                        number = input(f"Process {line_count}: ").strip()
                        if not number:
                            break

                        if re.match(r'^\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}$', number):
                            process_numbers.append(number)
                        else:
                            print(f"Invalid format for: {number}")
                        print("Expected format: NNNNNNN-DD.AAAA.J.TR.OOOO")
                        line_count -= 1

                    except KeyboardInterrupt:
                        print("\nEntry cancelled by user.")
                        break

                if not process_numbers:
                    print("No valid process numbers provided.")
                    continue

                print(f"\nEntered {len(process_numbers)} valid process numbers:")
                for i, pn in enumerate(process_numbers, 1):
                    print(f"  {i}. {pn}")

                confirm = input(f"\nProceed with extraction? (y/n): ").strip().lower()
                if confirm != 'y':
                    print("Operation cancelled.")
                    continue

                print(f"\n{'-' * 30}")
                print("CONFIGURATION:")
                print(f"{'-' * 30}")

                default_browser_count = min(4, max(1, psutil.cpu_count() // 2))
                browser_count_input = input(
                    f"Concurrent browsers (default: {default_browser_count}): ").strip()
                try:
                    browser_count = int(browser_count_input) if browser_count_input else default_browser_count
                    browser_count = max(1, min(browser_count, 8))

                    if browser_count > 6:
                        print("Warning: Using many browsers may cause system instability.")
                        if input("Continue? (y/n): ").strip().lower() != 'y':
                            browser_count = default_browser_count
                except ValueError:
                    browser_count = default_browser_count
                    print(f"Invalid browser count, using default: {default_browser_count}")

                print(f"\n{'-' * 40}")
                print("STARTING EXTRACTION:")
                print(f"Total processes: {len(process_numbers)}")
                print(f"Concurrent browsers: {browser_count}")
                print(f"{'-' * 40}")

                try:
                    scraper = ProcessValueScraper(max_concurrent_browsers=browser_count)
                    results = scraper.process_batch(process_numbers, batch_size=len(process_numbers))
                    scraper.display_results(results)
                    scraper.save_results(results)

                except KeyboardInterrupt:
                    print("\n\nExtraction interrupted by user.")
                    break
                except Exception as e:
                    print(f"\nExtraction failed: {e}")
                    print("Check the log files for detailed error information.")

            else:
                print("Invalid option. Please select 1-5.")

    except KeyboardInterrupt:
        print("\n\nProgram interrupted by user.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {str(e)}")
        if scraper:
            try:
                scraper.logger.error(f"Main function error: {e}")
            except:
                pass
    finally:
        if scraper:
            try:
                print("Cleaning up resources...")
                scraper._cleanup_all_drivers()
                del scraper
            except:
                pass

        print("Program terminated.")


if __name__ == "__main__":
    # Check if help is requested
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help', 'help']:
        show_help()
    else:
        # Check system requirements
        try:
            import selenium
            import bs4
            import pandas
            import psutil
            from tabulate import tabulate
        except ImportError as e:
            print(f"Missing required package: {e}")
            print("Please install required packages:")
            print("pip install selenium beautifulsoup4 pandas psutil tabulate")
            sys.exit(1)

        # Check available memory
        memory = psutil.virtual_memory()
        if memory.available < 2 * 1024 ** 3:  # Less than 2GB available
            print(f"Warning: Low available memory ({memory.available // (1024 ** 3):.1f}GB)")
            print("Consider closing other applications or reducing concurrent browsers.")
            if input("Continue anyway? (y/n): ").strip().lower() != 'y':
                sys.exit(1)

        main()