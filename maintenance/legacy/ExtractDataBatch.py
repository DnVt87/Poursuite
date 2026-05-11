from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
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
from tabulate import tabulate


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
        self.drivers = {}  # Dictionary to store browser instances by thread ID
        self.driver_lock = threading.Lock()  # Lock for thread-safe access to drivers dictionary
        self.results_queue = queue.Queue()  # Queue for storing results from threads

    def __del__(self):
        """Cleanup method to ensure all browser instances are closed"""
        self._cleanup_all_drivers()

    def _setup_webdriver(self) -> webdriver.Chrome:
        """Initialize and return a configured webdriver for the current thread"""
        thread_id = threading.get_ident()

        with self.driver_lock:
            if thread_id not in self.drivers:
                try:
                    driver = webdriver.Chrome(options=self.options)
                    self.drivers[thread_id] = driver
                except Exception as e:
                    print(f"Error creating webdriver: {str(e)}")
                    raise

        return self.drivers[thread_id]

    def _cleanup_thread_driver(self):
        """Clean up the driver for the current thread"""
        thread_id = threading.get_ident()

        with self.driver_lock:
            if thread_id in self.drivers:
                try:
                    self.drivers[thread_id].quit()
                except Exception:
                    pass
                finally:
                    del self.drivers[thread_id]

    def _cleanup_all_drivers(self):
        """Clean up all driver instances"""
        with self.driver_lock:
            for driver in self.drivers.values():
                try:
                    driver.quit()
                except Exception:
                    pass
            self.drivers.clear()

    @staticmethod
    def _configure_chrome_options():
        """Configure Chrome webdriver options"""
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
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
        """Fill and submit the process search form"""
        try:
            input_field = WebDriverWait(driver, 10).until(
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
        except Exception as e:
            raise Exception(f"Error filling form: {str(e)}")

    @staticmethod
    def _format_currency_value(value: str) -> Optional[str]:
        """Format currency value to maintain one space after R$"""
        if not value:
            return None

        # Remove all spaces first
        value = re.sub(r'\s+', '', value)

        # Add one space after R$ if it exists
        if value.startswith('R$'):
            value = 'R$ ' + value[2:]

        return value

    def _extract_field(self, soup: BeautifulSoup, config: dict) -> Optional[str]:
        """Extract a field from the page using the provided configuration"""
        element = soup.find(config['type'], id=config.get('id'), class_=config.get('class_'))
        if not element:
            return None

        value = element.text.strip()

        # Special handling for currency value
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
                other_processes=None,  # Will be filled later
                error=None
            )
        except Exception as e:
            return ProcessData(
                number=process_number,
                error = "Segredo de justiça"
                # error=str(e)
            )

    def _scrape_process_worker(self, process_number: str):
        """Worker function for thread to scrape a single process"""
        try:
            result = self.get_process_data(process_number)
            self.results_queue.put((process_number, result))
        except Exception as e:
            # error_result = ProcessData(number=process_number, error=str(e))
            error_result = ProcessData(number=process_number, error = "Segredo de justiça")
            self.results_queue.put((process_number, error_result))
        finally:
            self._cleanup_thread_driver()

    def get_process_data(self, process_number: str) -> ProcessData:
        """Scrape data for a single process number"""
        try:
            self._validate_process_number(process_number)
            driver = self._setup_webdriver()

            # First get the main process data
            driver.get(self.URL)
            time.sleep(1)

            self._fill_process_form(driver, process_number)
            time.sleep(2)

            try:
                mais_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.LINK_TEXT, "Mais"))
                )
                mais_button.click()
                time.sleep(2)
            except TimeoutException:
                # Continue even if "Mais" button is not found
                pass

            # Extract initial data
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            process_data = self._extract_process_data(soup, process_number)

            # If we have a defendant party, get their other processes count
            if process_data.defendant:
                other_processes = self._get_other_processes_count(driver, process_data.defendant)
                process_data.other_processes = other_processes

            return process_data

        except Exception as e:
            # return ProcessData(number=process_number, error=str(e))
            return ProcessData(number=process_number, error = "Segredo de justiça")

    def _get_other_processes_count(self, driver: webdriver.Chrome, defendant_name: str) -> Optional[int]:
        """Get the count of other processes for the defendant party"""
        try:
            # Navigate to the search page
            driver.get(self.URL)
            time.sleep(1)

            # Set search type to name
            select_element = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "cbPesquisa"))
            )
            select_element.send_keys("NMPARTE")

            # Check the exact name box
            checkbox = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "pesquisarPorNomeCompleto"))
            )
            driver.execute_script("arguments[0].click();", checkbox)

            # Input the name
            name_field = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "campo_NMPARTE"))
            )
            name_field.clear()
            name_field.send_keys(defendant_name)

            # Click search button
            search_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.ID, "botaoConsultarProcessos"))
            )
            search_button.click()

            try:
                # Wait for results and get count
                count_element = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.ID, "contadorDeProcessos"))
                )

                # Extract number from text (e.g., "873 Processos encontrados" -> "873")
                count_text = count_element.text.strip().split()[0]
                return int(count_text)

            except TimeoutException:
                return 0
            except (IndexError, ValueError):
                return 0

        except Exception as e:
            print(f"Error getting process count: {str(e)}")
            return 0
        finally:
            # Clear cookies to reduce memory usage
            driver.delete_all_cookies()

    def _process_batch_parallel(self, batch: List[str]) -> List[ProcessData]:
        """Process a batch of process numbers in parallel"""
        # Clear the results queue
        while not self.results_queue.empty():
            self.results_queue.get()

        # Create threads for each process number
        threads = []
        for process_number in batch:
            thread = threading.Thread(target=self._scrape_process_worker, args=(process_number,))
            threads.append(thread)

        # Start threads with concurrency limit
        active_threads = []
        results = []

        for thread in threads:
            # Wait if we've reached the max concurrent browsers
            while len(active_threads) >= self.max_concurrent_browsers:
                # Check for completed threads
                for t in active_threads[:]:
                    if not t.is_alive():
                        active_threads.remove(t)

                        # Get any results that are ready
                        while not self.results_queue.empty():
                            process_number, result = self.results_queue.get()
                            results.append((process_number, result))

                if len(active_threads) >= self.max_concurrent_browsers:
                    time.sleep(0.5)  # Short sleep to prevent CPU spinning

            # Start new thread
            thread.start()
            active_threads.append(thread)

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Get remaining results
        while not self.results_queue.empty():
            process_number, result = self.results_queue.get()
            results.append((process_number, result))

        # Make sure results are in the same order as input batch
        process_number_to_result = {process_number: result for process_number, result in results}
        ordered_results = [process_number_to_result.get(pn, ProcessData(number=pn, error="No result returned"))
                           for pn in batch]

        return ordered_results

    def process_batch(self, process_numbers: List[str], batch_size: int = 50) -> List[ProcessData]:
        """Process multiple process numbers and return results with parallel execution support"""
        results = []
        total = len(process_numbers)

        print(f"\nProcessing {total} process numbers in batches of {batch_size}...")
        print(f"Using up to {self.max_concurrent_browsers} concurrent browser instances")

        # Process in batches to manage memory better
        for i in range(0, total, batch_size):
            batch = process_numbers[i:i + batch_size]
            batch_total = len(batch)
            print(
                f"\nProcessing batch {(i // batch_size) + 1}/{(total + batch_size - 1) // batch_size} ({i + 1}-{min(i + batch_size, total)} of {total}):")

            batch_results = self._process_batch_parallel(batch)
            results.extend(batch_results)

            print(f"  Completed batch {(i // batch_size) + 1} ({len(batch_results)} processes)")

            # If batch is large, save intermediate results
            if batch_size > 100:
                self._save_intermediate_results(results, i + len(batch), is_batch=True)

        return results

    def _save_intermediate_results(self, results: List[ProcessData], processed_count: int, is_batch: bool = False):
        """Save intermediate results to avoid data loss"""
        if not results:
            return

        # Create DataFrame
        df = pd.DataFrame([result.to_dict() for result in results])

        # Generate filename with timestamp and count
        timestamp = datetime.now().strftime('%Y_%m_%d_%H%M%S')
        filename_prefix = "eSAJ_batch" if is_batch else "eSAJ_intermediate"
        filename = f"{filename_prefix}_{timestamp}_{processed_count}.csv"
        filepath = self.OUTPUT_DIR / filename

        # Save to CSV
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"    Intermediate results saved to: {filepath}")

    def display_results(self, results: List[ProcessData]):
        """Display results summary and limited table"""
        if not results:
            print("No results to display.")
            return

        # Display summary table
        successful = len([r for r in results if not r.error])
        errors = len([r for r in results if r.error])

        print(f"\nResults Summary:")
        print(f"Total processes processed: {len(results)}")
        print(f"Successful: {successful}")
        print(f"Errors: {errors}")

        # Display only the first 5 results in the table
        display_count = min(5, len(results))
        headers = ProcessData.get_headers()
        table_data = [[getattr(result, field.name) for field in fields(ProcessData)] for result in
                      results[:display_count]]

        print(f"\nShowing first {display_count} results:")
        print(tabulate(table_data, headers=headers, tablefmt="grid"))

        if len(results) > display_count:
            print(f"\n... and {len(results) - display_count} more results not shown.")

    def save_results(self, results: List[ProcessData], force_save: bool = False):
        """Save results to CSV (optional)"""
        if not results:
            print("No results to save.")
            return

        # Ask user if they want to save unless force_save is True
        save_to_csv = force_save
        if not force_save:
            response = input("\nSave results to CSV? (y/n): ").strip().lower()
            save_to_csv = response.startswith('y')

        if save_to_csv:
            # Create DataFrame
            df = pd.DataFrame([result.to_dict() for result in results])

            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y_%m_%d_%H%M%S')
            filename = f"eSAJ_final_{timestamp}.csv"
            filepath = self.OUTPUT_DIR / filename

            # Save to CSV
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"\nResults saved to: {filepath}")
        else:
            print("\nResults not saved to CSV.")


class CSVProcessExtractor:
    """Extracts process numbers from CSV files generated by NewSearchEngine.py"""

    def __init__(self):
        self.process_number_pattern = r'\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}'

    def extract_from_csv(self, csv_path: str) -> Set[str]:
        """Extract unique process numbers from a CSV file"""
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        print(f"Extracting process numbers from: {csv_path}")
        process_numbers = set()

        try:
            # First, determine if there's a summary section by reading the first few lines
            csv.field_size_limit(2000000000)
            with open(csv_path, 'r', encoding='utf-8-sig', errors='replace') as file:
                header_line_idx = 0
                for idx, line in enumerate(file):
                    if 'Process Number' in line:
                        header_line_idx = idx
                        break

            # Now read the file as a CSV, skipping the summary section
            with open(csv_path, 'r', encoding='utf-8-sig', errors='replace') as file:
                # Skip summary section
                for _ in range(header_line_idx):
                    next(file)

                # Read the CSV part
                reader = csv.reader(file)
                header = next(reader)  # Read the header

                # Find the index of the process number column
                process_col_idx = None
                for idx, col_name in enumerate(header):
                    if 'Process Number' in col_name:
                        process_col_idx = idx
                        break

                if process_col_idx is None:
                    raise ValueError("Could not find 'Process Number' column in the CSV file")

                # Extract process numbers
                row_count = 0
                for row in reader:
                    row_count += 1
                    if len(row) > process_col_idx:
                        # Sometimes the process number might be part of a larger cell value
                        cell_value = row[process_col_idx]
                        matches = re.findall(self.process_number_pattern, cell_value)
                        if matches:
                            for match in matches:
                                process_numbers.add(match)

                print(f"Processed {row_count} rows, found {len(process_numbers)} unique process numbers")

        except Exception as e:
            print(f"Error extracting process numbers: {str(e)}")
            # If the standard approach fails, use a fallback method
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


def main():
    extractor = None
    scraper = None

    try:
        print("\n===== Process Data Extraction Tool =====")
        print("This tool extracts process data from the eSAJ system")

        while True:
            print("\nOptions:")
            print("1. Extract data from CSV file")
            print("2. Extract data from manual process number entry")
            print("3. Exit")

            choice = input("\nSelect an option (1-3): ").strip()

            if choice == "3":
                print("Exiting program...")
                break

            if choice == "1":
                # Process CSV file
                csv_path = input("\nEnter the path to the CSV file: ").strip()
                if not csv_path:
                    print("No file path provided.")
                    continue

                # Extract process numbers from CSV
                extractor = CSVProcessExtractor()
                process_numbers = list(extractor.extract_from_csv(csv_path))

                if not process_numbers:
                    print("No process numbers found in the CSV file.")
                    continue

                print(f"\nFound {len(process_numbers)} process numbers.")
                max_display = min(5, len(process_numbers))
                print(f"Sample: {', '.join(process_numbers[:max_display])}" +
                      (f" (and {len(process_numbers) - max_display} more...)" if len(
                          process_numbers) > max_display else ""))

                confirm = input(
                    f"\nProceed with extracting data for these {len(process_numbers)} processes? (y/n): ").strip().lower()
                if confirm != 'y':
                    print("Operation cancelled.")
                    continue

                # Set up batch size
                default_batch_size = 50
                batch_size_input = input(f"\nEnter batch size (default: {default_batch_size}): ").strip()
                try:
                    batch_size = int(batch_size_input) if batch_size_input else default_batch_size
                except ValueError:
                    batch_size = default_batch_size
                    print(f"Invalid batch size, using default: {default_batch_size}")

                # Set up concurrent browser count
                default_browser_count = 4
                browser_count_input = input(
                    f"\nEnter number of concurrent browsers (default: {default_browser_count}): ").strip()
                try:
                    browser_count = int(browser_count_input) if browser_count_input else default_browser_count
                    # Limit to a reasonable number to prevent system overload
                    if browser_count > 8:
                        print("Warning: Using too many browsers may cause system instability.")
                        if input("Are you sure you want to continue? (y/n): ").strip().lower() != 'y':
                            browser_count = default_browser_count
                except ValueError:
                    browser_count = default_browser_count
                    print(f"Invalid browser count, using default: {default_browser_count}")

                # Process the data
                scraper = ProcessValueScraper(max_concurrent_browsers=browser_count)
                results = scraper.process_batch(process_numbers, batch_size=batch_size)
                scraper.display_results(results)
                scraper.save_results(results)

            elif choice == "2":
                # Manual entry
                print("\nEnter process numbers (one per line, empty line to finish):")
                process_numbers = []
                while True:
                    number = input().strip()
                    if not number:
                        break
                    process_numbers.append(number)

                if not process_numbers:
                    print("No process numbers provided.")
                    continue

                # Set up concurrent browser count
                default_browser_count = 4
                browser_count_input = input(
                    f"\nEnter number of concurrent browsers (default: {default_browser_count}): ").strip()
                try:
                    browser_count = int(browser_count_input) if browser_count_input else default_browser_count
                    # Limit to a reasonable number to prevent system overload
                    if browser_count > 8:
                        print("Warning: Using too many browsers may cause system instability.")
                        if input("Are you sure you want to continue? (y/n): ").strip().lower() != 'y':
                            browser_count = default_browser_count
                except ValueError:
                    browser_count = default_browser_count
                    print(f"Invalid browser count, using default: {default_browser_count}")

                # Process the data
                scraper = ProcessValueScraper(max_concurrent_browsers=browser_count)
                results = scraper.process_batch(process_numbers)
                scraper.display_results(results)
                scraper.save_results(results)

            else:
                print("Invalid option. Please try again.")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Clean up resources
        if scraper:
            try:
                del scraper
            except:
                pass


if __name__ == "__main__":
    main()