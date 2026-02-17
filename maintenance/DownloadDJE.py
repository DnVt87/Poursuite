from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
import logging
import time
from pathlib import Path
from typing import Optional, Dict, List
from dataclasses import dataclass

@dataclass
class Caderno:
    """Configuration for each document type to download."""
    value: str
    name: str

class TJSPScraper:
    """Handles document downloading from TJSP website."""

    BASE_URL = "https://dje.tjsp.jus.br/cdje"
    BASE_DIR = Path("C:/Poursuite/CourtDocs")

    CADERNOS = [
        Caderno("12", "Judicial_Capital_1"),
        Caderno("20", "Judicial_Capital_2"),
        Caderno("18", "Judicial_Interior_1"),
        Caderno("13", "Judicial_Interior_2"),
        Caderno("15", "Judicial_Interior_3")
    ]

    def __init__(self):
        """Initialize the scraper with Chrome options and logging setup."""
        self._setup_logging()
        self._setup_browser()

    def _setup_logging(self):
        """Configure logging with both file and console output."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('tjsp_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _setup_browser(self):
        """Configure and initialize Chrome WebDriver."""
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run in background
        prefs = {
            "download.default_directory": str(self.BASE_DIR),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True
        }
        chrome_options.add_experimental_option("prefs", prefs)

        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)

    def _is_valid_date(self, date: datetime) -> bool:
        """Check if date is valid for downloading (not weekend or future)."""
        today = datetime.now()

        if date > today:
            self.logger.warning(f"Skipping future date: {date.strftime('%d/%m/%Y')}")
            return False

        if date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            self.logger.warning(f"Skipping weekend date: {date.strftime('%d/%m/%Y')}")
            return False

        return True

    def _create_date_directory(self, date: datetime) -> Path:
        """Create and return path for year/month directory structure."""
        year_dir = self.BASE_DIR / str(date.year)
        month_dir = year_dir / f"{date.month:02d}"
        month_dir.mkdir(parents=True, exist_ok=True)
        return month_dir

    def _set_date(self, date: datetime) -> bool:
        """Set date in the website's date field using JavaScript."""
        try:
            date_str = date.strftime("%d/%m/%Y")
            self.driver.execute_script(
                f"document.getElementById('dtDiarioCad').value = '{date_str}';"
            )
            return True
        except Exception as e:
            self.logger.error(f"Error setting date {date_str}: {e}")
            return False

    def _download_caderno(self, caderno: Caderno, date: datetime) -> bool:
        """
        Download and move a specific caderno (document) for a given date.
        Includes retry logic and file lock checking.
        """
        try:
            # Select the document type
            select_element = self.wait.until(
                EC.presence_of_element_located((By.ID, "cadernosCad"))
            )
            Select(select_element).select_by_value(caderno.value)

            # Get directory for final file location
            final_dir = self._create_date_directory(date)
            date_str = date.strftime("%Y%m%d")
            final_file = final_dir / f"{date_str}_{caderno.name}.pdf"

            # If the final file already exists, skip download
            if final_file.exists():
                self.logger.info(f"File {final_file.name} already exists, skipping download")
                return True

            # Before downloading, check if any "caderno" PDF exists in BASE_DIR
            existing_files = list(self.BASE_DIR.glob("caderno*.pdf"))

            # Click download button
            download_button = self.wait.until(
                EC.element_to_be_clickable((By.ID, "download"))
            )
            download_button.click()

            # Wait for new file to appear in BASE_DIR
            download_timeout = 10
            while download_timeout > 0:
                current_files = list(self.BASE_DIR.glob("caderno*.pdf"))
                new_files = [f for f in current_files if f not in existing_files]

                if new_files:
                    downloaded_file = new_files[0]  # Get the first new file

                    # Try to move the file with retry logic
                    move_timeout = 10
                    while move_timeout > 0:
                        try:
                            # Check if file is locked
                            with open(downloaded_file, 'rb'):
                                pass

                            # If we can open it, try to move it
                            downloaded_file.rename(final_file)
                            self.logger.info(f"Successfully downloaded and moved {final_file.name}")
                            return True

                        except PermissionError:
                            # File is still locked, wait and retry
                            self.logger.debug(f"File {downloaded_file.name} is locked, retrying in 1 second")
                            time.sleep(1)
                            move_timeout -= 1
                            continue

                        except FileExistsError:
                            # If file already exists at destination, consider it a success
                            self.logger.info(f"File {final_file.name} already exists at destination")
                            downloaded_file.unlink()  # Delete the duplicate download
                            return True

                        except Exception as move_error:
                            self.logger.error(f"Unexpected error moving file: {move_error}")
                            return False

                    self.logger.error(f"Failed to move file after {10 - move_timeout} attempts")
                    return False

                time.sleep(1)
                download_timeout -= 1

            self.logger.error(f"Download failed or timed out for {caderno.name}")
            return False

        except Exception as e:
            self.logger.error(f"Error downloading {caderno.name}: {e}")
            return False

    def download_documents(self, start_date: str, end_date: Optional[str] = None) -> Dict[str, List[str]]:
        """Download documents for a date range."""
        results = {
            "successful": [],
            "failed": []
        }
        try:
            # Parse dates
            start = datetime.strptime(start_date, "%d/%m/%Y")
            end = datetime.strptime(end_date, "%d/%m/%Y") if end_date else start
            current = start
            while current <= end:
                if self._is_valid_date(current):
                    self.driver.get(self.BASE_URL)
                    if self._set_date(current):
                        for caderno in self.CADERNOS:
                            date_str = current.strftime("%d/%m/%Y")
                            if self._download_caderno(caderno, current):
                                results["successful"].append(f"{caderno.name}_{date_str}")
                            else:
                                results["failed"].append(f"{caderno.name}_{date_str}")
                    else:
                        results["failed"].append(f"All documents for {current.strftime('%d/%m/%Y')}")
                current += timedelta(days=1)

        except Exception as e:
            self.logger.error(f"Error in download_documents: {e}")
        finally:
            self.driver.quit()

        return results


if __name__ == "__main__":
    # Example usage
    scraper = TJSPScraper()

    # Download single date
    #results = scraper.download_documents("15/01/2024")

    # Download date range
    results = scraper.download_documents("01/01/2013", "31/12/2014")

    print("\nSuccessful downloads:", len(results["successful"]))
    print("Failed downloads:", len(results["failed"]))