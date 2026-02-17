import os
from pathlib import Path

# --- Directory paths (all overridable via environment variables) ---
DB_DIR: Path = Path(os.environ.get("POURSUITE_DB_DIR", "D:/Poursuite/Databases"))
OUTPUT_DIR: Path = Path(os.environ.get("POURSUITE_OUTPUT_DIR", "C:/Poursuite/SearchResults"))
ESAJ_OUTPUT_DIR: Path = Path(os.environ.get("POURSUITE_ESAJ_OUTPUT_DIR", "C:/Poursuite/eSAJ"))
LOG_DIR: Path = Path(os.environ.get("POURSUITE_LOG_DIR", "C:/Poursuite/Logs"))

# --- Logging ---
SEARCH_LOG_FILE: Path = LOG_DIR / "search_engine.log"
SCRAPER_LOG_FILE: Path = LOG_DIR / "tjsp_scraper.log"

# --- Search engine constants ---
DEFAULT_MAX_WORKERS: int = int(os.environ.get("POURSUITE_MAX_WORKERS", "16"))
DEFAULT_BATCH_SIZE: int = int(os.environ.get("POURSUITE_BATCH_SIZE", "50"))
DEFAULT_MAX_BROWSERS: int = int(os.environ.get("POURSUITE_MAX_BROWSERS", "4"))

# --- Process number regex (single definition for the entire project) ---
PROCESS_NUMBER_PATTERN: str = r'\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}'
PROCESS_NUMBER_PATTERN_STRICT: str = r'^\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}$'

# --- API settings ---
API_KEY: str = os.environ.get("POURSUITE_API_KEY", "")
SEARCH_TIMEOUT_SECONDS: int = int(os.environ.get("POURSUITE_SEARCH_TIMEOUT", "30"))
DEFAULT_PAGE_SIZE: int = 100
MAX_PAGE_SIZE: int = 500

# --- eSAJ scraper ---
ESAJ_URL: str = "https://esaj.tjsp.jus.br/cpopg/open.do"
ESAJ_SEALED_ELEMENT_ID: str = "labelSituacaoProcesso"
ESAJ_SEALED_TEXT: str = "Segredo de Justiça"
