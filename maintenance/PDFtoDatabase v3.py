import sqlite3
import re
import concurrent.futures
import multiprocessing as mp
import os
import zlib
import logging
import fitz
from pathlib import Path
from tqdm import tqdm
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple


def process_pdf_worker(pdf_path_str: str) -> Tuple[int, List[Dict], str]:
    pdf_path = Path(pdf_path_str)
    results = []

    # Extract date from filename
    try:
        date_str = pdf_path.stem[:8]
        document_date = datetime.strptime(date_str, '%Y%m%d').date()
    except (ValueError, IndexError):
        return -1, [], pdf_path_str

    year = document_date.year

    try:
        # Use PyMuPDF instead of PyPDF2
        accumulated_text = ""
        with fitz.open(pdf_path) as doc:
            # Process in batches to manage memory
            batch_size = 50
            for i in range(0, len(doc), batch_size):
                batch_text = ""
                for page_num in range(i, min(i + batch_size, len(doc))):
                    page = doc[page_num]
                    page_text = page.get_text()
                    batch_text += page_text + "\n"

                accumulated_text += batch_text

                # Process large batches incrementally to save memory
                if len(accumulated_text) > 5_000_000:  # ~5MB of text
                    extract_processes(accumulated_text, results, pdf_path, document_date)
                    # Keep only the last potential partial paragraph
                    last_part = accumulated_text.split('Processo')[-1]
                    accumulated_text = '' if 'Processo' in last_part else 'Processo' + last_part

            # Process any remaining text
            if accumulated_text:
                extract_processes(accumulated_text, results, pdf_path, document_date)

    except Exception as e:
        logging.error(f"Error processing {pdf_path}: {e}")

    return year, results, pdf_path_str


def extract_processes(text: str, results: List[Dict], pdf_path: Path, document_date):
    """Extract process information from text block"""
    paragraphs = re.split(r'(?=Processo \d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4})', text)

    for para in paragraphs:
        para = para.strip()
        if para.startswith('Processo'):
            match = re.match(r'Processo (\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4})', para)
            process_num = match.group(1) if match else None

            if process_num:
                cleaned_para = ' '.join(para.split())
                results.append({
                    'process_number': process_num,
                    'content': cleaned_para,
                    'file_path': str(pdf_path),
                    'document_date': document_date
                })


class DatabaseManager:
    """Handles creation and management of year-specific databases"""

    def __init__(self, base_dir: str = '.'):
        self.base_dir = Path(base_dir)
        self.db_dir = Path("C:\\Poursuite\\Databases")
        self.db_dir.mkdir(exist_ok=True, parents=True)
        self.setup_logging()
        self.connections = {}  # Cache for database connections

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('pdf_processor.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_db_path(self, year: int) -> Path:
        """Get database path for specific year"""
        return self.db_dir / f"legal_documents_{year}.db"

    def get_connection(self, year: int) -> sqlite3.Connection:
        """Get or create a database connection for a specific year"""
        if year not in self.connections:
            db_path = self.get_db_path(year)
            conn = sqlite3.connect(str(db_path))

            # Configure connection for performance
            conn.execute("PRAGMA journal_mode = WAL") # Write-Ahead logging for better concurrency
            conn.execute("PRAGMA synchronous = NORMAL")  # Balance between safety and speed
            conn.execute("PRAGMA cache_size = -81920")  # More RAM for cache (about 80MB)
            conn.execute("PRAGMA temp_store = MEMORY")  # Store temp tables in memory
            conn.execute("PRAGMA mmap_size = 1073741824")  # 1GB memory mapping

            self.setup_database(conn)
            self.connections[year] = conn
        return self.connections[year]

    def setup_database(self, conn: sqlite3.Connection):
        """Set up database schema"""
        c = conn.cursor()

        # Main table for paragraphs
        c.execute('''CREATE TABLE IF NOT EXISTS paragraphs
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      process_number TEXT,
                      content BLOB,
                      file_path TEXT,
                      document_date DATE)''')

        # Create FTS5 virtual table for efficient full-text search
        c.execute('''CREATE VIRTUAL TABLE IF NOT EXISTS paragraphs_fts 
                     USING fts5(content, content_rowid=id)''')

        # Table to track processed files
        c.execute('''CREATE TABLE IF NOT EXISTS processed_files
                     (file_path TEXT PRIMARY KEY,
                      processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

        # Create indices for faster searches
        c.execute('CREATE INDEX IF NOT EXISTS idx_process_number ON paragraphs(process_number)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_filepath ON paragraphs(file_path)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_document_date ON paragraphs(document_date)')

        # Create triggers to automatically maintain FTS index
        c.executescript('''
            CREATE TRIGGER IF NOT EXISTS paragraphs_ai AFTER INSERT ON paragraphs BEGIN
                INSERT INTO paragraphs_fts(rowid, content) 
                VALUES (new.id, new.content);
            END;

            CREATE TRIGGER IF NOT EXISTS paragraphs_ad AFTER DELETE ON paragraphs BEGIN
                INSERT INTO paragraphs_fts(paragraphs_fts, rowid, content) 
                VALUES('delete', old.id, old.content);
            END;

            CREATE TRIGGER IF NOT EXISTS paragraphs_au AFTER UPDATE ON paragraphs BEGIN
                INSERT INTO paragraphs_fts(paragraphs_fts, rowid, content) 
                VALUES('delete', old.id, old.content);
                INSERT INTO paragraphs_fts(rowid, content) VALUES (new.id, new.content);
            END;
        ''')

        conn.commit()

    def close_all_connections(self):
        """Close all open database connections"""
        for year, conn in self.connections.items():
            conn.commit()
            conn.close()
        self.connections = {}

    def get_processed_files(self, year: int) -> Set[str]:
        """Get set of already processed files for specific year"""
        conn = self.get_connection(year)
        c = conn.cursor()
        c.execute('SELECT file_path FROM processed_files')
        return {row[0] for row in c.fetchall()}

    def mark_file_as_processed(self, year: int, file_path: str):
        """Mark a file as processed in the year-specific database"""
        conn = self.get_connection(year)
        c = conn.cursor()
        c.execute('INSERT INTO processed_files (file_path) VALUES (?)', (str(file_path),))
        conn.commit()

    def store_results(self, year: int, results: List[Dict]):
        """Store processing results in the year-specific database"""
        if not results:
            return

        conn = self.get_connection(year)
        c = conn.cursor()

        # Compress content in parallel for large batches
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            def compress_content(result):
                result_copy = result.copy()
                result_copy['content'] = zlib.compress(result_copy['content'].encode('utf-8'), level=9)
                return result_copy

            compressed_results = list(executor.map(compress_content,results))

        # Use larger batch sizes for inserts
        batch_size = 1000
        for i in range(0, len(compressed_results), batch_size):
            batch = compressed_results[i:i+batch_size]

            c.executemany('''INSERT INTO paragraphs 
                             (process_number, content, file_path, document_date)
                             VALUES (?, ?, ?, ?)''',
                          [(r['process_number'], r['content'], r['file_path'], r['document_date'])
                           for r in batch])
            conn.commit()

    def optimize_database(self, year: int):
        """Optimize a year-specific database"""
        db_path = self.get_db_path(year)

        # Close connection to allow optimization
        if year in self.connections:
            self.connections[year].close()
            del self.connections[year]

        original_size = os.path.getsize(db_path)

        # Create new connection for vacuum
        conn = sqlite3.connect(str(db_path))
        print("Running VACUUM. This might take a while...")
        conn.execute("VACUUM;")
        conn.close()

        new_size = os.path.getsize(db_path)

        self.logger.info(
            f"Optimized database for {year}: "
            f"Original size: {original_size / 1024 / 1024:.2f}MB, "
            f"New size: {new_size / 1024 / 1024:.2f}MB, "
            f"Ratio: {new_size / original_size:.2%}"
        )


class PDFProcessor:
    """Processes PDF files into year-specific databases with compression"""

    def __init__(self, base_dir: str = '.'):
        self.base_dir = Path(base_dir)
        self.db_manager = DatabaseManager(base_dir)

    def extract_date_from_filename(self, file_path: Path) -> Optional[datetime]:
        """Extract date from filename (YYYYMMDD format)"""
        try:
            date_str = file_path.stem[:8]  # Get first 8 characters of filename
            return datetime.strptime(date_str, '%Y%m%d').date()
        except (ValueError, IndexError) as e:
            self.db_manager.logger.error(f"Error extracting date from filename {file_path}: {e}")
            return None

    def get_unprocessed_files(self, pdf_files: List[Path]) -> Dict[int, List[Path]]:
        """Group unprocessed files by year"""
        year_files = {}

        for pdf_path in pdf_files:
            document_date = self.extract_date_from_filename(pdf_path)
            if not document_date:
                continue

            year = document_date.year

            if year not in year_files:
                # Get processed files for this year
                processed_files = self.db_manager.get_processed_files(year)
                year_files[year] = []
            else:
                processed_files = set()  # Already loaded

            # Check if file was already processed
            if str(pdf_path) not in processed_files:
                year_files[year].append(pdf_path)

        # Log summary
        total_unprocessed = sum(len(files) for files in year_files.values())
        self.db_manager.logger.info(f"Found {total_unprocessed} unprocessed files across {len(year_files)} years")

        return year_files

    def process_all_pdfs(self):
        """Process all PDF files in the base directory"""
        pdf_files = list(Path(self.base_dir).rglob('*.pdf'))
        self.db_manager.logger.info(f"Found {len(pdf_files)} PDF files in total")

        # Group unprocessed files by year
        year_files = self.get_unprocessed_files(pdf_files)

        if not any(year_files.values()):
            self.db_manager.logger.info("No new files to process")
            return

        # Process files by year chunks
        for year, files in year_files.items():
            self.db_manager.logger.info(f"Processing {len(files)} files for year {year}")
            self._process_files_for_year(files)

            # Optimize database after processing all files for the year
            # self.db_manager.optimize_database(year)

    def _process_files_for_year(self, files: List[Path]):
        """Process all files for a specific year"""
        # Process in smaller chunks to manage memory
        chunk_size = 200  # Adjust based on your system's memory

        with tqdm(total=len(files), desc=f"Processing PDFs") as pbar:
            for i in range(0, len(files), chunk_size):
                chunk = files[i:i + chunk_size]

                # Convert Path objects to strings for serialization
                chunk_str = [str(pdf_file) for pdf_file in chunk]

                # Process chunk in parallel using standalone function
                with concurrent.futures.ProcessPoolExecutor(
                        max_workers=min(mp.cpu_count(),12), # Limit to avoid oversubscription
                        mp_context=mp.get_context('spawn') # More stable for large files
                ) as executor:
                    futures = {executor.submit(process_pdf_worker, pdf_file_str): pdf_file_str
                               for pdf_file_str in chunk_str}

                    for future in concurrent.futures.as_completed(futures):
                        pdf_file_str = futures[future]
                        try:
                            year, results, file_path = future.result()
                            if year > 0 and results:  # Valid year and results
                                self.db_manager.store_results(year, results)

                            # Mark file as processed if we got a valid year
                            if year > 0:
                                self.db_manager.mark_file_as_processed(year, file_path)

                        except Exception as e:
                            self.db_manager.logger.error(f"Error with {pdf_file_str}: {e}")
                        finally:
                            pbar.update(1)


class DatabaseValidator:
    """Validates database integrity and provides statistics"""

    def __init__(self, db_dir: str = '.'):
        self.db_dir = Path("C:\\Poursuite\\Databases")

    def validate_all_databases(self):
        """Run validation on all year databases"""
        db_files = list(self.db_dir.glob('legal_documents_*.db'))

        if not db_files:
            print("No database files found.")
            return

        total_files = 0
        total_paragraphs = 0
        total_processes = 0
        years_info = []

        for db_path in sorted(db_files):
            year = db_path.stem.split('_')[-1]
            stats = self.validate_database(db_path)

            total_files += stats['processed_files']
            total_paragraphs += stats['paragraphs']
            total_processes += stats['unique_processes']

            years_info.append({
                'year': year,
                'files': stats['processed_files'],
                'paragraphs': stats['paragraphs'],
                'processes': stats['unique_processes'],
                'size_mb': stats['size_mb'],
                'date_range': stats['date_range']
            })

        # Print summary
        print("\n=== Overall Database Statistics ===")
        print(f"Total database files: {len(db_files)}")
        print(f"Total processed files: {total_files:,}")
        print(f"Total paragraphs: {total_paragraphs:,}")
        print(f"Total unique process numbers: {total_processes:,}")

        print("\n=== Per-Year Statistics ===")
        for info in years_info:
            print(f"Year {info['year']}: {info['files']:,} files, "
                  f"{info['paragraphs']:,} paragraphs, "
                  f"{info['processes']:,} processes, "
                  f"{info['size_mb']:.2f} MB, "
                  f"Date range: {info['date_range']}")

    def validate_database(self, db_path: Path) -> Dict:
        """Validate a specific database file"""
        stats = {}

        try:
            conn = sqlite3.connect(str(db_path))
            c = conn.cursor()

            # Get processing statistics
            c.execute('SELECT COUNT(*) from processed_files')
            stats['processed_files'] = c.fetchone()[0]

            c.execute('SELECT COUNT(*) from paragraphs')
            stats['paragraphs'] = c.fetchone()[0]

            c.execute('SELECT COUNT(DISTINCT process_number) from paragraphs')
            stats['unique_processes'] = c.fetchone()[0]

            # Date-related statistics
            c.execute('SELECT MIN(document_date), MAX(document_date) from paragraphs')
            date_range = c.fetchone()
            stats['date_range'] = f"{date_range[0]} to {date_range[1]}"

            # Database size
            stats['size_mb'] = os.path.getsize(db_path) / (1024 * 1024)

            conn.close()
            return stats

        except Exception as e:
            print(f"Error validating {db_path}: {e}")
            return {
                'processed_files': 0,
                'paragraphs': 0,
                'unique_processes': 0,
                'date_range': 'N/A',
                'size_mb': 0
            }


def process_all_pdfs(base_dir: str):
    """Main function to process all PDFs with performance settings"""
    # Set process priority for better performance
    try:
        import psutil
        process = psutil.Process()
        process.nice(psutil.ABOVE_NORMAL_PRIORITY_CLASS if os.name == 'nt' else -5)
    except ImportError:
        pass  # psutil not available

    # Configure SQLite to use more memory
    sqlite3.connect(':memory:').execute('PRAGMA cache_size = -102400')  # 100MB global cache

    processor = PDFProcessor(base_dir)
    processor.process_all_pdfs()

    # Close all database connections
    processor.db_manager.close_all_connections()

    # Validate results
    validator = DatabaseValidator(base_dir)
    validator.validate_all_databases()


if __name__ == '__main__':
    base_dir = input("Enter path to PDF directory: ")
    os.environ["PYTHONTHREAD​POOLEXECUTOR_MAX_WORKERS"] = str(min(32, os.cpu_count() * 2))
    process_all_pdfs(base_dir)