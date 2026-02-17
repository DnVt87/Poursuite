import sqlite3
import os
import time
import hashlib
import zlib
import shutil
import argparse
import subprocess
import logging
import psutil
import gc
import  ctypes
import sys
import multiprocessing
from tqdm import tqdm
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from pybloom_live import BloomFilter

def setup_logging(log_file='static_db_optimizer.log'):
    """Configure logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


class StaticDatabaseOptimizer:
    """Comprehensive optimizer for static read-only databases"""

    def __init__(self, db_path, output_dir=None, batch_size=50000, logger=None):
        self.logger = logger or setup_logging()
        # Get system memory
        system_memory_gb = psutil.virtual_memory().total / (1024**3)
        self.logger.info(f"System has {system_memory_gb:.2f} GB of total RAM")
        # Reasonable cache size (25% of available RAM up to 8GB)
        self.cache_size_mb = min(int(system_memory_gb * 600), 18000)
        self.logger.info(f"Setting SQLite cache size to {self.cache_size_mb} MB")
        self.db_path = Path(db_path)
        self.year = self._extract_year_from_filename()
        self.output_dir = Path(output_dir) if output_dir else self.db_path.parent / "Optimized"
        self.batch_size = batch_size
        self.original_size = os.path.getsize(self.db_path)
        self.temp_dir = self.output_dir / "temp"

        # Create output directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        cpu_count = multiprocessing.cpu_count()
        self.max_workers = min(cpu_count - 1,32) # Reserve one core for system operations

    def _extract_year_from_filename(self):
        """Extract year from database filename"""
        try:
            # Assume filename format: legal_documents_YYYY.db
            year_str = self.db_path.stem.split('_')[-1]
            return int(year_str)
        except (ValueError, IndexError):
            # If we can't extract year, use current year but log warning
            current_year = time.localtime().tm_year
            self.logger.warning(f"Could not extract year from {self.db_path.name}, using current year {current_year}")
            return current_year

    def create_backup(self):
        """Create a backup of the original database"""
        backup_path = self.output_dir / f"{self.db_path.stem}_backup{self.db_path.suffix}"
        self.logger.info(f"Creating backup at {backup_path}")
        shutil.copy2(self.db_path, backup_path)
        return backup_path

    def optimize(self):
        """Run the full optimization process"""
        self.logger.info(f"Starting optimization of {self.db_path}")
        self.logger.info(f"Original size: {self.original_size / (1024 * 1024):.2f} MB")

        # Log initial memory usage
        mem = psutil.virtual_memory()
        self.logger.info(f"Initial RAM usage: {mem.percent}%, Available: {mem.available/1024/1024/1024:.2f}GB")

        # Record timing information
        start_time = time.time()

        # Step 1: Create backup
        # self.create_backup()

        # Step 2: Run internal optimization (deduplication + compression)
        optimized_db = self.temp_dir / f"{self.db_path.stem}_optimized{self.db_path.suffix}"
        self.run_internal_optimization(optimized_db)

        # Step 3: Create read-optimized archive
        archive_db = self.output_dir / f"archive_{self.year}{self.db_path.suffix}"
        self.create_optimized_archive(optimized_db, archive_db)

        # Step 4: Create compressed archive for long-term storage
        compressed_archive = self.output_dir / f"archive_{self.year}.7z"
        # self.create_compressed_archive(archive_db, compressed_archive)

        # Calculate results
        end_time = time.time()
        duration = end_time - start_time
        optimized_size = os.path.getsize(archive_db)
        compressed_size = os.path.getsize(compressed_archive) if os.path.exists(compressed_archive) else 0

        # Log results
        self.logger.info("\n=== Optimization Results ===")
        self.logger.info(f"Original database: {self.original_size / (1024 * 1024):.2f} MB")
        self.logger.info(f"Optimized database: {optimized_size / (1024 * 1024):.2f} MB")

        if compressed_size > 0:
            self.logger.info(f"Compressed archive: {compressed_size / (1024 * 1024):.2f} MB")
            self.logger.info(f"Space reduction: {(1 - compressed_size / self.original_size) * 100:.2f}%")
        else:
            self.logger.info("Compressed archive creation failed.")
            self.logger.info(f"Space reduction: {(1 - optimized_size / self.original_size) * 100:.2f}%")

        self.logger.info(f"Total processing time: {duration:.2f} seconds")

        #  Final memory usage
        mem = psutil.virtual_memory()
        self.logger.info(f"Final RAM usage: {mem.percent}%, Available: {mem.available/1024/1024/1024:.2f}GB")

        # Clean up temporary files
        self.cleanup_temp_files()

        return {
            "original_path": str(self.db_path),
            "original_size_mb": self.original_size / (1024 * 1024),
            "optimized_path": str(archive_db),
            "optimized_size_mb": optimized_size / (1024 * 1024),
            "compressed_path": str(compressed_archive) if compressed_size > 0 else None,
            "compressed_size_mb": compressed_size / (1024 * 1024) if compressed_size > 0 else 0,
            "reduction_percent": (1 - (compressed_size or optimized_size) / self.original_size) * 100,
            "duration_seconds": duration
        }

    def run_internal_optimization(self, output_path):
        """
        Run internal optimization: deduplication and content compression
        """
        self.logger.info("\nStep 1: Internal optimization (deduplication + compression)")

        # Connect to source database
        src_conn = sqlite3.connect(str(self.db_path))
        src_conn.execute("PRAGMA mmap_size = 8589934592")  # Use memory for temp storage
        src_cursor = src_conn.cursor()

        # Create optimized database
        opt_conn = sqlite3.connect(str(output_path))
        opt_conn.execute("PRAGMA mmap_size = 8589934592")

        # Set optimal parameters
        opt_conn.execute("PRAGMA page_size = 65536")  # 64KB page size
        opt_conn.execute(f"PRAGMA cache_size = -{self.cache_size_mb * 1000}") # Dynamic cache
        opt_conn.execute("PRAGMA temp_store = MEMORY")
        opt_conn.execute("PRAGMA journal_mode = MEMORY")  # For write operations
        opt_conn.execute("PRAGMA synchronous = OFF")  # No sync needed for initial creation
        opt_conn.execute("PRAGMA wal_autocheckpoint = 0")
        opt_conn.execute("PRAGMA cache_spill = FALSE") # Keep cache in memory
        opt_conn.execute("PRAGMA secure_delete = FALSE")  # Faster deletes
        opt_conn.execute("PRAGMA threads = 8")  # Allow SQLite to use multiple threads
        opt_conn.execute("PRAGMA journal_size_limit = 134217728")  # 128MB journal size limit

        # Create tables in optimized database
        opt_cursor = opt_conn.cursor()

        # Get schema from original database
        src_cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='paragraphs'")
        table_schema = src_cursor.fetchone()[0]

        # Create table in optimized database
        opt_cursor.execute(table_schema)

        # Get total rows to process
        src_cursor.execute("SELECT COUNT(*) FROM paragraphs")
        total_rows = src_cursor.fetchone()[0]
        self.logger.info(f"Processing {total_rows} rows")

        # Disable automatic garbage collection
        gc.disable()

        # Process in batches
        processed = 0
        content_hashes = set() # For deduplication
        optimized_count = 0

        # Define helper function for parallel processing
        def process_row(row_data):
            row_id, process_num, content, file_path, doc_date = row_data
            content_str = self._content_to_string(content)
            content_hash = hashlib.md5(content_str.encode('utf-8')).hexdigest()
            compressed = zlib.compress(content_str.encode('utf-8'), level=9)
            return content_hash,row_id,process_num, compressed, file_path, doc_date

        with tqdm(total=total_rows, desc="Optimizing content") as pbar:
            while processed < total_rows:
                # Get a batch of rows
                src_cursor.execute(
                    "SELECT id, process_number, content, file_path, document_date FROM paragraphs LIMIT ? OFFSET ?",
                    (self.batch_size, processed)
                )
                rows = src_cursor.fetchall()

                if not rows:
                    break

                # Process rows in parallel
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    results = list(executor.map(process_row, rows))

                # Process results and deduplicate
                batch_insert_rows = []
                for content_hash, row_id, process_num, compressed, file_path, doc_date in results:
                    if content_hash not in content_hashes:
                            content_hashes.add(content_hash)
                            batch_insert_rows.append((optimized_count, process_num, compressed, file_path, doc_date))
                            optimized_count +=1
                # Batch insert
                try:
                    # Only begin a new transaction periodically
                    if processed % (self.batch_size * 10) == 0:
                        # Commit any existing transaction first
                        try:
                            opt_conn.commit()
                        except:
                            pass
                        # Then begin a new one
                        opt_conn.execute("BEGIN TRANSACTION")

                    # Batch insert
                    opt_cursor.executemany(
                        "INSERT INTO paragraphs (id, process_number, content, file_path, document_date) VALUES (?, ?, ?, ?, ?)",
                        batch_insert_rows
                    )

                    # Only commit occasionally, not after every batch
                    if processed % (self.batch_size * 10) == (self.batch_size * 10) - 1 or len(batch_insert_rows) < 1000:
                        opt_conn.commit()
                        # After committing, give the I/O subsystem time to recover
                        if processed > total_rows * 0.7:  # If we're in the slowdown zone (>70%)
                            self.logger.info(
                                f"Extended I/O pause at {processed} records ({processed / total_rows:.1%})")
                            time.sleep(1.5)  # Longer pause when we're in the slowdown zone

                    # Log success for large batches
                    if len(batch_insert_rows) > 10000:
                        self.logger.info(f"Successfully committed batch of {len(batch_insert_rows)} rows")

                except sqlite3.Error as e:
                    # Rollback on error
                    self.logger.error(f"Error during batch insert: {e}")
                    try:
                        opt_conn.rollback()
                        self.logger.info("Transaction rolled back successfully")
                    except Exception as rollback_error:
                        self.logger.error(f"Error during rollback: {rollback_error}")

                # Update progress
                processed += len(rows)
                pbar.update(min(len(rows), total_rows - (processed - len(rows))))

                # Periodic garbage collection
                if processed % (self.batch_size * 10) == 0:
                    gc.collect()
                    opt_conn.execute("PRAGMA shrink_memory")

                # Monitor memory usage periodically
                if processed % (self.batch_size * 10) == 0:
                    mem = psutil.virtual_memory()
                    self.logger.info(f"RAM usage: {mem.percent}%, Available: {mem.available/1024/1024/1024:.2f}GB")
                    gc.collect() # Force garbage collection

        # Free up memory used by the hash set
        del content_hashes
        gc.collect()

        # Create necessary indices
        self.logger.info("Creating optimized indices...")
        opt_cursor.execute("CREATE INDEX idx_process_number ON paragraphs(process_number)")
        opt_cursor.execute("CREATE INDEX idx_document_date ON paragraphs(document_date)")

        # Optimize database
        self.logger.info("Running VACUUM...")
        opt_conn.execute("PRAGMA journal_mode = DELETE")
        opt_conn.commit() # Ensure all transactions are committed before VACUUM

        try:
            vacuum_cache_size = max(int(self.cache_size_mb * 750), 2000)
            opt_conn.execute(f"PRAGMA cache_size = -{vacuum_cache_size}")
            opt_conn.execute("VACUUM")
        except MemoryError:
            self.logger.warning("Not enough memory for VACUUM operation skipping...")
        except sqlite3.OperationalError as e:
            self.logger.warning(f"VACUUM failed: {e}, skipping...")

        opt_conn.execute("ANALYZE")

        # Close connections
        src_conn.close()
        opt_conn.close()

        # Log results
        optimized_size = os.path.getsize(output_path)
        self.logger.info(f"Internal optimization complete. Size: {optimized_size / (1024 * 1024):.2f} MB")
        self.logger.info(f"Kept {optimized_count} rows after deduplication")

    @staticmethod
    def _content_to_string(content):
        """Convert content to string, handling compressed and binary data"""
        if isinstance(content, bytes):
            try:
                # Try to decompress first (in case it's already compressed)
                try:
                    return zlib.decompress(content).decode('utf-8')
                except zlib.error:
                    return content.decode('utf-8', errors='replace')
            except UnicodeDecodeError:
                # Use a hash of the raw bytes if all else fails
                return hashlib.md5(content).hexdigest()
        return str(content)

    def create_optimized_archive(self, source_db, archive_db):
        """
        Create a read-optimized archive of the database with FTS support
        """
        self.logger.info("\nStep 2: Creating read-optimized archive with FTS support")

        # Connect to source database
        src_conn = sqlite3.connect(str(source_db))
        src_conn.execute("PRAGMA mmap_size = 8589934592")
        src_cursor = src_conn.cursor()

        # Set optimal parameters for destination database
        archive_conn = sqlite3.connect(str(archive_db))
        archive_conn.execute("PRAGMA mmap_size = 8589934592") # 8GB for memory-mapped I/O
        archive_conn.execute("PRAGMA page_size = 32768")  # 32KB page size
        archive_conn.execute(f"PRAGMA cache_size = -{self.cache_size_mb * 1000}") # Dynamic cache
        archive_conn.execute("PRAGMA journal_mode = OFF")  # No journaling needed for read-only
        archive_conn.execute("PRAGMA synchronous = OFF")  # No sync needed for initial creation
        archive_conn.execute("PRAGMA locking_mode = NORMAL")  # Normal locking for read-only
        archive_conn.execute("PRAGMA temp_store = MEMORY")  # Use memory for temp storage
        archive_conn.execute("PRAGMA cache_spill = FALSE") # Keep cache in memory
        archive_conn.execute("PRAGMA secure_delete = FALSE")  # Faster deletes
        archive_conn.execute("PRAGMA threads = 8")  # Allow SQLite to use multiple threads

        # Use backup API for efficient copy
        self.logger.info("Copying database with backup API...")
        src_conn.backup(archive_conn)

        # Check if FTS table exists in source database
        self.logger.info("Checking for FTS table in source database...")
        src_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='paragraphs_fts'")
        fts_exists = src_cursor.fetchone() is not None

        if fts_exists:
            self.logger.info("FTS table found in source database. Recreating in optimized database...")
            # Drop FTS table if it exists in destination (from backup)
            archive_conn.execute("DROP TABLE IF EXISTS paragraphs_fts")

            # Create the FTS5 virtual table
            self.logger.info("Creating FTS5 virtual table...")
            archive_conn.execute('''CREATE VIRTUAL TABLE paragraphs_fts 
                                 USING fts5(content, content_rowid=id)''')

            # Get the total count of rows to index
            self.logger.info("Getting row count for FTS indexing...")
            archive_cursor = archive_conn.cursor()
            archive_cursor.execute("SELECT COUNT(*) FROM paragraphs")
            total_rows = archive_cursor.fetchone()[0]
            self.logger.info(f"Need to index {total_rows} rows")

            # Create triggers to maintain FTS index
            self.logger.info("Creating triggers for FTS table maintenance...")
            archive_conn.executescript('''
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

            # Define a helper function for parallel processing
            def process_fts_row(row_data):
                row_id, content = row_data
                content_text = None
                if isinstance(content, bytes):
                    try:
                        # Try to decompress zlib content
                        content_text = zlib.decompress(content).decode('utf-8')
                    except:
                        # If decompression fails, try to decode directly
                        try:
                            content_text = content.decode('utf-8', errors='replace')
                        except:
                            # Skip if we can't decode
                            return None
                else:
                    content_text = str(content)

                return row_id, content_text

            # Process in batches to populate FTS table
            batch_size = 25000
            processed = 0

            self.logger.info("Populating FTS table...")
            with tqdm(total=total_rows, desc="Indexing for FTS") as pbar:
                while processed < total_rows:
                    # Get batch of rows
                    archive_cursor.execute(
                        "SELECT id, content FROM paragraphs LIMIT ? OFFSET ?",
                        (batch_size, processed)
                    )
                    rows = archive_cursor.fetchall()

                    if not rows:
                        break

                    # Process rows in parallel for better performance
                    with ThreadPoolExecutor(max_workers=32) as executor:
                        results = list(executor.map(process_fts_row, rows))

                    # Insert into FTS table
                    batch_insert = []
                    for result in results:
                        if result:  # Skip None results
                            batch_insert.append(result)

                    if batch_insert:
                        try:
                            archive_conn.execute("BEGIN TRANSACTION")

                            archive_cursor.executemany(
                                "INSERT INTO paragraphs_fts(rowid, content) VALUES (?, ?)",
                                batch_insert
                            )
                            archive_conn.commit()

                            if processed % (self.batch_size * 2) == 0:
                                self.logger.info(f"Pausing to allow I/O queue to clear at {processed} records")
                                time.sleep(0.5)
                                disk_io = psutil.disk_io_counters()
                                if disk_io:
                                    time.sleep(0.5)

                            if len(batch_insert) > 10000:
                                self.logger.info(f"Successfully committed FTS batch of {len(batch_insert)} rows")
                        except sqlite3.Error as e:
                            self.logger.error(f"Error inserting into FTS: {e}")
                            try:
                                archive_conn.rollback()
                                self.logger.info("FTS transaction rolled back successfully")
                            except Exception as rollback_error:
                                self.logger.info(f"Error during FTS rollback: {rollback_error}")

                    # Commit batch
                    archive_conn.commit()
                    processed += len(rows)
                    pbar.update(len(rows))

                    # Periodic garbage collection
                    if processed % (batch_size * 10) == 0:
                        mem = psutil.virtual_memory()
                        self.logger.info(f"RAM usage: {mem.percent}%, Available: {mem.available/1024/1024/1024:.2f}GB")
                        gc.collect()

            # Optimize the FTS table
            self.logger.info("Optimizing FTS table...")
            archive_conn.execute("INSERT INTO paragraphs_fts(paragraphs_fts) VALUES('optimize')")
        else:
            self.logger.warning("No FTS table found in source database. Creating new one...")

            # Create a new FTS table from scratch
            archive_conn.execute('''CREATE VIRTUAL TABLE IF NOT EXISTS paragraphs_fts 
                                 USING fts5(content, content_rowid=id)''')

            # Create triggers
            archive_conn.executescript('''
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

            self.logger.info("Will populate FTS from paragraphs table...")

            # Continue with populating FTS identical to the code above
            archive_cursor = archive_conn.cursor()
            archive_cursor.execute("SELECT COUNT(*) FROM paragraphs")
            total_rows = archive_cursor.fetchone()[0]

            # Define the processing function (same as above)
            def process_fts_row(row_data):
                row_id, content = row_data
                content_text = None
                if isinstance(content, bytes):
                    try:
                        content_text = zlib.decompress(content).decode('utf-8')
                    except:
                        try:
                            content_text = content.decode('utf-8', errors='replace')
                        except:
                            return None
                else:
                    content_text = str(content)

                return row_id, content_text

            # Process in batches using the same logic as above
            batch_size = 25000
            processed = 0

            self.logger.info("Populating FTS table...")
            with tqdm(total=total_rows, desc="Indexing for FTS") as pbar:
                while processed < total_rows:
                    archive_cursor.execute(
                        "SELECT id, content FROM paragraphs LIMIT ? OFFSET ?",
                        (batch_size, processed)
                    )
                    rows = archive_cursor.fetchall()

                    if not rows:
                        break

                    with ThreadPoolExecutor(max_workers=32) as executor:
                        results = list(executor.map(process_fts_row, rows))

                    batch_insert = []
                    for result in results:
                        if result:
                            batch_insert.append(result)

                    if batch_insert:
                        try:
                            archive_cursor.executemany(
                                "INSERT INTO paragraphs_fts(rowid, content) VALUES (?, ?)",
                                batch_insert
                            )
                        except sqlite3.Error as e:
                            self.logger.error(f"Error inserting into FTS: {e}")

                    archive_conn.commit()
                    processed += len(rows)
                    pbar.update(len(rows))

                    if processed % (batch_size * 5) == 0:
                        mem = psutil.virtual_memory()
                        self.logger.info(f"RAM usage: {mem.percent}%, Available: {mem.available/1024/1024/1024:.2f}GB")
                        gc.collect()

            self.logger.info("Optimizing FTS table...")
            archive_conn.execute("INSERT INTO paragraphs_fts(paragraphs_fts) VALUES('optimize')")

        # Create optimized indices for common query patterns
        self.logger.info("Creating specialized indices for read-only access...")
        archive_conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_process_content ON paragraphs(process_number, document_date)")
        archive_conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_document_process ON paragraphs(document_date, process_number)")
        archive_conn.execute("CREATE INDEX IF NOT EXISTS idx_file_path ON paragraphs(file_path)")

        # Integrity check
        self.logger.info("Running integrity check before VACUUM...")
        skip_vacuum = False
        try:
            archive_cursor = archive_conn.cursor()
            archive_cursor.execute("PRAGMA integrity_check")
            result = archive_cursor.fetchall()

            if len(result) == 1 and result[0][0] == 'ok':
                self.logger.info("Integrity check passed. Database is healthy.")
            else:
                self.logger.warning("Integrity check failed. Database may be corrupted:")
                for error in result:
                    self.logger.warning(f"  - {error[0]}")
                self.logger.warning("Skipping VACUUM to avoid further issues.")
                skip_vacuum = True
        except Exception as e:
            self.logger.error(f"Error during integrity check: {e}")
            self.logger.warning("Skipping VACUUM due to integrity check error.")
            skip_vacuum = True
        else:
            skip_vacuum = False

        # Final vacuum for maximum compression - Must be done BEFORE setting read-only

        if not skip_vacuum:
            # Run VACUUM with error handling
            self.logger.info("Running final VACUUM...")
            archive_conn = sqlite3.connect(str(archive_db))
            archive_conn.execute("PRAGMA page_size = 32768")  # 32KB page size
            vacuum_cache_size = max(int(self.cache_size_mb * 750), 2000)
            archive_conn.execute(f"PRAGMA cache_size = -{vacuum_cache_size}")  # Reduce size for VACUUM
            archive_conn.execute("PRAGMA temp_store = MEMORY")
            archive_conn.execute("PRAGMA journal_mode = OFF")
            archive_conn.execute("PRAGMA mmap_size =0")

            vacuum_success = self.run_vacuum_with_fallback(archive_conn, archive_db)

            try:
                self.logger.info("Running ANALYZE to update statistics...")
                archive_conn.execute("ANALYZE")
                self.logger.info("ANALYZE completed successfully")
            except Exception as e:
                self.logger.warning(f"ANALYZE failed: {e}")
            try:
                archive_conn.commit()
                archive_conn.close()
            except:
                pass

        # Now set read-only pragmas AFTER VACUUM is complete and adjust settings
        self.logger.info("Setting read-only pragmas...")
        archive_conn = sqlite3.connect(str(archive_db))
        archive_conn.execute("PRAGMA mmap_size = 8589934592")  # 8GB for memory-mapped I/O
        archive_conn.execute("PRAGMA page_size = 32768")  # 32KB page size
        archive_conn.execute(f"PRAGMA cache_size = -{self.cache_size_mb * 1000}") # Dynamic cache
        archive_conn.execute("PRAGMA synchronous = OFF")
        archive_conn.execute("PRAGMA locking_mode = NORMAL")
        archive_conn.execute("PRAGMA query_only = ON")  # Mark as read-only

        # Close connections
        src_conn.close()
        archive_conn.close()

        # Reset any remaining connections
        self.logger.info("Ensuring all database connections are close...")
        temp_conn = sqlite3.connect(":memory:")
        temp_conn.execute("PRAGMA shrink_memory")
        temp_conn.close()
        gc.collect()

        # Force exclusive access to ensure no lingering connections
        self._close_all_connections(archive_db)

        # Pause to allow system to release resources
        time.sleep(3)

        # Log results
        archive_size = os.path.getsize(archive_db)
        self.logger.info(
            f"Read-optimized archive created with FTS support. Size: {archive_size / (1024 * 1024):.2f} MB")

    def create_compressed_archive(self, source_db, compressed_archive):
        """Create a compressed archive for long-term storage"""
        self.logger.info("\nStep 3: Creating compressed archive for long-term storage")

        # Completely reset all SQLite connections and resources
        self._reset_all_database_resources()

        # Make sure the source file exists and is accessible
        source_db_path = Path(source_db)
        if not source_db_path.exists():
            self.logger.error(f"Source database {source_db} does not exist")
            return False

        # Wait to ensure resources are freed
        self.logger.info("Waiting for system resources to be released...")
        time.sleep(30)

        # Check if 7zip is available at the specific path
        seven_zip_path = r"C:\Program Files\7-Zip\7z.exe"
        if os.path.exists(seven_zip_path):
            # Use 7zip for maximum compression
            self.logger.info("Using 7zip for compression...")

            try:
                # Format command for 7zip - use list instead of string to avoid shell problems
                cmd = [
                    seven_zip_path,
                    "a",
                    "-t7z",
                    "-m0=lzma2",
                    "-mx=9",
                    "-mfb=64",
                    "-md=128m",
                    "-ms=on",
                    "-mmt=8", # Add thread control for compression
                    str(compressed_archive),
                    str(source_db)
                ]

                # Run 7zip with explicit encoding settings
                self.logger.info("Running 7zip compression (this may take a while)...")
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding='utf-8',
                    errors='replace'
                )
                stdout, stderr = process.communicate()

                if process.returncode != 0:
                    self.logger.error(f"7zip compression failed: {stderr}")
                    self.logger.info("Falling back to built-in compression...")
                    self._create_builtin_archive(source_db, compressed_archive)
                else:
                    self.logger.info("7zip compression successful")
            except Exception as e:
                self.logger.error(f"Error using 7zip: {e}")
                self.logger.info("Falling back to built-in compression...")
                self._create_builtin_archive(source_db, compressed_archive)
        else:
            # Fall back to built-in compression
            self.logger.info("7zip not available at expected path, using built-in compression...")
            self._create_builtin_archive(source_db, compressed_archive)

        if os.path.exists(compressed_archive):
            # Log results
            archive_size = os.path.getsize(compressed_archive)
            self.logger.info(f"Compressed archive created. Size: {archive_size / (1024 * 1024):.2f} MB")
            return True
        else:
            self.logger.error("Failed to create compressed archive.")
            return False

    def _reset_all_database_resources(self):
        """Complete reset of all SQLite resources"""
        # Close any connections explicitly
        for conn in [c for c in gc.get_objects() if isinstance(c, sqlite3.Connection)]:
            try:
                conn.close()
            except:
                pass

        # Force garbage collection multiple times
        gc.collect()
        time.sleep(2)
        gc.collect()

        # Reset SQLite's internal state
        conn = sqlite3.connect(":memory:")
        conn.execute("PRAGMA shrink_memory")
        conn.close()

        # Additional sleep to ensure resources are released
        time.sleep(10)

        # This often helps flush memory on Windows
        ctypes.windll.kernel32.SetProcessWorkingSetSize(-1, -1) if 'ctypes' in sys.modules else None

    def _close_all_connections(self, db_path):
        """Force close all connections to a database file with multiple strategies"""

        # Strategy 1: Try exclusive access
        try:
            db_path_str = str(db_path).replace('\\','\\\\')
            conn = sqlite3.connect(f"file:{db_path_str}?mode=exclusive", uri=True)
            conn.close()
            self.logger.info(f"Successfully obtained exclusive access to {db_path}")
            return True
        except sqlite3.OperationalError:
            self.logger.warning(f"Could not get exclusive access to {db_path}, trying alternatives...")

        # Additional cleanup steps
        try:
            self.logger.info("Attempting to close any remaining connections")
            # Reset SQLite's internal state
            temp_conn = sqlite3.connect(":memory:")
            temp_conn.execute("PRAGMA shrink_memory")
            temp_conn.close()
            # Force garbage collection to release any connection objects
            gc.collect()
            # Waiut longer on Windows
            time.sleep(30)

            # Try again with the corrected syntax
            db_path_str = str(db_path)
            conn = sqlite3.connect(f"file:{db_path_str}?mode=exclusive", uri=True)
            conn.close()
            self.logger.info(f"Successfully obtained exclusive access to {db_path}")
            return True
        except sqlite3.OperationalError as e:
            self.logger.warning(f"Still could not get exlcusive acces: {e}")
            return False

    def _check_7zip(self):
        """Check if 7zip is available"""
        seven_zip_path = r"C:\Program Files\7-Zip\7z.exe"
        try:
            result = subprocess.run([seven_zip_path, "i"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return result.returncode == 0
        except (FileNotFoundError, subprocess.SubprocessError):
            return False

    def _create_builtin_archive(self, source_db, compressed_archive):
        """Create a compressed archive using built-in Python libraries"""
        try:
            import zipfile

            with zipfile.ZipFile(str(compressed_archive), 'w', compression=zipfile.ZIP_DEFLATED,
                                 compresslevel=9) as zipf:
                zipf.write(source_db, arcname=os.path.basename(str(source_db)))

            self.logger.info("Built-in compression completed successsfully")
        except Exception as e:
            self.logger.error(f"Error using built-in compression: {e}")

    def cleanup_temp_files(self):
        """Clean up temporary files after processing"""
        self.logger.info("Cleaning up temporary files...")
        try:
            # Attempt to remove all files in temp directory
            for file in self.temp_dir.glob("*"):
                try:
                    if file.is_file():
                        file.unlink()
                except Exception as e:
                    self.logger.warning(f"Could not remove temporary file {file}: {e}")

            # Try to remove the temp directory itself
            try:
                self.temp_dir.rmdir()
            except:
                pass
        except Exception as e:
            self.logger.error(f"Error cleaning up temporary files: {e}")

            # Try to remove the temp directory itself
            try:
                self.temp_dir.rmdir()
            except:
                pass

    def run_vacuum_with_fallback(self, conn, db_path):
        """Run VACUUM with fallback options to ensure it completes"""

        # First try direct vacuum
        try:
            self.logger.info("Attempting normal VACUUM operation...")
            conn.execute("PRAGMA busy_timeout = 60000")  # 60 seconds
            conn.execute("VACUUM")
            self.logger.info("VACUUM completed successfully")
            return True
        except sqlite3.OperationalError as e:
            self.logger.warning(f"Initial VACUUM failed: {e}")

        # Try with a completely new connection
        try:
            conn.close()
            gc.collect()
            time.sleep(20)

            # Simpler approach without the URI
            vacuum_conn = sqlite3.connect(str(db_path))
            vacuum_conn.execute("PRAGMA journal_mode = OFF")
            vacuum_conn.execute("PRAGMA synchronous = OFF")
            vacuum_conn.execute("PRAGMA temp_store = MEMORY")
            vacuum_conn.execute("VACUUM")
            vacuum_conn.close()
            self.logger.info("Simple reconnect VACUUM completed successfully")
            return True
        except sqlite3.OperationalError as e:
            self.logger.warning(f"Reconnect VACUUM failed: {e}")

        self.logger.warning("All VACUUM attempts failed. Continuing without VACUUM.")
        return False

def cleanup_resources():
    """Clean up resources after processing"""
    # Force garbage collection
    gc.collect()

    # Reset SQLite shared cache
    try:
        conn = sqlite3.connect(":memory:")
        conn.execute("PRAGMA shrink_memory")
        conn.close()
    except:
        pass

    # Log memory state after cleanup
    mem = psutil.virtual_memory()
    logging.info(f"Final RAM usage after cleanup: {mem.percent}%, Available: {mem.available/1024/1024/1024:.2f}GB")


def process_all_databases(base_dir, output_dir=None, batch_size=50000):
    """Process all databases in the directory"""
    base_path = Path(base_dir)
    db_files = list(base_path.glob('legal_documents_*.db'))

    logger = setup_logging()
    logger.info(f"Found {len(db_files)} database files to process")

    # Log initial system state
    mem = psutil.virtual_memory()
    logger.info(f"Initial RAM usage: {mem.percent}%, Available: {mem.available/1024/1024/1024:.2f}GB")

    results = {}

    for db_path in db_files:
        logger.info(f"\n=== Processing {db_path.name} ===")
        try:
            optimizer = StaticDatabaseOptimizer(
                db_path,
                output_dir=output_dir,
                batch_size=batch_size,
                logger=logger
            )
            result = optimizer.optimize()
            results[db_path.name] = result

            # Reset shared SQLite cache between database processing
            conn = sqlite3.connect(":memory:")
            conn.execute("PRAGMA shrink_memory")
            conn.close()

            # Force garbage collection between databases
            gc.collect()
        except Exception as e:
            logger.error(f"Error processing {db_path.name}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    # Print summary
    if results:
        logger.info("\n=== Optimization Summary ===")
        total_original = sum(r["original_size_mb"] for r in results.values())
        total_optimized = sum(r["optimized_size_mb"] for r in results.values())
        total_compressed = sum(r["compressed_size_mb"] for r in results.values() if r["compressed_size_mb"] > 0)

        logger.info(f"Total original size: {total_original:.2f} MB")
        logger.info(f"Total optimized size: {total_optimized:.2f} MB")

        if total_compressed > 0:
            logger.info(f"Total compressed size: {total_compressed:.2f} MB")
            logger.info(f"Average reduction: {(1 - total_compressed / total_original) * 100:.2f}%")
        else:
            logger.info(f"Average reduction (optimized only): {(1 - total_optimized / total_original) * 100:.2f}%")
    else:
        logger.error("No databases were successfully processed.")

    # Clean up resources
    cleanup_resources()
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Static Database Optimizer")
    parser.add_argument("--single", "-s", help="Path to a single database file to optimize")
    parser.add_argument("--directory", "-d", help="Directory containing multiple database files")
    parser.add_argument("--output", "-o", help="Output directory for optimized databases")
    parser.add_argument("--batch", "-b", type=int, default=50000, help="Batch size for processing (default: 1000)")
    args = parser.parse_args()

    print("Static Database Optimizer")
    print("------------------------")
    print("This tool optimizes static SQLite databases for read-only access and long-term storage.")
    print("WARNING: Backups will be created, but proceed with caution!")
    print()

    try:
        if args.single:
            db_path = args.single
            print(f"Processing single database: {db_path}")
            optimizer = StaticDatabaseOptimizer(db_path, output_dir=args.output, batch_size=args.batch)
            optimizer.optimize()
        elif args.directory:
            base_dir = args.directory
            print(f"Processing all databases in: {base_dir}")
            process_all_databases(base_dir, output_dir=args.output, batch_size=args.batch)
        else:
            # Interactive mode
            choice = input("Enter '1' to process a single database or '2' to process all databases: ")

            if choice == '1':
                db_path = input("Enter the full path to the database file: ")
                output_dir = input("Enter output directory (or leave blank for default): ")

                optimizer = StaticDatabaseOptimizer(
                    db_path,
                    output_dir=output_dir if output_dir else None,
                    batch_size=50000 # Using optimized batch size
                )
                optimizer.optimize()
            elif choice == '2':
                base_dir = input("Enter the directory containing database files: ")
                output_dir = input("Enter output directory (or leave blank for default): ")

                process_all_databases(
                    base_dir,
                    output_dir=output_dir if output_dir else None,
                    batch_size=50000 # Using optimized batch size
                )
            else:
                print("Invalid choice. Exiting.")
    finally:
        # Make sure to clean up resources even if an error occurs
        cleanup_resources()