import sqlite3
import os
import time
import logging
import argparse
import sys
import gc
import psutil
from pathlib import Path
import shutil


def setup_logging(log_file=None):
    """Configure logging"""
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding='utf-8'))

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    return logging.getLogger("vacuum_tool")


class DatabaseVacuum:
    """A focused tool for running VACUUM on SQLite databases with search optimization"""

    def __init__(self, db_path, logger=None, optimize_for_search=True):
        self.db_path = Path(db_path)
        self.logger = logger or setup_logging()
        self.optimize_for_search = optimize_for_search

        # Get original file size
        self.original_size = os.path.getsize(self.db_path) if self.db_path.exists() else 0

        # Create temporary directory
        self.temp_dir = self.db_path.parent / "temp_vacuum"
        self.temp_dir.mkdir(exist_ok=True, parents=True)

        # Set page and cache size based on system memory
        system_memory_gb = psutil.virtual_memory().total / (1024 ** 3)
        self.cache_size_mb = min(int(system_memory_gb * 250), 8000)  # Use at most 8GB
        self.logger.info(f"System memory: {system_memory_gb:.2f} GB")
        self.logger.info(f"Using cache size: {self.cache_size_mb} MB")

        # Default page size for optimization (32KB is good for larger DBs)
        self.page_size = 32768

    def vacuum_database(self):
        """Run vacuum on the database file with multiple strategies"""
        if not self.db_path.exists():
            self.logger.error(f"Database file {self.db_path} does not exist")
            return False

        self.logger.info(f"Starting VACUUM process on {self.db_path}")
        self.logger.info(f"Original size: {self.original_size / (1024 * 1024):.2f} MB")

        # Force cleanup before beginning
        self._cleanup_resources()

        # Create a backup of the original file
        backup_path = self._create_backup()

        # Try with direct approach first
        if self._try_direct_vacuum():
            # Set read-only optimizations after successful VACUUM
            self._configure_for_search_optimization()
            self._log_results()
            return True

        # If direct approach fails, try with a copy-based approach
        self.logger.info("Direct VACUUM failed, trying alternative approach...")
        if self._try_copy_vacuum():
            # Set read-only optimizations after successful VACUUM
            self._configure_for_search_optimization()
            self._log_results()
            return True

        # If all approaches fail, restore from backup
        self.logger.error("All VACUUM attempts failed, restoring from backup")
        try:
            shutil.copy2(backup_path, self.db_path)
            self.logger.info("Restored original database from backup")
        except Exception as e:
            self.logger.error(f"Failed to restore backup: {e}")

        return False

    def _create_backup(self):
        """Create a backup of the original database"""
        backup_path = self.temp_dir / f"{self.db_path.stem}_backup{self.db_path.suffix}"
        try:
            self.logger.info(f"Creating backup at {backup_path}")
            shutil.copy2(self.db_path, backup_path)
            return backup_path
        except Exception as e:
            self.logger.error(f"Failed to create backup: {e}")
            return None

    def _try_direct_vacuum(self):
        """Attempt a direct VACUUM on the database file"""
        try:
            self.logger.info("Trying direct VACUUM approach...")

            # Release all existing resources
            self._cleanup_resources()
            time.sleep(2)

            # Create a new connection with optimal settings for VACUUM
            vacuum_conn = sqlite3.connect(str(self.db_path))
            vacuum_conn.execute(f"PRAGMA cache_size = -{self.cache_size_mb * 1000}")
            vacuum_conn.execute("PRAGMA temp_store = MEMORY")
            vacuum_conn.execute("PRAGMA journal_mode = DELETE")  # DELETE is safer for VACUUM than OFF
            vacuum_conn.execute("PRAGMA synchronous = OFF")
            vacuum_conn.execute("PRAGMA busy_timeout = 300000")  # 5 minute timeout

            # Execute VACUUM
            self.logger.info("Executing VACUUM command...")
            start_time = time.time()
            vacuum_conn.execute("VACUUM")
            vacuum_conn.execute("ANALYZE")
            elapsed = time.time() - start_time

            # Close the connection properly
            vacuum_conn.close()
            vacuum_conn = None

            self.logger.info(f"Direct VACUUM completed successfully in {elapsed:.2f} seconds")
            return True

        except sqlite3.OperationalError as e:
            self.logger.warning(f"Direct VACUUM failed: {e}")
            # Try to close any connection that might still be open
            try:
                if 'vacuum_conn' in locals() and vacuum_conn:
                    vacuum_conn.close()
            except:
                pass
            return False

        except Exception as e:
            self.logger.error(f"Unexpected error during direct VACUUM: {e}")
            return False

    def _try_copy_vacuum(self):
        """Try VACUUM by copying to a new database file"""
        try:
            self.logger.info("Trying copy-based VACUUM approach...")

            # Release all existing resources
            self._cleanup_resources()
            time.sleep(5)

            # Create a temporary destination file
            temp_db = self.temp_dir / f"vacuum_temp_{int(time.time())}.db"

            # Use a separate connection for the source database
            src_conn = sqlite3.connect(str(self.db_path))
            src_conn.execute("PRAGMA query_only = ON")  # Read-only mode

            # Create and configure destination database
            dest_conn = sqlite3.connect(str(temp_db))
            dest_conn.execute(f"PRAGMA cache_size = -{self.cache_size_mb * 1000}")
            dest_conn.execute("PRAGMA page_size = 32768")  # 32KB page size
            dest_conn.execute("PRAGMA temp_store = MEMORY")
            dest_conn.execute("PRAGMA journal_mode = DELETE")
            dest_conn.execute("PRAGMA synchronous = OFF")

            # Use backup API to copy the database efficiently
            self.logger.info(f"Copying database to temporary file: {temp_db}")
            start_time = time.time()
            src_conn.backup(dest_conn)

            # Close source connection as we no longer need it
            src_conn.close()
            src_conn = None

            # Now run VACUUM on the new database
            self.logger.info("Running VACUUM on the copied database...")
            dest_conn.execute("VACUUM")
            dest_conn.execute("ANALYZE")

            # If we're not going to apply read-only optimizations later, do them here
            if self.optimize_for_search:
                self.logger.info("Setting initial read-optimized parameters...")
                # Memory-mapped I/O
                dest_conn.execute("PRAGMA mmap_size = 8589934592")
                # Optimal page size (may already be set)
                dest_conn.execute(f"PRAGMA page_size = {self.page_size}")
                # Cache settings
                dest_conn.execute(f"PRAGMA cache_size = -{self.cache_size_mb * 1000}")
                # Read-only settings
                dest_conn.execute("PRAGMA journal_mode = OFF")
                dest_conn.execute("PRAGMA synchronous = OFF")

            # Close destination connection
            dest_conn.close()
            dest_conn = None

            # Make sure all connections are closed
            self._cleanup_resources()
            time.sleep(2)

            # Replace the original with the vacuumed copy
            self.logger.info("Replacing original database with vacuumed copy...")

            # On Windows, we need to retry file operations that might fail due to timing issues
            max_attempts = 5
            for attempt in range(max_attempts):
                try:
                    # Rename original file first to avoid data loss if replacement fails
                    temp_original = self.temp_dir / f"original_{int(time.time())}.db"
                    self.db_path.rename(temp_original)

                    # Move the vacuumed file to the original location
                    temp_db.rename(self.db_path)

                    # Success - remove the old file
                    temp_original.unlink()

                    elapsed = time.time() - start_time
                    self.logger.info(f"Copy-based VACUUM completed successfully in {elapsed:.2f} seconds")
                    return True

                except PermissionError:
                    if attempt < max_attempts - 1:
                        self.logger.warning(
                            f"Permission error during file replacement, retrying in 5 seconds (attempt {attempt + 1}/{max_attempts})")
                        time.sleep(5)
                    else:
                        raise

            return False

        except Exception as e:
            self.logger.error(f"Copy-based VACUUM failed: {e}")
            return False

    def _cleanup_resources(self):
        """Force cleanup of database connections and memory"""
        self.logger.info("Cleaning up resources...")

        # Close any SQLite connections we can find
        # This is a best-effort approach to find any open connections in the current process
        for obj in gc.get_objects():
            if isinstance(obj, sqlite3.Connection):
                try:
                    obj.close()
                except:
                    pass

        # Force garbage collection
        gc.collect()

        # Create and close a memory database to reset SQLite's shared state
        try:
            temp_conn = sqlite3.connect(":memory:")
            temp_conn.execute("PRAGMA shrink_memory")
            temp_conn.close()
        except:
            pass

        # Force another garbage collection
        gc.collect()

    def _configure_for_search_optimization(self):
        """Configure the database for optimal read-only search performance"""
        if not self.optimize_for_search:
            return

        try:
            self.logger.info("Configuring database for optimal search performance...")

            # Clean up any existing connections first
            self._cleanup_resources()
            time.sleep(2)

            # Open a new connection to set read-only optimizations
            conn = sqlite3.connect(str(self.db_path))

            # First apply all optimizations that can modify the database

            # Analyze for query planning optimization - do this first as it modifies the db
            self.logger.info("Running ANALYZE for query optimization...")
            conn.execute("ANALYZE")
            conn.commit()

            # Set all performance PRAGMAs that don't make the database read-only
            self.logger.info("Setting performance optimizations...")

            # Memory-mapped I/O significantly improves read performance
            conn.execute("PRAGMA mmap_size = 8589934592")  # 8GB for memory-mapped I/O

            # Page size should already be set, but ensure it's correct
            conn.execute(f"PRAGMA page_size = {self.page_size}")

            # Set optimal cache size
            conn.execute(f"PRAGMA cache_size = -{self.cache_size_mb * 1000}")

            # Read-only optimizations that don't prevent writes
            conn.execute("PRAGMA journal_mode = OFF")  # No journaling needed for read-only
            conn.execute("PRAGMA synchronous = OFF")  # No need to sync for read-only
            conn.execute("PRAGMA locking_mode = NORMAL")
            conn.execute("PRAGMA temp_store = MEMORY")  # Use memory for temp storage

            # Commit all changes before setting to read-only
            conn.commit()

            # Finally, set query_only mode as the very last operation
            self.logger.info("Setting database to query-only mode...")
            conn.execute("PRAGMA query_only = ON")  # Mark as read-only - must be LAST

            # Close connection
            conn.close()

            self.logger.info("Database successfully configured for optimal search performance")

        except Exception as e:
            self.logger.error(f"Error configuring database for search: {e}")
            self.logger.info("Database was vacuumed successfully, but search optimization failed")

    def _log_results(self):
        """Log the results of the VACUUM operation"""
        if self.db_path.exists():
            new_size = os.path.getsize(self.db_path)
            reduction = self.original_size - new_size
            percent_reduction = (reduction / self.original_size) * 100 if self.original_size > 0 else 0

            self.logger.info("=== VACUUM Results ===")
            self.logger.info(f"Original size: {self.original_size / (1024 * 1024):.2f} MB")
            self.logger.info(f"New size: {new_size / (1024 * 1024):.2f} MB")
            self.logger.info(f"Reduction: {reduction / (1024 * 1024):.2f} MB ({percent_reduction:.2f}%)")

            if self.optimize_for_search:
                self.logger.info("Database is configured for optimal search performance")
        else:
            self.logger.error("Cannot get results: database file does not exist")

    def cleanup_temp_files(self):
        """Clean up temporary files"""
        try:
            self.logger.info("Cleaning up temporary files...")
            for file_path in self.temp_dir.glob("*"):
                try:
                    if file_path.is_file():
                        file_path.unlink()
                except Exception as e:
                    self.logger.warning(f"Could not remove temp file {file_path}: {e}")

            try:
                self.temp_dir.rmdir()
                self.logger.info("Temporary directory removed")
            except:
                self.logger.warning("Could not remove temporary directory")

        except Exception as e:
            self.logger.error(f"Error cleaning up temporary files: {e}")


def main():
    parser = argparse.ArgumentParser(description="SQLite Database VACUUM Tool")
    parser.add_argument("db_path", help="Path to the SQLite database file")
    parser.add_argument("--log", help="Path to log file (optional)")
    parser.add_argument("--keep-temp", action="store_true", help="Keep temporary files (for debugging)")
    parser.add_argument("--no-search-optimize", action="store_true",
                        help="Skip search optimization (don't configure for read-only search)")
    parser.add_argument("--page-size", type=int, default=32768,
                        help="Page size in bytes (default: 32768)")

    args = parser.parse_args()

    logger = setup_logging(args.log)

    # Log system information
    logger.info("=== SQLite Database VACUUM Tool ===")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"SQLite version: {sqlite3.sqlite_version}")

    mem = psutil.virtual_memory()
    logger.info(
        f"System memory: {mem.total / (1024 ** 3):.2f} GB total, {mem.available / (1024 ** 3):.2f} GB available")

    try:
        vacuum_tool = DatabaseVacuum(
            args.db_path,
            logger,
            optimize_for_search=not args.no_search_optimize
        )

        # Set page size if specified
        if args.page_size:
            vacuum_tool.page_size = args.page_size
            logger.info(f"Using custom page size: {args.page_size} bytes")

        success = vacuum_tool.vacuum_database()

        if not args.keep_temp:
            vacuum_tool.cleanup_temp_files()

        if success:
            logger.info("VACUUM operation completed successfully")
            return 0
        else:
            logger.error("VACUUM operation failed")
            return 1

    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())