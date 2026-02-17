import sqlite3
import os
import logging
from pathlib import Path
from datetime import datetime
import time
from tqdm import tqdm


def setup_logging(log_file='split_database.log'):
    """Configure logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


class DatabaseSplitter:
    """Split a large database into multiple smaller ones based on date ranges"""

    def __init__(self, source_db_path, output_dir=None, batch_size=10000):
        self.source_path = Path(source_db_path)
        self.output_dir = Path(output_dir) if output_dir else self.source_path.parent
        self.batch_size = batch_size
        self.logger = setup_logging()

        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def split_by_date_range(self, date_ranges, db_name_pattern="legal_documents_{}.db"):
        """
        Split database by specified date ranges

        Args:
            date_ranges: List of tuples with (start_date, end_date, identifier)
            db_name_pattern: Pattern for output database names
        """
        start_time = time.time()
        self.logger.info(f"Starting database split of {self.source_path}")

        # Create source connection
        try:
            source_conn = sqlite3.connect(str(self.source_path))
            source_conn.execute("PRAGMA journal_mode = OFF")
            source_conn.execute("PRAGMA synchronous = OFF")
            source_conn.execute("PRAGMA mmap_size = 8589934592")  # Use memory-mapped I/O
            source_cursor = source_conn.cursor()

            # Check if source database has the expected structure
            source_cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='paragraphs'")
            if source_cursor.fetchone()[0] == 0:
                self.logger.error("Source database does not have a 'paragraphs' table")
                source_conn.close()
                return False

            # Get the schema for the paragraphs table
            source_cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='paragraphs'")
            table_schema = source_cursor.fetchone()[0]

            # Get schema for FTS table if it exists
            source_cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='paragraphs_fts'")
            fts_schema = source_cursor.fetchone()
            if fts_schema:
                fts_schema = fts_schema[0]

            # Get schema for triggers
            trigger_schemas = {}
            source_cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name='paragraphs'")
            for name, sql in source_cursor.fetchall():
                trigger_schemas[name] = sql

            # Get schema for indices
            index_schemas = {}
            source_cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='paragraphs'")
            for name, sql in source_cursor.fetchall():
                index_schemas[name] = sql

            # Get total count for progress tracking
            source_cursor.execute("SELECT COUNT(*) FROM paragraphs")
            total_rows = source_cursor.fetchone()[0]
            self.logger.info(f"Total rows in source database: {total_rows}")

            # Process each date range
            for start_date, end_date, identifier in date_ranges:
                dest_db_name = db_name_pattern.format(identifier)
                dest_path = self.output_dir / dest_db_name

                self.logger.info(f"Creating {dest_path} for dates {start_date} to {end_date}")

                # Create destination database
                dest_conn = sqlite3.connect(str(dest_path))
                dest_conn.execute("PRAGMA journal_mode = OFF")
                dest_conn.execute("PRAGMA synchronous = OFF")
                dest_conn.execute("PRAGMA mmap_size = 8589934592")
                dest_conn.execute("PRAGMA page_size = 65536")  # 64KB pages
                dest_cursor = dest_conn.cursor()

                # Create paragraphs table
                dest_cursor.execute(table_schema)

                # Count rows in this date range
                source_cursor.execute(
                    "SELECT COUNT(*) FROM paragraphs WHERE document_date >= ? AND document_date <= ?",
                    (start_date, end_date)
                )
                range_rows = source_cursor.fetchone()[0]
                self.logger.info(f"Found {range_rows} rows in date range {start_date} to {end_date}")

                # Process in batches
                processed = 0
                with tqdm(total=range_rows, desc=f"Processing {identifier}") as pbar:
                    while True:
                        # Get batch of rows in this date range
                        source_cursor.execute(
                            """
                            SELECT id, process_number, content, file_path, document_date 
                            FROM paragraphs 
                            WHERE document_date >= ? AND document_date <= ?
                            LIMIT ? OFFSET ?
                            """,
                            (start_date, end_date, self.batch_size, processed)
                        )

                        rows = source_cursor.fetchall()
                        if not rows:
                            break

                        # Insert into destination database
                        dest_cursor.executemany(
                            """
                            INSERT INTO paragraphs (id, process_number, content, file_path, document_date)
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            rows
                        )

                        # Update progress
                        processed += len(rows)
                        pbar.update(len(rows))
                        dest_conn.commit()

                # Create indices (after data is inserted for performance)
                self.logger.info(f"Creating indices for {dest_path}")
                for name, sql in index_schemas.items():
                    dest_cursor.execute(sql)

                # Create FTS table if it existed in source
                if fts_schema:
                    self.logger.info(f"Creating FTS table for {dest_path}")
                    dest_cursor.execute(fts_schema)

                    # Create triggers for FTS
                    for name, sql in trigger_schemas.items():
                        dest_cursor.execute(sql)

                # Close destination connection
                dest_conn.commit()
                dest_conn.close()
                self.logger.info(f"Completed {dest_path} with {processed} rows")

            source_conn.close()

            # Log completion
            duration = time.time() - start_time
            self.logger.info(f"Database split completed in {duration:.2f} seconds")
            return True

        except Exception as e:
            self.logger.error(f"Error splitting database: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False


def split_2024_database():
    """Split the 2024 database into two half-year databases"""
    # Get source database path
    source_path = input("Enter the path to the 2024 database: ")
    output_dir = input("Enter output directory (press Enter to use the same directory): ")

    if not output_dir:
        output_dir = os.path.dirname(source_path)

    # Create database splitter
    splitter = DatabaseSplitter(source_path, output_dir)

    # Define date ranges for first and second half of 2024
    date_ranges = [
        ("2024-01-01", "2024-06-30", "2024_1"),  # First half
        ("2024-07-01", "2024-12-31", "2024_2")  # Second half
    ]

    # Run the splitter
    success = splitter.split_by_date_range(date_ranges)

    if success:
        print(f"\nDatabase successfully split into two parts:")
        print(f"1. legal_documents_2024_1.db (Jan-Jun 2024)")
        print(f"2. legal_documents_2024_2.db (Jul-Dec 2024)")
        print("\nYou can now run the optimization script on each of these databases.")
    else:
        print("\nDatabase split operation failed. Check the log for details.")


if __name__ == "__main__":
    split_2024_database()