import sqlite3
import csv
from time import sleep
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
import re
import zlib
import logging
from pathlib import Path
import concurrent.futures
import os
from datetime import datetime


@dataclass
class SearchResult:
    """Container for search results with metadata"""
    process_number: str
    content: str
    document_date: str
    file_path: str
    db_id: str


@dataclass
class DatabaseInfo:
    """Information about a database"""
    path: Path
    start_date: str
    end_date: str
    size_mb: float = 0.0


class MultiDBSearchEngine:
    """Handles searching through multiple databases with compression support"""

    def __init__(self, base_dir: str = '.'):
        """Initialize search engine with directory containing the databases"""
        self.base_dir = Path("D:/Poursuite/Databases")
        self.db_dir = self.base_dir
        self.setup_logging()
        self.db_cache = {}  # Cache for database connections
        self.db_info = self._discover_databases()

    def setup_logging(self):
        """Configure logging with both file and console output"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('search_engine.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _discover_databases(self) -> Dict[str, DatabaseInfo]:
        """Discover and validate all database files regardless of naming convention"""
        databases = {}

        if not self.db_dir.exists():
            self.logger.warning(f"Database directory {self.db_dir} not found")
            return databases

        # Find all .db files without requiring specific naming
        for db_path in sorted(self.db_dir.glob('*.db')):
            try:
                # Generate a unique ID for the database
                db_id = db_path.stem  # Use filename without extension as ID

                # Get database stats
                with sqlite3.connect(str(db_path)) as conn:
                    cursor = conn.cursor()

                    # Check if paragraphs table exists
                    cursor.execute("""
                        SELECT name FROM sqlite_master 
                        WHERE type='table' AND name='paragraphs'
                    """)
                    if not cursor.fetchone():
                        self.logger.warning(f"Database {db_path} missing paragraphs table, skipping")
                        continue

                    # Get date range
                    cursor.execute("""
                        SELECT MIN(document_date), MAX(document_date)
                        FROM paragraphs
                    """)
                    start_date, end_date = cursor.fetchone()

                    # Get file size
                    size_mb = os.path.getsize(db_path) / (1024 * 1024)

                    databases[db_id] = DatabaseInfo(
                        path=db_path,
                        start_date=start_date,
                        end_date=end_date,
                        size_mb=size_mb
                    )

                    self.logger.info(f"Found database {db_id}: {start_date} to {end_date}, {size_mb:.2f}MB")

            except Exception as e:
                self.logger.error(f"Error validating database {db_path}: {e}")

        self.logger.info(f"Discovered {len(databases)} valid databases")
        return databases

    def _get_db_connection(self, db_id: str) -> Optional[sqlite3.Connection]:
        """Get cached connection to a database"""
        if db_id not in self.db_info:
            return None

        if db_id not in self.db_cache:
            try:
                conn = sqlite3.connect(str(self.db_info[db_id].path))
                conn.row_factory = sqlite3.Row
                self.db_cache[db_id] = conn
            except Exception as e:
                self.logger.error(f"Error connecting to database {db_id}: {e}")
                return None

        return self.db_cache[db_id]

    def close_connections(self):
        """Close all open database connections"""
        for db_id, conn in self.db_cache.items():
            try:
                conn.close()
            except:
                pass
        self.db_cache = {}

    def _decompress_content(self, content) -> str:
        """Decompress content field if it's compressed"""
        if isinstance(content, bytes):
            try:
                return zlib.decompress(content).decode('utf-8')
            except:
                # If decompression fails, it might not be compressed
                return content.decode('utf-8', errors='replace')
        return content

    def _build_search_query(self,
                            keywords: Optional[str] = None,
                            process_number: Optional[str] = None,
                            start_date: Optional[str] = None,
                            end_date: Optional[str] = None) -> Tuple[str, List]:
        """Build SQL query based on search parameters"""
        conditions = []
        params = []

        if keywords and keywords.strip():
            tokens = re.findall(r'(?:"[^"]*"|\S)+', keywords)
            fts_terms = []

            for token in tokens:
                if token.upper() in ('AND', 'OR', 'NOT'):
                    fts_terms.append(token.upper())
                else:
                    # Handle quoted strings and regular terms
                    fts_terms.append(token)

            if fts_terms:
                conditions.append("""
                    id IN (
                        SELECT rowid 
                        FROM paragraphs_fts 
                        WHERE paragraphs_fts MATCH ?
                    )
                """)
                params.append(' '.join(fts_terms))

        # Process number search
        if process_number and process_number.strip():
            conditions.append("process_number LIKE ?")
            params.append(f"%{process_number}%")

        # Date range search
        if start_date and start_date.strip():
            conditions.append("document_date >= ?")
            params.append(start_date)

        if end_date and end_date.strip():
            conditions.append("document_date <= ?")
            params.append(end_date)

        # If no conditions were added, return all records
        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
            SELECT process_number, content, document_date, file_path
            FROM paragraphs
            WHERE {where_clause}
            ORDER BY document_date DESC
        """

        return query, params

    def _identify_relevant_databases(self, start_date: Optional[str], end_date: Optional[str]) -> List[str]:
        """Identify which databases are relevant to the search based on date range"""
        if not start_date and not end_date:
            # No date filter, search all databases
            return list(self.db_info.keys())

        relevant_dbs = []

        for db_id, info in self.db_info.items():
            if start_date and info.end_date < start_date:
                continue  # Database ends before search start date
            if end_date and info.start_date > end_date:
                continue  # Database starts after search end date
            relevant_dbs.append(db_id)

        return sorted(relevant_dbs)

    def _search_database(self,
                         db_id: str,
                         keywords: Optional[str] = None,
                         process_number: Optional[str] = None,
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None) -> Dict[str, List[SearchResult]]:
        """Search a single database"""
        results = defaultdict(list)
        conn = self._get_db_connection(db_id)

        if not conn:
            self.logger.warning(f"Could not connect to database {db_id}")
            return {}

        try:
            query, params = self._build_search_query(
                keywords, process_number, start_date, end_date
            )

            cursor = conn.cursor()
            cursor.execute(query, params)

            for row in cursor:
                # Decompress content
                content = self._decompress_content(row['content'])

                result = SearchResult(
                    process_number=row['process_number'],
                    content=content,
                    document_date=row['document_date'],
                    file_path=row['file_path'],
                    db_id=db_id
                )
                results[row['process_number']].append(result)

            return dict(results)

        except Exception as e:
            self.logger.error(f"Error searching database {db_id}: {e}")
            return {}

    def search(self,
               keywords: Optional[str] = None,
               process_number: Optional[str] = None,
               start_date: Optional[str] = None,
               end_date: Optional[str] = None,
               limit: Optional[int] = None,
               max_workers: int = 16) -> Dict[str, List[SearchResult]]:
        """
        Search across multiple databases based on provided criteria
        """
        if not self.db_info:
            self.logger.warning("No databases found to search")
            return {}

        # Identify which databases to search based on date range
        relevant_dbs = self._identify_relevant_databases(start_date, end_date)

        if not relevant_dbs:
            self.logger.info("No relevant databases found for the specified date range")
            return {}

        self.logger.info(f"Searching across {len(relevant_dbs)} databases")
        all_results = defaultdict(list)

        # Use thread pool to search databases in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(relevant_dbs), max_workers)) as executor:
            future_to_db = {
                executor.submit(self._search_database,
                                db_id,
                                keywords,
                                process_number,
                                start_date,
                                end_date): db_id
                for db_id in relevant_dbs
            }

            for future in concurrent.futures.as_completed(future_to_db):
                db_id = future_to_db[future]
                try:
                    db_results = future.result()

                    # Merge results
                    for process_num, mentions in db_results.items():
                        all_results[process_num].extend(mentions)

                except Exception as e:
                    self.logger.error(f"Error processing results for database {db_id}: {e}")

        # Sort results by date and apply limit if specified
        for process_num, mentions in all_results.items():
            # Sort by date (newest first)
            mentions.sort(key=lambda x: x.document_date, reverse=True)

            # Apply per-process limit if specified
            if limit:
                all_results[process_num] = mentions[:limit]

        return dict(all_results)

    def filter_processes(self, results: Dict[str, List[SearchResult]], exclusion_terms: str) -> Dict[
        str, List[SearchResult]]:
        """
        Filter out processes that contain any of the exclusion terms in any of their mentions

        Args:
            results: The original search results dictionary
            exclusion_terms: Space-separated terms to exclude with support for quoted phrases

        Returns:
            Filtered results dictionary
        """
        if not exclusion_terms.strip():
            return results

        # Parse exclusion terms, respecting quotes
        terms = []
        # Find all quoted phrases and individual words
        matches = re.findall(r'(?:"[^"]*"|\S+)', exclusion_terms)

        for match in matches:
            if match.startswith('"') and match.endswith('"'):
                # Remove quotes and add as a phrase
                terms.append(match[1:-1].lower())
            else:
                # Add as a single word
                terms.append(match.lower())

        filtered_results = {}

        for process_num, mentions in results.items():
            # Check if any mention contains any exclusion term
            exclude_process = False

            for mention in mentions:
                content_lower = mention.content.lower()
                if any(term in content_lower for term in terms):
                    exclude_process = True
                    break

            # Keep the process if it doesn't contain any exclusion term
            if not exclude_process:
                filtered_results[process_num] = mentions

        return filtered_results

    def get_results_summary(self, results: Dict[str, List[SearchResult]]) -> Dict:
        """Generate a summary of search results"""
        summary = {
            'total_processes': len(results),
            'total_mentions': sum(len(mentions) for mentions in results.values()),
            'date_range': {
                'earliest': None,
                'latest': None
            },
            'db_distribution': defaultdict(int),
            'process_counts': {
                process_num: len(mentions)
                for process_num, mentions in results.items()
            }
        }

        # Get date range and database distribution
        all_dates = []
        for mentions in results.values():
            for mention in mentions:
                all_dates.append(mention.document_date)
                summary['db_distribution'][mention.db_id] += 1

        if all_dates:
            summary['date_range']['earliest'] = min(all_dates)
            summary['date_range']['latest'] = max(all_dates)

        return summary

    def export_results_to_csv(self,
                              results: Dict[str, List[SearchResult]],
                              output_path: str,
                              include_summary: bool = True,
                              search_params: Optional[Dict] = None) -> None:
        """Export search results to CSV file"""

        # Ensure the output directory exists
        output_dir = Path("C:/Poursuite/SearchResults")
        output_dir.mkdir(parents=True, exist_ok=True)

        #Make sure the output_path is within the specified directory
        full_output_path = output_dir / output_path

        try:
            with open(full_output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)

                if include_summary:
                    summary = self.get_results_summary(results)
                    writer.writerow(['=== Search Results Summary ==='])

                    if search_params:
                        writer.writerow(['=== Search Parameters ==='])
                        for param_name, param_value in search_params.items():
                            if param_value:  # Only include non-empty parameters
                                writer.writerow([param_name, param_value])
                        writer.writerow([''])  # Add blank line after parameters

                    writer.writerow(['Total Processes', summary['total_processes']])
                    writer.writerow(['Total Mentions', summary['total_mentions']])

                    if summary['date_range']['earliest']:
                        writer.writerow(['Date Range',
                                         f"{summary['date_range']['earliest']} to {summary['date_range']['latest']}"])

                    writer.writerow(['=== Database Distribution ==='])
                    for db_id, count in sorted(summary['db_distribution'].items()):
                        writer.writerow([f'Database {db_id}', count])

                    writer.writerow([''])

                writer.writerow(['Process Number', 'Mention Count', 'Document Date',
                                 'Database', 'File Path', 'Content'])

                for process_num, mentions in results.items():
                    for idx, result in enumerate(mentions):
                        writer.writerow([
                            process_num,
                            f"{idx + 1}/{len(mentions)}",
                            result.document_date,
                            result.db_id,
                            result.file_path,
                            result.content
                        ])
            self.logger.info(f"Results exported to {full_output_path}")

        except IOError as e:
            self.logger.info(f"Error writing to CSV file: {e}")
            raise IOError(f"Error writing to CSV file: {e}")

    def get_database_stats(self) -> Dict:
        """Get statistics about all available databases"""
        stats = {
            'total_databases': len(self.db_info),
            'total_size_mb': sum(info.size_mb for info in self.db_info.values()),
            'date_range': {
                'earliest': None,
                'latest': None
            },
            'databases': {}
        }

        all_start_dates = []
        all_end_dates = []

        for db_id, info in self.db_info.items():
            if info.start_date:
                all_start_dates.append(info.start_date)
            if info.end_date:
                all_end_dates.append(info.end_date)

            # Get count info for this database
            conn = self._get_db_connection(db_id)
            if conn:
                try:
                    cursor = conn.cursor()

                    cursor.execute('SELECT COUNT(*) FROM paragraphs')
                    paragraph_count = cursor.fetchone()[0]

                    cursor.execute('SELECT COUNT(DISTINCT process_number) FROM paragraphs')
                    process_count = cursor.fetchone()[0]

                    stats['databases'][db_id] = {
                        'size_mb': info.size_mb,
                        'paragraphs': paragraph_count,
                        'processes': process_count,
                        'date_range': f"{info.start_date} to {info.end_date}" if info.start_date else "Unknown"
                    }
                except:
                    stats['databases'][db_id] = {
                        'size_mb': info.size_mb,
                        'paragraphs': 'Error',
                        'processes': 'Error',
                        'date_range': 'Error'
                    }

        if all_start_dates:
            stats['date_range']['earliest'] = min(all_start_dates)
        if all_end_dates:
            stats['date_range']['latest'] = max(all_end_dates)

        return stats


if __name__ == "__main__":

    # Initialize search engine once
    search_engine = MultiDBSearchEngine('.')

    # Wait for everything to print
    sleep(1)

    while True: # Main program loop
        print("\nSearch Engine Options:")
        print("1. Search by keywords")
        print("2. Search by process number")
        print("3. Show database statistics")
        print("4. Exit")

        choice = input("\nSelect an option (1-4): ").strip()

        if choice == "1":
            keywords = input("Enter keywords to search (use quotes for phrases, AND/OR/NOT for boolean): ")
            start_date = input("Enter start date (YYYY-MM-DD) or leave empty: ")
            end_date = input("Enter end date (YYYY-MM-DD) or leave empty: ")

            results = search_engine.search(
                keywords=keywords,
                start_date=start_date if start_date else None,
                end_date=end_date if end_date else None
            )

        elif choice == "2":
            process_number = input("Enter process number (full or partial): ")
            results = search_engine.search(process_number=process_number)

        elif choice == "3":
            stats = search_engine.get_database_stats()
            print("\n=== Database Statistics ===")
            print(f"Total databases: {stats['total_databases']}")
            print(f"Total size: {stats['total_size_mb']:.2f} MB")

            if stats['date_range']['earliest']:
                print(f"Overall date range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}")

            print("\nDatabase details:")
            for db_id, db_stats in sorted(stats['databases'].items()):
                print(f"Database {db_id}: {db_stats['size_mb']:.2f} MB, "
                      f"{db_stats['paragraphs']} paragraphs, "
                      f"{db_stats['processes']} processes, "
                      f"Range: {db_stats['date_range']}")
            continue # Skip to the next iteration of the loop

        elif choice == "4":
            print("Exiting...")
            # CLose connections before exiting
            search_engine.close_connections()
            break

        else:
            print("Invalid option. Please try again.")
            continue # Skip to the next iteration of the loop

        # Handle search results
        if 'results' in locals():
            summary = search_engine.get_results_summary(results)
            print(f"\nFound {summary['total_processes']} processes with {summary['total_mentions']} total mentions")

            # Create a dictionary with the search parameters
            search_params = {
                'Keywords': keywords if 'keywords' in locals() and keywords else "None",
                'Process Number': process_number if 'process_number' in locals() and process_number else "None",
                'Start Date': start_date if 'start_date' in locals() and start_date else "None",
                'End Date': end_date if 'end_date' in locals() and end_date else "None",
                'Search Time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            if summary['total_processes'] > 0:
                export = input("Export results to CSV? (y/n): ").lower()
                if export.startswith('y'):
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    filename = f"search_results_{timestamp}.csv"
                    search_engine.export_results_to_csv(results, filename, search_params=search_params)
                    print(f"Results exported to C:/Poursuite/SearchResults/{filename}")

                # Add second-layer filtering option
                second_layer = input("\nApply second-layer filtering at process level? (y/n): ").lower()
                if second_layer.startswith('y'):
                    print("\n==== Second-Layer Filtering ====")
                    print("This will exclude entire processes if ANY mention contains the terms you specify.")
                    exclusion_terms = input("Enter terms to exclude (space-separated): ")

                    # Apply second-layer filtering
                    filtered_results = search_engine.filter_processes(results, exclusion_terms)
                    filtered_summary = search_engine.get_results_summary(filtered_results)

                    # Show filtering results
                    print(f"\nFiltered results: {filtered_summary['total_processes']} processes with {filtered_summary['total_mentions']} total mentions")
                    print(f"Removed {summary['total_processes'] - filtered_summary['total_processes']} processes")

                    if filtered_summary['total_processes'] > 0:
                        export_filtered = input("Export filtered results to CSV? (y/n): ").lower()
                        if export_filtered.startswith('y'):
                            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                            filename = f"search_results_2L_{timestamp}.csv"

                            # Update search params with exclusion terms
                            search_params['Second Layer Exclusion Terms'] = exclusion_terms
                            search_params['Original Process Count'] = str(summary['total_processes'])
                            search_params['Removed Process Count'] = str(
                                summary['total_processes'] - filtered_summary['total_processes'])

                            search_engine.export_results_to_csv(filtered_results, filename, search_params=search_params)
                            #print(f"Filtered results exported to C:/Poursuite/SearchResults/{filename}")
                            sleep(0.5)

        # Ensure connections are closed when exiting
        search_engine.close_connections()