import concurrent.futures
import csv
import logging
import re
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from poursuite.config import OUTPUT_DIR, DEFAULT_MAX_WORKERS, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from poursuite.db.connection import DatabaseManager
from poursuite.models import SearchResult, SearchPage
from poursuite.utils import setup_logging, decompress_content, sanitize_fts_query


class SearchEngine:
    """Handles searching across multiple databases with compression and pagination support."""

    def __init__(self, db_manager: DatabaseManager) -> None:
        self.db_manager = db_manager
        self.logger: logging.Logger = setup_logging("search_engine")

    def _build_search_query(
        self,
        keywords: Optional[str] = None,
        process_number: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Tuple[str, List]:
        """Build SQL query based on search parameters."""
        conditions = []
        params = []

        if keywords and keywords.strip():
            conditions.append("""
                id IN (
                    SELECT rowid
                    FROM paragraphs_fts
                    WHERE paragraphs_fts MATCH ?
                )
            """)
            params.append(sanitize_fts_query(keywords))

        if process_number and process_number.strip():
            conditions.append("process_number LIKE ?")
            params.append(f"%{process_number}%")

        if start_date and start_date.strip():
            conditions.append("document_date >= ?")
            params.append(start_date)

        if end_date and end_date.strip():
            conditions.append("document_date <= ?")
            params.append(end_date)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
            SELECT process_number, content, document_date, file_path
            FROM paragraphs
            WHERE {where_clause}
            ORDER BY document_date DESC
        """

        return query, params

    def _identify_relevant_databases(
        self,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> List[str]:
        """Identify which databases are relevant to the search based on date range."""
        if not start_date and not end_date:
            return list(self.db_manager.db_info.keys())

        relevant_dbs = []
        for db_id, info in self.db_manager.db_info.items():
            if start_date and info.end_date < start_date:
                continue
            if end_date and info.start_date > end_date:
                continue
            relevant_dbs.append(db_id)

        return sorted(relevant_dbs)

    def _search_database(
        self,
        db_id: str,
        keywords: Optional[str] = None,
        process_number: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        deadline: Optional[float] = None,
    ) -> Optional[Dict[str, List[SearchResult]]]:
        """
        Search a single database.
        Returns None if skipped due to deadline, {} if searched with no results,
        or a dict of results.
        """
        if deadline is not None and time.time() > deadline:
            self.logger.info(f"Skipping {db_id}: deadline exceeded")
            return None

        results = defaultdict(list)
        conn = self.db_manager.get_connection(db_id)

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
                content = decompress_content(row['content'])
                result = SearchResult(
                    process_number=row['process_number'],
                    content=content,
                    document_date=row['document_date'],
                    file_path=row['file_path'],
                    db_id=db_id,
                )
                results[row['process_number']].append(result)

            return dict(results)

        except Exception as e:
            self.logger.error(f"Error searching database {db_id}: {e}")
            return {}

    def search(
        self,
        keywords: Optional[str] = None,
        process_number: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        deadline: Optional[float] = None,
        max_workers: int = DEFAULT_MAX_WORKERS,
    ) -> SearchPage:
        """
        Search across all relevant databases in parallel.

        Args:
            keywords:       FTS keyword query (supports AND/OR/NOT and quoted phrases)
            process_number: Partial or full process number to filter by
            start_date:     Earliest document date (YYYY-MM-DD)
            end_date:       Latest document date (YYYY-MM-DD)
            page:           1-based page number
            page_size:      Results per page (capped at MAX_PAGE_SIZE)
            deadline:       Unix timestamp after which DB queries are skipped.
                            Pass None (CLI) for no timeout; pass time.time() + N (API) for a hard cutoff.
            max_workers:    Thread pool size

        Returns:
            SearchPage with paginated results and a truncated flag.
        """
        page_size = min(page_size, MAX_PAGE_SIZE)

        if not self.db_manager.db_info:
            self.logger.warning("No databases found to search")
            return SearchPage(results={}, total_processes=0, page=page, page_size=page_size)

        relevant_dbs = self._identify_relevant_databases(start_date, end_date)

        if not relevant_dbs:
            self.logger.info("No relevant databases found for the specified date range")
            return SearchPage(results={}, total_processes=0, page=page, page_size=page_size)

        self.logger.info(f"Searching across {len(relevant_dbs)} databases")
        all_results: Dict[str, List[SearchResult]] = defaultdict(list)
        skipped_count = 0

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(len(relevant_dbs), max_workers)
        ) as executor:
            future_to_db = {
                executor.submit(
                    self._search_database,
                    db_id, keywords, process_number, start_date, end_date, deadline
                ): db_id
                for db_id in relevant_dbs
            }

            for future in concurrent.futures.as_completed(future_to_db):
                db_id = future_to_db[future]
                try:
                    db_results = future.result()
                    if db_results is None:
                        skipped_count += 1
                    else:
                        for proc_num, mentions in db_results.items():
                            all_results[proc_num].extend(mentions)
                except Exception as e:
                    self.logger.error(f"Error processing results for database {db_id}: {e}")

        truncated = skipped_count > 0

        # Sort each process's mentions by date descending
        for mentions in all_results.values():
            mentions.sort(key=lambda x: x.document_date, reverse=True)

        # Sort processes by their most-recent mention date descending
        sorted_items = sorted(
            all_results.items(),
            key=lambda item: item[1][0].document_date if item[1] else "",
            reverse=True,
        )

        total_processes = len(sorted_items)
        offset = (page - 1) * page_size
        page_slice = sorted_items[offset: offset + page_size]

        return SearchPage(
            results=dict(page_slice),
            total_processes=total_processes,
            page=page,
            page_size=page_size,
            truncated=truncated,
        )

    def filter_processes(
        self,
        results: Dict[str, List[SearchResult]],
        exclusion_terms: str,
    ) -> Dict[str, List[SearchResult]]:
        """
        Filter out processes where any mention contains any of the exclusion terms.

        Args:
            results:         Search results dict
            exclusion_terms: Space-separated terms; quoted phrases are treated as single terms

        Returns:
            Filtered results dict
        """
        if not exclusion_terms.strip():
            return results

        terms = []
        for match in re.findall(r'(?:"[^"]*"|\S+)', exclusion_terms):
            if match.startswith('"') and match.endswith('"'):
                terms.append(match[1:-1].lower())
            else:
                terms.append(match.lower())

        filtered = {}
        for proc_num, mentions in results.items():
            exclude = False
            for mention in mentions:
                content_lower = mention.content.lower()
                if any(term in content_lower for term in terms):
                    exclude = True
                    break
            if not exclude:
                filtered[proc_num] = mentions

        return filtered

    def get_results_summary(self, results: Dict[str, List[SearchResult]]) -> Dict:
        """Generate a summary of search results."""
        summary = {
            'total_processes': len(results),
            'total_mentions': sum(len(mentions) for mentions in results.values()),
            'date_range': {'earliest': None, 'latest': None},
            'db_distribution': defaultdict(int),
            'process_counts': {
                proc_num: len(mentions)
                for proc_num, mentions in results.items()
            },
        }

        all_dates = []
        for mentions in results.values():
            for mention in mentions:
                all_dates.append(mention.document_date)
                summary['db_distribution'][mention.db_id] += 1

        if all_dates:
            summary['date_range']['earliest'] = min(all_dates)
            summary['date_range']['latest'] = max(all_dates)

        return summary

    def export_results_to_csv(
        self,
        results: Dict[str, List[SearchResult]],
        output_path: str,
        include_summary: bool = True,
        search_params: Optional[Dict] = None,
    ) -> None:
        """Export search results to a CSV file in OUTPUT_DIR."""
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        full_output_path = OUTPUT_DIR / output_path

        try:
            with open(full_output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)

                if include_summary:
                    summary = self.get_results_summary(results)
                    writer.writerow(['=== Search Results Summary ==='])

                    if search_params:
                        writer.writerow(['=== Search Parameters ==='])
                        for param_name, param_value in search_params.items():
                            if param_value:
                                writer.writerow([param_name, param_value])
                        writer.writerow([''])

                    writer.writerow(['Total Processes', summary['total_processes']])
                    writer.writerow(['Total Mentions', summary['total_mentions']])

                    if summary['date_range']['earliest']:
                        writer.writerow([
                            'Date Range',
                            f"{summary['date_range']['earliest']} to {summary['date_range']['latest']}"
                        ])

                    writer.writerow(['=== Database Distribution ==='])
                    for db_id, count in sorted(summary['db_distribution'].items()):
                        writer.writerow([f'Database {db_id}', count])

                    writer.writerow([''])

                writer.writerow([
                    'Process Number', 'Mention Count', 'Document Date',
                    'Database', 'File Path', 'Content'
                ])

                for proc_num, mentions in results.items():
                    for idx, result in enumerate(mentions):
                        writer.writerow([
                            proc_num,
                            f"{idx + 1}/{len(mentions)}",
                            result.document_date,
                            result.db_id,
                            result.file_path,
                            result.content,
                        ])

            self.logger.info(f"Results exported to {full_output_path}")

        except IOError as e:
            self.logger.error(f"Error writing to CSV file: {e}")
            raise IOError(f"Error writing to CSV file: {e}")
