"""
Poursuite CLI — single entry point replacing NewSearchEngine.__main__ and ExtractDataBatch.main().

All input() and print() calls in the project live here.
Scraping (eSAJ) is available as an optional post-search step.
"""

import sys
from dataclasses import fields
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from tabulate import tabulate

from poursuite.config import DEFAULT_BATCH_SIZE, DEFAULT_MAX_BROWSERS, ESAJ_OUTPUT_DIR
from poursuite.db.connection import DatabaseManager
from poursuite.db.search import SearchEngine
from poursuite.models import ProcessData, SearchResult


def main() -> None:
    """Entry point registered in pyproject.toml."""
    db_manager = DatabaseManager()
    search_engine = SearchEngine(db_manager)
    try:
        _search_loop(search_engine)
    finally:
        db_manager.close_connections()


# ---------------------------------------------------------------------------
# Main menu loop
# ---------------------------------------------------------------------------

def _search_loop(search_engine: SearchEngine) -> None:
    while True:
        print("\n=== Poursuite ===")
        print("1. Search by keywords")
        print("2. Search by process number")
        print("3. Show database statistics")
        print("4. Scrape eSAJ data from CSV")
        print("5. Exit")

        choice = input("\nSelect an option (1-5): ").strip()

        if choice == "1":
            _handle_keyword_search(search_engine)
        elif choice == "2":
            _handle_process_search(search_engine)
        elif choice == "3":
            _handle_stats(search_engine)
        elif choice == "4":
            _handle_scrape_from_csv()
        elif choice == "5":
            print("Exiting...")
            break
        else:
            print("Invalid option. Please try again.")


# ---------------------------------------------------------------------------
# Search handlers
# ---------------------------------------------------------------------------

def _handle_keyword_search(search_engine: SearchEngine) -> None:
    keywords = input("Enter keywords (use quotes for phrases, AND/OR/NOT for boolean): ").strip()
    start_date = input("Start date (YYYY-MM-DD) or leave empty: ").strip() or None
    end_date = input("End date (YYYY-MM-DD) or leave empty: ").strip() or None

    search_params = {
        'Keywords': keywords or "None",
        'Start Date': start_date or "None",
        'End Date': end_date or "None",
        'Search Time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }

    page_result = search_engine.search(
        keywords=keywords or None,
        start_date=start_date,
        end_date=end_date,
        deadline=None,
    )

    _handle_search_results(search_engine, page_result.results, search_params)


def _handle_process_search(search_engine: SearchEngine) -> None:
    process_number = input("Enter process number (full or partial): ").strip()

    search_params = {
        'Process Number': process_number,
        'Search Time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }

    page_result = search_engine.search(
        process_number=process_number,
        deadline=None,
    )

    _handle_search_results(search_engine, page_result.results, search_params)


def _handle_search_results(
    search_engine: SearchEngine,
    results: Dict[str, List[SearchResult]],
    search_params: Dict,
) -> None:
    summary = search_engine.get_results_summary(results)
    print(f"\nFound {summary['total_processes']} processes with {summary['total_mentions']} total mentions")

    if summary['total_processes'] == 0:
        return

    # --- Export to CSV ---
    if input("Export results to CSV? (y/n): ").lower().startswith('y'):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"search_results_{timestamp}.csv"
        search_engine.export_results_to_csv(results, filename, search_params=search_params)
        from poursuite.config import OUTPUT_DIR
        print(f"Results exported to {OUTPUT_DIR / filename}")

    # --- Second-layer filter ---
    if input("\nApply second-layer filtering? (y/n): ").lower().startswith('y'):
        print("This excludes entire processes if ANY mention contains the specified terms.")
        exclusion_terms = input("Enter terms to exclude (space-separated, quotes for phrases): ").strip()

        filtered = search_engine.filter_processes(results, exclusion_terms)
        filtered_summary = search_engine.get_results_summary(filtered)

        print(f"\nFiltered: {filtered_summary['total_processes']} processes "
              f"({summary['total_processes'] - filtered_summary['total_processes']} removed)")

        if filtered_summary['total_processes'] > 0:
            if input("Export filtered results to CSV? (y/n): ").lower().startswith('y'):
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"search_results_2L_{timestamp}.csv"
                search_params_filtered = {
                    **search_params,
                    'Second Layer Exclusion Terms': exclusion_terms,
                    'Original Process Count': str(summary['total_processes']),
                    'Removed Process Count': str(
                        summary['total_processes'] - filtered_summary['total_processes']
                    ),
                }
                search_engine.export_results_to_csv(
                    filtered, filename, search_params=search_params_filtered
                )
                from poursuite.config import OUTPUT_DIR
                print(f"Filtered results exported to {OUTPUT_DIR / filename}")

            results = filtered  # Use filtered results for eSAJ scraping below

    # --- eSAJ scraping ---
    if input("\nScrape eSAJ data for these processes? (y/n): ").lower().startswith('y'):
        _handle_scrape_from_results(list(results.keys()))


# ---------------------------------------------------------------------------
# Database stats
# ---------------------------------------------------------------------------

def _handle_stats(search_engine: SearchEngine) -> None:
    stats = search_engine.db_manager.get_database_stats()
    print("\n=== Database Statistics ===")
    print(f"Total databases: {stats['total_databases']}")
    print(f"Total size: {stats['total_size_mb']:.2f} MB")

    if stats['date_range']['earliest']:
        print(f"Overall date range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}")

    print("\nDatabase details:")
    for db_id, db_stats in sorted(stats['databases'].items()):
        print(f"  {db_id}: {db_stats['size_mb']:.2f} MB, {db_stats['date_range']}")


# ---------------------------------------------------------------------------
# eSAJ scraping
# ---------------------------------------------------------------------------

def _handle_scrape_from_csv() -> None:
    from poursuite.scraper.csv_extractor import CSVProcessExtractor

    csv_path = input("\nEnter path to CSV file: ").strip()
    if not csv_path:
        print("No file path provided.")
        return

    extractor = CSVProcessExtractor()
    try:
        process_numbers = list(extractor.extract_from_csv(csv_path))
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return

    if not process_numbers:
        print("No process numbers found in the CSV file.")
        return

    print(f"\nFound {len(process_numbers)} process numbers.")
    sample = process_numbers[:5]
    suffix = f" (and {len(process_numbers) - 5} more...)" if len(process_numbers) > 5 else ""
    print(f"Sample: {', '.join(sample)}{suffix}")

    if not input(f"\nProceed? (y/n): ").strip().lower().startswith('y'):
        print("Operation cancelled.")
        return

    _handle_scrape_from_results(process_numbers)


def _handle_scrape_from_results(process_numbers: List[str]) -> None:
    from poursuite.scraper.esaj import ProcessValueScraper

    batch_size = _prompt_int(f"Batch size (default {DEFAULT_BATCH_SIZE}): ", DEFAULT_BATCH_SIZE)
    browser_count = _prompt_int(
        f"Concurrent browsers (default {DEFAULT_MAX_BROWSERS}): ", DEFAULT_MAX_BROWSERS
    )

    if browser_count > 8:
        print("Warning: too many browsers may cause system instability.")
        if not input("Continue? (y/n): ").strip().lower().startswith('y'):
            browser_count = DEFAULT_MAX_BROWSERS

    scraper = ProcessValueScraper(max_concurrent_browsers=browser_count)
    try:
        results = scraper.process_batch(process_numbers, batch_size=batch_size)
        _display_scrape_results(results)
        _save_scrape_results(results)
    finally:
        del scraper


def _display_scrape_results(results: List[ProcessData]) -> None:
    if not results:
        print("No results to display.")
        return

    successful = sum(1 for r in results if not r.error)
    errors = sum(1 for r in results if r.error)

    print(f"\nResults Summary:")
    print(f"  Total:      {len(results)}")
    print(f"  Successful: {successful}")
    print(f"  Errors:     {errors}")

    display_count = min(5, len(results))
    headers = ProcessData.get_headers()
    table_data = [
        [getattr(r, f.name) for f in fields(ProcessData)]
        for r in results[:display_count]
    ]

    print(f"\nShowing first {display_count} result(s):")
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

    if len(results) > display_count:
        print(f"... and {len(results) - display_count} more not shown.")


def _save_scrape_results(results: List[ProcessData]) -> None:
    import pandas as pd

    if not results:
        print("No results to save.")
        return

    if not input("\nSave results to CSV? (y/n): ").strip().lower().startswith('y'):
        print("Results not saved.")
        return

    df = pd.DataFrame([r.to_dict() for r in results])
    timestamp = datetime.now().strftime('%Y_%m_%d_%H%M%S')
    filepath = ESAJ_OUTPUT_DIR / f"eSAJ_final_{timestamp}.csv"
    ESAJ_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(filepath, index=False, encoding='utf-8-sig')
    print(f"Results saved to: {filepath}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _prompt_int(prompt: str, default: int) -> int:
    raw = input(prompt).strip()
    try:
        return int(raw) if raw else default
    except ValueError:
        print(f"Invalid input, using default: {default}")
        return default


if __name__ == "__main__":
    main()
