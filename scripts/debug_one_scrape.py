"""One-shot scrape repro with full Python traceback.

Usage:
    .venv/Scripts/python.exe scripts/debug_one_scrape.py 1033164-10.2022.8.26.0602
"""
from __future__ import annotations

import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from poursuite.scraper.esaj import ProcessValueScraper


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: debug_one_scrape.py <CNJ-formatted process number> [--headed]", file=sys.stderr)
        return 2

    pn = sys.argv[1]
    headed = "--headed" in sys.argv[2:]
    if headed:
        # Re-monkey patch configure_chrome_options for a headed run.
        import poursuite.scraper._chrome as cmod
        original = cmod.configure_chrome_options
        cmod.configure_chrome_options = lambda headless=False: original(headless=False)
        # Also patch the one already imported into esaj.py
        import poursuite.scraper.esaj as esaj_mod
        esaj_mod.configure_chrome_options = cmod.configure_chrome_options

    scraper = ProcessValueScraper(max_concurrent_browsers=1)
    print(f"Scraping {pn!r} ({'headed' if headed else 'headless'} Chrome, single thread)…")

    # Bypass the outer try/except in get_process_record so the real traceback
    # bubbles up. We replicate its happy path so we exercise the same code.
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    from bs4 import BeautifulSoup

    try:
        scraper._validate_process_number(pn)
        print("  ✓ validation OK")
        driver = scraper._get_driver()
        print("  ✓ driver acquired")

        from poursuite.config import ESAJ_URL
        driver.get(ESAJ_URL)
        print("  ✓ driver.get(ESAJ_URL)")
        print(f"     current_url  = {driver.current_url!r}")
        print(f"     page title   = {driver.title!r}")
        # Dump the page source so we can see what eSAJ actually served.
        dump = Path(__file__).resolve().parent / "_last_esaj_page.html"
        dump.write_text(driver.page_source, encoding="utf-8")
        print(f"     page source dumped to: {dump} ({len(driver.page_source):,} chars)")
        # Try to find the form field — same call _fill_process_form makes.
        form_field_count = len(driver.find_elements(By.ID, "numeroDigitoAnoUnificado"))
        print(f"     numeroDigitoAnoUnificado present? {form_field_count > 0}")
        if form_field_count == 0:
            # Headed mode pause so the user can inspect.
            if headed:
                input("     [headed] press ENTER after you've inspected the browser window… ")
        scraper._fill_process_form(driver, pn)
        print("  ✓ form filled + submitted")
        scraper._wait_for_results(driver)
        print("  ✓ results wait completed (or timed out gracefully)")

        try:
            mais = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href='#maisDetalhes']"))
            )
            driver.execute_script("arguments[0].click();", mais)
            print("  ✓ #maisDetalhes click")
        except TimeoutException:
            print("  ⚠ #maisDetalhes link not clickable (page may have different layout)")

        soup = BeautifulSoup(driver.page_source, "html.parser")
        data = scraper._extract_process_data(soup, pn)
        print(f"  → ProcessData: number={data.number!r} error={data.error!r}")
        print(f"     class_type={data.class_type!r} foro={data.foro!r} value={data.value!r}")
        print(f"     last_movement={data.last_movement!r} status={data.status!r}")

        # Section expand attempt
        from poursuite.scraper.sections import expand_section_collapsibles
        clicked = expand_section_collapsibles(driver, ids=("linkmovimentacoes",))
        print(f"  ✓ section expand attempted: {clicked}")

        from poursuite.scraper.movimentos import parse_movimentos, is_full_timeline
        page_soup = BeautifulSoup(driver.page_source, "html.parser")
        print(f"  is_full_timeline: {is_full_timeline(page_soup)}")
        movs = parse_movimentos(page_soup)
        print(f"  movimentos parsed: {len(movs)}")
    except Exception:
        print("FAIL — full traceback:")
        traceback.print_exc()
        return 1
    finally:
        scraper._cleanup_all_drivers()

    return 0


if __name__ == "__main__":
    sys.exit(main())
