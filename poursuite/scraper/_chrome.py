"""Shared Chrome/Selenium options for the scraper and the probes."""
from selenium import webdriver

# Default Chrome headless ships at 800x600, which collapses eSAJ into its
# stacked mobile layout. The production scraper happens to work because it
# targets specific element IDs that exist in both layouts, but the inventory
# walker walks structure and needs the desktop view. 1920x1080 clears every
# eSAJ breakpoint.
DESKTOP_WINDOW_SIZE = "1920,1080"

# Real-Chrome user agent. eSAJ may serve different responses by UA; matching
# a desktop Chrome string is the safe default for both scraping and probing.
DESKTOP_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def configure_chrome_options(headless: bool = True) -> webdriver.ChromeOptions:
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-logging")
    options.add_argument(f"--window-size={DESKTOP_WINDOW_SIZE}")
    options.add_argument(f"--user-agent={DESKTOP_USER_AGENT}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    return options
