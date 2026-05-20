"""Verify every URL referenced in spa_v2.html maps to a registered route."""
import re
from pathlib import Path

from poursuite.api.main import app

spa = Path(__file__).resolve().parent.parent / "poursuite" / "api" / "routes" / "spa_v2.html"
text = spa.read_text(encoding="utf-8")

urls = set()
# apiFetch('/some/url') or apiFetch(`/some/url`) — first arg only.
for m in re.finditer(r"""apiFetch\(\s*['"`]([^'"`?$\\{}]+)""", text):
    urls.add(m.group(1))
# direct fetch('/some/url')
for m in re.finditer(r"""\bfetch\(\s*['"`]([^'"`?$\\{}]+)""", text):
    urls.add(m.group(1))

route_paths = [r.path for r in app.routes if hasattr(r, "path")]

def matches(spa_url: str, route_template: str) -> bool:
    """Match either a full URL against a template, or a template-prefix where
    the SPA URL stopped at the start of a ${...} interpolation."""
    rx = re.sub(r"\{[^}]+\}", r"[^/]+", route_template)
    if re.fullmatch(rx, spa_url):
        return True
    # The SPA URL ends with '/' followed by template interpolation; check
    # whether the registered route has a param segment right after this prefix.
    if spa_url.endswith("/"):
        # `prefix/` should match a route shaped `prefix/{x}` (with optional
        # additional path segments after the param).
        if route_template.startswith(spa_url) and "{" in route_template[len(spa_url):]:
            return True
    return False

unmatched = [u for u in urls if not any(matches(u, rp) for rp in route_paths)]
print(f"SPA fetch URLs: {len(urls)}")
print(f"Registered routes: {len(route_paths)}")
print(f"Unmatched URLs: {unmatched if unmatched else 'none'}")
if unmatched:
    raise SystemExit(1)
