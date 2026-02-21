"""Serve the single-page HTML search frontend."""
from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter(include_in_schema=False)

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Poursuite</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
      background: #0d0f1a;
      color: #dde1f0;
      min-height: 100vh;
    }

    /* ── Header ── */
    header {
      background: #13162a;
      border-bottom: 1px solid #252947;
      padding: 14px 24px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      position: sticky;
      top: 0;
      z-index: 10;
    }
    header h1 {
      font-size: 1.15rem;
      font-weight: 700;
      color: #7b8fff;
      letter-spacing: 0.06em;
    }
    .api-key-row {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .api-key-row label {
      font-size: 0.8rem;
      color: #6b7280;
    }
    #apiKeyInput {
      background: #1c1f35;
      border: 1px solid #2e3255;
      border-radius: 6px;
      padding: 6px 10px;
      color: #dde1f0;
      font-size: 0.82rem;
      width: 220px;
      outline: none;
      transition: border-color 0.2s;
    }
    #apiKeyInput:focus { border-color: #7b8fff; }

    /* ── Layout ── */
    .container {
      max-width: 1100px;
      margin: 0 auto;
      padding: 28px 20px;
    }

    /* ── Search card ── */
    .search-card {
      background: #13162a;
      border: 1px solid #252947;
      border-radius: 12px;
      padding: 22px 24px 20px;
      margin-bottom: 20px;
    }
    .search-card h2 {
      font-size: 0.72rem;
      font-weight: 600;
      color: #5a607a;
      text-transform: uppercase;
      letter-spacing: 0.1em;
      margin-bottom: 16px;
    }

    .form-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 14px;
    }
    .form-group { display: flex; flex-direction: column; gap: 5px; }
    .form-group.span2 { grid-column: 1 / -1; }

    label {
      font-size: 0.78rem;
      font-weight: 500;
      color: #7b8499;
    }
    input[type="text"], input[type="date"], select {
      background: #0d0f1a;
      border: 1px solid #252947;
      border-radius: 7px;
      padding: 9px 13px;
      color: #dde1f0;
      font-size: 0.875rem;
      outline: none;
      transition: border-color 0.2s;
      width: 100%;
    }
    input[type="text"]:focus,
    input[type="date"]:focus,
    select:focus { border-color: #7b8fff; background: #101328; }
    input::placeholder { color: #3d4260; }

    .form-actions {
      display: flex;
      gap: 10px;
      margin-top: 18px;
      align-items: center;
      flex-wrap: wrap;
    }

    /* ── Buttons ── */
    .btn {
      border: none;
      border-radius: 7px;
      padding: 9px 18px;
      font-size: 0.85rem;
      font-weight: 600;
      cursor: pointer;
      transition: background 0.18s, color 0.18s;
      display: inline-flex;
      align-items: center;
      gap: 7px;
      user-select: none;
    }
    .btn-primary { background: #4a58e8; color: #fff; }
    .btn-primary:hover:not(:disabled) { background: #6070f0; }
    .btn-primary:disabled { background: #1e2240; color: #404870; cursor: not-allowed; }

    .btn-csv { background: #0d6e40; color: #fff; }
    .btn-csv:hover:not(:disabled) { background: #0f8a52; }
    .btn-csv:disabled { background: #0d2a1e; color: #2a5040; cursor: not-allowed; }

    .btn-ghost {
      background: #1c1f35;
      color: #6b7280;
      border: 1px solid #252947;
    }
    .btn-ghost:hover { background: #252947; color: #dde1f0; }

    /* ── Spinner ── */
    .spinner {
      width: 14px; height: 14px;
      border: 2px solid currentColor;
      border-top-color: transparent;
      border-radius: 50%;
      display: inline-block;
      animation: spin 0.7s linear infinite;
      vertical-align: middle;
    }
    @keyframes spin { to { transform: rotate(360deg); } }

    /* ── Status bar ── */
    .status {
      padding: 10px 14px;
      border-radius: 8px;
      font-size: 0.875rem;
      margin-bottom: 16px;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .status.info    { background: #0e2340; border: 1px solid #1e4880; color: #7eb8f5; }
    .status.warn    { background: #2a2010; border: 1px solid #6e4a10; color: #f0c060; }
    .status.error   { background: #2a1010; border: 1px solid #6e2020; color: #f08080; }
    .status.success { background: #0d2a1e; border: 1px solid #1e6040; color: #60d090; }
    .hidden { display: none !important; }

    /* ── Results header ── */
    .results-header {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 14px;
    }
    .results-summary {
      font-size: 0.875rem;
      color: #6b7280;
    }
    .results-summary strong { color: #dde1f0; }

    /* ── Process list ── */
    .process-list { display: flex; flex-direction: column; gap: 6px; }

    .process-card {
      background: #13162a;
      border: 1px solid #252947;
      border-radius: 9px;
      overflow: hidden;
    }

    .process-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 13px 16px;
      cursor: pointer;
      user-select: none;
      transition: background 0.15s;
    }
    .process-header:hover { background: #181b30; }

    .process-num {
      font-family: "Cascadia Code", "Fira Code", "Courier New", monospace;
      font-size: 0.9rem;
      font-weight: 600;
      color: #7b8fff;
    }

    .process-right {
      display: flex;
      align-items: center;
      gap: 10px;
    }

    .badge {
      background: #1c1f35;
      color: #6b7280;
      font-size: 0.75rem;
      padding: 3px 8px;
      border-radius: 10px;
      font-weight: 500;
    }

    .chevron {
      color: #3d4260;
      font-size: 0.8rem;
      transition: transform 0.2s;
    }
    .process-card.open .chevron { transform: rotate(180deg); }

    /* ── Mentions ── */
    .mentions-wrap {
      display: none;
      border-top: 1px solid #1c1f35;
    }
    .process-card.open .mentions-wrap { display: block; }

    .mention-item {
      padding: 13px 16px;
      border-bottom: 1px solid #0d0f1a;
    }
    .mention-item:last-child { border-bottom: none; }

    .mention-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 14px;
      margin-bottom: 7px;
    }
    .mention-meta span {
      font-size: 0.78rem;
      color: #4a5170;
      display: flex;
      align-items: center;
      gap: 4px;
    }

    .mention-content {
      font-size: 0.85rem;
      color: #a0aabb;
      line-height: 1.65;
      white-space: pre-wrap;
      word-break: break-word;
    }

    .expand-btn {
      background: none;
      border: none;
      color: #5a6aee;
      font-size: 0.78rem;
      cursor: pointer;
      padding: 4px 0 0;
      display: block;
    }
    .expand-btn:hover { text-decoration: underline; }

    /* ── Pagination ── */
    .pagination {
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 6px;
      margin-top: 20px;
      flex-wrap: wrap;
    }
    .page-btn {
      background: #13162a;
      border: 1px solid #252947;
      border-radius: 6px;
      padding: 7px 13px;
      color: #7b8499;
      cursor: pointer;
      font-size: 0.82rem;
      transition: all 0.15s;
    }
    .page-btn:hover:not(:disabled) { background: #252947; color: #dde1f0; }
    .page-btn:disabled { opacity: 0.35; cursor: not-allowed; }
    .page-btn.active { background: #4a58e8; border-color: #4a58e8; color: #fff; }
    .page-info { color: #3d4260; font-size: 0.82rem; padding: 0 4px; }

    /* ── Empty state ── */
    .empty {
      text-align: center;
      padding: 48px 0;
      color: #3d4260;
      font-size: 0.9rem;
    }

    /* ── Responsive ── */
    @media (max-width: 600px) {
      .form-grid { grid-template-columns: 1fr; }
      .form-group.span2 { grid-column: 1; }
    }
  </style>
</head>
<body>

<header>
  <h1>Poursuite</h1>
  <div class="api-key-row">
    <label for="apiKeyInput">API Key</label>
    <input type="password" id="apiKeyInput" placeholder="Enter API key..." autocomplete="off">
  </div>
</header>

<div class="container">

  <div class="search-card">
    <h2>Search Court Documents</h2>
    <form id="searchForm">
      <div class="form-grid">

        <div class="form-group">
          <label for="fKeywords">Keywords</label>
          <input type="text" id="fKeywords" name="keywords"
                 placeholder='e.g. SISBAJUD OR (penhora AND conta)'>
        </div>

        <div class="form-group">
          <label for="fProcess">Process Number</label>
          <input type="text" id="fProcess" name="process_number"
                 placeholder="1234567-89.2023.8.26.0001">
        </div>

        <div class="form-group">
          <label for="fStart">Start Date</label>
          <input type="date" id="fStart" name="start_date">
        </div>

        <div class="form-group">
          <label for="fEnd">End Date</label>
          <input type="date" id="fEnd" name="end_date">
        </div>

        <div class="form-group">
          <label for="fExclude">Exclusion Terms</label>
          <input type="text" id="fExclude" name="exclusion_terms"
                 placeholder="Terms to exclude from results">
        </div>

        <div class="form-group">
          <label for="fPageSize">Results per page</label>
          <select id="fPageSize" name="page_size">
            <option value="25">25</option>
            <option value="50">50</option>
            <option value="100" selected>100</option>
            <option value="250">250</option>
            <option value="500">500</option>
          </select>
        </div>

      </div>

      <div class="form-actions">
        <button type="submit" id="searchBtn" class="btn btn-primary">
          Search
        </button>
        <button type="button" id="csvBtn" class="btn btn-csv" disabled>
          Download CSV
        </button>
        <button type="button" id="clearBtn" class="btn btn-ghost">Clear</button>
      </div>
    </form>
  </div>

  <div id="statusBar" class="status hidden"></div>

  <div id="resultsSection" class="hidden">
    <div class="results-header">
      <div class="results-summary" id="resultsSummary"></div>
    </div>
    <div id="processList" class="process-list"></div>
    <div id="pagination" class="pagination"></div>
  </div>

</div>

<script>
  // ── Config ──────────────────────────────────────────────────
  const PREVIEW_LEN = 400;

  // ── State ───────────────────────────────────────────────────
  let currentPage = 1;
  let searching   = false;

  // ── API key (sessionStorage) ─────────────────────────────────
  const keyInput = document.getElementById('apiKeyInput');
  const stored = sessionStorage.getItem('poursuiteKey');
  if (stored) keyInput.value = stored;
  keyInput.addEventListener('input', () => {
    sessionStorage.setItem('poursuiteKey', keyInput.value.trim());
  });
  function apiKey() { return keyInput.value.trim(); }

  // ── Build URLSearchParams from form ─────────────────────────
  function buildParams(page) {
    const form = document.getElementById('searchForm');
    const p = new URLSearchParams();
    const names = ['keywords', 'process_number', 'start_date', 'end_date',
                   'exclusion_terms', 'page_size'];
    for (const n of names) {
      const el = form.elements[n];
      if (el && el.value.trim()) p.set(n, el.value.trim());
    }
    p.set('page', String(page));
    return p;
  }

  // ── Status bar ──────────────────────────────────────────────
  function setStatus(type, msg) {
    const bar = document.getElementById('statusBar');
    bar.className = 'status ' + type;
    bar.innerHTML = msg;
    bar.classList.remove('hidden');
  }
  function clearStatus() {
    document.getElementById('statusBar').classList.add('hidden');
  }

  // ── Search ──────────────────────────────────────────────────
  async function doSearch(page) {
    if (searching) return;

    const key = apiKey();
    if (!key) { setStatus('error', 'Please enter your API key.'); return; }

    const params = buildParams(page);
    if (!params.has('keywords') && !params.has('process_number')) {
      setStatus('error', 'Enter keywords or a process number to search.');
      return;
    }

    searching = true;
    currentPage = page;

    const btn = document.getElementById('searchBtn');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Searching\u2026';
    document.getElementById('csvBtn').disabled = true;
    clearStatus();

    try {
      const resp = await fetch('/search?' + params.toString(), {
        headers: { 'X-API-Key': key }
      });

      if (!resp.ok) {
        let detail = resp.statusText;
        try { detail = (await resp.json()).detail || detail; } catch (_) {}
        setStatus('error', 'Error ' + resp.status + ': ' + detail);
        document.getElementById('resultsSection').classList.add('hidden');
        return;
      }

      const data = await resp.json();
      renderResults(data, page);

    } catch (err) {
      setStatus('error', 'Network error: ' + err.message);
    } finally {
      searching = false;
      btn.disabled = false;
      btn.textContent = 'Search';
    }
  }

  // ── Render results ──────────────────────────────────────────
  function renderResults(data, page) {
    const section   = document.getElementById('resultsSection');
    const list      = document.getElementById('processList');
    const summary   = document.getElementById('resultsSummary');
    const csvBtn    = document.getElementById('csvBtn');

    if (data.truncated) {
      setStatus('warn',
        '\u26a0\ufe0f Search timed out \u2014 results are partial. '
        + 'Try a narrower query or a shorter date range.');
    }

    const pageSize   = parseInt(document.getElementById('fPageSize').value, 10);
    const totalPages = Math.max(1, Math.ceil(data.total_processes / pageSize));

    summary.innerHTML =
      '<strong>' + data.total_processes.toLocaleString() + '</strong> processes found'
      + ' \u2014 page ' + page + ' of ' + totalPages;

    list.innerHTML = '';

    if (!data.results || data.results.length === 0) {
      const empty = document.createElement('div');
      empty.className = 'empty';
      empty.textContent = 'No results found.';
      list.appendChild(empty);
      csvBtn.disabled = true;
      section.classList.remove('hidden');
      renderPagination(page, totalPages);
      return;
    }

    for (const proc of data.results) {
      list.appendChild(buildProcessCard(proc));
    }

    csvBtn.disabled = false;
    section.classList.remove('hidden');
    renderPagination(page, totalPages);
  }

  // ── Build a process card ─────────────────────────────────────
  function buildProcessCard(proc) {
    const card = document.createElement('div');
    card.className = 'process-card';

    // Header
    const header = document.createElement('div');
    header.className = 'process-header';

    const numSpan = document.createElement('span');
    numSpan.className = 'process-num';
    numSpan.textContent = proc.process_number;

    const right = document.createElement('div');
    right.className = 'process-right';

    const badge = document.createElement('span');
    badge.className = 'badge';
    badge.textContent =
      proc.mention_count + ' mention' + (proc.mention_count !== 1 ? 's' : '');

    const chevron = document.createElement('span');
    chevron.className = 'chevron';
    chevron.textContent = '\u25bc';

    right.appendChild(badge);
    right.appendChild(chevron);
    header.appendChild(numSpan);
    header.appendChild(right);
    header.addEventListener('click', () => card.classList.toggle('open'));

    // Mentions wrapper
    const wrap = document.createElement('div');
    wrap.className = 'mentions-wrap';
    for (const m of proc.mentions) {
      wrap.appendChild(buildMentionItem(m));
    }

    card.appendChild(header);
    card.appendChild(wrap);
    return card;
  }

  // ── Build a single mention item ──────────────────────────────
  function buildMentionItem(m) {
    const item = document.createElement('div');
    item.className = 'mention-item';

    const meta = document.createElement('div');
    meta.className = 'mention-meta';

    function metaSpan(icon, text) {
      const s = document.createElement('span');
      s.textContent = icon + ' ' + text;
      return s;
    }
    if (m.document_date) meta.appendChild(metaSpan('📅', m.document_date));
    if (m.db_id)         meta.appendChild(metaSpan('🗄', m.db_id));
    if (m.file_path)     meta.appendChild(metaSpan('📄', m.file_path));

    const full    = m.content || '';
    const needsCut = full.length > PREVIEW_LEN;
    const preview  = needsCut ? full.slice(0, PREVIEW_LEN) + '\u2026' : full;

    const contentDiv = document.createElement('div');
    contentDiv.className = 'mention-content';
    contentDiv.textContent = preview;

    item.appendChild(meta);
    item.appendChild(contentDiv);

    if (needsCut) {
      const btn = document.createElement('button');
      btn.className = 'expand-btn';
      btn.textContent = 'Show more';
      let expanded = false;
      btn.addEventListener('click', () => {
        expanded = !expanded;
        contentDiv.textContent = expanded ? full : preview;
        btn.textContent = expanded ? 'Show less' : 'Show more';
      });
      item.appendChild(btn);
    }

    return item;
  }

  // ── Pagination ──────────────────────────────────────────────
  function renderPagination(current, total) {
    const pag = document.getElementById('pagination');
    pag.innerHTML = '';
    if (total <= 1) return;

    function pageBtn(label, page, active, disabled) {
      const b = document.createElement('button');
      b.className = 'page-btn' + (active ? ' active' : '');
      b.textContent = label;
      b.disabled = disabled;
      if (!disabled) b.addEventListener('click', () => doSearch(page));
      return b;
    }

    pag.appendChild(pageBtn('\u2190 Prev', current - 1, false, current <= 1));

    const start = Math.max(1, current - 2);
    const end   = Math.min(total, start + 4);
    for (let p = start; p <= end; p++) {
      pag.appendChild(pageBtn(String(p), p, p === current, false));
    }

    const info = document.createElement('span');
    info.className = 'page-info';
    info.textContent = 'of ' + total;
    pag.appendChild(info);

    pag.appendChild(pageBtn('Next \u2192', current + 1, false, current >= total));
  }

  // ── CSV download ─────────────────────────────────────────────
  document.getElementById('csvBtn').addEventListener('click', async () => {
    const key = apiKey();
    if (!key) { setStatus('error', 'API key required.'); return; }

    const params = buildParams(currentPage);
    const csvBtn = document.getElementById('csvBtn');

    csvBtn.disabled = true;
    csvBtn.innerHTML = '<span class="spinner"></span> Preparing\u2026';
    setStatus('info', '<span class="spinner"></span> Preparing CSV\u2026');

    try {
      const resp = await fetch('/search/export?' + params.toString(), {
        headers: { 'X-API-Key': key }
      });
      if (!resp.ok) {
        setStatus('error', 'Export error ' + resp.status + ': ' + resp.statusText);
        return;
      }
      const blob = await resp.blob();
      const url  = URL.createObjectURL(blob);
      const a    = document.createElement('a');
      a.href     = url;
      a.download = 'search_results.csv';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
      clearStatus();
    } catch (err) {
      setStatus('error', 'Network error: ' + err.message);
    } finally {
      csvBtn.disabled = false;
      csvBtn.textContent = 'Download CSV';
    }
  });

  // ── Form submit / clear ──────────────────────────────────────
  document.getElementById('searchForm').addEventListener('submit', e => {
    e.preventDefault();
    doSearch(1);
  });

  document.getElementById('clearBtn').addEventListener('click', () => {
    document.getElementById('searchForm').reset();
    document.getElementById('resultsSection').classList.add('hidden');
    document.getElementById('csvBtn').disabled = true;
    clearStatus();
    currentPage = 1;
  });
</script>

</body>
</html>"""


@router.get("/", response_class=HTMLResponse)
def serve_frontend():
    """Serve the search frontend."""
    return _HTML
