"""Serve the single-page HTML frontend (Search + Extract tabs)."""
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
      padding: 12px 24px;
      display: flex;
      align-items: center;
      gap: 20px;
      position: sticky;
      top: 0;
      z-index: 10;
    }
    header h1 {
      font-size: 1.1rem;
      font-weight: 700;
      color: #7b8fff;
      letter-spacing: 0.06em;
      white-space: nowrap;
    }
    .api-key-row {
      display: flex;
      align-items: center;
      gap: 8px;
      margin-left: auto;
    }
    .api-key-row label {
      font-size: 0.8rem;
      color: #6b7280;
      white-space: nowrap;
    }
    #apiKeyInput {
      background: #1c1f35;
      border: 1px solid #2e3255;
      border-radius: 6px;
      padding: 6px 10px;
      color: #dde1f0;
      font-size: 0.82rem;
      width: 200px;
      outline: none;
      transition: border-color 0.2s;
    }
    #apiKeyInput:focus { border-color: #7b8fff; }

    /* ── Tab bar ── */
    .tab-bar {
      display: flex;
      gap: 4px;
    }
    .tab-btn {
      background: none;
      border: none;
      border-radius: 6px;
      padding: 7px 16px;
      font-size: 0.84rem;
      font-weight: 600;
      color: #5a607a;
      cursor: pointer;
      transition: background 0.15s, color 0.15s;
    }
    .tab-btn:hover { background: #1c1f35; color: #dde1f0; }
    .tab-btn.active { background: #1c1f35; color: #7b8fff; }

    /* ── Layout ── */
    .container {
      max-width: 1100px;
      margin: 0 auto;
      padding: 28px 20px;
    }
    .tab-content.hidden { display: none !important; }

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
    input[type="text"], input[type="date"], select, textarea {
      background: #0d0f1a;
      border: 1px solid #252947;
      border-radius: 7px;
      padding: 9px 13px;
      color: #dde1f0;
      font-size: 0.875rem;
      outline: none;
      transition: border-color 0.2s;
      width: 100%;
      font-family: inherit;
    }
    input[type="text"]:focus,
    input[type="date"]:focus,
    select:focus,
    textarea:focus { border-color: #7b8fff; background: #101328; }
    input::placeholder, textarea::placeholder { color: #3d4260; }
    textarea { resize: vertical; line-height: 1.5; }

    /* ── Checkbox label ── */
    .checkbox-label {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 0.83rem;
      font-weight: 500;
      color: #7b8499;
      cursor: pointer;
    }
    .checkbox-label input[type="checkbox"] {
      width: 15px;
      height: 15px;
      accent-color: #4a58e8;
      cursor: pointer;
    }
    .hint {
      font-size: 0.74rem;
      color: #3d4260;
      margin-top: 2px;
    }

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

    .btn-extract {
      background: #1c1f35;
      color: #7b8fff;
      border: 1px solid #2e3255;
      font-size: 0.8rem;
    }
    .btn-extract:hover:not(:disabled) { background: #252947; }
    .btn-extract:disabled { opacity: 0.4; cursor: not-allowed; }

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
    .results-header-left {
      display: flex;
      align-items: baseline;
      gap: 14px;
      flex-wrap: wrap;
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

    /* ── Progress bar ── */
    .progress-wrap {
      margin-bottom: 16px;
    }
    .progress-track {
      background: #1c1f35;
      border-radius: 6px;
      height: 8px;
      overflow: hidden;
      margin-bottom: 8px;
    }
    .progress-fill {
      background: #4a58e8;
      height: 100%;
      width: 0%;
      border-radius: 6px;
      transition: width 0.4s ease;
    }
    .progress-text {
      font-size: 0.82rem;
      color: #5a607a;
    }

    /* ── Extract results table ── */
    .extract-table-wrap {
      overflow-x: auto;
      border-radius: 9px;
      border: 1px solid #252947;
    }
    .extract-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.81rem;
      white-space: nowrap;
    }
    .extract-table th {
      background: #1c1f35;
      padding: 10px 14px;
      text-align: left;
      font-weight: 600;
      color: #7b8499;
      border-bottom: 1px solid #252947;
      position: sticky;
      top: 0;
      cursor: pointer;
      user-select: none;
      white-space: nowrap;
    }
    .extract-table th:hover { color: #dde1f0; background: #22263d; }
    .extract-table th.sort-asc::after  { content: ' \25b2'; font-size: 0.65rem; color: #7b8fff; }
    .extract-table th.sort-desc::after { content: ' \25bc'; font-size: 0.65rem; color: #7b8fff; }
    .extract-table td {
      padding: 9px 14px;
      border-bottom: 1px solid #0d0f1a;
      color: #a0aabb;
      max-width: 220px;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .extract-table tr:last-child td { border-bottom: none; }
    .extract-table tr:hover td { background: #111428; }
    .extract-table td.col-number {
      font-family: "Cascadia Code", "Fira Code", "Courier New", monospace;
      color: #7b8fff;
      font-size: 0.82rem;
    }
    .extract-table td.col-error { color: #f08080; }

    /* ── Responsive ── */
    @media (max-width: 600px) {
      .form-grid { grid-template-columns: 1fr; }
      .form-group.span2 { grid-column: 1; }
      .tab-btn { padding: 7px 10px; font-size: 0.78rem; }
    }
  </style>
</head>
<body>

<header>
  <h1>Poursuite</h1>
  <nav class="tab-bar">
    <button type="button" class="tab-btn active" data-tab="search">Search</button>
    <button type="button" class="tab-btn" data-tab="extract">Extract eSAJ</button>
  </nav>
  <div class="api-key-row">
    <label for="apiKeyInput">API Key</label>
    <input type="password" id="apiKeyInput" placeholder="Enter API key..." autocomplete="off">
  </div>
</header>

<div class="container">

  <!-- ═══════════════════════════════════════════════════════════════ -->
  <!--  SEARCH TAB                                                     -->
  <!-- ═══════════════════════════════════════════════════════════════ -->
  <div class="tab-content" id="tab-search">

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
          <button type="submit" id="searchBtn" class="btn btn-primary">Search</button>
          <button type="button" id="csvBtn" class="btn btn-csv" disabled>Download CSV</button>
          <button type="button" id="clearBtn" class="btn btn-ghost">Clear</button>
        </div>
      </form>
    </div>

    <div id="statusBar" class="status hidden"></div>

    <div id="resultsSection" class="hidden">
      <div class="results-header">
        <div class="results-header-left">
          <div class="results-summary" id="resultsSummary"></div>
          <button id="sendExtractBtn" class="btn btn-extract hidden" disabled>
            Send to Extract &rarr;
          </button>
        </div>
      </div>
      <div id="processList" class="process-list"></div>
      <div id="pagination" class="pagination"></div>
    </div>

  </div><!-- /tab-search -->

  <!-- ═══════════════════════════════════════════════════════════════ -->
  <!--  EXTRACT TAB                                                    -->
  <!-- ═══════════════════════════════════════════════════════════════ -->
  <div class="tab-content hidden" id="tab-extract">

    <div class="search-card">
      <h2>Extract from eSAJ</h2>
      <div class="form-grid">

        <div class="form-group span2">
          <label for="eNumbers">Process Numbers <span style="font-weight:400;color:#3d4260">(one per line)</span></label>
          <textarea id="eNumbers" rows="7"
                    placeholder="1234567-89.2023.8.26.0001&#10;9876543-21.2022.8.26.0100&#10;..."></textarea>
        </div>

        <div class="form-group">
          <label for="eConcurrent">Concurrent browsers</label>
          <select id="eConcurrent">
            <option value="2">2</option>
            <option value="4" selected>4</option>
            <option value="6">6</option>
            <option value="8">8</option>
          </select>
        </div>

        <div class="form-group">
          <label class="checkbox-label" for="eIncludeOther">
            <input type="checkbox" id="eIncludeOther">
            Include defendant's process count
          </label>
          <span class="hint">Makes an extra eSAJ request per process</span>
        </div>

      </div>
      <div class="form-actions">
        <button type="button" id="eStartBtn" class="btn btn-primary">Start Extraction</button>
        <button type="button" id="eClearBtn" class="btn btn-ghost">Clear</button>
      </div>
    </div>

    <div id="eStatusBar" class="status hidden"></div>

    <div id="eProgressWrap" class="progress-wrap hidden">
      <div class="progress-track">
        <div id="eProgressFill" class="progress-fill"></div>
      </div>
      <div id="eProgressText" class="progress-text">Preparing&hellip;</div>
    </div>

    <div id="eResultsSection" class="hidden">
      <div class="results-header">
        <div id="eResultsSummary" class="results-summary"></div>
        <button id="eExportBtn" class="btn btn-csv" disabled>Download CSV</button>
      </div>
      <div id="eTableWrap" class="extract-table-wrap"></div>
    </div>

  </div><!-- /tab-extract -->

</div><!-- /container -->

<script>
  // ── Config ──────────────────────────────────────────────────
  const PREVIEW_LEN = 400;

  // ── Shared: API key ──────────────────────────────────────────
  const keyInput = document.getElementById('apiKeyInput');
  const stored = sessionStorage.getItem('poursuiteKey');
  if (stored) keyInput.value = stored;
  keyInput.addEventListener('input', () => {
    sessionStorage.setItem('poursuiteKey', keyInput.value.trim());
  });
  function apiKey() { return keyInput.value.trim(); }

  // ── Tab switching ────────────────────────────────────────────
  function switchTab(name) {
    document.querySelectorAll('.tab-btn').forEach(b => {
      b.classList.toggle('active', b.dataset.tab === name);
    });
    document.querySelectorAll('.tab-content').forEach(c => {
      c.classList.toggle('hidden', c.id !== 'tab-' + name);
    });
  }
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => switchTab(btn.dataset.tab));
  });

  // ════════════════════════════════════════════════════════════
  //  SEARCH TAB
  // ════════════════════════════════════════════════════════════

  let currentPage = 1;
  let searching   = false;
  let lastSearchNumbers = [];

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

  function setStatus(type, msg) {
    const bar = document.getElementById('statusBar');
    bar.className = 'status ' + type;
    bar.innerHTML = msg;
    bar.classList.remove('hidden');
  }
  function clearStatus() {
    document.getElementById('statusBar').classList.add('hidden');
  }

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

  function renderResults(data, page) {
    const section  = document.getElementById('resultsSection');
    const list     = document.getElementById('processList');
    const summary  = document.getElementById('resultsSummary');
    const csvBtn   = document.getElementById('csvBtn');
    const sendBtn  = document.getElementById('sendExtractBtn');

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
      sendBtn.classList.add('hidden');
      sendBtn.disabled = true;
      section.classList.remove('hidden');
      renderPagination(page, totalPages);
      return;
    }

    for (const proc of data.results) {
      list.appendChild(buildProcessCard(proc));
    }

    // Store process numbers for "Send to Extract"
    lastSearchNumbers = data.results.map(r => r.process_number);
    sendBtn.textContent = 'Send ' + lastSearchNumbers.length + ' to Extract \u2192';
    sendBtn.classList.remove('hidden');
    sendBtn.disabled = false;

    csvBtn.disabled = false;
    section.classList.remove('hidden');
    renderPagination(page, totalPages);
  }

  function buildProcessCard(proc) {
    const card = document.createElement('div');
    card.className = 'process-card';

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

    const wrap = document.createElement('div');
    wrap.className = 'mentions-wrap';
    for (const m of proc.mentions) {
      wrap.appendChild(buildMentionItem(m));
    }

    card.appendChild(header);
    card.appendChild(wrap);
    return card;
  }

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

    const full     = m.content || '';
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

  // ── CSV download (search) ────────────────────────────────────
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

  // ── Send to Extract ──────────────────────────────────────────
  document.getElementById('sendExtractBtn').addEventListener('click', () => {
    document.getElementById('eNumbers').value = lastSearchNumbers.join('\\n');
    switchTab('extract');
  });

  // ── Search form submit / clear ───────────────────────────────
  document.getElementById('searchForm').addEventListener('submit', e => {
    e.preventDefault();
    doSearch(1);
  });

  document.getElementById('clearBtn').addEventListener('click', () => {
    document.getElementById('searchForm').reset();
    document.getElementById('resultsSection').classList.add('hidden');
    document.getElementById('csvBtn').disabled = true;
    document.getElementById('sendExtractBtn').classList.add('hidden');
    clearStatus();
    currentPage = 1;
    lastSearchNumbers = [];
  });

  // ════════════════════════════════════════════════════════════
  //  EXTRACT TAB
  // ════════════════════════════════════════════════════════════

  let extractJobId        = null;
  let extractPollTimer    = null;
  let extractResultCount  = 0;
  let extractResults      = [];
  let extractSortCol      = null;
  let extractSortDir      = 'asc';

  function setExtractStatus(type, msg) {
    const bar = document.getElementById('eStatusBar');
    bar.className = 'status ' + type;
    bar.innerHTML = msg;
    bar.classList.remove('hidden');
  }
  function clearExtractStatus() {
    document.getElementById('eStatusBar').classList.add('hidden');
  }

  function parseProcessNumbers() {
    return document.getElementById('eNumbers').value
      .split('\\n')
      .map(s => s.trim())
      .filter(s => s.length > 0);
  }

  function setExtractProgress(done, total) {
    const pct = total > 0 ? Math.round((done / total) * 100) : 0;
    document.getElementById('eProgressFill').style.width = pct + '%';
    document.getElementById('eProgressText').textContent =
      'Processing ' + done + ' / ' + total + ' (' + pct + '%)';
  }

  async function startExtraction() {
    const numbers = parseProcessNumbers();
    if (!numbers.length) {
      setExtractStatus('error', 'No process numbers entered.');
      return;
    }
    const key = apiKey();
    if (!key) {
      setExtractStatus('error', 'Please enter your API key.');
      return;
    }

    // Reset state
    clearExtractStatus();
    extractJobId       = null;
    extractResultCount = 0;
    document.getElementById('eProgressFill').style.width = '0%';
    document.getElementById('eProgressText').textContent = 'Starting\u2026';
    document.getElementById('eProgressWrap').classList.remove('hidden');
    document.getElementById('eResultsSection').classList.remove('hidden');
    document.getElementById('eResultsSummary').textContent = '';
    document.getElementById('eTableWrap').innerHTML = '';
    document.getElementById('eExportBtn').disabled = true;

    const eStartBtn = document.getElementById('eStartBtn');
    eStartBtn.disabled = true;
    eStartBtn.innerHTML = '<span class="spinner"></span> Starting\u2026';

    try {
      const resp = await fetch('/extract/start', {
        method: 'POST',
        headers: { 'X-API-Key': key, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          process_numbers: numbers,
          concurrent: parseInt(document.getElementById('eConcurrent').value),
          include_other_processes: document.getElementById('eIncludeOther').checked,
        }),
      });

      if (!resp.ok) {
        const err = await resp.json().catch(() => ({}));
        setExtractStatus('error', 'Failed to start: ' + (err.detail || resp.statusText));
        eStartBtn.disabled = false;
        eStartBtn.textContent = 'Start Extraction';
        return;
      }

      const { job_id } = await resp.json();
      extractJobId = job_id;
      setExtractProgress(0, numbers.length);

      if (extractPollTimer) clearInterval(extractPollTimer);
      extractPollTimer = setInterval(pollExtract, 2000);

    } catch (err) {
      setExtractStatus('error', 'Network error: ' + err.message);
      eStartBtn.disabled = false;
      eStartBtn.textContent = 'Start Extraction';
    }
  }

  async function pollExtract() {
    if (!extractJobId) return;
    try {
      const resp = await fetch('/extract/status/' + extractJobId, {
        headers: { 'X-API-Key': apiKey() },
      });
      if (!resp.ok) return;

      const data = await resp.json();
      setExtractProgress(data.done, data.total);

      if (data.results.length > extractResultCount) {
        renderExtractTable(data.results);
        extractResultCount = data.results.length;
      }

      if (data.status === 'done') {
        finishExtraction(data.results);
      } else if (data.status === 'error') {
        clearInterval(extractPollTimer);
        extractPollTimer = null;
        document.getElementById('eStartBtn').disabled = false;
        document.getElementById('eStartBtn').textContent = 'Start Extraction';
        setExtractStatus('error', 'Extraction error: ' + (data.error || 'Unknown error'));
      }
    } catch (_) {
      // Network blip — retry on next tick
    }
  }

  function finishExtraction(results) {
    clearInterval(extractPollTimer);
    extractPollTimer = null;

    const eStartBtn = document.getElementById('eStartBtn');
    eStartBtn.disabled = false;
    eStartBtn.textContent = 'Start Extraction';
    document.getElementById('eExportBtn').disabled = false;

    const successful = results.filter(r => !r.error).length;
    const errors     = results.filter(r => r.error).length;
    document.getElementById('eResultsSummary').innerHTML =
      '<strong>' + results.length + '</strong> processed \u2014 ' +
      '<strong>' + successful + '</strong> successful, ' +
      '<strong>' + errors + '</strong> errors';
    document.getElementById('eProgressText').textContent =
      'Done. ' + results.length + ' processes extracted.';
    document.getElementById('eProgressFill').style.width = '100%';
  }

  const EXTRACT_COLS = [
    { key: 'number',          label: 'Process Number', cls: 'col-number' },
    { key: 'initial_date',    label: 'Date' },
    { key: 'class_type',      label: 'Class' },
    { key: 'subject',         label: 'Subject' },
    { key: 'value',           label: 'Value' },
    { key: 'last_movement',   label: 'Last Movement' },
    { key: 'status',          label: 'Status' },
    { key: 'plaintiff',       label: 'Plaintiff' },
    { key: 'defendant',       label: 'Defendant' },
    { key: 'other_processes', label: 'Other Proc.' },
    { key: 'error',           label: 'Error', cls: 'col-error' },
  ];

  // Parse a value cell (e.g. "R$ 1.234,56") to a number for sorting, or Infinity for empty
  function parseSortValue(key, val) {
    if (val === null || val === undefined || val === '') return Infinity;
    if (key === 'other_processes') return Number(val) || 0;
    if (key === 'value') {
      // Strip currency symbols and convert BR decimal format to float
      const n = parseFloat(String(val).replace(/[^\d,]/g, '').replace(',', '.'));
      return isNaN(n) ? Infinity : n;
    }
    return String(val).toLowerCase();
  }

  function sortedResults() {
    if (!extractSortCol) return extractResults;
    return [...extractResults].sort((a, b) => {
      const va = parseSortValue(extractSortCol, a[extractSortCol]);
      const vb = parseSortValue(extractSortCol, b[extractSortCol]);
      // Nulls/empty always last
      if (va === Infinity && vb === Infinity) return 0;
      if (va === Infinity) return 1;
      if (vb === Infinity) return -1;
      const cmp = va < vb ? -1 : va > vb ? 1 : 0;
      return extractSortDir === 'asc' ? cmp : -cmp;
    });
  }

  function renderExtractTable(results) {
    if (results !== undefined) extractResults = results;

    const table = document.createElement('table');
    table.className = 'extract-table';

    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    for (const col of EXTRACT_COLS) {
      const th = document.createElement('th');
      th.textContent = col.label;
      if (extractSortCol === col.key) {
        th.classList.add(extractSortDir === 'asc' ? 'sort-asc' : 'sort-desc');
      }
      th.addEventListener('click', () => {
        if (extractSortCol === col.key) {
          extractSortDir = extractSortDir === 'asc' ? 'desc' : 'asc';
        } else {
          extractSortCol = col.key;
          extractSortDir = 'asc';
        }
        renderExtractTable();
      });
      headerRow.appendChild(th);
    }
    thead.appendChild(headerRow);
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    for (const row of sortedResults()) {
      const tr = document.createElement('tr');
      for (const col of EXTRACT_COLS) {
        const td = document.createElement('td');
        const val = row[col.key];
        td.textContent = (val !== null && val !== undefined) ? String(val) : '';
        if (col.cls && (col.cls !== 'col-error' || val)) {
          td.className = col.cls;
        }
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);

    const wrap = document.getElementById('eTableWrap');
    wrap.innerHTML = '';
    wrap.appendChild(table);
  }

  // ── Extract CSV download ─────────────────────────────────────
  document.getElementById('eExportBtn').addEventListener('click', async () => {
    if (!extractJobId) return;
    const key = apiKey();
    if (!key) return;

    const eExportBtn = document.getElementById('eExportBtn');
    eExportBtn.disabled = true;
    eExportBtn.innerHTML = '<span class="spinner"></span> Preparing\u2026';

    try {
      const resp = await fetch('/extract/export/' + extractJobId, {
        headers: { 'X-API-Key': key },
      });
      if (!resp.ok) {
        setExtractStatus('error', 'Export error ' + resp.status);
        return;
      }
      const blob = await resp.blob();
      const url  = URL.createObjectURL(blob);
      const a    = document.createElement('a');
      a.href     = url;
      a.download = 'esaj_results.csv';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (err) {
      setExtractStatus('error', 'Network error: ' + err.message);
    } finally {
      eExportBtn.disabled = false;
      eExportBtn.textContent = 'Download CSV';
    }
  });

  // ── Extract start / clear ────────────────────────────────────
  document.getElementById('eStartBtn').addEventListener('click', startExtraction);

  document.getElementById('eClearBtn').addEventListener('click', () => {
    if (extractPollTimer) { clearInterval(extractPollTimer); extractPollTimer = null; }
    document.getElementById('eNumbers').value = '';
    document.getElementById('eProgressWrap').classList.add('hidden');
    document.getElementById('eResultsSection').classList.add('hidden');
    document.getElementById('eExportBtn').disabled = true;
    document.getElementById('eStartBtn').disabled = false;
    document.getElementById('eStartBtn').textContent = 'Start Extraction';
    clearExtractStatus();
    extractJobId       = null;
    extractResultCount = 0;
    extractResults     = [];
    extractSortCol     = null;
    extractSortDir     = 'asc';
  });
</script>

</body>
</html>"""


@router.get("/", response_class=HTMLResponse)
def serve_frontend():
    """Serve the search + extract frontend."""
    return _HTML
