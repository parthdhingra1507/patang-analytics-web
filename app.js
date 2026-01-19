import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/+esm';
import JSZip from 'https://cdn.jsdelivr.net/npm/jszip@3.10.1/+esm';

const statusEl = document.getElementById('status');
const loadButton = document.getElementById('load-release');
const zipInput = document.getElementById('zip-input');
const runButton = document.getElementById('run');
const clearButton = document.getElementById('clear');
const sqlInput = document.getElementById('sql');
const resultsEl = document.getElementById('results');

const ownerInput = document.getElementById('owner');
const repoInput = document.getElementById('repo');
const tagInput = document.getElementById('tag');

let db;
let conn;
let isReady = false;

const LOCAL_BUNDLE = {
  mainModule: new URL('./vendor/duckdb/duckdb-browser-mvp.wasm', import.meta.url).toString(),
  mainWorker: new URL('./vendor/duckdb/duckdb-browser-mvp.worker.js', import.meta.url).toString(),
  pthreadWorker: null,
};

const viewSql = `
CREATE OR REPLACE VIEW analytics_events AS
SELECT * FROM read_parquet('analytics_store/parquet/analytics_events/*.parquet');

CREATE OR REPLACE VIEW level_analytics AS
SELECT * FROM read_parquet('analytics_store/parquet/level_analytics/*.parquet');

CREATE OR REPLACE VIEW users AS
SELECT * FROM read_parquet('analytics_store/parquet/users/*.parquet');

CREATE OR REPLACE VIEW game_sessions AS
SELECT * FROM read_parquet('analytics_store/parquet/game_sessions/*.parquet');

CREATE OR REPLACE VIEW events AS
SELECT * FROM read_parquet('analytics_store/parquet/events/*.parquet');

CREATE OR REPLACE VIEW leaderboard AS
SELECT * FROM read_parquet('analytics_store/parquet/leaderboard/*.parquet');

CREATE OR REPLACE VIEW level_results AS
SELECT * FROM read_parquet('analytics_store/parquet/level_results/*.parquet');
`;

function setStatus(message, tone = 'info') {
  statusEl.textContent = message;
  statusEl.dataset.tone = tone;
}

async function resolveBundle() {
  try {
    const res = await fetch(LOCAL_BUNDLE.mainWorker, { method: 'HEAD' });
    if (res.ok) return LOCAL_BUNDLE;
  } catch (err) {
    // Ignore and fall back to CDN.
  }
  return duckdb.selectBundle(duckdb.getJsDelivrBundles());
}

async function initDuckDB() {
  setStatus('Starting DuckDB engine...');
  try {
    const bundle = await resolveBundle();
    const worker = new Worker(bundle.mainWorker);
    db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    conn = await db.connect();
    isReady = true;
    setStatus('DuckDB ready. Load a snapshot.');
  } catch (err) {
    isReady = false;
    setStatus(`DuckDB init failed: ${err.message}`, 'error');
    throw err;
  }
}

function normalizePath(path) {
  return path.replace(/^\//, '');
}

async function registerParquetFiles(zip) {
  const files = Object.keys(zip.files).filter((name) => name.endsWith('.parquet'));
  if (!files.length) {
    throw new Error('No parquet files found in zip.');
  }

  for (const fileName of files) {
    const file = zip.files[fileName];
    const buffer = await file.async('uint8array');
    await db.registerFileBuffer(normalizePath(fileName), buffer);
  }
}

async function createViews() {
  await conn.query(viewSql);
}

async function loadZip(buffer) {
  if (!isReady) {
    throw new Error('DuckDB is not initialized');
  }
  const zip = await JSZip.loadAsync(buffer);
  await registerParquetFiles(zip);
  await createViews();
  setStatus('Snapshot loaded. Ready to query.');
}

async function fetchLatestReleaseZip(owner, repo, tag) {
  const localUrl = new URL('./analytics-parquet.zip', window.location.href).toString();
  const localRes = await fetch(localUrl);
  if (localRes.ok) return localRes.arrayBuffer();

  const downloadUrl = `https://github.com/${owner}/${repo}/releases/download/${tag}/analytics-parquet.zip`;
  const assetRes = await fetch(downloadUrl);
  if (!assetRes.ok) {
    throw new Error(`Release asset not found (${assetRes.status}).`);
  }
  return assetRes.arrayBuffer();
}

async function runQuery() {
  if (!conn) {
    setStatus('Load a snapshot before running queries.', 'error');
    return;
  }

  const sql = sqlInput.value.trim();
  if (!sql) {
    setStatus('Type a SQL query first.', 'error');
    return;
  }

  setStatus('Running query...');
  try {
    const result = await conn.query(sql);
    const rows = result.toArray();
    renderResults(rows, result.schema.fields.map((field) => field.name));
    setStatus(`Query complete. Rows: ${rows.length}`);
  } catch (err) {
    renderError(err);
    setStatus('Query error. Check output.', 'error');
  }
}

function renderResults(rows, columns) {
  if (!rows.length) {
    resultsEl.innerHTML = '<div class="empty">No rows returned.</div>';
    return;
  }

  const header = columns.map((col) => `<th>${col}</th>`).join('');
  const body = rows
    .slice(0, 500)
    .map((row) => {
      const cells = columns.map((col) => `<td>${String(row[col] ?? '')}</td>`).join('');
      return `<tr>${cells}</tr>`;
    })
    .join('');

  resultsEl.innerHTML = `
    <table class="result-table">
      <thead><tr>${header}</tr></thead>
      <tbody>${body}</tbody>
    </table>
  `;
}

function renderError(err) {
  resultsEl.innerHTML = `<pre>${String(err.message || err)}</pre>`;
}

loadButton.addEventListener('click', async () => {
  if (!isReady) {
    setStatus('DuckDB not ready yet. Please wait a moment.', 'error');
    return;
  }
  const owner = ownerInput.value.trim();
  const repo = repoInput.value.trim();
  const tag = tagInput.value.trim() || 'analytics-latest';

  if (!owner || !repo) {
    setStatus('Enter GitHub owner and repo name.', 'error');
    return;
  }

  setStatus('Downloading snapshot... (public release)');
  try {
    const buffer = await fetchLatestReleaseZip(owner, repo, tag);
    setStatus('Unpacking snapshot...');
    await loadZip(buffer);
  } catch (err) {
    setStatus(`Load failed: ${err.message}`, 'error');
  }
});

zipInput.addEventListener('change', async (event) => {
  if (!isReady) {
    setStatus('DuckDB not ready yet. Please wait a moment.', 'error');
    return;
  }
  const file = event.target.files[0];
  if (!file) return;

  setStatus('Unpacking uploaded snapshot...');
  try {
    const buffer = await file.arrayBuffer();
    await loadZip(buffer);
  } catch (err) {
    setStatus(`Upload failed: ${err.message}`, 'error');
  }
});

runButton.addEventListener('click', runQuery);
clearButton.addEventListener('click', () => {
  resultsEl.innerHTML = '';
  setStatus('Output cleared.');
});

await initDuckDB();
