/*
  Read-only Turso export to NDJSON for analytics.
  Uses cursors to do incremental pulls per table.
*/

const fs = require('node:fs');
const path = require('node:path');

const TURSO_URL = process.env.TURSO_URL;
const TURSO_AUTH_TOKEN = process.env.TURSO_AUTH_TOKEN;
const OUTPUT_DIR = process.env.OUTPUT_DIR || 'analytics_work/ndjson';
const CURSOR_PATH = process.env.CURSOR_PATH || 'analytics_store/cursors.json';
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 1000);
const MAX_PAGES = Number(process.env.MAX_PAGES || 100);
const DEFAULT_CURSOR = process.env.DEFAULT_CURSOR || '1970-01-01T00:00:00Z';

if (!TURSO_URL || !TURSO_AUTH_TOKEN) {
  console.error('Missing TURSO_URL or TURSO_AUTH_TOKEN');
  process.exit(1);
}

function normalizeTursoUrl(url) {
  let normalized = url;
  if (normalized.startsWith('libsql://')) {
    normalized = normalized.replace('libsql://', 'https://');
  }
  if (!normalized.endsWith('/v2/pipeline')) {
    normalized = normalized.replace(/\/$/, '');
    normalized = `${normalized}/v2/pipeline`;
  }
  return normalized;
}

const TURSO_PIPELINE_URL = normalizeTursoUrl(TURSO_URL);

function wrapArg(val) {
  if (val === null || val === undefined) return { type: 'null' };
  if (typeof val === 'number') return { type: 'integer', value: String(val) };
  return { type: 'text', value: String(val) };
}

async function tursoExecute(sql, params) {
  const requests = [
    { type: 'execute', stmt: { sql, args: (params || []).map(wrapArg) } },
    { type: 'close' },
  ];

  const res = await fetch(TURSO_PIPELINE_URL, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${TURSO_AUTH_TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ requests }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Turso HTTP ${res.status}: ${text}`);
  }

  const data = await res.json();
  const result = data.results && data.results[0];
  if (!result || result.type === 'error') {
    throw new Error(`Turso error: ${JSON.stringify(result)}`);
  }

  return result.response?.result?.rows || [];
}

function parseCell(cell) {
  if (cell && typeof cell === 'object' && 'value' in cell) return cell.value;
  return cell;
}

function ensureNumber(value, fallback = 0) {
  if (value === null || value === undefined || value === '') return fallback;
  const num = Number(value);
  return Number.isNaN(num) ? fallback : num;
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function loadCursors() {
  try {
    const raw = fs.readFileSync(CURSOR_PATH, 'utf8');
    return JSON.parse(raw);
  } catch (err) {
    return {};
  }
}

function saveCursors(cursors) {
  ensureDir(path.dirname(CURSOR_PATH));
  fs.writeFileSync(CURSOR_PATH, JSON.stringify(cursors, null, 2));
}

function buildSelectSql(table) {
  return (
    `SELECT ${table.columns.join(', ')}, ${table.cursorExpr} AS cursor_ts ` +
    `FROM ${table.name} WHERE ${table.cursorExpr} > ? ` +
    `ORDER BY ${table.cursorExpr} LIMIT ?`
  );
}

async function exportTable(table, cursors) {
  const cursorKey = table.name;
  let cursor = cursors[cursorKey] || DEFAULT_CURSOR;
  let wroteAny = false;

  const outFile = path.join(OUTPUT_DIR, `${table.name}.ndjson`);
  if (fs.existsSync(outFile)) fs.unlinkSync(outFile);

  for (let page = 0; page < MAX_PAGES; page += 1) {
    const rows = await tursoExecute(table.selectSql, [cursor, BATCH_SIZE]);
    if (!rows.length) break;

    const ndjson = rows
      .map((row) => {
        const mapped = {};
        for (let i = 0; i < table.columns.length; i += 1) {
          mapped[table.columns[i]] = parseCell(row[i]);
        }
        for (const key of table.numericColumns) {
          if (key in mapped) mapped[key] = ensureNumber(mapped[key]);
        }
        return JSON.stringify(mapped);
      })
      .join('\n');

    fs.appendFileSync(outFile, ndjson + '\n');
    wroteAny = true;

    const lastCursor = parseCell(rows[rows.length - 1][table.columns.length]);
    if (lastCursor) cursor = lastCursor;

    if (rows.length < BATCH_SIZE) break;
  }

  if (wroteAny) {
    cursors[cursorKey] = cursor;
  }
}

const TABLES = [
  {
    name: 'analytics_events',
    columns: ['id', 'user_id', 'session_id', 'event_type', 'event_data', 'created_at'],
    cursorExpr: 'created_at',
    numericColumns: [],
  },
  {
    name: 'level_analytics',
    columns: ['id', 'session_id', 'level_number', 'kites_cut', 'play_time_seconds', 'outcome', 'started_at', 'ended_at'],
    cursorExpr: 'started_at',
    numericColumns: ['level_number', 'kites_cut', 'play_time_seconds'],
  },
  {
    name: 'events',
    columns: ['id', 'user_id', 'session_id', 'event_type', 'event_data', 'created_at', 'deleted_at'],
    cursorExpr: 'created_at',
    numericColumns: [],
  },
  {
    name: 'level_results',
    columns: [
      'id',
      'session_id',
      'level_number',
      'started_at',
      'ended_at',
      'kites_cut',
      'enemies_at_start',
      'outcome',
      'play_time_seconds',
      'deleted_at',
    ],
    cursorExpr: 'started_at',
    numericColumns: ['level_number', 'kites_cut', 'enemies_at_start', 'play_time_seconds'],
  },
  {
    name: 'users',
    columns: [
      'id',
      'display_name',
      'is_anonymous',
      'created_at',
      'updated_at',
      'total_games',
      'total_wins',
      'highest_level',
      'deleted_at',
      'email',
      'avatar_url',
      'auth_provider',
      'coins',
      'experience_points',
      'level',
      'highest_level_reached',
      'total_play_time_seconds',
      'last_platform',
      'fcm_token',
      'last_login_at',
      'total_kites_cut',
    ],
    cursorExpr: 'COALESCE(updated_at, created_at)',
    numericColumns: [
      'is_anonymous',
      'total_games',
      'total_wins',
      'highest_level',
      'coins',
      'experience_points',
      'level',
      'highest_level_reached',
      'total_play_time_seconds',
      'total_kites_cut',
    ],
  },
  {
    name: 'game_sessions',
    columns: [
      'id',
      'user_id',
      'started_at',
      'ended_at',
      'final_level',
      'total_kites_cut',
      'total_play_time_seconds',
      'outcome',
      'deleted_at',
      'rewarded_kites_cut',
      'rewarded_play_time_seconds',
      'updated_at',
      'total_games_rewarded',
      'rewarded_final_level',
    ],
    cursorExpr: 'COALESCE(updated_at, started_at)',
    numericColumns: [
      'final_level',
      'total_kites_cut',
      'total_play_time_seconds',
      'rewarded_kites_cut',
      'rewarded_play_time_seconds',
      'total_games_rewarded',
      'rewarded_final_level',
    ],
  },
  {
    name: 'leaderboard',
    columns: ['user_id', 'display_name', 'highest_level', 'total_kites_cut', 'best_time_seconds', 'updated_at'],
    cursorExpr: 'updated_at',
    numericColumns: ['highest_level', 'total_kites_cut', 'best_time_seconds'],
  },
];

for (const table of TABLES) {
  table.selectSql = buildSelectSql(table);
}

async function main() {
  ensureDir(OUTPUT_DIR);
  const cursors = loadCursors();

  for (const table of TABLES) {
    await exportTable(table, cursors);
  }

  saveCursors(cursors);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
