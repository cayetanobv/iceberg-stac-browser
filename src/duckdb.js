/**
 * DuckDB-WASM initialization and query runner.
 * Lazy singleton — only initialized on first use.
 * Isolated module, no Vue dependency.
 */

let db = null;
let conn = null;

export async function initDuckDB() {
  if (conn) return conn;
  const duckdb = await import('@duckdb/duckdb-wasm');
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);
  const worker = new Worker(bundle.mainWorker);
  const logger = new duckdb.ConsoleLogger();
  db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  conn = await db.connect();
  await conn.query("LOAD iceberg; LOAD httpfs; LOAD spatial;");
  await conn.query("SET unsafe_enable_version_guessing = true;");
  await conn.query("SET geometry_always_xy = true;");
  return conn;
}

export async function setGCSToken(token) {
  const c = await initDuckDB();
  await c.query(`CREATE OR REPLACE SECRET (TYPE GCS, TOKEN '${token.replace(/'/g, "''")}');`);
}

export async function setS3Credentials({ accessKeyId, secretAccessKey, region, sessionToken }) {
  const c = await initDuckDB();
  let sql = `CREATE OR REPLACE SECRET (TYPE S3, KEY_ID '${accessKeyId}', SECRET '${secretAccessKey}'`;
  if (region) sql += `, REGION '${region}'`;
  if (sessionToken) sql += `, SESSION_TOKEN '${sessionToken}'`;
  sql += ");";
  await c.query(sql);
}

export async function query(sql) {
  const c = await initDuckDB();
  const result = await c.query(sql);
  return arrowToObjects(result);
}

export async function previewTable(href, limit = 100) {
  return query(`
    SELECT * EXCLUDE (geometry, bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax)
    FROM iceberg_scan('${href}', allow_moved_paths := true)
    LIMIT ${limit}
  `);
}

export async function getSnapshots(href) {
  return query(`SELECT * FROM iceberg_snapshots('${href}')`);
}

export async function sampleGeometries(href, limit = 500) {
  return query(`
    SELECT ST_AsGeoJSON(ST_GeomFromWKB(geometry)) as geojson
    FROM iceberg_scan('${href}', allow_moved_paths := true)
    WHERE geometry IS NOT NULL
    LIMIT ${limit}
  `);
}

function arrowToObjects(arrowResult) {
  const columns = arrowResult.schema.fields.map(f => f.name);
  const rows = [];
  for (const batch of arrowResult.batches) {
    for (let i = 0; i < batch.numRows; i++) {
      const row = {};
      for (const col of columns) {
        const val = batch.getChild(col)?.get(i);
        row[col] = val instanceof Object && typeof val.toString === 'function' && !(val instanceof Date)
          ? val.toString()
          : val;
      }
      rows.push(row);
    }
  }
  return { columns, rows, numRows: rows.length };
}
