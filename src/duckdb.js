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

  // Fetch worker script as blob to avoid cross-origin Worker restriction
  const workerResponse = await fetch(bundle.mainWorker);
  const workerBlob = new Blob([await workerResponse.text()], { type: 'application/javascript' });
  const workerUrl = URL.createObjectURL(workerBlob);
  const worker = new Worker(workerUrl);

  const logger = new duckdb.ConsoleLogger();
  db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  conn = await db.connect();
  for (const ext of ['httpfs', 'iceberg', 'spatial']) {
    try {
      await conn.query(`LOAD ${ext};`);
      console.log(`[duckdb] Loaded extension: ${ext}`);
    } catch (err) {
      console.warn(`[duckdb] Failed to load extension '${ext}':`, err.message);
    }
  }
  return conn;
}

export async function listExtensions() {
  const c = await initDuckDB();
  const result = await c.query("SELECT extension_name, loaded, installed, install_path FROM duckdb_extensions() ORDER BY extension_name;");
  return arrowToObjects(result);
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

/**
 * Convert cloud storage URIs to HTTPS URLs.
 * DuckDB-WASM httpfs cannot resolve gs:// or s3:// — only https://.
 */
function toHttpUrl(href) {
  if (href.startsWith('gs://')) {
    return href.replace('gs://', 'https://storage.googleapis.com/');
  }
  if (href.startsWith('s3://')) {
    return href.replace(/^s3:\/\/([^/]+)/, 'https://$1.s3.amazonaws.com');
  }
  return href;
}

/**
 * Resolve the latest Iceberg metadata version for a table.
 *
 * Enterprise metastores (BigLake, Glue, etc.) don't write version-hint.text
 * files — they manage metadata via catalog APIs. Since DuckDB-WASM can't talk
 * to REST catalogs, we list the metadata directory via the cloud storage API
 * and find the latest metadata file ourselves.
 *
 * Returns the version string to pass as `version` param to iceberg_scan(),
 * e.g. "00003-69bbdf9d-0000-24b6-b7ab-b8db38fbc632"
 */
export async function resolveIcebergVersion(href) {
  const match = href.match(/^gs:\/\/([^/]+)\/(.+)$/);
  if (match) {
    return resolveGCSVersion(match[1], match[2]);
  }
  return null;
}

async function resolveGCSVersion(bucket, prefix) {
  const metadataPrefix = `${prefix}/metadata/`;
  const url = `https://storage.googleapis.com/storage/v1/b/${bucket}/o?prefix=${encodeURIComponent(metadataPrefix)}&delimiter=/`;
  try {
    const resp = await fetch(url);
    if (!resp.ok) return null;
    const data = await resp.json();
    const metadataFiles = (data.items || [])
      .map(item => item.name.split('/').pop())
      .filter(name => name.endsWith('.metadata.json'))
      .sort();
    if (metadataFiles.length === 0) return null;
    // Latest is last (sorted by sequence number prefix like 00003-...)
    // Extract version = filename without .metadata.json suffix
    const latest = metadataFiles[metadataFiles.length - 1];
    return latest.replace('.metadata.json', '');
  } catch {
    return null;
  }
}

/**
 * Build the iceberg_scan() SQL expression with proper version resolution.
 */
function icebergScanExpr(href, version) {
  const url = toHttpUrl(href);
  if (version) {
    return `iceberg_scan('${url}', allow_moved_paths := true, version_name_format := '%s%s.metadata.json', version := '${version}')`;
  }
  return `iceberg_scan('${url}', allow_moved_paths := true)`;
}

export function buildDefaultSQL(href, version) {
  const url = toHttpUrl(href);
  if (version) {
    return `SELECT *\nFROM iceberg_scan('${url}', allow_moved_paths := true, version_name_format := '%s%s.metadata.json', version := '${version}')\nLIMIT 100`;
  }
  return `SELECT *\nFROM iceberg_scan('${url}', allow_moved_paths := true)\nLIMIT 100`;
}

export async function previewTable(href, version, limit = 100) {
  const scan = icebergScanExpr(href, version);
  return query(`
    SELECT * EXCLUDE (geometry, bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax)
    FROM ${scan}
    LIMIT ${limit}
  `);
}

export async function getSnapshots(href, version) {
  const url = toHttpUrl(href);
  if (version) {
    return query(`SELECT * FROM iceberg_snapshots('${url}', version_name_format := '%s%s.metadata.json', version := '${version}')`);
  }
  return query(`SELECT * FROM iceberg_snapshots('${url}')`);
}

export async function sampleGeometries(href, version, limit = 500) {
  const scan = icebergScanExpr(href, version);
  return query(`
    SELECT ST_AsGeoJSON(ST_GeomFromWKB(geometry)) as geojson
    FROM ${scan}
    WHERE geometry IS NOT NULL
    LIMIT ${limit}
  `);
}

/**
 * Export a SQL query result as GeoParquet bytes.
 *
 * DuckDB's spatial extension writes proper GeoParquet metadata (including CRS)
 * when it sees a GEOMETRY-typed column. Iceberg stores geometry as WKB binary,
 * so we cast it via ST_GeomFromWKB() to produce a valid GeoParquet file.
 *
 * Set `convertGeometry: false` to skip the conversion (e.g. for non-spatial queries).
 */
export async function exportToParquet(sql, { convertGeometry = true } = {}) {
  const c = await initDuckDB();
  const filename = `export_${Date.now()}.parquet`;

  let exportSql = sql;
  if (convertGeometry) {
    // Check if the source query has a geometry column
    try {
      const probe = await c.query(`SELECT * FROM (${sql}) AS __probe LIMIT 0`);
      const hasGeom = probe.schema.fields.some(f => f.name === 'geometry');
      if (hasGeom) {
        // Convert WKB binary → native GEOMETRY so DuckDB writes
        // the GeoParquet 'geo' metadata key (with CRS) in the Parquet footer
        exportSql = `
          SELECT * REPLACE (ST_GeomFromWKB(geometry) AS geometry)
          FROM (${sql}) AS __src
        `;
      }
    } catch {
      // Probe failed, export as-is
    }
  }

  await c.query(`COPY (${exportSql}) TO '${filename}' (FORMAT PARQUET);`);
  const buffer = await db.copyFileToBuffer(filename);
  // Clean up the file
  try { await db.dropFile(filename); } catch { /* ignore */ }
  return buffer;
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
