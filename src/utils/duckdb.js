// DuckDB-WASM runner for Iceberg tables.
//
// A lazy singleton: nothing loads until the user opens the query panel, so a
// collection page never pays for the WASM bundle it does not use.

import { sqlIdent, sqlString } from './iceberg.js';

let db = null;
let connPromise = null;

function toDisplayValue(value) {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === 'bigint') {
    return value.toString();
  }
  if (value instanceof Uint8Array) {
    return `<binary, ${value.byteLength} bytes>`;
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (typeof value === 'object') {
    if (typeof value.toJSON === 'function') {
      return JSON.stringify(value.toJSON(), (k, v) => (typeof v === 'bigint' ? v.toString() : v));
    }
    return String(value);
  }
  return value;
}

function arrowToObjects(table) {
  const fields = table.schema.fields;
  const columns = fields.map(f => f.name);
  const types = fields.map(f => String(f.type));
  const rows = [];
  for (const batch of table.batches) {
    const children = columns.map(name => batch.getChild(name));
    for (let i = 0; i < batch.numRows; i++) {
      const row = {};
      columns.forEach((name, c) => {
        let value = children[c]?.get(i);
        // Arrow timestamps arrive as epoch milliseconds.
        if (typeof value === 'number' && /^Timestamp/.test(types[c])) {
          value = new Date(value);
        }
        row[name] = toDisplayValue(value);
      });
      rows.push(row);
    }
  }
  return { columns, types, rows, numRows: rows.length };
}

async function createConnection() {
  const duckdb = await import('@duckdb/duckdb-wasm');
  const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());

  // A Worker cannot start from a cross-origin script, so load it as a blob.
  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts(${JSON.stringify(bundle.mainWorker)});`], { type: 'text/javascript' })
  );
  const worker = new Worker(workerUrl);
  db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerUrl);

  const conn = await db.connect();
  await Promise.all(['httpfs', 'iceberg', 'spatial'].map(ext =>
    conn.query(`LOAD ${ext};`).catch(error => {
      console.warn(`[duckdb] Failed to load extension '${ext}':`, error.message);
    })
  ));
  return conn;
}

export function initDuckDB() {
  if (!connPromise) {
    connPromise = createConnection().catch(error => {
      connPromise = null;
      throw error;
    });
  }
  return connPromise;
}

export async function setGcsToken(token) {
  const conn = await initDuckDB();
  await conn.query(`CREATE OR REPLACE SECRET iceberg_gcs (TYPE GCS, TOKEN ${sqlString(token)});`);
}

const ENDPOINT_SECRET = 'iceberg_s3_endpoint';

/**
 * Send `s3://bucket` paths to an S3-compatible endpoint instead of AWS,
 * reading anonymously, or drop that routing with `null`.
 *
 * The endpoint comes from the collection, and DuckDB lives for the whole
 * session. So only the table on screen has a routing: each call replaces the
 * previous one. Otherwise one collection could send another collection's
 * bucket to a host of its choice.
 */
export async function setS3Endpoint(storage) {
  const conn = await initDuckDB();
  await conn.query(`DROP SECRET IF EXISTS ${ENDPOINT_SECRET};`);
  if (!storage) {
    return;
  }
  const { bucket, endpoint, urlStyle, useSsl } = storage;
  await conn.query(`CREATE SECRET ${ENDPOINT_SECRET} (TYPE S3, ENDPOINT ${sqlString(endpoint)}, URL_STYLE ${sqlString(urlStyle)}, USE_SSL ${useSsl ? 'true' : 'false'}, SCOPE ${sqlString(`s3://${bucket}`)});`);
}

export async function setS3Credentials({ accessKeyId, secretAccessKey, region, sessionToken }) {
  const conn = await initDuckDB();
  const parts = [`TYPE S3`, `KEY_ID ${sqlString(accessKeyId)}`, `SECRET ${sqlString(secretAccessKey)}`];
  if (region) {
    parts.push(`REGION ${sqlString(region)}`);
  }
  if (sessionToken) {
    parts.push(`SESSION_TOKEN ${sqlString(sessionToken)}`);
  }
  await conn.query(`CREATE OR REPLACE SECRET iceberg_s3 (${parts.join(', ')});`);
}

// DuckDB-WASM 1.5.6 fails with "stoi: no conversion" on the CRS name
// OGC:CRS84. That is the default of a Parquet GEOMETRY column that declares
// no CRS, so it hits most Iceberg v3 tables. Native DuckDB 1.5.6 is not
// affected. With the GeoParquet conversion off the column still reads as a
// native GEOMETRY, and a column that declares a CRS keeps it, so the
// fallback is applied only after the error occurs.
const CRS84_BUG = /stoi: no conversion/;
let geoConversionDisabled = false;

async function run(sql) {
  const conn = await initDuckDB();
  try {
    return await conn.query(sql);
  } catch (error) {
    if (geoConversionDisabled || !CRS84_BUG.test(error?.message || '')) {
      throw error;
    }
    // Set the flag first so that concurrent queries do not retry twice.
    geoConversionDisabled = true;
    console.warn('[duckdb] OGC:CRS84 bug in DuckDB-WASM: retrying with enable_geoparquet_conversion = false');
    await conn.query('SET enable_geoparquet_conversion = false;');
    return conn.query(sql);
  }
}

export async function query(sql) {
  return arrowToObjects(await run(sql));
}

export async function engineVersion() {
  const { rows } = await query('SELECT version() AS version');
  return rows[0]?.version || null;
}

/**
 * Column names and DuckDB types of a relation, without reading rows.
 */
export async function describe(relation) {
  const { rows } = await query(`DESCRIBE SELECT * FROM ${relation}`);
  return rows.map(r => ({ name: r.column_name, type: String(r.column_type) }));
}

export function isGeometryType(type) {
  return /^geometry\b/i.test(type || '');
}

/**
 * The geometry column of a relation and how to turn it into a GEOMETRY value.
 *
 * Iceberg v3 hands DuckDB a native GEOMETRY. A v2 table built on the
 * GeoParquet convention stores WKB in a BLOB, which needs ST_GeomFromWKB.
 */
export function geometryColumn(columns, preferred = null) {
  const byName = name => columns.find(c => c.name === name);
  let column = preferred ? byName(preferred) : null;
  if (!column) {
    column = columns.find(c => isGeometryType(c.type))
      || byName('geometry') || byName('geom');
  }
  if (!column) {
    return null;
  }
  const native = isGeometryType(column.type);
  if (!native && !/^blob$/i.test(column.type)) {
    return null;
  }
  const ref = sqlIdent(column.name);
  return {
    name: column.name,
    native,
    expr: native ? ref : `ST_GeomFromWKB(${ref})`,
  };
}

/**
 * The first rows of the table, with the geometry shown as its type and a
 * short WKT so a wide polygon does not flood the table.
 */
export function previewRows(scan, columns, geometry, limit = 100) {
  const select = columns.map(c => {
    const ref = sqlIdent(c.name);
    if (geometry && c.name === geometry.name) {
      return `left(ST_AsText(${geometry.expr}), 120) AS ${ref}`;
    }
    return ref;
  });
  return query(`SELECT ${select.join(', ')} FROM ${scan} LIMIT ${Number(limit)}`);
}

/**
 * Geometries as GeoJSON strings, in the table's own CRS.
 */
export function sampleGeoJson(scan, geometry, limit = 1000) {
  return query(`
    SELECT ST_AsGeoJSON(${geometry.expr}) AS geojson
    FROM ${scan}
    WHERE ${sqlIdent(geometry.name)} IS NOT NULL
    LIMIT ${Number(limit)}
  `);
}

/**
 * Write a query result to Parquet and return the bytes.
 *
 * DuckDB writes GeoParquet metadata, with the CRS, only for a GEOMETRY
 * column, so a WKB BLOB geometry is converted first. The file is GeoParquet
 * 2.0: the `geo` key plus the native Parquet GEOMETRY type, the data file the
 * Portolan Iceberg convention expects.
 */
export async function exportToParquet(sql, { geometryName = null } = {}) {
  await initDuckDB();
  let exportSql = sql;
  try {
    const columns = await describe(`(${sql}) AS __probe`);
    const geometry = geometryColumn(columns, geometryName);
    if (geometry && !geometry.native) {
      exportSql = `SELECT * REPLACE (${geometry.expr} AS ${sqlIdent(geometry.name)}) FROM (${sql}) AS __src`;
    }
  } catch {
    // The probe failed: export the query as it is.
  }
  const filename = `export_${Date.now()}.parquet`;
  await db.registerEmptyFileBuffer(filename);
  await run(`COPY (${exportSql}) TO ${sqlString(filename)} (FORMAT PARQUET, GEOPARQUET_VERSION 'V2');`);
  const buffer = await db.copyFileToBuffer(filename);
  try {
    await db.dropFile(filename);
  } catch {
    // Nothing to clean up.
  }
  return buffer;
}

/**
 * Resolve the newest metadata file of a v1.0.0 table directory.
 *
 * v1.0.0 assets pointed at the table directory, and a managed catalog writes
 * no version-hint.text. On Google Cloud Storage the bucket listing API finds
 * the newest `*.metadata.json`. Returns the version name iceberg_scan takes,
 * or null.
 */
export async function resolveDirectoryVersion(directoryUrl) {
  const match = directoryUrl.match(/^https:\/\/storage\.googleapis\.com\/([^/]+)\/(.+)$/);
  if (!match) {
    return null;
  }
  const prefix = `${match[2].replace(/\/+$/, '')}/metadata/`;
  const url = `https://storage.googleapis.com/storage/v1/b/${match[1]}/o?prefix=${encodeURIComponent(prefix)}&delimiter=/`;
  try {
    const response = await fetch(url);
    if (!response.ok) {
      return null;
    }
    const data = await response.json();
    const files = (data.items || [])
      .map(item => item.name.split('/').pop())
      .filter(name => name.endsWith('.metadata.json'))
      .sort();
    return files.length > 0 ? files[files.length - 1].replace(/\.metadata\.json$/, '') : null;
  } catch {
    return null;
  }
}
