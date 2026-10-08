// Support for the STAC Iceberg extension
// (https://github.com/portolan-sdi/stac-iceberg-extension), v1.1.0, with a
// fallback for collections written against v1.0.0.
//
// Everything here is pure: no Vue, no DuckDB. The component decides when to
// fetch and when to start DuckDB-WASM; this module only reads the collection
// and the Iceberg metadata document.

// The media type v1.1.0 gives the metadata.json asset. It is unregistered, and
// query engines never dispatch on it: it is a hint for STAC clients only.
export const ICEBERG_MEDIA_TYPE = 'application/vnd.apache.iceberg+json';

// Media types earlier drafts used. A v1.0.0 asset pointed at the table
// directory, not at a metadata.json.
export const LEGACY_ICEBERG_MEDIA_TYPES = ['application/x-iceberg', 'application/x-iceberg+json'];

export const ICEBERG_FIELDS = [
  'iceberg:catalog_type',
  'iceberg:catalog_uri',
  'iceberg:rest_prefix',
  'iceberg:authorization_type',
  'iceberg:table_id',
  'iceberg:metadata_location',
  'iceberg:format_version',
  'iceberg:current_snapshot_id',
  'iceberg:partition_spec',
];

// Catalog backends whose server resolves the current metadata. `static` is
// the Portolan convention for a serverless table on object storage.
export const MANAGED_CATALOG_TYPES = ['sql', 'rest', 'glue', 'hive', 'dynamodb'];

const METADATA_FILE = /\.metadata\.json$/i;

function mediaType(asset) {
  return typeof asset?.type === 'string' ? asset.type.split(';')[0].trim().toLowerCase() : '';
}

export function isIcebergAsset(asset) {
  const type = mediaType(asset);
  return type === ICEBERG_MEDIA_TYPE || LEGACY_ICEBERG_MEDIA_TYPES.includes(type);
}

export function hasIcebergMetadata(stac) {
  if (!stac || typeof stac !== 'object') {
    return false;
  }
  if (ICEBERG_FIELDS.some(field => stac[field] !== undefined)) {
    return true;
  }
  return Object.values(stac.assets || {}).some(isIcebergAsset);
}

/**
 * Rewrite an object-store URI to the HTTPS form DuckDB-WASM httpfs can read.
 * The extension allows either form, and a writer that reports what PyIceberg
 * reports publishes `gs://` or `s3://`.
 */
export function toHttpUrl(href) {
  if (typeof href !== 'string') {
    return href;
  }
  if (href.startsWith('gs://')) {
    return href.replace('gs://', 'https://storage.googleapis.com/');
  }
  if (href.startsWith('s3://')) {
    return href.replace(/^s3:\/\/([^/]+)/, 'https://$1.s3.amazonaws.com');
  }
  return href;
}

/**
 * The inverse rewrite, for PyIceberg and BigQuery, which expect the
 * object-store scheme. Returns null when the host is not a known bucket host.
 */
export function toObjectStoreUri(href) {
  if (typeof href !== 'string') {
    return null;
  }
  let match = href.match(/^https:\/\/storage\.googleapis\.com\/([^/]+)\/(.*)$/);
  if (match) {
    return `gs://${match[1]}/${match[2]}`;
  }
  match = href.match(/^https:\/\/([^./]+)\.s3(?:[.-][a-z0-9-]+)?\.amazonaws\.com\/(.*)$/);
  if (match) {
    return `s3://${match[1]}/${match[2]}`;
  }
  return null;
}

/**
 * The S3-compatible store a table lives on, when it is not AWS.
 *
 * A table written to a self-hosted S3 store (MinIO, R2, a lakehouse box)
 * records `s3://bucket/...` paths, which DuckDB and PyIceberg send to AWS by
 * default. The HTTPS URL the collection gives for the metadata file names the
 * real host. `tableLocation` is the `location` in metadata.json.
 */
export function inferS3Storage(metadataUrl, tableLocation) {
  const loc = typeof tableLocation === 'string' ? tableLocation.match(/^s3a?:\/\/([^/]+)/) : null;
  if (!loc || typeof metadataUrl !== 'string') {
    return null;
  }
  let url;
  try {
    url = new URL(metadataUrl);
  } catch {
    return null;
  }
  if (!/^https?:$/.test(url.protocol) || /(^|\.)amazonaws\.com$/.test(url.hostname)) {
    return null;
  }
  const bucket = loc[1];
  const path = decodeURIComponent(url.pathname);
  let urlStyle;
  let endpoint;
  let key;
  if (path.startsWith(`/${bucket}/`)) {
    urlStyle = 'path';
    endpoint = url.host;
    key = path.slice(bucket.length + 2);
  }
  else if (url.host.startsWith(`${bucket}.`)) {
    urlStyle = 'vhost';
    endpoint = url.host.slice(bucket.length + 1);
    key = path.slice(1);
  }
  else {
    return null;
  }
  return {
    bucket,
    endpoint,
    urlStyle,
    useSsl: url.protocol === 'https:',
    metadataUri: `s3://${bucket}/${key}`,
  };
}

function isAbsolute(href) {
  return /^[a-z][a-z0-9+.-]*:/i.test(href);
}

function resolve(href, base) {
  if (!href || isAbsolute(href) || !base) {
    return href || null;
  }
  try {
    return new URL(href, base).toString();
  } catch {
    return href;
  }
}

/**
 * The base a relative `iceberg:metadata_location` resolves against: the
 * collection's absolute `self` link, else the URL that served it.
 */
export function collectionBaseUrl(stac, servedUrl = null) {
  const links = Array.isArray(stac?.links) ? stac.links : [];
  const self = links.find(link => link?.rel === 'self' && typeof link.href === 'string');
  if (self && /^https?:\/\//i.test(self.href)) {
    return self.href;
  }
  return servedUrl;
}

/**
 * Normalize a partition spec entry. v1.1.0 mirrors the Iceberg shape
 * (`name`, `transform`, `source-id`, `field-id`); v1.0.0 used `field`.
 */
export function normalizePartitionField(entry) {
  if (!entry || typeof entry !== 'object') {
    return null;
  }
  const name = entry.name ?? entry.field;
  if (typeof name !== 'string' || typeof entry.transform !== 'string') {
    return null;
  }
  return {
    name,
    transform: entry.transform,
    sourceId: Number.isInteger(entry['source-id']) ? entry['source-id'] : null,
    fieldId: Number.isInteger(entry['field-id']) ? entry['field-id'] : null,
  };
}

function snapshotIdToString(value) {
  if (typeof value === 'string' && /^-?[0-9]+$/.test(value)) {
    return value;
  }
  // v1.0.0 wrote a JSON number. Above 2^53 it has already lost precision,
  // so it is shown, but it is not compared with the table.
  if (typeof value === 'number' && Number.isInteger(value)) {
    return String(value);
  }
  return null;
}

/**
 * Read the Iceberg extension fields of a collection into one object.
 *
 * `servedUrl` is the URL STAC Browser loaded the collection from.
 */
export function getIcebergInfo(stac, servedUrl = null) {
  if (!hasIcebergMetadata(stac)) {
    return null;
  }
  const assets = Object.entries(stac.assets || {})
    .filter(([, asset]) => isIcebergAsset(asset))
    .map(([key, asset]) => ({ key, ...asset }));
  // Prefer the v1.1.0 asset over a legacy one, and the conventional key.
  assets.sort((a, b) => {
    const score = x => (mediaType(x) === ICEBERG_MEDIA_TYPE ? 2 : 0) + (x.key === 'iceberg' ? 1 : 0);
    return score(b) - score(a);
  });
  const asset = assets[0] || null;
  const base = collectionBaseUrl(stac, servedUrl);

  const catalogType = typeof stac['iceberg:catalog_type'] === 'string' ? stac['iceberg:catalog_type'] : null;
  const declaredLocation = typeof stac['iceberg:metadata_location'] === 'string' ? stac['iceberg:metadata_location'] : null;
  const metadataLocation = resolve(declaredLocation, base);
  const assetHref = asset ? resolve(asset.href, base) : null;
  const legacyAsset = Boolean(asset) && LEGACY_ICEBERG_MEDIA_TYPES.includes(mediaType(asset));

  // What DuckDB reads. iceberg:metadata_location is the field the extension
  // names as the reader's handle; the asset is the fallback.
  let scanSource = null;
  let scanMode = null;
  if (metadataLocation) {
    scanSource = toHttpUrl(metadataLocation);
    scanMode = 'metadata';
  }
  else if (assetHref && METADATA_FILE.test(assetHref)) {
    scanSource = toHttpUrl(assetHref);
    scanMode = 'metadata';
  }
  else if (assetHref && legacyAsset) {
    // v1.0.0 pointed the asset at the table directory.
    scanSource = toHttpUrl(assetHref.replace(/\/+$/, ''));
    scanMode = 'directory';
  }

  const partitionSpec = Array.isArray(stac['iceberg:partition_spec'])
    ? stac['iceberg:partition_spec'].map(normalizePartitionField).filter(Boolean)
    : [];

  const snapshotRaw = stac['iceberg:current_snapshot_id'];
  const warnings = [];
  if (catalogType === 'static' && !declaredLocation) {
    warnings.push('staticWithoutLocation');
  }
  if (typeof snapshotRaw === 'number') {
    warnings.push('numericSnapshotId');
  }
  if (Array.isArray(stac['iceberg:partition_spec']) && stac['iceberg:partition_spec'].some(p => p && 'field' in p && !('name' in p))) {
    warnings.push('legacyPartitionSpec');
  }
  if (legacyAsset) {
    warnings.push('legacyMediaType');
  }
  if (declaredLocation && /^(gs|s3):\/\//.test(metadataLocation)) {
    warnings.push('objectStoreLocation');
  }

  return {
    catalogType,
    managed: MANAGED_CATALOG_TYPES.includes(catalogType),
    catalogUri: typeof stac['iceberg:catalog_uri'] === 'string' ? stac['iceberg:catalog_uri'] : null,
    restPrefix: typeof stac['iceberg:rest_prefix'] === 'string' ? stac['iceberg:rest_prefix'] : null,
    authorizationType: typeof stac['iceberg:authorization_type'] === 'string' ? stac['iceberg:authorization_type'] : null,
    tableId: typeof stac['iceberg:table_id'] === 'string' ? stac['iceberg:table_id'] : null,
    formatVersion: Number.isInteger(stac['iceberg:format_version']) ? stac['iceberg:format_version'] : null,
    currentSnapshotId: snapshotIdToString(snapshotRaw),
    snapshotIdIsExact: typeof snapshotRaw === 'string',
    partitionSpec,
    metadataLocation,
    asset,
    assetHref,
    // The asset's roles tell whether the table is the only distribution
    // (`data` + `metadata`) or sits beside a GeoParquet data asset.
    icebergOnly: Boolean(asset?.roles?.includes?.('data')),
    scanSource,
    scanMode,
    queryable: Boolean(scanSource),
    primaryGeometry: typeof stac['table:primary_geometry'] === 'string' ? stac['table:primary_geometry'] : null,
    tableColumns: Array.isArray(stac['table:columns']) ? stac['table:columns'] : [],
    rowCount: Number.isInteger(stac['table:row_count']) ? stac['table:row_count'] : null,
    projCode: typeof stac['proj:code'] === 'string' ? stac['proj:code'] : null,
    warnings,
  };
}

export function formatPartitionField(field) {
  return field.transform === 'identity' ? field.name : `${field.name} (${field.transform})`;
}

/**
 * Parse an Iceberg metadata.json without losing 64-bit snapshot ids.
 * JSON.parse stores numbers as doubles, which round above 2^53, so the id
 * fields are quoted before parsing.
 */
export function parseTableMetadata(text) {
  const quoted = text.replace(
    /("(?:current-snapshot-id|snapshot-id|parent-snapshot-id)"\s*:\s*)(-?\d+)/g,
    '$1"$2"'
  );
  return JSON.parse(quoted);
}

/**
 * Summarize a parsed metadata.json: what the browser shows and what it
 * compares with the collection.
 */
export function summarizeTableMetadata(metadata) {
  if (!metadata || typeof metadata !== 'object') {
    return null;
  }
  let schemas = Array.isArray(metadata.schemas) ? metadata.schemas : [];
  if (schemas.length === 0 && metadata.schema) {
    // Format version 1 carries a single `schema`.
    schemas = [metadata.schema];
  }
  const currentSchemaId = metadata['current-schema-id'];
  const schema = schemas.find(s => s['schema-id'] === currentSchemaId) || schemas[schemas.length - 1] || null;
  const fields = Array.isArray(schema?.fields) ? schema.fields.map(f => ({
    id: f.id,
    name: f.name,
    type: typeof f.type === 'string' ? f.type : (f.type?.type || 'struct'),
    required: Boolean(f.required),
    doc: f.doc || null,
  })) : [];

  const specs = Array.isArray(metadata['partition-specs']) ? metadata['partition-specs'] : [];
  const spec = specs.find(s => s['spec-id'] === metadata['default-spec-id']) || specs[specs.length - 1] || null;
  const partitionSpec = Array.isArray(spec?.fields) ? spec.fields.map(normalizePartitionField).filter(Boolean) : [];

  const currentSnapshotId = metadata['current-snapshot-id'] != null ? String(metadata['current-snapshot-id']) : null;
  const snapshots = (Array.isArray(metadata.snapshots) ? metadata.snapshots : []).map(s => ({
    id: String(s['snapshot-id']),
    parentId: s['parent-snapshot-id'] != null ? String(s['parent-snapshot-id']) : null,
    sequenceNumber: s['sequence-number'] ?? null,
    timestamp: Number.isFinite(s['timestamp-ms']) ? new Date(s['timestamp-ms']) : null,
    operation: s.summary?.operation || null,
    totalRecords: s.summary?.['total-records'] != null ? Number(s.summary['total-records']) : null,
    totalFiles: s.summary?.['total-data-files'] != null ? Number(s.summary['total-data-files']) : null,
    addedRecords: s.summary?.['added-records'] != null ? Number(s.summary['added-records']) : null,
    current: currentSnapshotId !== null && String(s['snapshot-id']) === currentSnapshotId,
  })).sort((a, b) => (b.timestamp?.getTime() ?? 0) - (a.timestamp?.getTime() ?? 0));

  return {
    formatVersion: Number.isInteger(metadata['format-version']) ? metadata['format-version'] : null,
    tableUuid: metadata['table-uuid'] || null,
    location: metadata.location || null,
    lastUpdated: Number.isFinite(metadata['last-updated-ms']) ? new Date(metadata['last-updated-ms']) : null,
    currentSnapshotId,
    fields,
    partitionSpec,
    snapshots,
    properties: metadata.properties && typeof metadata.properties === 'object' ? metadata.properties : {},
  };
}

/**
 * Parse an Iceberg geometry/geography type string such as
 * `geometry(EPSG:3067)` or `geography(OGC:CRS84, spherical)`.
 * The parameter defaults to OGC:CRS84. PyIceberg 0.12 quotes it.
 */
export function parseGeoType(type) {
  if (typeof type !== 'string') {
    return null;
  }
  const match = type.trim().match(/^(geometry|geography)\s*(?:\((.*)\))?$/i);
  if (!match) {
    return null;
  }
  const params = (match[2] || '').split(',').map(p => p.trim().replace(/^['"]|['"]$/g, '')).filter(Boolean);
  return {
    kind: match[1].toLowerCase(),
    crs: params[0] || 'OGC:CRS84',
    edges: match[1].toLowerCase() === 'geography' ? (params[1] || 'spherical') : null,
    quoted: /\(\s*['"]/.test(type),
  };
}

/**
 * Compare the collection with the table it points at. The spec calls a
 * collection whose pinned snapshot differs from the table's "stale".
 */
export function compareWithTable(info, summary) {
  const issues = [];
  if (!info || !summary) {
    return issues;
  }
  if (info.currentSnapshotId && summary.currentSnapshotId && info.snapshotIdIsExact
    && info.currentSnapshotId !== summary.currentSnapshotId) {
    const known = summary.snapshots.some(s => s.id === info.currentSnapshotId);
    issues.push({ code: known ? 'snapshotBehind' : 'snapshotUnknown', stac: info.currentSnapshotId, table: summary.currentSnapshotId });
  }
  if (info.formatVersion && summary.formatVersion && info.formatVersion !== summary.formatVersion) {
    issues.push({ code: 'formatVersion', stac: info.formatVersion, table: summary.formatVersion });
  }
  if (summary.fields.some(f => parseGeoType(f.type)?.kind === 'geography')) {
    issues.push({ code: 'geography' });
  }
  if (summary.fields.some(f => parseGeoType(f.type)?.quoted)) {
    issues.push({ code: 'quotedCrs' });
  }
  if (info.rowCount !== null) {
    const current = summary.snapshots.find(s => s.current);
    if (current?.totalRecords != null && current.totalRecords !== info.rowCount) {
      issues.push({ code: 'rowCount', stac: info.rowCount, table: current.totalRecords });
    }
  }
  return issues;
}

/**
 * Quote a string as a SQL literal.
 */
export function sqlString(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

/**
 * Quote an identifier for DuckDB.
 */
export function sqlIdent(name) {
  return `"${String(name).replace(/"/g, '""')}"`;
}

/**
 * The DuckDB table function that reads the table.
 *
 * `snapshotId` pins a snapshot (time travel). `version` is the metadata file
 * a v1.0.0 directory resolves to.
 */
export function icebergScanExpr(info, { snapshotId = null, version = null } = {}) {
  if (!info?.scanSource) {
    return null;
  }
  const args = [sqlString(info.scanSource)];
  if (info.scanMode === 'directory') {
    args.push('allow_moved_paths := true');
    if (version) {
      args.push(`version_name_format := '%s%s.metadata.json'`, `version := ${sqlString(version)}`);
    }
  }
  if (snapshotId && /^-?[0-9]+$/.test(String(snapshotId))) {
    args.push(`snapshot_from_id := ${snapshotId}`);
  }
  return `iceberg_scan(${args.join(', ')})`;
}

export function defaultSql(info, options = {}) {
  const scan = icebergScanExpr(info, options);
  return scan ? `SELECT *\nFROM ${scan}\nLIMIT 100` : '';
}

/**
 * A BigQuery table name from the Iceberg table id. The id comes from the
 * collection, which may be hostile, so only letters, digits and underscores
 * reach the copied snippet: a backtick would end the quoted identifier.
 */
export function bigQueryTableName(tableId) {
  const name = String(tableId || '').split('.').pop().replace(/[^A-Za-z0-9_]/g, '_');
  return name || 'iceberg_table';
}

/**
 * Code a user copies to open the table outside the browser. Only snippets the
 * collection has the fields for are returned.
 */
export function connectionSnippets(info, storage = null) {
  if (!info) {
    return [];
  }
  const snippets = [];
  const location = info.metadataLocation || (info.scanMode === 'metadata' ? info.assetHref : null);
  const httpLocation = toHttpUrl(location);
  if (httpLocation) {
    const duckdb = ['INSTALL iceberg; LOAD iceberg;', 'INSTALL spatial; LOAD spatial;', ''];
    if (storage) {
      duckdb.push(
        '-- The table\'s files are on an S3-compatible store, not AWS.',
        'CREATE SECRET (',
        '  TYPE s3,',
        `  ENDPOINT ${sqlString(storage.endpoint)},`,
        `  URL_STYLE ${sqlString(storage.urlStyle)},`,
        `  USE_SSL ${storage.useSsl},`,
        `  SCOPE ${sqlString(`s3://${storage.bucket}`)}`,
        ');',
        ''
      );
    }
    duckdb.push(`SELECT * FROM iceberg_scan(${sqlString(httpLocation)}) LIMIT 10;`);
    snippets.push({ id: 'duckdb-scan', title: 'DuckDB', language: 'sql', code: duckdb.join('\n') });

    // PyIceberg 0.12 opens no https:// metadata file, only an object-store URI.
    const objectStore = storage?.metadataUri || toObjectStoreUri(httpLocation)
      || (/^(gs|s3):\/\//.test(location) ? location : null);
    if (objectStore) {
      const open = storage
        ? [
          'table = StaticTable.from_metadata(',
          `    ${JSON.stringify(objectStore)},`,
          `    properties={"s3.endpoint": ${JSON.stringify(`${storage.useSsl ? 'https' : 'http'}://${storage.endpoint}`)}, "s3.anonymous": "true"},`,
          ')',
        ]
        : [`table = StaticTable.from_metadata(${JSON.stringify(objectStore)})`];
      snippets.push({
        id: 'pyiceberg',
        title: 'PyIceberg',
        language: 'python',
        code: ['from pyiceberg.table import StaticTable', '', ...open, 'df = table.scan(limit=10).to_pandas()'].join('\n'),
      });
    }
    if (objectStore && objectStore.startsWith('gs://')) {
      snippets.push({
        id: 'bigquery',
        title: 'BigQuery',
        language: 'sql',
        code: [
          `CREATE EXTERNAL TABLE \`my_dataset.${bigQueryTableName(info.tableId)}\``,
          `OPTIONS (format = 'ICEBERG', uris = [${sqlString(objectStore)}]);`,
        ].join('\n'),
      });
    }
  }
  // ATTACH works against a REST surface: a REST catalog, or the pre-rendered
  // REST surface a static catalog may publish at iceberg:catalog_uri.
  if (info.catalogUri && info.tableId && ['rest', 'static'].includes(info.catalogType)) {
    const lines = ['INSTALL iceberg; LOAD iceberg;', ''];
    const options = ['TYPE iceberg', `ENDPOINT ${sqlString(info.catalogUri)}`];
    if (info.authorizationType === 'none') {
      options.push("AUTHORIZATION_TYPE 'none'");
    }
    else if (info.authorizationType === 'sigv4') {
      options.push("AUTHORIZATION_TYPE 'sigv4'");
    }
    else {
      lines.push(
        'CREATE SECRET iceberg_secret (',
        '  TYPE iceberg,',
        "  CLIENT_ID '<client id>',",
        "  CLIENT_SECRET '<client secret>',",
        `  OAUTH2_SERVER_URI ${sqlString(info.catalogUri.replace(/\/+$/, '') + '/v1/oauth/tokens')}`,
        ');',
        ''
      );
      options.push('SECRET iceberg_secret');
    }
    // The first argument is the warehouse; a prefixed surface takes its prefix.
    lines.push(
      `ATTACH ${sqlString(info.restPrefix || '')} AS lake (`,
      options.map(o => `  ${o}`).join(',\n'),
      ');',
      '',
      `SELECT * FROM lake.${info.tableId.split('.').map(sqlIdent).join('.')} LIMIT 10;`
    );
    snippets.push({
      id: 'duckdb-attach',
      title: 'DuckDB (ATTACH catalog)',
      language: 'sql',
      code: lines.join('\n'),
    });
  }
  return snippets;
}
