import { describe, it, expect } from 'vitest'
import {
  bigQueryTableName,
  compareWithTable,
  connectionSnippets,
  defaultSql,
  getIcebergInfo,
  hasIcebergMetadata,
  icebergScanExpr,
  inferS3Storage,
  isIcebergAsset,
  parseGeoType,
  parseTableMetadata,
  summarizeTableMetadata,
  toHttpUrl,
  toObjectStoreUri,
} from '../../src/utils/iceberg.js'

const EXT = 'https://schemas.portolan-sdi.org/incubating/iceberg/v1.1.0/schema.json'

// examples/portolan-collection.json of stac-iceberg-extension v1.1.0
const portolanCollection = () => ({
  type: 'Collection',
  id: 'boston-open-space',
  stac_extensions: [EXT],
  links: [
    { rel: 'self', href: 'https://data.example.org/catalog/boston-open-space/collection.json' },
  ],
  'table:columns': [
    { name: 'OBJECTID', type: 'int64' },
    { name: 'geometry', type: 'geometry' },
  ],
  'table:row_count': 1012,
  'table:primary_geometry': 'geometry',
  'iceberg:catalog_type': 'static',
  'iceberg:table_id': 'portolan.boston_open_space',
  'iceberg:metadata_location': './iceberg/metadata/v1.metadata.json',
  'iceberg:format_version': 3,
  'iceberg:current_snapshot_id': '4358210771845678999',
  assets: {
    data: { href: './boston-open-space.parquet', type: 'application/vnd.apache.parquet', roles: ['data'] },
    iceberg: { href: './iceberg/metadata/v1.metadata.json', type: 'application/vnd.apache.iceberg+json', roles: ['metadata'] },
  },
})

// examples/collection.json of stac-iceberg-extension v1.1.0
const icebergOnlyCollection = () => ({
  type: 'Collection',
  id: 'kunta_2025',
  stac_extensions: [EXT],
  links: [{ rel: 'self', href: './collection.json' }],
  'proj:code': 'EPSG:3067',
  'iceberg:catalog_type': 'static',
  'iceberg:catalog_uri': 'https://storage.googleapis.com/example-bucket/finland',
  'iceberg:rest_prefix': 'sdi',
  'iceberg:authorization_type': 'none',
  'iceberg:table_id': 'v3.kunta_2025',
  'iceberg:metadata_location': 'https://storage.googleapis.com/example-bucket/finland/data/v3/kunta_2025/metadata/v1.metadata.json',
  'iceberg:format_version': 3,
  'iceberg:current_snapshot_id': '1',
  assets: {
    iceberg: {
      href: 'https://storage.googleapis.com/example-bucket/finland/data/v3/kunta_2025/metadata/v1.metadata.json',
      type: 'application/vnd.apache.iceberg+json',
      roles: ['data', 'metadata'],
    },
  },
})

describe('detection', () => {
  it('recognizes the v1.1.0 and the legacy media types', () => {
    expect(isIcebergAsset({ type: 'application/vnd.apache.iceberg+json' })).toBe(true)
    expect(isIcebergAsset({ type: 'application/x-iceberg' })).toBe(true)
    expect(isIcebergAsset({ type: 'application/vnd.apache.parquet' })).toBe(false)
  })

  it('finds a collection by its fields alone, with no asset', () => {
    expect(hasIcebergMetadata({ 'iceberg:catalog_type': 'rest', 'iceberg:table_id': 'a.b' })).toBe(true)
    expect(hasIcebergMetadata({ assets: {} })).toBe(false)
  })
})

describe('getIcebergInfo', () => {
  it('resolves a relative metadata location against the absolute self link', () => {
    const info = getIcebergInfo(portolanCollection(), 'https://elsewhere.example/collection.json')
    expect(info.metadataLocation).toBe('https://data.example.org/catalog/boston-open-space/iceberg/metadata/v1.metadata.json')
    expect(info.scanSource).toBe(info.metadataLocation)
    expect(info.scanMode).toBe('metadata')
    expect(info.icebergOnly).toBe(false)
    expect(info.currentSnapshotId).toBe('4358210771845678999')
    expect(info.warnings).toEqual([])
  })

  it('falls back to the URL that served the collection when self is relative', () => {
    const info = getIcebergInfo(
      { ...portolanCollection(), links: [{ rel: 'self', href: './collection.json' }] },
      'https://cdn.example/c/collection.json'
    )
    expect(info.metadataLocation).toBe('https://cdn.example/c/iceberg/metadata/v1.metadata.json')
  })

  it('reads the static REST fields of an Iceberg-only distribution', () => {
    const info = getIcebergInfo(icebergOnlyCollection(), null)
    expect(info.icebergOnly).toBe(true)
    expect(info.restPrefix).toBe('sdi')
    expect(info.authorizationType).toBe('none')
    expect(info.projCode).toBe('EPSG:3067')
    expect(info.queryable).toBe(true)
  })

  it('rewrites an object-store metadata location for the browser', () => {
    const info = getIcebergInfo({
      'iceberg:catalog_type': 'static',
      'iceberg:metadata_location': 's3://bucket/t/metadata/v2.metadata.json',
    })
    expect(info.scanSource).toBe('https://bucket.s3.amazonaws.com/t/metadata/v2.metadata.json')
    expect(info.warnings).toContain('objectStoreLocation')
  })

  it('reports a static catalog without a metadata location', () => {
    const info = getIcebergInfo({ 'iceberg:catalog_type': 'static', 'iceberg:table_id': 'a.b' })
    expect(info.warnings).toContain('staticWithoutLocation')
    expect(info.queryable).toBe(false)
  })

  it('treats a managed catalog with no metadata file as not queryable', () => {
    const info = getIcebergInfo({
      'iceberg:catalog_type': 'rest',
      'iceberg:catalog_uri': 'https://catalog.example/iceberg',
      'iceberg:table_id': 'ns.t',
      'iceberg:authorization_type': 'oauth2',
    })
    expect(info.managed).toBe(true)
    expect(info.queryable).toBe(false)
  })

  it('normalizes v1.1.0 and v1.0.0 partition specs', () => {
    const info = getIcebergInfo({
      'iceberg:catalog_type': 'rest',
      'iceberg:partition_spec': [
        { name: 'geohash_3', transform: 'identity', 'source-id': 5, 'field-id': 1000 },
        { field: 'region', transform: 'bucket[16]' },
      ],
    })
    expect(info.partitionSpec).toEqual([
      { name: 'geohash_3', transform: 'identity', sourceId: 5, fieldId: 1000 },
      { name: 'region', transform: 'bucket[16]', sourceId: null, fieldId: null },
    ])
    expect(info.warnings).toContain('legacyPartitionSpec')
  })

  it('keeps a v1.0.0 numeric snapshot id, but marks it inexact', () => {
    const info = getIcebergInfo({ 'iceberg:catalog_type': 'rest', 'iceberg:current_snapshot_id': 12345 })
    expect(info.currentSnapshotId).toBe('12345')
    expect(info.snapshotIdIsExact).toBe(false)
    expect(info.warnings).toContain('numericSnapshotId')
  })

  it('reads a v1.0.0 directory asset through the GCS endpoint', () => {
    const info = getIcebergInfo({
      'iceberg:catalog_type': 'rest',
      assets: { table: { href: 'gs://bucket/warehouse/t/', type: 'application/x-iceberg' } },
    })
    expect(info.scanMode).toBe('directory')
    expect(info.scanSource).toBe('https://storage.googleapis.com/bucket/warehouse/t')
    expect(info.warnings).toContain('legacyMediaType')
  })
})

describe('scan expressions', () => {
  it('reads a metadata file directly and pins a snapshot', () => {
    const info = getIcebergInfo(icebergOnlyCollection())
    expect(icebergScanExpr(info)).toBe(`iceberg_scan('${info.scanSource}')`)
    expect(icebergScanExpr(info, { snapshotId: '4358210771845678999' }))
      .toBe(`iceberg_scan('${info.scanSource}', snapshot_from_id := 4358210771845678999)`)
    expect(defaultSql(info)).toContain('LIMIT 100')
  })

  it('ignores a snapshot id that is not an integer', () => {
    const info = getIcebergInfo(icebergOnlyCollection())
    expect(icebergScanExpr(info, { snapshotId: '1; DROP' })).toBe(`iceberg_scan('${info.scanSource}')`)
  })

  it('escapes quotes in the location', () => {
    const info = getIcebergInfo({ 'iceberg:catalog_type': 'static', 'iceberg:metadata_location': "https://x/it's/v1.metadata.json" })
    expect(icebergScanExpr(info)).toBe("iceberg_scan('https://x/it''s/v1.metadata.json')")
  })

  it('selects a resolved version for a legacy directory', () => {
    const info = getIcebergInfo({ assets: { t: { href: 'https://storage.googleapis.com/b/t', type: 'application/x-iceberg' } } })
    expect(icebergScanExpr(info, { version: '00003-abc' })).toBe(
      "iceberg_scan('https://storage.googleapis.com/b/t', allow_moved_paths := true, version_name_format := '%s%s.metadata.json', version := '00003-abc')"
    )
  })
})

describe('URI rewrites', () => {
  it('round-trips GCS and S3', () => {
    expect(toHttpUrl('gs://b/k/v1.metadata.json')).toBe('https://storage.googleapis.com/b/k/v1.metadata.json')
    expect(toObjectStoreUri('https://storage.googleapis.com/b/k/v1.metadata.json')).toBe('gs://b/k/v1.metadata.json')
    expect(toHttpUrl('s3://b/k')).toBe('https://b.s3.amazonaws.com/k')
    expect(toObjectStoreUri('https://b.s3.eu-central-1.amazonaws.com/k')).toBe('s3://b/k')
    expect(toObjectStoreUri('https://data.example.org/k')).toBe(null)
  })
})

describe('table metadata', () => {
  const text = `{
    "format-version": 3,
    "table-uuid": "u",
    "location": "https://x/t",
    "current-schema-id": 0,
    "schemas": [{"schema-id": 0, "type": "struct", "fields": [
      {"id": 1, "name": "id", "required": false, "type": "long"},
      {"id": 2, "name": "geom", "required": false, "type": "geometry(EPSG:3067)"}
    ]}],
    "default-spec-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "current-snapshot-id": 4358210771845678999,
    "snapshots": [
      {"snapshot-id": 4358210771845678000, "timestamp-ms": 1000, "summary": {"operation": "append", "total-records": "10"}},
      {"snapshot-id": 4358210771845678999, "parent-snapshot-id": 4358210771845678000, "timestamp-ms": 2000,
       "summary": {"operation": "overwrite", "total-records": "1012", "total-data-files": "1"}}
    ]
  }`

  it('keeps 64-bit snapshot ids exact', () => {
    const summary = summarizeTableMetadata(parseTableMetadata(text))
    expect(summary.currentSnapshotId).toBe('4358210771845678999')
    expect(summary.snapshots.map(s => s.id)).toEqual(['4358210771845678999', '4358210771845678000'])
    expect(summary.snapshots[0].current).toBe(true)
    expect(summary.snapshots[0].parentId).toBe('4358210771845678000')
    expect(summary.snapshots[0].totalRecords).toBe(1012)
    expect(summary.fields[1].type).toBe('geometry(EPSG:3067)')
  })

  it('finds no issue when the collection matches the table', () => {
    const summary = summarizeTableMetadata(parseTableMetadata(text))
    expect(compareWithTable(getIcebergInfo(portolanCollection()), summary)).toEqual([])
  })

  it('reports a collection that pins an older snapshot', () => {
    const summary = summarizeTableMetadata(parseTableMetadata(text))
    const info = getIcebergInfo({ ...portolanCollection(), 'iceberg:current_snapshot_id': '4358210771845678000' })
    expect(compareWithTable(info, summary).map(i => i.code)).toEqual(['snapshotBehind'])
  })
})

describe('parseGeoType', () => {
  it('reads the CRS and edge parameters', () => {
    expect(parseGeoType('geometry')).toEqual({ kind: 'geometry', crs: 'OGC:CRS84', edges: null, quoted: false })
    expect(parseGeoType('geometry(EPSG:3067)').crs).toBe('EPSG:3067')
    expect(parseGeoType('geography(OGC:CRS84, spherical)')).toMatchObject({ kind: 'geography', edges: 'spherical' })
    expect(parseGeoType("geometry('EPSG:4326')")).toMatchObject({ crs: 'EPSG:4326', quoted: true })
    expect(parseGeoType('string')).toBe(null)
  })
})

describe('connectionSnippets', () => {
  it('offers DuckDB, PyIceberg, BigQuery and ATTACH for a static GCS table', () => {
    const snippets = connectionSnippets(getIcebergInfo(icebergOnlyCollection()))
    expect(snippets.map(s => s.id)).toEqual(['duckdb-scan', 'pyiceberg', 'bigquery', 'duckdb-attach'])
    const py = snippets.find(s => s.id === 'pyiceberg').code
    expect(py).toContain('"gs://example-bucket/finland/data/v3/kunta_2025/metadata/v1.metadata.json"')
    const attach = snippets.find(s => s.id === 'duckdb-attach').code
    expect(attach).toContain("ATTACH 'sdi' AS lake (")
    expect(attach).toContain("AUTHORIZATION_TYPE 'none'")
    expect(attach).toContain('lake."v3"."kunta_2025"')
  })

  it('offers only ATTACH with a secret for a managed OAuth2 catalog', () => {
    const snippets = connectionSnippets(getIcebergInfo({
      'iceberg:catalog_type': 'rest',
      'iceberg:catalog_uri': 'https://catalog.example/iceberg',
      'iceberg:table_id': 'ns.t',
      'iceberg:authorization_type': 'oauth2',
    }))
    expect(snippets.map(s => s.id)).toEqual(['duckdb-attach'])
    expect(snippets[0].code).toContain('SECRET iceberg_secret')
  })
})

describe('inferS3Storage', () => {
  // A lakehousebox collection: a managed REST catalog, s3:// table paths, and
  // the metadata file served path-style from the store's own host.
  const metadataUrl = 'https://s3.lakehousebox.com/carto-sdi--berlin-geoportal/alkis/bezirke/metadata/v5.metadata.json'

  it('derives a path-style endpoint from the metadata URL', () => {
    expect(inferS3Storage(metadataUrl, 's3://carto-sdi--berlin-geoportal/alkis/bezirke')).toEqual({
      bucket: 'carto-sdi--berlin-geoportal',
      endpoint: 's3.lakehousebox.com',
      urlStyle: 'path',
      useSsl: true,
      metadataUri: 's3://carto-sdi--berlin-geoportal/alkis/bezirke/metadata/v5.metadata.json',
    })
  })

  it('derives a virtual-host endpoint', () => {
    expect(inferS3Storage('https://b.minio.example/t/metadata/v1.metadata.json', 's3://b/t'))
      .toMatchObject({ endpoint: 'minio.example', urlStyle: 'vhost', metadataUri: 's3://b/t/metadata/v1.metadata.json' })
  })

  it('leaves AWS, other schemes and unrelated hosts alone', () => {
    expect(inferS3Storage('https://b.s3.amazonaws.com/t/v1.metadata.json', 's3://b/t')).toBe(null)
    expect(inferS3Storage(metadataUrl, 'gs://carto-sdi--berlin-geoportal/t')).toBe(null)
    expect(inferS3Storage('https://cdn.example/x/v1.metadata.json', 's3://carto-sdi--berlin-geoportal/t')).toBe(null)
  })

  it('puts the endpoint into the DuckDB and PyIceberg snippets', () => {
    const info = getIcebergInfo({
      'iceberg:catalog_type': 'rest',
      'iceberg:catalog_uri': 'https://catalog.lakehousebox.com',
      'iceberg:table_id': 'alkis.bezirke',
      'iceberg:authorization_type': 'oauth2',
      'iceberg:metadata_location': metadataUrl,
    })
    const storage = inferS3Storage(info.scanSource, 's3://carto-sdi--berlin-geoportal/alkis/bezirke')
    const snippets = connectionSnippets(info, storage)
    expect(snippets.map(s => s.id)).toEqual(['duckdb-scan', 'pyiceberg', 'duckdb-attach'])
    expect(snippets[0].code).toContain("ENDPOINT 's3.lakehousebox.com'")
    expect(snippets[1].code).toContain('"s3://carto-sdi--berlin-geoportal/alkis/bezirke/metadata/v5.metadata.json"')
    expect(snippets[1].code).toContain('"s3.anonymous": "true"')
  })

  it('offers no PyIceberg snippet for a location it cannot open', () => {
    const info = getIcebergInfo({ 'iceberg:catalog_type': 'static', 'iceberg:metadata_location': 'https://cdn.example/t/metadata/v1.metadata.json' })
    expect(connectionSnippets(info).map(s => s.id)).toEqual(['duckdb-scan'])
  })
})

describe('bigQueryTableName', () => {
  it('keeps a hostile table id inside the quoted identifier', () => {
    for (const id of ['ns.t` ; DROP TABLE prod.users; --', 'ns.x`; SELECT 1; --']) {
      expect(bigQueryTableName(id)).toMatch(/^[A-Za-z0-9_]+$/)
    }
    expect(bigQueryTableName(null)).toBe('iceberg_table')
    expect(bigQueryTableName('v3.kunta_2025')).toBe('kunta_2025')
  })
})
