<template>
  <div class="iceberg-explorer">
    <!-- Schema Panel -->
    <section class="schema-panel mb-4">
      <h3>Table Schema</h3>
      <div class="schema-summary mb-2" v-if="columnCount || rowCount || partitionInfo">
        <span v-if="columnCount" class="badge bg-secondary me-2">{{ columnCount }} columns</span>
        <span v-if="rowCount" class="badge bg-secondary me-2">{{ rowCount.toLocaleString() }} rows</span>
        <span v-if="partitionInfo" class="badge bg-secondary">{{ partitionInfo }}</span>
      </div>
      <div class="schema-columns" v-if="tableColumns.length">
        <table class="table table-sm table-striped">
          <thead>
            <tr>
              <th>Name</th>
              <th>Type</th>
              <th>Description</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="col in tableColumns" :key="col.name">
              <td><code>{{ col.name }}</code></td>
              <td>{{ col.type }}</td>
              <td>{{ col.description || '' }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <p v-else class="text-muted">No schema information available in collection metadata.</p>
    </section>

    <!-- Auth Panel (optional, for private buckets) -->
    <section class="auth-panel mb-4">
      <details>
        <summary class="mb-2" style="cursor: pointer;">
          <strong>Authentication</strong>
          <span class="text-muted ms-2">(only needed for private buckets)</span>
        </summary>
        <div class="d-flex align-items-end gap-2 flex-wrap mt-2">
          <div class="flex-grow-1">
            <label class="form-label" for="iceberg-token">GCS / S3 Bearer Token</label>
            <input
              id="iceberg-token"
              v-model="token"
              type="password"
              class="form-control"
              placeholder="Paste access token..."
              @keyup.enter="applyToken"
            />
          </div>
          <button class="btn btn-primary" @click="applyToken" :disabled="!token">
            Apply Token
          </button>
        </div>
        <div v-if="tokenApplied" class="text-success mt-2">Token applied</div>
        <div v-if="authError" class="text-danger mt-2">{{ authError }}</div>
      </details>
    </section>

    <!-- DuckDB init error -->
    <div v-if="initError" class="text-danger mb-3">Failed to initialize DuckDB: {{ initError }}</div>

    <!-- Data Preview -->
    <section class="preview-panel mb-4" v-if="ready">
      <h3>Data Preview</h3>
      <button class="btn btn-outline-primary btn-sm mb-2" @click="loadPreview" :disabled="previewLoading">
        <span v-if="previewLoading" class="spinner-border spinner-border-sm me-1" />
        Load Preview (100 rows)
      </button>
      <ResultsTable v-if="previewData" :data="previewData" />
      <div v-if="previewError" class="text-danger mt-2">{{ previewError }}</div>
    </section>

    <!-- SQL Editor -->
    <section class="sql-panel mb-4" v-if="ready">
      <h3>SQL Query</h3>
      <div class="mb-2">
        <textarea
          v-model="sqlText"
          class="form-control font-monospace"
          rows="4"
          spellcheck="false"
        />
      </div>
      <div class="d-flex gap-2">
        <button class="btn btn-primary btn-sm" @click="runQuery" :disabled="queryLoading || !sqlText.trim()">
          <span v-if="queryLoading" class="spinner-border spinner-border-sm me-1" />
          Run
        </button>
        <button class="btn btn-outline-secondary btn-sm" @click="resetSQL">Reset</button>
      </div>
      <ResultsTable v-if="queryData" :data="queryData" class="mt-3" />
      <div v-if="queryError" class="text-danger mt-2">{{ queryError }}</div>
    </section>

    <!-- Snapshots -->
    <section class="snapshots-panel mb-4" v-if="ready">
      <h3>Snapshots</h3>
      <button class="btn btn-outline-primary btn-sm mb-2" @click="loadSnapshots" :disabled="snapshotsLoading">
        <span v-if="snapshotsLoading" class="spinner-border spinner-border-sm me-1" />
        Load Snapshots
      </button>
      <ResultsTable v-if="snapshotsData" :data="snapshotsData" />
      <div v-if="snapshotsError" class="text-danger mt-2">{{ snapshotsError }}</div>
    </section>

    <!-- Map Preview -->
    <section class="map-panel mb-4" v-if="ready && hasGeometry">
      <h3>Geometry Preview</h3>
      <button class="btn btn-outline-primary btn-sm mb-2" @click="loadMapPreview" :disabled="mapLoading">
        <span v-if="mapLoading" class="spinner-border spinner-border-sm me-1" />
        {{ mapLoaded ? 'Reload geometries' : 'Load sample geometries (500)' }}
      </button>
      <div v-if="mapError" class="text-danger mt-2">{{ mapError }}</div>
      <div v-if="mapLoaded" class="map-container mt-2" ref="mapContainer"></div>
      <div v-if="mapFeatureCount" class="text-muted mt-1">{{ mapFeatureCount }} features rendered</div>
    </section>

    <!-- Download -->
    <section class="download-panel mb-4" v-if="ready">
      <h3>Download</h3>
      <div class="d-flex gap-2 flex-wrap align-items-center">
        <button class="btn btn-primary btn-sm" @click="downloadFullGeoParquet" :disabled="fullDownloading">
          <span v-if="fullDownloading" class="spinner-border spinner-border-sm me-1" />
          Full table (GeoParquet)
        </button>
        <button class="btn btn-outline-primary btn-sm" @click="downloadQueryResult" :disabled="queryDownloading || !queryData" v-if="queryData">
          <span v-if="queryDownloading" class="spinner-border spinner-border-sm me-1" />
          Query result (GeoParquet)
        </button>
        <button class="btn btn-outline-secondary btn-sm" @click="exportCSV" v-if="queryData || previewData">CSV</button>
        <button class="btn btn-outline-secondary btn-sm" @click="exportGeoJSON" v-if="(queryData || previewData) && hasGeometry">GeoJSON</button>
      </div>
      <div v-if="rowCount" class="text-muted mt-1">Full table: {{ rowCount.toLocaleString() }} rows. Large tables may take a while.</div>
      <div v-if="downloadError" class="text-danger mt-2">{{ downloadError }}</div>
      <div v-if="downloadProgress" class="text-muted mt-1">{{ downloadProgress }}</div>
    </section>
  </div>
</template>

<script>
import { defineComponent, defineAsyncComponent } from 'vue';

const ICEBERG_MEDIA_TYPE = 'application/x-iceberg';

export default defineComponent({
  name: 'IcebergExplorer',
  components: {
    ResultsTable: defineAsyncComponent(() => import('./IcebergResultsTable.vue'))
  },
  props: {
    asset: {
      type: Object,
      required: true
    },
    collection: {
      type: Object,
      required: true
    }
  },
  data() {
    return {
      token: sessionStorage.getItem('iceberg_token') || '',
      tokenApplied: false,
      authError: null,
      ready: false,
      initError: null,
      previewLoading: false,
      previewData: null,
      previewError: null,
      sqlText: '',
      queryLoading: false,
      queryData: null,
      queryError: null,
      snapshotsLoading: false,
      snapshotsData: null,
      snapshotsError: null,
      icebergVersion: null,
      mapLoading: false,
      mapLoaded: false,
      mapError: null,
      mapFeatureCount: null,
      olMap: null,
      fullDownloading: false,
      queryDownloading: false,
      downloadError: null,
      downloadProgress: null
    };
  },
  computed: {
    icebergHref() {
      return this.asset?.href || '';
    },
    stacData() {
      // STAC Browser stores the raw STAC JSON on the data object.
      // Extension fields like table:columns are top-level properties.
      const d = this.collection;
      if (!d) return {};
      // Try direct access (raw JSON), then .properties, then .summaries
      return d;
    },
    tableColumns() {
      const cols = this.stacData['table:columns'];
      if (!Array.isArray(cols)) return [];
      return cols;
    },
    columnCount() {
      return this.tableColumns.length || null;
    },
    rowCount() {
      return this.stacData['table:row_count'] || null;
    },
    partitionInfo() {
      const spec = this.stacData['iceberg:partition_spec'];
      if (!Array.isArray(spec) || spec.length === 0) return null;
      const parts = spec.map(p => `${p.field} (${p.transform})`).join(', ');
      return `Partitioned by: ${parts}`;
    },
    httpHref() {
      const href = this.icebergHref;
      if (href.startsWith('gs://')) return href.replace('gs://', 'https://storage.googleapis.com/');
      if (href.startsWith('s3://')) return href.replace(/^s3:\/\/([^/]+)/, 'https://$1.s3.amazonaws.com');
      return href;
    },
    defaultSQL() {
      if (!this.httpHref) return '';
      if (this.icebergVersion) {
        return `SELECT *\nFROM iceberg_scan('${this.httpHref}', allow_moved_paths := true, version_name_format := '%s%s.metadata.json', version := '${this.icebergVersion}')\nLIMIT 100`;
      }
      return `SELECT *\nFROM iceberg_scan('${this.httpHref}', allow_moved_paths := true)\nLIMIT 100`;
    },
    hasGeometry() {
      const primaryGeom = this.stacData['table:primary_geometry'];
      if (primaryGeom) return true;
      return this.tableColumns.some(c => c.name === 'geometry' || c.type === 'geometry' || c.type === 'binary');
    },
    activeData() {
      return this.queryData || this.previewData;
    }
  },
  watch: {
    icebergHref: {
      immediate: true,
      handler() {
        this.sqlText = this.defaultSQL;
      }
    }
  },
  async mounted() {
    try {
      const { initDuckDB, setGCSToken, resolveIcebergVersion } = await import('../duckdb.js');
      await initDuckDB();
      // Re-apply saved token if present
      const saved = sessionStorage.getItem('iceberg_token');
      if (saved) {
        await setGCSToken(saved);
        this.tokenApplied = true;
      }
      // Debug: log collection data to verify extension fields
      console.log('[iceberg] collection keys:', Object.keys(this.collection || {}));
      console.log('[iceberg] table:columns:', this.collection?.['table:columns']);
      console.log('[iceberg] table:primary_geometry:', this.collection?.['table:primary_geometry']);
      console.log('[iceberg] hasGeometry:', this.hasGeometry);
      // Resolve latest metadata version via cloud storage API
      if (this.icebergHref) {
        this.icebergVersion = await resolveIcebergVersion(this.icebergHref);
        console.log(`[iceberg] Resolved version: ${this.icebergVersion}`);
        this.sqlText = this.defaultSQL;
      }
      this.ready = true;
    } catch (err) {
      this.initError = err.message;
    }
  },
  methods: {
    async applyToken() {
      this.authError = null;
      this.tokenApplied = false;
      try {
        const { setGCSToken } = await import('../duckdb.js');
        await setGCSToken(this.token);
        sessionStorage.setItem('iceberg_token', this.token);
        this.tokenApplied = true;
      } catch (err) {
        this.authError = `Failed to apply token: ${err.message}`;
      }
    },
    async loadPreview() {
      this.previewLoading = true;
      this.previewError = null;
      try {
        const { previewTable } = await import('../duckdb.js');
        this.previewData = await previewTable(this.icebergHref, this.icebergVersion);
      } catch (err) {
        this.previewError = this.formatError(err);
      } finally {
        this.previewLoading = false;
      }
    },
    async runQuery() {
      this.queryLoading = true;
      this.queryError = null;
      try {
        const { query } = await import('../duckdb.js');
        this.queryData = await query(this.sqlText);
      } catch (err) {
        this.queryError = this.formatError(err);
      } finally {
        this.queryLoading = false;
      }
    },
    async loadSnapshots() {
      this.snapshotsLoading = true;
      this.snapshotsError = null;
      try {
        const { getSnapshots } = await import('../duckdb.js');
        this.snapshotsData = await getSnapshots(this.icebergHref, this.icebergVersion);
      } catch (err) {
        this.snapshotsError = this.formatError(err);
      } finally {
        this.snapshotsLoading = false;
      }
    },
    async loadMapPreview() {
      this.mapLoading = true;
      this.mapError = null;
      try {
        const { sampleGeometries } = await import('../duckdb.js');
        const result = await sampleGeometries(this.icebergHref, this.icebergVersion);

        // Build GeoJSON FeatureCollection
        const features = result.rows
          .filter(r => r.geojson)
          .map((r, i) => ({
            type: 'Feature',
            geometry: JSON.parse(r.geojson),
            properties: { id: i }
          }));
        const fc = { type: 'FeatureCollection', features };
        this.mapFeatureCount = features.length;

        // Render with OpenLayers
        await this.$nextTick();
        this.mapLoaded = true;
        await this.$nextTick();
        this.renderMap(fc);
      } catch (err) {
        this.mapError = this.formatError(err);
      } finally {
        this.mapLoading = false;
      }
    },
    async renderMap(geojson) {
      const [
        { default: Map },
        { default: View },
        { default: TileLayer },
        { default: VectorLayer },
        { default: VectorSource },
        { default: OSM },
        { default: GeoJSON },
        { Fill, Stroke, Style, Circle: CircleStyle }
      ] = await Promise.all([
        import('ol/Map.js'),
        import('ol/View.js'),
        import('ol/layer/Tile.js'),
        import('ol/layer/Vector.js'),
        import('ol/source/Vector.js'),
        import('ol/source/OSM.js'),
        import('ol/format/GeoJSON.js'),
        import('ol/style.js')
      ]);

      if (this.olMap) {
        this.olMap.setTarget(null);
        this.olMap = null;
      }

      const vectorSource = new VectorSource({
        features: new GeoJSON().readFeatures(geojson, {
          featureProjection: 'EPSG:3857'
        })
      });

      const vectorLayer = new VectorLayer({
        source: vectorSource,
        style: new Style({
          fill: new Fill({ color: 'rgba(0, 119, 182, 0.2)' }),
          stroke: new Stroke({ color: '#0077b6', width: 1.5 }),
          image: new CircleStyle({
            radius: 5,
            fill: new Fill({ color: '#0077b6' }),
            stroke: new Stroke({ color: '#fff', width: 1 })
          })
        })
      });

      this.olMap = new Map({
        target: this.$refs.mapContainer,
        layers: [
          new TileLayer({ source: new OSM() }),
          vectorLayer
        ],
        view: new View({
          center: [0, 0],
          zoom: 2
        })
      });

      const extent = vectorSource.getExtent();
      if (extent && isFinite(extent[0])) {
        this.olMap.getView().fit(extent, { padding: [40, 40, 40, 40], maxZoom: 16 });
      }
    },
    async downloadFullGeoParquet() {
      this.fullDownloading = true;
      this.downloadError = null;
      this.downloadProgress = 'Reading latest snapshot...';
      try {
        const { exportToParquet, query: duckQuery } = await import('../duckdb.js');
        const scan = this.buildScanExpr();
        // Probe columns to exclude derived spatial ones (added by portolake for partitioning)
        const probe = await duckQuery(`SELECT * FROM ${scan} LIMIT 0`);
        const derivedCols = (probe.columns || []).filter(c =>
          c.startsWith('bbox_') || c.startsWith('geohash_')
        );
        const sql = derivedCols.length > 0
          ? `SELECT * EXCLUDE (${derivedCols.join(', ')}) FROM ${scan}`
          : `SELECT * FROM ${scan}`;
        this.downloadProgress = 'Exporting to GeoParquet (this may take a while for large tables)...';
        const buffer = await exportToParquet(sql);
        const collectionId = this.collection?.id || 'data';
        this.downloadBlob(buffer, `${collectionId}_latest.parquet`, 'application/vnd.apache.parquet');
        this.downloadProgress = null;
      } catch (err) {
        this.downloadError = this.formatError(err);
        this.downloadProgress = null;
      } finally {
        this.fullDownloading = false;
      }
    },
    async downloadQueryResult() {
      this.queryDownloading = true;
      this.downloadError = null;
      this.downloadProgress = 'Exporting query result...';
      try {
        const { exportToParquet } = await import('../duckdb.js');
        const buffer = await exportToParquet(this.sqlText);
        const collectionId = this.collection?.id || 'query';
        this.downloadBlob(buffer, `${collectionId}_query.parquet`, 'application/vnd.apache.parquet');
        this.downloadProgress = null;
      } catch (err) {
        this.downloadError = this.formatError(err);
        this.downloadProgress = null;
      } finally {
        this.queryDownloading = false;
      }
    },
    buildScanExpr() {
      const href = this.httpHref;
      if (this.icebergVersion) {
        return `iceberg_scan('${href}', allow_moved_paths := true, version_name_format := '%s%s.metadata.json', version := '${this.icebergVersion}')`;
      }
      return `iceberg_scan('${href}', allow_moved_paths := true)`;
    },
    downloadBlob(buffer, filename, mime) {
      const blob = new Blob([buffer], { type: mime });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      a.click();
      URL.revokeObjectURL(url);
    },
    resetSQL() {
      this.sqlText = this.defaultSQL;
      this.queryData = null;
      this.queryError = null;
    },
    formatError(err) {
      const msg = err.message || String(err);
      if (msg.includes('403') || msg.includes('Forbidden')) {
        return 'Access denied — token may be expired. Paste a new token and reconnect.';
      }
      return msg;
    },
    exportCSV() {
      const data = this.activeData;
      if (!data) return;
      const header = data.columns.join(',');
      const rows = data.rows.map(r =>
        data.columns.map(c => {
          const v = r[c];
          if (v === null || v === undefined) return '';
          const s = String(v);
          return s.includes(',') || s.includes('"') || s.includes('\n')
            ? `"${s.replace(/"/g, '""')}"`
            : s;
        }).join(',')
      );
      this.downloadFile(`${header}\n${rows.join('\n')}`, 'results.csv', 'text/csv');
    },
    exportGeoJSON() {
      // Placeholder — requires geometry column in results
      const data = this.activeData;
      if (!data) return;
      const features = data.rows
        .filter(r => r.geojson || r.geometry)
        .map(r => {
          const geom = r.geojson ? JSON.parse(r.geojson) : null;
          const props = { ...r };
          delete props.geojson;
          delete props.geometry;
          return { type: 'Feature', geometry: geom, properties: props };
        });
      const fc = { type: 'FeatureCollection', features };
      this.downloadFile(JSON.stringify(fc, null, 2), 'results.geojson', 'application/geo+json');
    },
    downloadFile(content, filename, mime) {
      const blob = new Blob([content], { type: mime });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      a.click();
      URL.revokeObjectURL(url);
    }
  }
});
</script>

<style lang="scss">
@import "ol/ol.css";
</style>

<style lang="scss" scoped>
.iceberg-explorer {
  padding: 1rem 0;
}

.schema-panel .table {
  font-size: 0.875rem;
}

.schema-panel code {
  font-size: 0.8125rem;
}

.sql-panel textarea {
  font-size: 0.875rem;
  resize: vertical;
}

.badge {
  font-weight: 500;
}

.map-container {
  width: 100%;
  height: 400px;
  border: 1px solid #dee2e6;
  border-radius: 0.375rem;
}
</style>
