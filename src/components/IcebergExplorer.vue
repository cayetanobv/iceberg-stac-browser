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

    <!-- Auth Panel -->
    <section class="auth-panel mb-4">
      <h3>Connect</h3>
      <div class="d-flex align-items-end gap-2 flex-wrap">
        <div class="flex-grow-1">
          <label class="form-label" for="iceberg-token">GCS / S3 Bearer Token</label>
          <input
            id="iceberg-token"
            v-model="token"
            type="password"
            class="form-control"
            placeholder="Paste access token..."
            @keyup.enter="connect"
          />
        </div>
        <button class="btn btn-primary" @click="connect" :disabled="connecting || !token">
          <span v-if="connecting" class="spinner-border spinner-border-sm me-1" />
          {{ connected ? 'Reconnect' : 'Connect' }}
        </button>
      </div>
      <div v-if="connected" class="text-success mt-2">Connected</div>
      <div v-if="authError" class="text-danger mt-2">{{ authError }}</div>
    </section>

    <!-- Data Preview -->
    <section class="preview-panel mb-4" v-if="connected">
      <h3>Data Preview</h3>
      <button class="btn btn-outline-primary btn-sm mb-2" @click="loadPreview" :disabled="previewLoading">
        <span v-if="previewLoading" class="spinner-border spinner-border-sm me-1" />
        Load Preview (100 rows)
      </button>
      <ResultsTable v-if="previewData" :data="previewData" />
      <div v-if="previewError" class="text-danger mt-2">{{ previewError }}</div>
    </section>

    <!-- SQL Editor -->
    <section class="sql-panel mb-4" v-if="connected">
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
    <section class="snapshots-panel mb-4" v-if="connected">
      <h3>Snapshots</h3>
      <button class="btn btn-outline-primary btn-sm mb-2" @click="loadSnapshots" :disabled="snapshotsLoading">
        <span v-if="snapshotsLoading" class="spinner-border spinner-border-sm me-1" />
        Load Snapshots
      </button>
      <ResultsTable v-if="snapshotsData" :data="snapshotsData" />
      <div v-if="snapshotsError" class="text-danger mt-2">{{ snapshotsError }}</div>
    </section>

    <!-- Export -->
    <section class="export-panel mb-4" v-if="queryData || previewData">
      <div class="d-flex gap-2">
        <button class="btn btn-outline-secondary btn-sm" @click="exportCSV">Export CSV</button>
        <button class="btn btn-outline-secondary btn-sm" @click="exportGeoJSON" v-if="hasGeometry">Export GeoJSON</button>
      </div>
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
      connecting: false,
      connected: false,
      authError: null,
      previewLoading: false,
      previewData: null,
      previewError: null,
      sqlText: '',
      queryLoading: false,
      queryData: null,
      queryError: null,
      snapshotsLoading: false,
      snapshotsData: null,
      snapshotsError: null
    };
  },
  computed: {
    icebergHref() {
      return this.asset?.href || '';
    },
    extraFields() {
      return this.collection?.extra_fields || this.collection?.properties || {};
    },
    tableColumns() {
      const cols = this.extraFields['table:columns'];
      if (!Array.isArray(cols)) return [];
      return cols;
    },
    columnCount() {
      return this.tableColumns.length || null;
    },
    rowCount() {
      return this.extraFields['table:row_count'] || null;
    },
    partitionInfo() {
      const spec = this.extraFields['iceberg:partition_spec'];
      if (!Array.isArray(spec) || spec.length === 0) return null;
      const parts = spec.map(p => `${p.field} (${p.transform})`).join(', ');
      return `Partitioned by: ${parts}`;
    },
    defaultSQL() {
      if (!this.icebergHref) return '';
      return `SELECT *\nFROM iceberg_scan('${this.icebergHref}', allow_moved_paths := true)\nLIMIT 100`;
    },
    hasGeometry() {
      return this.tableColumns.some(c => c.name === 'geometry' || c.type === 'geometry');
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
  methods: {
    async connect() {
      this.connecting = true;
      this.authError = null;
      try {
        const { initDuckDB, setGCSToken } = await import('../duckdb.js');
        await initDuckDB();
        if (this.token) {
          await setGCSToken(this.token);
          sessionStorage.setItem('iceberg_token', this.token);
        }
        this.connected = true;
      } catch (err) {
        this.authError = `Connection failed: ${err.message}`;
      } finally {
        this.connecting = false;
      }
    },
    async loadPreview() {
      this.previewLoading = true;
      this.previewError = null;
      try {
        const { previewTable } = await import('../duckdb.js');
        this.previewData = await previewTable(this.icebergHref);
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
        this.snapshotsData = await getSnapshots(this.icebergHref);
      } catch (err) {
        this.snapshotsError = this.formatError(err);
      } finally {
        this.snapshotsLoading = false;
      }
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
</style>
