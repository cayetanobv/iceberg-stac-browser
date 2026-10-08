<template>
  <section v-if="info" class="iceberg-table mb-4">
    <div class="iceberg-header" @click="open = !open">
      <span class="iceberg-header-left">
        <BIconChevronDown v-if="open" />
        <BIconChevronRight v-else />
        <span class="iceberg-title">{{ $t('iceberg.title') }}</span>
        <b-badge v-if="formatVersion" variant="primary">v{{ formatVersion }}</b-badge>
        <b-badge v-if="info.catalogType" variant="secondary">{{ info.catalogType }}</b-badge>
      </span>
      <code v-if="info.tableId" class="iceberg-table-id">{{ info.tableId }}</code>
    </div>
    <b-collapse v-model="open">
      <div class="iceberg-body">
        <div v-for="notice in notices" :key="notice.code" :class="['alert', 'py-2', 'px-3', 'mb-2', `alert-${notice.variant}`]">
          {{ $t(`iceberg.notices.${notice.code}`, notice.params) }}
        </div>

        <b-tabs v-model="activeTab" small class="iceberg-tabs">
          <b-tab id="iceberg-connection" :title="$t('iceberg.tabs.connection')">
            <dl class="iceberg-fields">
              <template v-for="row in connectionRows" :key="row.label">
                <dt>{{ row.label }}</dt>
                <dd>
                  <a v-if="row.href" :href="row.href" target="_blank" rel="noopener noreferrer">{{ row.value }}</a>
                  <code v-else-if="row.code">{{ row.value }}</code>
                  <span v-else>{{ row.value }}</span>
                  <b-badge v-for="tag in row.tags || []" :key="tag.text" :variant="tag.variant" class="ms-2">{{ tag.text }}</b-badge>
                </dd>
              </template>
            </dl>
            <p class="iceberg-hint">{{ distributionHint }}</p>
          </b-tab>

          <b-tab id="iceberg-schema" :title="$t('iceberg.tabs.schema')">
            <Loading v-if="tableLoading" />
            <div v-else-if="schemaRows.length > 0" class="table-responsive">
              <table class="table table-sm table-striped">
                <thead>
                  <tr>
                    <th v-if="hasFieldIds">{{ $t('iceberg.schema.id') }}</th>
                    <th>{{ $t('iceberg.schema.name') }}</th>
                    <th>{{ $t('iceberg.schema.type') }}</th>
                    <th v-if="hasStacTypes">{{ $t('iceberg.schema.stacType') }}</th>
                    <th>{{ $t('description') }}</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="row in schemaRows" :key="row.name">
                    <td v-if="hasFieldIds">{{ row.id }}</td>
                    <td>
                      <code>{{ row.name }}</code>
                      <b-badge v-if="row.primary" variant="info" class="ms-1">{{ $t('iceberg.schema.primaryGeometry') }}</b-badge>
                      <b-badge v-if="row.partition" variant="secondary" class="ms-1">{{ $t('iceberg.schema.partition') }}</b-badge>
                    </td>
                    <td><code>{{ row.type }}</code><span v-if="row.required" class="text-muted"> ({{ $t('iceberg.schema.required') }})</span></td>
                    <td v-if="hasStacTypes"><code v-if="row.stacType">{{ row.stacType }}</code></td>
                    <td>{{ row.description }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
            <p v-else class="text-muted">{{ $t('iceberg.schema.none') }}</p>
            <p v-if="tableError" class="text-danger small">{{ tableError }}</p>
          </b-tab>

          <b-tab v-if="tableSummary || tableLoading" id="iceberg-snapshots" :title="$t('iceberg.tabs.snapshots')">
            <Loading v-if="tableLoading" />
            <div v-else-if="tableSummary.snapshots.length > 0" class="table-responsive">
              <table class="table table-sm table-striped">
                <thead>
                  <tr>
                    <th>{{ $t('iceberg.snapshots.time') }}</th>
                    <th>{{ $t('iceberg.snapshots.id') }}</th>
                    <th>{{ $t('iceberg.snapshots.operation') }}</th>
                    <th class="text-end">{{ $t('iceberg.snapshots.records') }}</th>
                    <th class="text-end">{{ $t('iceberg.snapshots.files') }}</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="snap in tableSummary.snapshots" :key="snap.id">
                    <td>{{ snap.timestamp ? snap.timestamp.toISOString().replace('T', ' ').replace(/\.\d+Z$/, 'Z') : '' }}</td>
                    <td>
                      <code>{{ snap.id }}</code>
                      <b-badge v-if="snap.current" variant="success" class="ms-1">{{ $t('iceberg.snapshots.current') }}</b-badge>
                      <b-badge v-if="snap.id === info.currentSnapshotId" variant="info" class="ms-1">{{ $t('iceberg.snapshots.pinned') }}</b-badge>
                    </td>
                    <td>{{ snap.operation }}</td>
                    <td class="text-end">{{ snap.totalRecords !== null ? snap.totalRecords.toLocaleString() : '' }}</td>
                    <td class="text-end">{{ snap.totalFiles !== null ? snap.totalFiles.toLocaleString() : '' }}</td>
                    <td class="text-end">
                      <b-button v-if="info.queryable" size="sm" variant="outline-primary" @click="querySnapshot(snap)">
                        {{ $t('iceberg.snapshots.query') }}
                      </b-button>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
            <p v-else class="text-muted">{{ $t('iceberg.snapshots.none') }}</p>
          </b-tab>

          <b-tab v-if="snippets.length > 0" id="iceberg-code" :title="$t('iceberg.tabs.code')">
            <div v-for="snippet in snippets" :key="snippet.id" class="mb-3">
              <h4 class="iceberg-subtitle">{{ snippet.title }}</h4>
              <CodeHighlighted :code="snippet.code" :language="snippet.language" :file="snippetFile(snippet)" />
            </div>
            <p class="iceberg-hint">{{ $t('iceberg.code.engines') }}</p>
          </b-tab>

          <b-tab v-if="info.queryable" id="iceberg-query" :title="$t('iceberg.tabs.query')">
            <div v-if="!duck.ready" class="iceberg-start">
              <p>{{ $t('iceberg.query.intro') }}</p>
              <b-button variant="primary" :disabled="duck.starting" @click="startDuckDB">
                <span v-if="duck.starting" class="spinner-border spinner-border-sm me-1" />
                {{ $t('iceberg.query.start') }}
              </b-button>
              <p v-if="duck.error" class="text-danger small mt-2">{{ duck.error }}</p>
            </div>
            <template v-else>
              <p class="iceberg-hint">{{ $t('iceberg.query.engine', { version: duck.version }) }}</p>

              <details class="mb-3">
                <summary>{{ $t('iceberg.auth.title') }}</summary>
                <div class="d-flex align-items-end gap-2 flex-wrap mt-2">
                  <div class="flex-grow-1">
                    <label class="form-label small" for="iceberg-token">{{ $t('iceberg.auth.token') }}</label>
                    <input id="iceberg-token" v-model="token" type="password" class="form-control form-control-sm" @keyup.enter="applyToken">
                  </div>
                  <b-button size="sm" variant="primary" :disabled="!token" @click="applyToken">{{ $t('iceberg.auth.apply') }}</b-button>
                </div>
                <p v-if="tokenMessage" class="small mt-1">{{ tokenMessage }}</p>
              </details>

              <div class="mb-2">
                <textarea v-model="sql" class="form-control font-monospace iceberg-sql" rows="5" spellcheck="false" @keydown.ctrl.enter="runQuery" @keydown.meta.enter="runQuery" />
              </div>
              <div class="d-flex gap-2 flex-wrap mb-3">
                <b-button size="sm" variant="primary" :disabled="Boolean(busy) || !sql.trim()" @click="runQuery">
                  <span v-if="busy === 'query'" class="spinner-border spinner-border-sm me-1" />
                  {{ $t('iceberg.query.run') }}
                </b-button>
                <b-button size="sm" variant="outline-primary" :disabled="Boolean(busy)" @click="loadPreview">
                  <span v-if="busy === 'preview'" class="spinner-border spinner-border-sm me-1" />
                  {{ $t('iceberg.query.preview') }}
                </b-button>
                <b-button v-if="geometry" size="sm" variant="outline-primary" :disabled="Boolean(busy)" @click="loadMap">
                  <span v-if="busy === 'map'" class="spinner-border spinner-border-sm me-1" />
                  {{ $t('iceberg.query.map', { count: MAP_LIMIT.toLocaleString() }) }}
                </b-button>
                <b-button size="sm" variant="outline-secondary" :disabled="Boolean(busy)" @click="resetSql">{{ $t('reset') }}</b-button>
                <span class="flex-grow-1" />
                <b-button size="sm" variant="outline-primary" :disabled="Boolean(busy) || !results" @click="downloadResult">
                  <span v-if="busy === 'download-result'" class="spinner-border spinner-border-sm me-1" />
                  {{ $t('iceberg.download.result') }}
                </b-button>
                <b-button size="sm" variant="outline-primary" :disabled="Boolean(busy)" @click="downloadTable">
                  <span v-if="busy === 'download-table'" class="spinner-border spinner-border-sm me-1" />
                  {{ $t('iceberg.download.table') }}
                </b-button>
              </div>
              <p v-if="queryError" class="text-danger small">{{ queryError }}</p>
              <p v-if="status" class="text-muted small">{{ status }}</p>
              <div v-show="mapShown" ref="map" class="iceberg-map mb-3" />
              <IcebergResults v-if="results" :data="results" />
            </template>
          </b-tab>
        </b-tabs>
      </div>
    </b-collapse>
  </section>
</template>

<script>
import { defineComponent, defineAsyncComponent, markRaw } from 'vue';
import { BBadge, BButton, BCollapse, BTab, BTabs } from 'bootstrap-vue-next';
import BIconChevronDown from '~icons/bi/chevron-down';
import BIconChevronRight from '~icons/bi/chevron-right';
import Loading from './Loading.vue';
import {
  compareWithTable,
  connectionSnippets,
  defaultSql,
  formatPartitionField,
  getIcebergInfo,
  icebergScanExpr,
  inferS3Storage,
  parseGeoType,
  parseTableMetadata,
  sqlIdent,
  sqlString,
  summarizeTableMetadata,
} from '../utils/iceberg.js';
import { createLonLatTransform, reprojectGeometry } from '../utils/crs.js';
import configureBasemap from '../../basemaps.config.js';

const MAP_LIMIT = 1000;
const LONLAT_CRS = ['OGC:CRS84', 'EPSG:4326', 'CRS84'];
const TOKEN_KEY = 'iceberg_token';

function readToken() {
  try {
    return sessionStorage.getItem(TOKEN_KEY) || '';
  } catch {
    return '';
  }
}

export default defineComponent({
  name: 'IcebergTable',
  components: {
    BBadge,
    BButton,
    BCollapse,
    BIconChevronDown,
    BIconChevronRight,
    BTab,
    BTabs,
    CodeHighlighted: defineAsyncComponent(() => import('./CodeHighlighted.vue')),
    IcebergResults: defineAsyncComponent(() => import('./IcebergResults.vue')),
    Loading,
  },
  props: {
    collection: {
      type: Object,
      required: true
    }
  },
  data() {
    return {
      MAP_LIMIT,
      open: true,
      activeTab: 'iceberg-connection',
      tableLoading: false,
      tableSummary: null,
      tableError: null,
      resolvedVersion: null,
      duck: { ready: false, starting: false, error: null, version: null },
      columns: [],
      geometryInfo: null,
      sql: '',
      busy: null,
      results: null,
      queryError: null,
      status: null,
      mapShown: false,
      token: readToken(),
      tokenMessage: null,
    };
  },
  computed: {
    info() {
      const url = typeof this.collection?.getAbsoluteUrl === 'function' ? this.collection.getAbsoluteUrl() : null;
      return getIcebergInfo(this.collection, url);
    },
    scanOptions() {
      return { version: this.resolvedVersion };
    },
    scan() {
      return icebergScanExpr(this.info, this.scanOptions);
    },
    formatVersion() {
      return this.info.formatVersion || this.tableSummary?.formatVersion || null;
    },
    issues() {
      return compareWithTable(this.info, this.tableSummary);
    },
    notices() {
      const notices = [];
      const warn = (code, params = {}, variant = 'warning') => notices.push({ code, params, variant });
      for (const code of this.info.warnings) {
        warn(code);
      }
      for (const issue of this.issues) {
        warn(issue.code, { stac: issue.stac, table: issue.table });
      }
      if (!this.info.queryable) {
        warn(this.info.managed ? 'managedNoLocation' : 'notQueryable', { type: this.info.catalogType }, 'info');
      }
      return notices;
    },
    connectionRows() {
      const info = this.info;
      const rows = [];
      const add = (key, value, extra = {}) => {
        if (value !== null && value !== undefined && value !== '') {
          rows.push({ label: this.$t(`iceberg.fields.${key}`), value, ...extra });
        }
      };
      add('catalogType', info.catalogType, { tags: info.catalogType === 'static' ? [{ text: this.$t('iceberg.serverless'), variant: 'light' }] : [] });
      add('catalogUri', info.catalogUri, { href: /^https?:/.test(info.catalogUri || '') ? info.catalogUri : null });
      add('restPrefix', info.restPrefix, { code: true });
      add('authorizationType', info.authorizationType);
      add('tableId', info.tableId, { code: true });
      add('metadataLocation', info.metadataLocation, { href: /^https?:/.test(info.metadataLocation || '') ? info.metadataLocation : null });
      if (!info.metadataLocation && info.assetHref) {
        add('asset', info.assetHref, { href: /^https?:/.test(info.assetHref) ? info.assetHref : null });
      }
      add('formatVersion', info.formatVersion);
      const snapshotTags = [];
      if (info.currentSnapshotId && this.tableSummary?.currentSnapshotId) {
        const same = info.currentSnapshotId === this.tableSummary.currentSnapshotId;
        snapshotTags.push({ text: this.$t(same ? 'iceberg.snapshots.matches' : 'iceberg.snapshots.differs'), variant: same ? 'success' : 'warning' });
      }
      add('currentSnapshotId', info.currentSnapshotId, { code: true, tags: snapshotTags });
      if (info.partitionSpec.length > 0) {
        add('partitionSpec', info.partitionSpec.map(formatPartitionField).join(', '));
      }
      add('rowCount', info.rowCount !== null ? info.rowCount.toLocaleString() : null);
      return rows;
    },
    distributionHint() {
      if (!this.info.asset) {
        return this.$t('iceberg.distribution.noAsset');
      }
      return this.$t(this.info.icebergOnly ? 'iceberg.distribution.only' : 'iceberg.distribution.beside');
    },
    partitionSources() {
      const spec = this.tableSummary?.partitionSpec?.length ? this.tableSummary.partitionSpec : this.info.partitionSpec;
      return new Set(spec.map(p => p.sourceId).filter(id => id !== null));
    },
    schemaRows() {
      const stacColumns = new Map(this.info.tableColumns.map(c => [c.name, c]));
      const primary = this.info.primaryGeometry;
      if (this.tableSummary?.fields?.length) {
        return this.tableSummary.fields.map(f => ({
          id: f.id,
          name: f.name,
          type: f.type,
          required: f.required,
          stacType: stacColumns.get(f.name)?.type || null,
          description: stacColumns.get(f.name)?.description || f.doc || '',
          primary: f.name === primary,
          partition: this.partitionSources.has(f.id),
        }));
      }
      return this.info.tableColumns.map(c => ({
        id: null,
        name: c.name,
        type: c.type,
        required: false,
        stacType: null,
        description: c.description || '',
        primary: c.name === primary,
        partition: false,
      }));
    },
    hasFieldIds() {
      return this.schemaRows.some(r => r.id !== null && r.id !== undefined);
    },
    hasStacTypes() {
      return this.schemaRows.some(r => r.stacType);
    },
    storage() {
      return inferS3Storage(this.info.scanSource, this.tableSummary?.location);
    },
    snippets() {
      return connectionSnippets(this.info, this.storage);
    },
    geometry() {
      return this.geometryInfo;
    },
    geometryCrs() {
      const name = this.geometryInfo?.name || this.info.primaryGeometry;
      const field = this.tableSummary?.fields?.find(f => f.name === name);
      const geo = parseGeoType(field?.type);
      // The Iceberg type parameter is the table's own statement of the CRS;
      // proj:code describes the collection.
      if (geo && field.type.includes('(')) {
        return geo.crs;
      }
      return this.info.projCode || 'OGC:CRS84';
    },
  },
  watch: {
    info: {
      immediate: true,
      handler(info, old) {
        if (!info || (old && old.scanSource === info.scanSource)) {
          return;
        }
        this.resolvedVersion = null;
        this.tableSummary = null;
        this.sql = defaultSql(info);
        this.metadataLoaded = this.loadTableMetadata();
      }
    }
  },
  beforeUnmount() {
    // A start still in flight must not route S3 requests for the collection
    // that replaced this one.
    this.unmounted = true;
    this.destroyMap();
  },
  methods: {
    async loadTableMetadata() {
      const info = this.info;
      if (!info.scanSource) {
        return;
      }
      this.tableLoading = true;
      this.tableError = null;
      try {
        let url = info.scanSource;
        if (info.scanMode === 'directory') {
          const { resolveDirectoryVersion } = await import('../utils/duckdb.js');
          this.resolvedVersion = await resolveDirectoryVersion(info.scanSource);
          this.sql = defaultSql(info, this.scanOptions);
          if (!this.resolvedVersion) {
            return;
          }
          url = `${info.scanSource}/metadata/${this.resolvedVersion}.metadata.json`;
        }
        const response = await fetch(url);
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        this.tableSummary = summarizeTableMetadata(parseTableMetadata(await response.text()));
      } catch (error) {
        this.tableError = this.$t('iceberg.errors.metadata', { error: this.formatError(error) });
      } finally {
        this.tableLoading = false;
      }
    },
    snippetFile(snippet) {
      const base = (this.info.tableId || this.collection.id || 'iceberg').replace(/[^\w.-]+/g, '_');
      return `${base}.${snippet.language === 'python' ? 'py' : 'sql'}`;
    },
    async startDuckDB() {
      this.duck.starting = true;
      this.duck.error = null;
      try {
        const duckdb = await import('../utils/duckdb.js');
        // The S3 endpoint comes from metadata.json, so wait for it.
        await this.metadataLoaded;
        await duckdb.initDuckDB();
        this.duck.version = await duckdb.engineVersion();
        if (this.unmounted) {
          return;
        }
        if (this.token) {
          await duckdb.setGcsToken(this.token);
        }
        const table = this.currentTable();
        this.columns = await duckdb.withRouting(table.storage, () => duckdb.describe(table.scan));
        this.geometryInfo = duckdb.geometryColumn(this.columns, this.info.primaryGeometry);
        this.duck.ready = true;
      } catch (error) {
        // DuckDB may have started while the table failed to open: still
        // offer the editor so the user can fix a token or a query.
        const duckdb = await import('../utils/duckdb.js');
        try {
          await duckdb.initDuckDB();
          this.duck.version = this.duck.version || await duckdb.engineVersion();
          this.duck.ready = true;
          this.queryError = this.$t('iceberg.errors.open', { error: this.formatError(error) });
        } catch {
          this.duck.error = this.$t('iceberg.errors.start', { error: this.formatError(error) });
        }
      } finally {
        this.duck.starting = false;
      }
    },
    currentTable() {
      return Object.freeze({ storage: this.storage, scan: this.scan });
    },
    async run(kind, fn) {
      this.busy = kind;
      this.queryError = null;
      this.status = null;
      try {
        // The routing and the scan come from one snapshot of the table, and
        // the query runs with that routing, atomically.
        const table = this.currentTable();
        const { withRouting } = await import('../utils/duckdb.js');
        await withRouting(table.storage, () => fn(table));
      } catch (error) {
        this.queryError = this.formatError(error);
      } finally {
        this.busy = null;
      }
    },
    runQuery() {
      if (this.busy || !this.sql.trim()) {
        return;
      }
      return this.run('query', async () => {
        const { query } = await import('../utils/duckdb.js');
        const started = performance.now();
        this.results = await query(this.sql);
        this.status = this.$t('iceberg.query.took', { seconds: ((performance.now() - started) / 1000).toFixed(2) });
      });
    },
    loadPreview() {
      return this.run('preview', async table => {
        const duckdb = await import('../utils/duckdb.js');
        if (this.columns.length === 0) {
          this.columns = await duckdb.describe(table.scan);
          this.geometryInfo = duckdb.geometryColumn(this.columns, this.info.primaryGeometry);
        }
        this.results = await duckdb.previewRows(table.scan, this.columns, this.geometryInfo);
      });
    },
    querySnapshot(snapshot) {
      this.sql = defaultSql(this.info, { ...this.scanOptions, snapshotId: snapshot.id });
      this.results = null;
      this.activeTab = 'iceberg-query';
    },
    resetSql() {
      this.sql = defaultSql(this.info, this.scanOptions);
      this.results = null;
      this.queryError = null;
      this.status = null;
    },
    loadMap() {
      return this.run('map', async table => {
        const { sampleGeoJson, query } = await import('../utils/duckdb.js');
        const crs = this.geometryCrs;
        const needsReprojection = !LONLAT_CRS.includes(crs.toUpperCase());
        let geometries;
        if (needsReprojection) {
          // DuckDB spatial ships the PROJ database, so it knows national grids
          // that proj4 does not.
          try {
            const { rows } = await query(`
              SELECT ST_AsGeoJSON(ST_Transform(${this.geometryInfo.expr}, ${sqlString(crs)}, 'EPSG:4326', always_xy := true)) AS geojson
              FROM ${table.scan} WHERE ${sqlIdent(this.geometryInfo.name)} IS NOT NULL LIMIT ${MAP_LIMIT}`);
            geometries = rows.map(r => r.geojson && JSON.parse(r.geojson));
          } catch (error) {
            console.warn('[iceberg] ST_Transform failed, trying proj4', error);
            const transform = createLonLatTransform(crs, null);
            if (!transform) {
              throw new Error(this.$t('iceberg.errors.crs', { crs }));
            }
            const { rows } = await sampleGeoJson(table.scan, this.geometryInfo, MAP_LIMIT);
            geometries = rows.map(r => r.geojson && reprojectGeometry(JSON.parse(r.geojson), transform));
          }
        }
        else {
          const { rows } = await sampleGeoJson(table.scan, this.geometryInfo, MAP_LIMIT);
          geometries = rows.map(r => r.geojson && JSON.parse(r.geojson));
        }
        const features = geometries.filter(Boolean).map((geometry, id) => ({ type: 'Feature', id, geometry, properties: {} }));
        await this.renderMap({ type: 'FeatureCollection', features });
        this.status = this.$t('iceberg.query.mapCount', { count: features.length.toLocaleString(), crs });
      });
    },
    async renderMap(featureCollection) {
      const { default: maplibregl } = await import('maplibre-gl');
      await import('maplibre-gl/dist/maplibre-gl.css');
      this.mapShown = true;
      await this.$nextTick();
      const bounds = new maplibregl.LngLatBounds();
      const extend = coords => (typeof coords[0] === 'number' ? bounds.extend(coords) : coords.forEach(extend));
      const visit = geometry => (geometry.geometries ? geometry.geometries.forEach(visit) : extend(geometry.coordinates));
      featureCollection.features.forEach(f => visit(f.geometry));

      const addData = map => {
        if (map.getSource('iceberg')) {
          map.getSource('iceberg').setData(featureCollection);
        }
        else {
          map.addSource('iceberg', { type: 'geojson', data: featureCollection });
          map.addLayer({ id: 'iceberg-fill', type: 'fill', source: 'iceberg', filter: ['==', ['geometry-type'], 'Polygon'], paint: { 'fill-color': '#0077b6', 'fill-opacity': 0.25 } });
          map.addLayer({ id: 'iceberg-line', type: 'line', source: 'iceberg', filter: ['!=', ['geometry-type'], 'Point'], paint: { 'line-color': '#0077b6', 'line-width': 1.2 } });
          map.addLayer({ id: 'iceberg-point', type: 'circle', source: 'iceberg', filter: ['==', ['geometry-type'], 'Point'], paint: { 'circle-color': '#0077b6', 'circle-radius': 3.5, 'circle-stroke-color': '#fff', 'circle-stroke-width': 1 } });
        }
        if (!bounds.isEmpty()) {
          map.fitBounds(bounds, { padding: 30, maxZoom: 15, duration: 0 });
        }
      };

      if (this.map) {
        addData(this.map);
        return;
      }
      const basemap = configureBasemap(this.collection)[0];
      let style = { version: 8, sources: {}, layers: [] };
      if (basemap?.url) {
        style = basemap.url;
      }
      else if (basemap?.tiles) {
        style.sources.basemap = { type: 'raster', tiles: basemap.tiles, tileSize: 256, attribution: basemap.attribution };
        style.layers.push({ id: 'basemap', type: 'raster', source: 'basemap' });
      }
      const map = markRaw(new maplibregl.Map({ container: this.$refs.map, style, attributionControl: { compact: true } }));
      map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right');
      this.map = map;
      map.once('load', () => addData(map));
    },
    destroyMap() {
      if (this.map) {
        this.map.remove();
        this.map = null;
      }
    },
    downloadResult() {
      return this.run('download-result', async () => {
        const { exportToParquet } = await import('../utils/duckdb.js');
        this.status = this.$t('iceberg.download.progress');
        const buffer = await exportToParquet(this.sql, { geometryName: this.geometryInfo?.name });
        this.saveFile(buffer, `${this.collection.id || 'iceberg'}_query.parquet`);
        this.status = null;
      });
    },
    downloadTable() {
      return this.run('download-table', async table => {
        const { exportToParquet } = await import('../utils/duckdb.js');
        this.status = this.$t('iceberg.download.progress');
        // portolake partitioning wrote helper columns the STAC schema leaves
        // out. Drop them so the file matches table:columns.
        const listed = new Set(this.info.tableColumns.map(c => c.name));
        const derived = listed.size > 0
          ? this.columns.map(c => c.name).filter(n => !listed.has(n) && /^(bbox_|geohash_)/.test(n))
          : [];
        const select = derived.length > 0 ? `* EXCLUDE (${derived.map(sqlIdent).join(', ')})` : '*';
        const buffer = await exportToParquet(`SELECT ${select} FROM ${table.scan}`, { geometryName: this.geometryInfo?.name });
        this.saveFile(buffer, `${this.collection.id || 'iceberg'}.parquet`);
        this.status = null;
      });
    },
    saveFile(buffer, filename) {
      const url = URL.createObjectURL(new Blob([buffer], { type: 'application/vnd.apache.parquet' }));
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      a.click();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    },
    async applyToken() {
      this.tokenMessage = null;
      try {
        const { setGcsToken } = await import('../utils/duckdb.js');
        await setGcsToken(this.token);
        try {
          sessionStorage.setItem(TOKEN_KEY, this.token);
        } catch {
          // Storage is unavailable: the token still applies to this session.
        }
        this.tokenMessage = this.$t('iceberg.auth.applied');
      } catch (error) {
        this.tokenMessage = this.formatError(error);
      }
    },
    formatError(error) {
      const message = error?.message || String(error);
      if (/\b403\b|Forbidden/.test(message)) {
        return this.$t('iceberg.errors.forbidden');
      }
      if (/Failed to fetch|NetworkError|CORS/i.test(message)) {
        return this.$t('iceberg.errors.network', { error: message });
      }
      return message;
    }
  }
});
</script>

<style lang="scss">
@import "../theme/variables.scss";
@import "../theme/mixins.scss";

.iceberg-table {
  .iceberg-header {
    @include section-header(".iceberg-title");
    gap: 0.5rem;
  }

  .iceberg-header-left {
    display: flex;
    align-items: center;
    gap: 0.5rem;

    svg {
      color: $secondary;
      flex-shrink: 0;
    }
  }

  .iceberg-table-id {
    font-size: 0.8rem;
    color: $secondary;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .iceberg-body {
    padding-top: 0.75rem;
  }

  .iceberg-tabs .tab-content {
    padding-top: 0.75rem;
  }

  .iceberg-fields {
    display: grid;
    grid-template-columns: minmax(8rem, max-content) 1fr;
    gap: 0.25rem 1rem;
    margin-bottom: 0.5rem;
    font-size: 0.9rem;

    dt {
      font-weight: 600;
    }

    dd {
      margin: 0;
      overflow-wrap: anywhere;
    }
  }

  .iceberg-hint {
    font-size: 0.8rem;
    color: $secondary;
  }

  .iceberg-subtitle {
    font-size: 0.95rem;
    font-weight: 600;
  }

  .table {
    font-size: 0.85rem;
  }

  .iceberg-sql {
    font-size: 0.85rem;
    resize: vertical;
  }

  .iceberg-map {
    width: 100%;
    height: 400px;
    border: 1px solid $border-color;
    border-radius: $border-radius;
  }

  details summary {
    cursor: pointer;
    font-size: 0.9rem;
  }
}
</style>
