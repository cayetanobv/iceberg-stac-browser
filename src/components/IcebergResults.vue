<template>
  <div class="iceberg-results">
    <div class="iceberg-results-info">
      {{ $t('iceberg.query.resultInfo', { rows: data.numRows.toLocaleString(), columns: data.columns.length }) }}
    </div>
    <div v-if="data.numRows > 0" class="table-responsive">
      <table class="table table-sm table-striped table-hover">
        <thead>
          <tr>
            <th v-for="col in data.columns" :key="col" class="sortable" @click="toggleSort(col)">
              {{ col }}
              <span v-if="sortCol === col">{{ sortAsc ? '▲' : '▼' }}</span>
            </th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, idx) in pageRows" :key="idx">
            <td v-for="col in data.columns" :key="col" :title="cellTitle(row[col])">{{ formatCell(row[col]) }}</td>
          </tr>
        </tbody>
      </table>
    </div>
    <div v-if="totalPages > 1" class="d-flex justify-content-between align-items-center">
      <b-button size="sm" variant="outline-secondary" :disabled="page === 0" @click="page--">{{ $t('pagination.previous') }}</b-button>
      <span class="text-muted small">{{ page + 1 }} / {{ totalPages }}</span>
      <b-button size="sm" variant="outline-secondary" :disabled="page >= totalPages - 1" @click="page++">{{ $t('pagination.next') }}</b-button>
    </div>
  </div>
</template>

<script>
import { defineComponent } from 'vue';

const MAX_CELL = 80;

export default defineComponent({
  name: 'IcebergResults',
  props: {
    data: {
      type: Object,
      required: true
    },
    pageSize: {
      type: Number,
      default: 25
    }
  },
  data() {
    return {
      page: 0,
      sortCol: null,
      sortAsc: true
    };
  },
  computed: {
    sortedRows() {
      const rows = [...this.data.rows];
      if (!this.sortCol) {
        return rows;
      }
      const col = this.sortCol;
      const dir = this.sortAsc ? 1 : -1;
      return rows.sort((a, b) => {
        const va = a[col];
        const vb = b[col];
        if (va === vb) {return 0;}
        if (va === null || va === undefined) {return 1;}
        if (vb === null || vb === undefined) {return -1;}
        if (typeof va === 'number' && typeof vb === 'number') {return (va - vb) * dir;}
        return String(va).localeCompare(String(vb), undefined, { numeric: true }) * dir;
      });
    },
    totalPages() {
      return Math.ceil(this.data.numRows / this.pageSize);
    },
    pageRows() {
      const start = this.page * this.pageSize;
      return this.sortedRows.slice(start, start + this.pageSize);
    }
  },
  watch: {
    data() {
      this.page = 0;
      this.sortCol = null;
    }
  },
  methods: {
    toggleSort(col) {
      if (this.sortCol === col) {
        this.sortAsc = !this.sortAsc;
      }
      else {
        this.sortCol = col;
        this.sortAsc = true;
      }
      this.page = 0;
    },
    formatCell(value) {
      if (value === null || value === undefined) {
        return '';
      }
      const text = String(value);
      return text.length > MAX_CELL ? `${text.slice(0, MAX_CELL)}…` : text;
    },
    cellTitle(value) {
      return value !== null && value !== undefined && String(value).length > MAX_CELL ? String(value) : null;
    }
  }
});
</script>

<style lang="scss" scoped>
.iceberg-results-info {
  font-size: 0.8rem;
  color: var(--bs-secondary-color);
  margin-bottom: 0.25rem;
}

.table {
  font-size: 0.8rem;
  white-space: nowrap;
}

.sortable {
  cursor: pointer;
  user-select: none;
}
</style>
