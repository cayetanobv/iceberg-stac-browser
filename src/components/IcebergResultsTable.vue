<template>
  <div class="results-table-wrapper">
    <div class="results-info mb-2 text-muted" v-if="data">
      {{ data.numRows }} row{{ data.numRows !== 1 ? 's' : '' }} · {{ data.columns.length }} column{{ data.columns.length !== 1 ? 's' : '' }}
    </div>
    <div class="table-responsive" v-if="data && data.numRows > 0">
      <table class="table table-sm table-striped table-hover">
        <thead>
          <tr>
            <th
              v-for="col in data.columns"
              :key="col"
              @click="toggleSort(col)"
              class="sortable"
            >
              {{ col }}
              <span v-if="sortCol === col">{{ sortAsc ? '▲' : '▼' }}</span>
            </th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, idx) in paginatedRows" :key="idx">
            <td v-for="col in data.columns" :key="col">{{ formatCell(row[col]) }}</td>
          </tr>
        </tbody>
      </table>
    </div>
    <div v-if="totalPages > 1" class="d-flex justify-content-between align-items-center mt-2">
      <button class="btn btn-sm btn-outline-secondary" :disabled="page === 0" @click="page--">Previous</button>
      <span class="text-muted">Page {{ page + 1 }} of {{ totalPages }}</span>
      <button class="btn btn-sm btn-outline-secondary" :disabled="page >= totalPages - 1" @click="page++">Next</button>
    </div>
  </div>
</template>

<script>
export default {
  name: 'IcebergResultsTable',
  props: {
    data: {
      type: Object,
      default: null
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
      if (!this.data) return [];
      const rows = [...this.data.rows];
      if (this.sortCol) {
        const col = this.sortCol;
        const dir = this.sortAsc ? 1 : -1;
        rows.sort((a, b) => {
          const va = a[col], vb = b[col];
          if (va === vb) return 0;
          if (va === null || va === undefined) return 1;
          if (vb === null || vb === undefined) return -1;
          return va < vb ? -dir : dir;
        });
      }
      return rows;
    },
    totalPages() {
      return Math.ceil(this.sortedRows.length / this.pageSize);
    },
    paginatedRows() {
      const start = this.page * this.pageSize;
      return this.sortedRows.slice(start, start + this.pageSize);
    }
  },
  watch: {
    data() {
      this.page = 0;
      this.sortCol = null;
      this.sortAsc = true;
    }
  },
  methods: {
    toggleSort(col) {
      if (this.sortCol === col) {
        this.sortAsc = !this.sortAsc;
      } else {
        this.sortCol = col;
        this.sortAsc = true;
      }
    },
    formatCell(val) {
      if (val === null || val === undefined) return '';
      if (val instanceof Date) return val.toISOString();
      if (typeof val === 'object') return JSON.stringify(val);
      return String(val);
    }
  }
};
</script>

<style lang="scss" scoped>
.results-table-wrapper {
  font-size: 0.875rem;
}

.table-responsive {
  max-height: 500px;
  overflow: auto;
}

th.sortable {
  cursor: pointer;
  user-select: none;
  white-space: nowrap;
}

td {
  max-width: 300px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
</style>
