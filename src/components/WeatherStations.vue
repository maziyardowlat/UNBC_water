<template>
  <section class="weather-stations" aria-label="Weather station visualization">
    <div class="d-flex flex-wrap align-center ga-4 mb-4">
      <div>
        <h1 class="text-h4">Weather stations</h1>
        <p class="text-body-1 mt-1">02FW005 · CR350-1379 · Supplied QA/QC observations</p>
      </div>
      <v-spacer />
      <v-btn :href="weatherSource" download prepend-icon="mdi-download" color="primary" variant="outlined">Download original CSV</v-btn>
    </div>
    <v-alert type="info" variant="tonal" class="mb-4">
      Displays the values and QC codes supplied in your file. QA/QC has not been rerun.
      Codes such as snow free, night time and visit describe context and do not necessarily mean a failed observation.
    </v-alert>
    <v-progress-linear v-if="loading" indeterminate color="primary" aria-label="Loading weather data" />
    <v-alert v-else-if="error" type="error">
      {{ error }} <v-btn class="ml-3" variant="outlined" @click="load">Retry</v-btn>
    </v-alert>
    <template v-else-if="dataset">
      <v-card class="pa-4 mb-4" variant="outlined">
        <v-row>
          <v-col cols="12" md="4"><v-select v-model="variable" :items="dataset.variables" item-title="title" item-value="key" label="Weather variable" hide-details /></v-col>
          <v-col cols="12" sm="6" md="2"><v-text-field v-model="startDate" type="date" label="From (UTC−07:00)" :min="firstDate" :max="lastDate" hide-details /></v-col>
          <v-col cols="12" sm="6" md="2"><v-text-field v-model="endDate" type="date" label="To (UTC−07:00)" :min="firstDate" :max="lastDate" hide-details /></v-col>
          <v-col cols="12" md="4"><v-select v-model="display" :items="displayOptions" label="Observations to display" hide-details /></v-col>
        </v-row>
        <div class="d-flex flex-wrap align-center ga-3 mt-3">
          <v-btn size="small" variant="tonal" @click="lastMonth">Latest month</v-btn>
          <v-btn size="small" variant="tonal" @click="startDate = firstDate; endDate = lastDate">Full record</v-btn>
          <span class="text-caption">Available: {{ firstDate }} to {{ lastDate }} · {{ dataset.rows.length.toLocaleString() }} records · Fixed UTC−07:00 as supplied</span>
        </div>
      </v-card>
      <v-alert v-if="dateError" type="warning" class="mb-4">{{ dateError }}</v-alert>
      <v-row class="mb-1">
        <v-col v-for="stat in stats" :key="stat.label" cols="6" md="3">
          <v-card class="pa-4" variant="outlined"><div class="text-caption">{{ stat.label }}</div><div class="text-h5 mt-1">{{ stat.value.toLocaleString() }}</div></v-card>
        </v-col>
      </v-row>
      <v-card class="pa-4 mb-4" variant="outlined">
        <h2 class="text-h6">{{ selectedVariable.title }} <span class="text-body-2">({{ selectedVariable.unit }})</span></h2>
        <p class="text-caption mb-2">Source resolution, without averaging. Amber points carry codes other than P. Missing values and timestamp gaps are left open.</p>
        <highcharts :constructor-type="'stockChart'" :options="chartOptions" />
        <v-alert v-if="!visibleRows.length" type="info" variant="tonal">No observations match these controls.</v-alert>
      </v-card>
      <v-row>
        <v-col cols="12" lg="4">
          <v-card class="pa-4" variant="outlined">
            <h2 class="text-h6">QC code summary</h2>
            <p class="text-caption mb-3">For the selected variable and date range, before the display filter. Multiple codes can occur on one observation.</p>
            <v-table density="compact"><thead><tr><th>Code</th><th>Meaning</th><th>Records</th></tr></thead><tbody>
              <tr v-for="flag in flagSummary" :key="flag.code"><td><strong>{{ flag.code }}</strong></td><td>{{ flagNames[flag.code] || 'Unmapped source code' }}</td><td>{{ flag.count.toLocaleString() }}</td></tr>
            </tbody></v-table>
          </v-card>
        </v-col>
        <v-col cols="12" lg="8">
          <v-card class="pa-4" variant="outlined">
            <div class="d-flex flex-wrap align-center ga-3 mb-3"><h2 class="text-h6">Observation review</h2><v-spacer /><v-btn :disabled="!visibleRows.length" size="small" color="primary" prepend-icon="mdi-download" @click="downloadSelection">Download selection</v-btn></div>
            <v-data-table :headers="headers" :items="tableRows" :items-per-page="10" density="compact" :items-per-page-options="[10, 25, 50]">
              <template #item.value="{ item }">{{ item.value ?? 'Missing' }}</template>
              <template #item.flags="{ item }"><span :class="item.other ? 'text-orange-darken-3' : ''">{{ item.flags || 'Not supplied' }}</span></template>
            </v-data-table>
          </v-card>
        </v-col>
      </v-row>
      <p class="text-caption mt-4">Source: {{ weatherSource.split('/').pop() }} · CSV units row retained in downloads. Station name and site metadata have not been supplied.</p>
    </template>
  </section>
</template>

<script setup>
import { computed, onMounted, ref, shallowRef } from 'vue'
import { DateTime } from 'luxon'
import Papa from 'papaparse'
import { weatherSource, flagNames, flagsFor, hasOtherCodes, numericValue, parseWeatherCSV } from '@/lib/weather'

const dataset = shallowRef(null)
const loading = ref(true)
const error = ref('')
const variable = ref('AirT_C_Avg')
const startDate = ref('')
const endDate = ref('')
const display = ref('all')
const displayOptions = [
  { title: 'All supplied observations', value: 'all' },
  { title: 'Pass only (P, with no other codes)', value: 'pass' },
  { title: 'Codes other than P only', value: 'other' }
]
const firstDate = computed(() => dataset.value?.rows[0].TIMESTAMP.slice(0, 10) || '')
const lastDate = computed(() => dataset.value?.rows.at(-1).TIMESTAMP.slice(0, 10) || '')
const selectedVariable = computed(() => dataset.value?.variables.find(v => v.key === variable.value))
const dateError = computed(() => !startDate.value || !endDate.value || startDate.value > endDate.value ? 'Choose a valid start and end date, with the start on or before the end.' : '')
const periodRows = computed(() => dateError.value ? [] : (dataset.value?.rows || []).filter(row => {
  const date = row.TIMESTAMP.slice(0, 10)
  return date >= startDate.value && date <= endDate.value
}))
const visibleRows = computed(() => periodRows.value.filter(row => {
  if (display.value === 'pass') return flagsFor(row, variable.value).includes('P') && !hasOtherCodes(row, variable.value)
  return display.value !== 'other' || hasOtherCodes(row, variable.value)
}))
const stats = computed(() => [
  { label: 'Records in date range', value: periodRows.value.length },
  { label: 'Pass only (P)', value: periodRows.value.filter(r => flagsFor(r, variable.value).includes('P') && !hasOtherCodes(r, variable.value)).length },
  { label: 'With codes other than P', value: periodRows.value.filter(r => hasOtherCodes(r, variable.value)).length },
  { label: 'Missing / nonnumeric values', value: periodRows.value.filter(r => numericValue(r, variable.value) === null).length }
])
const flagSummary = computed(() => {
  const counts = new Map()
  for (const row of periodRows.value) for (const flag of flagsFor(row, variable.value)) counts.set(flag, (counts.get(flag) || 0) + 1)
  return [...counts].map(([code, count]) => ({ code, count })).sort((a, b) => b.count - a.count)
})
const headers = [
  { title: 'Timestamp (UTC−07:00)', key: 'timestamp' }, { title: 'Value', key: 'value' },
  { title: 'QC codes', key: 'flags' }, { title: 'Data ID', key: 'dataId' }
]
const tableRows = computed(() => visibleRows.value.map(row => ({
  timestamp: row.TIMESTAMP, value: numericValue(row, variable.value),
  flags: flagsFor(row, variable.value).join(', '), other: hasOtherCodes(row, variable.value), dataId: row.Data_ID
})))
const chartOptions = computed(() => {
  // Retain excluded records as nulls so filtering never bridges across them.
  const included = new Set(visibleRows.value)
  const points = periodRows.value.map(row => ({
    x: row.millis, y: included.has(row) ? numericValue(row, variable.value) : null,
    custom: { timestamp: row.TIMESTAMP, flags: flagsFor(row, variable.value).join(', ') || 'Not supplied' }
  }))
  const flagged = points.filter((p, i) => p.y !== null && hasOtherCodes(periodRows.value[i], variable.value))
  return {
    chart: { height: 430, animation: false },
    time: { timezoneOffset: 420 },
    title: { text: undefined }, credits: { enabled: false },
    rangeSelector: { enabled: false },
    navigator: { enabled: true, series: { data: points.map(p => [p.x, p.y]) } },
    xAxis: { type: 'datetime' },
    yAxis: { title: { text: selectedVariable.value?.unit }, opposite: false },
    legend: { enabled: true },
    tooltip: {
      shared: false, useHTML: false,
      pointFormatter: function () {
        // All custom text comes from the fixed imported source, never rendered as HTML.
        return `${this.y} · QC: ${this.custom.flags}`
      },
      xDateFormat: '%Y-%m-%d %H:%M (UTC−07:00)'
    },
    plotOptions: { series: { animation: false, dataGrouping: { enabled: false }, turboThreshold: 0, connectNulls: false, gapSize: 30 * 60 * 1000, gapUnit: 'value' } },
    series: [
      { type: 'line', name: 'Supplied values', color: '#176d59', lineWidth: 1.5, data: points },
      { type: 'scatter', name: 'Codes other than P', color: '#b86c00', marker: { radius: 3, symbol: 'circle' }, data: flagged }
    ]
  }
})

function lastMonth () {
  endDate.value = lastDate.value
  startDate.value = DateTime.fromISO(lastDate.value).minus({ months: 1 }).toISODate()
  if (startDate.value < firstDate.value) startDate.value = firstDate.value
}

async function load () {
  loading.value = true
  error.value = ''
  try {
    const response = await fetch(weatherSource)
    if (!response.ok) throw new Error(`Weather data request failed (${response.status}).`)
    dataset.value = parseWeatherCSV(await response.text())
    lastMonth()
  } catch (err) {
    error.value = err.message || 'Could not load weather data.'
  } finally {
    loading.value = false
  }
}

function downloadSelection () {
  const fields = dataset.value.fields
  const data = [dataset.value.units, ...visibleRows.value].map(row => fields.map(key => row[key]))
  const csv = Papa.unparse({ fields, data })
  const url = URL.createObjectURL(new Blob([csv], { type: 'text/csv;charset=utf-8' }))
  const link = document.createElement('a')
  link.href = url
  link.download = `02FW005_weather_${startDate.value}_${endDate.value}_${variable.value}_${display.value}.csv`
  document.body.appendChild(link)
  link.click()
  link.remove()
  setTimeout(() => URL.revokeObjectURL(url), 1000)
}

onMounted(load)
</script>

<style scoped>
.weather-stations { width: 100%; min-width: 0; padding: 12px 0 24px; }
</style>
