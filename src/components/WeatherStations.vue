<template>
  <section class="weather-stations" aria-label="Weather station visualization">
    <div class="d-flex flex-wrap align-center ga-4 mb-4">
      <div><h1 class="text-h4">Weather stations</h1><p class="mt-1">Explore station observations and their supplied QA/QC codes.</p></div>
      <v-spacer />
      <v-btn v-if="selectedStation?.files.length" :href="selectedStation.files[0].url" download prepend-icon="mdi-download" color="primary" variant="outlined">
        {{ selectedStation.files[0].format === 'zip' ? 'Download original CSV (ZIP)' : 'Download original CSV' }}
      </v-btn>
    </div>
    <v-progress-linear v-if="catalogLoading" indeterminate aria-label="Loading weather stations" />
    <v-alert v-if="catalogError" type="error">{{ catalogError }} <v-btn variant="outlined" @click="loadCatalog">Retry</v-btn></v-alert>
    <v-row v-if="stations.length" class="mb-4">
      <v-col cols="12" md="4">
        <v-card variant="outlined" class="station-picker">
          <div class="pa-4 pb-0"><h2 class="text-h6">Station locations</h2><v-text-field v-model="search" label="Search station name or code" prepend-inner-icon="mdi-magnify" density="compact" hide-details class="mt-3" /></div>
          <v-list aria-label="Weather stations" class="station-list">
            <v-list-item v-for="station in filteredStations" :key="station.id" :active="station.id === selectedId" color="primary"
              :title="station.name" :subtitle="`${station.id} · ${station.status === 'available' ? 'Tidy data available' : 'Awaiting tidy data'}`" @click="selectStation(station.id)">
              <template #prepend><v-icon :color="station.status === 'available' ? 'green-darken-3' : 'grey'" size="small">mdi-map-marker</v-icon></template>
            </v-list-item>
            <v-list-item v-if="!filteredStations.length" title="No matching stations" />
          </v-list>
        </v-card>
      </v-col>
      <v-col cols="12" md="8"><v-card variant="outlined"><WeatherStationMap :stations="stations" :selected="selectedId" @select="selectStation" />
        <p class="text-caption px-3 py-2">Select a marker or a station name. Blue: selected · Green: tidy data available · Grey: awaiting tidy data.</p>
      </v-card></v-col>
    </v-row>
    <v-card v-if="selectedStation" variant="tonal" class="pa-4 mb-4">
      <div class="d-flex flex-wrap align-center ga-3"><div><h2 class="text-h5">{{ selectedStation.name }}</h2><p class="text-body-2 mt-1">{{ selectedStation.id }} · {{ selectedStation.latitude.toFixed(6) }}°, {{ selectedStation.longitude.toFixed(6) }}°</p></div><v-spacer />
        <v-btn v-if="selectedStation.metadataUrl" :href="selectedStation.metadataUrl" download variant="outlined" size="small">Download metadata</v-btn>
      </div>
      <p class="text-caption mt-2">Location source: {{ selectedStation.coordinateSource }}</p>
    </v-card>
    <v-alert v-if="selectedStation?.status === 'pending'" type="info" variant="tonal">{{ selectedStation.name }} is included on the map. No tidy QA/QC CSV was supplied for this station; its raw data has not been presented as quality controlled.</v-alert>
    <template v-if="selectedStation?.status === 'available'">
      <v-alert type="info" variant="tonal" class="mb-4">Displays the values and QC codes supplied in your file. QA/QC has not been rerun. Codes such as snow free, night time and visit describe context and do not necessarily mean a failed observation.</v-alert>
      <v-card class="pa-4 mb-4" variant="outlined">
        <v-row>
          <v-col cols="12" md="4"><v-select v-model="variable" :items="variableOptions" item-title="title" item-value="key" label="Weather variable" hide-details /></v-col>
          <v-col cols="12" sm="6" md="2"><v-text-field v-model="startDate" type="date" :label="`From (${timezoneLabel})`" :min="firstDate" :max="lastDate" hide-details /></v-col>
          <v-col cols="12" sm="6" md="2"><v-text-field v-model="endDate" type="date" :label="`To (${timezoneLabel})`" :min="firstDate" :max="lastDate" hide-details /></v-col>
          <v-col cols="12" md="4"><v-select v-model="display" :items="displayOptions" label="Observations to display" hide-details /></v-col>
        </v-row>
        <div class="d-flex flex-wrap align-center ga-3 mt-3">
          <v-btn size="small" variant="tonal" @click="lastMonth">Latest month</v-btn>
          <v-btn v-if="largeStation" size="small" variant="tonal" @click="latestYear">Latest year</v-btn>
          <v-btn v-else size="small" variant="tonal" @click="startDate = firstDate; endDate = lastDate">Full record</v-btn>
          <span class="text-caption">Available: {{ firstDate }} to {{ lastDate }} · {{ selectedStation.records.toLocaleString() }} records · {{ timezoneLabel }} as supplied</span>
        </div>
        <p v-if="largeStation" class="text-caption mt-2">For this long historical record, choose up to one year at a time. The original CSV download contains the full record.</p>
      </v-card>
      <v-alert v-if="dateError" type="warning" class="mb-4">{{ dateError }}</v-alert>
      <v-progress-linear v-if="loading" indeterminate color="primary" aria-label="Loading selected weather observations" />
      <v-alert v-if="error" type="error" class="mb-4">{{ error }} <v-btn variant="outlined" @click="loadRange">Retry</v-btn></v-alert>
      <template v-if="dataset && !loading && !dateError && !error">
        <v-row class="mb-1"><v-col v-for="stat in stats" :key="stat.label" cols="6" md="3"><v-card class="pa-4" variant="outlined"><div class="text-caption">{{ stat.label }}</div><div class="text-h5 mt-1">{{ stat.value.toLocaleString() }}</div></v-card></v-col></v-row>
        <v-card class="pa-4 mb-4" variant="outlined">
          <h2 class="text-h6">{{ selectedStation.name }} — {{ selectedVariable.title }} <span class="text-body-2">({{ selectedVariable.unit }})</span></h2>
          <p class="text-caption mb-2">Source resolution, without averaging. Amber points carry codes other than P. Missing values and timestamp gaps are left open.</p>
          <highcharts :constructor-type="'stockChart'" :options="chartOptions" />
          <v-alert v-if="!visibleRows.length" type="info" variant="tonal">No observations match these controls.</v-alert>
        </v-card>
        <v-row>
          <v-col cols="12" lg="4"><v-card class="pa-4" variant="outlined">
            <h2 class="text-h6">QC code summary</h2><p class="text-caption mb-3">For the selected variable and date range, before the display filter. Multiple codes can occur on one observation.</p>
            <v-table density="compact"><thead><tr><th>Code</th><th>Meaning</th><th>Records</th></tr></thead><tbody><tr v-for="flag in flagSummary" :key="flag.code"><td><strong>{{ flag.code }}</strong></td><td>{{ flagNames[flag.code] || 'Unmapped source code' }}</td><td>{{ flag.count.toLocaleString() }}</td></tr></tbody></v-table>
          </v-card></v-col>
          <v-col cols="12" lg="8"><v-card class="pa-4" variant="outlined">
            <div class="d-flex flex-wrap align-center ga-3 mb-3"><h2 class="text-h6">Observation review</h2><v-spacer /><v-btn :disabled="!visibleRows.length" size="small" color="primary" prepend-icon="mdi-download" @click="downloadSelection">Download selection</v-btn></div>
            <v-data-table :headers="headers" :items="tableRows" :items-per-page="10" density="compact" :items-per-page-options="[10, 25, 50]">
              <template #item.value="{ item }">{{ item.value ?? 'Missing' }}</template><template #item.flags="{ item }"><span :class="item.other ? 'text-orange-darken-3' : ''">{{ item.flags || 'Not supplied' }}</span></template>
            </v-data-table>
          </v-card></v-col>
        </v-row>
        <p class="text-caption mt-4">Tidy source: 03_Data_Tidy / {{ selectedStation.files[0].name }} · CSV units row and all source columns retained in selection downloads.</p>
      </template>
    </template>
  </section>
</template>

<script setup>
import { computed, onMounted, onBeforeUnmount, ref, shallowRef, watch } from 'vue'
import { DateTime } from 'luxon'
import Papa from 'papaparse'
import WeatherStationMap from '@/components/WeatherStationMap.vue'
import { weatherCatalog, flagNames, flagsFor, hasOtherCodes, numericValue, weatherVariables, loadWeatherRange } from '@/lib/weather'

const stations = shallowRef([])
const selectedId = ref('02FW005')
const search = ref('')
const catalogLoading = ref(true)
const catalogError = ref('')
const dataset = shallowRef(null)
const loading = ref(false)
const error = ref('')
const variable = ref('AirT_C_Avg')
const startDate = ref('')
const endDate = ref('')
const display = ref('all')
let controller
let requestId = 0
const selectedStation = computed(() => stations.value.find(s => s.id === selectedId.value))
const filteredStations = computed(() => stations.value.filter(s => `${s.name} ${s.id}`.toLowerCase().includes(search.value.toLowerCase())))
const firstDate = computed(() => selectedStation.value?.start || '')
const lastDate = computed(() => selectedStation.value?.end || '')
const largeStation = computed(() => selectedStation.value?.records > 100000)
const timezoneLabel = computed(() => selectedStation.value?.offsets.length === 1 ? `UTC${selectedStation.value.offsets[0]}` : 'Source offset')
const variableOptions = computed(() => selectedStation.value?.fields ? weatherVariables(selectedStation.value.fields, selectedStation.value.units) : [])
const selectedVariable = computed(() => variableOptions.value.find(v => v.key === variable.value))
const displayOptions = [{ title: 'All supplied observations', value: 'all' }, { title: 'Pass only (P, with no other codes)', value: 'pass' }, { title: 'Codes other than P only', value: 'other' }]
const dateError = computed(() => {
  const start = DateTime.fromISO(startDate.value)
  const end = DateTime.fromISO(endDate.value)
  if (!start.isValid || !end.isValid || startDate.value > endDate.value) return 'Choose valid dates, with the start on or before the end.'
  if (startDate.value < firstDate.value || endDate.value > lastDate.value) return 'Choose dates within this station’s available record.'
  if (largeStation.value && end.diff(start, 'days').days > 366) return 'Choose up to one year at a time for this historical station. Download the original CSV for the full record.'
  return ''
})
const periodRows = computed(() => dateError.value ? [] : dataset.value?.rows || [])
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
const headers = computed(() => [{ title: `Timestamp (${timezoneLabel.value})`, key: 'timestamp' }, { title: 'Value', key: 'value' }, { title: 'QC codes', key: 'flags' }, { title: 'Data ID', key: 'dataId' }])
const tableRows = computed(() => visibleRows.value.map(row => ({ timestamp: row.TIMESTAMP, value: numericValue(row, variable.value), flags: flagsFor(row, variable.value).join(', '), other: hasOtherCodes(row, variable.value), dataId: row.Data_ID })))
const chartOptions = computed(() => {
  const included = new Set(visibleRows.value)
  const points = periodRows.value.map(row => ({ x: row.millis, y: included.has(row) ? numericValue(row, variable.value) : null, custom: { flags: flagsFor(row, variable.value).join(', ') || 'Not supplied' } }))
  const flagged = points.filter((p, i) => p.y !== null && hasOtherCodes(periodRows.value[i], variable.value))
  const offset = selectedStation.value?.offsets[0] || '-07:00'
  const offsetMinutes = (Number(offset.slice(1, 3)) * 60 + Number(offset.slice(4, 6))) * (offset[0] === '-' ? 1 : -1)
  return {
    chart: { height: 430, animation: false }, time: { timezoneOffset: offsetMinutes }, title: { text: undefined }, credits: { enabled: false },
    rangeSelector: { enabled: false }, navigator: { enabled: true, series: { data: points.map(p => [p.x, p.y]) } }, xAxis: { type: 'datetime' },
    yAxis: { title: { text: selectedVariable.value?.unit }, opposite: false }, legend: { enabled: true },
    tooltip: { shared: false, useHTML: false, pointFormatter: function () { return `${this.y} · QC: ${this.custom.flags}` }, xDateFormat: `%Y-%m-%d %H:%M (${timezoneLabel.value})` },
    plotOptions: { series: { animation: false, dataGrouping: { enabled: false }, turboThreshold: 0, connectNulls: false, gapSize: 30 * 60 * 1000, gapUnit: 'value' } },
    series: [{ type: 'line', name: 'Supplied values', color: '#176d59', lineWidth: 1.5, data: points }, { type: 'scatter', name: 'Codes other than P', color: '#b86c00', marker: { radius: 3, symbol: 'circle' }, data: flagged }]
  }
})
function lastMonth () {
  endDate.value = lastDate.value
  startDate.value = DateTime.fromISO(lastDate.value).minus({ months: 1 }).toISODate()
  if (startDate.value < firstDate.value) startDate.value = firstDate.value
}
function latestYear () {
  endDate.value = lastDate.value
  startDate.value = DateTime.fromISO(lastDate.value).minus({ years: 1 }).toISODate()
  if (startDate.value < firstDate.value) startDate.value = firstDate.value
}
function selectStation (id) {
  if (id === selectedId.value && startDate.value && endDate.value) {
    if (error.value) loadRange()
    return
  }
  selectedId.value = id
  dataset.value = null
  error.value = ''
  display.value = 'all'
  variable.value = variableOptions.value.some(v => v.key === 'AirT_C_Avg') ? 'AirT_C_Avg' : variableOptions.value[0]?.key || ''
  if (selectedStation.value?.status === 'available') lastMonth()
  else { startDate.value = ''; endDate.value = '' }
}
async function loadCatalog () {
  catalogLoading.value = true
  catalogError.value = ''
  try {
    const response = await fetch(weatherCatalog)
    if (!response.ok) throw new Error(`Station catalog request failed (${response.status}).`)
    stations.value = await response.json()
    selectStation(stations.value.some(s => s.id === selectedId.value) ? selectedId.value : stations.value[0]?.id)
  } catch (err) { catalogError.value = err.message || 'Could not load weather stations.' }
  finally { catalogLoading.value = false }
}
async function loadRange () {
  controller?.abort()
  const current = ++requestId
  dataset.value = null
  error.value = ''
  loading.value = false
  if (selectedStation.value?.status !== 'available' || dateError.value) return
  controller = new AbortController()
  loading.value = true
  try {
    const result = await loadWeatherRange(selectedStation.value, startDate.value, endDate.value, controller.signal)
    if (current === requestId) dataset.value = result
  } catch (err) {
    if (current === requestId && err.name !== 'AbortError') error.value = err.message || 'Could not load weather data.'
  } finally { if (current === requestId) loading.value = false }
}
function downloadSelection () {
  const fields = dataset.value.fields
  const data = [dataset.value.units, ...visibleRows.value].map(row => fields.map(key => row[key]))
  const url = URL.createObjectURL(new Blob([Papa.unparse({ fields, data })], { type: 'text/csv;charset=utf-8' }))
  const link = document.createElement('a')
  link.href = url
  link.download = `${selectedStation.value.id}_${selectedStation.value.name.replaceAll(' ', '_')}_${startDate.value}_${endDate.value}_${variable.value}_${display.value}.csv`
  document.body.appendChild(link); link.click(); link.remove()
  setTimeout(() => URL.revokeObjectURL(url), 1000)
}
watch([selectedId, startDate, endDate], loadRange)
onMounted(loadCatalog)
onBeforeUnmount(() => { requestId++; controller?.abort() })
</script>

<style scoped>
.weather-stations { width: 100%; min-width: 0; padding: 12px 0 24px; }
.station-picker { height: 100%; }
.station-list { max-height: 328px; overflow-y: auto; }
</style>
