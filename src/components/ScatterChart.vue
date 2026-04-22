<template>
  <div class="pt-2">
    <div class="d-flex align-center mx-4">
      <div class="d-flex align-center mr-4">
        <div class="text-subtitle-2 mr-2" style="white-space: nowrap;">Min. Air Temp. (°C):</div>
        <div>
          <v-text-field
            type="number"
            v-model.number="minAirTemp"
            step="1"
            width="80px"
            density="compact"
            variant="outlined"
            hide-details
          />
        </div>
      </div>
      <v-spacer></v-spacer>
      <div class="d-flex align-center text-caption" style="min-width: 200px">
        <v-alert
          color="grey-darken-2"
          density="compact"
          class="text-caption"
          variant="tonal"
        >
          {{ airTempAvailabilityLabel }}
        </v-alert>
      </div>
    </div>
    <v-divider class="my-2"></v-divider>
    <highcharts :options="settings" ref="chartEl"></highcharts>
  </div>
</template>

<script setup>
import { onMounted, watch, ref, computed } from 'vue'
import { DateTime } from 'luxon'

const props = defineProps(['series', 'loading', 'config'])

const chartEl = ref(null)
const minAirTemp = ref(-10)

const airTempAvailabilityLabel = computed(() => {
  const dates = (props.series || [])
    .flatMap(s => s.data || [])
    .filter(d => d.airtemp_c !== null && d.airtemp_c !== undefined && d.date)
    .map(d => d.date)
    .sort()

  if (dates.length === 0) {
    return (props.series || []).length > 0
      ? 'No air temp values for selected stations'
      : 'Select a station to view air temp availability'
  }

  const start = dates[0]
  const end = dates[dates.length - 1]
  return start === end
    ? `Air temp available on ${start}`
    : `Air temp available ${start} to ${end}`
})

watch(() => [props.series, minAirTemp.value], update)
watch(() => props.loading, toggleLoading)

onMounted(() => {
  if (props.series) update()
})

function toggleLoading (loading) {
  const chart = chartEl.value.chart
  if (!chart) return

  if (loading) {
    chart.showLoading()
  } else {
    chart.hideLoading()
  }
}

function tooltipFormatter ({ station_id, date, temp_c, airtemp_c }) {
  return `<table>
    <tbody>
      <tr>
        <td class="pr-2 text-right">Station</td>
        <td><b>${station_id}</b></td>
      </tr>
      <tr>
        <td class="pr-2 text-right">Date</td>
        <td><b>${DateTime.fromISO(date, { zone: 'America/Vancouver' }).toFormat('MMMM d, yyyy')}</b></td>
      </tr>
      <tr>
        <td class="pr-2 text-right">Water Temp</td>
        <td><b>${temp_c?.toFixed(1) + '°C'} </b></td>
      </tr>
      <tr>
        <td class="pr-2 text-right">Air Temp</td>
        <td><b>${airtemp_c?.toFixed(1) + '°C'} </b></td>
      </tr>
    </tbody>
  </table>`
}

function update () {
  const chart = chartEl.value.chart
  if (!chart || !props.series) return

  const totalValues = props.series.reduce((acc, s) => acc + s.data.length, 0)
  const opacity = totalValues <= 1 ? 0.9 : Math.min(0.9, Math.sqrt(2 / Math.log10(totalValues)))

  // Get the min and max values across all data points
  const allPoints = props.series.flatMap(s => s.data.filter(d =>
    d.airtemp_c !== null &&
    d.airtemp_c !== undefined &&
    d.temp_c !== null &&
    d.temp_c !== undefined
  ))

  if (allPoints.length === 0) {
    chart.update({ series: [] }, true, true)
    return
  }

  const minTemp = Math.min(...allPoints.map(d => Math.min(d.airtemp_c, d.temp_c)))
  const maxTemp = Math.max(...allPoints.map(d => Math.max(d.airtemp_c, d.temp_c)))

  const series = []
  if (props.series.length > 0) {
    series.push({
      type: 'line',
      name: '1:1 Line',
      data: [[0, 0], [maxTemp, maxTemp]],
      color: '#000000',
      lineWidth: 2,
      dashStyle: 'dash',
      enableMouseTracking: false,
      showInLegend: false
    })
  }

  // Map the data series
  series.push(...props.series.map(s => ({
      ...s,
      name: s.station_id,
      opacity: opacity > 0.9 ? 0.9 : opacity,
      data: s.data
        .filter(d =>
          d.airtemp_c !== undefined &&
          d.airtemp_c !== null &&
          d.temp_c !== undefined &&
          d.temp_c !== null &&
          d.airtemp_c >= minAirTemp.value
        )
        .map(d => ({
          ...d,
          x: d.airtemp_c,
          y: d.temp_c
        }))
    }))
  )

  chart.update({
    series
  }, true, true)
}

const settings = {
  chart: {
    type: 'scatter',
    zoomType: 'xy',
    animation: false
  },
  lang: {
    noData: 'No data to display'
  },
  plotOptions: {
    series: {
      opacity: 0.75,
      marker: {
        symbol: 'circle',
        radius: 2
      },
      states: {
        hover: {
          enabled: true,
          opacity: 0.9,
          halo: {
            opacity: 0.5,
          }
        }
      }
    },
    line: {  // Add specific options for the 1:1 line
      opacity: 0.5,
      lineWidth: 1,
      marker: {
        enabled: false
      }
    }
  },
  title: {
    text: null
  },
  xAxis: {
    title: {
      text: 'Daily Mean Air Temperature (degC)'
    },
    gridLineWidth: 1
  },
  yAxis: {
    title: {
      text: 'Daily Mean<br>Water Temperature (degC)'
    },
    endOnTick: false
  },
  tooltip: {
    shared: false,
    useHTML: true,
    distance: 32,
    positioner: function (labelWidth, labelHeight, point) {
      const chartWidth = this.chart.chartWidth
      const chartHeight = this.chart.chartHeight
      return {
        x: point.plotX < (chartWidth - labelWidth) / 2 ? point.plotX : chartWidth - labelWidth - 10,
        y: chartHeight - labelHeight - 10,
      }
    },
    formatter: function () {
      return tooltipFormatter(this.point)
    }
  }
}
</script>
