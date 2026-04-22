<template>
  <highcharts :constructor-type="'stockChart'" :options="settings" ref="chartEl"></highcharts>
</template>

<script setup>
import { onMounted, watch, ref, computed } from 'vue'
import { DateTime } from 'luxon'
import { monthOptions } from '@/lib/constants'

const props = defineProps({
  series: Array,
  loading: Boolean,
  aggregation: String,
  aggregationLabel: String,
  season: Array,
  temperatureMetric: {
    type: String,
    default: 'water'
  }
})
const emit = defineEmits(['zoom'])
const chartEl = ref(null)

watch(() => props.series, update)
watch(() => props.loading, toggleLoading)
watch(() => [props.aggregation, props.aggregationLabel, props.temperatureMetric], update)

onMounted(() => {
  if (props.series) update()
})

function onTimeFilter (event) {
  if (!event) return
  if (event.min && event.max) {
    emit('zoom', [new Date(event.min), new Date(event.max)])
  }
}

function toggleLoading (loading) {
  const chart = chartEl.value.chart
  if (!chart) return

  if (loading) {
    chart.showLoading()
  } else {
    chart.hideLoading()
  }
}

function update () {
  const chart = chartEl.value.chart
  if (!chart || !props.series) return

  const nPrevious = chart.series.length
  const metricKeys = selectedTemperatureKeys.value
  const series = props.series.flatMap(s => {
    return metricKeys.map(metricKey => {
      const metric = temperatureMetrics[metricKey]
      return {
        ...s,
        name: metricKeys.length > 1 ? `${s.station_id} ${metric.shortLabel}` : s.station_id,
        dashStyle: metric.dashStyle,
        showInNavigator: metricKeys.length === 1 || metricKey === 'water',
        marker: {
          enabled: props.aggregation !== 'day'
        },
        data: s.data.map(d => {
          const temperatureValue = d[metric.field] ?? null
          return {
            ...d,
            x: d.millis,
            y: temperatureValue,
            temperature_metric: metricKey,
            temperature_label: metric.shortLabel,
            temperature_value: temperatureValue
          }
        })
      }
    })
  }).filter(s => s.data.some(d => d.y !== null))

  chart.yAxis[0].update({
    title: {
      text: yAxisTitle.value
    },
    min: props.temperatureMetric === 'water' ? 0 : null
  }, false)

  const noData = noDataMessage(series)
  chart.update({
    lang: {
      noData
    },
    series: [],
    tooltip: {
      enabled: false
    }
  }, true, true)
  chart.update({
    lang: {
      noData
    },
    series,
    tooltip: {
      enabled: true
    }
  }, true, true)

  if (nPrevious === 0) {
    // reset time filter
    chart.xAxis[0].setExtremes(null, null, true, false)
  }
}

function noDataMessage (series) {
  if (!props.series || props.series.length === 0) {
    return 'Select a station to view data'
  }
  if (props.temperatureMetric === 'air' && series.length === 0) {
    return 'There are no air temp values for this site right now'
  }
  return 'No data to display'
}

function tooltipFormatter(date, points) {
  if (!points || points.length === 0) return ''

  let header
  if (props.aggregation === 'day') {
    header = DateTime.fromISO(date, { zone: 'America/Vancouver' }).toFormat('MMMM d, yyyy')
  } else if (props.aggregation === 'month') {
    header = DateTime.fromISO(date, { zone: 'America/Vancouver' }).toFormat('MMMM yyyy')
  } else if (props.aggregation === 'season') {
    const startMonthLabel = monthOptions.find(m => m.value === props.season[0])?.label.substring(0,3)
    const endMonthLabel = monthOptions.find(m => m.value === props.season[1])?.label.substring(0,3)
    let seasonLabel = `${startMonthLabel}-${endMonthLabel}`
    if (startMonthLabel === endMonthLabel) {
      seasonLabel = startMonthLabel
    }
    header = `${seasonLabel} ${DateTime.fromISO(date, { zone: 'America/Vancouver' }).toFormat('yyyy')}`
  } else {
    header = DateTime.fromISO(date, { zone: 'America/Vancouver' }).toFormat('MMMM d, yyyy')
  }

  const normalizedPoints = points.map(d => d.point || d)
  const rows = normalizedPoints.map(point => {
    const value = typeof point.temperature_value === 'number'
      ? point.temperature_value.toFixed(1)
      : ''
    return `
    <tr>
      <td style="color: ${point.color}; padding-right: 10px; font-size: 18px;">&#9679;</td>
      <td style="font-weight: bold; padding-right: 12px;">${point.station_id}</td>
      <td style="padding-right: 12px;">${point.temperature_label}</td>
      <td style="text-align: right; font-weight: bold;">${value}</td>
    </tr>
  `
  }).join('')
  return `
    <div style="">
      <div style="font-size: 16px; font-weight: bold; margin-bottom: 10px; color: #333;">${header}</div>
      <table style="border-collapse: separate; border-spacing: 0 6px; font-size: 14px;">
        <thead>
          <tr>
            <th></th>
            <th style="text-align: left; color: #666; font-weight: normal;">Station</th>
            <th style="text-align: left; color: #666; font-weight: normal;">Metric</th>
            <th style="text-align: left; color: #666; font-weight: normal;">Temp (°C)</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `
}

const temperatureMetrics = {
  water: {
    field: 'temp_c',
    shortLabel: 'Water',
    axisLabel: 'Water Temperature',
    dashStyle: 'Solid'
  },
  air: {
    field: 'airtemp_c',
    shortLabel: 'Air',
    axisLabel: 'Air Temperature',
    dashStyle: 'ShortDash'
  }
}

const selectedTemperatureKeys = computed(() => {
  return props.temperatureMetric === 'both'
    ? ['water', 'air']
    : [props.temperatureMetric || 'water']
})

const yAxisTitle = computed(() => {
  const metricLabel = props.temperatureMetric === 'both'
    ? 'Temperature'
    : temperatureMetrics[props.temperatureMetric || 'water'].axisLabel
  return `${props.aggregationLabel}<br>${metricLabel} (°C)`
})

const settings = {
  chart: {
    height: 470,
    marginLeft: 70,
    zoomType: 'x',
    boost: {
      enabled: false
    }
  },
  time: {
    timezone: 'America/Vancouver'
  },
  title: {
    text: null
  },
  plotOptions: {
    series: {
      gapSize: 2,
      dataGrouping: {
        enabled: false
      },
      marker: {
        symbol: 'circle',
        radius: 4
      },
      lineWidth: 2
    },
  },
  lang: {
    noData: 'Select a station to view data'
  },
  noData: {
    style: {
      fontWeight: 'bold',
      fontSize: '15px',
      color: '#303030'
    }
  },
  loading: {
    style: {
      position: 'absolute',
      backgroundColor: '#ffffff',
      opacity: 1,
      textAlign: 'center'
    }
  },
  legend: {
    enabled: true,
    align: 'right'
  },
  tooltip: {
    shared: false,
    useHTML: true,
    formatter: function () {
      return tooltipFormatter(this.point.date, this.points || [this.point])
    }
  },
  scrollbar: {
    liveRedraw: false // enable for interactive filtering
  },
  navigator: {
    adaptToUpdatedData: false,
  },
  rangeSelector: {
    selected: 4,
    buttons: [{
      type: 'month',
      count: 1,
      text: '1m',
      title: 'View 1 month'
    }, {
      type: 'month',
      count: 3,
      text: '3m',
      title: 'View 3 months'
    }, {
      type: 'month',
      count: 6,
      text: '6m',
      title: 'View 6 months'
    }, {
      type: 'year',
      count: 1,
      text: '1y',
      title: 'View 1 year'
    }, {
      type: 'all',
      text: 'All',
      title: 'View all'
    }]
  },
  xAxis: {
    ordinal: false,
    minRange: 24 * 3600 * 1000,
    gridLineWidth: 1,
    events: {
      afterSetExtremes: onTimeFilter
    }
  },
  yAxis: {
    allowDecimals: false,
    opposite: false,
    startOnTick: false,
    endOnTick: false,
    tickAmount: 8,
    title: {
      text: yAxisTitle.value
    },
    min: 0
  },
  credits: {
    enabled: false
  },
  series: []
}

</script>
