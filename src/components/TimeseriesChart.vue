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
  season: Array
})
const emit = defineEmits(['zoom'])
const chartEl = ref(null)

watch(() => props.series, update)
watch(() => props.loading, toggleLoading)
watch(() => [props.aggregation, props.aggregationLabel], () => {
  if (chartEl.value?.chart) {
    chartEl.value.chart.series.forEach(s => {
      if (s.navigatorSeries) {
        s.update({
          marker: {
            enabled: props.aggregation !== 'day'
          }
        }, false)
      }
    })
    chartEl.value.chart.redraw()
    chartEl.value.chart.yAxis[0].setTitle({
      text: `${props.aggregationLabel}<br>Water Temperature (°C)`
    })
  }
})

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
  const series = props.series.map(s => {
    return {
      ...s,
      name: s.station_id,
      showInNavigator: true,
      marker: {
        enabled: props.aggregation !== 'day'
      },
      data: s.data.map(d => ({
        ...d,
        x: d.millis,
        y: d.temp_c ?? null,
      }))
    }
  })

  chart.update({
    series: [],
    tooltip: {
      enabled: false
    }
  }, true, true)
  chart.update({
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

  const rows = points.map(d => `
    <tr>
      <td style="color: ${d.point.color}; padding-right: 10px; font-size: 18px;">&#9679;</td>
      <td style="font-weight: bold; padding-right: 12px;">${d.point.station_id}</td>
      <td style="text-align: right; font-weight: bold;">${d.point.temp_c?.toFixed(1)}</td>
    </tr>
  `).join('');
  return `
    <div style="">
      <div style="font-size: 16px; font-weight: bold; margin-bottom: 10px; color: #333;">${header}</div>
      <table style="border-collapse: separate; border-spacing: 0 6px; font-size: 14px;">
        <thead>
          <tr>
            <th></th>
            <th style="text-align: left; color: #666; font-weight: normal;">Station</th>
            <th style="text-align: left; color: #666; font-weight: normal;">Water Temp (°C)</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

const yAxisTitle = computed(() => {
  return `${props.aggregationLabel}<br>Water Temperature (°C)`
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
    headerFormat: '<div style="font-size: 16px; font-weight: bold; margin-bottom: 10px; color: #333;">{point.date}asdfs</div>',
    formatter: function () {
      return tooltipFormatter(this.point.date, this.points)
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