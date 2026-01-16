import Papa from 'papaparse'
import { DateTime } from 'luxon'
import { saveAs } from 'file-saver'
import { uniq } from 'lodash'

function concatItems (items) {
  if (items.length === 1) {
    return items[0]
  } else {
    return items.slice(0, -1).join(', ') + ' and ' + items[items.length - 1]
  }
}

function dataSourceCitations (dataSources, providers, lastUpdated) {
  if (!dataSources || dataSources.length === 0) return '# No data sources selected'

  const lastUpdatedYear = DateTime.fromISO(lastUpdated).setZone('US/Alaska').toFormat('yyyy')
  const lastUpdatedDate = DateTime.fromISO(lastUpdated).setZone('US/Alaska').toFormat('DD')
  const citations = []
  for (const ds of dataSources) {
    if (ds === 'USGS') {
      const citation = `#     U.S. Geological Survey (USGS) (${lastUpdatedYear}). National Water Information System data available on the World Wide Web (USGS Water Data for the Nation). Accessed [${lastUpdatedDate}] at URL [http://waterdata.usgs.gov/nwis/].`
      citations.push(citation)
    } else if (ds === 'NPS') {
      const citation = `#     National Park Service (NPS) (${lastUpdatedYear}). National Park Service IRMA Portal (Integrated Resource Management Applications) for Continuous Water Data. Accessed [${lastUpdatedDate}] at URL [https://irma.nps.gov/AQWebPortal/].`
      citations.push(citation)
    } else if (ds === 'AKTEMP') {
      const citation = `#     Alaska Water Temperature Database (AKTEMP-DB) (${lastUpdatedYear}). Water temperature data collected by ${concatItems(providers)}. Accessed [${lastUpdatedDate}] at URL [https://aktemp.uaa.alaska.edu/database].`
      citations.push(citation)
    }
  }
  return citations.join('\n')
}

/**
 * Generates the file header for the CSV download.
 * Includes data source citations, timestamp info, and optional date range.
 * @param {Array} dataSources - List of data source names (USGS, NPS, AKTEMP)
 * @param {Array} providers - List of provider codes
 * @param {string} lastUpdated - ISO timestamp of last data update
 * @param {string} startDate - Optional start date filter (YYYY-MM-DD)
 * @param {string} endDate - Optional end date filter (YYYY-MM-DD)
 * @returns {string} CSV header comment block
 */
function fileHeader (dataSources, providers, lastUpdated, startDate, endDate) {
  // Build the date range line if custom dates are provided
  const dateRangeLine = startDate && endDate
    ? `# Date Range Filter: ${startDate} to ${endDate} (UTC)\n#`
    : ''

  return `# AKTEMP-VIZ | Alaska Stream Temperature Data Visualization Tool
# https://aktemp.uaa.alaska.edu/dataviz
#
# Daily Mean Water and Air Temperature at Select Stations
#
# IMPORTANT: All dates and times are in UTC (Coordinated Universal Time).
#
# Data Last Updated: ${DateTime.fromISO(lastUpdated).setZone('UTC').toFormat('D t')} UTC
# File Downloaded At: ${DateTime.now().setZone('UTC').toFormat('D t')} UTC
${dateRangeLine}
#
# Data Sources (note: Excel will break citations at commas -- open in text editor like Notepad to view correctly)
#
${dataSourceCitations(dataSources, providers, lastUpdated)}`
}

function valuesData (stations) {
  return stations.flatMap(station => {
    return station.data.map(d => {
      return {
        data_source: station.dataset,
        provider_code: station.provider_code,
        station_code: station.station_code,
        station_description: station.station_description || '',
        waterbody_name: station.waterbody_name || '',
        date: d.date,
        water_temp_c: d.temp_c !== null ? d.temp_c : '',
        air_temp_c: d.airtemp_c !== null ? d.airtemp_c : ''
      }
    })
  })
}

function stationsTable (stations) {
  return stations.map(station => {
    return {
      data_source: station.dataset,
      provider_code: station.provider_code,
      provider_name: station.provider_name,
      station_code: station.station_code,
      station_description: station.station_description || '',
      waterbody_name: station.waterbody_name || '',
      latitude: station.latitude,
      longitude: station.longitude,
      start_date: station.start,
      end_date: station.end,
      n_daily_values: station.n,
      station_url: station.url
    }
  })
}

/**
 * Downloads station data as a CSV file with optional date range filtering.
 * @param {Array} stations - Array of station objects with data
 * @param {string} lastUpdated - ISO timestamp of last data update
 * @param {string} startDate - Optional start date filter (YYYY-MM-DD)
 * @param {string} endDate - Optional end date filter (YYYY-MM-DD)
 */
export function downloadCSV(stations, lastUpdated, startDate, endDate) {
  if (stations.length === 0) {
    alert('No stations selected')
    return
  }

  const dataSources = uniq(stations.map(s => s.dataset))
  const providers = uniq(stations.map(s => s.provider_code))

  const header = fileHeader(dataSources, providers, lastUpdated, startDate, endDate)
  const stationsCsv = Papa.unparse(stationsTable(stations))
  const valuesCsv = Papa.unparse(valuesData(stations))

  const csv = [
    header,
    '#',
    '# ---------------------------------------------------------------',
    '# Station Metadata',
    '#',
    stationsCsv,
    '#',
    '# ---------------------------------------------------------------',
    '# Daily Mean Water and Air Temperature (UTC)',
    '#',
    valuesCsv
  ].join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' })

  const timestamp = DateTime.now().toFormat('yyyyMMdd_HHmmss')
  const filename = startDate && endDate
    ? `aktemp_viz_data_${startDate}_to_${endDate}_${timestamp}.csv`
    : `aktemp_viz_data_${timestamp}.csv`

  saveAs(blob, filename)
}
