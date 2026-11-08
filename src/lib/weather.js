import Papa from 'papaparse'
import { DateTime } from 'luxon'

export const weatherCatalog = '/data/weather/stations.json'

// Names match FLAG_DISPLAY_NAMES in the weatherqa_qc source application.
export const flagNames = {
  P: 'Pass', C: 'Caution', DC: 'Dependency caution', DF: 'Dependency failure',
  DZ: 'Divide by zero', E: 'Error', ERR: 'Error', M: 'Missing', V: 'Visit',
  Z: 'Night time value', T: 'Tilt', SF: 'Snow free', R: 'Range',
  NV: 'No value', BV: 'Battery voltage', PT: 'Panel temperature', LR: 'Logger restart'
}

const labels = {
  WS_Std: 'Wind speed standard deviation', WS_ms_Avg_2: 'Wind speed (sensor 2)',
  WindDir_Avg: 'Average wind direction', WindDir_2: 'Wind direction (sensor 2)',
  WindDir_SD1: 'Wind direction standard deviation', WindDir_SD1_WVT: 'Wind direction standard deviation (WVT)',
  WindDir_SD1_WVT_2: 'Wind direction standard deviation (sensor 2)',
  Original_WindDir: 'Original wind direction', Original_WindDir_Avg: 'Original average wind direction',
  Dist_km_Max: 'Maximum lightning distance', SlrFD_W_Std: 'Solar radiation standard deviation',
  T109_AirT_C_Avg: 'Air temperature (T109)', GTMP_13cm: 'Ground temperature (13 cm)',
  'Gtmp_24cm depth': 'Ground temperature (24 cm)',
  SR50AT_DBTCDT_Avg: 'Snow depth (SR50AT)', SR50AT_TCDT_Avg: 'Compensated distance (SR50AT)',
  SR50AT_Q_Avg: 'Signal quality (SR50AT)', SR50AT_SRTmp_C_Avg: 'Sensor temperature (SR50AT)',
  SV_DBTCDT_Avg: 'Snow depth (SnowVue)', SV_DT_Avg: 'Distance to target (SnowVue)',
  SV_TCDT_Avg: 'Compensated distance (SnowVue)', SV_Q_Avg: 'Signal quality (SnowVue)',
  SV_Tmp_Avg: 'Sensor temperature (SnowVue)', SV_RH: 'Sensor humidity (SnowVue)',
  SV_Pitch: 'Sensor pitch (SnowVue)', SV_Roll: 'Sensor roll (SnowVue)',
  SV_Alert: 'Sensor alert (SnowVue)', SV_Freq: 'Sensor frequency (SnowVue)', SV_Volt: 'Sensor voltage (SnowVue)',
  AirT_C_Avg: 'Air temperature', RH: 'Relative humidity', WS_ms_Avg: 'Wind speed',
  WindDir: 'Wind direction', MaxWS_ms_Avg: 'Maximum wind speed', Rain_mm_Tot: 'Rainfall',
  SlrFD_W_Avg: 'Solar radiation', SlrTF_MJ_Tot: 'Solar energy', BP_hPa_Avg: 'Air pressure',
  VP_hPa_Avg: 'Vapour pressure', RHT_C_Avg: 'RH sensor temperature',
  DBTCDT_Avg: 'Snow depth', DT_Avg: 'Distance to target', TCDT_Avg: 'Temperature-compensated distance',
  SWin_Avg: 'Incoming shortwave radiation', SWout_Avg: 'Outgoing shortwave radiation',
  LWin_Avg: 'Incoming longwave radiation', LWout_Avg: 'Outgoing longwave radiation',
  SWnet_Avg: 'Net shortwave radiation', LWnet_Avg: 'Net longwave radiation',
  SWalbedo_Avg: 'Shortwave albedo', NR_Avg: 'Net radiation', Stmp_Avg: 'Soil temperature',
  Gtmp_Avg: 'Ground temperature', BattV_Avg: 'Battery voltage', PTemp_C_Avg: 'Panel temperature',
  Strikes_Tot: 'Lightning strikes', Dist_km_Avg: 'Lightning distance',
  TiltNS_deg_Avg: 'Sensor tilt north–south', TiltWE_deg_Avg: 'Sensor tilt west–east'
}

export function weatherVariables (fields, units) {
  return fields.filter(key => key !== 'RECORD' && fields.includes(`${key}_Flag`)).map(key => ({
    key,
    title: labels[key] || (/^Gtmp_(\d+)cm_Avg$/.test(key) ? `Ground temperature (${key.match(/\d+/)[0]} cm)` : key),
    unit: units[key] && units[key].toLowerCase() !== 'nan' ? units[key] : 'Unit not supplied'
  })).sort((a, b) => a.title.localeCompare(b.title))
}

export async function loadWeatherRange (station, start, end, signal) {
  const months = station.months.filter(chunk => chunk.month >= start.slice(0, 7) && chunk.month <= end.slice(0, 7))
  const rows = []
  // Limit decompression/parsing to one month at a time, especially for historical files.
  for (const chunk of months) {
    const response = await fetch(chunk.url, { signal })
    if (!response.ok) throw new Error(`Weather data request failed (${response.status}).`)
    const bytes = new Uint8Array(await response.arrayBuffer())
    let csv
    if (bytes[0] === 0x1f && bytes[1] === 0x8b) {
      if (typeof DecompressionStream === 'undefined') throw new Error('This browser cannot decompress the weather files. Please use a current browser.')
      csv = await new Response(new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'))).text()
    } else csv = new TextDecoder().decode(bytes)
    if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
    const parsed = parseWeatherCSV(csv)
    rows.push(...parsed.rows.filter(row => {
      const date = row.TIMESTAMP.slice(0, 10)
      return date >= start && date <= end
    }))
  }
  return { rows: rows.sort((a, b) => a.millis - b.millis), units: station.units, fields: station.fields, variables: weatherVariables(station.fields, station.units) }
}

export function flagsFor (row, variable) {
  return (row[`${variable}_Flag`] || '').split(',').map(s => s.trim()).filter(s => s && s.toLowerCase() !== 'nan')
}

export function hasOtherCodes (row, variable) {
  return flagsFor(row, variable).some(code => code !== 'P')
}

export function numericValue (row, variable) {
  const value = row[variable]
  return value === '' || value == null || !Number.isFinite(Number(value)) ? null : Number(value)
}

export function parseWeatherCSV (csv) {
  // This source is comma-delimited. Avoid expensive automatic dialect detection
  // on the 25 MB file, and normalize mixed CRLF/LF endings for consistent fields.
  const parsed = Papa.parse(csv.replace(/\r\n?/g, '\n'), { header: true, delimiter: ',', newline: '\n', skipEmptyLines: true })
  if (parsed.errors.length) throw new Error(`Could not read weather CSV: ${parsed.errors[0].message}`)
  const [units, ...sourceRows] = parsed.data
  if (!units || !parsed.meta.fields.includes('TIMESTAMP') || !parsed.meta.fields.includes('UTC Offset')) {
    throw new Error('Weather data must include TIMESTAMP, UTC Offset and a units row.')
  }
  const rows = sourceRows.map(row => {
    const time = DateTime.fromISO(`${row.TIMESTAMP.replace(' ', 'T')}${row['UTC Offset']}`, { setZone: true })
    if (!time.isValid) throw new Error(`Invalid weather timestamp: ${row.TIMESTAMP}`)
    return { ...row, millis: time.toMillis() }
  }).sort((a, b) => a.millis - b.millis)
  if (!rows.length) throw new Error('No weather observations were found.')
  const variables = weatherVariables(parsed.meta.fields, units)
  return { rows, units, variables, fields: parsed.meta.fields }
}
