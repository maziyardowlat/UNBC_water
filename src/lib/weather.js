import Papa from 'papaparse'
import { DateTime } from 'luxon'

export const weatherSource = '/data/weather/02FW005_tidy_CR350_1379_20250521.csv'

// Names match FLAG_DISPLAY_NAMES in the weatherqa_qc source application.
export const flagNames = {
  P: 'Pass', C: 'Caution', DC: 'Dependency caution', DF: 'Dependency failure',
  DZ: 'Divide by zero', E: 'Error', ERR: 'Error', M: 'Missing', V: 'Visit',
  Z: 'Night time value', T: 'Tilt', SF: 'Snow free', R: 'Range',
  NV: 'No value', BV: 'Battery voltage', PT: 'Panel temperature', LR: 'Logger restart'
}

const labels = {
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
  const variables = parsed.meta.fields.filter(field => field !== 'RECORD' && parsed.meta.fields.includes(`${field}_Flag`)).map(key => ({
    key, title: labels[key] || key,
    // Unknown units stay unknown; do not infer snow-depth or albedo units.
    unit: units[key] && units[key].toLowerCase() !== 'nan' ? units[key] : 'Unit not supplied'
  }))
  return { rows, units, variables, fields: parsed.meta.fields }
}
