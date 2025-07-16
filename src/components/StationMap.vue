<template>
  <LMap ref="map" :zoom="6" :center="[54.2,-124.5]">
    <LControlLayers position="topleft"></LControlLayers>
    <LControl position="bottomleft" v-show="props.selected.length === 0">
      <div class="legend">
        <div class="mb-2">
          <strong>Data Source</strong>
        </div>
        <div style="" class="legend-item">
          <svg width="20" height="20">
            <circle cx="10" cy="10" r="9" :stroke="dataSourceColors['UNBC']" stroke-opacity="0.8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" :fill="dataSourceColors['UNBC']" fill-opacity="0.5" ></circle>
          </svg>
          <span class="pl-2">UNBC</span>
        </div>
      </div>
    </LControl>

    <LControl position="topright">
      <div class="legend">
        <div class="d-flex align-center">
          <v-select
            v-model="selectedBasinLayer"
            :items="basinLayerOptions"
            item-title="label"
            item-value="value"
            label="Select a Basin Layer"
            density="compact"
            variant="outlined"
            clearable
            hide-details
            class="mr-2"
            :width="300"
          ></v-select>
          <v-tooltip location="right">
            <template v-slot:activator="{ props }">
              <v-icon
                v-bind="props"
                icon="mdi-information"
                size="small"
                color="grey"
                class="ml-1"
              ></v-icon>
            </template>
            Select a basins layer to filter stations by watershed.
          </v-tooltip>
        </div>
        <div class="d-flex align-center mt-4" v-if="selectedBasinLayer">
          <v-autocomplete
            :model-value="props.selectedBasin"
            @update:model-value="emit('select-basin', $event)"
            @click:clear="emit('select-basin', null)"
            :items="basinOptions"
            item-title="label"
            item-value="value"
            label="Select a Basin"
            density="compact"
            variant="outlined"
            clearable
            hide-details
            class="mr-2"
            :width="300"
          ></v-autocomplete>
          <v-tooltip location="right">
            <template v-slot:activator="{ props }">
              <v-icon
                v-bind="props"
                icon="mdi-information"
                size="small"
                color="grey"
                class="ml-1"
              ></v-icon>
            </template>
            Select a basin from this menu or on the map to see only the stations within that basin.
          </v-tooltip>
        </div>
      </div>
    </LControl>
    <LTileLayer
      v-for="tile in basemaps"
      :key="tile.name"
      :name="tile.name"
      :visible="tile.visible"
      :url="tile.url"
      :attribution="tile.attribution"
      :options="tile.options"
      :opacity="0.5"
      layer-type="base"
    ></LTileLayer>

    <LCircleMarker
      v-for="station in props.stations"
      :key="station.station_id"
      :lat-lng="[station.latitude, station.longitude]"
      :color="props.selected.length > 0 ? 'gray': dataSourceColors[station.dataset]"
      :fill-color="props.selected.length > 0 ? 'gray': dataSourceColors[station.dataset]"
      :radius="8"
      :weight="2"
      :fill-opacity="0.25"
      pane="markerPane"
      :opacity="props.selected.length > 0 ? 0.5 : 1"
      @click="emit('select', station)"
    >
      <LTooltip>
        <div>
          <div class="text-body-1 font-weight-bold" style="max-width:800px; text-wrap:wrap;">{{ station.provider_station_code || 'Unknown' }}</div>
          <div>
            {{ station.waterbody_name || 'Unknown waterbody' }}<br>
            {{ station.provider_name || 'Unknown provider' }}<br>
            {{ station.start || 'Unknown' }} to {{ station.end || 'Unknown' }} ({{ (station.n || 0).toLocaleString() }} days)
          </div>
        </div>
      </LTooltip>
    </LCircleMarker>
    <LCircleMarker
      v-for="station in props.selected"
      :key="station.station_id"
      :lat-lng="[station.latitude, station.longitude]"
      :radius="10"
      :weight="5"
      :color="station.color"
      pane="markerPane"
      @click="emit('select', station)"
    >
      <LTooltip>
        <div>
          <div class="text-body-1 font-weight-bold" style="max-width:800px; text-wrap:wrap;">{{ station.provider_station_code || 'Unknown' }}</div>
          <div>
            {{ station.waterbody_name }}<br>
            {{ station.provider_name }}<br>
            {{ station.start }} to {{ station.end }} ({{ station.n.toLocaleString() }} days)
          </div>
          <v-divider class="my-2"></v-divider>
          <div class="text-caption font-weight-bold mt-2 text-left d-flex align-center">
            <v-icon start size="small" :color="station.color">mdi-check-circle</v-icon>
            Selected Station
          </div>
        </div>
      </LTooltip>
    </LCircleMarker>

    <LGeoJson
      v-if="basinGeoJson && basinGeoJson.visible && basinGeoJson.data"
      :geojson="basinGeoJson.data"
      :options="basinGeoJson.options"
      :options-style="basinGeoJson.style"
      pane="overlayPane"
      @click="onClickBasinLayer"
    />
    <LGeoJson
      v-if="basinGeoJson.visible && selectedBasinGeoJson"
      :geojson="selectedBasinGeoJson.feature"
      :options="selectedBasinOptions"
      :options-style="selectedBasinStyle"
      pane="overlayPane"
      @click="onClickBasinLayer"
    />
  </LMap>
</template>

<script setup>
import { ref, computed, watch } from 'vue'
import { LMap, LTileLayer, LCircleMarker, LGeoJson, LControlLayers, LControl, LTooltip } from '@vue-leaflet/vue-leaflet'

import { basemaps } from '@/lib/basemaps'

const props = defineProps({
  stations: {
    type: Array,
    default: () => []
  },
  selected: {
    type: Array,
    default: () => []
  },
  selectedBasin: {
    type: String,
    default: () => null
  }
})
const emit = defineEmits(['select', 'select-basin'])

const selectedBasinLayer = ref(null)

const dataSourceColors = {
  'UNBC': '#1b9e77'
}

const basinLayerOptions = ref([
  {
    value: 'nechako',
    label: 'Nechako Watershed'
  }
])

const basinGeoJson = ref({
  data: null,
  options: null,
  style: null,
  loading: false
})

const basinOptions = computed(() => {
  if (!basinGeoJson.value.data) return []
  return basinGeoJson.value.data.features.map(feature => ({
    value: feature.id,
    label: `${feature.id} - ${feature.properties.name}`
  })).sort((a, b) => a.value.localeCompare(b.value))
})

const selectedBasinOptions = {
  onEachFeature (feature, layer) {
    layer.bindTooltip(
      basinTooltipContent(feature),
      { permanent: false, sticky: true }
    )
  }
}

const selectedBasinStyle = () => {
  return {
    weight: 2,
    color: 'orangered',
    opacity: 1,
    fillOpacity: 0
  }
}

function basinTooltipContent (feature) {
  if (!feature?.properties?.name || !feature.id) return ''
  return `<span class="text-body-1 font-weight-bold">Basin: ${feature.properties.name}</span><br>HUC${feature.id.length}: ${feature.id}`
}

watch(() => selectedBasinLayer.value, loadBasinLayer)
async function loadBasinLayer (basinLayer) {
  if (!basinLayer) {
    basinGeoJson.value.visible = false
    emit('select-basin', null)
    return
  }
  basinGeoJson.value.loading = true
  emit('select-basin', null)
  try {
    const response = await fetch(`data/gis/nechako_watershed.geojson`)
    if (!response.ok) throw new Error(`Failed to load basin layer: ${response.statusText}`)
    const data = await response.json()
    basinGeoJson.value.data = data
    basinGeoJson.value.options = {
      onEachFeature: (feature, layer) => {
        layer.bindTooltip(
          basinTooltipContent(feature),
          { permanent: false, sticky: true }
        )
        layer.on({
          mousemove: (evt) => {
            evt.target.setStyle({
              weight: 3,
              color: 'black'
            })
          },
          mouseout: (evt) => {
            evt.target.setStyle({
              weight: 2,
              color: '#777',
            })
          }
        })
      }
    }
    basinGeoJson.value.style = () => {
      return {
        weight: 2,
        color: '#777',
        opacity: 1,
        fillOpacity: 0
      }
    }
    basinGeoJson.value.visible = true
  } catch (error) {
    console.error('Error loading basin layer:', error)
    basinGeoJson.value.visible = false
    basinGeoJson.value.data = null
  } finally {
    basinGeoJson.value.loading = false
  }
}

const selectedBasinGeoJson = computed(() => {
  if (!basinGeoJson.value.data) return null
  const feature = basinGeoJson.value.data.features.find(feature => feature.id === props.selectedBasin)
  return feature ? { feature } : null
})

function onClickBasinLayer (evt) {
  if (!evt?.layer?.feature) return
  const feature = evt.layer.feature
  evt.layer.bringToFront()
  emit('select-basin', feature.id)
}

</script>

<style>
/* remove the focus ring */
path.leaflet-interactive:focus {
    outline: none;
}

.leaflet-container {
  background-color: white;
}

.legend {
  background: rgba(255, 255, 255, 0.8);
  padding: 10px;
  border-radius: 5px;
  box-shadow: 0 0 15px rgba(0, 0, 0, 0.2);
}

.legend-item {
  display: flex;
  align-items: center;
  margin-bottom: 5px;
}

.legend-item-icon {
  display: inline-block;
  width: 18px;
  height: 18px;
  border-radius: 18px;
  margin-right: 8px;
}
</style>