<template>
  <div class="weather-map" aria-label="Weather station locations">
    <LMap ref="map" :zoom="6" :center="[54.1, -124]" @ready="showAll">
      <LControlLayers position="topright" />
      <LTileLayer v-for="tile in tiles" :key="tile.name" :name="tile.name" :visible="tile.visible"
        :url="tile.url" :attribution="tile.attribution" layer-type="base" />
      <LCircleMarker v-for="station in stations" :key="station.id" :lat-lng="[station.latitude, station.longitude]"
        :radius="station.id === selected ? 10 : 7" :weight="2"
        :color="station.id === selected ? '#004895' : station.status === 'available' ? '#176d59' : '#69747a'"
        :fill-opacity="station.id === selected ? 0.9 : 0.65" @click="$emit('select', station.id)">
        <LTooltip :options="{ direction: 'top' }">
          <strong>{{ station.name }}</strong><br>{{ station.id }}<br>
          {{ station.status === 'available' ? `${station.start} to ${station.end}` : 'No tidy data supplied' }}
        </LTooltip>
      </LCircleMarker>
      <LControl position="bottomleft">
        <button class="map-reset" @click="showAll">Show all stations</button>
      </LControl>
    </LMap>
  </div>
</template>

<script setup>
import { ref, watch } from 'vue'
import { LMap, LTileLayer, LCircleMarker, LControlLayers, LControl, LTooltip } from '@vue-leaflet/vue-leaflet'
import { basemaps } from '@/lib/basemaps'

const props = defineProps({ stations: { type: Array, required: true }, selected: String })
defineEmits(['select'])
const map = ref(null)
const tiles = basemaps.filter(tile => tile.url)
function showAll () {
  if (props.stations.length) map.value?.leafletObject?.fitBounds(props.stations.map(s => [s.latitude, s.longitude]), { padding: [35, 35], maxZoom: 8 })
}
watch(() => props.selected, id => {
  const station = props.stations.find(s => s.id === id)
  if (station) map.value?.leafletObject?.panTo([station.latitude, station.longitude])
})
</script>

<style scoped>
.weather-map { height: 380px; width: 100%; position: relative; z-index: 0; }
.map-reset { background: white; color: #193c3b; padding: 8px 12px; border-radius: 4px; box-shadow: 0 1px 5px #0005; font: 14px Arial, sans-serif; }
.map-reset:focus-visible { outline: 3px solid #004895; }
</style>
