library(targets)

tar_source(list.files("R", pattern = "\\.R$", full.names = TRUE))

tar_option_set(
  packages = c(
    "tidyverse", "glue", "janitor", "sf", "httr2", "jsonlite", "logger"
  )
)

mkdirp <- function (x) {
  dir.create(x, recursive = TRUE, showWarnings = FALSE)
  x
}

logger::log_threshold(Sys.getenv("LOG_LEVEL", unset = "INFO"))

# Define pipeline
list(
  tar_target(data_dir, Sys.getenv("UNBCWATERTEMP_DATA_DIR", unset = "data"), cue = tar_cue("always")),
  tar_target(gis_dir, mkdirp(file.path(data_dir, "gis"))),
  tar_target(daymet_dir, mkdirp(file.path(data_dir, "daymet"))),
  tar_target(output_dir, mkdirp(file.path(data_dir, "output"))),
  
  tar_target(
    nechako_watershed_file, 
    file.path(gis_dir, "nechako_watershed.geojson"),
    format = "file"
  ),
  tar_target(nechako_watershed, load_nechako_watershed(nechako_watershed_file)),
  
  tar_target(unbc_raw_data, collect_unbc_raw_data()),
  tar_target(unbc_data, transform_unbc_data(unbc_raw_data)),
  
  tar_target(combined_data, bind_rows(
    UNBC = unbc_data,
    .id = "dataset"
  )),
  tar_target(
    combined_station_tile_years, 
    extract_station_tile_years(combined_data)
  ),
  tar_target(
    daymet_last_year,
    find_daymet_last_year()
  ),
  tar_target(
    daymet_tile_years, 
    extract_daymet_tile_years(combined_station_tile_years, daymet_last_year)
  ),
  tar_target(
    daymet_tile_files, 
    collect_daymet_tile_files(daymet_tile_years, daymet_dir)
  ),
  tar_target(
    airtemp,
    extract_airtemp(combined_station_tile_years, daymet_tile_files)
  ),
  
  tar_target(combined_data_airtemp, merge_data_airtemp(combined_data, airtemp)),
  tar_target(station_watershed, extract_station_watershed(combined_data, nechako_watershed)),
  tar_target(output_data, transform_output_data(combined_data_airtemp, station_watershed)),
  tar_target(
    output_stations_file, 
    export_stations_file(output_data, output_dir),
    format = "file"
  ),
  tar_target(
    output_data_files,
    export_data_files(output_data, output_dir),
    format = "file"
  ),
  tar_target(config, list(
    daymet_last_year = daymet_last_year,
    last_updated = format_ISO8601(with_tz(Sys.time(), tzone = "America/Vancouver"), usetz = TRUE)
  )),
  tar_target(
    output_config_file,
    export_config_file(config, output_dir),
    format = "file"
  ),
  tar_target(
    output_watershed_files,
    export_watershed_files(nechako_watershed, output_dir),
    format = "file"
  )
)
