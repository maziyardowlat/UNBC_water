
db_connect <- function () {
  if (Sys.getenv("UNBC_DBNAME") == "") {
    stop("Environment variable UNBC_DBNAME is not set")
  }
  DBI::dbConnect(
    RPostgres::Postgres(),
    dbname = Sys.getenv("UNBC_DBNAME"),
    host = Sys.getenv("UNBC_HOST"),
    port = Sys.getenv("UNBC_PORT"),
    user = Sys.getenv("UNBC_USER"),
    password = Sys.getenv("UNBC_PASSWORD")
  )
}

collect_unbc_raw_data <- function () {
  con <- db_connect()

  db_providers <- tbl(con, "providers") |> 
    select(provider_id = id, provider_code = code, provider_name = name)
  
  db_stations <- tbl(con, "stations") |> 
    left_join(
      db_providers,
      by = c("provider_id")
    ) |> 
    filter(
      !private,
      waterbody_type == "STREAM"
    ) |> 
    semi_join(tbl(con, "series"), by = c("id" = "station_id")) |> 
    select(
      station_id = id, provider_code, provider_name,
      station_code = code, station_description = description,
      longitude, latitude, placement,
      waterbody_name, waterbody_type, mixed
    )
  
  db_series <- tbl(con, "series") |> 
    semi_join(db_stations, by = c("station_id")) |> 
    filter(interval == "CONTINUOUS") |> 
    select(
      series_id = id, station_id,
      depth_m, depth_category,
      start_datetime, end_datetime, n_values,
      interval, frequency, reviewed, accuracy
    )
  
  db_series_flags <- tbl(con, "series_flags") |>
    semi_join(db_series, by = c("series_id")) |>
    select(
      flag_id = id, series_id, start_datetime, end_datetime,
      flag_type_id, flag_type_other
    )
  
  db_series_daily <- tbl(con, "series_daily") |>
    semi_join(db_series, by = c("series_id")) |>
    select(
      series_id, date, n_values, min_temp_c, mean_temp_c, max_temp_c
    )
  
  # collect
  stations <- collect(db_stations)
  series <- collect(db_series)
  series_flags <- collect(db_series_flags) |> 
    rowwise() |> 
    mutate(
      flag_data = list({
        start_date <- as_date(with_tz(start_datetime, "America/Anchorage"))
        end_date <- as_date(with_tz(end_datetime, "America/Anchorage"))
        tibble(
          date = seq.Date(start_date, end_date, by = "day"),
          flag = flag_type_id
        )
      })
    ) |> 
    select(series_id, flag_id, flag_data) |> 
    nest_by(series_id, .key = "flags") |> 
    mutate(
      flags = list({
        bind_rows(flags) |> 
          select(flag_data) |> 
          unnest(flag_data) |> 
          group_by(date) |> 
          summarise(
            flag = str_c(sort(unique(flag)), collapse = ","),
            .groups = "drop"
          )
      })
    )
  series_daily <- collect(db_series_daily)
  
  DBI::dbDisconnect(con)
  
  list(
    stations = stations,
    series = series,
    flags = series_flags,
    daily = series_daily
  )
}

transform_unbc_data <- function (db_data) {
  series <- db_data[["series"]] |>
    left_join(
      db_data[["daily"]] |>
        nest_by(series_id),
      by = c("series_id")
    ) |>
    left_join(
      db_data[["flags"]],
      by = c("series_id")
    ) |>
    rowwise() |>
    mutate(
      data = list({
        if (is.null(flags)) {
          data[["flag"]] <- NA_character_
        } else {
          data <- data |>
            left_join(flags, by = "date")
        }
        data
      }),
      n_daily_values = nrow(data)
    ) |>
    select(-flags)
  
  series_n <- series |> 
    nest_by(station_id, .key = "series") |> 
    mutate(
      n_dates = {
        series |> 
          select(data) |> 
          unnest(data) |> 
          pull(date) |> 
          n_distinct()
      },
      n_dates_overlapping = {
        series |> 
          select(data) |> 
          unnest(data) |> 
          count(date) |> 
          filter(n > 1) |>
          nrow()
      },
      n_series = nrow(series)
    ) |> 
    ungroup()
  
  series <- series_n |> 
    filter(n_dates_overlapping <= 10)
  
  daily_series <- series |> 
    select(station_id, series) |> 
    unnest(series) |> 
    select(station_id, data) |> 
    unnest(data) |> 
    filter(is.na(flag)) |> 
    group_by(station_id, date) |> 
    summarise(
      min_temp_c = min(min_temp_c, na.rm = TRUE),
      mean_temp_c = mean(mean_temp_c, na.rm = TRUE),
      max_temp_c = max(max_temp_c, na.rm = TRUE),
      .groups = "drop"
    ) |> 
    nest_by(station_id) |> 
    ungroup()
  
  db_data$stations |> 
    mutate(
      url = glue("https://watertemp.unbc.ca/#/explorer/stations/{station_id}"),
    ) |> 
    inner_join(
      daily_series,
      by = c("station_id")
    ) |> 
    transmute(
      provider_station_code = glue("{provider_code}:{station_code}"),
      station_id = as.character(station_id),
      station_code,
      station_description,
      waterbody_name,
      latitude, longitude,
      provider_code,
      provider_name,
      url,
      data
    )
}
