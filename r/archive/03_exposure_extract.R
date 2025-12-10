source(here::here("r", "00_env.R"))

pm_ds <- read_ds(fs::path(paths$gold, "person_month"))

geo_mode <- yaml::read_yaml(
  here::here("config", "covariates.yaml")
)$geo_mode %||% "tract"

# Determine set of geographic units
if (geo_mode == "tract") {
  geos <- pm_ds %>%
    dplyr::filter(!is.na(tract_geoid)) %>%
    dplyr::distinct(tract_geoid) %>%
    dplyr::collect() %>%
    dplyr::pull()
} else {
  geos <- pm_ds %>%
    dplyr::filter(!is.na(zip5)) %>%
    dplyr::distinct(zip5) %>%
    dplyr::collect() %>%
    dplyr::pull()
}

start  <- as.Date("2015-01-01")
end    <- as.Date("2021-12-31")

features <- yaml::read_yaml(
  here::here("config", "covariates.yaml")
)$features

extract_one <- function(geo_id) {
  amadeus::calculate_covariates(
    geoid      = geo_id,
    geolevel   = geo_mode,
    start_date = start,
    end_date   = end,
    features   = features,
    cache_dir  = paths$am_cache
  )
}

res <- purrr::map_dfr(
  geos,
  function(g) {
    df <- extract_one(g)
    df$geo_id   <- g
    df$geo_type <- geo_mode
    df
  }
)

long <- res %>%
  tidyr::pivot_longer(
    -c(date, geo_id, geo_type),
    names_to  = "exposure_id",
    values_to = "value"
  ) %>%
  dplyr::mutate(
    source  = ifelse(grepl("^hms_", exposure_id), "hms_smoke", "pm25_aqs"),
    version = "2024-10"
  ) %>%
  dplyr::rename(obs_date = date) %>%
  dplyr::mutate(year = lubridate::year(obs_date))

write_parquet_ds(
  long,
  fs::path(paths$gold, "exposure_daily"),
  partitioning = c("geo_type", "exposure_id", "year")
)
