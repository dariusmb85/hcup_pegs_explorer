source(here::here("r", "00_env.R"))

pm <- read_ds(fs::path(paths$gold, "person_month")) %>%
  dplyr::collect()

ex <- read_ds(fs::path(paths$gold, "exposure_daily")) %>%
  dplyr::filter(geo_type == "tract") %>%
  dplyr::collect()

roll <- pm %>%
  dplyr::left_join(
    ex,
    by = c("tract_geoid" = "geo_id")
  ) %>%
  dplyr::filter(
    obs_date >= ym,
    obs_date < ym + lubridate::months(1)
  ) %>%
  dplyr::group_by(person_id, ym, exposure_id) %>%
  dplyr::summarise(
    mean = mean(value, na.rm = TRUE),
    sum  = sum(value, na.rm = TRUE),
    p90  = quantile(value, 0.9, na.rm = TRUE),
    days = sum(!is.na(value)),
    .groups = "drop"
  ) %>%
  tidyr::pivot_longer(
    c(mean, sum, p90, days),
    names_to  = "metric",
    values_to = "value"
  ) %>%
  dplyr::mutate(
    year = lubridate::year(ym)
  )

write_parquet_ds(
  roll,
  fs::path(paths$gold, "exposure_rollup"),
  partitioning = c("exposure_id", "metric", "year")
)
