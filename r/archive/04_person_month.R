source(here::here("r", "00_env.R"))

vis <- read_ds(fs::path(paths$silver, "visit"))

pm <- vis %>%
  dplyr::transmute(
    person_id,
    ym          = lubridate::floor_date(admit_date, "month"),
    zip5,
    tract_geoid = NA_character_,
    n_visits    = 1L,
    asthma_flag = dplyr::if_else(
      startsWith(dx_primary %||% NA_character_, "J45"),
      TRUE,
      FALSE,
      missing = FALSE
    )
  ) %>%
  dplyr::group_by(person_id, ym) %>%
  dplyr::summarise(
    zip5        = dplyr::last(na.omit(zip5)),
    tract_geoid = dplyr::last(na.omit(tract_geoid)),
    n_visits    = sum(n_visits),
    asthma_flag = any(asthma_flag),
    .groups     = "drop"
  ) %>%
  dplyr::mutate(
    year = lubridate::year(ym)
  )

write_parquet_ds(
  pm,
  fs::path(paths$gold, "person_month"),
  partitioning = "year"
)
