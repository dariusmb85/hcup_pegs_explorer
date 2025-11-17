source(here::here("r", "00_env.R"))

pm <- read_ds(fs::path(paths$gold, "person_month")) %>%
  dplyr::collect()

ex <- read_ds(fs::path(paths$gold, "exposure_rollup")) %>%
  dplyr::filter(metric == "mean") %>%
  dplyr::collect()

wide <- ex %>%
  tidyr::pivot_wider(
    names_from  = exposure_id,
    values_from = value
  ) %>%
  dplyr::right_join(
    pm,
    by = c("person_id", "ym")
  )

exposure_cols <- setdiff(
  names(wide),
  c(
    "person_id",
    "ym",
    "zip5",
    "tract_geoid",
    "n_visits",
    "asthma_flag",
    "outcome_flags",
    "year"
  )
)

fit_one <- function(var) {
  df <- wide %>%
    dplyr::select(asthma_flag, !!rlang::sym(var)) %>%
    dplyr::filter(
      !is.na(asthma_flag),
      !is.na(.data[[var]])
    )

  if (nrow(df) < 500) {
    return(NULL)
  }

  m <- glm(
    asthma_flag ~ scale(.data[[var]]),
    family = binomial(),
    data   = df
  )

  s <- summary(m)$coef[2, ]

  tibble::tibble(
    exposure_id = var,
    estimate    = unname(s["Estimate"]),
    se          = unname(s["Std. Error"]),
    p_value     = unname(s["Pr(>|z|)"]),
    n           = nrow(df)
  )
}

res <- purrr::map_dfr(exposure_cols, fit_one) %>%
  dplyr::mutate(
    q_value       = p.adjust(p_value, method = "BH"),
    ci_low        = estimate - 1.96 * se,
    ci_high       = estimate + 1.96 * se,
    model_spec_id = paste0(
      "logit_asthma_mean_",
      format(Sys.time(), "%Y%m%d%H%M%S")
    )
  )

write_parquet_ds(
  res,
  fs::path(paths$gold, "exwas_result"),
  partitioning = "model_spec_id"
)
