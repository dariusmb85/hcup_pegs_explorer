# r/60_exwas.R (Enhanced)
# Environment-wide association study across multiple phenotypes

source(here::here("r", "00_env.R"))

# Load configurations
pheno_cfg <- yaml::read_yaml(here::here("config", "covariates.yaml"))$phenotypes
model_cfg <- yaml::read_yaml(here::here("config", "covariates.yaml"))$exwas_models

# Fit single exposure-outcome pair
fit_exposure_outcome <- function(wide_df, exposure_var, outcome_var, model_spec) {

  # Prepare data
  df <- wide_df %>%
    select(all_of(c(outcome_var, exposure_var, "year", "month", "zip5"))) %>%
    filter(!is.na(.data[[outcome_var]]), !is.na(.data[[exposure_var]]))

  # Minimum sample size check
  if (nrow(df) < 500) {
    return(NULL)
  }

  # Check outcome variance (must have both TRUE and FALSE)
  if (length(unique(df[[outcome_var]])) < 2) {
    return(NULL)
  }

  # Build formula
  formula_str <- model_spec$formula_template
  formula_str <- str_replace(formula_str, "\\{outcome\\}", outcome_var)
  formula_str <- str_replace(formula_str, "\\{exposure\\}", glue("scale(.data[['{exposure_var}']])"))

  # Fit model
  tryCatch({
    if (model_spec$family == "binomial") {
      m <- glm(
        as.formula(formula_str),
        family = binomial(),
        data = df
      )
    } else {
      stop("Only binomial family currently supported")
    }

    # Extract coefficients for exposure term
    coef_name <- names(coef(m))[2]  # First covariate after intercept
    s <- summary(m)$coef[coef_name, ]

    # Calculate OR and CI
    or <- exp(s["Estimate"])
    or_ci_low <- exp(s["Estimate"] - 1.96 * s["Std. Error"])
    or_ci_high <- exp(s["Estimate"] + 1.96 * s["Std. Error"])

    tibble::tibble(
      outcome = outcome_var,
      exposure_id = exposure_var,
      model_spec_id = model_spec$id,
      n = nrow(df),
      n_cases = sum(df[[outcome_var]], na.rm = TRUE),
      estimate = s["Estimate"],
      se = s["Std. Error"],
      or = or,
      or_ci_low = or_ci_low,
      or_ci_high = or_ci_high,
      z_value = s["z value"],
      p_value = s["Pr(>|z|)"]
    )

  }, error = function(e) {
    warning(glue("Model failed for {outcome_var} ~ {exposure_var}: {e$message}"))
    return(NULL)
  })
}

# Run ExWAS for one model specification
run_exwas_model <- function(wide_df, exposure_cols, outcome_cols, model_spec) {

  message(glue("\nRunning ExWAS: {model_spec$id}"))
  message(glue("  {length(exposure_cols)} exposures × {length(outcome_cols)} outcomes = {length(exposure_cols) * length(outcome_cols)} tests"))

  # Create all exposure-outcome combinations
  combinations <- expand.grid(
    exposure = exposure_cols,
    outcome = outcome_cols,
    stringsAsFactors = FALSE
  )

  # Fit all models with progress
  pb <- txtProgressBar(max = nrow(combinations), style = 3)

  results <- purrr::map_dfr(1:nrow(combinations), function(i) {
    setTxtProgressBar(pb, i)

    res <- fit_exposure_outcome(
      wide_df,
      combinations$exposure[i],
      combinations$outcome[i],
      model_spec
    )

    return(res)
  })

  close(pb)

  # Remove failed models
  results <- results %>% filter(!is.na(estimate))

  message(glue("  ✓ {nrow(results)} successful models"))

  return(results)
}

main <- function() {
  message("\n=== Running Environment-Wide Association Study ===\n")

  # Load person-month cohort
  message("Loading person-month cohort...")
  pm <- read_ds(path(paths$gold, "person_month")) %>%
    collect()

  message(glue("  {scales::comma(nrow(pm))} person-months"))

  # Load exposure rollups
  message("Loading exposure rollups...")
  ex <- read_ds(path(paths$gold, "exposure_rollup")) %>%
    filter(metric == "mean") %>%  # Use mean for now
    collect()

  message(glue("  {scales::comma(nrow(ex))} exposure measurements"))

  # Pivot exposures wide
  message("Pivoting exposures to wide format...")
  wide <- ex %>%
    select(person_id, ym, exposure_id, value) %>%
    pivot_wider(names_from = exposure_id, values_from = value) %>%
    right_join(pm, by = c("person_id", "ym"))

  # Identify columns
  exposure_cols <- names(wide)[str_detect(names(wide), "^(hms_|pm25_|ozone_|temp_)")]
  outcome_cols <- names(wide)[str_detect(names(wide), "_flag$")]

  message(glue("\nExposures: {length(exposure_cols)}"))
  message(glue("Outcomes: {length(outcome_cols)}"))

  if (length(exposure_cols) == 0 || length(outcome_cols) == 0) {
    stop("No exposures or outcomes found in data")
  }

  # Run ExWAS for each model specification
  all_results <- purrr::map_dfr(model_cfg, function(model_spec) {
    run_exwas_model(wide, exposure_cols, outcome_cols, model_spec)
  })

  # Multiple testing correction (within each model spec)
  message("\nApplying multiple testing correction...")
  all_results <- all_results %>%
    group_by(model_spec_id, outcome) %>%
    mutate(
      q_value = p.adjust(p_value, method = "BH"),
      bonferroni_sig = p_value < (0.05 / n())
    ) %>%
    ungroup() %>%
    mutate(
      run_timestamp = Sys.time(),
      run_id = paste0(format(Sys.time(), "%Y%m%d_%H%M%S"))
    )

  # Summary statistics
  message("\n=== Results Summary ===")

  summary_stats <- all_results %>%
    group_by(model_spec_id) %>%
    summarise(
      n_tests = n(),
      n_sig_p05 = sum(p_value < 0.05, na.rm = TRUE),
      n_sig_q05 = sum(q_value < 0.05, na.rm = TRUE),
      n_sig_bonf = sum(bonferroni_sig, na.rm = TRUE),
      .groups = "drop"
    )

  print(summary_stats)

  # Show top hits
  message("\n=== Top 10 Associations (by p-value) ===")
  top_hits <- all_results %>%
    arrange(p_value) %>%
    head(10) %>%
    select(outcome, exposure_id, model_spec_id, or, or_ci_low, or_ci_high, p_value, q_value)

  print(top_hits, n = 10)

  # Write results
  message("\nWriting results...")
  write_parquet_ds(
    all_results,
    path(paths$gold, "exwas_result"),
    partitioning = c("model_spec_id", "outcome")
  )

  message("✓ ExWAS complete\n")

  invisible(all_results)
}

# Run if called directly
if (!interactive()) {
  main()
}