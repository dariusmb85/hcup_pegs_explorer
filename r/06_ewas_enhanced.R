# r/06_exwas_enhanced.R
# Environment-Wide Association Study using config-driven approach

source(here::here("r", "00_env.R"))

library(arrow)
library(dplyr)
library(tidyr)
library(broom)
library(yaml)

# Load configurations
cat("=== Running Enhanced ExWAS ===\n\n")

pheno_cfg <- read_yaml(here::here("config", "covariates.yaml"))$phenotypes
model_cfg <- read_yaml(here::here("config", "covariates.yaml"))$exwas_models
feature_cfg <- read_yaml(here::here("config", "covariates.yaml"))$features

# Create feature lookup for labels
feature_labels <- setNames(
  sapply(feature_cfg, function(f) f$description),
  sapply(feature_cfg, function(f) f$id)
)

cat("Phenotypes:", length(pheno_cfg), "\n")
cat("Models:", length(model_cfg), "\n")
cat("Features:", length(feature_cfg), "\n")

# Fit single exposure-outcome pair
fit_exposure_outcome <- function(wide_df, exposure_var, outcome_var, model_spec) {

  # Prepare data
  df <- wide_df %>%
    filter(!is.na(.data[[outcome_var]]), !is.na(.data[[exposure_var]]))

  # Minimum sample size check
  if (nrow(df) < 100) {
    return(NULL)
  }

  # Check outcome variance
  n_cases <- sum(df[[outcome_var]])
  if (n_cases < 11 || n_cases >= (nrow(df) - 11)) {
    return(NULL)
  }

  # Build formula
  formula_str <- model_spec$formula_template
  formula_str <- gsub("\\{outcome\\}", outcome_var, formula_str)
  formula_str <- gsub("\\{exposure\\}", paste0("scale(", exposure_var, ")"), formula_str)

  # Fit model
  tryCatch({
    m <- glm(
      as.formula(formula_str),
      family = binomial(),
      data = df
    )

    # Extract exposure coefficient (first non-intercept term)
    coef_summary <- tidy(m) %>%
      filter(term != "(Intercept)", grepl("scale", term)) %>%
      slice(1)

    if (nrow(coef_summary) == 0) return(NULL)

    tibble(
      outcome = outcome_var,
      exposure_id = exposure_var,
      model_spec_id = model_spec$id,
      n = nrow(df),
      n_cases = n_cases,
      estimate = coef_summary$estimate,
      se = coef_summary$std.error,
      or = exp(coef_summary$estimate),
      or_ci_low = exp(coef_summary$estimate - 1.96 * coef_summary$std.error),
      or_ci_high = exp(coef_summary$estimate + 1.96 * coef_summary$std.error),
      z_value = coef_summary$statistic,
      p_value = coef_summary$p.value
    )

  }, error = function(e) {
    return(NULL)
  })
}

# Run ExWAS for one model specification
run_exwas_model <- function(wide_df, exposure_cols, outcome_cols, model_spec) {

  cat("\nRunning model:", model_spec$id, "\n")
  cat("  Formula:", model_spec$formula_template, "\n")

  # All combinations
  combinations <- expand.grid(
    exposure = exposure_cols,
    outcome = outcome_cols,
    stringsAsFactors = FALSE
  )

  cat("  Total tests:", nrow(combinations), "\n")

  # Fit all models
  results <- list()
  for (i in 1:nrow(combinations)) {
    if (i %% 10 == 0) cat("  Progress:", i, "/", nrow(combinations), "\n")

    res <- fit_exposure_outcome(
      wide_df,
      combinations$exposure[i],
      combinations$outcome[i],
      model_spec
    )

    if (!is.null(res)) {
      results[[length(results) + 1]] <- res
    }
  }

  results_df <- bind_rows(results)
  cat("  Successful fits:", nrow(results_df), "\n")

  return(results_df)
}

main <- function() {

  # Load person-month cohort
  cat("\nLoading person-month cohort...\n")
  pm <- open_dataset(path(paths$gold, "person_month")) %>%
    collect()

  cat("  Person-months:", format(nrow(pm), big.mark=","), "\n")

  # Load exposure rollup
  cat("Loading exposure rollup...\n")
  ex <- open_dataset(path(paths$gold, "exposure_rollup")) %>%
    filter(metric == "mean") %>%
    collect()

  cat("  Exposure observations:", format(nrow(ex), big.mark=","), "\n")

  # Pivot exposures wide
  cat("Pivoting exposures to wide format...\n")
  ex_wide <- ex %>%
    select(person_id, ym, exposure_id, value) %>%
    pivot_wider(names_from = exposure_id, values_from = value)

  # Join with person-months
  wide <- pm %>%
    left_join(ex_wide, by = c("person_id", "ym"))

  # Identify columns
  exposure_cols <- unique(ex$exposure_id)
  outcome_cols <- names(wide)[grepl("_flag$", names(wide))]

  cat("\nExposures:", length(exposure_cols), "\n")
  cat("Outcomes:", length(outcome_cols), "\n")

  if (length(exposure_cols) == 0 || length(outcome_cols) == 0) {
    stop("No exposures or outcomes found")
  }

  # Run ExWAS for each model
  all_results <- bind_rows(lapply(model_cfg, function(model_spec) {
    run_exwas_model(wide, exposure_cols, outcome_cols, model_spec)
  }))

  # Multiple testing correction
  cat("\nApplying multiple testing correction...\n")
  all_results <- all_results %>%
    group_by(model_spec_id) %>%
    mutate(
      p.adj.fdr = p.adjust(p_value, method = "fdr"),
      p.adj.bonferroni = p.adjust(p_value, method = "bonferroni")
    ) %>%
    ungroup() %>%
    arrange(p_value)

  # Summary
  cat("\n=== Results Summary ===\n")
  summary_stats <- all_results %>%
    group_by(model_spec_id) %>%
    summarise(
      n_tests = n(),
      n_sig_p05 = sum(p_value < 0.05),
      n_sig_fdr = sum(p.adj.fdr < 0.05),
      n_sig_bonf = sum(p.adj.bonferroni < 0.05),
      .groups = "drop"
    )

  print(summary_stats)

  # Top hits
  cat("\n=== Top 10 Associations ===\n")
  top_hits <- all_results %>%
    head(10) %>%
    mutate(
      or_ci = sprintf("%.2f (%.2f-%.2f)", or, or_ci_low, or_ci_high),
      exposure_label = feature_labels[exposure_id]
    ) %>%
    select(model_spec_id, outcome, exposure_label, n_cases, or_ci, p_value, p.adj.fdr)

  print(top_hits, n = 10)

  # Write results
  cat("\nWriting results...\n")
  write_parquet_ds(
    all_results,
    path(paths$gold, "exwas_result"),
    partitioning = c("model_spec_id")
  )

  cat("\n✓ Enhanced ExWAS complete\n")

  invisible(all_results)
}

# Run
if (!interactive()) {
  main()
}