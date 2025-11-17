# r/70_data_quality.R
# Comprehensive data quality checks across pipeline stages

source(here::here("r", "00_env.R"))

# Helper to format percentages
pct <- function(x, total) {
  sprintf("%.1f%%", 100 * x / total)
}

check_bronze <- function() {
  message("\n=== Bronze Layer Quality Checks ===\n")

  bronze_files <- fs::dir_ls(paths$bronze, recurse = TRUE, glob = "*.parquet")

  if (length(bronze_files) == 0) {
    warning("No bronze files found")
    return(NULL)
  }

  message(glue("Found {length(bronze_files)} bronze files"))

  # Sample first file for column inspection
  sample_file <- bronze_files[1]
  sample_df <- arrow::open_dataset(sample_file) %>% head(100) %>% collect()

  message("\nSample columns from first file:")
  print(names(sample_df))

  invisible(bronze_files)
}

check_silver <- function() {
  message("\n=== Silver Layer Quality Checks ===\n")

  # Check visits
  if (fs::dir_exists(path(paths$silver, "visit"))) {
    vis <- read_ds(path(paths$silver, "visit"))

    # Collect sample for detailed checks
    vis_sample <- vis %>%
      select(person_id, admit_date, dx_primary, zip5, tract_geoid) %>%
      head(10000) %>%
      collect()

    vis_summary <- vis %>%
      summarise(
        n_visits = n(),
        n_persons = n_distinct(person_id),
        min_date = min(admit_date, na.rm = TRUE),
        max_date = max(admit_date, na.rm = TRUE),
        missing_dx = sum(is.na(dx_primary)),
        missing_zip = sum(is.na(zip5)),
        missing_tract = sum(is.na(tract_geoid))
      ) %>%
      collect()

    message("Visit Summary:")
    message(glue("  Total visits: {scales::comma(vis_summary$n_visits)}"))
    message(glue("  Unique persons: {scales::comma(vis_summary$n_persons)}"))
    message(glue("  Date range: {vis_summary$min_date} to {vis_summary$max_date}"))
    message(glue("  Missing primary DX: {scales::comma(vis_summary$missing_dx)} ({pct(vis_summary$missing_dx, vis_summary$n_visits)})"))
    message(glue("  Missing ZIP: {scales::comma(vis_summary$missing_zip)} ({pct(vis_summary$missing_zip, vis_summary$n_visits)})"))
    message(glue("  Missing tract: {scales::comma(vis_summary$missing_tract)} ({pct(vis_summary$missing_tract, vis_summary$n_visits)})"))

    # Check for duplicates in sample
    n_dup <- sum(duplicated(vis_sample$person_id))
    message(glue("\n  Sample check: {n_dup} duplicate person_ids in first 10K (expected for multi-visit persons)"))

    # DX code patterns
    dx_patterns <- vis_sample %>%
      filter(!is.na(dx_primary)) %>%
      mutate(dx_chapter = substr(dx_primary, 1, 1)) %>%
      count(dx_chapter, sort = TRUE) %>%
      head(10)

    message("\nTop diagnosis chapters (ICD-10):")
    print(dx_patterns)

  } else {
    warning("No silver/visit dataset found")
  }

  invisible(vis_summary)
}

check_gold_person_month <- function() {
  message("\n=== Gold: Person-Month Quality Checks ===\n")

  if (!fs::dir_exists(path(paths$gold, "person_month"))) {
    warning("No person_month dataset found")
    return(NULL)
  }

  pm <- read_ds(path(paths$gold, "person_month"))

  pm_summary <- pm %>%
    summarise(
      n_records = n(),
      n_persons = n_distinct(person_id),
      min_date = min(ym, na.rm = TRUE),
      max_date = max(ym, na.rm = TRUE),
      avg_visits = mean(n_visits, na.rm = TRUE),
      missing_geo = sum(is.na(tract_geoid) & is.na(zip5))
    ) %>%
    collect()

  message("Person-Month Summary:")
  message(glue("  Records: {scales::comma(pm_summary$n_records)}"))
  message(glue("  Unique persons: {scales::comma(pm_summary$n_persons)}"))
  message(glue("  Date range: {pm_summary$min_date} to {pm_summary$max_date}"))
  message(glue("  Avg visits/person-month: {round(pm_summary$avg_visits, 2)}"))
  message(glue("  Missing all geography: {scales::comma(pm_summary$missing_geo)} ({pct(pm_summary$missing_geo, pm_summary$n_records)})"))

  # Phenotype flags
  pm_sample <- pm %>% head(1000) %>% collect()
  flag_cols <- names(pm_sample)[str_detect(names(pm_sample), "_flag$")]

  if (length(flag_cols) > 0) {
    message("\nPhenotype prevalence (sample of 1000):")
    for (col in flag_cols) {
      n_pos <- sum(pm_sample[[col]], na.rm = TRUE)
      message(glue("  {col}: {n_pos} ({pct(n_pos, 1000)})"))
    }
  }

  invisible(pm_summary)
}

check_gold_exposures <- function() {
  message("\n=== Gold: Exposure Data Quality Checks ===\n")

  # Daily exposures
  if (fs::dir_exists(path(paths$gold, "exposure_daily"))) {
    exp <- read_ds(path(paths$gold, "exposure_daily"))

    exp_summary <- exp %>%
      group_by(exposure_id, geo_type) %>%
      summarise(
        n_obs = n(),
        n_geos = n_distinct(geo_id),
        min_date = min(obs_date, na.rm = TRUE),
        max_date = max(obs_date, na.rm = TRUE),
        mean_val = mean(value, na.rm = TRUE),
        pct_missing = 100 * sum(is.na(value)) / n(),
        .groups = "drop"
      ) %>%
      collect()

    message("Exposure Daily Summary:")
    print(exp_summary, n = Inf)

  } else {
    warning("No exposure_daily dataset found")
  }

  # Rollups
  if (fs::dir_exists(path(paths$gold, "exposure_rollup"))) {
    roll <- read_ds(path(paths$gold, "exposure_rollup"))

    roll_summary <- roll %>%
      group_by(exposure_id, metric) %>%
      summarise(
        n_person_months = n(),
        mean_val = mean(value, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      collect()

    message("\nExposure Rollup Summary:")
    print(roll_summary, n = Inf)

  } else {
    warning("No exposure_rollup dataset found")
  }

  invisible(exp_summary)
}

check_gold_exwas <- function() {
  message("\n=== Gold: ExWAS Results Quality Checks ===\n")

  if (!fs::dir_exists(path(paths$gold, "exwas_result"))) {
    warning("No exwas_result dataset found")
    return(NULL)
  }

  exwas <- read_ds(path(paths$gold, "exwas_result")) %>%
    collect()

  message(glue("Total tests: {scales::comma(nrow(exwas))}"))

  # By model spec
  by_model <- exwas %>%
    group_by(model_spec_id) %>%
    summarise(
      n_tests = n(),
      n_sig_p05 = sum(p_value < 0.05, na.rm = TRUE),
      n_sig_q05 = sum(q_value < 0.05, na.rm = TRUE),
      median_n = median(n, na.rm = TRUE),
      .groups = "drop"
    )

  message("\nBy model specification:")
  print(by_model)

  # By outcome
  by_outcome <- exwas %>%
    group_by(outcome) %>%
    summarise(
      n_tests = n(),
      n_sig_q05 = sum(q_value < 0.05, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(desc(n_sig_q05))

  message("\nBy outcome:")
  print(by_outcome)

  # Top hits
  top_hits <- exwas %>%
    filter(q_value < 0.05) %>%
    arrange(p_value) %>%
    select(outcome, exposure_id, model_spec_id, or, or_ci_low, or_ci_high, p_value, q_value) %>%
    head(20)

  if (nrow(top_hits) > 0) {
    message("\nTop 20 significant associations:")
    print(top_hits, n = 20)
  } else {
    message("\nNo significant associations at FDR < 0.05")
  }

  invisible(exwas)
}

# Main quality report
main <- function() {
  message("\n")
  message("╔════════════════════════════════════════════════════════════╗")
  message("║         PEGS Explorer Data Quality Report                 ║")
  message("╚════════════════════════════════════════════════════════════╝")
  message(glue("\nGenerated: {Sys.time()}"))

  check_bronze()
  check_silver()
  check_gold_person_month()
  check_gold_exposures()
  check_gold_exwas()

  message("\n")
  message("╔════════════════════════════════════════════════════════════╗")
  message("║                    Report Complete                         ║")
  message("╚════════════════════════════════════════════════════════════╝")
  message("\n")
}

# Run if called directly
if (!interactive()) {
  main()
}