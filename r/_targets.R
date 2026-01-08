# _targets.R - HCUP PEGS Pipeline Orchestration
library(targets)
library(tarchetypes)

# ============================================================================
# Configuration
# ============================================================================

tar_option_set(
  packages = c("arrow", "dplyr", "tidyr", "yaml", "here", "lubridate",
               "broom", "stringr", "fs", "glue", "httr", "jsonlite", "digest"),
  format = "qs",
  memory = "transient",
  garbage_collection = TRUE,
  error = "continue"  # Continue pipeline even if one target fails
)

# HPC Configuration
USE_HPC <- Sys.getenv("USE_HPC", "FALSE") == "TRUE"

if (USE_HPC) {
  library(crew)
  library(crew.cluster)

  tar_option_set(
    controller = crew_controller_slurm(
      name = "hcup_cluster",
      workers = 20,
      slurm_memory_gigabytes_per_cpu = 12,
      slurm_cpus_per_task = 4,
      slurm_time_minutes = 480,
      slurm_partition = "highmem",
      script_lines = "export R_LIBS_USER=~/R/x86_64-pc-linux-gnu-library/4.3"
    )
  )
}

# ============================================================================
# Helper Functions
# ============================================================================

run_script <- function(script_path) {
  source(script_path)
  return(TRUE)
}

check_file_exists <- function(path) {
  if (dir.exists(path)) {
    return(path)
  } else if (file.exists(path)) {
    return(path)
  } else {
    stop("Path does not exist: ", path)
  }
}

# ============================================================================
# Pipeline Definition
# ============================================================================

list(
  # ==========================================================================
  # Stage 1: Data Ingestion & Harmonization
  # ==========================================================================

  tar_target(
    name = silver_layer,
    command = {
      source("01_hcup_silver.R")
      check_file_exists("data_test/silver/visit")
    },
    format = "file",
    deployment = "main"
  ),

  # QC Gate 1: Visit-level quality checks
  tar_target(
    name = qc_silver,
    command = {
      source("07_data_quality.R")
      check_file_exists("data_test/gold/quality_checks")
    },
    format = "file",
    deployment = "main"
  ),

  # ==========================================================================
  # Stage 2: Geocoding (can run parallel with exposures)
  # ==========================================================================

  tar_target(
    name = geocoded_visits,
    command = {
      source("015_geocode_enrich.R")
      check_file_exists("data_test/silver/visit")
    },
    format = "file",
    deployment = "main"
  ),

  # ==========================================================================
  # Stage 3: Environmental Exposures (parallel with geocoding)
  # ==========================================================================

  tar_target(
    name = exposures_downloaded,
    command = {
      source("02_dataverse_exposures.R")
      check_file_exists("data_test/gold/exposures_monthly")
    },
    format = "file",
    deployment = "main"
  ),

  # ==========================================================================
  # Stage 4: Cohort Building
  # ==========================================================================

  tar_target(
    name = person_month_cohort,
    command = {
      source("04_person_monthV2.R")
      check_file_exists("data_test/gold/person_month")
    },
    format = "file",
    deployment = "main"
  ),

  # ==========================================================================
  # Stage 5: Join Exposures to Cohort
  # ==========================================================================

  tar_target(
    name = joined_data,
    command = {
      source("05_join_exposures.R")
      check_file_exists("data_test/gold/person_month_exposures")
    },
    format = "file",
    deployment = "main"
  ),

  # ==========================================================================
  # Stage 6: Exposure Rollup
  # ==========================================================================

  tar_target(
    name = exposure_rollup_complete,
    command = {
      source("03_exposure_rollup.R")
      check_file_exists("data_test/gold/exposure_rollup")
    },
    format = "file",
    deployment = "main"
  ),

  # ==========================================================================
  # Stage 7: ExWAS - Stratified by Sex (3 parallel jobs)
  # ==========================================================================

  # Overall stratum
  tar_target(
    name = exwas_overall,
    command = {
      library(arrow)
      library(dplyr)
      library(tidyr)
      library(broom)
      library(yaml)

      # Load data
      pm <- open_dataset("data_test/gold/person_month") %>% collect()
      ex <- open_dataset("data_test/gold/exposure_rollup") %>%
        filter(metric == "mean") %>% collect()

      # Pivot and join
      ex_wide <- ex %>%
        select(person_id, ym, exposure_id, value) %>%
        pivot_wider(names_from = exposure_id, values_from = value)
      wide <- pm %>% left_join(ex_wide, by = c("person_id", "ym"))

      # Get models
      model_cfg <- read_yaml("../config/covariates.yaml")$exwas_models
      models <- Filter(function(x) grepl("_overall$", x$id), model_cfg)

      # Run ExWAS
      source("r/06_exwas_stratified.R")
      results <- bind_rows(lapply(models, function(spec) {
        run_exwas_model(wide, unique(ex$exposure_id),
                       names(wide)[grepl("_flag$", names(wide))], spec)
      }))

      # Apply MTC
      results <- results %>%
        group_by(model_spec_id) %>%
        mutate(
          p.adj.fdr = p.adjust(p_value, method = "fdr"),
          p.adj.bonferroni = p.adjust(p_value, method = "bonferroni")
        ) %>%
        ungroup()

      # Save
      write_parquet(results, "data_test/gold/exwas_overall.parquet")
      "data_test/gold/exwas_overall.parquet"
    },
    format = "file",
    deployment = if (USE_HPC) "worker" else "main"
  ),

  # Male stratum
  tar_target(
    name = exwas_male,
    command = {
      library(arrow)
      library(dplyr)
      library(tidyr)
      library(broom)
      library(yaml)

      pm <- open_dataset("data_test/gold/person_month") %>% collect()
      ex <- open_dataset("data_test/gold/exposure_rollup") %>%
        filter(metric == "mean") %>% collect()

      ex_wide <- ex %>%
        select(person_id, ym, exposure_id, value) %>%
        pivot_wider(names_from = exposure_id, values_from = value)
      wide <- pm %>% left_join(ex_wide, by = c("person_id", "ym"))

      model_cfg <- read_yaml("../config/covariates.yaml")$exwas_models
      models <- Filter(function(x) grepl("_male$", x$id), model_cfg)

      source("r/06_exwas_stratified.R")
      results <- bind_rows(lapply(models, function(spec) {
        run_exwas_model(wide, unique(ex$exposure_id),
                       names(wide)[grepl("_flag$", names(wide))], spec)
      }))

      results <- results %>%
        group_by(model_spec_id) %>%
        mutate(
          p.adj.fdr = p.adjust(p_value, method = "fdr"),
          p.adj.bonferroni = p.adjust(p_value, method = "bonferroni")
        ) %>%
        ungroup()

      write_parquet(results, "data_test/gold/exwas_male.parquet")
      "data_test/gold/exwas_male.parquet"
    },
    format = "file",
    deployment = if (USE_HPC) "worker" else "main"
  ),

  # Female stratum
  tar_target(
    name = exwas_female,
    command = {
      library(arrow)
      library(dplyr)
      library(tidyr)
      library(broom)
      library(yaml)

      pm <- open_dataset("data_test/gold/person_month") %>% collect()
      ex <- open_dataset("data_test/gold/exposure_rollup") %>%
        filter(metric == "mean") %>% collect()

      ex_wide <- ex %>%
        select(person_id, ym, exposure_id, value) %>%
        pivot_wider(names_from = exposure_id, values_from = value)
      wide <- pm %>% left_join(ex_wide, by = c("person_id", "ym"))

      model_cfg <- read_yaml("../config/covariates.yaml")$exwas_models
      models <- Filter(function(x) grepl("_female$", x$id), model_cfg)

      source("r/06_exwas_stratified.R")
      results <- bind_rows(lapply(models, function(spec) {
        run_exwas_model(wide, unique(ex$exposure_id),
                       names(wide)[grepl("_flag$", names(wide))], spec)
      }))

      results <- results %>%
        group_by(model_spec_id) %>%
        mutate(
          p.adj.fdr = p.adjust(p_value, method = "fdr"),
          p.adj.bonferroni = p.adjust(p_value, method = "bonferroni")
        ) %>%
        ungroup()

      write_parquet(results, "data_test/gold/exwas_female.parquet")
      "data_test/gold/exwas_female.parquet"
    },
    format = "file",
    deployment = if (USE_HPC) "worker" else "main"
  ),

  # ==========================================================================
  # Stage 8: Combine ExWAS Results
  # ==========================================================================

  tar_target(
    name = exwas_combined,
    command = {
      library(arrow)
      library(dplyr)

      overall <- read_parquet(exwas_overall)
      male <- read_parquet(exwas_male)
      female <- read_parquet(exwas_female)

      all_results <- bind_rows(overall, male, female) %>%
        arrange(p_value)

      # Save partitioned by strata
      write_parquet_ds(
        all_results,
        "data_test/gold/exwas_result_stratified",
        partitioning = c("strata", "model_spec_id")
      )

      # Also save combined flat file
      write_parquet(all_results, "data_test/gold/exwas_all_results.parquet")

      "data_test/gold/exwas_result_stratified"
    },
    format = "file",
    deployment = "main"
  ),

  # ==========================================================================
  # Stage 9: Final Quality Report
  # ==========================================================================

  tar_target(
    name = final_report,
    command = {
      library(arrow)
      library(dplyr)

      results <- read_parquet("data_test/gold/exwas_all_results.parquet")

      # Summary statistics
      summary <- list(
        total_tests = nrow(results),
        sig_p05 = sum(results$p_value < 0.05, na.rm = TRUE),
        sig_fdr = sum(results$p.adj.fdr < 0.05, na.rm = TRUE),
        sig_bonf = sum(results$p.adj.bonferroni < 0.05, na.rm = TRUE),
        by_strata = results %>%
          group_by(strata) %>%
          summarise(
            n_tests = n(),
            n_sig = sum(p_value < 0.05, na.rm = TRUE),
            .groups = "drop"
          )
      )

      saveRDS(summary, "data_test/gold/exwas_summary.rds")

      cat("\n=== PIPELINE COMPLETE ===\n")
      cat("Total tests:", summary$total_tests, "\n")
      cat("Significant (p<0.05):", summary$sig_p05, "\n")
      cat("Significant (FDR):", summary$sig_fdr, "\n")
      cat("Significant (Bonferroni):", summary$sig_bonf, "\n\n")

      "data_test/gold/exwas_summary.rds"
    },
    format = "file",
    deployment = "main"
  )
)