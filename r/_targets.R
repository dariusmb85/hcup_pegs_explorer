# _targets.R - HCUP PEGS Explorer Pipeline
library(targets)
library(tarchetypes)

# ==============================================================================
# SLURM Controller Setup (Based on HCUP AP Template)
# ==============================================================================

USE_HPC <- Sys.getenv("USE_HPC", "FALSE") == "TRUE"

if (USE_HPC) {
  library(crew)
  library(crew.cluster)

  # Standard compute jobs (geocoding, data processing)
  controller_normal <- crew.cluster::crew_controller_slurm(
    name = "controller_normal",
    workers = 4,
    seconds_interval = 30,
    options_cluster = crew.cluster::crew_options_slurm(
      partition = "norm",  # ← YOUR SLURM PARTITION
      cpus_per_task = 4,
      memory_gigabytes_per_cpu = 16,
      time_minutes = 240,
      log_output = "logs/slurm_normal_%j.out",
      log_error = "logs/slurm_normal_%j.err",
      script_lines = "export R_LIBS_USER=~/R/x86_64-pc-linux-gnu-library/4.3"
    ),
    tasks_max = Inf
  )

  # High-memory jobs (ExWAS analysis)
  controller_highmem <- crew.cluster::crew_controller_slurm(
    name = "controller_highmem",
    workers = 3,  # 3 parallel ExWAS jobs (overall, male, female)
    seconds_interval = 30,
    options_cluster = crew.cluster::crew_options_slurm(
      partition = "highmem",  # ← YOUR HIGHMEM PARTITION
      cpus_per_task = 8,
      memory_gigabytes_per_cpu = 32,
      time_minutes = 720,  # 12 hours for ExWAS
      log_output = "logs/slurm_highmem_%j.out",
      log_error = "logs/slurm_highmem_%j.err",
      script_lines = "export R_LIBS_USER=~/R/x86_64-pc-linux-gnu-library/4.3"
    ),
    tasks_max = Inf
  )

  # Set controller group
  tar_option_set(
    controller = crew::crew_controller_group(
      controller_normal,
      controller_highmem
    )
  )
}

# ==============================================================================
# Global Options
# ==============================================================================

tar_option_set(
  packages = c(
    "arrow", "dplyr", "tidyr", "yaml", "here", "lubridate",
    "broom", "stringr", "fs", "glue", "httr", "jsonlite", "digest"
  ),
  format = "file",  # ← BETTER: Track file paths, not R objects
  memory = "transient",
  garbage_collection = TRUE,
  error = "continue",  # Continue even if one target fails
  storage = "worker",
  retrieval = "worker",
  deployment = "worker",
  resources = tar_resources(
    crew = tar_resources_crew(
      controller = "controller_normal"  # Default controller
    )
  )
)

# ==============================================================================
# Helper Functions
# ==============================================================================

check_file_exists <- function(path) {
  if (dir.exists(path) || file.exists(path)) {
    return(path)
  } else {
    stop("Path does not exist: ", path)
  }
}

# ==============================================================================
# Pipeline Definition
# ==============================================================================

list(

  # ============================================================================
  # Stage 1: Silver Layer
  # ============================================================================
  tar_target(
    name = silver_layer,
    command = {
      source(here::here("r", "01_hcup_silver.R"))
      check_file_exists("data_test/silver/visit")
    },
    format = "file"
  ),

  # ============================================================================
  # Stage 1b: Quality Checks on Silver
  # ============================================================================
  tar_target(
    name = qc_silver,
    command = {
      source(here::here("r", "07_data_quality.R"))
      check_file_exists("data_test/gold/quality_checks")
    },
    format = "file"
  ),

  # ============================================================================
  # Stage 2: Geocoding (runs in parallel with Stage 3)
  # ============================================================================
  tar_target(
    name = geocoded_visits,
    command = {
      source(here::here("r", "015_geocode_enrich.R"))
      check_file_exists("data_test/silver/visit")
    },
    format = "file"
  ),

  # ============================================================================
  # Stage 3: Download Exposures (runs in parallel with Stage 2)
  # ============================================================================
  tar_target(
    name = exposures_downloaded,
    command = {
      source(here::here("r", "02_dataverse_exposures.R"))
      check_file_exists("data_test/gold/exposures_monthly")
    },
    format = "file"
  ),

  # ============================================================================
  # Stage 4: Person-Month Cohort (depends on geocoding)
  # ============================================================================
  tar_target(
    name = person_month_cohort,
    command = {
      source(here::here("r", "04_person_monthV2.R"))
      check_file_exists("data_test/gold/person_month")
    },
    format = "file"
  ),

  # ============================================================================
  # Stage 5: Join Exposures (depends on cohort + exposures)
  # ============================================================================
  tar_target(
    name = joined_data,
    command = {
      source(here::here("r", "05_join_exposures.R"))
      check_file_exists("data_test/gold/person_month_exposures")
    },
    format = "file"
  ),

  # ============================================================================
  # Stage 6: Exposure Rollup
  # ============================================================================
  tar_target(
    name = exposure_rollup_complete,
    command = {
      source(here::here("r", "03_exposure_rollup.R"))
      check_file_exists("data_test/gold/exposure_rollup")
    },
    format = "file"
  ),

  # ============================================================================
  # Stage 7: ExWAS Analysis (3 parallel jobs - HIGHMEM)
  # ============================================================================

  # 7a: Overall (all person-months)
  tar_target(
    name = exwas_overall,
    command = {
      library(arrow)
      library(dplyr)
      library(tidyr)
      library(broom)
      library(yaml)

      # Load data
      pm <- arrow::open_dataset("data_test/gold/person_month") %>% dplyr::collect()
      ex <- arrow::open_dataset("data_test/gold/exposure_rollup") %>%
        dplyr::filter(metric == "mean") %>%
        dplyr::collect()

      # Pivot wide
      ex_wide <- ex %>%
        dplyr::select(person_id, ym, exposure_id, value) %>%
        tidyr::pivot_wider(names_from = exposure_id, values_from = value)

      wide <- pm %>% dplyr::left_join(ex_wide, by = c("person_id", "ym"))

      # Get overall models from config
      model_cfg <- yaml::read_yaml(here::here("config", "covariates.yaml"))$exwas_models
      models_overall <- model_cfg[sapply(model_cfg, function(m) {
        grepl("_overall$", m$id) || m$strata == "all"
      })]

      # Source ExWAS functions
      source(here::here("r", "06_ewas_enhanced.R"), local = TRUE)

      # Run models
      exposure_cols <- unique(ex$exposure_id)
      outcome_cols <- names(wide)[grepl("_flag$", names(wide))]

      results <- dplyr::bind_rows(
        lapply(models_overall, function(spec) {
          run_exwas_model(wide, exposure_cols, outcome_cols, spec)
        })
      )

      # Multiple testing correction
      results <- results %>%
        dplyr::group_by(model_spec_id) %>%
        dplyr::mutate(
          p.adj.fdr = p.adjust(p_value, method = "fdr"),
          p.adj.bonferroni = p.adjust(p_value, method = "bonferroni")
        ) %>%
        dplyr::ungroup()

      # Save
      arrow::write_parquet(results, "data_test/gold/exwas_overall.parquet")
      "data_test/gold/exwas_overall.parquet"
    },
    format = "file",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "controller_highmem")
    )
  ),

  # 7b: Male stratified
  tar_target(
    name = exwas_male,
    command = {
      library(arrow)
      library(dplyr)
      library(tidyr)
      library(broom)
      library(yaml)

      # Load data - filter to males
      pm <- arrow::open_dataset("data_test/gold/person_month") %>%
        dplyr::collect() %>%
        dplyr::filter(female == 0)

      if (nrow(pm) == 0) {
        warning("No male person-months found")
        return(NULL)
      }

      ex <- arrow::open_dataset("data_test/gold/exposure_rollup") %>%
        dplyr::filter(metric == "mean") %>%
        dplyr::collect()

      ex_wide <- ex %>%
        dplyr::select(person_id, ym, exposure_id, value) %>%
        tidyr::pivot_wider(names_from = exposure_id, values_from = value)

      wide <- pm %>% dplyr::left_join(ex_wide, by = c("person_id", "ym"))

      # Get male models
      model_cfg <- yaml::read_yaml(here::here("config", "covariates.yaml"))$exwas_models
      models_male <- model_cfg[sapply(model_cfg, function(m) {
        grepl("_male$", m$id) || m$strata == "male"
      })]

      if (length(models_male) == 0) {
        warning("No male models defined in config")
        return(NULL)
      }

      source(here::here("r", "06_ewas_enhanced.R"), local = TRUE)

      exposure_cols <- unique(ex$exposure_id)
      outcome_cols <- names(wide)[grepl("_flag$", names(wide))]

      results <- dplyr::bind_rows(
        lapply(models_male, function(spec) {
          run_exwas_model(wide, exposure_cols, outcome_cols, spec)
        })
      )

      results <- results %>%
        dplyr::group_by(model_spec_id) %>%
        dplyr::mutate(
          p.adj.fdr = p.adjust(p_value, method = "fdr"),
          p.adj.bonferroni = p.adjust(p_value, method = "bonferroni")
        ) %>%
        dplyr::ungroup()

      arrow::write_parquet(results, "data_test/gold/exwas_male.parquet")
      "data_test/gold/exwas_male.parquet"
    },
    format = "file",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "controller_highmem")
    )
  ),

  # 7c: Female stratified
  tar_target(
    name = exwas_female,
    command = {
      library(arrow)
      library(dplyr)
      library(tidyr)
      library(broom)
      library(yaml)

      # Load data - filter to females
      pm <- arrow::open_dataset("data_test/gold/person_month") %>%
        dplyr::collect() %>%
        dplyr::filter(female == 1)

      if (nrow(pm) == 0) {
        warning("No female person-months found")
        return(NULL)
      }

      ex <- arrow::open_dataset("data_test/gold/exposure_rollup") %>%
        dplyr::filter(metric == "mean") %>%
        dplyr::collect()

      ex_wide <- ex %>%
        dplyr::select(person_id, ym, exposure_id, value) %>%
        tidyr::pivot_wider(names_from = exposure_id, values_from = value)

      wide <- pm %>% dplyr::left_join(ex_wide, by = c("person_id", "ym"))

      # Get female models
      model_cfg <- yaml::read_yaml(here::here("config", "covariates.yaml"))$exwas_models
      models_female <- model_cfg[sapply(model_cfg, function(m) {
        grepl("_female$", m$id) || m$strata == "female"
      })]

      if (length(models_female) == 0) {
        warning("No female models defined in config")
        return(NULL)
      }

      source(here::here("r", "06_ewas_enhanced.R"), local = TRUE)

      exposure_cols <- unique(ex$exposure_id)
      outcome_cols <- names(wide)[grepl("_flag$", names(wide))]

      results <- dplyr::bind_rows(
        lapply(models_female, function(spec) {
          run_exwas_model(wide, exposure_cols, outcome_cols, spec)
        })
      )

      results <- results %>%
        dplyr::group_by(model_spec_id) %>%
        dplyr::mutate(
          p.adj.fdr = p.adjust(p_value, method = "fdr"),
          p.adj.bonferroni = p.adjust(p_value, method = "bonferroni")
        ) %>%
        dplyr::ungroup()

      arrow::write_parquet(results, "data_test/gold/exwas_female.parquet")
      "data_test/gold/exwas_female.parquet"
    },
    format = "file",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "controller_highmem")
    )
  ),

  # ============================================================================
  # Stage 8: Combine All Results
  # ============================================================================
  tar_target(
    name = exwas_combined,
    command = {
      library(arrow)
      library(dplyr)

      # Read all results
      overall <- if (file.exists(exwas_overall)) {
        arrow::read_parquet(exwas_overall)
      } else NULL

      male <- if (file.exists(exwas_male)) {
        arrow::read_parquet(exwas_male)
      } else NULL

      female <- if (file.exists(exwas_female)) {
        arrow::read_parquet(exwas_female)
      } else NULL

      # Combine
      all_results <- dplyr::bind_rows(
        overall,
        male,
        female
      ) %>%
        dplyr::arrange(p_value)

      # Save combined
      arrow::write_parquet(all_results, "data_test/gold/exwas_all_results.parquet")

      "data_test/gold/exwas_all_results.parquet"
    },
    format = "file"
  ),

  # ============================================================================
  # Stage 9: Final Report
  # ============================================================================
  tar_target(
    name = final_report,
    command = {
      library(arrow)
      library(dplyr)

      results <- arrow::read_parquet(exwas_combined)

      # Summary statistics
      summary <- list(
        total_tests = nrow(results),
        sig_p05 = sum(results$p_value < 0.05, na.rm = TRUE),
        sig_fdr = sum(results$p.adj.fdr < 0.05, na.rm = TRUE),
        sig_bonf = sum(results$p.adj.bonferroni < 0.05, na.rm = TRUE),
        by_model = results %>%
          dplyr::group_by(model_spec_id) %>%
          dplyr::summarise(
            n_tests = dplyr::n(),
            n_sig_fdr = sum(p.adj.fdr < 0.05, na.rm = TRUE),
            .groups = "drop"
          )
      )

      saveRDS(summary, "data_test/gold/exwas_summary.rds")

      cat("\n=== PIPELINE COMPLETE ===\n")
      cat("Total tests:", summary$total_tests, "\n")
      cat("Significant (FDR<0.05):", summary$sig_fdr, "\n\n")
      print(summary$by_model)

      "data_test/gold/exwas_summary.rds"
    },
    format = "file"
  )
)