# _targets.R - HCUP PEGS Explorer Pipeline
library(targets)
library(tarchetypes)

# Set CRAN mirror to fix package installation errors
options(repos = c(CRAN = "https://cloud.r-project.org/"))

# ==============================================================================
# SLURM Controller Setup
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
    slurm_log_output = "logs/slurm_normal_%j.out",
    slurm_log_error = "logs/slurm_normal_%j.err",
    slurm_partition = "norm",
    slurm_cpus_per_task = 4,
    slurm_memory_gigabytes_per_cpu = 16,
    slurm_time_minutes = 240,
    script_lines = c(
      "export R_LIBS_USER=~/R/x86_64-pc-linux-gnu-library/4.3",
      "Rscript -e \"options(repos = c(CRAN = 'https://cloud.r-project.org/'))\""
    ),
    verbose = FALSE
  )

  # High-memory controller for ExWAS
  controller_highmem <- crew.cluster::crew_controller_slurm(
    name = "controller_highmem",
    workers = 3,
    seconds_interval = 30,
    slurm_log_output = "logs/slurm_highmem_%j.out",
    slurm_log_error = "logs/slurm_highmem_%j.err",
    slurm_partition = "highmem",
    slurm_cpus_per_task = 8,
    slurm_memory_gigabytes_per_cpu = 32,
    slurm_time_minutes = 720,
    script_lines = c(
      "export R_LIBS_USER=~/R/x86_64-pc-linux-gnu-library/4.3",
      "Rscript -e \"options(repos = c(CRAN = 'https://cloud.r-project.org/'))\""
    ),
    verbose = FALSE
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
# Pipeline Definition with EXPLICIT DEPENDENCIES
# ==============================================================================

list(

  # ============================================================================
  # Stage 1: Silver Layer
  # ============================================================================
  tar_target(
    name = silver_layer,
    command = {
      source(here::here("r", "01_hcup_silver.R"))
      TRUE
      # check_file_exists("data_test/silver/visit")
    },
    format = "qs",
    deployment = "main"
  ),

  # ============================================================================
  # Stage 2: Geocoding (DEPENDS ON silver_layer)
  # ============================================================================
  tar_target(
    name = geocoded_visits,
    command = {
      silver_layer
      source(here::here("r", "015_geocode_enrich.R"))
      TRUE
      # check_file_exists("data_test/silver/visit")
    },
    format = "qs",
    deployment = "main"
  ),

 # ============================================================================
  # Stage QA + CLEANING (creates visit_clean)
  # ============================================================================
  tar_target(
    name = qc_and_clean,
    command = {
      geocoded_visits
      source(here::here("r", "07_data_quality.R"))
      check_file_exists("data_test/silver/visit_clean")  # ← Check for cleaned data
    },
    format = "file",
    deployment = "main"
  ),

  # ============================================================================
  # Stage 3: Person-Month Cohort (DEPENDS ON geocoded_visits)
  # ============================================================================
  tar_target(
    name = person_month_cohort,
    command = {
      qc_and_clean
      source(here::here("r", "04_person_monthV2.R"))
      check_file_exists("data_test/gold/person_month")
    },
    format = "file",
    deployment = "main"
  ),

  # ============================================================================
  # Stage 4: Download Exposures
  # ============================================================================
  tar_target(
    name = exposures_downloaded,
    command = {
      person_month_cohort
      source(here::here("r", "02_dataverse_exposures.R"))
      check_file_exists("data_test/gold/exposures_monthly")
    },
    format = "file",
    deployment = "main"
  ),


  # ============================================================================
  # Stage 5: Join Exposures (DEPENDS ON person_month_cohort + exposures_downloaded)
  # ============================================================================
  tar_target(
    name = joined_data,
    command = {
      person_month_cohort
      exposures_downloaded
      source(here::here("r", "05_join_exposures.R"))
      check_file_exists("data_test/gold/person_month_exposures")
    },
    format = "file",
    deployment = "main"
  ),

  # ============================================================================
  # Stage 6: Exposure Rollup (DEPENDS ON joined_data)
  # ============================================================================
  tar_target(
    name = exposure_rollup_complete,
    command = {
      joined_data
      source(here::here("r", "03_exposure_rollup.R"))
      check_file_exists("data_test/gold/exposure_rollup")
    },
    format = "file",
    deployment = "main"
  ),

  # ============================================================================
  # Stage 7a: ExWAS Overall (DEPENDS ON exposure_rollup_complete)
  # ============================================================================
  tar_target(
    name = exwas_overall,
    command = {
      exposure_rollup_complete

      library(arrow)
      library(dplyr)
      library(tidyr)
      library(broom)
      library(yaml)

      pm <- arrow::open_dataset("data_test/gold/person_month") %>% dplyr::collect()
      ex <- arrow::open_dataset("data_test/gold/exposure_rollup") %>%
        dplyr::filter(metric == "mean") %>%
        dplyr::collect()

      ex_wide <- ex %>%
        dplyr::select(person_id, ym, exposure_id, value) %>%
        tidyr::pivot_wider(names_from = exposure_id, values_from = value)

      wide <- pm %>% dplyr::left_join(ex_wide, by = c("person_id", "ym"))

      model_cfg <- yaml::read_yaml(here::here("config", "covariates.yaml"))$exwas_models
      models_overall <- model_cfg[sapply(model_cfg, function(m) {
        grepl("_overall$", m$id) || (is.null(m$strata) || m$strata == "all")
      })]

      source(here::here("r", "06_ewas_enhanced.R"), local = TRUE)

      exposure_cols <- unique(ex$exposure_id)
      outcome_cols <- names(wide)[grepl("_flag$", names(wide))]

      results <- dplyr::bind_rows(
        lapply(models_overall, function(spec) {
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

      arrow::write_parquet(results, "data_test/gold/exwas_overall.parquet")
      "data_test/gold/exwas_overall.parquet"
    },
    format = "file",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "controller_highmem")
    )
  ),

  # 7b: ExWAS Male (DEPENDS ON exposure_rollup_complete)
  tar_target(
    name = exwas_male,
    command = {
      exposure_rollup_complete

      library(arrow)
      library(dplyr)
      library(tidyr)
      library(broom)
      library(yaml)

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

      model_cfg <- yaml::read_yaml(here::here("config", "covariates.yaml"))$exwas_models
      models_male <- model_cfg[sapply(model_cfg, function(m) {
        grepl("_male$", m$id) || (!is.null(m$strata) && m$strata == "male")
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

  # 7c: ExWAS Female (DEPENDS ON exposure_rollup_complete)
  tar_target(
    name = exwas_female,
    command = {
      exposure_rollup_complete  # ← EXPLICIT DEPENDENCY

      library(arrow)
      library(dplyr)
      library(tidyr)
      library(broom)
      library(yaml)

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

      model_cfg <- yaml::read_yaml(here::here("config", "covariates.yaml"))$exwas_models
      models_female <- model_cfg[sapply(model_cfg, function(m) {
        grepl("_female$", m$id) || (!is.null(m$strata) && m$strata == "female")
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
  # Stage 8: Combine Results (DEPENDS ON all 3 ExWAS targets)
  # ============================================================================
  tar_target(
    name = exwas_combined,
    command = {
      exwas_overall  # ← DEPENDENCY 1
      exwas_male     # ← DEPENDENCY 2
      exwas_female   # ← DEPENDENCY 3

      library(arrow)
      library(dplyr)

      overall <- if (file.exists(exwas_overall)) {
        arrow::read_parquet(exwas_overall)
      } else NULL

      male <- if (!is.null(exwas_male) && file.exists(exwas_male)) {
        arrow::read_parquet(exwas_male)
      } else NULL

      female <- if (!is.null(exwas_female) && file.exists(exwas_female)) {
        arrow::read_parquet(exwas_female)
      } else NULL

      all_results <- dplyr::bind_rows(overall, male, female) %>%
        dplyr::arrange(p_value)

      arrow::write_parquet(all_results, "data_test/gold/exwas_all_results.parquet")

      "data_test/gold/exwas_all_results.parquet"
    },
    format = "file",
    deployment = "main"
  ),

  # ============================================================================
  # Stage 9: Quality Checks (DEPENDS ON silver_layer)
  # ============================================================================
  tar_target(
    name = qc_silver,
    command = {
      silver_layer  # ← EXPLICIT DEPENDENCY
      person_month_cohort
      source(here::here("r", "07_data_quality.R"))
      check_file_exists("data_test/gold/quality_checks")
    },
    format = "file",
    deployment = "main"
  ),

  # ============================================================================
  # Stage 10: Final Report (DEPENDS ON exwas_combined)
  # ============================================================================
  tar_target(
    name = final_report,
    command = {
      exwas_combined  # ← EXPLICIT DEPENDENCY

      library(arrow)
      library(dplyr)

      results <- arrow::read_parquet(exwas_combined)

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
    format = "file",
    deployment = "main"
  )
)