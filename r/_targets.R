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
    slurm_partition = "highmem",  # Changed: Use highmem as default
    slurm_cpus_per_task = 4,
    slurm_memory_gigabytes_per_cpu = 12,  # Changed: Use proper memory specification
    slurm_time_minutes = 240,
    script_lines = ("export R_LIBS_USER=~/R/x86_64-pc-linux-gnu-library/4.3"),
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
    slurm_cpus_per_task = 10,  # Changed: Even number, follows 12GB/CPU rule
    slurm_memory_gigabytes_per_cpu = 12,  # Changed: Use default highmem ratio
    slurm_time_minutes = 720,
    script_lines = ("export R_LIBS_USER=~/R/x86_64-pc-linux-gnu-library/4.3"),
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
  ),
  debug = "logs/targets_debug.txt",
  workspaces = "logs/targets_workspaces/"
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
      check_file_exists("data_test/silver/visit")
    },
    format = "file",
    deployment = "main"
    #resources = tar_resources(
    #  crew = tar_resources_crew(controller = "controller_normal")
    #)
  ),

  # ============================================================================
  # Stage 2: Geocoding (DEPENDS ON silver_layer)
  # ============================================================================
  tar_target(
    name = geocoded_visits,
    command = {
      silver_layer
      source(here::here("r", "015_geocode_enrich.R"))
      check_file_exists("data_test/silver/visit")
    },
    format = "file",
    deployment = "main"
    #resources = tar_resources(
    #  crew = tar_resources_crew(controller = "controller_normal")
    #)
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
    resources = tar_resources(
      crew = tar_resources_crew(controller = "controller_highmem")
    )
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
      system("Rscript r/06_ewas_enhanced.R all")
      "data_test/gold/exwas_all.parquet"
    },
    format = "file",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "controller_highmem"))
  ),
  # ============================================================================
  # Stage 7b: ExWAS Male (DEPENDS ON exposure_rollup_complete)
  # ============================================================================
  tar_target(
    name = exwas_male,
    command = {
      exposure_rollup_complete
      system("Rscript r/06_ewas_enhanced.R male")
      "data_test/gold/exwas_male.parquet"
    },
    format = "file",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "controller_highmem"))
  ),

  # ============================================================================
  # Stage 7c: ExWAS Female (DEPENDS On exposure_rollup_complete)
  # ============================================================================
  tar_target(
    name = exwas_female,
    command = {
      exposure_rollup_complete
      system("Rscript r/06_ewas_enhanced.R female")
      "data_test/gold/exwas_female.parquet"
    },
    format = "file",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "controller_highmem"))
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
  # Stage 09: Final Report (DEPENDS ON exwas_combined)
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