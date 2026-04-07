# _targets.R - HCUP PEGS Explorer Pipeline
library(targets)
library(tarchetypes)

options(repos = c(CRAN = "https://cloud.r-project.org/"))
source("r/00_env.R")

# ==============================================================================
# SLURM Controller Setup
# ==============================================================================

USE_HPC <- Sys.getenv("USE_HPC", "FALSE") == "TRUE"

if (USE_HPC) {
  library(crew)
  library(crew.cluster)

  # Standard compute jobs (geocoding, data processing)
  controller_normal <- crew.cluster::crew_controller_slurm(
    workers              = 4,
    seconds_idle         = 300,          # replaces launch_max: workers shut down after 5min idle
    seconds_interval     = 30,
    reset_globals        = TRUE,         # moved from launcher to controller
    reset_packages       = TRUE,
    reset_options        = FALSE,
    garbage_collection   = TRUE,
    slurm_log_output     = "logs/slurm_normal_%j.out",
    slurm_log_error      = "logs/slurm_normal_%j.err",
    slurm_partition      = "highmem",
    slurm_cpus_per_task  = 4,
    slurm_memory_gigabytes_per_cpu = 12,
    slurm_time_minutes   = 240,
    script_lines         = "export R_LIBS_USER=~/R/x86_64-pc-linux-gnu-library/4.3",
    verbose              = FALSE
  )

  # High-memory controller for ExWAS / person-month
  controller_highmem <- crew.cluster::crew_controller_slurm(
    workers              = 3,
    seconds_idle         = 300,
    seconds_interval     = 30,
    reset_globals        = TRUE,
    reset_packages       = TRUE,
    reset_options        = FALSE,
    garbage_collection   = TRUE,
    slurm_log_output     = "logs/slurm_highmem_%j.out",
    slurm_log_error      = "logs/slurm_highmem_%j.err",
    slurm_partition      = "highmem",
    slurm_cpus_per_task  = 10,
    slurm_memory_gigabytes_per_cpu = 12,
    slurm_time_minutes   = 720,
    script_lines         = "export R_LIBS_USER=~/R/x86_64-pc-linux-gnu-library/4.3",
    verbose              = FALSE
  )

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
  format             = "file",
  memory             = "transient",
  garbage_collection = TRUE,
  error              = "continue",
  storage            = "worker",
  retrieval          = "worker",
  deployment         = "worker",
  resources = tar_resources(
    crew = tar_resources_crew(controller = "controller_normal")
  )
)

# ==============================================================================
# Helper Functions
# ==============================================================================

check_file_exists <- function(path) {
  if (dir.exists(path) || file.exists(path)) {
    return(as.character(fs::path_abs(path)))   # always return absolute path
  } else {
    stop("Path does not exist: ", path)
  }
}

# ==============================================================================
# Pipeline
# ==============================================================================

list(

  # --------------------------------------------------------------------------
  # Stage 0: Harmonize 2015 quarterly files
  # --------------------------------------------------------------------------
  tar_target(
    name = harmonize_2015,
    command = {
      source(here::here("r", "harmonization_2015.R"))
      list.files(
        paths$bronze,
        pattern   = "_2015_COMBINED\\.parquet$",
        full.names = TRUE
      )
    },
    format     = "file",
    deployment = "main"
  ),

  # --------------------------------------------------------------------------
  # Stage 0b: Index all bronze parquet files
  # --------------------------------------------------------------------------
  tar_target(
    name = bronze_files,
    command = {
      harmonize_2015
      list.files(
        paths$bronze,
        pattern   = "\\.parquet$",
        recursive = TRUE,
        full.names = TRUE
      ) %>%
        .[!grepl("2015.*(q1q3|q4)", .)]
    },
    format     = "file",
    deployment = "main"
  ),

  # --------------------------------------------------------------------------
  # Stage 1: Silver layer
  # --------------------------------------------------------------------------
  tar_target(
    name = silver_layer,
    command = {
      bronze_files
      source(here::here("r", "01_hcup_silver.R"))
      check_file_exists(fs::path(paths$silver, "visit"))
    },
    format     = "file",
    deployment = "main"
  ),

  # --------------------------------------------------------------------------
  # Stage 2: Geocoding
  # --------------------------------------------------------------------------
  tar_target(
    name = geocoded_visits,
    command = {
      silver_layer
      source(here::here("r", "015_geocode_enrich.R"))
      check_file_exists(fs::path(paths$silver, "visit"))
    },
    format     = "file",
    deployment = "main"
  ),

  # --------------------------------------------------------------------------
  # Stage 3: QC + cleaning  →  visit_clean
  # --------------------------------------------------------------------------
  tar_target(
    name = qc_and_clean,
    command = {
      geocoded_visits
      source(here::here("r", "07_data_quality.R"))
      check_file_exists(fs::path(paths$silver, "visit_clean"))
    },
    format     = "file",
    deployment = "main"
  ),

  # --------------------------------------------------------------------------
  # Stage 4: Person-month cohort  (high-memory worker)
  # --------------------------------------------------------------------------
  tar_target(
    name = person_month_cohort,
    command = {
      qc_and_clean
      source(here::here("r", "04_person_monthV2.R"))
      check_file_exists(fs::path(paths$gold, "person_month"))
    },
    format    = "file",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "controller_highmem")
    )
  ),

  # --------------------------------------------------------------------------
  # Stage 5: Download exposures
  # --------------------------------------------------------------------------
  tar_target(
    name = exposures_downloaded,
    command = {
      person_month_cohort
      source(here::here("r", "02_dataverse_exposures.R"))
      check_file_exists(fs::path(paths$gold, "exposures_monthly"))
    },
    format     = "file",
    deployment = "main"
  ),

  # --------------------------------------------------------------------------
  # Stage 6: Join exposures
  # --------------------------------------------------------------------------
  tar_target(
    name = joined_data,
    command = {
      person_month_cohort
      exposures_downloaded
      source(here::here("r", "05_join_exposures.R"))
      check_file_exists(fs::path(paths$gold, "person_month_exposures"))
    },
    format     = "file",
    deployment = "main"
  ),

  # --------------------------------------------------------------------------
  # Stage 7: Exposure rollup
  # --------------------------------------------------------------------------
  tar_target(
    name = exposure_rollup_complete,
    command = {
      joined_data
      source(here::here("r", "03_exposure_rollup.R"))
      check_file_exists(fs::path(paths$gold, "exposure_rollup"))
    },
    format     = "file",
    deployment = "main"
  )
)