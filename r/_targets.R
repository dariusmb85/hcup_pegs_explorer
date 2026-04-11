# _targets.R - HCUP PEGS Explorer Pipeline
library(targets)
library(tarchetypes)
library(crew)
library(crew.cluster)

options(repos = c(CRAN = "https://cloud.r-project.org/"))
source("r/00_env.R")

# ==============================================================================
# SLURM Controllers (crew.cluster >= 0.4.0 API)
# SLURM-specific args now go in options_cluster = crew_options_slurm()
# ==============================================================================

controller_normal <- crew.cluster::crew_controller_slurm(
  name                       = "controller_normal",
  workers                    = 4,
  seconds_interval           = 30,
  seconds_idle               = 300,
  seconds_launch             = 86400,
  reset_globals              = TRUE,
  reset_packages             = TRUE,
  reset_options              = FALSE,
  garbage_collection         = TRUE,
  options_cluster            = crew.cluster::crew_options_slurm(
    verbose                  = FALSE,
    script_lines             = "export R_LIBS_USER=~/R/x86_64-pc-linux-gnu-library/4.3",
    log_output               = "logs/slurm_normal_%j.out",
    log_error                = "logs/slurm_normal_%j.err",
    partition                = "highmem",
    cpus_per_task            = 4,
    memory_gigabytes_per_cpu = 12,
    time_minutes             = 480
  )
)

controller_highmem <- crew.cluster::crew_controller_slurm(
  name                       = "controller_highmem",
  workers                    = 2,
  seconds_interval           = 30,
  seconds_idle               = 300,
  seconds_launch             = 86400,
  reset_globals              = TRUE,
  reset_packages             = TRUE,
  reset_options              = FALSE,
  garbage_collection         = TRUE,
  options_cluster            = crew.cluster::crew_options_slurm(
    verbose                  = FALSE,
    script_lines             = "export R_LIBS_USER=~/R/x86_64-pc-linux-gnu-library/4.3",
    log_output               = "logs/slurm_highmem_%j.out",
    log_error                = "logs/slurm_highmem_%j.err",
    partition                = "highmem",
    cpus_per_task            = 8,
    memory_gigabytes_per_cpu = 16,
    time_minutes             = 720
  )
)

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
  controller         = crew::crew_controller_group(
    controller_normal,
    controller_highmem
  ),
  resources = tar_resources(
    crew = tar_resources_crew(controller = "controller_normal")
  )
)

# ==============================================================================
# Helper
# ==============================================================================

check_file_exists <- function(path) {
  p <- as.character(fs::path_abs(path))
  if (dir.exists(p) || file.exists(p)) {
    return(p)
  } else {
    stop("Path does not exist: ", p)
  }
}

# ==============================================================================
# Pipeline
# ==============================================================================

list(

  # --------------------------------------------------------------------------
  # Stage 0a: Harmonize 2015 quarterly files
  # --------------------------------------------------------------------------
  tar_target(
    name = harmonize_2015,
    command = {
      source(here::here("r", "harmonization_2015.R"))
      list.files(
        paths$bronze,
        pattern    = "_2015_COMBINED\\.parquet$",
        full.names = TRUE
      )
    },
    format     = "file",
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
        pattern    = "\\.parquet$",
        recursive  = TRUE,
        full.names = TRUE
      ) %>%
        .[!grepl("2015.*(q1q3|q4)", .)]
    },
    format     = "file",
  ),

  # --------------------------------------------------------------------------
  # Stage 1: Silver layer  (heavy I/O — main process)
  # --------------------------------------------------------------------------
  tar_target(
    name = silver_layer,
    command = {
      bronze_files
      source(here::here("r", "01_hcup_silver.R"))
      check_file_exists(fs::path(paths$silver, "visit"))
    },
    format     = "file",
  ),

  # --------------------------------------------------------------------------
  # Stage 2: Geocoding  (heavy I/O — main process)
  # --------------------------------------------------------------------------
  tar_target(
    name = geocoded_visits,
    command = {
      silver_layer
      source(here::here("r", "015_geocode_enrich.R"))
      check_file_exists(fs::path(paths$silver, "visit"))
    },
    format     = "file",
  ),

  # --------------------------------------------------------------------------
  # Stage 3: QC + cleaning  (heavy I/O — main process)
  # --------------------------------------------------------------------------
  tar_target(
    name = qc_and_clean,
    command = {
      geocoded_visits
      source(here::here("r", "07_data_quality.R"))
      check_file_exists(fs::path(paths$silver, "visit_clean"))
    },
    format     = "file",
  ),

  # --------------------------------------------------------------------------
  # Stage 4: Person-month cohort  (high-memory SLURM worker)
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
  # Stage 5: Download exposures  (normal worker — network bound)
  # --------------------------------------------------------------------------
  tar_target(
    name = exposures_downloaded,
    command = {
      person_month_cohort
      source(here::here("r", "02_dataverse_exposures.R"))
      check_file_exists(fs::path(paths$gold, "exposures_monthly"))
    },
    format = "file"
  ),

  # --------------------------------------------------------------------------
  # Stage 6: Join exposures  (normal worker)
  # --------------------------------------------------------------------------
  tar_target(
    name = joined_data,
    command = {
      person_month_cohort
      exposures_downloaded
      source(here::here("r", "05_join_exposures.R"))
      check_file_exists(fs::path(paths$gold, "person_month_exposures"))
    },
    format = "file"
  ),

  # --------------------------------------------------------------------------
  # Stage 7: Exposure rollup  (normal worker)
  # --------------------------------------------------------------------------
  tar_target(
    name = exposure_rollup_complete,
    command = {
      joined_data
      source(here::here("r", "03_exposure_rollup.R"))
      check_file_exists(fs::path(paths$gold, "exposure_rollup"))
    },
    format = "file"
  )
)