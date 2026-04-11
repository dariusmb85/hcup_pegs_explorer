# r/04_person_monthV2.R (Enhanced)
# Build person-month cohort with multiple phenotype flags
# Processes state+year chunks in parallel to avoid memory exhaustion

source(here::here("r", "00_env.R"))

# Load phenotype definitions
pheno_cfg <- yaml::read_yaml(here::here("config", "covariates.yaml"))$phenotypes

`%||%` <- function(x, y) if (!is.null(x)) x else y

create_phenotype_flags <- function(df, phenotypes) {
  for (pheno_name in names(phenotypes)) {
    pheno_def <- phenotypes[[pheno_name]]
    flag_col  <- paste0(pheno_name, "_flag")
    df[[flag_col]] <- !is.na(df$dx_primary_phecode) &
      df$dx_primary_phecode %in% pheno_def$phecodes
  }
  return(df)
}

# Core person-month builder — operates on a single in-memory data frame
create_person_months <- function(vis_collected, pheno_cfg) {

  # Add phenotype flags
  vis_flagged <- create_phenotype_flags(vis_collected, pheno_cfg)
  pheno_cols  <- names(vis_flagged)[stringr::str_detect(names(vis_flagged), "_flag$")]

   # Build person-month records
  vis_flagged %>%
    transmute(
      person_id,
      ym = admit_date,
      zip5,
      tract_geoid,
      n_visits = 1L,
      db_type,
      facility_state,
      age,
      female,
      race,
      across(all_of(pheno_cols), ~.)
    ) %>%
    group_by(person_id, ym) %>%
    summarise(
      zip5           = last(na.omit(zip5)),
      tract_geoid    = last(na.omit(tract_geoid)),
      n_visits       = sum(n_visits, na.rm = TRUE),
      db_type        = paste(unique(na.omit(db_type)), collapse = ","),
      facility_state = first(facility_state),
      age            = first(na.omit(age)),
      female         = first(na.omit(female)),
      race           = first(na.omit(race)),
      across(all_of(pheno_cols), ~any(., na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    mutate(
      year   = lubridate::year(ym),
      month  = lubridate::month(ym),
      season = case_when(
        month %in% c(12, 1, 2)  ~ "winter",
        month %in% c(3, 4, 5)   ~ "spring",
        month %in% c(6, 7, 8)   ~ "summer",
        month %in% c(9, 10, 11) ~ "fall"
      )
    )
}

# ── Per-chunk worker (called in parallel) ─────────────────────────────────────
# vis_ds is re-opened inside the worker from a path string because Arrow dataset
# objects cannot be serialized across furrr worker processes
process_one_chunk <- function(chunk_row, vis_ds_path, paths, pheno_cfg) {
  state <- chunk_row$facility_state
  yr    <- chunk_row$year

  message(glue::glue("  Processing {state}/{yr}"))

  vis_ds <- arrow::open_dataset(vis_ds_path)

  vis_chunk <- vis_ds %>%
    dplyr::filter(facility_state == !!state, year == !!yr) %>%
    dplyr::collect()

  if (nrow(vis_chunk) == 0L) {
    message(glue::glue("  (no visits for {state}/{yr} — skipping)"))
    return(NULL)
  }

  pm_chunk <- create_person_months(vis_chunk, pheno_cfg)

  arrow::write_dataset(
    pm_chunk,
    fs::path(paths$gold, "person_month"),
    partitioning           = c("facility_state", "db_type", "year"),
    existing_data_behavior = "delete_matching"
  )

  message(glue::glue("  ✓ {state}/{yr} complete ({scales::comma(nrow(pm_chunk))} person-months)"))

  list(
    n_persons       = dplyr::n_distinct(pm_chunk$person_id),
    n_person_months = nrow(pm_chunk),
    min_date        = min(pm_chunk$ym, na.rm = TRUE),
    max_date        = max(pm_chunk$ym, na.rm = TRUE)
  )
}

main <- function() {
  message("\n=== Building person-month cohort (parallel state+year chunks) ===\n")

  vis_ds     <- read_ds(path(paths$silver, "visit_clean"))
  vis_ds_path <- as.character(fs::path(paths$silver, "visit_clean"))

  # Get all state+year combinations present in the data
  chunks <- vis_ds %>%
    dplyr::select(facility_state, year) %>%
    dplyr::distinct() %>%
    dplyr::collect() %>%
    dplyr::arrange(facility_state, year) %>%
    dplyr::filter(!is.na(facility_state), !is.na(year))

  message(glue("Found {nrow(chunks)} state+year chunk(s) to process"))

  # Split into list of single-row data frames for furrr
  chunk_list <- split(chunks, seq_len(nrow(chunks)))

  n_cores <- min(length(chunk_list), parallel::detectCores() - 1L, 8L)
  message(glue("Processing {length(chunk_list)} chunks using {n_cores} workers\n"))

  library(furrr)
  plan(multisession, workers = n_cores)

  results <- furrr::future_map(
    chunk_list,
    process_one_chunk,
    vis_ds_path = vis_ds_path,
    paths       = paths,
    pheno_cfg   = pheno_cfg,
    .options    = furrr_options(seed = TRUE)
  )

  plan(sequential)

  # ── Final summary ────────────────────────────────────────────────────────────
  results <- Filter(Negate(is.null), results)

  total_persons       <- sum(sapply(results, `[[`, "n_persons"))
  total_person_months <- sum(sapply(results, `[[`, "n_person_months"))
  min_date            <- min(sapply(results, `[[`, "min_date"))
  max_date            <- max(sapply(results, `[[`, "max_date"))

  message("\n=== Cohort Summary (all states) ===")
  message(glue("Unique persons (approx): {scales::comma(total_persons)}"))
  message(glue("Person-months:           {scales::comma(total_person_months)}"))
  message(glue("Date range:              {as.Date(min_date, origin='1970-01-01')} to {as.Date(max_date, origin='1970-01-01')}"))

  message("\n✓ All states complete\n")
  invisible(NULL)
}

if (!interactive()) main()