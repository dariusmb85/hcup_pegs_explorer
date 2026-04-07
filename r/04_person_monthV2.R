# r/04_person_month.R (Enhanced)
# Build person-month cohort with multiple phenotype flags
# Processes state-by-state to avoid memory exhaustion on large datasets

source(here::here("r", "00_env.R"))

# Load phenotype definitions
pheno_cfg <- yaml::read_yaml(here::here("config", "covariates.yaml"))$phenotypes

# Function to check if PheCodes match phenotype
phecode_matches_phenotype <- function(phecode_values, target_phecodes) {
  if (is.null(phecode_values) || all(is.na(phecode_values))) return(FALSE)
  any(phecode_values %in% target_phecodes, na.rm = TRUE)
}

create_phenotype_flags <- function(df, phenotypes) {
  for (pheno_name in names(phenotypes)) {
    pheno_def <- phenotypes[[pheno_name]]
    flag_col <- paste0(pheno_name, "_flag")

    df[[flag_col]] <- sapply(df$dx_primary_phecode, function(phecode) {
      !is.na(phecode) && phecode %in% pheno_def$phecodes
    })
  }
  return(df)
}

# Core person-month builder — operates on a single in-memory data frame
create_person_months <- function(vis_collected, pheno_cfg) {

  # Add phenotype flags
  vis_flagged <- create_phenotype_flags(vis_collected, pheno_cfg)

  # Get all phenotype flag columns
  pheno_cols <- names(vis_flagged)[str_detect(names(vis_flagged), "_flag$")]

  # Build person-month records
  pm <- vis_flagged %>%
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
      zip5            = last(na.omit(zip5)),
      tract_geoid     = last(na.omit(tract_geoid)),
      n_visits        = sum(n_visits, na.rm = TRUE),
      db_type         = paste(unique(na.omit(db_type)), collapse = ","),
      facility_state  = first(facility_state),
      age             = first(na.omit(age)),
      female          = first(na.omit(female)),
      race            = first(na.omit(race)),
      across(all_of(pheno_cols), ~any(., na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    mutate(
      year  = lubridate::year(ym),
      month = lubridate::month(ym),
      season = case_when(
        month %in% c(12, 1, 2) ~ "winter",
        month %in% c(3, 4, 5)  ~ "spring",
        month %in% c(6, 7, 8)  ~ "summer",
        month %in% c(9, 10, 11) ~ "fall"
      )
    )

  return(pm)
}

main <- function() {
  message("\n=== Building person-month cohort (chunked by state) ===\n")

  # Open dataset without collecting — used only for metadata queries
  vis_ds <- read_ds(path(paths$silver, "visit_clean"))

  # DEBUG: Check available columns from a small sample
  sample_cols <- vis_ds %>% head(1) %>% collect()
  cat("Available columns:\n")
  print(names(sample_cols))
  cat("Columns with 'phecode':\n")
  print(names(sample_cols)[grepl("phecode", names(sample_cols), ignore.case = TRUE)])

  # Get list of states to iterate over
  states <- vis_ds %>%
    select(facility_state) %>%
    distinct() %>%
    collect() %>%
    pull(facility_state) %>%
    sort()

  message(glue("Found {length(states)} state(s) to process: {paste(states, collapse = ', ')}"))

  # Accumulators for summary stats across all states
  total_persons       <- 0L
  total_person_months <- 0L
  date_range_all      <- c(Inf, -Inf)
  pheno_totals        <- list()

  # ── Main loop ────────────────────────────────────────────────────────────────
  for (state in states) {
    message(glue("\n--- Processing state: {state} ---"))

    # Load only this state's visits into memory
    vis_state <- vis_ds %>%
      filter(facility_state == !!state) %>%
      collect()

    message(glue("  Loaded {scales::comma(nrow(vis_state))} visits"))

    if (nrow(vis_state) == 0L) {
      message("  (no visits — skipping)")
      next
    }

    # Build person-months for this state
    pm_state <- create_person_months(vis_state, pheno_cfg)

    # Accumulate summary stats
    total_persons       <- total_persons + n_distinct(pm_state$person_id)
    total_person_months <- total_person_months + nrow(pm_state)
    date_range_all      <- c(
      min(date_range_all[1], min(pm_state$ym, na.rm = TRUE)),
      max(date_range_all[2], max(pm_state$ym, na.rm = TRUE))
    )

    pheno_cols <- names(pm_state)[str_detect(names(pm_state), "_flag$")]
    for (col in pheno_cols) {
      pheno_totals[[col]] <- (pheno_totals[[col]] %||% 0L) +
        sum(pm_state[[col]], na.rm = TRUE)
    }

    # Write this state's data — appending to the partitioned dataset
    message(glue("  Writing {scales::comma(nrow(pm_state))} person-months..."))
    write_parquet_ds(
      pm_state,
      path(paths$gold, "person_month"),
      partitioning = c("facility_state", "db_type", "year"),
      existing_data_behavior = "delete_matching"
    )

    message(glue("  ✓ {state} complete"))

    # Free memory before next iteration
    rm(vis_state, pm_state)
    gc()
  }

  # ── Final summary ─────────────────────────────────────────────────────────
  message("\n=== Cohort Summary (all states) ===")
  message(glue("Unique persons (approx): {scales::comma(total_persons)}"))
  message(glue("Person-months:           {scales::comma(total_person_months)}"))
  message(glue("Date range:              {date_range_all[1]} to {date_range_all[2]}"))

  if (length(pheno_totals) > 0) {
    message("\n=== Phenotype Prevalence (all states) ===")
    for (col in names(pheno_totals)) {
      n_pos <- pheno_totals[[col]]
      pct   <- round(100 * n_pos / total_person_months, 2)
      message(glue("  {col}: {scales::comma(n_pos)} ({pct}%)"))
    }
  }

  message("\n✓ All states complete\n")
  invisible(NULL)
}

# Run if called directly
if (!interactive()) {
  main()
}