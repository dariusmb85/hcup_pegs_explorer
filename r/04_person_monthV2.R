# r/40_person_month.R (Enhanced)
# Build person-month cohort with multiple phenotype flags

source(here::here("r", "00_env.R"))

# Load phenotype definitions
pheno_cfg <- yaml::read_yaml(here::here("config", "covariates.yaml"))$phenotypes

# Function to check if diagnosis matches phenotype
dx_matches_phenotype <- function(dx_codes, icd9_prefixes = NULL, icd10_prefixes = NULL) {
  if (is.null(dx_codes) || all(is.na(dx_codes))) return(FALSE)

  # Check ICD-9 codes (numeric, 3-5 digits)
  icd9_match <- FALSE
  if (!is.null(icd9_prefixes) && length(icd9_prefixes) > 0) {
    icd9_match <- any(sapply(icd9_prefixes, function(prefix) {
      grepl(paste0("^", prefix), dx_codes)
    }))
  }

  # Check ICD-10 codes (alphanumeric, starts with letter)
  icd10_match <- FALSE
  if (!is.null(icd10_prefixes) && length(icd10_prefixes) > 0) {
    icd10_match <- any(sapply(icd10_prefixes, function(prefix) {
      grepl(paste0("^", prefix), dx_codes)
    }))
  }

  return(icd9_match || icd10_match)
}

# Vectorized version
create_phenotype_flags <- function(df, phenotypes) {
  for (pheno_name in names(phenotypes)) {
    pheno_def <- phenotypes[[pheno_name]]

    flag_col <- paste0(pheno_name, "_flag")

    df[[flag_col]] <- sapply(df$dx_primary, function(dx) {
      dx_matches_phenotype(dx, pheno_def$icd9_prefixes, pheno_def$icd10_prefixes)
    })

    message(glue("Created {flag_col}: {sum(df[[flag_col]], na.rm=TRUE)} positive cases"))
  }

  return(df)
}

main <- function() {
  message("\n=== Building person-month cohort ===\n")

  # Load visits
  vis <- read_ds(path(paths$silver, "visit"))

  message("Creating phenotype flags...")
  vis_collected <- vis %>% collect()

  # Add phenotype flags
  vis_flagged <- create_phenotype_flags(vis_collected, pheno_cfg)

  # Get all phenotype flag columns
  pheno_cols <- names(vis_flagged)[str_detect(names(vis_flagged), "_flag$")]

  message(glue("Processing {nrow(vis_flagged)} visits into person-months..."))

  # Build person-month records
  pm <- vis_flagged %>%
    transmute(
      person_id,
      ym = admit_date,
      zip5,
      tract_geoid,
      n_visits = 1L,
      db_type,  # ADDED
      across(all_of(pheno_cols), ~.)
    ) %>%
    group_by(person_id, ym) %>%
    summarise(
      zip5 = last(na.omit(zip5)),
      tract_geoid = last(na.omit(tract_geoid)),
      n_visits = sum(n_visits, na.rm = TRUE),
      # Aggregate db_type (comma-separated if multiple visit types)
      db_type = paste(unique(na.omit(db_type)), collapse=","),  # ADDED
      across(all_of(pheno_cols), ~any(., na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    mutate(
      year = lubridate::year(ym),
      month = lubridate::month(ym),
      # Add season variable
      season = case_when(
        month %in% c(12, 1, 2) ~ "winter",
        month %in% c(3, 4, 5) ~ "spring",
        month %in% c(6, 7, 8) ~ "summer",
        month %in% c(9, 10, 11) ~ "fall"
      )  # ADDED
    )

  # Report summary
  n_persons <- n_distinct(pm$person_id)
  n_person_months <- nrow(pm)
  date_range <- range(pm$ym)

  message("\n=== Cohort Summary ===")
  message(glue("Unique persons: {scales::comma(n_persons)}"))
  message(glue("Person-months: {scales::comma(n_person_months)}"))
  message(glue("Date range: {date_range[1]} to {date_range[2]}"))
  message(glue("Avg visits/person-month: {round(mean(pm$n_visits), 2)}"))

  # Phenotype prevalence
  message("\n=== Phenotype Prevalence ===")
  for (col in pheno_cols) {
    n_pos <- sum(pm[[col]], na.rm = TRUE)
    pct <- round(100 * n_pos / nrow(pm), 2)
    message(glue("  {col}: {scales::comma(n_pos)} ({pct}%)"))
  }

  # Geography coverage
  n_with_tract <- sum(!is.na(pm$tract_geoid))
  pct_tract <- round(100 * n_with_tract / nrow(pm), 1)
  message(glue("\nGeocoding: {scales::comma(n_with_tract)} / {scales::comma(nrow(pm))} ({pct_tract}%) with tract"))

  # Write out
  message("\nWriting person-month dataset...")
  write_parquet_ds(
    pm,
    path(paths$gold, "person_month"),
    partitioning = c("year")
  )

  message("✓ Complete\n")

  invisible(pm)
}

# Run if called directly
if (!interactive()) {
  main()
}