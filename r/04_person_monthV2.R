# r/40_person_month.R (Enhanced)
# Build person-month cohort with multiple phenotype flags

source(here::here("r", "00_env.R"))

# Load phenotype definitions
pheno_cfg <- yaml::read_yaml(here::here("config", "covariates.yaml"))$phenotypes

# Function to check if diagnosis matches phenotype
dx_matches_phenotype <- function(dx_codes, icd10_prefixes) {
  if (is.na(dx_codes) || dx_codes == "") return(FALSE)

  # Split concatenated codes
  codes <- str_split(dx_codes, ";")[[1]]

  # Check if any code starts with any of the prefixes
  any(sapply(codes, function(code) {
    any(sapply(icd10_prefixes, function(prefix) {
      startsWith(code, prefix)
    }))
  }))
}

# Vectorized version
create_phenotype_flags <- function(df, phenotypes) {
  for (pheno_name in names(phenotypes)) {
    pheno_def <- phenotypes[[pheno_name]]

    flag_col <- paste0(pheno_name, "_flag")

    df[[flag_col]] <- sapply(df$dx_primary, function(dx) {
      if (is.na(dx) || dx == "") return(FALSE)
      any(sapply(pheno_def$icd10_prefixes, function(prefix) {
        startsWith(dx, prefix)
      }))
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
      ym = admit_date,  # already first-of-month from silver processing
      zip5,
      tract_geoid,
      n_visits = 1L,
      across(all_of(pheno_cols), ~.)  # carry through all phenotype flags
    ) %>%
    group_by(person_id, ym) %>%
    summarise(
      # Take last non-missing geography
      zip5 = last(na.omit(zip5)),
      tract_geoid = last(na.omit(tract_geoid)),

      # Sum visits
      n_visits = sum(n_visits, na.rm = TRUE),

      # Any() across all phenotype flags
      across(all_of(pheno_cols), ~any(., na.rm = TRUE)),

      .groups = "drop"
    ) %>%
    mutate(
      year = lubridate::year(ym),
      month = lubridate::month(ym)
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