# r/15_geocode_enrich.R
# Enrich silver visits with Census tract GEOIDs using HUD ZIP-Tract crosswalk

source(here::here("r", "00_env.R"))

# Download HUD USPS ZIP-Tract crosswalk if needed
download_hud_crosswalk <- function(year = 2023, cache_dir = paths$am_cache) {
  dir_create(path(cache_dir, "hud_crosswalk"))
  out_file <- path(cache_dir, "hud_crosswalk", glue("ZIP_TRACT_{year}Q4.xlsx"))

  if (file_exists(out_file)) {
    message("Using cached crosswalk: ", out_file)
    return(out_file)
  }

  url <- glue("https://www.huduser.gov/portal/datasets/usps/ZIP_TRACT_{year}Q4.xlsx")
  message("Downloading HUD crosswalk for ", year, "...")

  tryCatch({
    download.file(url, out_file, mode = "wb", quiet = FALSE)
    return(out_file)
  }, error = function(e) {
    warning("Failed to download ", year, " crosswalk: ", e$message)
    return(NULL)
  })
}

# Load and prepare crosswalk
prepare_crosswalk <- function(xwalk_file) {
  require(readxl)

  xw <- read_excel(xwalk_file) %>%
    janitor::clean_names() %>%
    transmute(
      zip5 = as.character(zip),
      tract_geoid = as.character(tract),
      res_ratio = as.numeric(res_ratio),
      bus_ratio = as.numeric(bus_ratio),
      oth_ratio = as.numeric(oth_ratio),
      tot_ratio = as.numeric(tot_ratio)
    ) %>%
    # Use residential ratio as primary weight, fallback to total
    mutate(weight = coalesce(res_ratio, tot_ratio)) %>%
    # For each ZIP, take the tract(s) with highest allocation
    group_by(zip5) %>%
    arrange(desc(weight)) %>%
    slice_head(n = 3) %>%  # Keep top 3 tracts per ZIP
    ungroup()

  return(xw)
}

# Main enrichment function
enrich_visits_with_tracts <- function(year = 2023) {
  message("\n=== Enriching visits with tract GEOIDs ===\n")

  # Download/load crosswalk
  xwalk_file <- download_hud_crosswalk(year = year)
  if (is.null(xwalk_file)) {
    stop("Could not obtain crosswalk file")
  }

  xw <- prepare_crosswalk(xwalk_file)
  message("Loaded ", nrow(xw), " ZIP-Tract mappings")

  # Load visits
  visits <- read_ds(path(paths$silver, "visit")) %>%
    collect()

  message("Processing ", nrow(visits), " visits...")

  # Join with crosswalk - take primary tract (highest weight)
  visits_enriched <- visits %>%
    left_join(
      xw %>%
        group_by(zip5) %>%
        slice_head(n = 1) %>%
        select(zip5, tract_geoid, weight),
      by = "zip5"
    ) %>%
    select(-weight)

  # Report enrichment success
  n_enriched <- sum(!is.na(visits_enriched$tract_geoid))
  pct_enriched <- round(100 * n_enriched / nrow(visits_enriched), 1)

  message(glue("\nEnrichment complete:"))
  message(glue("  {n_enriched} / {nrow(visits_enriched)} visits ({pct_enriched}%) matched to tracts"))

  # Write back to silver
  arrow::write_dataset(
    visits_enriched,
    path(paths$silver, "visit"),
    partitioning = c("year"),
    existing_data_behavior = "overwrite_or_ignore"
  )

  message("\nUpdated silver/visit dataset")

  invisible(visits_enriched)
}

# Run if called directly
if (!interactive()) {
  enrich_visits_with_tracts()
}