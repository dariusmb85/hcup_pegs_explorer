# r/015_geocode_enrich.R
# Enrich silver visits with Census tract GEOIDs using HUD ZIP-Tract crosswalk

source(here::here("r", "00_env.R"))

# Download HUD USPS ZIP-Tract crosswalk via API
download_hud_crosswalk <- function(year = 2024, quarter = 3, cache_dir = paths$am_cache) {
  dir_create(path(cache_dir, "hud_crosswalk"))
  out_file <- path(cache_dir, "hud_crosswalk", glue("ZIP_TRACT_{year}Q{quarter}.csv"))
  
  if (file_exists(out_file)) {
    message("Using cached crosswalk: ", out_file)
    return(out_file)
  }
  
  # Use HUD API
  url <- glue("https://www.huduser.gov/hudapi/public/usps?type=5&query=All&year={year}&quarter={quarter}")
  message("Downloading HUD crosswalk for ", year, " Q", quarter, "...")
  
  tryCatch({
    download.file(url, out_file, mode = "wb", quiet = FALSE)
    return(out_file)
  }, error = function(e) {
    warning("Failed to download ", year, " Q", quarter, " crosswalk: ", e$message)
    return(NULL)
  })
}

# Load and prepare crosswalk
prepare_crosswalk <- function(xwalk_file) {
  require(readr)
  
  xw <- read_csv(xwalk_file, show_col_types = FALSE) %>%
    janitor::clean_names() %>%
    transmute(
      zip5 = as.character(zip),
      tract_geoid = as.characte      tract_geoid = as.characte      tract_geoid = as.characte      tract_geoid = as.chao),
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
enrich_visits_with_tracts <- function(year = 2024, quarter = 3) {
  message("\n=== Enriching visits with tract GEOIDs ===\n")
  
  # Download/load crosswalk
  xwalk_file <- download_hud_crosswalk(year = year, quarter = quarter)
  i  i  i  i  i  i  i  i  i      stop("Could not obtain crosswalk file")
  }
  
  xw <- prepare_  xw <- prepare_  xw <- presage("Loaded ", nrow(xw), " ZIP-Tract mappings")
  
  # Load visits
  visits <- read_ds(path(paths$silver, "  visits <- read_ds(path(paths$silver, "  visits <- read_ds(path(paths$silver, "  visits <- read_ds(path(paths$silver, "mar  visits <- read_ds(path(pathsits_enriched <- visits %>%
    left_join(
      xw %>%
        group_by(zip5) %>%
        slice_head(n = 1) %>%
        select        select        select        select        sel%>%
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
    existing_data_behavior = "overwrite"
  )
  
  message("\nUpdated silver/visit dataset")
  
  invisible(visits_enriched)
}

# Run if called directly
if (!interactive()) {
  enrich_visits_with_tracts()
}
