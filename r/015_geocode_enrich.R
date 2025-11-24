# r/015_geocode_enrich.R
# Enrich silver visits with Census tract GEOIDs using HUD ZIP-Tract crosswalk API

source(here::here("r", "00_env.R"))

# Download HUD USPS ZIP-Tract crosswalk via authenticated API
download_hud_crosswalk <- function(year = 2024, quarter = 3, cache_dir = paths$am_cache) {
  require(httr)
  require(jsonlite)
  
  dir_create(path(cache_dir, "hud_crosswalk"))
  out_file <- path(cache_dir, "hud_crosswalk", glue("ZIP_TRACT_{year}Q{quarter}.rds"))
  
  if (file_exists(out_file)) {
    message("Using cached crosswalk: ", out_file)
    return(readRDS(out_file))
  }
  
  # Get API key from environment
  api_key <- Sys.getenv("HUD_API_KEY")
  if (nchar(api_key) == 0) {
    stop("HUD_API_KEY not found in .env file. Please add it.")
  }
  
  message("Downloading HUD crosswalk for ", year, " Q", quarter, " via API...")
  
  # Use HUD API - type=5 for ZIP-Tract crosswalk, query=All for all states
  url <- "https://www.huduser.gov/hudapi/public/usps"
  
  tryCatch({
    response <- httr::GET(
      url, 
      query = list(type = 5, query = "All", year = year, quarter = quarter),
      add_headers(Authorization = paste("Bearer", api_key))
    )
    
    # Check for errors
    if (httr::http_error(response)) {
      stop("API request failed with status: ", httr::status_code(response))
    }
    
    # Parse JSON response
    data <- httr::content(response, as = "parsed", type = "application/json")
    
    # Convert to data frame
    df <- data.frame(
      zip = sapply(data$data$results, function(x) x$zip),
      tract = sapply(data$data$results, function(x) x$tract),
      res_ratio = sapply(data$data$results, function(x) as.numeric(x$res_ratio)),
      bus_ratio = sapply(data$data$results, function(x) as.numeric(x$bus_ratio)),
      oth_ratio = sapply(data$data$results, function(x) as.numeric(x$oth_ratio)),
      tot_ratio = sapply(data$data$results, function(x) as.numeric(x$tot_ratio)),
      stringsAsFactors = FALSE
    )
    
    message("Downloaded ",     messag" Z    message("Downloaded 
    # Cache th    # Cache th    # Cache th    # Cache th    # Cache th    # }, error = function(e) {
    stop("Failed to download HUD crosswalk: ", e$message)
  })
}

# Prepare crosswalk for joining
prepare_crosswalk <- function(xw_data) {
  xw <- xw_data %>%
    transmute(
      zip5 = as.character(zip),
      tract_geoid = as.character(tract),
      res_ratio = as.numeric(res_ratio),
      bus_ratio = as.numeric(bus_ratio),
      oth_ratio =      oth_ratio =atio),
      tot_ratio = as.numeric(tot_ratio)
    ) %>%
    # Use residential ratio as primary weight, fallback to total
    mutate(weight = coalesce(res_ratio, tot_ratio)) %>%
    # For each ZIP, take the tract with highest allocation
    group_by(zip5) %>%
    arrange(desc(weight)) %>%
    slice_head(n = 1) %>%
    ungroup()
  
  return(xw)
}

# Main enrichment func# Main enrichment func# Main enrichment func# Main enrichment func# Main enriche("\n=== Enriching visits with tract GEOIDs ===\n")
  
  # Download/load crosswalk
  xw_data <- download_hud_crosswalk(year = year, quarter = quarter)
  xw <- prepare_crosswalk(xw_data)
  
  message("Prepared ", nrow(xw), " unique ZIP→Tract mappings")
  
  # Load visits
  visits <- read_ds(path(paths$silver, "visit")) %>%
    collect()
  
  message("Processing ", format(nrow(visits), big.mark=","), " visits...")
  
  # Join with crosswalk
  visits_enriched <- visits %>%
    left_join(
      xw %>% select(zip5, tract_geoid, weight),
      by = "zip5"
    ) %>%
    select(-weight)
  
  # Report enrichment success
  n_enriched <- sum(!is.na(visits_enriched$tract_geoid))
  pct_enriched <-   pct_enriched <-   pct_enriched <-   pct_enriched <-
  message(glue("\nEnrichment complete:"))
  message(glue("  {format(n_enriched, big.mark=',')} / {format(nrow(visits_enriched), big.mark=',')} visits ({pct_enriched}%) matched to tracts"))
  
  # Write back to silver
  arrow::write_dataset(
    visits_enriched,
    path(paths$silver, "visit"),
    partitioning = c("year"),
    existing_data_behavior = "overwrite"
  )
  
  message("\n✓ Updated silver/visit dataset with tract_geoid column")
  
  invisible(visits_enriched)
}

# Run if called directly
if (!interactive()) {
  enrich_visits_with_tracts()
}
