source(here::here("r", "00_env.R"))

download_hud_crosswalk <- function(year = 2024, quarter = 3, cache_dir = paths$am_cache) {
  require(httr)
  require(jsonlite)
  
  dir_create(path(cache_dir, "hud_crosswalk"))
  out_file <- path(cache_dir, "hud_crosswalk", glue("ZIP_TRACT_{year}Q{quarter}.rds"))
  
  if (file_exists(out_file)) {
    message("Using cached crosswalk")
    return(readRDS(out_file))
  }
  
  api_key <- Sys.getenv("HUD_API_KEY")
  if (nchar(api_key) == 0) stop("HUD_API_KEY not found")
  
  message("Downloading HUD crosswalk...")
  
  url <- "https://www.huduser.gov/hudapi/public/usps"
  
  response <- httr::GET(
    url, 
    query = list(type = 5, query = "All", year = year, quarter = quarter),
    add_headers(Authorization = paste("Bearer", api_key))
  )
  
  if (httr::http_error(response)) {
    stop("API failed: ", httr::status_code(response))
  }
  
  data <- httr::content(response, as = "parsed")
  
  df <- do.call(rbind, lapply(data$data$results, function(x) {
    data.frame(
      zip = x$zip,
      tract = x$tract,
      res_ratio = as.numeric(x$res_ratio),
      stringsAsFactors = FALSE
    )
  }))
  
  message("Downloaded ", format(nrow(df), big.mark=","), " mappings")
}

enrich_visits_with_tracts <- function() {
  message("=== Enriching visits ===")
  
  xw_data <- download_hud_crosswalk()
  
  xw <- xw_data %>%
    transmute(
      zip5 = as.character(zip),
      tract_geoid = as.character(tract),
      weight = as.numeric(res_ratio)
    ) %>%
    group_by(zip5) %>%
    slice_max(weight, n = 1, with_ties = FALSE) %>%
    ungroup()
  
  visits <- read_ds(path(paths$silver, "visit")) %>% collect()
  
  message("Joining ", nrow(visits), " visits...")
  
  visits_enriched <- visits %>%
    left_join(xw %>% select(zip5, tract_geoid), by = "zip5")
  
  n_enriched <- sum(!is.na(visits_enriched$tract_geoid))
  pct <- round(100 * n_enriched  pct <- round(100 * n_enriched  pct <- roundnriched, " / ",
                                              nrow(visits_enriched), " (", pct, "%) matched")
  
  arrow::write_dataset(
    visits_enriched,
    path(paths$silver, "visit"),
    partitioning = c("year"),
    existing_data_behavior = "overwrite"
  )
  
  message("Done!")
}

if (!interactive()) {
  enrich_visits_with_tracts()
}
