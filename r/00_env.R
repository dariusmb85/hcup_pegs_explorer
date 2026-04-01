options(warn = 1)

# Load .env file FIRST
if (file.exists(".env")) {
  dotenv::load_dot_env(".env")
}

if (!requireNamespace("here", quietly = TRUE)) {
  install.packages("here")
}

library(here)
library(fs)
library(glue)

pkgs <- c(
  "arrow",
  "dplyr",
  "tidyr",
  "purrr",
  "stringr",
  "lubridate",
  "jsonlite",
  "yaml",
  "digest",
  "readr",
  "sf",
  "amadeus",
  "httr",
  "dataverse"
)

# Install if missing
invisible(
  lapply(
    pkgs,
    function(p) {
      if (!requireNamespace(p, quietly = TRUE)) {
        install.packages(p)
      }
    }
  )
)

# LOAD the packages
invisible(lapply(pkgs, library, character.only = TRUE))

root <- Sys.getenv("PARQUET_ROOT", unset = here("data"))

paths <- list(
  bronze   = path(root, "bronze"),
  silver   = path(root, "silver"),
  gold     = path(root, "gold"),
  am_cache = Sys.getenv("AMADEUS_CACHE", unset = here("data", "amadeus_cache"))
)

for (p in paths) {
  dir_create(p)
}

source(here("r", "utils.R"))
