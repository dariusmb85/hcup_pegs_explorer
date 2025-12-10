options(warn = 1)

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
  "amadeus" # from NIEHS org (install per your setup)
)

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
