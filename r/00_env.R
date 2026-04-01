# Suppress common HPC warnings
options(warn = 1)  # Show warnings but don't stop

# Suppress sp package retirement warnings
options(sp_evolution_status = 2)

# Suppress libxml version warnings (HPC-specific)
suppressMessages({
  if (requireNamespace("xml2", quietly = TRUE)) {
    library(xml2)
  }
})

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

# Install if missing (suppress messages)
invisible(
  suppressMessages(
    lapply(pkgs, function(p) {
      if (!requireNamespace(p, quietly = TRUE)) {
        install.packages(p, quiet = TRUE)
      }
    })
  )
)

# Load packages (suppress startup messages)
invisible(
  suppressMessages(
    lapply(pkgs, library, character.only = TRUE, quietly = TRUE)
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