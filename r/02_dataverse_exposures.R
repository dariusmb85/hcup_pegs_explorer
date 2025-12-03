# r/02_dataverse_exposures.R
# Download monthly ZIP-level environmental exposures from Dataverse

source(here::here("r", "00_env.R"))

library(dataverse)
library(dplyr)
library(arrow)

# Dataverse configuration
DATAVERSE_DOI <- "doi:10.7910/DVN/0WILGX"
SERVER <- "dataverse.harvard.edu"
TARGET_FILENAME <- "zip_monthly_long.zip"

# Get API key from environment
api_key <- Sys.getenv("DATAVERSE_KEY")
if (nchar(api_key) == 0) {
  stop("DATAVERSE_KEY not found in .env file")
}

Sys.setenv("DATAVERSE_KEY" = api_key)
Sys.setenv("DATAVERSE_SERVER" = SERVER)

cat("=== Downloading Environmental Exposures from Dataverse ===\n\n")

# Setup cache directory
cache_dir <- path(paths$am_cache, "dataverse")
dir_create(cache_dir)
zip_file <- path(cache_dir, TARGET_FILENAME)

# Download if not cached
if (file_exists(zip_file)) {
  cat("Using cached zip file:", zip_file, "\n")
} else {
  cat("Downloading", TARGET_FILENAME, "from Dataverse...\n")

  # Download directly by filename
  file_content <- get_file_by_name(
    filename = TARGET_FILENAME,
    dataset = DATAVERSE_DOI,
    server = SERVER
  )

  writeBin(file_content, zip_file)
  cat("Downloaded successfully (", round(file.size(zip_file) / 1024^2, 1), "MB )\n")
}

# Extract
extracted_dir <- path(cache_dir, "zip_monthly_long_extracted")
dir_create(extracted_dir)

if (length(list.files(extracted_dir)) == 0) {
  cat("\nExtracting zip file...\n")
  unzip(zip_file, exdir = extracted_dir)
  cat("Extracted\n")
}

# Find parquet file
parquet_files <- list.files(extracted_dir, pattern = "\\.parquet$", full.names = TRUE, recursive = TRUE)

if (length(parquet_files) == 0) {
  stop("No parquet files found after extraction")
}

cat("\nFound parquet file:", basename(parquet_files[1]), "\n")

# Load and inspect
cat("Loading exposure data...\n")
exposures_raw <- read_parquet(parquet_files[1])

cat("Raw data dimensions:", format(nrow(exposures_raw), big.mark=","), "rows x", ncol(exposures_raw), "cols\n")
cat("Sample columns:", paste(head(names(exposures_raw), 10), collapse=", "), "\n")
cat("Year range:", min(exposures_raw$year, na.rm=TRUE), "-", max(exposures_raw$year, na.rm=TRUE), "\n")

# Show unique variables
cat("\nAvailable variables (first 20):\n")
print(head(unique(exposures_raw$variable), 20))

# Get year range from person-month cohort
cat("\nDetermining year range from person-month cohort...\n")
person_months <- read_ds(path(paths$gold, "person_month")) %>%
  collect()

cohort_years <- sort(unique(person_months$year))
min_year <- min(cohort_years, na.rm = TRUE)
max_year <- max(cohort_years, na.rm = TRUE)

cat("Cohort year range:", min_year, "-", max_year, "\n")

# Get unique ZIPs from cohort
cohort_zips <- unique(person_months$zip5)
cat("Cohort has", length(cohort_zips), "unique ZIP codes\n")

# Filter exposures to match cohort
cat("\nFiltering exposures to match cohort geography and time...\n")

exposures_filtered <- exposures_raw %>%
  filter(
    year >= min_year & year <= max_year,
    zip %in% cohort_zips,  # Only ZIPs in your cohort
    grepl("pm25|smoke|hms", variable, ignore.case = TRUE)
  )

cat("Filtered data:", format(nrow(exposures_filtered), big.mark=","), "rows\n")

if (nrow(exposures_filtered) > 0) {
  cat("Variables included:\n")
  print(unique(exposures_filtered$variable))

  # Save to gold layer
  output_path <- path(paths$gold, "exposures_monthly")
  write_parquet_ds(exposures_filtered, output_path)

  cat("\n✓ Environmental exposures ready\n")
  cat("  Output:", output_path, "\n")
} else {
  cat("\n⚠ Warning: No data matched filters. Check variable names and ZIP format.\n")
}