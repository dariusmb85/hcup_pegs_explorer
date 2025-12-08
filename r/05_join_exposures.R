# r/05_join_exposures.R
# Join monthly exposures to person-month cohort

source(here::here("r", "00_env.R"))

library(arrow)
library(dplyr)
library(tidyr)
library(lubridate)

cat("=== Joining Exposures to Person-Month Cohort ===\n\n")

# Load person-month cohort
cat("Loading person-month cohort...\n")
person_months <- open_dataset(path(paths$gold, "person_month")) %>%
  collect()

cat("  Person-months:", format(nrow(person_months), big.mark=","), "\n")

# Extract year and month from ym date column
person_months <- person_months %>%
  mutate(
    year = as.character(year(ym)),
    month = month(ym)
  )

cat("  Added year/month columns\n")

# Load exposures (already has zip5 from previous script)
cat("\nLoading exposures...\n")
exposures <- open_dataset(path(paths$gold, "exposures_monthly")) %>%
  collect()

cat("  Exposure rows:", format(nrow(exposures), big.mark=","), "\n")
cat("  Variables:", paste(unique(exposures$variable), collapse=", "), "\n")

# Check column names
cat("\nExposure columns:", paste(names(exposures), collapse=", "), "\n")

# If geoid wasn't renamed, do it now
if ("geoid" %in% names(exposures)) {
  exposures <- exposures %>% rename(zip5 = geoid)
  cat("  Renamed geoid to zip5\n")
}

# Pivot exposures to wide format
cat("\nPivoting exposures to wide format...\n")
exposures_wide <- exposures %>%
  select(zip5, year, month, variable, value) %>%
  pivot_wider(
    names_from = variable,
    values_from = value
  )

cat("  Wide format:", format(nrow(exposures_wide), big.mark=","), "rows x", ncol(exposures_wide), "cols\n")

# Join with person-months
cat("\nJoining on zip5 + year + month...\n")
person_months_exp <- person_months %>%
  left_join(
    exposures_wide,
    by = c("zip5", "year", "month")
  )

# Check join success
n_with_exp <- sum(!is.na(person_months_exp$tmax))
pct_matched <- round(100 * n_with_exp / nrow(person_months_exp), 1)

cat("\n=== Join Summary ===\n")
cat("Total person-months:", format(nrow(person_months_exp), big.mark=","), "\n")
cat("With exposures:", format(n_with_exp, big.mark=","), "(", pct_matched, "%)\n")

# Show exposure coverage by variable
cat("\nExposure coverage:\n")
exp_cols <- c("prop_light_coverage", "prop_med_coverage", "prop_heavy_coverage",
              "dusmass25", "bcsmass", "tmax", "tmin")

for (col in exp_cols) {
  if (col %in% names(person_months_exp)) {
    n_avail <- sum(!is.na(person_months_exp[[col]]))
    pct <- round(100 * n_avail / nrow(person_months_exp), 1)
    cat("  ", col, ":", format(n_avail, big.mark=","), "(", pct, "%)\n")
  }
}

# Save final dataset
cat("\nSaving person-month dataset with exposures...\n")
output_path <- path(paths$gold, "person_month_exposures")

write_parquet_ds(
  person_months_exp,
  output_path,
  partitioning = c("year")
)

cat("\n✓ Complete\n")
cat("  Output:", output_path, "\n")
cat("  Ready for ExWAS analysis\n")
