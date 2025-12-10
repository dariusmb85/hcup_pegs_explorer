# r/03_exposure_rollup.R
# Create exposure_rollup table in long format with metrics

source(here::here("r", "00_env.R"))

library(arrow)
library(dplyr)
library(tidyr)
library(yaml)

cat("=== Creating Exposure Rollup ===\n\n")

# Load configuration
cat("Loading feature definitions...\n")
config <- read_yaml(here::here("config", "covariates.yaml"))
features <- config$features
rollup_metrics <- config$rollup_metrics

cat("  Features:", length(features), "\n")
cat("  Metrics:", length(rollup_metrics), "\n")

# Load person-month exposures (wide format)
cat("\nLoading person-month exposures...\n")
pm_exp <- open_dataset(path(paths$gold, "person_month_exposures")) %>%
  collect()

cat("  Person-months:", format(nrow(pm_exp), big.mark=","), "\n")

# Create feature mapping (var -> id)
feature_map <- setNames(
  sapply(features, function(f) f$id),
  sapply(features, function(f) f$var)
)

cat("\nFeature mapping:\n")
for (var_name in names(feature_map)) {
  cat("  ", var_name, "->", feature_map[[var_name]], "\n")
}

# Convert to long format
cat("\nPivoting to long format...\n")

# Get exposure columns from the data
exposure_vars <- names(feature_map)
available_vars <- exposure_vars[exposure_vars %in% names(pm_exp)]

if (length(available_vars) == 0) {
  stop("No exposure variables found in person_month_exposures!")
}

cat("  Available exposure columns:", paste(available_vars, collapse=", "), "\n")

# Pivot to long format
exposure_long <- pm_exp %>%
  select(person_id, ym, year, month, all_of(available_vars)) %>%
  pivot_longer(
    cols = all_of(available_vars),
    names_to = "variable",
    values_to = "value"
  ) %>%
  filter(!is.na(value)) %>%  # Remove missing values
  mutate(
    exposure_id = feature_map[variable]  # Map var -> id
  )

cat("  Long format:", format(nrow(exposure_long), big.mark=","), "rows\n")

# For monthly data, we already have the aggregation
# Just create the rollup with "mean" metric (since it's already monthly mean)
cat("\nCreating exposure rollup...\n")

exposure_rollup <- exposure_long %>%
  mutate(metric = "mean") %>%  # Monthly data is already aggregated as mean
  select(person_id, ym, exposure_id, metric, value)

cat("  Rollup rows:", format(nrow(exposure_rollup), big.mark=","), "\n")

# Summary by exposure
cat("\nExposure summary:\n")
summary_stats <- exposure_rollup %>%
  group_by(exposure_id) %>%
  summarise(
    n = n(),
    min_val = min(value, na.rm=TRUE),
    mean_val = mean(value, na.rm=TRUE),
    max_val = max(value, na.rm=TRUE),
    .groups = "drop"
  )

print(summary_stats, n = Inf)

# Save
cat("\nSaving exposure rollup...\n")
output_path <- path(paths$gold, "exposure_rollup")

write_parquet_ds(
  exposure_rollup,
  output_path,
  partitioning = c("exposure_id", "metric")
)

cat("\n✓ Complete\n")
cat("  Output:", output_path, "\n")
cat("  Ready for ExWAS\n")
