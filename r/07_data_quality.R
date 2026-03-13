# r/07_data_quality.R
# Comprehensive data quality checks and cleaning

source(here::here("r", "00_env.R"))

library(arrow)
library(dplyr)
library(tidyr)
library(lubridate)
library(ggplot2)

cat("=== HCUP PEGS Data Quality Checks ===\n\n")

# Create output directory
qa_dir <- path(paths$gold, "quality_checks")
dir_create(qa_dir)

# ============================================================================
# Load Data
# ============================================================================

cat("Loading data...\n")
visits <- open_dataset(path(paths$silver, "visit")) %>% collect()
# person_months <- open_dataset(path(paths$gold, "person_month")) %>% collect()

cat("  Visits:", format(nrow(visits), big.mark=","), "\n")
#cat("  Person-months:", format(nrow(person_months), big.mark=","), "\n\n")

# Initialize results list
qa_results <- list()
issues <- list()

# ============================================================================
# CHECK 1: Overlapping Admissions
# ============================================================================

cat("CHECK 1: Overlapping hospital admissions\n")
cat("  Rule: A person cannot be admitted while already admitted\n")

overlaps <- visits %>%
  filter(db_type == "SID") %>%  # Only inpatient
  arrange(person_id, admit_date) %>%
  group_by(person_id) %>%
  mutate(
    prev_discharge = lag(discharge_date),
    overlap = admit_date < prev_discharge
  ) %>%
  filter(overlap == TRUE) %>%
  select(person_id, visit_id, admit_date, discharge_date, prev_discharge, dx_primary)

n_overlaps <- nrow(overlaps)
pct_overlaps <- round(100 * n_overlaps / sum(visits$db_type == "SID", na.rm=TRUE), 2)

cat("  Found:", format(n_overlaps, big.mark=","), "overlapping admissions (", pct_overlaps, "%)\n")

qa_results$overlapping_admissions <- list(
  n_issues = n_overlaps,
  pct_of_inpatient = pct_overlaps
)

if (n_overlaps > 0) {
  write_csv(head(overlaps, 1000), path(qa_dir, "overlapping_admissions.csv"))
  issues$overlapping_admissions <- overlaps
  cat("  ⚠️  Sample saved to:", path(qa_dir, "overlapping_admissions.csv"), "\n")
}
cat("\n")

# ============================================================================
# CHECK 2: Discharge Before Admission
# ============================================================================

cat("CHECK 2: Discharge before admission\n")
cat("  Rule: Discharge date must be >= admission date\n")

invalid_dates <- visits %>%
  filter(!is.na(discharge_date), !is.na(admit_date)) %>%
  filter(discharge_date < admit_date) %>%
  select(person_id, visit_id, admit_date, discharge_date, db_type, dx_primary)

n_invalid <- nrow(invalid_dates)
pct_invalid <- round(100 * n_invalid / nrow(visits), 2)

cat("  Found:", format(n_invalid, big.mark=","), "invalid date sequences (", pct_invalid, "%)\n")

qa_results$invalid_date_sequences <- list(
  n_issues = n_invalid,
  pct_of_visits = pct_invalid
)

if (n_invalid > 0) {
  write_csv(invalid_dates, path(qa_dir, "invalid_date_sequences.csv"))
  issues$invalid_dates <- invalid_dates
  cat("  ⚠️  Sample saved to:", path(qa_dir, "invalid_date_sequences.csv"), "\n")
}
cat("\n")

# ============================================================================
# CHECK 3: Implausible Length of Stay
# ============================================================================

cat("CHECK 3: Implausible length of stay\n")
cat("  Rule: LOS should be 0-365 days for most cases\n")

los_issues <- visits %>%
  filter(!is.na(los_days)) %>%
  mutate(
    los_issue = case_when(
      los_days < 0 ~ "negative",
      los_days > 365 ~ "over_1_year",
      TRUE ~ "ok"
    )
  ) %>%
  filter(los_issue != "ok") %>%
  select(person_id, visit_id, admit_date, discharge_date, los_days, los_issue, db_type, dx_primary)

n_los_issues <- nrow(los_issues)
pct_los <- round(100 * n_los_issues / nrow(visits), 2)

cat("  Found:", format(n_los_issues, big.mark=","), "implausible LOS (", pct_los, "%)\n")
cat("    Negative LOS:", sum(los_issues$los_issue == "negative"), "\n")
cat("    Over 1 year:", sum(los_issues$los_issue == "over_1_year"), "\n")

qa_results$implausible_los <- list(
  n_issues = n_los_issues,
  n_negative = sum(los_issues$los_issue == "negative"),
  n_over_year = sum(los_issues$los_issue == "over_1_year"),
  pct_of_visits = pct_los
)

if (n_los_issues > 0) {
  write_csv(los_issues, path(qa_dir, "implausible_los.csv"))
  issues$los_issues <- los_issues
  cat("  ⚠️  Issues saved to:", path(qa_dir, "implausible_los.csv"), "\n")
}
cat("\n")

# ============================================================================
# CHECK 4: Missing Critical Variables
# ============================================================================

cat("CHECK 4: Missing critical variables\n")

missing_summary <- tibble(
  variable = c("person_id", "admit_date", "zip5", "dx_primary", "age", "female", "race"),
  n_missing = c(
    sum(is.na(visits$person_id)),
    sum(is.na(visits$admit_date)),
    sum(is.na(visits$zip5)),
    sum(is.na(visits$dx_primary)),
    sum(is.na(visits$age)),
    sum(is.na(visits$female)),
    sum(is.na(visits$race))
  ),
  pct_missing = round(100 * n_missing / nrow(visits), 2)
)

print(missing_summary)

qa_results$missing_data <- missing_summary

write_csv(missing_summary, path(qa_dir, "missing_data_summary.csv"))
cat("\n")

# ============================================================================
# CHECK 5: Implausible Demographics
# ============================================================================

cat("CHECK 5: Implausible demographics\n")

demo_issues <- visits %>%
  filter(
    age < 0 | age > 120 |  # Implausible age
    (female != 0 & female != 1 & !is.na(female)) |  # Invalid sex
    race < 0 | race > 10  # Invalid race code
  ) %>%
  select(person_id, visit_id, age, female, race, dx_primary)

n_demo_issues <- nrow(demo_issues)
cat("  Found:", format(n_demo_issues, big.mark=","), "demographic issues\n")

qa_results$demographic_issues <- list(n_issues = n_demo_issues)

if (n_demo_issues > 0) {
  write_csv(demo_issues, path(qa_dir, "demographic_issues.csv"))
  issues$demo_issues <- demo_issues
}
cat("\n")

# ============================================================================
# CHECK 6: Duplicate Visits
# ============================================================================

cat("CHECK 6: Duplicate visits\n")
cat("  Rule: Same person + same date + same diagnosis = likely duplicate\n")

duplicates <- visits %>%
  group_by(person_id, admit_date, dx_primary) %>%
  filter(n() > 3) %>%
  arrange(person_id, admit_date) %>%
  select(person_id, visit_id, admit_date, discharge_date, dx_primary, db_type, facility_state)

n_dups <- nrow(duplicates)
cat("  Found:", format(n_dups, big.mark=","), "potential duplicate visits\n")

qa_results$potential_duplicates <- list(n_issues = n_dups)

if (n_dups > 0) {
  write_csv(head(duplicates, 1000), path(qa_dir, "potential_duplicates.csv"))
  issues$duplicates <- duplicates
  cat("  ⚠️  Sample saved to:", path(qa_dir, "potential_duplicates.csv"), "\n")
}
cat("\n")

# ============================================================================
# CHECK 7: Pregnancy in Males
# ============================================================================

cat("CHECK 7: Pregnancy diagnoses in males\n")

preg_codes_icd9 <- c("630", "631", "632", "633", "634", "635", "636", "637", "638", "639",
                     "640", "641", "642", "643", "644", "645", "646", "647", "648", "649",
                     "650", "651", "652", "653", "654", "655", "656", "657", "658", "659",
                     "660", "661", "662", "663", "664", "665", "666", "667", "668", "669",
                     "670", "671", "672", "673", "674", "675", "676", "677", "678", "679",
                     "V22", "V23", "V24", "V27", "V28")
preg_codes_icd10 <- "O"

male_pregnancy <- visits %>%
  filter(female == 0) %>%  # Males
  filter(
    grepl(paste0("^(", paste(preg_codes_icd9, collapse="|"), ")"), dx_primary) |
    grepl(paste0("^", preg_codes_icd10), dx_primary)
  ) %>%
  select(person_id, visit_id, admit_date, age, female, dx_primary)

n_male_preg <- nrow(male_pregnancy)
cat("  Found:", format(n_male_preg, big.mark=","), "pregnancy diagnoses in males\n")

qa_results$male_pregnancy <- list(n_issues = n_male_preg)

if (n_male_preg > 0) {
  write_csv(male_pregnancy, path(qa_dir, "male_pregnancy_diagnoses.csv"))
  issues$male_pregnancy <- male_pregnancy
  cat("  ⚠️  Issues saved to:", path(qa_dir, "male_pregnancy_diagnoses.csv"), "\n")
}
cat("\n")

# ============================================================================
# CHECK 8: Pediatric Visits with Adult-Only Diagnoses
# ============================================================================

# cat("CHECK 8: Pediatric visits (<18) with adult-only diagnoses\n")

# Common adult-only conditions (e.g., prostate, menopause-related)
# adult_codes <- c("600", "185", "N40", "C61")  # Prostate-related

# pediatric_adult_dx <- visits %>%
#   filter(age < 18) %>%
#   filter(grepl(paste0("^(", paste(adult_codes, collapse="|"), ")"), dx_primary)) %>%
#   select(person_id, visit_id, age, female, dx_primary)

# n_ped_adult <- nrow(pediatric_adult_dx)
# cat("  Found:", format(n_ped_adult, big.mark=","), "pediatric visits with adult-only diagnoses\n")

# qa_results$pediatric_adult_diagnoses <- list(n_issues = n_ped_adult)

# if (n_ped_adult > 0) {
#   write_csv(pediatric_adult_dx, path(qa_dir, "pediatric_adult_diagnoses.csv"))
#   issues$ped_adult_dx <- pediatric_adult_dx
# }
# cat("\n")

# ============================================================================
# CHECK 9: Geographic Outliers
# ============================================================================

cat("CHECK 9: Geographic outliers\n")

geo_issues <- visits %>%
  filter(
    is.na(zip5) |
    nchar(zip5) != 5 |
    grepl("[^0-9]", zip5)  # Non-numeric ZIP
  ) %>%
  select(person_id, visit_id, zip5, tract_geoid, facility_state)

n_geo_issues <- nrow(geo_issues)
cat("  Found:", format(n_geo_issues, big.mark=","), "geographic data issues\n")

qa_results$geographic_issues <- list(n_issues = n_geo_issues)

if (n_geo_issues > 0) {
  write_csv(head(geo_issues, 1000), path(qa_dir, "geographic_issues.csv"))
  issues$geo_issues <- geo_issues
}
cat("\n")

# ============================================================================
# CHECK 10: Extreme Visit Frequencies
# ============================================================================

#cat("CHECK 10: Extreme visit frequencies\n")
#cat("  Rule: >50 visits/month suggests data quality issue\n")

#high_utilizers <- person_months %>%
#filter(n_visits > 50) %>%
# arrange(desc(n_visits)) %>%
# select(person_id, ym, n_visits, zip5, asthma_flag:mental_health_flag)

#n_high_util <- nrow(high_utilizers)
#cat("  Found:", format(n_high_util, big.mark=","), "person-months with >50 visits\n")

#qa_results$high_utilization <- list(
# n_person_months = n_high_util,
# max_visits = max(person_months$n_visits, na.rm=TRUE)
#)

#if (n_high_util > 0) {
#  write_csv(high_utilizers, path(qa_dir, "high_utilization.csv"))
#  issues$high_utilizers <- high_utilizers
#  cat("  Max visits in one month:", max(person_months$n_visits, na.rm=TRUE), "\n")
#  cat("  ⚠️  Issues saved to:", path(qa_dir, "high_utilization.csv"), "\n")
#}
#cat("\n")

# ============================================================================
# Summary Report
# ============================================================================

cat("=== QUALITY CHECK SUMMARY ===\n\n")

summary_df <- tibble(
  check = names(qa_results),
  n_issues = sapply(qa_results, function(x) x$n_issues %||% x$n_person_months %||% 0),
  severity = case_when(
    n_issues == 0 ~ "✓ PASS",
    n_issues < 100 ~ "⚠️  WARNING",
    TRUE ~ "❌ FAIL"
  )
)

print(summary_df, n = Inf)

# Save summary
write_csv(summary_df, path(qa_dir, "quality_check_summary.csv"))

# Save full results
saveRDS(qa_results, path(qa_dir, "qa_results.rds"))
saveRDS(issues, path(qa_dir, "qa_issues.rds"))

cat("\n")
cat("Quality check outputs saved to:", qa_dir, "\n")
cat("  - quality_check_summary.csv\n")
cat("  - qa_results.rds (full results)\n")
cat("  - qa_issues.rds (flagged records)\n")
cat("  - Individual CSV files for each issue type\n")

# ============================================================================
# DATA CLEANING - Use QC Results to Filter
# ============================================================================

cat("\n=== APPLYING DATA CLEANING ===\n\n")

cat("Starting visits:", format(nrow(visits), big.mark=","), "\n")

# Build exclusion list from QC results
exclude_visits <- c()

# Add invalid date sequences
if (n_invalid > 0) {
  exclude_visits <- c(exclude_visits, issues$invalid_dates$visit_id)
  cat("Excluding invalid dates:", format(n_invalid, big.mark=","), "\n")
}

# Add geographic issues
if (n_geo_issues > 0) {
  exclude_visits <- c(exclude_visits, issues$geo_issues$visit_id)
  cat("Excluding geographic issues:", format(n_geo_issues, big.mark=","), "\n")
}

# Add duplicates (keep first, exclude rest)
if (n_dups > 0) {
  # From duplicates, keep only first occurrence per person-date-dx
  dups_to_exclude <- issues$duplicates %>%
    group_by(person_id, admit_date, dx_primary) %>%
    arrange(visit_id) %>%
    slice(-1) %>%  # Remove first, keep rest for exclusion
    ungroup() %>%
    pull(visit_id)

  exclude_visits <- c(exclude_visits, dups_to_exclude)
  cat("Excluding duplicate visits:", format(length(dups_to_exclude), big.mark=","), "\n")
}

# Add overlapping admissions
if (n_overlaps > 0) {
  # For overlaps, exclude the later admission
  overlaps_to_exclude <- issues$overlapping_admissions %>%
    pull(visit_id)

  exclude_visits <- c(exclude_visits, overlaps_to_exclude)
  cat("Excluding overlapping admissions:", format(n_overlaps, big.mark=","), "\n")
}

# Remove duplicates from exclusion list
exclude_visits <- unique(exclude_visits)

# Apply filter
visits_clean <- visits %>%
  filter(!visit_id %in% exclude_visits)

n_excluded <- nrow(visits) - nrow(visits_clean)
pct_excluded <- round(100 * n_excluded / nrow(visits), 2)

cat("\n=== CLEANING SUMMARY ===\n")
cat("Original visits:", format(nrow(visits), big.mark=","), "\n")
cat("Cleaned visits:", format(nrow(visits_clean), big.mark=","), "\n")
cat("Excluded:", format(n_excluded, big.mark=","),
    "(", pct_excluded, "%)\n\n")

# Write cleaned visits
cat("Writing cleaned visits...\n")
write_dataset(
  visits_clean,
  path(paths$silver, "visit_clean"),
  partitioning = "year",
  format = "parquet",
  existing_data_behavior = "overwrite"
)

# Update person table to only include people with remaining visits
cat("Updating person table...\n")
persons_clean <- visits_clean %>%
  group_by(person_id) %>%
  summarise(
    first_visit = min(admit_date, na.rm = TRUE),
    last_visit = max(admit_date, na.rm = TRUE),
    n_visits = n(),
    age = first(na.omit(age)),
    female = first(na.omit(female)),
    race = first(na.omit(race)),
    .groups = "drop"
  )

write_dataset(
  persons_clean,
  path(paths$silver, "person_clean"),
  format = "parquet",
  existing_data_behavior = "overwrite"
)

cat("✓ Cleaned data saved:\n")
cat("  - ", path(paths$silver, "visit_clean"), "\n")
cat("  - ", path(paths$silver, "person_clean"), "\n")

cat("\n✓ Quality checks and cleaning complete\n")