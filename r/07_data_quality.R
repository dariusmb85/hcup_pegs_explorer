# r/07_data_quality.R
# Comprehensive data quality checks and cleaning
# Optimized: pushes filters into Arrow before collecting to minimize memory use

source(here::here("r", "00_env.R"))

library(arrow)
library(dplyr)
library(lubridate)

`%||%` <- function(x, y) if (!is.null(x)) x else y

cat("=== HCUP PEGS Data Quality Checks ===\n\n")

qa_dir <- path(paths$gold, "quality_checks")
dir_create(qa_dir)

# ── Lazy dataset reference — never fully collected ────────────────────────────
vis_ds <- open_dataset(path(paths$silver, "visit"))

cat("Counting rows...\n")
n_total <- vis_ds %>% count() %>% collect() %>% pull(n)
n_sid   <- vis_ds %>% filter(db_type == "SID") %>% count() %>% collect() %>% pull(n)
cat("  Total visits:", format(n_total, big.mark = ","), "\n\n")

qa_results <- list()
issues     <- list()

# ============================================================================
# CHECK 1: Overlapping Admissions (SID only)
# ============================================================================

cat("CHECK 1: Overlapping hospital admissions\n")
cat("  Rule: A person cannot be admitted while already admitted\n")

sid_visits <- vis_ds %>%
  filter(db_type == "SID") %>%
  select(person_id, visit_id, admit_date, discharge_date, dx_primary) %>%
  arrange(person_id, admit_date) %>%
  collect()

overlaps <- sid_visits %>%
  group_by(person_id) %>%
  mutate(
    prev_discharge = lag(discharge_date),
    overlap        = !is.na(prev_discharge) & admit_date < prev_discharge
  ) %>%
  ungroup() %>%
  filter(overlap) %>%
  select(person_id, visit_id, admit_date, discharge_date, prev_discharge, dx_primary)

n_overlaps   <- nrow(overlaps)
pct_overlaps <- round(100 * n_overlaps / n_sid, 2)
cat("  Found:", format(n_overlaps, big.mark = ","),
    "overlapping admissions (", pct_overlaps, "%)\n")

qa_results$overlapping_admissions <- list(n_issues = n_overlaps,
                                          pct_of_inpatient = pct_overlaps)
if (n_overlaps > 0) {
  write_csv(head(overlaps, 1000), path(qa_dir, "overlapping_admissions.csv"))
  issues$overlapping_admissions <- overlaps
  cat("  Sample saved\n")
}
rm(sid_visits); gc()
cat("\n")

# ============================================================================
# CHECK 2: Discharge Before Admission
# ============================================================================

cat("CHECK 2: Discharge before admission\n")
cat("  Rule: Discharge date must be >= admission date\n")

invalid_dates <- vis_ds %>%
  filter(!is.na(discharge_date), !is.na(admit_date),
         discharge_date < admit_date) %>%
  select(person_id, visit_id, admit_date, discharge_date, db_type, dx_primary) %>%
  collect()

n_invalid   <- nrow(invalid_dates)
pct_invalid <- round(100 * n_invalid / n_total, 2)
cat("  Found:", format(n_invalid, big.mark = ","),
    "invalid date sequences (", pct_invalid, "%)\n")

qa_results$invalid_date_sequences <- list(n_issues = n_invalid,
                                          pct_of_visits = pct_invalid)
if (n_invalid > 0) {
  write_csv(invalid_dates, path(qa_dir, "invalid_date_sequences.csv"))
  issues$invalid_dates <- invalid_dates
  cat("  Saved\n")
}
cat("\n")

# ============================================================================
# CHECK 3: Implausible Length of Stay
# ============================================================================

cat("CHECK 3: Implausible length of stay\n")
cat("  Rule: LOS should be 0-365 days for most cases\n")

los_issues <- vis_ds %>%
  filter(!is.na(los_days), (los_days < 0 | los_days > 365)) %>%
  select(person_id, visit_id, admit_date, discharge_date,
         los_days, db_type, dx_primary) %>%
  collect() %>%
  mutate(los_issue = if_else(los_days < 0, "negative", "over_1_year"))

n_los   <- nrow(los_issues)
pct_los <- round(100 * n_los / n_total, 2)
cat("  Found:", format(n_los, big.mark = ","),
    "implausible LOS (", pct_los, "%)\n")
cat("    Negative LOS:", sum(los_issues$los_issue == "negative"), "\n")
cat("    Over 1 year:",  sum(los_issues$los_issue == "over_1_year"), "\n")

qa_results$implausible_los <- list(
  n_issues      = n_los,
  n_negative    = sum(los_issues$los_issue == "negative"),
  n_over_year   = sum(los_issues$los_issue == "over_1_year"),
  pct_of_visits = pct_los
)
if (n_los > 0) {
  write_csv(los_issues, path(qa_dir, "implausible_los.csv"))
  issues$los_issues <- los_issues
  cat("  Saved\n")
}
cat("\n")

# ============================================================================
# CHECK 4: Missing Critical Variables
# ============================================================================

cat("CHECK 4: Missing critical variables\n")

missing_summary <- vis_ds %>%
  summarise(
    person_id  = sum(is.na(person_id),  na.rm = TRUE),
    admit_date = sum(is.na(admit_date), na.rm = TRUE),
    zip5       = sum(is.na(zip5),       na.rm = TRUE),
    dx_primary = sum(is.na(dx_primary), na.rm = TRUE),
    age        = sum(is.na(age),        na.rm = TRUE),
    female     = sum(is.na(female),     na.rm = TRUE),
    race       = sum(is.na(race),       na.rm = TRUE)
  ) %>%
  collect() %>%
  tidyr::pivot_longer(everything(),
                      names_to  = "variable",
                      values_to = "n_missing") %>%
  mutate(pct_missing = round(100 * n_missing / n_total, 2))

print(missing_summary)
qa_results$missing_data <- missing_summary
write_csv(missing_summary, path(qa_dir, "missing_data_summary.csv"))
cat("\n")

# ============================================================================
# CHECK 5: Implausible Demographics
# ============================================================================

cat("CHECK 5: Implausible demographics\n")

demo_issues <- vis_ds %>%
  filter(
    (age < 0 | age > 120) |
    (female != 0 & female != 1 & !is.na(female)) |
    (race < 0 | race > 10)
  ) %>%
  select(person_id, visit_id, age, female, race, dx_primary) %>%
  collect()

n_demo <- nrow(demo_issues)
cat("  Found:", format(n_demo, big.mark = ","), "demographic issues\n")
qa_results$demographic_issues <- list(n_issues = n_demo)
if (n_demo > 0) {
  write_csv(demo_issues, path(qa_dir, "demographic_issues.csv"))
  issues$demo_issues <- demo_issues
}
cat("\n")

# ============================================================================
# CHECK 6: Duplicate Visits
# ============================================================================

cat("CHECK 6: Duplicate visits\n")
cat("  Rule: Same person + same date + same diagnosis = likely duplicate\n")

duplicates <- vis_ds %>%
  select(person_id, visit_id, admit_date, discharge_date,
         dx_primary, db_type, facility_state) %>%
  collect() %>%
  group_by(person_id, admit_date, dx_primary) %>%
  filter(n() > 3) %>%
  arrange(person_id, admit_date) %>%
  ungroup()

n_dups <- nrow(duplicates)
cat("  Found:", format(n_dups, big.mark = ","), "potential duplicate visits\n")
qa_results$potential_duplicates <- list(n_issues = n_dups)
if (n_dups > 0) {
  write_csv(head(duplicates, 1000), path(qa_dir, "potential_duplicates.csv"))
  issues$duplicates <- duplicates
  cat("  Sample saved\n")
}
cat("\n")

# ============================================================================
# CHECK 7: Pregnancy in Males
# ============================================================================

cat("CHECK 7: Pregnancy diagnoses in males\n")

preg_icd9 <- paste0("^(", paste(c(
  "630","631","632","633","634","635","636","637","638","639",
  "640","641","642","643","644","645","646","647","648","649",
  "650","651","652","653","654","655","656","657","658","659",
  "660","661","662","663","664","665","666","667","668","669",
  "670","671","672","673","674","675","676","677","678","679",
  "V22","V23","V24","V27","V28"), collapse = "|"), ")")

male_pregnancy <- vis_ds %>%
  filter(female == 0) %>%
  select(person_id, visit_id, admit_date, age, female, dx_primary) %>%
  collect() %>%
  filter(grepl(preg_icd9, dx_primary) | grepl("^O", dx_primary))

n_male_preg <- nrow(male_pregnancy)
cat("  Found:", format(n_male_preg, big.mark = ","),
    "pregnancy diagnoses in males\n")
qa_results$male_pregnancy <- list(n_issues = n_male_preg)
if (n_male_preg > 0) {
  write_csv(male_pregnancy, path(qa_dir, "male_pregnancy_diagnoses.csv"))
  issues$male_pregnancy <- male_pregnancy
  cat("  Saved\n")
}
cat("\n")

# ============================================================================
# CHECK 9: Geographic Issues
# ============================================================================

cat("CHECK 9: Geographic outliers\n")

geo_issues <- vis_ds %>%
  filter(is.na(zip5) | nchar(zip5) != 5) %>%
  select(person_id, visit_id, zip5, tract_geoid, facility_state) %>%
  collect() %>%
  filter(is.na(zip5) | nchar(zip5) != 5 | grepl("[^0-9]", zip5))

n_geo <- nrow(geo_issues)
cat("  Found:", format(n_geo, big.mark = ","), "geographic data issues\n")
qa_results$geographic_issues <- list(n_issues = n_geo)
if (n_geo > 0) {
  write_csv(head(geo_issues, 1000), path(qa_dir, "geographic_issues.csv"))
  issues$geo_issues <- geo_issues
}
cat("\n")

# ============================================================================
# Summary Report
# ============================================================================

cat("=== QUALITY CHECK SUMMARY ===\n\n")

summary_df <- tibble(
  check    = names(qa_results),
  n_issues = sapply(qa_results, function(x) x$n_issues %||% 0),
  severity = case_when(
    n_issues == 0  ~ "PASS",
    n_issues < 100 ~ "WARNING",
    TRUE           ~ "FAIL"
  )
)

print(summary_df, n = Inf)
write_csv(summary_df, path(qa_dir, "quality_check_summary.csv"))
saveRDS(qa_results, path(qa_dir, "qa_results.rds"))
saveRDS(issues,     path(qa_dir, "qa_issues.rds"))
cat("\nOutputs saved to:", qa_dir, "\n\n")

# ============================================================================
# DATA CLEANING — partition-by-partition to avoid loading 111M rows at once
# ============================================================================

cat("=== APPLYING DATA CLEANING ===\n\n")
cat("Starting visits:", format(n_total, big.mark = ","), "\n")

# Build exclusion lookup
exclude_visits <- character(0)

if (n_invalid > 0) {
  exclude_visits <- c(exclude_visits, as.character(issues$invalid_dates$visit_id))
  cat("Excluding invalid dates:", format(n_invalid, big.mark = ","), "\n")
}
if (n_geo > 0) {
  exclude_visits <- c(exclude_visits, as.character(issues$geo_issues$visit_id))
  cat("Excluding geographic issues:", format(n_geo, big.mark = ","), "\n")
}
if (n_dups > 0) {
  dups_to_exclude <- issues$duplicates %>%
    group_by(person_id, admit_date, dx_primary) %>%
    arrange(visit_id) %>%
    slice(-1) %>%
    ungroup() %>%
    pull(visit_id)
  exclude_visits <- c(exclude_visits, as.character(dups_to_exclude))
  cat("Excluding duplicates:", format(length(dups_to_exclude), big.mark = ","), "\n")
}
if (n_overlaps > 0) {
  exclude_visits <- c(exclude_visits,
                      as.character(issues$overlapping_admissions$visit_id))
  cat("Excluding overlapping admissions:", format(n_overlaps, big.mark = ","), "\n")
}

exclude_visits <- unique(exclude_visits)
cat("Total unique exclusions:", format(length(exclude_visits), big.mark = ","), "\n\n")

exclude_df <- if (length(exclude_visits) > 0) {
  tibble(visit_id = as.numeric(exclude_visits))
} else {
  NULL
}

# Get all partitions
partitions <- vis_ds %>%
  select(facility_state, db_type, year) %>%
  distinct() %>%
  collect() %>%
  filter(!is.na(facility_state), !is.na(db_type), !is.na(year)) %>%
  arrange(facility_state, db_type, year)

cat(glue::glue("Writing cleaned visits across {nrow(partitions)} partitions...\n\n"))

n_written <- 0L

for (i in seq_len(nrow(partitions))) {
  st <- partitions$facility_state[i]
  dt <- partitions$db_type[i]
  yr <- partitions$year[i]

  chunk <- vis_ds %>%
    filter(facility_state == !!st, db_type == !!dt, year == !!yr) %>%
    collect()

  if (!is.null(exclude_df) && nrow(exclude_df) > 0) {
    chunk <- chunk %>% anti_join(exclude_df, by = "visit_id")
  }

  write_dataset(
    chunk,
    path(paths$silver, "visit_clean"),
    partitioning           = c("facility_state", "db_type", "year"),
    format                 = "parquet",
    existing_data_behavior = "delete_matching",
    compression            = "snappy"
  )

  n_written <- n_written + nrow(chunk)
  rm(chunk); gc()
}

n_excluded  <- n_total - n_written
pct_excluded <- round(100 * n_excluded / n_total, 2)

cat("=== CLEANING SUMMARY ===\n")
cat("Original visits:", format(n_total,    big.mark = ","), "\n")
cat("Cleaned visits: ", format(n_written,  big.mark = ","), "\n")
cat("Excluded:       ", format(n_excluded, big.mark = ","),
    "(", pct_excluded, "%)\n\n")

cat("Writing cleaned visits...\n")
cat("  - ", path(paths$silver, "visit_clean"), "\n")
cat("\n OK Quality checks and cleaning complete\n")