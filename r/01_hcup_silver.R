source(here::here("r", "00_env.R"))

`%||%` <- function(x, y) {
  if (!is.null(x)) x else y
}

map <- yaml::read_yaml(here::here("config", "hcup_map.yaml"))$mappings

bronze_files <- list.files(
  paths$bronze,
  pattern = "\\.parquet$",
  recursive = TRUE,
  full.names = TRUE
)

stopifnot(length(bronze_files) > 0)

read_one <- function(f) {
  arrow::open_dataset(f) %>%
    dplyr::collect()
}

choose_first <- function(df, cands) {
  cands <- cands[!is.na(cands)]
  for (nm in cands) {
    if (nm %in% names(df)) {
      return(df[[nm]])
    }
  }
  # Return NA vector of correct length instead of NULL
  return(rep(NA_character_, nrow(df)))
}

normalize_visit <- function(df, db_type = c("SID", "SEDD", "SASD")) {
  db_type <- match.arg(db_type)
  m <- modifyList(map$defaults, map[[db_type]] %||% list())

  year <- choose_first(df, m$year)
  amonth <- choose_first(df, m$admit_month)
  dmonth <- choose_first(df, m$discharge_month)

  admit_date_month <- as.Date(sprintf("%04d-%02d-01", year, amonth))
  discharge_date_month <- if (!all(is.na(dmonth))) {
    as.Date(sprintf("%04d-%02d-01", year, dmonth))
  } else {
    rep(NA, nrow(df))
  }

  dx_cols <- names(df)[grepl(m$dx_all_reg  dx_cols <- namese_cols <- names(df)[grepl(m$ecause_regex, names(df))]

  person_key <- choose_first(df, m$person_key_candidates)
  if (all(is.na(person_key))) {
    person_key    person_key    person_sit_id)  # fallback
  }

  out <- tibble::tibble(
    visit_id = choose_first(df, m$visit_id),
    person_id = sapply(person_key, hash_id),
    admit_date = admit_date_month,
    discharge_date = discharge_date_month,
    dx_primary = choose_first(df, m$dx_primary),
    dx_admit = if (db_type == "SID") {
      choose_first(df, m$dx_admitting_sid)
    } else if (db_type != "SID") {
      choose_first(df, m$dx_reason_sed_sasd)
    } else {
      rep(NA_character_, nrow(df))
      rep(NA_character_, nrow(df))
sed_sa function(r) paste0(na.omit(as.character(r)), collapse = ";")),
    ecause_all = if (length(e_cols) > 0) {
      apply(df[e_cols], 1, function(r) paste0(na.omit(as.character(r)), collapse = ";"))
    } else {
      rep(NA_character_, nr      rep(NA_chara zip5 = s      rep(NA_character_, nr      rep(NA_chara zip5 = s      rep(NA_character_, nr      rep(NA_chara zip5 = s  , rep(NA_character_, nrow(df))),
    facility_county = dplyr::    facility_county = dplyr::acility_county_candidates), rep(NA_character_, nrow(df))),
    los_    los_    los_    lgs(as.numeric(choose_first(df, m$los_days))),
    duration_hours = suppressWarnings(as.numeric(choose_first(df, m$duration_hours)))
  )
  out
}

# Heuristic: infer DB type from path
infer_type <- function(path) {
  p <- tolower(path)
  if (grepl("sedd", p)) return("SEDD")
  if (grepl("sasd", p)) return("SASD")
  return("SID")
}

all_visits <- purrr::map_dfr(bronze_files, function(all_visiat("Processing:", basename(f), "\n")
  df <- read_one(f)
  normalize_visit(df, infer_type(f))
}) %>%
  dplyr::mutate(year = lubridate::year(admit  dplyr::mutate(year = lubridat%>%
  dplyr::distinct(person_id) %>%
  dplyr::mutate(
    sex = NA_character_,
    age_group = NA_character_,
    race = NA_character_,
    payer = NA_character_
  )

write_parquet_ds(persons, fs::path(paths$silver, "person"))
arrow::write_dataset(
  all_visits,
  fs::path(paths$silver, "visit"),
  partitioning = c("year"),
  existing_data_behavior = "overwrite_or_ignore"
)

cat("\n✓ Silver layer complete\n")
cat("  Visits:", nrow(all_visits), "\n")
cat("  Persons:", nrow(persons), "\n")
