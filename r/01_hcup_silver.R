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

# Load PheCode mapping and create lookup
phecode_lookup <- setNames(phecode_map$phecode, phecode_map$code)

# format_icd_for_phecode function
format_icd_for_phecode <- function(code) {
  if(is.na(code) || code == "") return(NA_character_)

  # ICD-9 E-codes: decimal after 4th position (check BEFORE general letters)
  if(grepl("^E[0-9]", code)) {
    if(nchar(code) <= 4) return(code)
    if(nchar(code) > 4) {
      return(paste0(substr(code, 1, 4), ".", substr(code, 5, nchar(code))))
    }
  }

  # ICD-9 V-codes: decimal after 3rd position
  if(grepl("^V[0-9]", code)) {
    if(nchar(code) <= 3) return(code)
    if(nchar(code) > 3) {
      return(paste0(substr(code, 1, 3), ".", substr(code, 4, nchar(code))))
    }
  }

  # ICD-10 detection: starts with letter (but not E or V followed by numbers)
  if(grepl("^[A-Z]", code)) {
    if(nchar(code) == 3) return(code)
    if(nchar(code) > 3) {
      return(paste0(substr(code, 1, 3), ".", substr(code, 4, nchar(code))))
    }
  }

  # ICD-9 numeric codes: decimal after 3rd position
  if(grepl("^[0-9]+$", code)) {
    if(nchar(code) == 3) return(code)
    if(nchar(code) > 3) {
      return(paste0(substr(code, 1, 3), ".", substr(code, 4, nchar(code))))
    }
  }

  return(code)
}

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

  rep(NA_character_, nrow(df))
}

normalize_visit <- function(df, db_type = c("SID", "SEDD", "SASD")) {
  db_type <- match.arg(db_type)
  m <- modifyList(map$defaults, map[[db_type]] %||% list())

  year   <- choose_first(df, m$year)
  amonth <- choose_first(df, m$admit_month)
  dmonth <- choose_first(df, m$discharge_month)

  admit_date_month <- as.Date(sprintf("%04d-%02d-01", year, amonth))

  discharge_date_month <- if (!is.null(dmonth)) {
    as.Date(sprintf("%04d-%02d-01", year, dmonth))
  } else {
    NA
  }

  dx_cols <- names(df)[grepl(m$dx_all_regex, names(df))]
  e_cols  <- names(df)[grepl(m$ecause_regex, names(df))]

 person_key <- choose_first(df, m$person_key_candidates)

if (is.null(person_key)) {
  # No person linkage variable exists at all
  person_key <- choose_first(df, m$visit_id)
  cat(" [X] No person linkage found, using visit_id as person_key\n")
} else {
  # Person linkage exists (e.g., VisitLink)
  # For visits with NA linkage, use visit_id as fallback for those rows only
  visit_id <- choose_first(df, m$visit_id)

  n_linked_before <- sum(!is.na(person_key))
  person_key <- ifelse(is.na(person_key), visit_id, person_key)
  n_linked_after <- sum(!is.na(person_key))

  cat("  ✓ Person linkage found:", n_linked_before, "/", length(person_key),
      "(", round(100 * n_linked_before / length(person_key), 1), "%)\n")
  cat("  ✓ After fallback:", n_linked_after, "/", length(person_key), "\n")
}

  out <- tibble::tibble(
    visit_id          = choose_first(df, m$visit_id),
    person_id = purrr::map_chr(format(person_key, scientific=FALSE, trim=TRUE), hash_id),
    admit_date        = admit_date_month,
    discharge_date    = discharge_date_month,
    dx_primary        = choose_first(df, m$dx_primary),
    dx_admit          = if (db_type == "SID") {
      choose_first(df, m$dx_admitting_sid)
    } else if (db_type != "SID") {
      choose_first(df, m$dx_reason_sed_sasd)
    } else {
      NA_character_
    },
    dx_all            = apply(
      df[dx_cols],
      1,
      function(r) paste0(na.omit(as.character(r)), collapse = ";")
    ),
    ecause_all        = if (length(e_cols)) {
      apply(
        df[e_cols],
        1,
        function(r) paste0(na.omit(as.character(r)), collapse = ";")
      )
    } else {
      NA_character_
    },
    zip5              = substr(choose_first(df, m$zip5), 1, 5),
    facility_state    = dplyr::coalesce(
      choose_first(df, m$facility_state),
      NA_character_
    ),
    facility_county   = dplyr::coalesce(
      choose_first(df, m$facility_county_candidates),
      NA_character_
    ),
    los_days          = suppressWarnings(
      as.numeric(choose_first(df, m$los_days))
    ),
    duration_hours    = suppressWarnings(
      as.numeric(choose_first(df, m$duration_hours))
    ),
    age = suppressWarnings(as.numeric(choose_first(df, c("AGE")))
    ),
    female = suppressWarnings(as.numeric(choose_first(df, c("FEMALE")))
    ),
    race = suppressWarnings(as.numeric(choose_first(df, c("RACE")))
    ),
    db_type = db_type
  )

  out
}

# Heuristic: infer DB type from path
infer_type <- function(path) {
  p <- tolower(path)

  if (grepl("sedd", p)) {
    return("SEDD")
  }

  if (grepl("sasd", p)) {
    return("SASD")
  }

  "SID"
}

all_visits <- purrr::map_dfr(
  bronze_files,
  function(f) {
    df <- read_one(f)
    normalize_visit(df, infer_type(f))
  }
) %>%
  dplyr::mutate(year = lubridate::year(admit_date))

all_visits <- all_visits %>%
  mutate(
    dx_primary_phecode = phecode_lookup[sapply(dx_primary, format_icd_for_phecode)]
  )

# Add all secondary diagnosis PheCodes (dx1-dx30)
dx_cols <- names(all_visits)[grepl("^dx[0-9]+$", names(all_visits))]
for(col in dx_cols) {
  new_col <- paste0(col, "_phecode")
  all_visits[[new_col]] <- sapply(all_visits[[col]], function(icd) {
    if(is.na(icd)) return(NA_character_)
    phecode_match <- phecode_map$phecode[phecode_map$icd_code == icd][1]
    if(length(phecode_match) > 0) phecode_match else NA_character_
  })
}

persons <- all_visits %>%
  dplyr::distinct(person_id) %>%
  dplyr::mutate(
    sex       = NA_character_,
    age_group = NA_character_,
    race      = NA_character_,
    payer     = NA_character_
  )

write_parquet_ds(persons, fs::path(paths$silver, "person"))

arrow::write_dataset(
  all_visits,
  fs::path(paths$silver, "visit"),
  partitioning = c("year"),
  existing_data_behavior = "overwrite"
)
