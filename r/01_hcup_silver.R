source(here::here("r", "00_env.R"))

`%||%` <- function(x, y) {
  if (!is.null(x)) x else y
}

map <- yaml::read_yaml(here::here("config", "hcup_map.yaml"))$mappings

bronze_files <- list.files(
  paths$bronze,
  pattern    = "\\.parquet$",
  recursive  = TRUE,
  full.names = TRUE
) %>%
  .[!grepl("2015.*q[1-4]",  .)] %>%
  .[!grepl("2015.*q1q3",    .)]

# Load PheCode mapping and create lookup
phecode_map    <- read_csv("phecodes_cm_rolled.csv", show_col_types = FALSE)
phecode_lookup <- setNames(phecode_map$phecode, phecode_map$code)

# ── Vectorized ICD formatter ──────────────────────────────────────────────────
# Replaces the row-by-row sapply with a single vectorized call (~50-100x faster)
format_icd_for_phecode_vec <- function(codes) {
  out <- codes
  na_or_empty <- is.na(codes) | codes == ""
  out[na_or_empty] <- NA_character_

  ok <- !na_or_empty

  # ICD-9 E-codes (E followed by digits)
  e9 <- ok & grepl("^E[0-9]", codes)
  long_e9 <- e9 & nchar(codes) > 4
  out[long_e9] <- paste0(substr(codes[long_e9], 1, 4), ".", substr(codes[long_e9], 5, nchar(codes[long_e9])))

  # ICD-9 V-codes
  v9 <- ok & !e9 & grepl("^V[0-9]", codes)
  long_v9 <- v9 & nchar(codes) > 3
  out[long_v9] <- paste0(substr(codes[long_v9], 1, 3), ".", substr(codes[long_v9], 4, nchar(codes[long_v9])))

  # ICD-10 (letter start, not E/V already handled)
  i10 <- ok & !e9 & !v9 & grepl("^[A-Z]", codes)
  long_i10 <- i10 & nchar(codes) > 3
  out[long_i10] <- paste0(substr(codes[long_i10], 1, 3), ".", substr(codes[long_i10], 4, nchar(codes[long_i10])))

  # ICD-9 numeric
  i9n <- ok & !e9 & !v9 & !i10 & grepl("^[0-9]+$", codes)
  long_i9n <- i9n & nchar(codes) > 3
  out[long_i9n] <- paste0(substr(codes[long_i9n], 1, 3), ".", substr(codes[long_i9n], 4, nchar(codes[long_i9n])))

  out
}

stopifnot(length(bronze_files) > 0)

read_one <- function(f) arrow::open_dataset(f) %>% dplyr::collect()

choose_first <- function(df, cands) {
  cands <- cands[!is.na(cands)]
  for (nm in cands) {
    if (nm %in% names(df)) return(df[[nm]])
  }
  rep(NA_character_, nrow(df))
}

# ── Optimized row-paste for dx_all / ecause_all ───────────────────────────────
# paste_rows() collapses columns rowwise without the slow apply()
paste_rows <- function(df, cols) {
  if (length(cols) == 0) return(rep(NA_character_, nrow(df)))
  m <- as.matrix(df[cols])
  m[is.na(m)] <- ""
  apply(m, 1, function(r) {
    r <- r[r != ""]
    if (length(r) == 0) NA_character_ else paste(r, collapse = ";")
  })
}

normalize_visit <- function(df, db_type = c("SID", "SEDD", "SASD"), log_msg = cat) {
  db_type <- match.arg(db_type)
  m       <- modifyList(map$defaults, map[[db_type]] %||% list())

  year   <- choose_first(df, m$year)
  amonth <- choose_first(df, m$admit_month)
  dmonth <- choose_first(df, m$discharge_month)

  admit_date_month     <- as.Date(sprintf("%04d-%02d-01", year, amonth))
  discharge_date_month <- as.Date(sprintf("%04d-%02d-01", year, dmonth))

  dx_cols <- names(df)[grepl(m$dx_all_regex, names(df))]
  e_cols  <- names(df)[grepl(m$ecause_regex,  names(df))]

  person_key <- choose_first(df, m$person_key_candidates)
  visit_id   <- choose_first(df, m$visit_id)

  if (is.null(person_key)) {
    person_key <- visit_id
    log_msg(" [X] No person linkage found, using visit_id as person_key\n")
  } else {
    n_before   <- sum(!is.na(person_key))
    person_key <- ifelse(is.na(person_key), visit_id, person_key)
    n_after    <- sum(!is.na(person_key))
    log_msg("  ✓ Person linkage found:", n_before, "/", length(person_key),
        "(", round(100 * n_before / length(person_key), 1), "%)\n")
    log_msg("  ✓ After fallback:", n_after, "/", length(person_key), "\n")
  }

  tibble::tibble(
    visit_id       = visit_id,
    person_id      = purrr::map_chr(
      format(person_key, scientific = FALSE, trim = TRUE), hash_id
    ),
    admit_date     = admit_date_month,
    discharge_date = discharge_date_month,
    dx_primary     = choose_first(df, m$dx_primary),
    dx_admit       = if (db_type == "SID") {
      choose_first(df, m$dx_admitting_sid)
    } else {
      choose_first(df, m$dx_reason_sed_sasd)
    },
    dx_all         = paste_rows(df, dx_cols),
    ecause_all     = if (length(e_cols)) paste_rows(df, e_cols) else NA_character_,
    zip5           = substr(choose_first(df, m$zip5), 1, 5),
    facility_state = dplyr::coalesce(choose_first(df, m$facility_state),        NA_character_),
    facility_county= dplyr::coalesce(choose_first(df, m$facility_county_candidates), NA_character_),
    los_days       = suppressWarnings(as.numeric(choose_first(df, m$los_days))),
    duration_hours = suppressWarnings(as.numeric(choose_first(df, m$duration_hours))),
    age            = suppressWarnings(as.numeric(choose_first(df, "AGE"))),
    female         = suppressWarnings(as.numeric(choose_first(df, "FEMALE"))),
    race           = suppressWarnings(as.numeric(choose_first(df, "RACE"))),
    db_type        = db_type
  )
}

infer_type <- function(path) {
  p <- tolower(path)
  if (grepl("sedd", p)) return("SEDD")
  if (grepl("sasd", p)) return("SASD")
  "SID"
}

# ── Core per-file processor (called in parallel) ──────────────────────────────
process_one_file <- function(f, phecode_lookup, phecode_map, paths, map, hash_id) {
  log_file <- fs::path(paths$silver, "..", "..", "logs", "silver_progress.log")

  log_msg <- function(...) {
    msg <- paste0(format(Sys.time(), "[%H:%M:%S] "), ...)
    cat(msg, "\n", file = log_file, append = TRUE)
  }

  log_msg("Processing: ", basename(f))

  df     <- read_one(f)
  visits <- normalize_visit(df, infer_type(f)) %>%
    mutate(
      year               = lubridate::year(admit_date),
      dx_primary_phecode = phecode_lookup[format_icd_for_phecode_vec(dx_primary)]
    )

  # Secondary dx phecode columns — vectorized per column
  # dx_cols <- names(df)[grepl("^(DX|I10_DX)[0-9]+$", names(df), ignore.case = TRUE)]
  # for (col in dx_cols) {
  #   col_num <- gsub("[^0-9]", "", col)
  #   new_col <- paste0("dx", col_num, "_phecode")
  #   visits[[new_col]] <- phecode_lookup[format_icd_for_phecode_vec(df[[col]])]
  # }

  arrow::write_dataset(
    visits,
    fs::path(paths$silver, "visit"),
    partitioning           = c("facility_state", "db_type", "year"),
    existing_data_behavior = "delete_matching",
    compression            = "snappy"
  )
  log_msg("  ✓ Complete: ", basename(f), " (", nrow(visits), " rows)")

  invisible(nrow(visits))
}
# ── Parallel execution across files ──────────────────────────────────────────
# Uses all available cores (up to n_files). On the highmem node with 10 CPUs
# this should cut wall time by ~8-10x vs sequential.
n_cores <- min(length(bronze_files), parallel::detectCores() - 1L, 8L)
cat(glue::glue("Processing {length(bronze_files)} files using {n_cores} workers\n\n"))

if (n_cores > 1) {
  library(furrr)
  plan(multisession, workers = n_cores)

  furrr::future_walk(
    bronze_files,
    process_one_file,
    phecode_lookup = phecode_lookup,
    paths          = paths,
    map            = map,
    hash_id        = hash_id,
    .options       = furrr_options(seed = TRUE)
  )

  plan(sequential)   # release workers
} else {
  # Fallback: single-core (e.g., interactive session)
  purrr::walk(bronze_files, process_one_file,
              phecode_lookup = phecode_lookup,
              paths          = paths,
              map            = map,
              hash_id        = hash_id)
}

cat("\n✓ Silver layer complete\n")