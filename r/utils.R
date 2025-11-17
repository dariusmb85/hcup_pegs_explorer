hash_id <- function(..., salt = Sys.getenv("PERSON_ID_SALT")) {
  stopifnot(nzchar(salt))

  digest::digest(
    paste(c(..., salt), collapse = "|"),
    algo = "sha256"
  )
}

write_parquet_ds <- function(df,
                             out_dir,
                             partitioning = NULL,
                             filename = NULL) {

  arrow::write_dataset(
    df,
    out_dir,
    partitioning = partitioning,
    existing_data_behavior = "overwrite_or_ignore"
  )

  invisible(out_dir)
}

read_ds <- function(dir) {
  arrow::open_dataset(dir, format = "parquet")
}

suppress_counts <- function(df,
                            thresh = 11L,
                            cols = c("n")) {

  dplyr::mutate(
    df,
    dplyr::across(
      dplyr::all_of(cols),
      ~ ifelse(.x < thresh, NA, .x)
    )
  )
}

# Ensures a package is installed and loaded
ensure_package <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg)
  }
  invisible(TRUE)
}

# Vectorized wrapper
ensure_packages <- function(pkgs) {
  invisible(lapply(pkgs, ensure_package))
}