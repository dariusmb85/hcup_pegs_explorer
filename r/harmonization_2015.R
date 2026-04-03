source(here::here("r", "00_env.R"))

harmonize_2015_quarters <- function() {
  cat("=== Harmonizing 2015 Quarterly Files ===\n\n")

  # Find all 2015 quarterly files
  q1q3_files <- list.files(paths$bronze, pattern = "2015.*q1q3", full.names = TRUE)
  q4_files <- list.files(paths$bronze, pattern = "2015.*q4", full.names = TRUE)

  if (length(q1q3_files) == 0 && length(q4_files) == 0) {
    cat("No 2015 quarterly files found, skipping harmonization\n")
    return()
  }

  # Extract state_dtype combinations (e.g., "UT_SEDD", "NC_SID")
  state_dtype_q1q3 <- gsub("(.*)_2015.*", "\\1", basename(q1q3_files))
  state_dtype_q4 <- gsub("(.*)_2015.*", "\\1", basename(q4_files))

  # Process each state+dtype combination that has both quarters
  common_combos <- intersect(state_dtype_q1q3, state_dtype_q4)

  if (length(common_combos) == 0) {
    cat("No matching Q1Q3/Q4 pairs found\n")
    return()
  }

  for (combo in common_combos) {
    cat("Processing:", combo, "_2015\n")

    # Find matching files
    q1q3_file <- q1q3_files[grepl(combo, basename(q1q3_files))][1]
    q4_file <- q4_files[grepl(combo, basename(q4_files))][1]

    # Read both files
    q1q3 <- arrow::read_parquet(q1q3_file)
    q4 <- arrow::read_parquet(q4_file)

    # Harmonize Q4 column names to match Q1Q3
    names(q4) <- gsub("^I10_DX", "DX", names(q4))
    names(q4) <- gsub("^I10_ECAUSE", "ECODE", names(q4))
    names(q4) <- gsub("^I10_DX_Visit_Reason", "DX_Visit_Reason", names(q4))

    # Convert all shared columns to character to avoid type conflicts
    shared_cols <- intersect(names(q1q3), names(q4))

    for(col in shared_cols) {
      q1q3[[col]] <- as.character(q1q3[[col]])
      q4[[col]] <- as.character(q4[[col]])
    }

    # Combine them
    combined_2015 <- bind_rows(q1q3, q4)

    # Write unified file
    output_file <- file.path(paths$bronze, paste0(combo, "_2015_COMBINED.parquet"))
    arrow::write_parquet(combined_2015, output_file, compression = "snappy")

    cat("  ✓ Created:", basename(output_file), "\n")
    cat("    Q1-Q3 rows:", format(nrow(q1q3), big.mark=","), "\n")
    cat("    Q4 rows:", format(nrow(q4), big.mark=","), "\n")
    cat("    Combined rows:", format(nrow(combined_2015), big.mark=","), "\n\n")
  }

  cat("✓ 2015 harmonization complete\n")
}

# Run the function
harmonize_2015_quarters()