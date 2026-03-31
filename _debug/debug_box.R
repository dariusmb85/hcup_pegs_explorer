tar_target(
  name = exwas_overall,
  command = {
    exposure_rollup_complete

    # Your existing ExWAS code...
    results <- dplyr::bind_rows(
      lapply(models_overall, function(spec) {
        run_exwas_model(wide, exposure_cols, outcome_cols, spec)
      })
    )

    # ADD DEBUG HERE:
    cat("Results columns:", paste(names(results), collapse=", "), "\n")
    cat("Results rows:", nrow(results), "\n")
    if(nrow(results) > 0) cat("First few model_spec_id values:", head(results$model_spec_id), "\n")

    # Then the grouping that's failing:
    results <- results %>%
      dplyr::group_by(model_spec_id) %>%  # This line is failing
      dplyr::mutate(
        p.adj.fdr = p.adjust(p_value, method = "fdr"),
        p.adj.bonferroni = p.adjust(p_value, method = "bonferroni")
      ) %>%
      dplyr::ungroup()




# In your ExWAS target, add this debug before model fitting:
cat("Data columns:", paste(names(wide), collapse=", "), "\n")
cat("Exposure columns found:", paste(exposure_cols, collapse=", "), "\n")
cat("Outcome columns found:", paste(outcome_cols, collapse=", "), "\n")
cat("Data rows:", nrow(wide), "\n")

# Check for specific columns the formula needs
cat("Has 'year' column:", "year" %in% names(wide), "\n")
cat("Has 'season' column:", "season" %in% names(wide), "\n")

# Try fitting one model manually to see the error:
outcome <- outcome_cols[1]  # e.g., "asthma_flag"
exposure <- exposure_cols[1]  # e.g., "pm25_black_carbon"

# Check if these columns exist and have data
cat("Outcome values:", table(wide[[outcome]], useNA="always"), "\n")
cat("Exposure values:", summary(wide[[exposure]]), "\n")

# Try the actual formula
formula_str <- "{outcome} ~ scale({exposure})"
formula_filled <- glue::glue(formula_str, outcome=outcome, exposure=exposure)
cat("Formula:", formula_filled, "\n")

# Try fitting
try({
  fit <- glm(as.formula(formula_filled), data=wide, family=binomial())
  cat("Model fit successfully!\n")
})

  }
}
# Convert list columns to proper numeric, handling NULLs
exposure_cols_clean <- exposure_cols

for(col in exposure_cols_clean) {
  wide[[col]] <- sapply(wide[[col]], function(x) {
    if(is.null(x) || length(x) == 0) return(NA_real_)
    as.numeric(x)[1]  # Take first value if somehow a vector
  })
}

# Verify the fix
cat("After cleaning - hms_smoke_heavy type:", class(wide$hms_smoke_heavy), "\n")
cat("Sample values:", head(wide$hms_smoke_heavy, 5), "\n")
cat("Any remaining lists?", any(sapply(wide[exposure_cols_clean], is.list)), "\n")


>
>       ex_wide <- ex %>%
+         dplyr::select(person_id, ym, exposure_id, value) %>%
+         tidyr::pivot_wider(names_from = exposure_id, values_from = value)
Warning message:
Values from `value` are not uniquely identified; output will contain list-cols.
• Use `values_fn = list` to suppress this warning.
• Use `values_fn = {summary_fun}` to summarise duplicates.
• Use the following dplyr code to identify duplicates.
  {data} %>%
  dplyr::group_by(person_id, ym, exposure_id) %>%
  dplyr::summarise(n = dplyr::n(), .groups = "drop") %>%
  dplyr::filter(n > 1L)
