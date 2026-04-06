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


# Around line 137, after this section:
all_visits <- rbind(all_visits, silver_visits)
all_persons <- rbind(all_persons, new_persons)

# ADD THE DEBUGGING CODE HERE:
cat("  File:", basename(fpath), "\n")
cat("  Current persons table size:", round(object.size(all_persons) / 1024^2, 1), "MB\n")
cat("  Current visits table size:", round(object.size(all_visits) / 1024^2, 1), "MB\n")
cat("  Visits in this file:", nrow(silver_visits), "\n")
cat("  Total persons so far:", nrow(all_persons), "\n")
cat("  Total visits so far:", nrow(all_visits), "\n\n")

# Then continue with existing code:
} # End of for loop


  # ============================================================================
  # Stage 7a: ExWAS Overall (DEPENDS ON exposure_rollup_complete)
  # ============================================================================
  tar_target(
    name = exwas_overall,
    command = {
      exposure_rollup_complete
      system("Rscript r/06_ewas_enhanced.R all")
      fs::path(paths$gold,"exwas_all.parquet")
    },
    format = "file",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "controller_highmem"))
  ),
  # ============================================================================
  # Stage 7b: ExWAS Male (DEPENDS ON exposure_rollup_complete)
  # ============================================================================
  tar_target(
    name = exwas_male,
    command = {
      exposure_rollup_complete
      system("Rscript r/06_ewas_enhanced.R male")
      fs::path(paths$gold,"exwas_male.parquet")
    },
    format = "file",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "controller_highmem"))
  ),

  # ============================================================================
  # Stage 7c: ExWAS Female (DEPENDS On exposure_rollup_complete)
  # ============================================================================
  tar_target(
    name = exwas_female,
    command = {
      exposure_rollup_complete
      system("Rscript r/06_ewas_enhanced.R female")
      fs::path(paths$gold,"exwas_female.parquet")
    },
    format = "file",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "controller_highmem"))
  ),

  # ============================================================================
  # Stage 8: Combine Results (DEPENDS ON all 3 ExWAS targets)
  # ============================================================================
  tar_target(
    name = exwas_combined,
    command = {
      exwas_overall  # ← DEPENDENCY 1
      exwas_male     # ← DEPENDENCY 2
      exwas_female   # ← DEPENDENCY 3
      library(arrow)
      library(dplyr)

      # FIX: Use actual file paths, not target names
      overall_file <- fs::path(paths$gold, "exwas_all.parquet")
      male_file <- fs::path(paths$gold, "exwas_male.parquet")
      female_file <- fs::path(paths$gold, "exwas_female.parquet")

      overall <- if (file.exists(overall_file)) {
        arrow::read_parquet(overall_file)
      } else NULL

      male <- if (file.exists(male_file)) {
        arrow::read_parquet(male_file)
      } else NULL

      female <- if (file.exists(female_file)) {
        arrow::read_parquet(female_file)
      } else NULL

      all_results <- dplyr::bind_rows(overall, male, female) %>%
        dplyr::arrange(p_value)

      arrow::write_parquet(all_results,
                           fs::path(paths$gold, "exwas_combined_results.parquet"),
                           compression = 'snappy')

      fs::path(paths$gold, "exwas_combined_results.parquet")
    },
    format = "file",
    deployment = "main")
  ,
  # ============================================================================
  # Stage 09: Final Report (DEPENDS ON exwas_combined)
  # ============================================================================
  tar_target(
    name = final_report,
    command = {
      exwas_combined  # ← EXPLICIT DEPENDENCY

      library(arrow)
      library(dplyr)

      results <- arrow::read_parquet(fs::path(paths$gold, "exwas_combined_results.parquet"))

      summary <- list(
        total_tests = nrow(results),
        sig_p05 = sum(results$p_value < 0.05, na.rm = TRUE),
        sig_fdr = sum(results$p.adj.fdr < 0.05, na.rm = TRUE),
        sig_bonf = sum(results$p.adj.bonferroni < 0.05, na.rm = TRUE),
        by_model = results %>%
          dplyr::group_by(model_spec_id) %>%
          dplyr::summarise(
            n_tests = dplyr::n(),
            n_sig_fdr = sum(p.adj.fdr < 0.05, na.rm = TRUE),
            .groups = "drop"
          )
      )

      saveRDS(summary, fs::path(paths$gold,"exwas_summary.rds"))

      cat("\n=== PIPELINE COMPLETE ===\n")
      cat("Total tests:", summary$total_tests, "\n")
      cat("Significant (FDR<0.05):", summary$sig_fdr, "\n\n")
      print(summary$by_model)

      fs::path(paths$gold,"exwas_summary.rds")
    },
    format = "file",
    deployment = "main"
  )