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




model_cfg <- yaml::read_yaml(here::here("config", "covariates.yaml"))$exwas_models
print(names(model_cfg))
print(length(model_cfg))


Running model: logit_unadjusted_overall
  Strata: all
  Formula: {outcome} ~ scale({exposure})
  Total tests: 4
  Successful fits: 0