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


    > print(length(model_cfg))
[1] 4
> model_cfg
[[1]]
[[1]]$id
[1] "logit_unadjusted_overall"

[[1]]$strata
[1] "all"

[[1]]$outcome_type
[1] "binary"

[[1]]$family
[1] "binomial"

[[1]]$formula_template
[1] "{outcome} ~ scale({exposure})"

[[1]]$description
[1] "Unadjusted - All"


[[2]]
[[2]]$id
[1] "logit_adjusted_temporal_overall"

[[2]]$strata
[1] "all"

[[2]]$outcome_type
[1] "binary"

[[2]]$family
[1] "binomial"

[[2]]$formula_template
[1] "{outcome} ~ scale({exposure}) + year + factor(season)"

[[2]]$description
[1] "Temporal adjusted - All"


[[3]]
[[3]]$id
[1] "logit_adjusted_temporal_male"

[[3]]$strata
[1] "male"

[[3]]$outcome_type
[1] "binary"

[[3]]$family
[1] "binomial"

[[3]]$formula_template
[1] "{outcome} ~ scale({exposure}) + year + factor(season)"

[[3]]$description
[1] "Temporal adjusted - Males"


[[4]]
[[4]]$id
[1] "logit_adjusted_temporal_female"

[[4]]$strata
[1] "female"

[[4]]$outcome_type
[1] "binary"

[[4]]$family
[1] "binomial"

[[4]]$formula_template
[1] "{outcome} ~ scale({exposure}) + year + factor(season)"

[[4]]$description
[1] "Temporal adjusted - Females"

