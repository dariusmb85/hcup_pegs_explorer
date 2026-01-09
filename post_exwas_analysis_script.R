library(arrow)
library(dplyr)
library(ggplot2)

results <- open_dataset("data_test/gold/exwas_result_stratified") %>% collect()

cat("=== Top 20 with Full P-values ===\n\n")

top20_detailed <- results %>%
  arrange(p_value) %>%
  head(20) %>%
  mutate(
    or_ci = sprintf("%.2f (%.2f-%.2f)", or, or_ci_low, or_ci_high),
    p_scientific = format(p_value, scientific = TRUE, digits = 3),
    fdr_scientific = format(p.adj.fdr, scientific = TRUE, digits = 3)
  ) %>%
  select(strata, model_spec_id, outcome, exposure_id, n_cases, or_ci,
         p_scientific, fdr_scientific)

print(top20_detailed, n = 20, width = 150)

# Key findings summary
cat("\n=== Key Findings ===\n\n")

# 1. Strongest associations (FDR < 0.05, temporal adjusted)
strongest <- results %>%
  filter(grepl("temporal", model_spec_id), p.adj.fdr < 0.05) %>%
  arrange(p_value) %>%
  head(10) %>%
  mutate(
    direction = ifelse(or > 1, "Risk", "Protective"),
    effect_size = abs(log(or))
  ) %>%
  select(strata, outcome, exposure_id, or, or_ci_low, or_ci_high,
         direction, p_value, p.adj.fdr, n_cases)

cat("Top 10 Robust Associations (Temporal Adjusted, FDR < 0.05):\n")
print(strongest, n = 10)

# 2. Temporal confounding examples
cat("\n\n=== Temporal Confounding Impact ===\n")
cat("Comparing Unadjusted vs Temporal Adjusted for same associations:\n\n")

confounding_check <- results %>%
  filter(outcome == "respiratory_infection_flag",
         exposure_id %in% c("hms_smoke_light", "temp_maximum")) %>%
  select(model_spec_id, exposure_id, or, p_value, p.adj.fdr) %>%
  arrange(exposure_id, model_spec_id)

print(confounding_check)

# 3. Sex differences
cat("\n\n=== Sex-Specific Effects ===\n")
cat("Associations that differ by sex (temporal adjusted models):\n\n")

sex_comparison <- results %>%
  filter(grepl("temporal", model_spec_id)) %>%
  select(strata, model_spec_id, outcome, exposure_id, or, p_value) %>%
  tidyr::pivot_wider(
    names_from = strata,
    values_from = c(or, p_value),
    names_sep = "_"
  ) %>%
  filter(!is.na(or_male) & !is.na(or_female)) %>%
  mutate(
    or_ratio = or_male / or_female,
    both_sig = p_value_male < 0.05 & p_value_female < 0.05,
    different_direction = (or_male > 1 & or_female < 1) | (or_male < 1 & or_female > 1)
  ) %>%
  filter(both_sig & (abs(or_ratio - 1) > 0.2 | different_direction)) %>%
  arrange(desc(abs(or_ratio - 1))) %>%
  select(outcome, exposure_id, or_male, or_female, or_ratio,
         p_value_male, p_value_female)

if (nrow(sex_comparison) > 0) {
  print(sex_comparison, n = 20)
} else {
  cat("No major sex differences detected (OR ratio > 20% difference)\n")
}

# 4. Most robust associations across all models
cat("\n\n=== Most Robust Associations ===\n")
cat("Significant in ALL model specifications:\n\n")

robust <- results %>%
  group_by(outcome, exposure_id, strata) %>%
  summarise(
    n_models = n(),
    n_sig = sum(p.adj.fdr < 0.05),
    mean_or = mean(or),
    min_p = min(p_value),
    .groups = "drop"
  ) %>%
  filter(n_sig == n_models, n_models > 1) %>%
  arrange(min_p) %>%
  head(15)

print(robust, n = 15)
