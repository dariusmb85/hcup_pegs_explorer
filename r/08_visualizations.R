# r/08_visualizations.R
# ExWAS Visualizations

library(arrow)
library(dplyr)
library(ggplot2)
library(tidyr)

# Set publication-ready theme
theme_set(theme_bw(base_size = 12))

# Create output directory
dir.create(path(paths$gold,"figures"), showWarnings = FALSE)

cat("=== Creating ExWAS Visualizations ===\n\n")

# Load results
results <- open_dataset(path(paths$gold,"exwas_result_stratified")) %>% collect()

# Clean up labels
results <- results %>%
  mutate(
    # Exposure labels
    exposure_label = case_when(
      exposure_id == "hms_smoke_light" ~ "Smoke (Light)",
      exposure_id == "hms_smoke_medium" ~ "Smoke (Medium)",
      exposure_id == "hms_smoke_heavy" ~ "Smoke (Heavy)",
      exposure_id == "pm25_black_carbon" ~ "Black Carbon",
      exposure_id == "pm25_dust" ~ "Dust PM2.5",
      exposure_id == "temp_maximum" ~ "Temperature (Max)",
      exposure_id == "temp_minimum" ~ "Temperature (Min)",
      TRUE ~ exposure_id
    ),
    # Outcome labels
    outcome_label = case_when(
      outcome == "asthma_flag" ~ "Asthma",
      outcome == "copd_flag" ~ "COPD",
      outcome == "respiratory_infection_flag" ~ "Respiratory Infection",
      outcome == "cardiovascular_flag" ~ "Cardiovascular",
      outcome == "stroke_flag" ~ "Stroke",
      outcome == "diabetes_flag" ~ "Diabetes",
      outcome == "pregnancy_flag" ~ "Pregnancy",
      outcome == "mental_health_flag" ~ "Mental Health",
      TRUE ~ outcome
    ),
    # Strata labels
    strata_label = case_when(
      strata == "all" ~ "Overall",
      strata == "male" ~ "Males",
      strata == "female" ~ "Females",
      TRUE ~ strata
    ),
    # Model labels
    model_label = case_when(
      model_spec_id == "logit_unadjusted_overall" ~ "Unadjusted",
      model_spec_id == "logit_adjusted_temporal_overall" ~ "Temporal Adjusted",
      model_spec_id == "logit_adjusted_temporal_male" ~ "Temporal Adjusted (Male)",
      model_spec_id == "logit_adjusted_temporal_female" ~ "Temporal Adjusted (Female)",
      TRUE ~ model_spec_id
    )
  )

# ============================================================================
# Plot 1: Manhattan Plot (Overall Temporal Adjusted)
# ============================================================================

cat("Creating Manhattan plot...\n")

manhattan_data <- results %>%
  filter(model_spec_id == "logit_adjusted_temporal_overall") %>%
  mutate(
    neglog10p = -log10(p_value),
    sig_level = case_when(
      p.adj.bonferroni < 0.05 ~ "Bonferroni",
      p.adj.fdr < 0.05 ~ "FDR",
      p_value < 0.05 ~ "p < 0.05",
      TRUE ~ "NS"
    ),
    sig_level = factor(sig_level, levels = c("Bonferroni", "FDR", "p < 0.05", "NS"))
  )

p1 <- ggplot(manhattan_data, aes(x = exposure_label, y = neglog10p, color = sig_level)) +
  geom_hline(yintercept = -log10(0.05), linetype = "dashed", color = "gray50") +
  geom_hline(yintercept = -log10(0.05/nrow(manhattan_data)), linetype = "dashed", color = "red") +
  geom_point(aes(shape = outcome_label), size = 3, alpha = 0.7) +
  scale_color_manual(
    values = c("Bonferroni" = "#E41A1C", "FDR" = "#377EB8",
               "p < 0.05" = "#4DAF4A", "NS" = "gray70"),
    name = "Significance"
  ) +
  scale_shape_manual(values = c(15:22), name = "Outcome") +
  labs(
    title = "ExWAS Manhattan Plot",
    subtitle = "Overall Population, Temporal Adjusted Models",
    x = "Environmental Exposure",
    y = "-log10(p-value)"
  ) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    legend.position = "bottom",
    legend.box = "vertical"
  ) +
  guides(shape = guide_legend(nrow = 2))

ggsave(path(paths$gold,"figures/01_manhattan_plot.png"), p1, width = 12, height = 8, dpi = 300)

# ============================================================================
# Plot 2: Forest Plot - Top 20 Associations
# ============================================================================

cat("Creating forest plot...\n")

forest_data <- results %>%
  filter(model_spec_id == "logit_adjusted_temporal_overall") %>%
  arrange(p_value) %>%
  head(20) %>%
  mutate(
    label = paste0(outcome_label, " ~ ", exposure_label),
    label = factor(label, levels = rev(label))
  )

p2 <- ggplot(forest_data, aes(x = or, y = label)) +
  geom_vline(xintercept = 1, linetype = "dashed", color = "gray50") +
  geom_errorbarh(aes(xmin = or_ci_low, xmax = or_ci_high), height = 0.3, linewidth = 0.5) +
  geom_point(aes(color = outcome_label), size = 3) +
  scale_x_log10(breaks = c(0.5, 0.7, 1.0, 1.2, 1.5)) +
  scale_color_brewer(palette = "Set2", name = "Outcome") +
  labs(
    title = "Forest Plot: Top 20 Associations",
    subtitle = "Overall Population, Temporal Adjusted",
    x = "Odds Ratio (95% CI)",
    y = NULL
  ) +
  theme(legend.position = "bottom")

ggsave(path(paths$gold,"figures/02_forest_plot_top20.png"), p2, width = 10, height = 8, dpi = 300)

# ============================================================================
# Plot 3: Heatmap - All Associations
# ============================================================================

cat("Creating heatmap...\n")

heatmap_data <- results %>%
  filter(model_spec_id == "logit_adjusted_temporal_overall") %>%
  mutate(
    log_or = log(or),
    sig = ifelse(p.adj.fdr < 0.05, "*", "")
  )

p3 <- ggplot(heatmap_data, aes(x = exposure_label, y = outcome_label, fill = log_or)) +
  geom_tile(color = "white", linewidth = 0.5) +
  geom_text(aes(label = sig), size = 6, vjust = 0.7) +
  scale_fill_gradient2(
    low = "#2166AC", mid = "white", high = "#B2182B",
    midpoint = 0,
    name = "log(OR)",
    limits = c(-0.5, 0.5)
  ) +
  labs(
    title = "ExWAS Heatmap: All Exposure-Outcome Associations",
    subtitle = "Overall Population, Temporal Adjusted\n* indicates FDR < 0.05",
    x = "Environmental Exposure",
    y = "Health Outcome"
  ) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    panel.grid = element_blank()
  )

ggsave(path(paths$gold, "figures/03_heatmap_all.png"), p3, width = 10, height = 7, dpi = 300)

# ============================================================================
# Plot 4: Temporal Confounding Comparison
# ============================================================================

cat("Creating temporal confounding plot...\n")

confound_data <- results %>%
  filter(
    outcome == "respiratory_infection_flag",
    exposure_id %in% c("hms_smoke_light", "hms_smoke_medium", "hms_smoke_heavy",
                       "temp_maximum", "temp_minimum"),
    model_spec_id %in% c("logit_unadjusted_overall", "logit_adjusted_temporal_overall")
  ) %>%
  mutate(
    model_label = ifelse(model_spec_id == "logit_unadjusted_overall",
                         "Unadjusted", "Temporal Adjusted")
  )

p4 <- ggplot(confound_data, aes(x = exposure_label, y = or, fill = model_label)) +
  geom_hline(yintercept = 1, linetype = "dashed", color = "gray50") +
  geom_col(position = position_dodge(width = 0.8), width = 0.7) +
  geom_errorbar(
    aes(ymin = or_ci_low, ymax = or_ci_high),
    position = position_dodge(width = 0.8),
    width = 0.3
  ) +
  scale_fill_manual(
    values = c("Unadjusted" = "#FC8D62", "Temporal Adjusted" = "#66C2A5"),
    name = "Model"
  ) +
  labs(
    title = "Impact of Temporal Adjustment",
    subtitle = "Respiratory Infections - Overall Population",
    x = "Environmental Exposure",
    y = "Odds Ratio (95% CI)"
  ) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    legend.position = "bottom"
  )

ggsave(path(paths$gold, "figures/04_temporal_confounding.png"), p4, width = 10, height = 6, dpi = 300)

# ============================================================================
# Plot 5: Sex Stratification - Black Carbon Effects
# ============================================================================

cat("Creating sex stratification plot...\n")

sex_data <- results %>%
  filter(
    exposure_id == "pm25_black_carbon",
    grepl("temporal", model_spec_id)
  ) %>%
  mutate(
    strata_label = factor(strata_label, levels = c("Overall", "Males", "Females"))
  )

p5 <- ggplot(sex_data, aes(x = outcome_label, y = or, color = strata_label)) +
  geom_hline(yintercept = 1, linetype = "dashed", color = "gray50") +
  geom_point(position = position_dodge(width = 0.5), size = 3) +
  geom_errorbar(
    aes(ymin = or_ci_low, ymax = or_ci_high),
    position = position_dodge(width = 0.5),
    width = 0.3,
    linewidth = 0.5
  ) +
  scale_color_manual(
    values = c("Overall" = "#7570B3", "Males" = "#1B9E77", "Females" = "#D95F02"),
    name = "Stratum"
  ) +
  labs(
    title = "Sex-Stratified Effects: Black Carbon",
    subtitle = "Temporal Adjusted Models",
    x = "Health Outcome",
    y = "Odds Ratio (95% CI)"
  ) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    legend.position = "bottom"
  ) +
  coord_flip()

ggsave(path(paths$gold, "figures/05_sex_stratification_black_carbon.png"), p5,
       width = 10, height = 7, dpi = 300)

# ============================================================================
# Plot 6: Volcano Plot
# ============================================================================

cat("Creating volcano plot...\n")

volcano_data <- results %>%
  filter(model_spec_id == "logit_adjusted_temporal_overall") %>%
  mutate(
    log2_or = log2(or),
    neglog10p = -log10(p_value),
    sig_category = case_when(
      p.adj.bonferroni < 0.05 & abs(log2_or) > log2(1.1) ~ "Bonferroni + Effect",
      p.adj.fdr < 0.05 & abs(log2_or) > log2(1.1) ~ "FDR + Effect",
      p_value < 0.05 ~ "p < 0.05",
      TRUE ~ "NS"
    )
  )

p6 <- ggplot(volcano_data, aes(x = log2_or, y = neglog10p)) +
  geom_hline(yintercept = -log10(0.05), linetype = "dashed", color = "gray50") +
  geom_vline(xintercept = c(-log2(1.1), log2(1.1)), linetype = "dashed", color = "gray50") +
  geom_point(aes(color = sig_category), size = 2, alpha = 0.7) +
  scale_color_manual(
    values = c(
      "Bonferroni + Effect" = "#E41A1C",
      "FDR + Effect" = "#377EB8",
      "p < 0.05" = "#4DAF4A",
      "NS" = "gray70"
    ),
    name = "Significance"
  ) +
  labs(
    title = "Volcano Plot: ExWAS Results",
    subtitle = "Overall Population, Temporal Adjusted",
    x = "log2(Odds Ratio)",
    y = "-log10(p-value)"
  ) +
  theme(legend.position = "bottom")

ggsave(path(paths$gold, "figures/06_volcano_plot.png"), p6, width = 10, height = 8, dpi = 300)

# ============================================================================
# Summary
# ============================================================================

cat("\n✓ Visualizations complete!\n\n")
cat("Figures saved to:", path(paths$gold, "figures"), "/\n")
cat("  01_manhattan_plot.png\n")
cat("  02_forest_plot_top20.png\n")
cat("  03_heatmap_all.png\n")
cat("  04_temporal_confounding.png\n")
cat("  05_sex_stratification_black_carbon.png\n")
cat("  06_volcano_plot.png\n")