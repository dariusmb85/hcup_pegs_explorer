# HCUP PEGS Explorer Pipeline Makefile
# Supports both local and SLURM execution

PARQUET_ROOT := $(PWD)/data_test
R := Rscript
SLURM := sbatch
SQUEUE := squeue -u $(USER)

# SLURM configuration
SLURM_PARTITION := highmem
SLURM_TIME := 08:00:00
SLURM_MEM := 72g
SLURM_CPUS := 6

.PHONY: help init pipeline clean clean-all
.PHONY: silver geocode exposures cohort join rollup exwas quality
.PHONY: slurm-pipeline slurm-exwas check-slurm cancel-all

# ============================================================================
# Help
# ============================================================================

help:
	@echo "HCUP PEGS Explorer Pipeline"
	@echo "============================"
	@echo ""
	@echo "Setup:"
	@echo "  make init           - Install R packages"
	@echo ""
	@echo "Local Pipeline (runs on login node):"
	@echo "  make pipeline       - Run full pipeline locally"
	@echo "  make silver         - Step 1: HCUP harmonization"
	@echo "  make geocode        - Step 2: ZIP → tract geocoding"
	@echo "  make exposures      - Step 3: Download Dataverse exposures"
	@echo "  make cohort         - Step 4: Build person-month cohort"
	@echo "  make join           - Step 5: Join exposures to cohort"
	@echo "  make rollup         - Step 6: Create exposure rollup"
	@echo "  make exwas          - Step 7: Run ExWAS (local, slow)"
	@echo ""
	@echo "SLURM Pipeline (submits to cluster):"
	@echo "  make slurm-pipeline - Submit full pipeline to SLURM"
	@echo "  make slurm-exwas    - Submit only ExWAS to SLURM (parallel)"
	@echo "  make check-slurm    - Check SLURM job status"
	@echo "  make cancel-all     - Cancel all your SLURM jobs"
	@echo ""
	@echo "Quality & Validation:"
	@echo "  make quality        - Run data quality checks"
	@echo ""
	@echo "Applications:"
	@echo "  make shiny      - Launch Shiny app"
	@echo "  make api        - Launch FastAPI"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean          - Remove generated data (keeps bronze)"
	@echo "  make clean-all      - Remove ALL data"
	@echo ""
	@echo "Variables:"
	@echo "  PARQUET_ROOT=$(PARQUET_ROOT)"
	@echo "  SLURM_PARTITION=$(SLURM_PARTITION)"

# ============================================================================
# Setup
# ============================================================================

init:
	@echo "==> Installing R packages..."
	$(R) -e "pkgs <- c('arrow','dplyr','tidyr','purrr','stringr','lubridate','jsonlite','yaml','digest','readr','sf','httr','janitor','broom','glue','fs','here','dotenv'); install.packages(pkgs[!pkgs %in% installed.packages()], repos='https://cloud.r-project.org')"
	@echo "✓ R packages installed"

# ============================================================================
# Local Pipeline Steps
# ============================================================================

silver:
	@echo "==> Step 1: Creating silver layer..."
	@mkdir -p logs
	$(R) r/01_hcup_silver.R 2>&1 | tee logs/01_silver_$$(date +%Y%m%d_%H%M%S).log
	@echo "✓ Silver layer complete"

geocode: silver
	@echo "==> Step 2: Geocoding..."
	@mkdir -p logs
	$(R) r/015_geocode_enrich.R 2>&1 | tee logs/02_geocode_$$(date +%Y%m%d_%H%M%S).log
	@echo "✓ Geocoding complete"

exposures:
	@echo "==> Step 3: Downloading exposures from Dataverse..."
	@mkdir -p logs
	$(R) r/02_dataverse_exposures.R 2>&1 | tee logs/03_exposures_$$(date +%Y%m%d_%H%M%S).log
	@echo "✓ Exposures downloaded"

cohort: geocode
	@echo "==> Step 4: Building person-month cohort..."
	@mkdir -p logs
	$(R) r/04_person_monthV2.R 2>&1 | tee logs/04_cohort_$$(date +%Y%m%d_%H%M%S).log
	@echo "✓ Cohort built"

join: cohort exposures
	@echo "==> Step 5: Joining exposures to cohort..."
	@mkdir -p logs
	$(R) r/05_join_exposures.R 2>&1 | tee logs/05_join_$$(date +%Y%m%d_%H%M%S).log
	@echo "✓ Join complete"

rollup: join
	@echo "==> Step 6: Creating exposure rollup..."
	@mkdir -p logs
	$(R) r/03_exposure_rollup.R 2>&1 | tee logs/06_rollup_$$(date +%Y%m%d_%H%M%S).log
	@echo "✓ Rollup complete"

exwas: rollup
	@echo "==> Step 7: Running ExWAS (WARNING: slow on login node)..."
	@echo "    Consider using 'make slurm-exwas' instead"
	@mkdir -p logs
	$(R) r/06_exwas_stratified.R 2>&1 | tee logs/07_exwas_$$(date +%Y%m%d_%H%M%S).log
	@echo "✓ ExWAS complete"

quality:
	@echo "==> Running data quality checks..."
	@mkdir -p logs
	$(R) r/07_data_quality.R 2>&1 | tee logs/quality_$$(date +%Y%m%d_%H%M%S).log
	@echo "✓ Quality checks complete"

# ============================================================================
# Complete Pipelines
# ============================================================================

pipeline: silver geocode exposures cohort join rollup
	@echo ""
	@echo "════════════════════════════════════════"
	@echo "✓ Pipeline complete (except ExWAS)!"
	@echo "════════════════════════════════════════"
	@echo ""
	@echo "Next: Run ExWAS with SLURM for speed:"
	@echo "  make slurm-exwas"
	@echo ""
	@echo "Or run locally (slow):"
	@echo "  make exwas"

# ============================================================================
# SLURM Pipeline
# ============================================================================

slurm-pipeline:
	@echo "==> Submitting full pipeline to SLURM..."
	@mkdir -p logs
	$(SLURM) --partition=$(SLURM_PARTITION) \
	         --time=12:00:00 \
	         --mem=64g \
	         --cpus-per-task=4 \
	         --output=logs/pipeline_%j.log \
	         --error=logs/pipeline_%j.err \
	         --job-name=hcup_pipeline \
	         --wrap="make pipeline"
	@echo "✓ Pipeline submitted. Check status with: make check-slurm"

slurm-exwas: rollup
	@echo "==> Submitting ExWAS (3 parallel jobs) to SLURM..."
	@mkdir -p logs
	@# Submit overall stratum
	$(SLURM) --partition=$(SLURM_PARTITION) \
	         --time=16:00:00 \
	         --mem=$(SLURM_MEM) \
	         --cpus-per-task=$(SLURM_CPUS) \
	         --output=logs/exwas_overall_%j.log \
	         --error=logs/exwas_overall_%j.err \
	         --job-name=exwas_overall \
	         submit_exwas_overall.slurm
	@# Submit male stratum
	$(SLURM) --partition=$(SLURM_PARTITION) \
	         --time=16:00:00 \
	         --mem=$(SLURM_MEM) \
	         --cpus-per-task=$(SLURM_CPUS) \
	         --output=logs/exwas_male_%j.log \
	         --error=logs/exwas_male_%j.err \
	         --job-name=exwas_male \
	         submit_exwas_male.slurm
	@# Submit female stratum
	$(SLURM) --partition=$(SLURM_PARTITION) \
	         --time=16:00:00 \
	         --mem=$(SLURM_MEM) \
	         --cpus-per-task=$(SLURM_CPUS) \
	         --output=logs/exwas_female_%j.log \
	         --error=logs/exwas_female_%j.err \
	         --job-name=exwas_female \
	         submit_exwas_female.slurm
	@echo "✓ 3 ExWAS jobs submitted"
	@echo ""
	@echo "Monitor progress:"
	@echo "  make check-slurm"
	@echo "  tail -f logs/exwas_*.log"

check-slurm:
	@echo "Your SLURM jobs:"
	@$(SQUEUE) || echo "No jobs running"

cancel-all:
	@echo "Cancelling all your SLURM jobs..."
	scancel -u $(USER)
	@echo "✓ All jobs cancelled"

# ============================================================================
# SLURM Job Scripts
# ============================================================================

submit_exwas_overall.slurm:
	@echo "#!/bin/bash" > $@
	@echo "cd $(PWD)" >> $@
	@echo 'echo "Running overall stratum ExWAS..."' >> $@
	@echo '$(R) -e '"'"'source("r/06_exwas_stratified.R"); library(yaml); model_cfg <- read_yaml("config/covariates.yaml")$$exwas_models; models <- Filter(function(x) grepl("_overall$$", x$$id), model_cfg); source("r/00_env.R"); library(arrow); library(dplyr); library(broom); pm <- open_dataset("data_test/gold/person_month") %>% collect(); ex <- open_dataset("data_test/gold/exposure_rollup") %>% filter(metric == "mean") %>% collect(); ex_wide <- ex %>% select(person_id, ym, exposure_id, value) %>% pivot_wider(names_from = exposure_id, values_from = value); wide <- pm %>% left_join(ex_wide, by = c("person_id", "ym")); results <- bind_rows(lapply(models, function(spec) run_exwas_model(wide, unique(ex$$exposure_id), names(wide)[grepl("_flag$$", names(wide))], spec))); write_parquet(results, "data_test/gold/exwas_overall.parquet")'"'"'' >> $@
	@chmod +x $@

submit_exwas_male.slurm: submit_exwas_overall.slurm
	@sed 's/_overall/_male/g' $< | sed 's/exwas_overall.parquet/exwas_male.parquet/' > $@
	@chmod +x $@

submit_exwas_female.slurm: submit_exwas_overall.slurm
	@sed 's/_overall/_female/g' $< | sed 's/exwas_overall.parquet/exwas_female.parquet/' > $@
	@chmod +x $@

# ============================================================================
# Cleanup
# ============================================================================

clean:
	@echo "Cleaning generated data (keeping bronze)..."
	rm -rf $(PARQUET_ROOT)/silver/*
	rm -rf $(PARQUET_ROOT)/gold/*
	rm -rf logs/*.log
	@echo "✓ Clean complete"

clean-all:
	@echo "⚠️  WARNING: This will delete ALL data including bronze!"
	@echo "Press Ctrl+C to cancel, or Enter to continue..."
	@read confirmation
	rm -rf $(PARQUET_ROOT)/bronze/*
	rm -rf $(PARQUET_ROOT)/silver/*
	rm -rf $(PARQUET_ROOT)/gold/*
	rm -rf logs/*
	@echo "✓ All data removed"

# ============================================================================
# Phony targets
# ============================================================================

.PHONY: submit_exwas_overall.slurm submit_exwas_male.slurm submit_exwas_female.slurm
MAKEFILE_EOF

echo "✓ Makefile updated"