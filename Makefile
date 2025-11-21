PARQUET_ROOT := $(PWD)/data
R := R --vanilla --quiet

.PHONY: init silver geocode cohort ingest extract roll exwas quality shiny api clean help

help:
	@echo "PEGS Explorer Pipeline"
	@echo "======================"
	@echo ""
	@echo "Setup:"
	@echo "  make init       - Install R packages"
	@echo ""
	@echo "Data Pipeline:"
	@echo "  make silver     - HCUP bronze → silver harmonization"
	@echo "  make geocode    - Enrich visits with Census tracts"
	@echo "  make cohort     - Build person-month cohort"
	@echo "  make ingest     - Download/process Amadeus data (one-time)"
	@echo "  make extract    - Extract daily exposures"
	@echo "  make roll       - Roll up exposures to person-month"
	@echo "  make exwas      - Run environment-wide association study"
	@echo ""
	@echo "Quality & Validation:"
	@echo "  make quality    - Run data quality checks"
	@echo ""
	@echo "Applications:"
	@echo "  make shiny      - Launch Shiny app"
	@echo "  make api        - Launch FastAPI"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean      - Remove generated data (keeps bronze)"
	@echo "  make clean-all  - Remove ALL data including bronze"

init:
	$(R) -e "install.packages(c('arrow','dplyr','tidyr','purrr','stringr','lubridate','jsonlite','yaml','digest','readr','sf','readxl','janitor','scales','ggrepel'))"
	@echo "✓ R packages installed"

silver:
	@echo "==> Creating silver layer from bronze..."
	$(R) -f r/01_hcup_silver.R
	@echo "✓ Silver layer complete"

geocode:
	@echo "==> Enriching with Census tracts..."
	$(R) -f r/015_geocode_enrich.R
	@echo "✓ Geocoding complete"

cohort:
	@echo "==> Building person-month cohort..."
	$(R) -f r/04_person_month_v2.R
	@echo "✓ Cohort built"

ingest:
	@echo "==> Downloading and processing Amadeus data..."
	$(R) -f r/02_amadeus_ingest.R
	@echo "✓ Amadeus data cached"

extract:
	@echo "==> Extracting daily exposures..."
	$(R) -f r/03_exposure_extract.R
	@echo "✓ Exposure extraction complete"

roll:
	@echo "==> Rolling up exposures to person-month..."
	$(R) -f r/05_join_exposures.R
	@echo "✓ Rollup complete"

exwas:
	@echo "==> Running ExWAS..."
	$(R) -f r/06_exwas_v2.R
	@echo "✓ ExWAS complete"

quality:
	@echo "==> Running data quality checks..."
	$(R) -f r/07_data_quality.R

# Full pipeline from bronze → ExWAS
pipeline: silver geocode cohort extract roll exwas
	@echo ""
	@echo "════════════════════════════════════════"
	@echo "✓ Full pipeline complete!"
	@echo "════════════════════════════════════════"
	@echo ""
	@echo "Next steps:"
	@echo "  make quality  - Validate results"
	@echo "  make shiny    - Explore interactively"
	@echo "  make api      - Launch REST API"

shiny:
	@echo "==> Launching Shiny app..."
	@echo "    Navigate to http://localhost:XXXX in your browser"
	PARQUET_ROOT=$(PARQUET_ROOT) $(R) -f shiny/app_enhanced.R

api:
	@echo "==> Launching FastAPI..."
	@echo "    API docs: http://localhost:8000/docs"
	PARQUET_ROOT=$(PARQUET_ROOT) uvicorn api.main:app --reload --port 8000

clean:
	@echo "Cleaning generated data (keeping bronze)..."
	rm -rf $(PARQUET_ROOT)/silver/*
	rm -rf $(PARQUET_ROOT)/gold/*
	@echo "✓ Clean complete"

clean-all:
	@echo "WARNING: This will delete ALL data including bronze!"
	@echo "Press Ctrl+C to cancel, or Enter to continue..."
	@read confirmation
	rm -rf $(PARQUET_ROOT)/bronze/*
	rm -rf $(PARQUET_ROOT)/silver/*
	rm -rf $(PARQUET_ROOT)/gold/*
	@echo "✓ All data removed"