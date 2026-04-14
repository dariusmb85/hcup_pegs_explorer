# HCUP PEGS Explorer

Environment-Wide Association Study (ExWAS) platform linking administrative health data (HCUP) with environmental exposures, modeled after the NIEHS [PEGS (Personalized Environment and Genes Study)](https://www.niehs.nih.gov/research/clinical/studies/pegs) approach.

---

## Overview

This pipeline analyzes associations between area-level environmental exposures (wildfire smoke, air pollution, temperature) and health outcomes using hospital administrative data from the Healthcare Cost and Utilization Project (HCUP).

**Key Features:**
- Fully automated end-to-end pipeline orchestrated with `{targets}` + `{crew}` on SLURM
- Parallel processing via `{furrr}` for silver harmonization and person-month cohort building
- Monthly temporal resolution preserving acute exposure effects
- Sex-stratified analysis with temporal confounding controls
- Dual ICD-9/ICD-10 phenotype definitions via PheCode mapping
- SHA-256 person ID hashing for PHI protection
- Scalable partitioned Parquet output throughout (Apache Arrow)
- Config-driven ExWAS via `analysis_spec.yaml` — no code changes needed between runs
- Interactive Shiny explorer for pre-computed ExWAS results *(in development)*

**Production Status:**
- 4 states: Colorado (2017–2020), North Carolina (2007–2020), Oregon (2015–2021), Utah (2000–2020)
- ~109M raw visits → ~111M harmonized visits after geocoding
- 8 phenotypes, 10 exposures, 4 model specifications
- Full pipeline runtime: ~4 hours on NIEHS SLURM highmem partition

---

## Pipeline Architecture

```
┌─────────────────────────────────┐
│         HCUP Bronze             │  Raw state parquet files (SID, SEDD, SASD)
│  CO·NC·OR·UT  │  2000–2021      │  Includes 2015 quarterly harmonization
└──────────────┬──────────────────┘
               │  harmonize_2015 → bronze_files
               ▼
┌─────────────────────────────────┐
│           Silver Layer          │  Harmonized visits + demographics
│  01_hcup_silver.R               │  Parallel: 8 furrr workers
│  ICD→PheCode mapping            │  ~57 min (SLURM main process)
└──────────────┬──────────────────┘
               │
        ┌──────┴──────┐
        ▼             ▼
┌──────────────┐  ┌───────────────────┐
│  Geocoding   │  │    Exposures      │
│  015_geocode │  │  02_dataverse     │
│  ZIP→Tract   │  │  HMS·MERRA2·      │
│  99.6% match │  │  TerraClimate     │
│  ~15 min     │  │  ~5 min           │
└──────┬───────┘  └────────┬──────────┘
       │                   │
       ▼                   │
┌──────────────┐           │
│  QC+Cleaning │           │
│  07_data_    │           │
│  quality.R   │           │
│  ~2h 8m      │           │
└──────┬───────┘           │
       │                   │
       ▼                   │
┌──────────────┐           │
│ Person-Month │           │
│   Cohort     │           │
│ 04_person_   │           │
│ monthV2.R    │           │
│ Parallel:    │           │
│ state×year   │           │
│ chunks       │           │
│ ~1h 52m      │           │
└──────┬───────┘           │
       │                   │
       └──────────┬────────┘
                  ▼
         ┌─────────────────┐
         │   Join          │  Person-months × Exposures
         │ 05_join_        │  by person_id + ym
         │ exposures.R     │
         │ ~6 min          │
         └────────┬────────┘
                  ▼
         ┌─────────────────┐
         │   Rollup        │  Long format: mean/max/p90/sum
         │ 03_exposure_    │  per person × month × exposure
         │ rollup.R        │
         │ ~33 min         │
         └────────┬────────┘
                  ▼
         ┌─────────────────┐
         │    ExWAS        │  Config-driven stratified logistic
         │ 06_exwas_       │  regression
         │ stratified.R    │
         └─────────────────┘
```

**Total pipeline runtime: ~4 hours on NIEHS SLURM highmem partition**

---

## Data Sources

### Health Data: HCUP
| Database | Description | States Available |
|---|---|---|
| SID | State Inpatient Databases | CO, NC, OR, UT |
| SEDD | State Emergency Department Databases | CO, NC, OR, UT |
| SASD | State Ambulatory Surgery Databases | — |

### Environmental Data: Harvard Dataverse
Pre-aggregated monthly ZIP-level exposures from the [Amadeus-aggregated dataset](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/0WILGX):

| ID | Source | Description |
|---|---|---|
| `hms_smoke_light/medium/heavy` | NOAA HMS | Wildfire smoke coverage by density |
| `pm25_dust` | MERRA-2 | Dust PM2.5 surface mass concentration |
| `pm25_black_carbon` | MERRA-2 | Black carbon surface mass concentration |
| `ozone_daily_max_8hr` | AQS | Daily max 8-hour ozone |
| `temp_min/max` | gridMET | Daily min/max temperature |
| `temp_minimum/maximum` | TerraClimate | Monthly min/max temperature |

### Geocoding: HUD USPS Crosswalk
- ZIP Code → Census Tract mapping
- **Match rate: 99.6%** (111.5M → 109.5M matched visits)

---

## Installation

### Requirements
- R ≥ 4.3
- HPC cluster with SLURM
- ~50 GB disk (test data), ~500 GB+ (production)
- R packages: `targets`, `crew`, `crew.cluster`, `arrow`, `furrr`, `dplyr`, `tidyr`, `broom`, `yaml`, `here`, `fs`, `glue`, `lubridate`, `stringr`, `scales`, `digest`, `httr`, `jsonlite`, `dataverse`, `amadeus`

### Setup
```bash
git clone https://github.com/dariusmb85/hcup_pegs_explorer.git
cd hcup_pegs_explorer

# Install R dependencies
make init

# Configure environment
cp .env.example .env
# Edit .env:
#   PARQUET_ROOT=./data         (or path to your data directory)
#   PERSON_ID_SALT=<random>     (for SHA-256 person ID hashing)
#   HUD_API_KEY=<key>           (https://www.huduser.gov/portal/dataset/uspszip-api.html)
#   DATAVERSE_API_KEY=<key>     (https://dataverse.harvard.edu)
```

---

## Running the Pipeline

### Full Pipeline (SLURM)
```bash
bash run_pipeline.sh
```

The pipeline is orchestrated via `{targets}`. The `run_pipeline.sh` script submits a SLURM job that calls `tar_make()`. Progress can be monitored via:

```bash
tail -f logs/full_pipeline_*.log
tail -f logs/silver_progress.log    # per-file silver progress
squeue -u $USER                     # SLURM job status
```

### Individual Targets
```r
library(targets)
tar_make(silver_layer)           # Step 1: harmonize + PheCode map
tar_make(geocoded_visits)        # Step 2: ZIP → tract
tar_make(qc_and_clean)           # Step 3: quality checks + clean
tar_make(person_month_cohort)    # Step 4: person-month cohort
tar_make(exposures_downloaded)   # Step 5: download exposures
tar_make(joined_data)            # Step 6: join exposures
tar_make(exposure_rollup_complete) # Step 7: rollup metrics
```

### ExWAS
```bash
# Using default analysis spec
Rscript r/06_exwas_stratified.R

# Using custom spec
Rscript r/06_exwas_stratified.R config/analysis_asthma_smoke.yaml
```

### Shiny Explorer *(in development)*
```r
shiny::runApp("shiny")
```

---

## Configuration

### Phenotype Definitions (`config/covariates.yaml`)
Phenotypes are defined using PheCode mappings (ICD-9 and ICD-10 compatible):

```yaml
phenotypes:
  asthma:
    label: "Asthma"
    phecodes: ["495"]
  copd:
    label: "COPD"
    phecodes: ["496", "496.1", "496.2"]
```

Current phenotypes: Asthma, COPD, Respiratory Infection, Cardiovascular Disease, Stroke, Diabetes, Pregnancy, Mental Health

### Analysis Spec (`config/analysis_spec.yaml`)
Controls ExWAS runs without touching code:

```yaml
analysis:
  name: "asthma_smoke_adjusted"
  outcomes: ["asthma_flag"]
  exposures: ["hms_smoke_heavy", "hms_smoke_medium"]
  models: ["logit_adjusted_temporal_overall"]
  filters:
    states: ["NC", "UT"]
    year_min: 2015
    year_max: 2020
    sex: "all"
  extra_covariates: ["age", "race"]
```

### ExWAS Models
| Model ID | Strata | Formula |
|---|---|---|
| `logit_unadjusted_overall` | All | `outcome ~ exposure` |
| `logit_adjusted_temporal_overall` | All | `outcome ~ exposure + year + season` |
| `logit_adjusted_temporal_male` | Male | `outcome ~ exposure + year + season` |
| `logit_adjusted_temporal_female` | Female | `outcome ~ exposure + year + season` |

---

## SLURM Configuration

The pipeline uses two SLURM controllers via `{crew.cluster}`:

| Controller | CPUs | Memory | Time | Used for |
|---|---|---|---|---|
| `controller_normal` | 4 | 48 GB | 4h | Exposures, join, rollup |
| `controller_highmem` | 8–16 | 96–192 GB | 12h | Person-month cohort |

Targets with `deployment = "main"` (silver, geocoding, QC) run on the submit node directly.

---

## Output Structure

```
data/
├── bronze/                          # Raw HCUP parquet files
├── silver/
│   ├── visit/                       # Harmonized visits (partitioned by state/db_type/year)
│   └── visit_clean/                 # QC-filtered visits
└── gold/
    ├── quality_checks/              # QC reports + issue CSVs
    ├── person_month/                # Person-month cohort (partitioned)
    ├── exposures_monthly/           # Downloaded exposure data
    ├── person_month_exposures/      # Joined dataset (partitioned)
    ├── exposure_rollup/             # Long format rollup (partitioned)
    └── exwas_result_stratified/     # ExWAS results (partitioned by state/strata/model)
        exwas_{analysis_name}.parquet  # Flat result files per analysis
```

All datasets use partitioned Apache Parquet format (`facility_state / db_type / year`) for efficient querying with `{arrow}`.

---

## Key Technical Features

### 1. Parallel Silver Processing
Bronze files are processed in parallel using `{furrr}` (8 workers). Each file writes independently to its own partition, eliminating write collisions.

### 2. ICD Version Heterogeneity
Handles ICD-9 (pre-October 2015) and ICD-10 (post-October 2015) via vectorized PheCode mapping. The 2015 HCUP quarterly split (Q1–Q3 ICD-9, Q4 ICD-10) is harmonized automatically.

### 3. Temporal Confounding Controls
**Problem:** Wildfire season (summer) ≠ Flu season (winter)  
**Solution:** Season + year fixed effects in all adjusted models  
**Impact:** Smoke → respiratory infection association changes from OR=0.72 (spuriously protective) to OR=0.95 (null) after adjustment

### 4. Memory-Efficient Cohort Building
Person-month cohort is built in state×year chunks (94 chunks for current dataset) processed in parallel, preventing OOM errors on 109M+ visit datasets.

### 5. Person ID Hashing
PHI protection via SHA-256 with project-specific salt:
```r
person_id <- digest::digest(paste0(visit_key, SALT), algo = "sha256")
```

---

## Roadmap

- [ ] Geographic event linkage (acute exposure events by ZIP/tract × time window)
- [ ] Shiny ExWAS explorer (forest plots, filtering by phenotype/exposure/p-value)
- [ ] Config-driven covariate selection in analysis specs
- [ ] Additional states and years
- [ ] Additional exposure sources (RSEI, LandScan, Radon, AP)
- [ ] Cumulative exposure metrics (rolling windows)

---

## Comparison to PEGS

| Feature | PEGS | HCUP PEGS Explorer |
|---|---|---|
| **Data** | Survey (individual-level) | Claims (area-level) |
| **Design** | Cross-sectional | Longitudinal |
| **Exposures** | Biomarkers, questionnaires | Environmental (ZIP/tract) |
| **Phenotypes** | Self-report + clinical | ICD-coded diagnoses |
| **Sample size** | ~19,000 enrolled | ~109M visits |
| **Adjustments** | Age, sex, race | + year, season, db_type |
| **Temporal controls** | None | Season + year fixed effects |
| **Stratification** | Sex | Sex, state, year range |
| **Pipeline** | Manual | Fully automated (targets + SLURM) |

---

## Acknowledgments

- **NIEHS PEGS** for methodological framework
- **Harvard Dataverse / Amadeus** for pre-aggregated environmental exposures
- **AHRQ HCUP** for administrative health data
- **HUD** for ZIP-tract crosswalk API

---

## License

**Data Access Requirements:**
- HCUP data requires a Data Use Agreement (DUA) with AHRQ
- Dataverse exposures are publicly available
- HUD API requires free registration

**Code:** MIT License — see `LICENSE`
