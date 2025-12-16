# HCUP PEGS Explorer

Environment-Wide Association Study (ExWAS) platform for linking administrative health data (HCUP) with environmental exposures, modeled after the NIEHS [PEGS (Population-Based Exposure and Genomic Studies)](https://www.niehs.nih.gov/research/clinical/studies/pegs) approach.

---

## Overview

This pipeline analyzes associations between area-level environmental exposures (wildfire smoke, air pollution, temperature) and health outcomes using hospital administrative data from the Healthcare Cost and Utilization Project (HCUP).

**Key Features:**
- Automated data harmonization across HCUP database types (SID, SEDD, SASD)
- Monthly temporal resolution preserving acute exposure effects
- Sex-stratified analysis with temporal confounding controls
- Dual ICD-9/ICD-10 phenotype definitions
- Scalable to multi-state analyses

**Current Implementation:**
- Test dataset: Utah 2012-2014 (2.9M person-months, 8 phenotypes)
- Production target: 4 states, 2016-2020+ (~40M+ person-months)

---

## Pipeline Architecture
```
┌─────────────┐
│ HCUP Bronze │  Raw state data (SID, SEDD, SASD)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Silver    │  Harmonized visits + demographics
└──────┬──────┘
       │
       ├──────────────┐
       ▼              ▼
┌─────────────┐  ┌──────────────┐
│  Geocoding  │  │  Exposures   │  Dataverse monthly ZIP-level
└──────┬──────┘  └──────┬───────┘
       │                │
       ▼                │
┌─────────────┐         │
│   Cohort    │  Person-months + phenotypes
└──────┬──────┘         │
       │                │
       └────────┬───────┘
                ▼
         ┌─────────────┐
         │    Join     │  Exposures → Person-months
         └──────┬──────┘
                ▼
         ┌─────────────┐
         │   Rollup    │  Long format for ExWAS
         └──────┬──────┘
                ▼
         ┌─────────────┐
         │    ExWAS    │  Stratified regressions
         └─────────────┘
```

---

## Data Sources

### Health Data: HCUP
- **SID (State Inpatient Databases):** Hospital discharges
- **SEDD (State Emergency Department Databases):** ED visits
- **SASD (State Ambulatory Surgery Databases):** Outpatient procedures

### Environmental Data: Harvard Dataverse
Pre-aggregated monthly ZIP-level exposures from [Amadeus-aggregated dataset](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/0WILGX):
- **HMS Wildfire Smoke:** Light, medium, heavy coverage (NOAA)
- **PM2.5 Proxies:** Dust, black carbon (MERRA-2 satellite)
- **Temperature:** Max, min (TerraClimate)

### Geocoding: HUD USPS Crosswalk
- ZIP Code → Census Tract mapping
- 99.6% match rate in test data

---

## Installation

### Requirements
- R ≥ 4.0
- HPC cluster with SLURM (for production runs)
- ~50GB disk space (test data)
- ~500GB+ disk space (production data)

### Setup
```bash
# Clone repository
git clone https://github.com/dariusmb85/hcup_pegs_explorer.git
cd hcup_pegs_explorer

# Install R dependencies
make init

# Configure environment
cp .env.example .env
# Edit .env with your:
#   - PERSON_ID_SALT (random string for hashing)
#   - HUD_API_KEY (from https://www.huduser.gov/portal/dataset/uspszip-api.html)
#   - DATAVERSE_API_KEY (from https://dataverse.harvard.edu)
```

---

## Quick Start

### Local Execution (Test Data)
```bash
# Run full pipeline
make pipeline

# Submit ExWAS to SLURM (recommended)
make slurm-exwas

# Monitor progress
make check-slurm
tail -f logs/exwas_*.log
```

### Individual Steps
```bash
make silver      # Step 1: HCUP harmonization
make geocode     # Step 2: ZIP → tract geocoding
make exposures   # Step 3: Download environmental data
make cohort      # Step 4: Build person-month cohort
make join        # Step 5: Join exposures to cohort
make rollup      # Step 6: Create exposure rollup
make exwas       # Step 7: Run ExWAS (local, slow)
```

---

## Configuration

### Phenotype Definitions (`config/covariates.yaml`)

Phenotypes defined with both ICD-9 and ICD-10 codes:
```yaml
phenotypes:
  asthma:
    label: "Asthma"
    icd9_prefixes: ["493"]
    icd10_prefixes: ["J45"]
    description: "Asthma encounters"
  
  respiratory_infection:
    label: "Respiratory Infection"
    icd9_prefixes: ["460", "461", ..., "487"]
    icd10_prefixes: ["J00", "J01", ..., "J22"]
```

### ExWAS Models

Three adjustment levels × three sex strata = 9 models:

**Adjustment Levels:**
1. **Unadjusted:** `outcome ~ exposure`
2. **Temporal:** `outcome ~ exposure + year + season`
3. **Full:** `outcome ~ exposure + year + season + db_type + age + female + race`

**Sex Strata:**
- Overall (sex as covariate)
- Males only
- Females only

---

## Key Technical Features

### 1. ICD Version Heterogeneity

Handles both ICD-9 (pre-October 2015) and ICD-10 (post-October 2015):
```r
dx_matches_phenotype <- function(dx_codes, icd9_prefixes, icd10_prefixes) {
  icd9_match <- any(sapply(icd9_prefixes, function(p) grepl(paste0("^", p), dx_codes)))
  icd10_match <- any(sapply(icd10_prefixes, function(p) grepl(paste0("^", p), dx_codes)))
  return(icd9_match || icd10_match)
}
```

### 2. Temporal Confounding Controls

Critical for environmental ExWAS:

**Problem:** Wildfire season (summer) ≠ Flu season (winter)

**Solution:** Season + year fixed effects

**Impact:** Smoke → RI association changes from OR=0.72 (protective?!) to OR=0.95 (null) after adjustment

### 3. Visit Type Adjustment

Accounts for severity differences:
- **SEDD (ED):** Acute presentations
- **SID (Inpatient):** Severe cases requiring admission

### 4. Person ID Hashing

PHI protection via SHA-256:
```r
person_id <- map_chr(format(visit_key, scientific=FALSE), 
                     ~digest::digest(paste0(., SALT), algo="sha256"))
```

---

## Output Structure
```
data_test/
├── bronze/              # Raw HCUP files (symlink)
├── silver/
│   ├── person/         # Person-level demographics
│   └── visit/          # Visit-level records
└── gold/
    ├── person_month/           # Person-month cohort
    ├── exposures_monthly/      # Environmental exposures
    ├── person_month_exposures/ # Joined dataset
    ├── exposure_rollup/        # Long format for ExWAS
    └── exwas_result_stratified/
        ├── all/        # Overall stratum results
        ├── male/       # Male-only results
        └── female/     # Female-only results
```

---



---

## Comparison to PEGS

| Feature | PEGS | This Pipeline |
|---------|------|---------------|
| **Data** | Survey (individual-level) | Claims (area-level) |
| **Design** | Cross-sectional | Longitudinal |
| **Exposures** | Biomarkers, questionnaires | Environmental (ZIP/tract) |
| **Phenotypes** | Self-report + clinical | ICD-coded diagnoses |
| **Adjustments** | Age, sex, race | **+ year, season, db_type** |
| **Temporal controls** | None | **Season + year** |
| **Stratification** | Sex | Sex |

**Advantages:**
- ✅ Rigorous temporal confounding control
- ✅ Longitudinal consistency assessment
- ✅ Objective diagnoses (ICD codes)
- ✅ Large sample sizes

**Limitations:**
- ⚠️ Ecological exposure (ZIP-level, not individual)
- ⚠️ Healthcare-seeking bias
- ⚠️ Only diagnosed cases

---

## Contributing

This is an active research project. Contributions welcome for:
- Additional phenotype definitions
- Exposure data sources
- Computational optimizations
- Visualization tools

Please open an issue to discuss before submitting pull requests.

---

## License

**Data Access Requirements:**
- HCUP data requires DUA with AHRQ
- Dataverse exposures are publicly available
- HUD API requires free registration

**Code:** MIT License (see LICENSE file)

---

## Acknowledgments

- **NIEHS PEGS** for methodological framework
- **Harvard Dataverse** for pre-aggregated environmental exposures
- **AHRQ HCUP** for administrative health data
- **HUD** for ZIP-tract crosswalk API

---

## Related Projects

- [NIEHS PEGS](https://www.niehs.nih.gov/research/clinical/studies/pegs)
- [Amadeus R Package](https://github.com/Spatiotemporal-Exposures-and-Toxicology)
- [Dataverse Environmental Data](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/0WILGX)

---
