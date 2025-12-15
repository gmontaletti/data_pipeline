# Labor Market Transition Analysis Pipeline

A comprehensive data preprocessing pipeline for analyzing labor market transitions, unemployment spells, and active labor market policy effectiveness in Lombardy, Italy.

**Author**: Giampaolo Montaletti
**Email**: giampaolo.montaletti@gmail.com
**GitHub**: [github.com/gmontaletti](https://github.com/gmontaletti)
**ORCID**: [0009-0002-5327-1122](https://orcid.org/0009-0002-5327-1122)

## Overview

This pipeline transforms raw employment administrative data into dashboard-ready analytical datasets. It computes employment-to-employment transitions (closed transitions), employment-to-unemployment transitions (open transitions), enriches them with unemployment declaration and active labor market policy information (DID - Dichiarazione di Immediata Disponibilità, POL - Politiche Attive), and produces comprehensive aggregates for interactive visualization.

### Dual-Branch Architecture

The pipeline processes data through **TWO parallel branches** to enable different analytical perspectives:

- **Residence Branch**: Filters by worker's residence (`COMUNE_LAVORATORE`) → Outputs to `output/dashboard/`
  - Includes workers domiciled in Lombardy, regardless of workplace location
  - A person living in Lombardy who works outside the region is **included**
  - A person living outside Lombardy who works in Lombardy is **excluded**

- **Workplace Branch**: Filters by job location (`COMUNE_SEDE_LAVORO`) → Outputs to `output/dashboard_workplace/`
  - Includes jobs located in Lombardy, regardless of worker's residence
  - A person living outside Lombardy who works in Lombardy is **included**
  - A person living in Lombardy who works outside the region is **excluded**

Both branches produce identical file structures and undergo the same analytical processing after the filtering stage.

### Key Features

- **Dual-Branch Processing**: Parallel analysis by residence and workplace location
- **Transition Analysis**: Computes closed (job-to-job) and open (job-to-unemployment) transitions
- **Policy Enrichment**: Links transitions to DID and POL active labor market policy support
- **Career Metrics**: Survival analysis, employment stability indices, job turnover rates
- **Career Clustering**: Groups individuals by career trajectory patterns
- **Geographic Aggregation**: Summaries by geographic area and CPI (Centro Per l'Impiego)
- **Time Series**: Monthly aggregates for trend analysis
- **45-Day Lag Analysis**: Filters rapid transitions to focus on meaningful career changes
- **Network Visualization**: Profession and sector transition network graphs

## Pipeline Architecture

The pipeline uses the `targets` framework to orchestrate a **DUAL-BRANCH workflow** with 13 phases (Phase 0-12):

### Shared Preprocessing (Before Fork)
- **Phase 0**: Load Raw Data - Load `rap.fst` (contracts), `did.fst` (unemployment declarations), `pol.fst` (active labor market policies)
- **Phase 1**: Prepare Data - Harmonize contract codes, standardize education levels, select relevant columns
- **Phase 2**: Fork - Apply location filters to create two parallel branches

### Parallel Branch Processing (Both Residence and Workplace)
Each branch then processes through:
- **Phase 3**: Consolidate & Enrich - Vecshift consolidation, add unemployment periods, match DID/POL events, LongworkR consolidation, add CPI/ATECO enrichment
- **Phase 4**: Compute Transitions - Extract demographics, compute closed and open transitions, enrich with DID/POL
- **Phase 5**: Create Transition Matrices - Aggregate by contract type, profession, sector (standard + 45-day lag)
- **Phase 6**: Career Metrics & Survival - Compute survival curves and trajectory metrics
- **Phase 7**: Career Clustering - Cluster trajectories, create person-level dataset
- **Phase 8**: Geographic Aggregations - Summaries by area and CPI
- **Phase 9**: Time Series - Monthly aggregates
- **Phase 10**: Policy Summary - DID/POL effectiveness analysis
- **Phase 11**: Policy Coverage Precomputations - Dashboard performance optimizations
- **Phase 12**: File Outputs - Write to separate output directories (`output/dashboard/` and `output/dashboard_workplace/`)

## Prerequisites

### Required R Packages

```r
install.packages(c("targets", "tarchetypes", "data.table", "fst", "devtools", "ggplot2"))
```

### External Dependencies

- **vecshift package** (development version): Must be installed from local source at `~/Documents/funzioni/vecshift/`
- **longworkR package** (development version): Must be installed from local source at `~/Documents/funzioni/longworkR/`
- **Shared data directory**: Raw employment data at `~/Documents/funzioni/shared_data/` (or set `$SHARED_DATA_DIR`)

### Data Requirements

The pipeline expects the following files in `$SHARED_DATA_DIR/raw/`:
- `rap.fst` - Raw employment contracts (all fields, before any filtering)
- `did.fst` - Unemployment declarations (DID/NASPI data)
- `pol.fst` - Active labor market policies participation

Additional required files:
- `comune_cpi_lookup.rds` - Geographic lookup table at `$SHARED_DATA_DIR/maps/comune_cpi_lookup.rds`
- Classifier lookup tables (loaded via `load_classifiers()` function)

## Installation

```bash
# Clone the repository
git clone https://github.com/gmontaletti/data_pipeline.git
cd data_pipeline

# Ensure vecshift and longworkR packages are available
# (Install from local source if needed)
# R CMD INSTALL ~/Documents/funzioni/vecshift/
# R CMD INSTALL ~/Documents/funzioni/longworkR/

# Set shared data directory (optional, defaults to ~/Documents/funzioni/shared_data)
export SHARED_DATA_DIR="/path/to/shared/data"
```

## Usage

### Running the Full Pipeline

```bash
# Execute the complete pipeline
./update_data.sh
```

This runs `tar_make()` and displays the validation report.

### Monitoring Progress

```bash
# Check real-time pipeline progress
./check_progress.sh
```

### R Console Usage

```r
library(targets)

# Run entire pipeline
tar_make()

# Run specific targets
tar_make(transitions)

# Visualize pipeline dependency graph
tar_visnetwork()

# Check which targets need rebuilding
tar_outdated()

# Read a computed target
tar_read(person_data)

# Invalidate specific target to force rebuild
tar_invalidate(transitions)
```

### Development Workflow

```r
# Reload functions after editing R/ scripts
devtools::load_all("R/")

# Rebuild specific targets after function changes
tar_invalidate(c("transitions", "transition_matrices"))
tar_make()

# Clean all targets (use cautiously!)
tar_destroy()
```

## Output Structure

The pipeline produces outputs in **TWO parallel directories**:

- **`output/dashboard/`**: Residence-based analysis (filtered by `COMUNE_LAVORATORE`)
- **`output/dashboard_workplace/`**: Workplace-based analysis (filtered by `COMUNE_SEDE_LAVORO`)

Both directories contain identical file structures:

### Large Datasets (FST format)
- `transitions.fst` - All transitions with full attributes (108M+ records)
- `person_data.fst` - Person-level summary with demographics and career metrics
- `monthly_timeseries.fst` - Monthly aggregates by area
- `monthly_timeseries_cpi.fst` - Monthly aggregates by CPI

### Lookup Tables & Summaries (RDS format)
- `classifiers.rds` - Contract/profession/sector lookup tables
- `survival_curves.rds` - Survival analysis results
- `transition_matrices.rds` / `transition_matrices_8day.rds` - Standard and 45-day lag transition matrices
- `profession_transitions.rds` / `sector_transitions.rds` - Labeled transition edge lists
- `profession_matrix.rds` / `sector_matrix.rds` - R matrix format for operations
- `profession_summary_stats.rds` / `sector_summary_stats.rds` - Top transitions, mobility indices
- `geo_summary.rds` / `geo_summary_cpi.rds` - Geographic aggregates
- `policy_*.rds` - Precomputed policy coverage aggregates (timeseries, demographics, geography, duration distributions)

### Visualizations (PNG format)
- `plots/profession_network.png` - Profession transition network
- `plots/sector_network.png` - Economic sector transition network
- `plots/profession_network_8day.png` - 45-day lag profession network
- `plots/sector_network_8day.png` - 45-day lag sector network

## Project Structure

```
funzioni/
├── data_pipeline/                  # Main project (git repository)
│   ├── R/                          # Function modules
│   │   ├── data_preparation.R     # Raw data loading, harmonization, location filtering
│   │   ├── data_loading.R         # Data consolidation (vecshift + longworkR)
│   │   ├── transitions.R          # Core transition computation
│   │   ├── aggregations.R         # Person-level and geographic aggregates
│   │   ├── policy_aggregates.R    # Precomputed policy coverage
│   │   ├── career_analysis.R      # Survival and career metrics
│   │   ├── classifiers.R          # Classification lookups
│   │   ├── transition_enrichment.R # Labels and visualizations
│   │   └── utils_geographic.R     # Geographic utilities
│   ├── _targets.R                  # Pipeline definition
│   ├── _targets/                   # Intermediate objects (gitignored)
│   ├── output/                     # Output files (gitignored)
│   │   ├── dashboard/             # Residence-based analysis outputs
│   │   └── dashboard_workplace/   # Workplace-based analysis outputs
│   ├── check_progress.sh          # Progress monitoring script
│   ├── update_data.sh             # Pipeline execution script
│   ├── CLAUDE.md                   # Claude Code guidance
│   ├── README.md                   # This file
│   └── .gitignore                  # Git exclusions
├── reference/                      # Reference materials (not in git)
│   └── data_pipeline/             # Project-specific references
│       ├── docs/                  # Technical documentation
│       ├── papers/                # Research papers
│       ├── notes/                 # Development notes
│       └── README.md              # Reference directory guide
├── shared_data/                    # Shared data directory
│   ├── raw/                       # Raw input data
│   │   ├── rap.fst               # Employment contracts
│   │   ├── did.fst               # Unemployment declarations
│   │   └── pol.fst               # Active labor market policies
│   └── maps/                      # Geographic lookup tables
│       └── comune_cpi_lookup.rds  # CPI mapping
├── vecshift/                       # Development package dependency
└── longworkR/                      # Development package dependency
```

## Key Concepts

### Residence vs Workplace Filtering

The dual-branch architecture enables different analytical perspectives:

**Residence Branch** (filtered by `COMUNE_LAVORATORE`):
- Tracks individuals **domiciled in Lombardy**
- Includes their employment regardless of where they work
- Use case: Regional labor force analysis, worker mobility patterns
- Example: A Milan resident working in Turin is **included**

**Workplace Branch** (filtered by `COMUNE_SEDE_LAVORO`):
- Tracks jobs **located in Lombardy**
- Includes all workers in these positions regardless of residence
- Use case: Regional economy analysis, labor demand in Lombardy
- Example: A Piedmont resident working in Milan is **included**

Geographic coverage for both branches: All 12 Lombard provinces (Milano, Brescia, Bergamo, Monza e Brianza, Varese, Como, Mantova, Pavia, Cremona, Lecco, Sondrio, Lodi)

### Transition Types

- **Closed Transition**: Employment → Unemployment → Employment (has destination job)
- **Open Transition**: Employment → Ongoing Unemployment (no destination job at observation end)

### 45-Day Lag Analysis

The pipeline computes two versions of transition matrices:
- **Standard**: All transitions included
- **45-Day Lag**: Filters to unemployment durations >45 days, excluding rapid rehires to focus on meaningful career changes

### Policy Support (DID/POL)

- **DID (Dichiarazione di Immediata Disponibilità)**: Declaration of Immediate Availability - the unemployment declaration system that registers individuals as available for work
- **POL (Politiche Attive)**: Active Labor Market Policies - interventions providing training, employment support, and other active measures
- Policy flags are matched to unemployment periods via non-equi join with unemployment spell records

## Git Workflow

### First-Time Setup

```bash
# Initialize git (already done if you cloned)
git init

# Add all files (respects .gitignore)
git add .

# Make initial commit
git commit -m "Initial commit: Labor market transition pipeline"

# Add remote repository
git remote add origin https://github.com/gmontaletti/data_pipeline.git

# Push to GitHub
git push -u origin main
```

### Regular Development

```bash
# Check status
git status

# Stage changes
git add R/transitions.R _targets.R

# Commit with descriptive message
git commit -m "Add 45-day lag transition matrix computation"

# Push to remote
git push

# Pull latest changes
git pull
```

### Creating Feature Branches

```bash
# Create and switch to new branch
git checkout -b feature/policy-coverage-improvements

# Make changes and commit
git add .
git commit -m "Improve policy coverage computation"

# Push branch to remote
git push -u origin feature/policy-coverage-improvements

# Create pull request on GitHub, then merge and delete branch
git checkout main
git pull
git branch -d feature/policy-coverage-improvements
```

## Troubleshooting

### Pipeline Stalls

Check progress and kill stuck processes:
```bash
./check_progress.sh
# If stuck, identify and kill R process
ps aux | grep R
kill <pid>
```

### Missing CPI Data

Ensure CPI lookup table exists:
```bash
ls ~/Documents/funzioni/shared_data/maps/comune_cpi_lookup.rds
```

### Package Dependencies Not Found

Verify the paths in `_targets.R` match your installation:
```r
devtools::load_all("~/Documents/funzioni/vecshift/")
devtools::load_all("~/Documents/funzioni/longworkR/")
```

### Obsolete CPI Codes

If new obsolete codes appear, add recoding logic in Phase 1.3 of `_targets.R`:
```r
data_with_geo[cpi_code == "OLD_CODE", cpi_code := "NEW_CODE"]
```

## Performance Notes

- **Data Format**: FST for large datasets (>10M records), RDS for small lookups
- **Compression**: FST compression level 85 balances speed and size
- **Memory**: Pipeline requires ~8-16GB RAM for full execution
- **Runtime**: Full pipeline takes 30-60 minutes depending on data size

## Version

**Current Version**: 0.2.3 (2025-12-15)

- v0.2.3: Bug fixes for MONECA clustering and improved data preparation robustness
- v0.2.2: Fix filter_by_location() bug and optimize performance (6-30x faster, Phase 2 now ~20s)
- v0.2.1: Bug fixes for Date/IDate type handling and DID/POL matching
- v0.2.0: Dual-branch architecture implementation
  - Added parallel processing by residence (COMUNE_LAVORATORE) and workplace (COMUNE_SEDE_LAVORO)
  - Dual output directories: output/dashboard/ and output/dashboard_workplace/
  - Added data_preparation.R module for location filtering
  - Added vecshift package dependency
  - Restructured pipeline into 13 phases (0-12) with fork at Phase 2
- v0.1.0: Initial release with single-branch processing

## Citation

If you use this pipeline in your research, please cite:

```
Montaletti, G. (2025). Labor Market Transition Analysis Pipeline (v0.2.3).
GitHub: https://github.com/gmontaletti/data_pipeline
ORCID: 0009-0002-5327-1122
```

## License

[Specify your license here, e.g., MIT, GPL-3, etc.]

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Contact

For questions or collaboration inquiries:

**Giampaolo Montaletti**
Email: giampaolo.montaletti@gmail.com
GitHub: [@gmontaletti](https://github.com/gmontaletti)
