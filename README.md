# Labor Market Transition Analysis Pipeline

A comprehensive data preprocessing pipeline for analyzing labor market transitions, unemployment spells, and active labor market policy effectiveness in Lombardy, Italy.

**Author**: Giampaolo Montaletti
**Email**: giampaolo.montaletti@gmail.com
**GitHub**: [github.com/gmontaletti](https://github.com/gmontaletti)
**ORCID**: [0009-0002-5327-1122](https://orcid.org/0009-0002-5327-1122)

## Overview

This pipeline transforms raw employment administrative data into dashboard-ready analytical datasets. It computes employment-to-employment transitions (closed transitions), employment-to-unemployment transitions (open transitions), enriches them with active labor market policy support information (DID - Dote Impresa, POL - Politiche Attive), and produces comprehensive aggregates for interactive visualization.

### Key Features

- **Transition Analysis**: Computes closed (job-to-job) and open (job-to-unemployment) transitions
- **Policy Enrichment**: Links transitions to DID and POL active labor market policy support
- **Career Metrics**: Survival analysis, employment stability indices, job turnover rates
- **Career Clustering**: Groups individuals by career trajectory patterns
- **Geographic Aggregation**: Summaries by geographic area and CPI (Centro Per l'Impiego)
- **Time Series**: Monthly aggregates for trend analysis
- **45-Day Lag Analysis**: Filters rapid transitions to focus on meaningful career changes
- **Network Visualization**: Profession and sector transition network graphs

## Pipeline Architecture

The pipeline uses the `targets` framework to orchestrate an 11-phase workflow:

1. **Data Loading & Consolidation** - Load and consolidate employment spells
2. **Load Classifiers** - Contract types, professions, economic sectors
3. **Compute Transitions** - Extract demographics and transitions with DID/POL enrichment
4. **Create Transition Matrices** - Aggregate by contract, profession, sector (standard + 45-day lag)
5. **Career Metrics & Survival** - Survival curves and comprehensive career metrics
6. **Career Clustering** - Cluster career trajectories
7. **Geographic Aggregations** - Summaries by area and CPI
8. **Time Series** - Monthly aggregates
9. **Policy Summary** - DID/POL effectiveness analysis
10. **File Outputs** - Write FST and RDS files
11. **Validation Report** - Comprehensive validation summary

## Prerequisites

### Required R Packages

```r
install.packages(c("targets", "tarchetypes", "data.table", "fst", "devtools", "ggplot2"))
```

### External Dependencies

- **longworkR package** (development version): Must be installed from local source at `~/Documents/funzioni/longworkR/`
- **Shared data directory**: Raw employment data at `~/Documents/funzioni/shared_data/` (or set `$SHARED_DATA_DIR`)

### Data Requirements

The pipeline expects:
- `indice.fst` - Raw employment data at `$SHARED_DATA_DIR/raw/indice.fst`
- `comune_cpi_lookup.rds` - Geographic lookup table at `$SHARED_DATA_DIR/maps/comune_cpi_lookup.rds`
- Classifier lookup tables (loaded via `load_classifiers()` function)

## Installation

```bash
# Clone the repository
git clone https://github.com/gmontaletti/data_pipeline.git
cd data_pipeline

# Ensure longworkR package is available
# (Install from local source if needed)

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

All outputs are written to `output/dashboard/`:

### Large Datasets (FST format)
- `transitions.fst` - All transitions with full attributes
- `person_data.fst` - Person-level summary with demographics and career metrics
- `monthly_timeseries.fst` - Monthly aggregates by area
- `monthly_timeseries_cpi.fst` - Monthly aggregates by CPI

### Lookup Tables & Summaries (RDS format)
- `classifiers.rds` - Contract/profession/sector lookup tables
- `survival_curves.rds` - Survival analysis results
- `transition_matrices.rds` / `transition_matrices_8day.rds` - Transition matrices
- `profession_transitions.rds` / `sector_transitions.rds` - Labeled transition edge lists
- `profession_matrix.rds` / `sector_matrix.rds` - R matrix format
- `profession_summary_stats.rds` / `sector_summary_stats.rds` - Top transitions, mobility indices
- `geo_summary.rds` / `geo_summary_cpi.rds` - Geographic aggregates
- `policy_*.rds` - Precomputed policy coverage aggregates

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
│   │   ├── data_loading.R         # Data loading and consolidation
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
│   │   └── dashboard/             # Dashboard-ready datasets
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
│   │   └── indice.fst            # Employment data
│   └── maps/                      # Geographic lookup tables
│       └── comune_cpi_lookup.rds  # CPI mapping
└── longworkR/                      # Development package dependency
```

## Key Concepts

### Transition Types

- **Closed Transition**: Employment → Unemployment → Employment (has destination job)
- **Open Transition**: Employment → Ongoing Unemployment (no destination job at observation end)

### 45-Day Lag Analysis

The pipeline computes two versions of transition matrices:
- **Standard**: All transitions included
- **45-Day Lag**: Filters to unemployment durations >45 days, excluding rapid rehires to focus on meaningful career changes

### Policy Support (DID/POL)

- **DID (Dote Impresa)**: Active labor market policy providing training/employment support
- **POL (Politiche Attive)**: General active labor market policies
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

### longworkR Not Found

Verify the path in `_targets.R` line 22 matches your installation:
```r
devtools::load_all("/Users/giampaolomontaletti/Documents/funzioni/longworkR/")
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

## Citation

If you use this pipeline in your research, please cite:

```
Montaletti, G. (2025). Labor Market Transition Analysis Pipeline.
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
