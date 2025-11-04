# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a data preprocessing pipeline for labor market transition analysis in Lombardy, Italy. It transforms raw employment data into dashboard-ready datasets analyzing job transitions, unemployment spells, career trajectories, and active labor market policy (DID/POL) effectiveness.

**Author**: Giampaolo Montaletti (giampaolo.montaletti@gmail.com)
**GitHub**: github.com/gmontaletti
**ORCID**: https://orcid.org/0009-0002-5327-1122

## Pipeline Architecture

The pipeline uses the `targets` framework (_targets.R) to orchestrate an 11-phase workflow:

### Phase Flow
1. **Data Loading & Consolidation** → Load raw employment data, consolidate overlapping spells
2. **Load Classifiers** → Load contract types, professions, economic sector lookups
3. **Compute Transitions** → Extract demographics, compute closed (employment→employment) and open (employment→unemployment) transitions, enrich with DID/POL policy flags
4. **Create Transition Matrices** → Aggregate transitions by contract type, profession, sector (includes standard + 45-day lag variants)
5. **Career Metrics & Survival** → Compute survival curves and career trajectory metrics
6. **Career Clustering** → Cluster career trajectories and create person-level dataset
7. **Geographic Aggregations** → Create summaries by area and CPI (Centro Per l'Impiego)
8. **Time Series** → Generate monthly aggregates
9. **Policy Summary** → Analyze DID/POL effectiveness
10. **File Outputs** → Write FST (large datasets) and RDS (lookup tables) files to `output/dashboard/`
11. **Validation Report** → Generate comprehensive validation summary

### 45-Day Lag Analysis
The pipeline computes transition matrices in two variants:
- **Standard**: All transitions
- **45-day lag**: Filters to unemployment durations >45 days to focus on meaningful career transitions (excludes rapid rehires)

## Key R Modules (R/ directory)

- **data_loading.R**: Load data, consolidate employment spells, standardize ATECO codes, add CPI geographic info
- **transitions.R**: Core transition computation (closed/open), DID/POL enrichment, transition matrices
- **aggregations.R**: Person-level data, geographic summaries, monthly time series, policy summaries
- **policy_aggregates.R**: Precomputed policy coverage aggregates for dashboard performance
- **career_analysis.R**: Survival analysis and career metrics computation
- **classifiers.R**: Load contract type/profession/sector classification tables
- **transition_enrichment.R**: Add labels to transition matrices, create network visualizations, compute summary statistics
- **utils_geographic.R**: Geographic utilities (CPI mapping via Belfiore codes)

## Running the Pipeline

### Full pipeline execution
```bash
./update_data.sh
```
This runs `tar_make()` and prints the validation report.

### Check pipeline progress
```bash
./check_progress.sh
```
Shows real-time progress from `_targets/meta/progress`.

### Run in R console
```r
library(targets)
tar_make()           # Run entire pipeline
tar_make(names)      # Run specific targets
tar_visnetwork()     # Visualize pipeline
tar_outdated()       # Check what needs rebuilding
tar_read(object)     # Read a specific target object
```

### Development workflow
```r
devtools::load_all("R/")  # Reload functions after changes
tar_invalidate(target)    # Force rebuild specific target
tar_destroy()             # Clean all targets (use cautiously)
```

## Data Dependencies

**Input**: Raw employment data at `~/Documents/funzioni/shared_data/raw/indice.fst` (or `$SHARED_DATA_DIR/raw/indice.fst`)

**Lookup tables** (in shared_data/maps/):
- `comune_cpi_lookup.rds`: Belfiore code → CPI mapping
- Contract type, profession, sector classifiers (loaded via `load_classifiers()`)

**External package**: Requires `longworkR` package loaded from `~/Documents/funzioni/longworkR/` (development version)

## Output Structure

All outputs written to `output/dashboard/`:

**Large datasets (FST format)**:
- `transitions.fst`: All transitions with full attributes (108M+ records)
- `person_data.fst`: Person-level summary (demographics + career metrics + clusters)
- `monthly_timeseries.fst`, `monthly_timeseries_cpi.fst`: Monthly aggregates

**Lookup tables & summaries (RDS format)**:
- `classifiers.rds`: Contract/profession/sector lookups
- `survival_curves.rds`: Survival analysis results
- `transition_matrices.rds`, `transition_matrices_8day.rds`: Standard and 45-day lag matrices
- `profession_transitions.rds`, `sector_transitions.rds`: Labeled transition edge lists
- `profession_matrix.rds`, `sector_matrix.rds`: R matrix format for operations
- `profession_summary_stats.rds`, `sector_summary_stats.rds`: Top transitions, mobility indices
- `geo_summary.rds`, `geo_summary_cpi.rds`: Geographic aggregates
- `policy_*.rds`: Precomputed policy coverage aggregates (timeseries, demographics, geography, duration distributions)

**Visualizations (PNG)**:
- `plots/profession_network.png`, `plots/sector_network.png`: Network graphs
- `plots/profession_network_8day.png`, `plots/sector_network_8day.png`: 45-day lag networks

## Important Implementation Notes

### Never modify these patterns
- **Never change the original moneca() function** (if present in related packages)
- **The .claude directory shouldn't be moved during cleanup** (contains project-specific instructions)

### Comment style
Use the R section comment syntax with trailing dashes:
```r
# 1. Section name -----
```
Avoid rows with only `####`.

### Data processing principles
- Use `data.table` for all data operations (mandatory for performance)
- FST format for large datasets (>10M records)
- RDS format for small lookup tables (<1M records)
- Compression level 85 for FST writes

### Geographic data handling
- Unemployment spells (arco==0) may have NA geography → inherit from previous employment spell
- CPI codes may be obsolete → recode in Phase 1.3 before processing
- Example: C816C530490 (obsolete Codogno) → C816C000692 (current)

### Transition logic
- **Closed transition**: employment → unemployment → employment (has unemployment_duration, has destination job)
- **Open transition**: employment → ongoing unemployment at observation end (has unemployment_duration, no destination job)
- DID/POL flags added via non-equi join with unemployment spells (arco==0) containing did_attribute/pol_attribute

### Version management
When bumping version, update citation in README.md (if present in related packages).

## Testing

No formal test suite. Validation occurs via:
1. Console output during pipeline execution (row counts, summary stats)
2. Final validation_report target (Phase 11)
3. Manual inspection of output files

## Common Issues

**Pipeline stalls**: Check `_targets/meta/progress` and kill stuck R processes if needed.

**CPI missing**: Ensure `comune_cpi_lookup.rds` exists at `$SHARED_DATA_DIR/maps/comune_cpi_lookup.rds`.

**longworkR not found**: Verify path in _targets.R matches actual location of development package.

**Obsolete CPI codes**: Add recoding logic in Phase 1.3 of _targets.R if new obsolete codes appear.

## Agent Usage

Always use agents for complex tasks (per user instructions). When working with R libraries and functions, use btw MCP tools. Don't use btw for websites or standard APIs.