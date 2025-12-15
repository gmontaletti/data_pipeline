# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a data preprocessing pipeline for labor market transition analysis in Lombardy, Italy. It transforms raw employment data into dashboard-ready datasets analyzing job transitions, unemployment spells, career trajectories, and active labor market policy (DID/POL) effectiveness.

**Author**: Giampaolo Montaletti (giampaolo.montaletti@gmail.com)
**GitHub**: github.com/gmontaletti
**ORCID**: https://orcid.org/0009-0002-5327-1122

## Pipeline Architecture

The pipeline uses the `targets` framework (_targets.R) to orchestrate a **DUAL-BRANCH workflow** that processes employment data in TWO parallel pipelines:

### Branch Structure
- **Residence Branch** (output/dashboard/): Filters by worker's residence (COMUNE_LAVORATORE) - *current behavior*
- **Workplace Branch** (output/dashboard_workplace/): Filters by job location (COMUNE_SEDE_LAVORO) - *new analysis*

### Phase Flow

**Phase 0: Load Raw Data** (shared)
- Load `rap.fst` (raw contracts), `did.fst` (unemployment declarations), `pol.fst` (active labor market policies)

**Phase 1: Prepare Data** (shared, before forking)
- Load classifiers (contract types, professions, sectors, territorial data)
- Harmonize contract codes (map obsolete codes to standard equivalents)
- Standardize education levels
- Select relevant columns
- **KEY**: Both COMUNE_LAVORATORE and COMUNE_SEDE_LAVORO are preserved at this stage

**Phase 2: Consolidate with Early Geographic Fork**
- Step 1: Single vecshift consolidation on full Lombardia-connected data (shared)
- Step 2: FORK by geography (residence vs workplace) - happens BEFORE adding unemployment to avoid memory explosion
- Steps 3-5: Parallel consolidation chains for each branch

**Phase 3-12: Parallel Processing** (both branches execute independently)

Each branch then processes through:
3. **Add Unemployment & Enrich** → Add unemployment periods → Merge attributes → Match DID/POL events → LongworkR consolidation (short gaps) → Add CPI/ATECO enrichment
4. **Compute Transitions** → Extract demographics, compute closed and open transitions, enrich with DID/POL
5. **Create Transition Matrices** → Aggregate by contract type, profession, sector (standard + 45-day lag)
6. **Career Metrics & Survival** → Compute survival curves and trajectory metrics
7. **Career Clustering** → Cluster trajectories, create person-level dataset
8. **Geographic Aggregations** → Summaries by area and CPI
9. **MONECA Labor Market Segmentation** → Cluster sectors by mobility patterns, generate naming templates, compute segment-based specializations (workplace branch only, conditional on manual naming)
10. **Annual Coverage Estimation** → Calculate FTE by year, comune, sector (workplace branch only)
11. **Time Series** → Monthly aggregates
12. **Policy Summary** → DID/POL effectiveness analysis
13. **Policy Coverage Precomputations** → Dashboard performance optimizations
14. **File Outputs** → Write to separate output directories

### 45-Day Lag Analysis
The pipeline computes transition matrices in two variants:
- **Standard**: All transitions
- **45-day lag**: Filters to unemployment durations >45 days to focus on meaningful career transitions (excludes rapid rehires)

## Key R Modules (R/ directory)

- **data_preparation.R**: Raw data loading, contract harmonization, location filtering (pre and post-vecshift), vecshift preparation
- **data_loading.R**: Consolidate employment spells (vecshift + longworkR), standardize ATECO codes, add CPI geographic info
- **transitions.R**: Core transition computation (closed/open), DID/POL enrichment, transition matrices
- **aggregations.R**: Person-level data, geographic summaries, monthly time series, policy summaries
- **policy_aggregates.R**: Precomputed policy coverage aggregates for dashboard performance
- **career_analysis.R**: Survival analysis and career metrics computation
- **coverage_analysis.R**: Annual contractual coverage estimation (FTE by year, Comune, sector)
- **balassa_specialization.R**: Balassa index calculation and territorial specialization mapping (with situas integration), segment-based specialization functions
- **moneca_segmentation.R**: MONECA clustering for labor market segmentation, template generation for manual segment naming, segment membership extraction and composition analysis
- **classifiers.R**: Load contract type/profession/sector/territorial classification tables
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

**Raw Input Files** (in `shared_data/raw/`):
- `rap.fst`: Raw employment contracts (all fields, before any filtering)
- `did.fst`: Unemployment declarations (DID/NASPI data)
- `pol.fst`: Active labor market policies participation

### Data Coverage and Dual-Branch Filtering

The pipeline now processes data in **TWO ways**:

**Residence Branch (CURRENT BEHAVIOR)**:
- Filters by `COMUNE_LAVORATORE` (worker's residence)
- Dataset contains contracts of people **domiciled in Lombardy**
- A person living in Lombardy who works outside the region is **INCLUDED**
- A person living outside Lombardy who works in Lombardy is **EXCLUDED**

**Workplace Branch (NEW ANALYSIS)**:
- Filters by `COMUNE_SEDE_LAVORO` (job location)
- Dataset contains contracts where **workplace is in Lombardy**
- A person living outside Lombardy who works in Lombardy is **INCLUDED**
- A person living in Lombardy who works outside the region is **EXCLUDED**

**Geographic Coverage**: All 12 Lombard provinces: Milano, Brescia, Bergamo, Monza e Brianza, Varese, Como, Mantova, Pavia, Cremona, Lecco, Sondrio, Lodi

**Lookup tables** (in shared_data/maps/):
- `comune_cpi_lookup.rds`: Belfiore code → CPI mapping
- Contract type, profession, sector classifiers (loaded via `load_classifiers()`)

**External packages**:
- `longworkR` package from `~/Documents/funzioni/longworkR/` (development version)
- `vecshift` package from `~/Documents/funzioni/vecshift/` (development version)
- `moneca` package from GitHub: `gmontaletti/MONECA` (for labor market segmentation)
- `situas` package from GitHub: `gmontaletti/situas` (for Lombardy geographic data)

**Reference materials**: Non-code documentation and references stored at `../reference/data_pipeline/` (outside git repository)

## Output Structure

**Dual Output Directories**:
- `output/dashboard/`: Residence-based analysis (current behavior)
- `output/dashboard_workplace/`: Workplace-based analysis (new)

Both directories contain identical file structures:

**Large datasets (FST format)**:
- `transitions.fst`: All transitions with full attributes (108M+ records)
- `person_data.fst`: Person-level summary (demographics + career metrics + clusters)
- `monthly_timeseries.fst`, `monthly_timeseries_cpi.fst`: Monthly aggregates
- `annual_coverage_by_year_comune_sector.fst`: Annual FTE coverage by year, workplace Comune, and sector with Balassa index (workplace branch only)
- `coverage_with_segments_standard.fst`: Annual coverage by year/comune/segment with Balassa index (conditional, workplace branch only)
- `coverage_with_segments_45day.fst`: Same for 45-day segmentation (conditional, workplace branch only)

**Lookup tables & summaries (RDS format)**:
- `classifiers.rds`: Contract/profession/sector lookups
- `survival_curves.rds`: Survival analysis results
- `transition_matrices.rds`, `transition_matrices_8day.rds`: Standard and 45-day lag matrices
- `profession_transitions.rds`, `sector_transitions.rds`: Labeled transition edge lists
- `profession_matrix.rds`, `sector_matrix.rds`: R matrix format for operations
- `profession_summary_stats.rds`, `sector_summary_stats.rds`: Top transitions, mobility indices
- `geo_summary.rds`, `geo_summary_cpi.rds`: Geographic aggregates
- `policy_*.rds`: Precomputed policy coverage aggregates (timeseries, demographics, geography, duration distributions)
- `comuni_specialization_2024.rds`: Spatial dataframe (sf) with Balassa indices and specializations by comune (workplace branch only)
- `moneca_standard.rds`, `moneca_45day.rds`: MONECA clustering objects (workplace branch only)
- `segment_membership_standard.rds`, `segment_membership_45day.rds`: Sector→segment mappings (workplace branch only)
- `segment_composition_standard.rds`, `segment_composition_45day.rds`: Segment statistics (workplace branch only)
- `segment_membership_named_standard.rds`, `segment_membership_named_45day.rds`: Named memberships (conditional, workplace branch only)
- `comuni_specialization_segments_standard_2024.rds`, `comuni_specialization_segments_45day_2024.rds`: Spatial dataframes with segment specializations (conditional, workplace branch only)

**Visualizations (PNG)**:
- `plots/profession_network.png`, `plots/sector_network.png`: Network graphs
- `plots/profession_network_8day.png`, `plots/sector_network_8day.png`: 45-day lag networks

**Templates for manual naming** (reference/data_pipeline/):
- `segment_names_standard.R`: Template for naming standard segmentation labor market segments
- `segment_names_45day.R`: Template for naming 45-day lag segmentation labor market segments

## Important Implementation Notes

### Terminology
- **DID**: Stands for "Dichiarazione di Immediata Disponibilità" (Declaration of Immediate Availability) - the unemployment declaration system
- **POL**: Stands for "Politiche Attive" (Active Labor Market Policies) - general active labor market policy interventions
- **NASPI**: Unemployment benefits program (often referenced alongside DID data)
- Note: DID is NOT "Dote Impresa" (that's a different program)

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

## Git & GitHub Workflow

### Repository Structure
- **Remote**: github.com/gmontaletti/data_pipeline
- **Branch strategy**: Main branch for stable releases, feature branches for development
- **Ignored files**: _targets/, output/, *.fst, *.rds, *.qs (see .gitignore)

### Common Git Operations

**Check status and stage changes**:
```bash
git status                          # See what's changed
git add R/transitions.R _targets.R  # Stage specific files
git add .                           # Stage all changes (respects .gitignore)
```

**Commit with proper formatting**:
```bash
git commit -m "Brief summary (50 chars or less)

Detailed explanation if needed. Use markdown formatting.
- Bullet points for changes
- Reference issues with #123

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

**Push and pull**:
```bash
git push                  # Push commits to remote
git pull                  # Pull latest changes
git push -u origin main   # First push (set upstream)
```

**Feature branch workflow**:
```bash
# Create feature branch
git checkout -b feature/improve-policy-coverage

# Work on changes, commit
git add .
git commit -m "Add quarterly policy aggregates"

# Push branch
git push -u origin feature/improve-policy-coverage

# Create PR on GitHub, review, merge
# Then clean up local branch
git checkout main
git pull
git branch -d feature/improve-policy-coverage
```

**View history and diffs**:
```bash
git log --oneline --graph --decorate  # Compact history
git diff                               # See unstaged changes
git diff --staged                      # See staged changes
git show <commit-hash>                 # View specific commit
```

### GitHub Setup (First Time)

```bash
# If repository doesn't exist on GitHub yet:
# 1. Create new repository on GitHub (don't initialize with README)
# 2. Add remote and push
git remote add origin https://github.com/gmontaletti/data_pipeline.git
git branch -M main
git push -u origin main

# If cloning existing repository:
git clone https://github.com/gmontaletti/data_pipeline.git
cd data_pipeline
```

### What Gets Committed
- **YES**: R scripts (R/), pipeline definition (_targets.R), documentation (*.md), configuration (.gitignore)
- **NO**: Data files (*.fst, *.rds), intermediate objects (_targets/), outputs (output/), R project artifacts

### Handling Large Files
If you need to track large files (not recommended for this project):
```bash
# Install git-lfs
git lfs install

# Track large file types
git lfs track "*.png"
git add .gitattributes
```

### Merge Conflicts in _targets.R
When merging feature branches, _targets.R often has conflicts. To resolve:
1. Open _targets.R in editor
2. Look for conflict markers: `<<<<<<`, `======`, `>>>>>>`
3. Keep both target definitions if they're independent
4. Test pipeline: `tar_visnetwork()` to verify dependencies
5. Stage and commit resolution

## Agent Usage

Always use agents for complex tasks (per user instructions). When working with R libraries and functions, use btw MCP tools. Don't use btw for websites or standard APIs.