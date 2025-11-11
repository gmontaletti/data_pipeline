# Annual Coverage Analysis

## Overview

This module estimates contractual annual coverage for the workplace branch, calculating the FTE (Full-Time Equivalent) coverage for each calendar year, municipality (Comune), and industrial sector (ATECO).

## Methodology

### Coverage Calculation Formula

For each employment contract:

1. **Weekly Effort**:
   - Full-time (prior == 1): `effort = 1` if hours is NA or 40, else `effort = hours/40`
   - Part-time (prior == 0): `effort = hours/40`, or `20/40` if hours is NA
   - Intermittent (prior == 2): `effort = hours/40`, or `20/40` if hours is NA
   - Overlapping contracts: Sum of efforts capped at `48/40` (1.2 FTE maximum)

2. **Annual Percentage**:
   ```
   annual_pct = contract_days_in_year / 365
   ```

3. **Annual Coverage**:
   ```
   annual_coverage = weekly_effort * annual_pct
   ```

### Multi-Year Contracts

Contracts spanning multiple calendar years are split, with separate coverage calculations for each year:
- Start/end dates are clipped to year boundaries
- A full-year full-time contract has coverage = 1.0

### Aggregation

Coverage is aggregated by:
- **Calendar Year**: 2021, 2022, 2023, 2024
- **Comune**: Workplace municipality (`COMUNE_SEDE_LAVORO`)
- **Sector**: ATECO industrial sector code

## Usage

### 1. Via Pipeline (Recommended)

The coverage analysis is integrated into the main `_targets.R` pipeline:

```bash
# Run full pipeline (includes coverage analysis)
./update_data.sh

# Or run specific targets
Rscript -e "library(targets); tar_make(annual_coverage_workplace)"

# Or use the dedicated script
Rscript run_coverage_analysis.R
```

The pipeline will:
1. Filter workplace data to employment spells only
2. Calculate annual coverage by year, Comune, and sector
3. Save results to `output/dashboard_workplace/annual_coverage_by_year_comune_sector.fst`

### 2. Standalone Testing

Run the test script with validation cases:

```bash
Rscript test_coverage_analysis.R
```

This runs validation tests and can optionally process the full workplace dataset if the pipeline has been run.

### 3. In R Console

```r
library(data.table)
library(targets)

# Load functions
source("R/coverage_analysis.R")

# Load workplace employment data
tar_load(data_enriched_workplace)
workplace_employment <- data_enriched_workplace[arco != 0]  # Employment spells only

# Run analysis
coverage_summary <- estimate_annual_coverage(workplace_employment)

# View results
head(coverage_summary)

# Summary by year
coverage_summary[, .(
  total_coverage = sum(total_coverage),
  n_comuni = uniqueN(COMUNE_SEDE_LAVORO),
  n_sectors = uniqueN(ateco)
), by = year]
```

### 4. Pipeline Integration

The coverage analysis is already integrated into `_targets.R` with these targets:

- `annual_coverage_workplace`: Computes annual coverage from workplace employment data
- `output_annual_coverage_workplace`: Writes results to FST file

Location in pipeline: After workplace transition matrices (Phase 9), before file outputs (Phase 10).

## Output Structure

The output file contains aggregated coverage with these columns:

- `year`: Calendar year (integer)
- `COMUNE_SEDE_LAVORO`: Workplace municipality code
- `ateco`: ATECO sector code
- `total_coverage`: Total FTE coverage for this combination
- `n_contracts`: Number of contracts
- `n_workers`: Number of unique workers
- `mean_coverage`: Mean coverage per contract
- `median_coverage`: Median coverage per contract

## Validation Tests

The module includes validation tests in `test_coverage_analysis.R`:

1. **Full-year full-time**: Contract from Jan 1 - Dec 31, 40h/week → coverage = 1.0 ✓
2. **Half-year full-time**: Contract from Jan 1 - Jun 30, 40h/week → coverage ≈ 0.5 ✓
3. **Full-year part-time**: Contract from Jan 1 - Dec 31, 20h/week → coverage = 0.5 ✓
4. **Multi-year contract**: Contract spanning 2 years splits correctly → total coverage = 1.0 ✓

## Notes and Limitations

### Overlap Detection

The current implementation detects overlapping contracts for the same person and sums their weekly hours (capped at 48). However, complex overlap scenarios may require additional refinement.

### Data Requirements

The function expects workplace branch data with these columns:
- `cf`: Person identifier
- `inizio`, `fine`: Contract start and end dates (IDate)
- `prior`: Full-time indicator (1 = full-time, 0 = part-time, 2 = intermittent)
- `ore`: Weekly hours
- `COMUNE_SEDE_LAVORO`: Workplace municipality
- `ateco`: ATECO sector code
- `arco`: Employment indicator (0 = unemployment, != 0 = employment)

### Performance

For large datasets (millions of contracts), the year-splitting operation may be memory-intensive. Consider processing in batches if needed.

## Author

Giampaolo Montaletti (giampaolo.montaletti@gmail.com)
