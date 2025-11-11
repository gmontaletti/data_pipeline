# MONECA Labor Market Segmentation

**Author**: Giampaolo Montaletti
**Email**: giampaolo.montaletti@gmail.com
**GitHub**: github.com/gmontaletti
**ORCID**: https://orcid.org/0009-0002-5327-1122

## Overview

This document describes the implementation of MONECA (MONtecarlo for Community Analysis) clustering for identifying labor market segments in the Lombardy employment data pipeline. MONECA applies hierarchical network segmentation to sector mobility matrices, revealing natural groupings of economic sectors based on worker transition patterns.

## Rationale

While ATECO sector codes provide a standardized economic classification, labor market dynamics may reveal different functional groupings. Workers often transition between sectors that share similar skills, working conditions, or career pathways even if those sectors appear distinct in traditional classifications. MONECA clustering identifies these "labor market segments" based on empirical transition data.

## Benefits

- **Data-Driven Segmentation**: Segments emerge from actual worker mobility patterns, not administrative classifications
- **Interpretable Groupings**: Segments represent sectors with high internal mobility (career clusters)
- **Comparative Analysis**: Parallel ATECO and segment-based specialization analyses reveal different territorial dynamics
- **Policy Relevance**: Identifies skill transfer opportunities and sector affinities for workforce development

## Implementation

### Pipeline Integration

The MONECA workflow is integrated into `_targets.R` as Phase 9, executing after sector transition matrices are computed. The workflow creates two parallel segmentation analyses:

1. **Standard Matrix Segmentation**: Uses all employment transitions
2. **45-Day Lag Segmentation**: Filters to unemployment durations >45 days (meaningful career transitions)

### Workflow Stages

#### Stage 0: Cutoff Analysis (Performance Optimization)

**Targets**: `optimal_cutoff_standard`, `optimal_cutoff_45day`

```r
moneca::find_optimal_cutoff(
  mx = sector_matrix_workplace,
  criterion = "elbow",
  cutoff_range = NULL,  # Auto-detect
  n_bootstrap = 30,
  verbose = TRUE
)
```

**Parameters**:
- `criterion = "elbow"`: Elbow method identifies the point of diminishing returns in the cutoff-strength curve
- `n_bootstrap = 30`: Number of bootstrap samples for robust estimation
- `cutoff_range = NULL`: Automatically detects appropriate cutoff range

**Performance**: ~10-30 seconds per matrix (5-7x faster than full auto-tuning)

**Output**:
- `optimal_cutoff`: The optimal relative risk threshold to use for clustering
- Analysis results including bootstrap statistics

#### Stage 1: MONECA Fast Clustering

**Targets**: `moneca_standard`, `moneca_45day`

```r
cluster_labor_market(
  sector_matrix_workplace,
  cut_off = optimal_cutoff_standard$optimal_cutoff,
  segment_levels = 3,
  verbose = TRUE
)
```

**Parameters**:
- `cut_off`: Pre-computed optimal cutoff from Stage 0 (e.g., 1.5, 2.0)
- `segment_levels = 3`: Number of hierarchical segmentation levels

**Implementation**: Uses `moneca_fast()` with:
- No auto-tuning (`auto_tune = FALSE`)
- Pre-computed optimal cutoff
- Fixed `small.cell.reduction = 0`
- Optimized performance

**Performance**: ~30-60 seconds per matrix (vs. 5-15 minutes with auto-tuning)

**Output**:
- Full MONECA object with hierarchy
- Segment membership (sector → segment_id mapping)
- Number of identified segments

#### Stage 2: Composition Analysis

**Targets**: `segment_composition_standard`, `segment_composition_45day`

Analyzes each segment's characteristics:
- Number of sectors
- Total and within-segment transition volumes
- Cohesion (proportion of within-segment transitions)
- Dominant sectors (top 5 by transition volume)
- Demographics (average salary, age, gender distribution) if transition data provided

**Function**: `analyze_segment_composition()`

#### Stage 3: Template Generation

**Targets**: `segment_naming_template_standard`, `segment_naming_template_45day`

Creates R script templates in `reference/data_pipeline/` with:
- Segment composition as comments
- Sector lists with transition volumes
- Empty strings for user to fill with meaningful names

**Template Format**:
```r
# Segment 1 (35 sectors)
# Total transitions: 125,430
# Cohesion (within-segment): 67.3%
# Avg salary: €1,850
# Median age: 38.2 years
# Female: 42.1%
# Top sectors by volume:
#   1. Manufacturing: Metal products (35.2%)
#   2. Manufacturing: Machinery (28.1%)
#   3. Construction: Building (22.4%)
#   4. Wholesale trade (14.3%)
segment_names <- c(
  "1" = "",  # <- Fill meaningful name here
  "2" = "",
  ...
)
```

**Files Generated**:
- `reference/data_pipeline/segment_names_standard.R`
- `reference/data_pipeline/segment_names_45day.R`

#### Stage 4: Manual Naming (User Action Required)

**Action**: Edit the generated template files to assign meaningful names based on segment composition.

**Naming Guidelines**:
1. **Review composition**: Examine dominant sectors, cohesion, and demographics
2. **Identify themes**: Look for patterns (industrial, services, knowledge work, manual labor, etc.)
3. **Use descriptive names**: Prefer interpretive labels over generic ones
   - Good: "Industrial Manufacturing Core", "Knowledge Economy", "Personal Services"
   - Avoid: "Segment 1", "Group A", "Cluster X"
4. **Consider context**: Names should be meaningful for dashboard users and policymakers
5. **Be concise**: Keep names under 30 characters when possible

**Example Names**:
- "Industrial Manufacturing Core" (metal, machinery, construction)
- "Knowledge Economy" (IT, professional services, finance)
- "Personal Services" (retail, hospitality, care services)
- "Logistics & Trade" (transport, wholesale, warehousing)

#### Stage 5: Conditional Segment-Based Specialization

**Targets**: `segment_membership_named_standard`, `coverage_with_segments_standard`, `comuni_specialization_segments_standard_2024` (and 45-day equivalents)

**Conditional Execution**: These targets only run if naming templates are complete (all segments have non-empty names).

**Process**:
1. **Load Names**: `apply_segment_names()` reads template and validates completeness
2. **Join with Coverage**: Annual coverage data joined with segment membership via ATECO code
3. **Aggregate**: Coverage summed by year, comune, and segment
4. **Balassa by Segment**: `calculate_balassa_index_by_segment()` computes RCA using segments instead of ATECO codes
5. **Spatial Dataframe**: `create_specialization_sf_segments()` creates sf object for mapping

**Formula**:
```
RCA_segment_comune = (Coverage_segment_comune / Coverage_comune) /
                     (Coverage_segment_region / Coverage_total_region)
```

Interpretation remains the same: RCA > 1 indicates a comune is specialized in that labor market segment.

### Key Modules

#### R/moneca_segmentation.R

**Functions**:
- `cluster_labor_market()`: MONECA clustering with auto-tuning
- `extract_segment_membership()`: Convert MONECA output to clean sector→segment mapping
- `analyze_segment_composition()`: Compute segment statistics
- `generate_naming_template()`: Create R script template with composition as comments
- `apply_segment_names()`: Load user names and merge with membership
- `is_template_complete()`: Check if user has filled all names

#### R/balassa_specialization.R (Extended)

**New Functions**:
- `calculate_balassa_index_by_segment()`: Balassa RCA by segment
- `aggregate_specialization_by_comune_segments()`: Per-comune segment summary
- `create_specialization_sf_segments()`: Spatial dataframe with segment specializations

### Output Files

All outputs are saved to `output/dashboard_workplace/`:

**Cutoff Analysis** (RDS):
- `optimal_cutoff_standard.rds`: Optimal cutoff results for standard matrix (includes cutoff value, bootstrap stats)
- `optimal_cutoff_45day.rds`: Optimal cutoff results for 45-day matrix

**MONECA Objects** (RDS):
- `moneca_standard.rds`: Full MONECA object (standard matrix)
- `moneca_45day.rds`: Full MONECA object (45-day lag matrix)
- `segment_membership_standard.rds`: Sector→segment mapping (unnamed)
- `segment_membership_45day.rds`: Sector→segment mapping (unnamed)
- `segment_composition_standard.rds`: Composition statistics
- `segment_composition_45day.rds`: Composition statistics
- `segment_membership_named_standard.rds`: Named memberships (if template complete)
- `segment_membership_named_45day.rds`: Named memberships (if template complete)

**Segment-Based Specialization** (conditional, if templates complete):
- `coverage_with_segments_standard.fst`: Annual coverage by year/comune/segment with Balassa index
- `coverage_with_segments_45day.fst`: Same for 45-day segmentation
- `comuni_specialization_segments_standard_2024.rds`: Spatial dataframe (sf) for 2024
- `comuni_specialization_segments_45day_2024.rds`: Spatial dataframe (sf) for 2024

**Templates** (reference/):
- `reference/data_pipeline/segment_names_standard.R`: Naming template for standard segmentation
- `reference/data_pipeline/segment_names_45day.R`: Naming template for 45-day segmentation

## Running the Pipeline

### First Run: Generate Templates

```r
library(targets)

# Run through template generation
tar_make(c("segment_naming_template_standard",
           "segment_naming_template_45day"))
```

**Expected Runtime**:
- Cutoff analysis: ~10-30 seconds per matrix
- MONECA fast clustering: ~30-60 seconds per matrix
- Composition analysis & templates: ~5-10 seconds
- **Total first run: ~2-3 minutes** (vs. 10-15 minutes with auto-tuning)

**Performance Improvement**: **5-7x speedup** compared to auto-tuning approach

### Manual Naming Step

1. Open `reference/data_pipeline/segment_names_standard.R`
2. Review composition for each segment
3. Replace empty strings with meaningful names
4. Save file
5. Repeat for `segment_names_45day.R`

### Second Run: Generate Segment-Based Specializations

```r
# After filling templates, run full pipeline
tar_make()
```

The conditional targets will now execute, generating segment-based Balassa indices and spatial dataframes.

## Dashboard Integration

### Spatial Visualization

Use the sf dataframes to create Leaflet maps showing comuni colored by dominant labor market segment:

```r
library(leaflet)
library(sf)

# Load segment specialization spatial data
comuni_sf <- readRDS("output/dashboard_workplace/comuni_specialization_segments_standard_2024.rds")

# Create map
leaflet(comuni_sf) %>%
  addProviderTiles("CartoDB.Positron") %>%
  addPolygons(
    fillColor = ~colorFactor("Set3", dominant_segment)(dominant_segment),
    fillOpacity = 0.7,
    weight = 1,
    color = "#666",
    label = ~paste0(COMUNE, ": ", dominant_segment)
  ) %>%
  addLegend(
    "bottomright",
    pal = colorFactor("Set3", comuni_sf$dominant_segment),
    values = ~dominant_segment,
    title = "Dominant Labor Market Segment"
  )
```

### Comparative Analysis

Compare ATECO-based and segment-based specializations:

```r
# Load both specialization datasets
ateco_sf <- readRDS("output/dashboard_workplace/comuni_specialization_2024.rds")
segment_sf <- readRDS("output/dashboard_workplace/comuni_specialization_segments_standard_2024.rds")

# Merge on comune
comparison <- merge(
  sf::st_drop_geometry(ateco_sf),
  sf::st_drop_geometry(segment_sf),
  by = "COMUNE",
  suffixes = c("_ateco", "_segment")
)

# Analyze differences
comparison[, .(
  COMUNE,
  n_specializations_ateco,
  n_specializations_segment,
  dominant_sector_label,
  dominant_segment
)]
```

### Time Series

Segment-based coverage data enables temporal analysis:

```r
library(data.table)

# Load segment coverage
coverage <- fst::read_fst("output/dashboard_workplace/coverage_with_segments_standard.fst",
                          as.data.table = TRUE)

# Annual FTE by segment
segment_ts <- coverage[, .(total_fte = sum(total_coverage)), by = .(year, segment_name)]

# Plot
library(ggplot2)
ggplot(segment_ts, aes(x = year, y = total_fte, color = segment_name)) +
  geom_line(linewidth = 1) +
  labs(title = "Employment by Labor Market Segment",
       x = "Year", y = "Full-Time Equivalents", color = "Segment") +
  theme_minimal()
```

## Performance Considerations

### Computational Requirements

- **Matrix Size**: 270 x 270 sectors = 72,900 cells
- **Cutoff Analysis**: ~10-30 seconds (elbow method with 30 bootstrap samples)
- **Clustering**: ~30-60 seconds with `moneca_fast()`
- **Total Time**: ~2-3 minutes for both segmentations (standard + 45-day)

### Performance Optimizations Implemented

✅ **Pre-computed Cutoffs**: Cutoff analysis replaces auto-tuning
✅ **moneca_fast()**: Optimized implementation (no parameter search)
✅ **Elbow Criterion**: Fast, robust cutoff selection
✅ **No Auto-Tuning**: Eliminates 15 parameter evaluations

**Result**: **5-7x speedup** compared to auto-tuning approach

### Additional Optimization Strategies

If further speedup is needed:

1. **Reduce bootstrap samples**: Use `n_bootstrap = 10` instead of 30 (faster but less robust)
2. **Reduce segment levels**: Use `segment_levels = 2` instead of 3
3. **Filter matrix**: Remove rare sectors with few transitions before clustering
4. **Cache cutoffs**: Reuse computed cutoffs if matrix doesn't change

### Memory Usage

- Cutoff analysis results: ~1-2 MB each
- MONECA objects: ~50-100 MB each
- Coverage with segments: ~200-300 MB (FST compressed)
- Spatial dataframes: ~5-10 MB each

Total additional storage: ~500-600 MB

## Interpretation Guidelines

### Segment Cohesion

- **High cohesion (>70%)**: Strong internal labor market, workers rarely leave segment
- **Moderate cohesion (40-70%)**: Mixed dynamics, some cross-segment mobility
- **Low cohesion (<40%)**: Transitional segment or bridge between labor markets

### Balassa Index by Segment

Same interpretation as ATECO-based RCA:
- **RCA > 2**: Strong specialization
- **1 < RCA ≤ 2**: Moderate specialization
- **RCA ≤ 1**: No specialization (under-represented)

### Standard vs. 45-Day Segmentations

- **Standard**: Includes all transitions (rapid rehires, short-term contracts)
- **45-Day**: Filters to meaningful career transitions (excludes rapid returns)

Differences reveal:
- Segments with high temporary/seasonal employment
- Stable career pathways vs. precarious transitions
- Policy-relevant distinctions for training programs

## Troubleshooting

### Cutoff Analysis Takes Too Long

**Symptom**: `find_optimal_cutoff` runs longer than expected
**Cause**: High bootstrap sample count or dense matrix
**Solution**:
1. Reduce bootstrap samples: `n_bootstrap = 10` (faster, slightly less robust)
2. Check matrix density: Very dense matrices take longer
3. Expected time: 10-30 seconds is normal for 270x270 matrix

### Pipeline Stalls During MONECA Clustering

**Symptom**: Clustering target doesn't complete
**Cause**: Large matrix size or insufficient memory
**Solution**:
1. Reduce `segment_levels` to 2
2. Check cutoff value (very high cutoffs may cause issues)
3. Typical time with `moneca_fast`: 30-60 seconds

### Template Incomplete Warning

**Symptom**: "⏸ Segment naming template not complete. Skipping."
**Cause**: Some segment names still have empty strings `""`
**Solution**: Edit template files and ensure all segments have names

### No Segment-Based Outputs Generated

**Symptom**: Conditional targets skipped
**Cause**: Templates not complete
**Solution**:
1. Check `reference/data_pipeline/segment_names_*.R`
2. Ensure no empty strings remain
3. Rerun pipeline

### Balassa Index Calculation Errors

**Symptom**: Error about missing segment column
**Cause**: Segment membership not properly joined with coverage data
**Solution**:
1. Verify ATECO codes match between coverage and membership
2. Check for NA values in segment assignments
3. Review merge logic in `coverage_with_segments_*` targets

## References

- **MONECA Package**: https://github.com/gmontaletti/MONECA
- **Balassa Index**: Balassa, B. (1965). Trade Liberalisation and "Revealed" Comparative Advantage. *The Manchester School*, 33(2), 99-123.
- **Targets Pipeline**: https://books.ropensci.org/targets/

## Version History

- **v1.1 (2025-11-11)**: Performance optimization - replaced auto-tuning with cutoff analysis + moneca_fast (5-7x speedup)
- **v1.0 (2025-11-11)**: Initial implementation with dual-branch segmentation and conditional specialization workflow
