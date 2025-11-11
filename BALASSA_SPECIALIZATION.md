# Balassa Index and Territorial Specialization Analysis

## Overview

This module calculates the Balassa Index (Revealed Comparative Advantage - RCA) for the annual coverage data and creates spatial visualizations of territorial specializations using the `situas` library.

## Balassa Index (RCA)

### Definition

The Balassa Index measures the relative concentration of a sector in a specific territory compared to the regional average.

### Formula

```
RCA_ij = (X_ij / X_i) / (X_j / X_total)
```

Where:
- **X_ij** = FTE coverage of sector j in comune i
- **X_i** = Total FTE coverage in comune i (all sectors)
- **X_j** = Total FTE coverage of sector j (all comuni)
- **X_total** = Total FTE coverage in Lombardy

### Interpretation

- **RCA > 1**: Comune is **specialized** in that sector (higher concentration than regional average)
- **RCA = 1**: Comune matches regional average
- **RCA < 1**: Comune is **under-represented** in that sector

### Thresholds

Common interpretation thresholds:
- **RCA > 1.0**: Specialized
- **RCA > 1.5**: Highly specialized
- **RCA > 2.0**: Very highly specialized
- **RCA > 5.0**: Extremely specialized (often microspecializations)

## Files and Modules

### R Modules

**`R/balassa_specialization.R`** - Core functions:
- `calculate_balassa_index()` - Calculates RCA for all year-comune-sector combinations
- `get_top_specializations()` - Identifies top N specializations per comune
- `aggregate_specialization_by_comune()` - Creates comune-level summary statistics
- `create_specialization_sf()` - Generates spatial dataframe with `situas` geometries

### Scripts

**`add_balassa_and_create_map.R`** - Main execution script:
1. Loads annual coverage data
2. Calculates Balassa indices
3. Updates coverage FST file with RCA column
4. Creates specialization sf dataframe for 2024
5. Saves outputs

## Output Files

### 1. Updated Coverage File

**File**: `output/dashboard_workplace/annual_coverage_by_year_comune_sector.fst`

**New column**: `balassa_index` (numeric)

Contains RCA values for all 284,590 year-comune-sector combinations (2021-2024).

**Statistics** (current):
- Mean RCA: 7.21
- Median RCA: 1.10
- Specialized combinations (RCA > 1): 149,776 (52.6%)
- Highly specialized (RCA > 1.5): 118,039 (41.5%)
- Very highly specialized (RCA > 2): 97,310 (34.2%)

### 2. Specialization Spatial Dataframe

**File**: `output/dashboard_workplace/comuni_specialization_2024.rds`

**Format**: sf dataframe (Simple Features)

**Rows**: 1,579 comuni (entire Lombardy)

**Key Columns**:
- `year`: 2024
- `PRO_COM_T`: ISTAT comune code (6 digits)
- `COMUNE`: Comune name
- `DEN_UTS`: Province name
- `cpi`: CPI code
- `total_coverage`: Total FTE in comune
- `n_workers`: Number of workers
- `n_sectors`: Number of active sectors
- `n_specializations`: Count of sectors with RCA > 1
- `dominant_sector`: Sector with highest FTE coverage
- `dominant_sector_label`: Name of dominant sector
- `dominant_sector_coverage`: FTE of dominant sector
- `dominant_sector_share`: % of total coverage
- `top_spec_sector`: Sector with highest RCA
- `top_spec_sector_label`: Name of most specialized sector
- `top_spec_rca`: RCA value of top specialization
- `mean_rca`: Average RCA across all sectors in comune
- `mean_coverage`: Average FTE per contract
- `geometry`: Spatial polygon (MULTIPOLYGON)

**CRS**: EPSG:4326 (WGS84 - suitable for web mapping)

**Coverage Statistics** (2024):
- Comuni with data: 1,576 / 1,579 (99.8%)
- Total FTE: 1,409,218
- Mean FTE per comune: 894
- Comuni with specializations: 1,576 (100% of comuni with data)
- Mean specializations per comune: 26.2
- Max specializations in a comune: 99

## Usage

### Running the Analysis

```bash
# Execute full process
Rscript add_balassa_and_create_map.R
```

This will:
1. Calculate Balassa indices (if not present or to recalculate)
2. Update the coverage FST file
3. Generate the 2024 specialization map

### In R Console

```r
library(data.table)
library(fst)
library(sf)

# Load updated coverage data with Balassa index
coverage <- read_fst("output/dashboard_workplace/annual_coverage_by_year_comune_sector.fst",
                     as.data.table = TRUE)

# View highly specialized combinations (RCA > 2)
highly_spec <- coverage[year == 2024 & balassa_index > 2]
print(highly_spec[order(-balassa_index)][1:10])

# Load specialization sf dataframe
comuni_sf <- readRDS("output/dashboard_workplace/comuni_specialization_2024.rds")

# Quick visualization
library(ggplot2)
ggplot(comuni_sf) +
  geom_sf(aes(fill = n_specializations), color = "white", size = 0.1) +
  scale_fill_viridis_c(option = "plasma") +
  theme_minimal() +
  labs(title = "Number of Specializations by Comune (RCA > 1)",
       fill = "N. Specializations")
```

### Dashboard Integration

The sf dataframe is ready for dashboard visualization with:
- **Leaflet**: Interactive web maps
- **ggplot2**: Static maps
- **Power BI**: Via `situas::sf_to_powerbi_topojson()`
- **Tableau**: Via GeoJSON export

Example for Leaflet:

```r
library(leaflet)
library(RColorBrewer)

# Create color palette
pal <- colorNumeric(palette = "YlOrRd", domain = comuni_sf$n_specializations)

# Create map
leaflet(comuni_sf) %>%
  addTiles() %>%
  addPolygons(
    fillColor = ~pal(n_specializations),
    fillOpacity = 0.7,
    color = "white",
    weight = 1,
    popup = ~paste0(
      "<b>", COMUNE, "</b><br>",
      "Province: ", DEN_UTS, "<br>",
      "Total FTE: ", round(total_coverage), "<br>",
      "Workers: ", n_workers, "<br>",
      "Specializations: ", n_specializations, "<br>",
      "Dominant sector: ", dominant_sector_label, " (",
      round(dominant_sector_share * 100, 1), "%)<br>",
      "Top RCA: ", round(top_spec_rca, 2), " (", top_spec_sector_label, ")"
    )
  ) %>%
  addLegend("bottomright", pal = pal, values = ~n_specializations,
            title = "N. Specializations",
            opacity = 0.7)
```

## Examples of Specialization Patterns

### Top 5 Most Specialized Combinations (2024)

| Comune | Sector | FTE | Workers | RCA |
|--------|--------|-----|---------|-----|
| H450 | 1.7 (Cereals) | 1.05 | 2 | 37,221 |
| H396 | 1.7 (Cereals) | 1.00 | 1 | 17,441 |
| L623 | 7.2 (Mining) | 0.92 | 1 | 13,310 |
| C506 | 98.1 (Private households) | 5.78 | 15 | 8,419 |
| A811 | 1.7 (Cereals) | 1.25 | 2 | 6,320 |

**Note**: Very high RCA values (>100) typically indicate **microspecializations** where:
- Small comuni have few workers in a specific niche sector
- That sector is rare regionally
- The combination creates extreme relative concentration

These are statistically valid but should be interpreted with caution for policy purposes.

### Meaningful Specializations

For policy-relevant analysis, consider filtering to:
- Minimum absolute coverage (e.g., `total_coverage > 10 FTE`)
- Moderate RCA thresholds (e.g., `1.5 < RCA < 10`)
- Minimum worker count (e.g., `n_workers > 20`)

Example:

```r
# Policy-relevant specializations
meaningful_spec <- coverage[
  year == 2024 &
  balassa_index > 1.5 &
  balassa_index < 10 &
  total_coverage > 10 &
  n_workers > 20
]
```

## Technical Notes

### Spatial Join with situas

The analysis uses the `situas` library to load Lombardy comuni boundaries. The key challenge is matching identifiers:

- **Coverage data** uses **Belfiore codes** (4-letter: "F205", "A473")
- **situas geometries** use **ISTAT codes** (6-digit: "015146", "098060")

The join is performed via the `territoriale` classifier:
1. Extract Belfiore → ISTAT mapping from `classifiers$territoriale`
2. Add ISTAT code to coverage data
3. Join with `comuni_lom` geometries on ISTAT code

Match rate: **1,576 / 1,579 comuni (99.8%)**

Unmatched comuni are typically:
- Historical comuni (abolished/merged)
- Special administrative units
- Border territories with partial coverage

### Performance

**Processing time**: ~10-15 seconds
- Balassa calculation: Fast (vectorized data.table operations)
- Spatial join: Fast (simple merge on integer keys)
- File I/O: ~2 seconds (FST write with compression 85)

**Memory usage**: ~50 MB peak

**Scalability**: Can handle millions of records efficiently

## Dependencies

### R Packages

Required:
- `data.table` - Data manipulation
- `fst` - Fast file I/O
- `sf` - Spatial data
- `situas` - Lombardy boundaries (install from GitHub)
- `targets` - For classifier access

Suggested for visualization:
- `ggplot2` - Static maps
- `leaflet` - Interactive maps
- `viridis` / `RColorBrewer` - Color palettes

### Installing situas

```r
devtools::install_github("gmontaletti/situas")
```

## References

### Balassa Index

- Balassa, B. (1965). "Trade Liberalisation and Revealed Comparative Advantage". The Manchester School, 33(2): 99-123.
- Commonly used in economic geography, industrial policy, and regional specialization analysis

### Related Analysis

The Balassa index complements:
- **Location Quotient (LQ)**: Similar concept, different normalization
- **Specialization indices**: Krugman, Herfindahl-Hirschman
- **Cluster analysis**: Identifies geographic concentrations of economic activity

## Future Developments

Possible extensions:
1. **Temporal dynamics**: Track RCA evolution 2021-2024, identify emerging/declining specializations
2. **Network analysis**: Identify sectoral clusters and complementarities
3. **Comparative analysis**: Residence vs Workplace branch specialization patterns
4. **Predictive modeling**: Forecast specialization trends
5. **Policy impact**: Link specialization with DID/POL interventions

## Author

Giampaolo Montaletti
Email: giampaolo.montaletti@gmail.com
ORCID: https://orcid.org/0009-0002-5327-1122
