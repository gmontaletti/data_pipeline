# balassa_specialization.R
# Functions for calculating Balassa index and creating specialization maps
# Author: Giampaolo Montaletti (giampaolo.montaletti@gmail.com)

# 1. Balassa Index calculation -----

#' Calculate Balassa Index (Revealed Comparative Advantage)
#'
#' Computes the Balassa index for each year-comune-sector combination.
#'
#' Formula: RCA_ij = (X_ij / X_i) / (X_j / X_total)
#' Where:
#'   X_ij = coverage of sector j in comune i
#'   X_i = total coverage in comune i (all sectors)
#'   X_j = total coverage of sector j (all comuni)
#'   X_total = total coverage in the region
#'
#' Interpretation:
#'   RCA > 1: Comune is specialized in that sector
#'   RCA < 1: Comune is under-represented in that sector
#'   RCA = 1: Comune matches regional average
#'
#' @param dt data.table with columns: year, COMUNE_SEDE_LAVORO, ateco, total_coverage
#' @return data.table with added balassa_index column
#' @export
calculate_balassa_index <- function(dt) {

  cat("Calculating Balassa Index...\n")

  # Ensure data.table
  dt <- data.table::copy(dt)
  data.table::setDT(dt)

  # Calculate totals by comune (X_i)
  dt[, total_comune := sum(total_coverage), by = .(year, COMUNE_SEDE_LAVORO)]

  # Calculate totals by sector (X_j)
  dt[, total_sector := sum(total_coverage), by = .(year, ateco)]

  # Calculate regional total (X_total)
  dt[, total_region := sum(total_coverage), by = year]

  # Calculate Balassa index
  dt[, balassa_index := (total_coverage / total_comune) / (total_sector / total_region)]

  # Clean up intermediate columns
  dt[, c("total_comune", "total_sector", "total_region") := NULL]

  # Summary statistics
  cat(sprintf("  Balassa index calculated for %s records\n",
              format(nrow(dt), big.mark = ",")))
  cat(sprintf("  Mean RCA: %.3f\n", mean(dt$balassa_index, na.rm = TRUE)))
  cat(sprintf("  Median RCA: %.3f\n", median(dt$balassa_index, na.rm = TRUE)))
  cat(sprintf("  Specialized combinations (RCA > 1): %s (%.1f%%)\n",
              format(sum(dt$balassa_index > 1, na.rm = TRUE), big.mark = ","),
              100 * mean(dt$balassa_index > 1, na.rm = TRUE)))

  return(dt)
}


# 2. Specialization analysis -----

#' Identify top specializations for each comune
#'
#' For each comune, identifies the sectors where it is most specialized
#' (highest Balassa index values).
#'
#' @param dt data.table with balassa_index column
#' @param year_filter Year to filter (default: 2024)
#' @param min_rca Minimum RCA threshold to consider as specialization (default: 1.0)
#' @param top_n Number of top specializations to keep per comune (default: 5)
#' @return data.table with top specializations per comune
#' @export
get_top_specializations <- function(dt, year_filter = 2024, min_rca = 1.0, top_n = 5) {

  cat(sprintf("Identifying top specializations for year %d...\n", year_filter))

  # Filter to specified year
  dt_year <- dt[year == year_filter]

  # Filter to specializations only
  dt_spec <- dt_year[balassa_index >= min_rca]

  cat(sprintf("  Comuni with at least one specialization: %d\n",
              data.table::uniqueN(dt_spec$COMUNE_SEDE_LAVORO)))

  # Rank specializations within each comune
  data.table::setorder(dt_spec, COMUNE_SEDE_LAVORO, -balassa_index)
  dt_spec[, rank := seq_len(.N), by = COMUNE_SEDE_LAVORO]

  # Keep only top N
  dt_top <- dt_spec[rank <= top_n]

  cat(sprintf("  Total specialization records: %s\n",
              format(nrow(dt_top), big.mark = ",")))

  return(dt_top)
}


#' Aggregate specialization data by comune
#'
#' Creates a summary of specializations for each comune, suitable for mapping.
#'
#' @param dt data.table with balassa_index column
#' @param year_filter Year to filter (default: 2024)
#' @param classifiers Optional list with settori classifier for sector labels
#' @return data.table with one row per comune
#' @export
aggregate_specialization_by_comune <- function(dt, year_filter = 2024, classifiers = NULL) {

  cat(sprintf("Aggregating specialization data by comune for year %d...\n", year_filter))

  # Filter to specified year
  dt_year <- dt[year == year_filter]

  # For each comune, calculate:
  # 1. Total coverage
  # 2. Number of sectors
  # 3. Dominant sector (highest coverage)
  # 4. Top specialization (highest RCA)
  # 5. Number of specializations (RCA > 1)

  comune_summary <- dt_year[, .(
    total_coverage = sum(total_coverage),
    n_sectors = .N,
    n_workers = sum(n_workers),
    mean_coverage = mean(mean_coverage),
    # Dominant sector (by coverage)
    dominant_sector = ateco[which.max(total_coverage)],
    dominant_sector_coverage = max(total_coverage),
    dominant_sector_share = max(total_coverage) / sum(total_coverage),
    # Top specialization (by RCA)
    top_spec_sector = ateco[which.max(balassa_index)],
    top_spec_rca = max(balassa_index, na.rm = TRUE),
    # Count specializations
    n_specializations = sum(balassa_index > 1, na.rm = TRUE),
    # Average RCA
    mean_rca = mean(balassa_index, na.rm = TRUE)
  ), by = COMUNE_SEDE_LAVORO]

  # Add sector labels if classifiers provided
  if (!is.null(classifiers) && "settori" %in% names(classifiers)) {
    settori <- classifiers$settori

    # Dominant sector label
    comune_summary <- merge(comune_summary,
                           settori[, .(ateco, dominant_sector_label = nome)],
                           by.x = "dominant_sector", by.y = "ateco",
                           all.x = TRUE)

    # Top specialization sector label
    comune_summary <- merge(comune_summary,
                           settori[, .(ateco, top_spec_sector_label = nome)],
                           by.x = "top_spec_sector", by.y = "ateco",
                           all.x = TRUE)
  }

  cat(sprintf("  Aggregated data for %d comuni\n", nrow(comune_summary)))
  cat(sprintf("  Mean specializations per comune: %.1f\n",
              mean(comune_summary$n_specializations)))

  return(comune_summary)
}


# 3. Spatial dataframe creation -----

#' Create sf dataframe for dashboard visualization
#'
#' Combines specialization data with Lombardy comuni geometries from situas.
#'
#' @param dt data.table with balassa_index column
#' @param year_filter Year to filter (default: 2024)
#' @param classifiers Optional list with settori classifier
#' @param crs_output Target CRS (default: "EPSG:4326" for web mapping)
#' @return sf dataframe with geometries and specialization data
#' @export
create_specialization_sf <- function(dt,
                                     year_filter = 2024,
                                     classifiers = NULL,
                                     crs_output = "EPSG:4326") {

  cat("\n")
  cat(rep("=", 70), "\n", sep = "")
  cat("CREATING SPECIALIZATION SF DATAFRAME\n")
  cat(rep("=", 70), "\n\n", sep = "")

  # Load Lombardy comuni geometries from situas
  cat("Loading Lombardy comuni geometries from situas...\n")
  pkg_dir <- system.file("data", package = "situas")
  comuni_lom <- readRDS(file.path(pkg_dir, "comuni_lom_map.rds"))

  cat(sprintf("  Loaded %d comuni geometries\n", nrow(comuni_lom)))
  cat(sprintf("  Current CRS: %s\n", sf::st_crs(comuni_lom)$input))

  # Aggregate specialization data by comune
  comune_summary <- aggregate_specialization_by_comune(dt, year_filter, classifiers)

  # Join with geometries
  cat("\nJoining specialization data with geometries...\n")

  # IMPORTANT: Our coverage data uses Belfiore codes (COMUNE_SEDE_LAVORO)
  # but comuni_lom uses ISTAT codes (PRO_COM_T)
  # We need to use the territoriale classifier to map between them

  if (!is.null(classifiers) && "territoriale" %in% names(classifiers)) {
    # Create Belfiore -> ISTAT mapping
    belfiore_to_istat <- unique(classifiers$territoriale[
      DES_REGIONE_PAUT == "LOMBARDIA" & !is.na(COD_ISTAT) & COD_ISTAT != "",
      .(belfiore = COD_COMUNE, istat = COD_ISTAT, comune_name = DES_COMUNE)
    ])

    cat(sprintf("  Created Belfiore-ISTAT mapping: %d codes\n", nrow(belfiore_to_istat)))

    # Add ISTAT code to comune_summary
    comune_summary <- merge(comune_summary, belfiore_to_istat,
                           by.x = "COMUNE_SEDE_LAVORO", by.y = "belfiore",
                           all.x = TRUE)

    # Join using ISTAT codes
    comuni_sf <- merge(comuni_lom, comune_summary,
                      by.x = "PRO_COM_T", by.y = "istat",
                      all.x = TRUE)

    n_matched <- sum(!is.na(comuni_sf$total_coverage))
    cat(sprintf("  Matched %d comuni with data\n", n_matched))
    cat(sprintf("  Unmatched comuni: %d\n", nrow(comuni_sf) - n_matched))

  } else {
    warning("Classifiers not available for Belfiore-ISTAT mapping. Direct join may fail.")
    # Fallback: direct join (will likely not match)
    comuni_sf <- merge(comuni_lom, comune_summary,
                      by.x = "PRO_COM_T", by.y = "COMUNE_SEDE_LAVORO",
                      all.x = TRUE)
  }

  # Ensure it's still an sf object
  comuni_sf <- sf::st_as_sf(comuni_sf)

  # Transform to desired CRS if different
  if (sf::st_crs(comuni_sf)$input != crs_output) {
    cat(sprintf("Transforming CRS to %s...\n", crs_output))
    comuni_sf <- sf::st_transform(comuni_sf, crs_output)
  }

  # Add year column
  comuni_sf$year <- year_filter

  # Reorder columns for better readability
  col_order <- c("year", "PRO_COM_T", "COMUNE", "DEN_UTS", "cpi",
                 "total_coverage", "n_workers", "n_sectors", "n_specializations",
                 "dominant_sector", "dominant_sector_label", "dominant_sector_coverage", "dominant_sector_share",
                 "top_spec_sector", "top_spec_sector_label", "top_spec_rca",
                 "mean_rca", "mean_coverage", "geometry")

  # Only include columns that exist
  col_order <- col_order[col_order %in% names(comuni_sf)]
  comuni_sf <- comuni_sf[, col_order]

  cat("\n")
  cat(rep("=", 70), "\n", sep = "")
  cat("SPECIALIZATION SF DATAFRAME CREATED\n")
  cat(rep("=", 70), "\n\n", sep = "")

  cat(sprintf("Rows: %d\n", nrow(comuni_sf)))
  cat(sprintf("Columns: %d\n", ncol(comuni_sf)))
  cat(sprintf("CRS: %s\n", sf::st_crs(comuni_sf)$input))
  cat(sprintf("Geometry type: %s\n", unique(sf::st_geometry_type(comuni_sf))))

  # Summary statistics
  cat("\nCoverage Statistics:\n")
  cat(sprintf("  Comuni with data: %d\n", sum(!is.na(comuni_sf$total_coverage))))
  cat(sprintf("  Total FTE: %.0f\n", sum(comuni_sf$total_coverage, na.rm = TRUE)))
  cat(sprintf("  Mean FTE per comune: %.0f\n", mean(comuni_sf$total_coverage, na.rm = TRUE)))

  cat("\nSpecialization Statistics:\n")
  cat(sprintf("  Comuni with specializations (RCA>1): %d\n",
              sum(comuni_sf$n_specializations > 0, na.rm = TRUE)))
  cat(sprintf("  Mean specializations per comune: %.1f\n",
              mean(comuni_sf$n_specializations, na.rm = TRUE)))
  max_spec <- max(comuni_sf$n_specializations, na.rm = TRUE)
  if (is.finite(max_spec)) {
    cat(sprintf("  Max specializations in a comune: %.0f\n", max_spec))
  } else {
    cat("  Max specializations in a comune: N/A\n")
  }

  return(comuni_sf)
}


# 4. Segment-based Balassa Index -----

#' Calculate Balassa Index by Labor Market Segment
#'
#' Computes the Balassa index for each year-comune-segment combination,
#' similar to calculate_balassa_index() but using labor market segments
#' instead of ATECO sectors.
#'
#' Formula: RCA_ij = (X_ij / X_i) / (X_j / X_total)
#' Where:
#'   X_ij = coverage of segment j in comune i
#'   X_i = total coverage in comune i (all segments)
#'   X_j = total coverage of segment j (all comuni)
#'   X_total = total coverage in the region
#'
#' @param dt data.table with columns: year, COMUNE_SEDE_LAVORO, segment_id or segment_name, total_coverage
#' @param segment_col Character. Name of segment column (default: "segment_name")
#' @return data.table with added balassa_index_segment column
#' @export
calculate_balassa_index_by_segment <- function(dt, segment_col = "segment_name") {

  cat("Calculating Balassa Index by labor market segment...\n")

  # Ensure data.table
  dt <- data.table::copy(dt)
  data.table::setDT(dt)

  # Check if segment column exists
  if (!segment_col %in% names(dt)) {
    stop("Segment column '", segment_col, "' not found in data")
  }

  # Calculate totals by comune (X_i)
  dt[, total_comune := sum(total_coverage), by = .(year, COMUNE_SEDE_LAVORO)]

  # Calculate totals by segment (X_j)
  dt[, total_segment := sum(total_coverage), by = c("year", segment_col)]

  # Calculate regional total (X_total)
  dt[, total_region := sum(total_coverage), by = year]

  # Calculate Balassa index
  dt[, balassa_index_segment := (total_coverage / total_comune) / (total_segment / total_region)]

  # Clean up intermediate columns
  dt[, c("total_comune", "total_segment", "total_region") := NULL]

  # Summary statistics
  cat(sprintf("  Balassa index (segment) calculated for %s records\n",
              format(nrow(dt), big.mark = ",")))
  cat(sprintf("  Mean RCA: %.3f\n", mean(dt$balassa_index_segment, na.rm = TRUE)))
  cat(sprintf("  Median RCA: %.3f\n", median(dt$balassa_index_segment, na.rm = TRUE)))
  cat(sprintf("  Specialized combinations (RCA > 1): %s (%.1f%%)\n",
              format(sum(dt$balassa_index_segment > 1, na.rm = TRUE), big.mark = ","),
              100 * mean(dt$balassa_index_segment > 1, na.rm = TRUE)))

  return(dt)
}


#' Aggregate segment-based specialization data by comune
#'
#' Creates a summary of segment specializations for each comune, suitable for mapping.
#' Similar to aggregate_specialization_by_comune() but works with labor market segments.
#'
#' @param dt data.table with balassa_index_segment column
#' @param year_filter Year to filter (default: 2024)
#' @param segment_col Character. Name of segment column (default: "segment_name")
#' @return data.table with one row per comune
#' @export
aggregate_specialization_by_comune_segments <- function(dt,
                                                        year_filter = 2024,
                                                        segment_col = "segment_name") {

  cat(sprintf("Aggregating segment specialization data by comune for year %d...\n", year_filter))

  # Filter to specified year
  dt_year <- dt[year == year_filter]

  # For each comune, calculate segment-based statistics
  comune_summary <- dt_year[, .(
    total_coverage = sum(total_coverage),
    n_segments = .N,
    n_workers = sum(n_workers),
    mean_coverage = mean(mean_coverage),
    # Dominant segment (by coverage)
    dominant_segment = get(segment_col)[which.max(total_coverage)],
    dominant_segment_coverage = max(total_coverage),
    dominant_segment_share = max(total_coverage) / sum(total_coverage),
    # Top specialization (by RCA)
    top_spec_segment = get(segment_col)[which.max(balassa_index_segment)],
    top_spec_rca = max(balassa_index_segment, na.rm = TRUE),
    # Count specializations
    n_specializations = sum(balassa_index_segment > 1, na.rm = TRUE),
    # Average RCA
    mean_rca = mean(balassa_index_segment, na.rm = TRUE)
  ), by = COMUNE_SEDE_LAVORO]

  cat(sprintf("  Aggregated data for %d comuni\n", nrow(comune_summary)))
  cat(sprintf("  Mean segment specializations per comune: %.1f\n",
              mean(comune_summary$n_specializations)))

  return(comune_summary)
}


#' Create sf dataframe for segment-based specialization visualization
#'
#' Combines segment specialization data with Lombardy comuni geometries from situas.
#' Similar to create_specialization_sf() but works with labor market segments.
#'
#' @param dt data.table with balassa_index_segment column
#' @param year_filter Year to filter (default: 2024)
#' @param segment_col Character. Name of segment column (default: "segment_name")
#' @param classifiers Optional list with territoriale classifier for Belfiore-ISTAT mapping
#' @param crs_output Target CRS (default: "EPSG:4326" for web mapping)
#' @return sf dataframe with geometries and segment specialization data
#' @export
create_specialization_sf_segments <- function(dt,
                                              year_filter = 2024,
                                              segment_col = "segment_name",
                                              classifiers = NULL,
                                              crs_output = "EPSG:4326") {

  cat("\n")
  cat(rep("=", 70), "\n", sep = "")
  cat("CREATING SEGMENT SPECIALIZATION SF DATAFRAME\n")
  cat(rep("=", 70), "\n\n", sep = "")

  # Load Lombardy comuni geometries from situas
  cat("Loading Lombardy comuni geometries from situas...\n")
  pkg_dir <- system.file("data", package = "situas")
  comuni_lom <- readRDS(file.path(pkg_dir, "comuni_lom_map.rds"))

  cat(sprintf("  Loaded %d comuni geometries\n", nrow(comuni_lom)))
  cat(sprintf("  Current CRS: %s\n", sf::st_crs(comuni_lom)$input))

  # Aggregate specialization data by comune
  comune_summary <- aggregate_specialization_by_comune_segments(dt, year_filter, segment_col)

  # Join with geometries
  cat("\nJoining segment specialization data with geometries...\n")

  # Use Belfiore -> ISTAT mapping from territoriale classifier
  if (!is.null(classifiers) && "territoriale" %in% names(classifiers)) {
    # Create Belfiore -> ISTAT mapping
    belfiore_to_istat <- unique(classifiers$territoriale[
      DES_REGIONE_PAUT == "LOMBARDIA" & !is.na(COD_ISTAT) & COD_ISTAT != "",
      .(belfiore = COD_COMUNE, istat = COD_ISTAT, comune_name = DES_COMUNE)
    ])

    cat(sprintf("  Created Belfiore-ISTAT mapping: %d codes\n", nrow(belfiore_to_istat)))

    # Add ISTAT code to comune_summary
    comune_summary <- merge(comune_summary, belfiore_to_istat,
                           by.x = "COMUNE_SEDE_LAVORO", by.y = "belfiore",
                           all.x = TRUE)

    # Join using ISTAT codes
    comuni_sf <- merge(comuni_lom, comune_summary,
                      by.x = "PRO_COM_T", by.y = "istat",
                      all.x = TRUE)

    n_matched <- sum(!is.na(comuni_sf$total_coverage))
    cat(sprintf("  Matched %d comuni with data\n", n_matched))
    cat(sprintf("  Unmatched comuni: %d\n", nrow(comuni_sf) - n_matched))

  } else {
    warning("Classifiers not available for Belfiore-ISTAT mapping. Direct join may fail.")
    comuni_sf <- merge(comuni_lom, comune_summary,
                      by.x = "PRO_COM_T", by.y = "COMUNE_SEDE_LAVORO",
                      all.x = TRUE)
  }

  # Ensure it's still an sf object
  comuni_sf <- sf::st_as_sf(comuni_sf)

  # Transform to desired CRS if different
  if (sf::st_crs(comuni_sf)$input != crs_output) {
    cat(sprintf("Transforming CRS to %s...\n", crs_output))
    comuni_sf <- sf::st_transform(comuni_sf, crs_output)
  }

  # Add year column
  comuni_sf$year <- year_filter

  # Reorder columns for better readability
  col_order <- c("year", "PRO_COM_T", "COMUNE", "DEN_UTS", "cpi",
                 "total_coverage", "n_workers", "n_segments", "n_specializations",
                 "dominant_segment", "dominant_segment_coverage", "dominant_segment_share",
                 "top_spec_segment", "top_spec_rca",
                 "mean_rca", "mean_coverage", "geometry")

  # Only include columns that exist
  col_order <- col_order[col_order %in% names(comuni_sf)]
  comuni_sf <- comuni_sf[, col_order]

  cat("\n")
  cat(rep("=", 70), "\n", sep = "")
  cat("SEGMENT SPECIALIZATION SF DATAFRAME CREATED\n")
  cat(rep("=", 70), "\n\n", sep = "")

  cat(sprintf("Rows: %d\n", nrow(comuni_sf)))
  cat(sprintf("Columns: %d\n", ncol(comuni_sf)))
  cat(sprintf("CRS: %s\n", sf::st_crs(comuni_sf)$input))
  cat(sprintf("Geometry type: %s\n", unique(sf::st_geometry_type(comuni_sf))))

  # Summary statistics
  cat("\nCoverage Statistics:\n")
  cat(sprintf("  Comuni with data: %d\n", sum(!is.na(comuni_sf$total_coverage))))
  cat(sprintf("  Total FTE: %.0f\n", sum(comuni_sf$total_coverage, na.rm = TRUE)))
  cat(sprintf("  Mean FTE per comune: %.0f\n", mean(comuni_sf$total_coverage, na.rm = TRUE)))

  cat("\nSegment Specialization Statistics:\n")
  cat(sprintf("  Comuni with specializations (RCA>1): %d\n",
              sum(comuni_sf$n_specializations > 0, na.rm = TRUE)))
  cat(sprintf("  Mean specializations per comune: %.1f\n",
              mean(comuni_sf$n_specializations, na.rm = TRUE)))
  max_spec <- max(comuni_sf$n_specializations, na.rm = TRUE)
  if (is.finite(max_spec)) {
    cat(sprintf("  Max specializations in a comune: %.0f\n", max_spec))
  } else {
    cat("  Max specializations in a comune: N/A\n")
  }

  return(comuni_sf)
}


# 5. Helper operator -----

# String concatenation operator (if not already defined)
if (!exists("%+%", mode = "function")) {
  `%+%` <- function(a, b) paste0(a, b)
}
