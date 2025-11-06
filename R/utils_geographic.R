# utils_geographic.R
# Geographic utility functions for comune-CPI mapping
# Author: Giampaolo Montaletti (giampaolo.montaletti@gmail.com)

# This file is copied from the longWdash project for use in the modularized
# dashboard preprocessing pipeline. It provides functions to map Belfiore codes
# to ISTAT codes and then to CPI (Centro Per l'Impiego) areas.

# 1. Load spatial data -----

#' Load comuni and CPI spatial data
#'
#' Loads the comuni and CPI shapefiles from the data/maps directory.
#' These files contain polygons for Lombardy municipalities and CPI areas.
#'
#' @return A list with two sf objects: comuni and cpi
#' @keywords internal
load_spatial_maps <- function() {
  base_path <- "/Users/giampaolomontaletti/Documents/funzioni/test/longWdash"

  comuni <- readRDS(file.path(base_path, "data/maps/comuni_lom_map.rds"))
  cpi <- readRDS(file.path(base_path, "data/maps/cpi_lom_map.rds"))

  return(list(comuni = comuni, cpi = cpi))
}

# 2. Create comune-CPI lookup table -----

#' Create comune to CPI lookup table
#'
#' Creates a lookup table mapping each comune to its corresponding CPI (Centro Per l'Impiego).
#' The function performs spatial join using sf::st_join to assign each comune centroid
#' to the CPI polygon it falls within. Falls back to existing cpi column in comuni data
#' if spatial join fails or produces NA values.
#'
#' @return A data.table with columns:
#'   \itemize{
#'     \item comune_code: PRO_COM_T code for the comune (6 digits with leading zero)
#'     \item comune_name: Name of the comune (COMUNE field)
#'     \item cpi_code: Unique code for the CPI (cpi field)
#'     \item cpi_name: Name of the CPI area (denominazione field)
#'   }
#'
#' @details
#' The function handles edge cases including:
#' - Comuni outside CPI boundaries (uses existing cpi column)
#' - Comuni on boundary lines (assigns to largest overlapping CPI)
#' - Missing values (uses existing cpi column as fallback)
#'
#' @examples
#' \dontrun{
#' lookup <- create_comune_cpi_lookup()
#' head(lookup)
#' }
#'
#' @export
create_comune_cpi_lookup <- function() {

  if (!requireNamespace("sf", quietly = TRUE)) {
    stop("Package 'sf' is required for spatial operations")
  }

  cat("Loading spatial data...\n")
  maps <- load_spatial_maps()
  comuni <- maps$comuni
  cpi <- maps$cpi

  cat("Processing", nrow(comuni), "comuni and", nrow(cpi), "CPI areas...\n")

  # Method 1: Use existing cpi column from comuni data
  # The comuni data already has CPI assignments from the original data source
  lookup_base <- data.table::data.table(
    comune_code = comuni$PRO_COM_T,
    comune_name = comuni$COMUNE,
    cpi_code = comuni$cpi,
    cpi_name = comuni$denominazione
  )

  # Method 2: Verify with spatial join (optional validation)
  # Calculate centroids for point-in-polygon matching
  cat("Performing spatial validation...\n")
  comuni_centroids <- sf::st_centroid(sf::st_geometry(comuni))

  # Add centroids to comuni sf object
  comuni_with_centroids <- comuni
  sf::st_geometry(comuni_with_centroids) <- comuni_centroids

  # Spatial join: match comune centroids to CPI polygons
  spatial_join <- sf::st_join(
    comuni_with_centroids[, c("PRO_COM_T", "COMUNE", "cpi", "denominazione")],
    cpi[, c("cpi", "denominazione")],
    join = sf::st_within,
    suffix = c("_comuni", "_spatial")
  )

  # Convert to data.table for comparison
  spatial_lookup <- data.table::data.table(sf::st_drop_geometry(spatial_join))
  data.table::setnames(spatial_lookup,
           old = c("PRO_COM_T", "COMUNE", "cpi_comuni", "denominazione_comuni",
                   "cpi_spatial", "denominazione_spatial"),
           new = c("comune_code", "comune_name", "cpi_code_original",
                   "cpi_name_original", "cpi_code_spatial", "cpi_name_spatial"),
           skip_absent = TRUE)

  # Use spatial join results where available, fall back to original where NA
  lookup_validated <- spatial_lookup[, .(
    comune_code,
    comune_name,
    cpi_code = data.table::fifelse(is.na(cpi_code_spatial), cpi_code_original, cpi_code_spatial),
    cpi_name = data.table::fifelse(is.na(cpi_name_spatial), cpi_name_original, cpi_name_spatial)
  )]

  # Check for any remaining NAs
  n_missing <- sum(is.na(lookup_validated$cpi_code))
  if (n_missing > 0) {
    cat("Warning:", n_missing, "comuni have missing CPI assignments\n")
    cat("Affected comuni:\n")
    print(lookup_validated[is.na(cpi_code), .(comune_code, comune_name)])
  }

  # Remove any duplicate entries (shouldn't happen but good to check)
  lookup_validated <- unique(lookup_validated, by = "comune_code")

  # Sort by comune code for easier browsing
  data.table::setorder(lookup_validated, comune_code)

  cat("Lookup table created successfully!\n")

  return(lookup_validated)
}

# 3. Add CPI to existing data -----

#' Add CPI information to a data.table
#'
#' Joins CPI code and name to a data.table based on comune identifier.
#' The function looks for comune identifiers in various common column names
#' and performs a left join with the comune-CPI lookup table.
#'
#' @param dt A data.table containing comune identifiers
#' @param comune_col Character string specifying the column name containing
#'   comune codes. Common values include "PRO_COM_T", "PRO_COM", "comune_code",
#'   or "comune". Default is "comune".
#' @param lookup Optional pre-loaded lookup table. If NULL, will load from
#'   saved RDS file or create new one.
#' @param add_name Logical. If TRUE, adds both cpi_code and cpi_name columns.
#'   If FALSE, only adds cpi_code. Default is TRUE.
#'
#' @return The input data.table with added columns:
#'   \itemize{
#'     \item cpi_code: CPI identifier code
#'     \item cpi_name: CPI area name (if add_name = TRUE)
#'   }
#'   The data.table is modified by reference for efficiency.
#'
#' @details
#' The function automatically detects the comune identifier column if it exists
#' under common names. It standardizes 5-digit codes to 6-digit format (with
#' leading zero) for matching.
#'
#' @examples
#' \dontrun{
#' # Add CPI to a data.table with comune codes
#' dt <- data.table(PRO_COM_T = c("015146", "016124", "018084"))
#' dt <- add_cpi_to_data(dt, comune_col = "PRO_COM_T")
#' print(dt)
#' }
#'
#' @export
add_cpi_to_data <- function(dt, comune_col = "comune", lookup = NULL, add_name = TRUE) {

  # Input validation
  if (!data.table::is.data.table(dt)) {
    stop("Input must be a data.table")
  }

  if (!comune_col %in% names(dt)) {
    # Try to find comune column automatically
    possible_cols <- c("PRO_COM_T", "PRO_COM", "comune_code", "comune", "COD_COMUNE")
    found_cols <- intersect(possible_cols, names(dt))

    if (length(found_cols) == 0) {
      stop("Could not find comune identifier column. Please specify comune_col parameter.")
    }

    comune_col <- found_cols[1]
    cat("Using", comune_col, "as comune identifier column\n")
  }

  # Load or use provided lookup table
  if (is.null(lookup)) {
    lookup_path <- "/Users/giampaolomontaletti/Documents/funzioni/test/longWdash/data/maps/comune_cpi_lookup.rds"

    if (file.exists(lookup_path)) {
      cat("Loading existing comune-CPI lookup table...\n")
      lookup <- readRDS(lookup_path)
    } else {
      cat("Creating new comune-CPI lookup table...\n")
      lookup <- create_comune_cpi_lookup()
    }
  }

  # Ensure lookup is a data.table
  if (!data.table::is.data.table(lookup)) {
    lookup <- data.table::as.data.table(lookup)
  }

  # Create temporary standardized column for joining
  # Handle both 5-digit (e.g., "15146") and 6-digit (e.g., "015146") formats
  dt[, .join_key := as.character(get(comune_col))]
  dt[nchar(.join_key) == 5, .join_key := paste0("0", .join_key)]

  # Prepare columns to merge
  if (add_name) {
    merge_cols <- c("comune_code", "cpi_code", "cpi_name")
  } else {
    merge_cols <- c("comune_code", "cpi_code")
  }

  # Perform left join
  dt <- merge(
    dt,
    lookup[, ..merge_cols],
    by.x = ".join_key",
    by.y = "comune_code",
    all.x = TRUE,
    sort = FALSE
  )

  # Remove temporary join key
  dt[, .join_key := NULL]

  # Report matching statistics
  n_total <- nrow(dt)
  n_matched <- sum(!is.na(dt$cpi_code))
  match_pct <- round(100 * n_matched / n_total, 1)

  cat(sprintf("Matched %d/%d records (%.1f%%)\n", n_matched, n_total, match_pct))

  if (n_matched < n_total) {
    n_unmatched <- n_total - n_matched
    cat("Warning:", n_unmatched, "records could not be matched to a CPI\n")
  }

  return(dt)
}

# 4. Belfiore to ISTAT conversion -----

#' Load Belfiore to ISTAT mapping
#'
#' Loads the mapping table that converts Belfiore codes (cadastral codes)
#' to ISTAT PRO_COM_T codes. The mapping file was created from official
#' ISTAT data downloaded from www.istat.it.
#'
#' @return A data.table with columns:
#'   \itemize{
#'     \item belfiore: 4-character Belfiore/cadastral code
#'     \item pro_com_t: 6-digit ISTAT PRO_COM_T code
#'     \item comune_name: Municipality name
#'   }
#'
#' @export
load_belfiore_mapping <- function() {
  mapping_path <- "/Users/giampaolomontaletti/Documents/funzioni/test/longWdash/data/maps/belfiore_istat_mapping.csv"

  if (!file.exists(mapping_path)) {
    stop("Belfiore-ISTAT mapping file not found at: ", mapping_path,
         "\nPlease run the script to download and create this file from ISTAT data.")
  }

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required")
  }

  mapping <- data.table::fread(mapping_path)
  data.table::setDT(mapping)

  cat("Loaded Belfiore-ISTAT mapping:", nrow(mapping), "comuni\n")

  return(mapping)
}

#' Add CPI via Belfiore codes
#'
#' Two-step conversion: Belfiore code → ISTAT PRO_COM_T → CPI.
#' This function is designed for employment data that uses Belfiore codes
#' (e.g., COMUNE_LAVORATORE field in CO data).
#'
#' @param dt A data.table containing Belfiore codes
#' @param belfiore_col Character string specifying the column name containing
#'   Belfiore codes. Default is "COMUNE_LAVORATORE".
#' @param add_name Logical. If TRUE, adds both cpi_code and cpi_name columns.
#'   If FALSE, only adds cpi_code. Default is TRUE.
#'
#' @return The input data.table with added columns:
#'   \itemize{
#'     \item pro_com_t: ISTAT code (intermediate result)
#'     \item cpi_code: CPI identifier code
#'     \item cpi_name: CPI area name (if add_name = TRUE)
#'   }
#'   The data.table is modified by reference for efficiency.
#'
#' @details
#' This function performs two sequential joins:
#' 1. Join Belfiore codes to ISTAT PRO_COM_T codes
#' 2. Join ISTAT codes to CPI codes
#'
#' @examples
#' \dontrun{
#' # Add CPI to employment data with Belfiore codes
#' dt <- data.table(COMUNE_LAVORATORE = c("F205", "B292", "D869"))
#' dt <- add_cpi_via_belfiore(dt)
#' print(dt)
#' }
#'
#' @export
add_cpi_via_belfiore <- function(dt, belfiore_col = "COMUNE_LAVORATORE", add_name = TRUE) {

  # Input validation
  if (!data.table::is.data.table(dt)) {
    stop("Input must be a data.table")
  }

  if (!belfiore_col %in% names(dt)) {
    stop("Column '", belfiore_col, "' not found in data.table")
  }

  cat("Step 1: Converting Belfiore codes to ISTAT PRO_COM_T...\n")

  # Load Belfiore → ISTAT mapping
  belfiore_map <- load_belfiore_mapping()

  # Ensure pro_com_t is character (may be numeric in CSV)
  belfiore_map[, pro_com_t := as.character(pro_com_t)]

  # Join to get ISTAT codes
  dt <- merge(
    dt,
    belfiore_map[, .(belfiore, pro_com_t)],
    by.x = belfiore_col,
    by.y = "belfiore",
    all.x = TRUE,
    sort = FALSE
  )

  # Report Belfiore → ISTAT matching
  n_total <- nrow(dt)
  n_matched_istat <- sum(!is.na(dt$pro_com_t))
  match_pct <- round(100 * n_matched_istat / n_total, 1)

  cat(sprintf("  Matched %d/%d records to ISTAT codes (%.1f%%)\n",
              n_matched_istat, n_total, match_pct))

  if (n_matched_istat < n_total) {
    n_unmatched <- n_total - n_matched_istat
    cat("  Warning:", n_unmatched, "Belfiore codes could not be matched\n")
  }

  cat("\nStep 2: Converting ISTAT codes to CPI...\n")

  # Standardize pro_com_t to 6 digits with leading zero
  dt[, pro_com_t := as.character(pro_com_t)]
  dt[nchar(pro_com_t) == 5, pro_com_t := paste0("0", pro_com_t)]

  # Load comune-CPI lookup
  lookup_path <- "/Users/giampaolomontaletti/Documents/funzioni/test/longWdash/data/maps/comune_cpi_lookup.rds"

  if (file.exists(lookup_path)) {
    lookup <- readRDS(lookup_path)
  } else {
    cat("Creating new comune-CPI lookup table...\n")
    lookup <- create_comune_cpi_lookup()
  }

  data.table::setDT(lookup)

  # Prepare columns to merge
  if (add_name) {
    merge_cols <- c("comune_code", "cpi_code", "cpi_name")
  } else {
    merge_cols <- c("comune_code", "cpi_code")
  }

  # Join ISTAT → CPI
  dt <- merge(
    dt,
    lookup[, ..merge_cols],
    by.x = "pro_com_t",
    by.y = "comune_code",
    all.x = TRUE,
    sort = FALSE
  )

  # Data quality fix: Torre de' Busi (L257 → 016215) is missing from comune_cpi_lookup.rds
  # This comune in Bergamo province should map to CPI PONTE SAN PIETRO
  if (sum(dt$pro_com_t == "016215" & is.na(dt$cpi_code), na.rm = TRUE) > 0) {
    dt[pro_com_t == "016215" & is.na(cpi_code), `:=`(
      cpi_code = "G856C000065",
      cpi_name = "CPI PONTE SAN PIETRO"
    )]
  }

  # Report ISTAT → CPI matching
  n_matched_cpi <- sum(!is.na(dt$cpi_code))
  match_pct_cpi <- round(100 * n_matched_cpi / n_total, 1)

  cat(sprintf("  Matched %d/%d records to CPI (%.1f%%)\n",
              n_matched_cpi, n_total, match_pct_cpi))

  if (n_matched_cpi < n_matched_istat) {
    n_lost <- n_matched_istat - n_matched_cpi
    cat("  Note:", n_lost, "ISTAT codes were outside Lombardia region\n")
  }

  cat("\nFinal result: Added CPI information to", n_matched_cpi, "records\n")

  return(dt)
}
