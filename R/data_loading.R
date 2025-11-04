# data_loading.R
# Data loading and preparation functions for dashboard preprocessing
# Author: Giampaolo Montaletti (giampaolo.montaletti@gmail.com)

# 1. Load and consolidate data -----

#' Load and consolidate employment data
#'
#' Loads employment data from an .fst file and applies consolidation of short gaps
#' to create clean employment spells.
#'
#' @param input_path Character string specifying path to input .fst file
#' @param variable_handling Character string specifying how to handle variables during
#'   consolidation. Options are "first", "last", "mode", "mean". Default is "first".
#'
#' @return A data.table with consolidated employment spells
#'
#' @details
#' This function loads raw employment data and applies the consolidate_short_gaps()
#' function from longworkR to merge employment spells separated by short unemployment
#' periods. The consolidation step is essential for accurate transition analysis.
#'
#' @examples
#' \dontrun{
#' dt <- load_and_consolidate_data("../../vecshift/indice.fst")
#' }
#'
#' @export
load_and_consolidate_data <- function(input_path, variable_handling = "first") {

  # Input validation
  if (!file.exists(input_path)) {
    stop("Input file not found: ", input_path)
  }

  if (!requireNamespace("fst", quietly = TRUE)) {
    stop("Package 'fst' is required but not installed")
  }

  cat("Loading raw employment data from:", input_path, "\n")
  dt <- fst::read.fst(input_path, as.data.table = TRUE)

  n_spells_before <- nrow(dt)
  n_persons <- dt[, data.table::uniqueN(cf)]

  cat("  Loaded", n_spells_before, "spells for", n_persons, "individuals\n")

  # Apply consolidation
  cat("  Applying consolidation of short gaps...\n")
  dt <- longworkR::consolidate_short_gaps(
    dt,
    variable_handling = variable_handling
  )

  n_spells_after <- nrow(dt)
  n_consolidated <- n_spells_before - n_spells_after
  pct_reduction <- round(100 * n_consolidated / n_spells_before, 1)

  cat("  Consolidated", n_consolidated, "spells",
      sprintf("(%.1f%% reduction)\n", pct_reduction))
  cat("  Final spell count:", n_spells_after, "\n")

  return(dt)
}


# 2. Standardize ATECO codes -----

#' Standardize ATECO codes to XX.Y format
#'
#' Converts ATECO economic sector codes to a standardized 3-digit format (XX.Y)
#' where XX is a two-digit sector code and Y is a single digit sub-sector.
#'
#' @param ateco_code Character vector of ATECO codes to standardize
#'
#' @return Character vector of standardized ATECO codes in XX.Y format
#'
#' @details
#' This function handles various input formats:
#' \itemize{
#'   \item X.X → 0X.X (adds leading zero)
#'   \item XX → XX.0 (adds decimal and zero)
#'   \item XX. → XX.0 (adds trailing zero)
#'   \item XX.X → XX.X (already standard)
#' }
#'
#' The standardization ensures consistent sector labeling and aggregation
#' across the entire dataset.
#'
#' @examples
#' standardize_ateco_3digit(c("1.5", "47", "62.", "10.2"))
#' # Returns: c("01.5", "47.0", "62.0", "10.2")
#'
#' @export
standardize_ateco_3digit <- function(ateco_code) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required but not installed")
  }

  # Handle NA values
  if (all(is.na(ateco_code))) {
    return(ateco_code)
  }

  # Extract first 4 characters or less
  code <- substring(ateco_code, 1, 4)

  # Vectorized logic using fcase() from data.table
  code_len <- nchar(code)
  char3 <- substring(code, 3, 3)

  # Pattern detection (vectorized)
  is_single_digit_format <- code_len == 3 & grepl("^\\d\\.\\d$", code)
  is_two_digit_no_dot <- code_len == 2
  is_two_digit_with_dot <- code_len == 3 & char3 == "."
  is_standard_format <- code_len == 4 & char3 == "."

  # Apply transformations using fcase
  result <- data.table::fcase(
    is_single_digit_format, paste0("0", code),      # Case 1: X.X -> 0X.X
    is_two_digit_no_dot, paste0(code, ".0"),        # Case 2: XX -> XX.0
    is_two_digit_with_dot, paste0(code, "0"),       # Case 3: XX. -> XX.0
    is_standard_format, code,                       # Case 4: XX.X -> XX.X
    default = substring(code, 1, 4)                 # Default: first 4 chars
  )

  return(result)
}


# 3. Add geographic and sector information -----

#' Add CPI and standardized ATECO information to employment data
#'
#' Enriches employment data by adding CPI (Centro Per l'Impiego) geographic
#' information and standardizing ATECO sector codes to 3-digit format.
#'
#' @param dt A data.table containing employment spells
#' @param belfiore_col Character string specifying the column name containing
#'   Belfiore comune codes. Default is "COMUNE_LAVORATORE".
#' @param ateco_col Character string specifying the column name containing
#'   ATECO codes. Default is "ateco".
#' @param add_cpi_name Logical. If TRUE, adds both cpi_code and cpi_name columns.
#'   Default is TRUE.
#'
#' @return The input data.table with added columns:
#'   \itemize{
#'     \item pro_com_t: ISTAT municipality code
#'     \item cpi_code: CPI area code
#'     \item cpi_name: CPI area name (if add_cpi_name = TRUE)
#'     \item ateco_3digit: Standardized 3-digit ATECO code (XX.Y format)
#'   }
#'
#' @details
#' This function performs two enrichment operations:
#' 1. Geographic enrichment: Maps Belfiore codes → ISTAT codes → CPI areas
#' 2. Sector standardization: Converts ATECO codes to consistent XX.Y format
#'
#' The CPI mapping uses the add_cpi_via_belfiore() function which requires
#' pre-computed lookup tables for Lombardy region.
#'
#' @examples
#' \dontrun{
#' dt <- add_geo_and_ateco(dt)
#' # Now dt has cpi_code, cpi_name, and ateco_3digit columns
#' }
#'
#' @export
add_geo_and_ateco <- function(dt,
                              belfiore_col = "COMUNE_LAVORATORE",
                              ateco_col = "ateco",
                              add_cpi_name = TRUE) {

  # Input validation
  if (!data.table::is.data.table(dt)) {
    stop("Input must be a data.table")
  }

  if (!belfiore_col %in% names(dt)) {
    warning("Belfiore column '", belfiore_col, "' not found in data. ",
            "Skipping CPI enrichment.")
  } else {
    # Add CPI information via Belfiore codes
    cat("Adding CPI information via Belfiore codes...\n")

    # Source geographic utilities directly
    utils_geo_path <- file.path(dirname(getwd()), "longworkR/R/utils_geographic.R")
    if (file.exists(utils_geo_path)) {
      source(utils_geo_path)
      dt <- add_cpi_via_belfiore(dt, belfiore_col = belfiore_col, add_name = add_cpi_name)
    } else {
      warning("Geographic utilities not found. Skipping CPI enrichment.")
    }
  }

  # Standardize ATECO codes
  if (!ateco_col %in% names(dt)) {
    warning("ATECO column '", ateco_col, "' not found in data. ",
            "Skipping ATECO standardization.")
  } else {
    cat("Standardizing ATECO codes to XX.Y format...\n")
    dt[, ateco_3digit := standardize_ateco_3digit(get(ateco_col))]
    cat("  Created ateco_3digit column\n")
  }

  return(dt)
}
