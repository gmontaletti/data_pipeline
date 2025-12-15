# data_loading.R
# Data loading and preparation functions for dashboard preprocessing
# Author: Giampaolo Montaletti (giampaolo.montaletti@gmail.com)

# 1. Combined consolidation and enrichment -----

#' Consolidate and enrich prepared employment data
#'
#' Applies vecshift consolidation (for overlaps), adds unemployment periods,
#' matches external events (DID/POL), then applies longworkR consolidation
#' (for short gaps), and finally adds geographic/ATECO enrichment.
#'
#' @param prepared_data Prepared contract data from prepare_contracts()
#' @param did_data DID data from load_raw_did()
#' @param pol_data POL data from load_raw_pol()
#' @param min_date Minimum date for unemployment periods (default: "2021-01-01")
#' @param max_date Maximum date for unemployment periods (default: "2024-12-31")
#' @param variable_handling How to handle variables during longworkR consolidation (default: "first")
#'
#' @return A data.table with fully consolidated and enriched employment history
#'
#' @details
#' Processing steps:
#' 1. Apply vecshift consolidation (handle overlapping spells)
#' 2. Add unemployment periods between employment spells
#' 3. Match DID/POL external events to unemployment spells
#' 4. Apply longworkR consolidation (merge short gaps)
#' 5. Add geographic (CPI) and ATECO standardization
#'
#' @export
consolidate_and_enrich <- function(prepared_data,
                                   did_data,
                                   pol_data,
                                   min_date = as.IDate("2021-01-01"),
                                   max_date = as.IDate("2024-12-31"),
                                   variable_handling = "first") {

  cat("\n")
  cat("=" %+% rep("=", 70) %+% "\n")
  cat("CONSOLIDATE AND ENRICH PIPELINE\n")
  cat("=" %+% rep("=", 70) %+% "\n\n")

  # CRITICAL FIX: Ensure inputs are data.table after FST deserialization
  data.table::setDT(prepared_data)
  data.table::setDT(did_data)
  data.table::setDT(pol_data)

  # CRITICAL FIX: Ensure date columns are IDate after deserialization
  # QS format may not preserve IDate type correctly when loading targets
  # This prevents "storage mode of IDate is somehow no longer integer" errors
  prepared_data[, inizio := as.IDate(inizio)]
  prepared_data[, fine := as.IDate(fine)]

  # MEMORY OPTIMIZATION: Filter contracts by date BEFORE consolidation
  # Keep only contracts that overlap with the analysis period (fine >= min_date)
  # This reduces memory usage significantly for vecshift consolidation
  n_before_filter <- nrow(prepared_data)
  prepared_data <- prepared_data[fine >= min_date]
  n_after_filter <- nrow(prepared_data)
  cat(sprintf("Date filter (fine >= %s): %s → %s rows (%.1f%% reduction)\n\n",
              min_date,
              format(n_before_filter, big.mark = ","),
              format(n_after_filter, big.mark = ","),
              100 * (n_before_filter - n_after_filter) / n_before_filter))

  # Step 1: Vecshift consolidation (overlaps)
  cat("STEP 1: Vecshift consolidation (overlapping spells)\n")
  cat("-" %+% rep("-", 70) %+% "\n")
  dt <- apply_vecshift_consolidation(prepared_data)

  # Step 2: Add unemployment periods
  cat("\nSTEP 2: Add unemployment periods\n")
  cat("-" %+% rep("-", 70) %+% "\n")
  dt <- pipeline_add_unemployment(dt, min_date = min_date, max_date = max_date)

  # Step 3: Merge original attributes back
  cat("\nSTEP 3: Merge original attributes\n")
  cat("-" %+% rep("-", 70) %+% "\n")
  dt <- pipeline_merge_attributes(dt, prepared_data)

  # Step 4: Match external events (DID/POL)
  cat("\nSTEP 4: Match external events (DID/POL)\n")
  cat("-" %+% rep("-", 70) %+% "\n")
  cat("  Matching DID and POL events with unemployment spells...\n")

  # CRITICAL FIX: Ensure DID/POL date columns are IDate after deserialization
  did_data[, DATA_EVENTO := as.IDate(DATA_EVENTO)]
  pol_data[, DATA_INIZIO := as.IDate(DATA_INIZIO)]
  pol_data[, DATA_FINE := as.IDate(DATA_FINE)]

  dt <- pipeline_match_events(dt, did_data, pol_data, min_date = min_date, max_date = max_date)

  # Step 5: LongworkR consolidation (short gaps)
  cat("\nSTEP 5: LongworkR consolidation (short gaps)\n")
  cat("-" %+% rep("-", 70) %+% "\n")
  cat("  Applying consolidate_short_gaps()...\n")
  n_before <- nrow(dt)
  dt <- longworkR::consolidate_short_gaps(dt, variable_handling = variable_handling)
  n_after <- nrow(dt)
  n_consolidated <- n_before - n_after
  pct_reduction <- 100 * n_consolidated / n_before
  cat(sprintf("  ✓ Consolidated %s spells (%.1f%% reduction)\n",
              format(n_consolidated, big.mark = ","), pct_reduction))

  # Step 6: Add geographic and ATECO enrichment
  cat("\nSTEP 6: Geographic and ATECO enrichment\n")
  cat("-" %+% rep("-", 70) %+% "\n")
  dt <- add_geo_and_ateco(dt)

  cat("\n")
  cat("=" %+% rep("=", 70) %+% "\n")
  cat(sprintf("✓ PIPELINE COMPLETE: %s final spells for %s individuals\n",
              format(nrow(dt), big.mark = ","),
              format(dt[, data.table::uniqueN(cf)], big.mark = ",")))
  cat("=" %+% rep("=", 70) %+% "\n\n")

  return(dt)
}


# 1.0.1 Memory-optimized pipeline functions (checkpointable) -----

#' Apply vecshift consolidation only (Step 1 of memory-optimized pipeline)
#'
#' Standalone function for checkpointable pipeline. Applies vecshift
#' consolidation to handle overlapping employment periods.
#'
#' @param prepared_data Prepared contract data from prepare_contracts()
#' @param min_date Minimum date for filtering (contracts with fine >= min_date)
#' @return data.table with consolidated (non-overlapping) employment spells
#' @export
apply_vecshift_only <- function(prepared_data, min_date = as.IDate("2021-01-01")) {
  cat("\n")
  cat("=" %+% rep("=", 70) %+% "\n")
  cat("STEP 1: VECSHIFT CONSOLIDATION (CHECKPOINTED)\n")
  cat("=" %+% rep("=", 70) %+% "\n\n")

  # Ensure data.table after FST deserialization

  data.table::setDT(prepared_data)
  prepared_data[, inizio := as.IDate(inizio)]
  prepared_data[, fine := as.IDate(fine)]

  # Filter by date before consolidation (memory optimization)
  n_before_filter <- nrow(prepared_data)
  prepared_data <- prepared_data[fine >= min_date]
  n_after_filter <- nrow(prepared_data)
  cat(sprintf("Date filter (fine >= %s): %s → %s rows (%.1f%% reduction)\n",
              min_date,
              format(n_before_filter, big.mark = ","),
              format(n_after_filter, big.mark = ","),
              100 * (n_before_filter - n_after_filter) / n_before_filter))

  # Apply vecshift consolidation
  dt <- apply_vecshift_consolidation(prepared_data)

  cat("✓ Step 1 complete - checkpoint saved as FST\n\n")
  return(dt)
}


#' Add unemployment periods only (Step 2 of memory-optimized pipeline)
#'
#' Standalone function for checkpointable pipeline. Adds unemployment
#' periods between employment spells.
#'
#' @param vecshift_data Data from apply_vecshift_only()
#' @param min_date Minimum date for unemployment periods
#' @param max_date Maximum date for unemployment periods
#' @return data.table with employment and unemployment spells
#' @export
add_unemployment_only <- function(vecshift_data,
                                   min_date = as.IDate("2021-01-01"),
                                   max_date = as.IDate("2024-12-31")) {
  cat("\n")
  cat("=" %+% rep("=", 70) %+% "\n")
  cat("STEP 2: ADD UNEMPLOYMENT PERIODS (CHECKPOINTED)\n")
  cat("=" %+% rep("=", 70) %+% "\n\n")

  # Ensure data.table after FST deserialization
  data.table::setDT(vecshift_data)
  vecshift_data[, inizio := as.IDate(inizio)]
  vecshift_data[, fine := as.IDate(fine)]

  # Add unemployment periods
  dt <- pipeline_add_unemployment(vecshift_data, min_date = min_date, max_date = max_date)

  cat("✓ Step 2 complete - checkpoint saved as FST\n\n")
  return(dt)
}


#' Merge original attributes only (Step 3 of memory-optimized pipeline)
#'
#' Standalone function for checkpointable pipeline. Merges original
#' contract attributes back after consolidation.
#'
#' @param data_with_unemployment Data from add_unemployment_only()
#' @param prepared_data Original prepared data with all columns
#' @return data.table with full attributes
#' @export
merge_attributes_only <- function(data_with_unemployment, prepared_data) {
  cat("\n")
  cat("=" %+% rep("=", 70) %+% "\n")
  cat("STEP 3: MERGE ORIGINAL ATTRIBUTES (CHECKPOINTED)\n")
  cat("=" %+% rep("=", 70) %+% "\n\n")

  # Ensure data.table after FST deserialization
  data.table::setDT(data_with_unemployment)
  data.table::setDT(prepared_data)

  data_with_unemployment[, inizio := as.IDate(inizio)]
  data_with_unemployment[, fine := as.IDate(fine)]
  prepared_data[, inizio := as.IDate(inizio)]
  prepared_data[, fine := as.IDate(fine)]

  # Merge attributes
  dt <- pipeline_merge_attributes(data_with_unemployment, prepared_data)

  cat("✓ Step 3 complete - checkpoint saved as FST\n\n")
  return(dt)
}


#' Match DID/POL events with memory-safe mode (Step 4 of memory-optimized pipeline)
#'
#' Memory-optimized version of pipeline_match_events that enables vecshift's
#' memory_safe mode and uses smaller batch sizes.
#'
#' @param data_with_attributes Data from merge_attributes_only()
#' @param did_data DID data from load_raw_did()
#' @param pol_data POL data from load_raw_pol()
#' @param min_date Minimum date for filtering DID/POL events
#' @param max_date Maximum date for filtering DID/POL events
#' @return data.table with DID/POL flags on unemployment spells
#' @export
match_did_pol_memory_safe <- function(data_with_attributes,
                                       did_data,
                                       pol_data,
                                       min_date = as.IDate("2021-01-01"),
                                       max_date = as.IDate("2024-12-31")) {
  cat("\n")
  cat("=" %+% rep("=", 70) %+% "\n")
  cat("STEP 4: MATCH DID/POL EVENTS - MEMORY SAFE (CHECKPOINTED)\n")
  cat("=" %+% rep("=", 70) %+% "\n\n")

  # Ensure data.table after FST deserialization
  data.table::setDT(data_with_attributes)
  data.table::setDT(did_data)
  data.table::setDT(pol_data)

  data_with_attributes[, inizio := as.IDate(inizio)]
  data_with_attributes[, fine := as.IDate(fine)]
  did_data[, DATA_EVENTO := as.IDate(DATA_EVENTO)]
  pol_data[, DATA_INIZIO := as.IDate(DATA_INIZIO)]
  pol_data[, DATA_FINE := as.IDate(DATA_FINE)]

  # Call memory-safe version of event matching
  dt <- pipeline_match_events_memory_safe(
    data_with_attributes,
    did_data,
    pol_data,
    min_date = min_date,
    max_date = max_date
  )

  cat("✓ Step 4 complete - checkpoint saved as FST\n\n")
  return(dt)
}


#' Final consolidation and enrichment (Step 5 of memory-optimized pipeline)
#'
#' Applies longworkR consolidation (short gaps) and geographic/ATECO enrichment.
#' This is the final step that produces the data_consolidated output.
#'
#' @param data_with_did_pol Data from match_did_pol_memory_safe()
#' @param variable_handling How to handle variables during longworkR consolidation
#' @return data.table with final consolidated and enriched employment history
#' @export
consolidate_and_enrich_final <- function(data_with_did_pol,
                                          variable_handling = "first") {
  cat("\n")
  cat("=" %+% rep("=", 70) %+% "\n")
  cat("STEP 5: FINAL CONSOLIDATION + ENRICHMENT (CHECKPOINTED)\n")
  cat("=" %+% rep("=", 70) %+% "\n\n")

  # Ensure data.table after FST deserialization
  data.table::setDT(data_with_did_pol)
  data_with_did_pol[, inizio := as.IDate(inizio)]
  data_with_did_pol[, fine := as.IDate(fine)]

  dt <- data_with_did_pol

  # Step 5a: LongworkR consolidation (short gaps)
  cat("Applying LongworkR consolidation (short gaps)...\n")
  n_before <- nrow(dt)
  dt <- longworkR::consolidate_short_gaps(dt, variable_handling = variable_handling)
  n_after <- nrow(dt)
  n_consolidated <- n_before - n_after
  pct_reduction <- 100 * n_consolidated / n_before
  cat(sprintf("  ✓ Consolidated %s spells (%.1f%% reduction)\n",
              format(n_consolidated, big.mark = ","), pct_reduction))

  # Step 5b: Add geographic and ATECO enrichment
  cat("\nAdding geographic and ATECO enrichment...\n")
  dt <- add_geo_and_ateco(dt)

  cat("\n")
  cat("=" %+% rep("=", 70) %+% "\n")
  cat(sprintf("✓ PIPELINE COMPLETE: %s final spells for %s individuals\n",
              format(nrow(dt), big.mark = ","),
              format(dt[, data.table::uniqueN(cf)], big.mark = ",")))
  cat("=" %+% rep("=", 70) %+% "\n\n")

  return(dt)
}


# 1.1. Consolidate employment (before filtering) -----

#' Consolidate employment spells (without unemployment/DID/POL)
#'
#' Performs employment spell consolidation that should happen BEFORE geographic filtering:
#' 1. Vecshift consolidation (overlaps) - creates minimal skeleton with id column
#' 2. LongworkR consolidation (short gaps) - operates on skeleton only
#'
#' This function does NOT merge attributes, add unemployment, or match DID/POL.
#' Those steps happen AFTER filtering via enrich_with_policies().
#' Attributes are preserved in the original prepared_data for later merging.
#'
#' @export
consolidate_employment <- function(prepared_data,
                                   variable_handling = "first") {

  cat("\n")
  cat("=" %+% rep("=", 70) %+% "\n")
  cat("CONSOLIDATE EMPLOYMENT (BEFORE FILTERING)\n")
  cat("=" %+% rep("=", 70) %+% "\n\n")

  # CRITICAL FIX: Ensure inputs are data.table after FST deserialization
  data.table::setDT(prepared_data)

  # CRITICAL FIX: Ensure date columns are IDate after deserialization
  prepared_data[, inizio := as.IDate(inizio)]
  prepared_data[, fine := as.IDate(fine)]

  # Step 1: Vecshift consolidation (overlaps) - creates skeleton with id column
  cat("STEP 1: Vecshift consolidation (overlapping spells)\n")
  cat("-" %+% rep("-", 70) %+% "\n")
  dt <- apply_vecshift_consolidation(prepared_data)

  # Step 2: LongworkR consolidation (short gaps) - on skeleton only
  cat("\nSTEP 2: LongworkR consolidation (short gaps)\n")
  cat("-" %+% rep("-", 70) %+% "\n")
  cat("  Applying consolidate_short_gaps()...\n")
  n_before <- nrow(dt)
  dt <- longworkR::consolidate_short_gaps(dt, variable_handling = variable_handling)
  n_after <- nrow(dt)
  n_consolidated <- n_before - n_after
  pct_reduction <- 100 * n_consolidated / n_before
  cat(sprintf("  ✓ Consolidated %s spells (%.1f%% reduction)\n",
              format(n_consolidated, big.mark = ","), pct_reduction))

  cat("\n")
  cat("=" %+% rep("=", 70) %+% "\n")
  cat(sprintf("✓ CONSOLIDATION COMPLETE: %s spells for %s individuals\n",
              format(nrow(dt), big.mark = ","),
              format(dt[, data.table::uniqueN(cf)], big.mark = ",")))
  cat("  Note: Attributes will be merged after filtering + unemployment\n")
  cat("=" %+% rep("=", 70) %+% "\n\n")

  return(dt)
}


# 1.2. Enrich with policies (after filtering) -----

#' Enrich filtered data with unemployment periods and DID/POL events
#'
#' Performs enrichment steps that should happen AFTER geographic filtering:
#' 1. Add unemployment periods between employment spells
#' 2. Merge original attributes back (employment gets attributes from prepared_data)
#' 3. Match DID/POL external events to unemployment spells
#' 4. Add geographic (CPI) and ATECO standardization
#'
#' This function operates on already-filtered employment data, ensuring that
#' unemployment spells are only created for the filtered population.
#'
#' @export
enrich_with_policies <- function(filtered_data,
                                  prepared_data,
                                  did_data,
                                  pol_data,
                                  min_date = as.IDate("2021-01-01"),
                                  max_date = as.IDate("2024-12-31")) {

  cat("\n")
  cat("=" %+% rep("=", 70) %+% "\n")
  cat("ENRICH WITH POLICIES (AFTER FILTERING)\n")
  cat("=" %+% rep("=", 70) %+% "\n\n")

  # CRITICAL FIX: Ensure inputs are data.table after deserialization
  data.table::setDT(filtered_data)
  data.table::setDT(prepared_data)
  data.table::setDT(did_data)
  data.table::setDT(pol_data)

  # CRITICAL FIX: Ensure date columns are IDate
  filtered_data[, inizio := as.IDate(inizio)]
  filtered_data[, fine := as.IDate(fine)]

  # Step 1: Add unemployment periods
  cat("STEP 1: Add unemployment periods\n")
  cat("-" %+% rep("-", 70) %+% "\n")
  dt <- pipeline_add_unemployment(filtered_data, min_date = min_date, max_date = max_date)

  # Step 2: Merge original attributes back
  cat("\nSTEP 2: Merge original attributes\n")
  cat("-" %+% rep("-", 70) %+% "\n")
  dt <- pipeline_merge_attributes(dt, prepared_data)

  # Step 3: Match external events (DID/POL)
  cat("\nSTEP 3: Match external events (DID/POL)\n")
  cat("-" %+% rep("-", 70) %+% "\n")
  cat("  Matching DID and POL events with unemployment spells...\n")

  # CRITICAL FIX: Ensure DID/POL date columns are IDate after deserialization
  did_data[, DATA_EVENTO := as.IDate(DATA_EVENTO)]
  pol_data[, DATA_INIZIO := as.IDate(DATA_INIZIO)]
  pol_data[, DATA_FINE := as.IDate(DATA_FINE)]

  dt <- pipeline_match_events(dt, did_data, pol_data, min_date = min_date, max_date = max_date)

  # Step 4: Add geographic and ATECO enrichment
  cat("\nSTEP 4: Geographic and ATECO enrichment\n")
  cat("-" %+% rep("-", 70) %+% "\n")
  dt <- add_geo_and_ateco(dt)

  # Step 5: Recode obsolete CPI codes
  cat("\nSTEP 5: Recode obsolete CPI codes\n")
  cat("-" %+% rep("-", 70) %+% "\n")
  if ("cpi_code" %in% names(dt)) {
    dt[cpi_code == "C816C530490", cpi_code := "C816C000692"]
    dt[cpi_code == "C816C000692",
       cpi_name := "CPI DELLA PROVINCIA DI LODI - UFFICIO PERIFERICO CODOGNO"]
    cat("  Recoded obsolete Codogno CPI code\n")
  } else {
    cat("  No cpi_code column found, skipping\n")
  }

  cat("\n")
  cat("=" %+% rep("=", 70) %+% "\n")
  cat(sprintf("✓ ENRICHMENT COMPLETE: %s final spells for %s individuals\n",
              format(nrow(dt), big.mark = ","),
              format(dt[, data.table::uniqueN(cf)], big.mark = ",")))
  cat("=" %+% rep("=", 70) %+% "\n\n")

  return(dt)
}


# 2. Load and consolidate data (legacy) -----

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


# 3. String concatenation helper -----

# String concatenation operator for cleaner output formatting
`%+%` <- function(a, b) paste0(a, b)


# 4. Standardize ATECO codes -----

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


# 5. Add geographic and sector information -----

#' Add CPI and standardized ATECO information to employment data
#'
#' Enriches employment data by adding CPI (Centro Per l'Impiego) geographic
#' information and standardizing ATECO sector codes to 3-digit format.
#'
#' @param dt A data.table containing employment spells
#' @param belfiore_col Character string specifying the column name containing
#'   Belfiore comune codes. Default is "COMUNE_LAVORATORE" (comune di domicilio
#'   del lavoratore, NON il comune dove lavora).
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
