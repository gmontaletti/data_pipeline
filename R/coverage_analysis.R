# coverage_analysis.R
# Functions for estimating contractual annual coverage
# Author: Giampaolo Montaletti (giampaolo.montaletti@gmail.com)

# 1. Weekly effort calculation -----

#' Calculate weekly effort for employment contracts
#'
#' Computes the weekly effort (FTE equivalent) for each contract period.
#'
#' Rules:
#' - Full-time (prior == 1): effort = 1 if ore is NA or 40, else ore/40
#' - Part-time (prior == 0): effort = ore/40, or 20/40 if ore is NA
#' - Intermittent (prior == 2): effort = ore/40, or 20/40 if ore is NA
#' - For overlapping contracts: sum of efforts capped at 48/40
#'
#' @param dt data.table with columns: cf, inizio, fine, prior, ore
#' @return data.table with added weekly_effort column
#' @export
calculate_weekly_effort <- function(dt) {

  cat("Calculating weekly effort...\n")

  # Ensure we have a copy to avoid modifying original
  dt <- data.table::copy(dt)
  data.table::setDT(dt)

  # Calculate individual contract effort
  dt[, contract_effort := data.table::fcase(
    # Full-time: 1 if ore is NA or 40, else actual hours/40
    prior == 1 & (is.na(ore) | ore == 40), 1.0,
    prior == 1, ore / 40,

    # Part-time or intermittent: ore/40, or 20/40 if NA
    prior %in% c(0, 2) & is.na(ore), 20 / 40,
    prior %in% c(0, 2), ore / 40,

    # Default: assume 20/40 if prior is somehow missing
    default = 20 / 40
  )]

  # Check for overlapping contracts (same person, overlapping dates)
  # Sort by person and start date
  data.table::setorder(dt, cf, inizio, fine)

  # Identify overlapping periods for each person
  # Two contracts overlap if: start_2 <= end_1
  dt[, prev_fine := data.table::shift(fine, n = 1, type = "lag"), by = cf]
  dt[, has_overlap := !is.na(prev_fine) & inizio <= prev_fine]

  # For overlapping contracts, we need to calculate combined effort
  # Mark overlap groups
  dt[, overlap_group := cumsum(!has_overlap | is.na(has_overlap)), by = cf]

  # Calculate total effort for each overlap group (capped at 48/40)
  dt[, weekly_effort := pmin(sum(contract_effort), 48 / 40),
     by = .(cf, overlap_group)]

  # Clean up temporary columns
  dt[, c("prev_fine", "has_overlap", "overlap_group", "contract_effort") := NULL]

  n_full_time <- sum(dt$weekly_effort >= 0.99 & dt$weekly_effort <= 1.01, na.rm = TRUE)
  n_part_time <- sum(dt$weekly_effort < 0.99, na.rm = TRUE)
  n_overtime <- sum(dt$weekly_effort > 1.01, na.rm = TRUE)

  cat(sprintf("  Full-time equivalent (≈1): %s contracts\n",
              format(n_full_time, big.mark = ",")))
  cat(sprintf("  Part-time (<1): %s contracts\n",
              format(n_part_time, big.mark = ",")))
  cat(sprintf("  Overtime (>1): %s contracts\n",
              format(n_overtime, big.mark = ",")))

  return(dt)
}


# 2. Year splitting function -----

#' Split contracts across calendar years
#'
#' Takes contracts that may span multiple years and creates separate
#' records for each calendar year, adjusting dates to year boundaries.
#'
#' @param dt data.table with columns: cf, inizio, fine, and other attributes
#' @return data.table with one row per contract-year
#' @export
split_contracts_by_year <- function(dt) {

  cat("Splitting contracts by calendar year...\n")

  dt <- data.table::copy(dt)
  data.table::setDT(dt)

  # Ensure date columns are IDate
  dt[, inizio := data.table::as.IDate(inizio)]
  dt[, fine := data.table::as.IDate(fine)]

  n_before <- nrow(dt)

  # Extract years
  dt[, year_start := data.table::year(inizio)]
  dt[, year_end := data.table::year(fine)]

  # For contracts spanning multiple years, create a row for each year
  # Use data.table's CJ (Cross Join) approach for efficiency

  # Identify multi-year contracts
  dt[, is_multi_year := year_start != year_end]

  # Split into single-year and multi-year
  single_year <- dt[is_multi_year == FALSE]
  multi_year <- dt[is_multi_year == TRUE]

  if (nrow(multi_year) > 0) {
    cat(sprintf("  %s contracts span multiple years\n",
                format(nrow(multi_year), big.mark = ",")))

    # Create expanded dataset with one row per year
    # For each contract, generate sequence of years it spans
    multi_year[, year_seq := list(list(seq(year_start, year_end, by = 1))), by = seq_len(nrow(multi_year))]

    # Unnest the year list
    multi_year_expanded <- multi_year[, .(year = unlist(year_seq)), by = seq_len(nrow(multi_year))]

    # Add back all other columns
    # Match expanded rows to original rows
    row_indices <- rep(seq_len(nrow(multi_year)), sapply(multi_year$year_seq, length))
    other_cols <- multi_year[row_indices, -c("year_seq", "year_start", "year_end", "is_multi_year")]

    multi_year_expanded <- cbind(multi_year_expanded[, -"seq_len"], other_cols)

    # Adjust start and end dates to year boundaries
    multi_year_expanded[, year_inizio := data.table::as.IDate(paste0(year, "-01-01"))]
    multi_year_expanded[, year_fine := data.table::as.IDate(paste0(year, "-12-31"))]

    # Clip to actual contract dates
    multi_year_expanded[, adj_inizio := pmax(inizio, year_inizio)]
    multi_year_expanded[, adj_fine := pmin(fine, year_fine)]

    # Replace original dates with adjusted ones
    multi_year_expanded[, inizio := adj_inizio]
    multi_year_expanded[, fine := adj_fine]
    multi_year_expanded[, c("year_inizio", "year_fine", "adj_inizio", "adj_fine") := NULL]

  } else {
    multi_year_expanded <- data.table::data.table()
  }

  # Add year column to single-year contracts
  single_year[, year := year_start]
  single_year[, c("year_start", "year_end", "is_multi_year") := NULL]

  # Combine
  result <- data.table::rbindlist(list(single_year, multi_year_expanded), use.names = TRUE, fill = TRUE)

  n_after <- nrow(result)
  cat(sprintf("  Contracts: %s → %s rows (split across years)\n",
              format(n_before, big.mark = ","),
              format(n_after, big.mark = ",")))

  return(result)
}


# 3. Annual coverage calculation -----

#' Calculate annual coverage for each contract-year
#'
#' Computes: coverage = weekly_effort * (durata / 365)
#' where durata is the number of days the contract was active in that year.
#'
#' @param dt data.table with columns: inizio, fine, weekly_effort, year
#' @return data.table with added durata and annual_coverage columns
#' @export
calculate_annual_coverage <- function(dt) {

  cat("Calculating annual coverage...\n")

  dt <- data.table::copy(dt)
  data.table::setDT(dt)

  # Calculate duration in days (inclusive of both start and end dates)
  dt[, durata := as.numeric(fine - inizio) + 1]

  # Calculate annual percentage (durata / 365)
  dt[, annual_pct := durata / 365]

  # Calculate annual coverage
  dt[, annual_coverage := weekly_effort * annual_pct]

  # Summary statistics
  cat(sprintf("  Mean annual coverage: %.3f\n", mean(dt$annual_coverage, na.rm = TRUE)))
  cat(sprintf("  Median annual coverage: %.3f\n", median(dt$annual_coverage, na.rm = TRUE)))
  cat(sprintf("  Max annual coverage: %.3f\n", max(dt$annual_coverage, na.rm = TRUE)))

  # Count full-year full-time contracts (should have coverage ≈ 1)
  full_year_full_time <- dt[durata >= 365 & weekly_effort >= 0.99 & weekly_effort <= 1.01]
  if (nrow(full_year_full_time) > 0) {
    mean_coverage_fyft <- mean(full_year_full_time$annual_coverage, na.rm = TRUE)
    cat(sprintf("  Full-year full-time contracts: %s (mean coverage: %.3f)\n",
                format(nrow(full_year_full_time), big.mark = ","),
                mean_coverage_fyft))
  }

  return(dt)
}


# 4. Aggregation by year, Comune, and sector -----

#' Aggregate coverage by year, Comune, and industrial sector
#'
#' Groups contracts by calendar year, workplace municipality (COMUNE_SEDE_LAVORO),
#' and ATECO sector, computing total coverage and worker counts.
#'
#' @param dt data.table with coverage data
#' @return data.table with aggregated coverage by year, Comune, and sector
#' @export
aggregate_coverage_by_year_comune_sector <- function(dt) {

  cat("Aggregating coverage by year, Comune, and sector...\n")

  dt <- data.table::copy(dt)
  data.table::setDT(dt)

  # Filter to employment spells only (exclude unemployment where arco == 0)
  if ("arco" %in% names(dt)) {
    dt <- dt[arco != 0]
    cat(sprintf("  Filtered to employment spells: %s contracts\n",
                format(nrow(dt), big.mark = ",")))
  }

  # Group by year, comune, and sector
  aggregated <- dt[!is.na(COMUNE_SEDE_LAVORO) & !is.na(ateco), .(
    total_coverage = sum(annual_coverage, na.rm = TRUE),
    n_contracts = .N,
    n_workers = data.table::uniqueN(cf),
    mean_coverage = mean(annual_coverage, na.rm = TRUE),
    median_coverage = median(annual_coverage, na.rm = TRUE)
  ), by = .(year, COMUNE_SEDE_LAVORO, ateco)]

  # Sort by year, total coverage descending
  data.table::setorder(aggregated, year, -total_coverage)

  cat(sprintf("\n  Aggregation complete:\n"))
  cat(sprintf("    Years covered: %d to %d\n",
              min(aggregated$year), max(aggregated$year)))
  cat(sprintf("    Comuni: %d\n",
              aggregated[, data.table::uniqueN(COMUNE_SEDE_LAVORO)]))
  cat(sprintf("    Sectors: %d\n",
              aggregated[, data.table::uniqueN(ateco)]))
  cat(sprintf("    Total records: %s\n",
              format(nrow(aggregated), big.mark = ",")))
  cat(sprintf("    Total coverage (all years): %.0f FTE\n",
              sum(aggregated$total_coverage)))

  # Show top 5 year-comune-sector combinations by coverage
  cat("\n  Top 5 combinations by total coverage:\n")
  top5 <- head(aggregated[order(-total_coverage)], 5)
  print(top5[, .(year, COMUNE_SEDE_LAVORO, ateco, total_coverage, n_workers)])

  return(aggregated)
}


# 5. Main workflow function -----

#' Estimate annual coverage from workplace branch data
#'
#' Complete workflow to estimate contractual annual coverage:
#' 1. Calculate weekly effort (handling full-time, part-time, overlaps)
#' 2. Split contracts across calendar years
#' 3. Calculate annual coverage for each contract-year
#' 4. Aggregate by year, Comune (workplace), and sector
#'
#' @param workplace_data Consolidated workplace branch data with employment contracts
#' @return data.table with coverage aggregated by year, Comune, and sector
#' @export
estimate_annual_coverage <- function(workplace_data) {

  cat("\n")
  cat("=" %+% rep("=", 70) %+% "\n")
  cat("ESTIMATING ANNUAL CONTRACTUAL COVERAGE\n")
  cat("=" %+% rep("=", 70) %+% "\n\n")

  # Ensure data.table
  data.table::setDT(workplace_data)

  cat(sprintf("Input: %s contracts for %s workers\n\n",
              format(nrow(workplace_data), big.mark = ","),
              format(workplace_data[, data.table::uniqueN(cf)], big.mark = ",")))

  # Step 1: Calculate weekly effort
  cat("STEP 1: Calculate weekly effort\n")
  cat("-" %+% rep("-", 70) %+% "\n")
  dt <- calculate_weekly_effort(workplace_data)

  # Step 2: Split by calendar year
  cat("\nSTEP 2: Split contracts by calendar year\n")
  cat("-" %+% rep("-", 70) %+% "\n")
  dt <- split_contracts_by_year(dt)

  # Step 3: Calculate annual coverage
  cat("\nSTEP 3: Calculate annual coverage\n")
  cat("-" %+% rep("-", 70) %+% "\n")
  dt <- calculate_annual_coverage(dt)

  # Step 4: Aggregate by year, comune, sector
  cat("\nSTEP 4: Aggregate by year, Comune, and sector\n")
  cat("-" %+% rep("-", 70) %+% "\n")
  coverage_summary <- aggregate_coverage_by_year_comune_sector(dt)

  cat("\n")
  cat("=" %+% rep("=", 70) %+% "\n")
  cat("✓ COVERAGE ESTIMATION COMPLETE\n")
  cat("=" %+% rep("=", 70) %+% "\n\n")

  return(coverage_summary)
}


# 6. Helper operator -----

# String concatenation operator (if not already defined)
if (!exists("%+%", mode = "function")) {
  `%+%` <- function(a, b) paste0(a, b)
}
