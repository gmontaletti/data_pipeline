# policy_aggregates.R
# Functions for precomputing policy coverage aggregates for dashboard performance
# Author: Giampaolo Montaletti (giampaolo.montaletti@gmail.com)

# 1. Create policy timeseries aggregates -----

#' Create policy coverage time series aggregates
#'
#' Precomputes monthly or quarterly aggregates of DID/POL coverage rates over time.
#' This eliminates the need for date parsing and aggregation in the dashboard,
#' dramatically improving Panel 4 (Time Series) performance.
#'
#' @param transitions data.table with transition records including:
#'   - transition_date: Date of transition
#'   - has_did: Logical, whether transition has DID support
#'   - has_pol: Logical, whether transition has POL support
#'   - sesso: Gender
#'   - cleta: Age class
#'   - istruzione: Education level
#'   - area: Geographic area
#'   - cpi_code: CPI code
#' @param freq Character: "month" or "quarter" for aggregation frequency
#'
#' @return data.table with columns:
#'   - period: Year-month or year-quarter
#'   - n_transitions: Total transitions in period
#'   - n_with_did: Number with DID support
#'   - n_with_pol: Number with POL support
#'   - pct_with_did: Percentage with DID
#'   - pct_with_pol: Percentage with POL
#'
#' @export
create_policy_timeseries_aggregates <- function(transitions, freq = "month") {

  if (!data.table::is.data.table(transitions)) {
    stop("transitions must be a data.table")
  }

  if (!freq %in% c("month", "quarter")) {
    stop("freq must be 'month' or 'quarter'")
  }

  # Filter to valid dates
  valid_data <- transitions[!is.na(transition_date)]

  if (nrow(valid_data) == 0) {
    warning("No valid transition dates found")
    return(data.table::data.table())
  }

  # Create period column
  if (freq == "month") {
    valid_data[, period := format(transition_date, "%Y-%m")]
  } else {
    valid_data[, period := paste0(
      data.table::year(transition_date),
      "-Q",
      data.table::quarter(transition_date)
    )]
  }

  # Aggregate by period
  timeseries <- valid_data[, .(
    n_transitions = .N,
    n_with_did = sum(has_did, na.rm = TRUE),
    n_with_pol = sum(has_pol, na.rm = TRUE)
  ), by = period]

  # Calculate percentages
  timeseries[, `:=`(
    pct_with_did = round(100 * n_with_did / n_transitions, 2),
    pct_with_pol = round(100 * n_with_pol / n_transitions, 2)
  )]

  # Sort by period
  data.table::setorder(timeseries, period)

  return(timeseries)
}


# 2. Create policy coverage by demographics -----

#' Create policy coverage aggregates by demographics
#'
#' Precomputes DID/POL coverage rates grouped by demographic variables.
#' This eliminates aggregation in Panel 1 (Demographics) of the dashboard.
#'
#' @param transitions data.table with transition records including:
#'   - has_did: Logical, whether transition has DID support
#'   - has_pol: Logical, whether transition has POL support
#'   - sesso: Gender
#'   - cleta: Age class
#'   - istruzione: Education level
#'
#' @return data.table with columns:
#'   - group_by: Grouping variable (sesso, cleta, istruzione, or "Tutti")
#'   - group_value: Value of grouping variable
#'   - n_transitions: Total transitions
#'   - n_with_did: Number with DID
#'   - n_with_pol: Number with POL
#'   - pct_with_did: Percentage with DID
#'   - pct_with_pol: Percentage with POL
#'
#' @export
create_policy_coverage_by_demographics <- function(transitions) {

  if (!data.table::is.data.table(transitions)) {
    stop("transitions must be a data.table")
  }

  # Overall statistics
  overall <- transitions[, .(
    group_by = "Tutti",
    group_value = "Tutti",
    n_transitions = .N,
    n_with_did = sum(has_did, na.rm = TRUE),
    n_with_pol = sum(has_pol, na.rm = TRUE)
  )]

  # By gender
  by_gender <- transitions[!is.na(sesso), .(
    group_by = "sesso",
    group_value = sesso,
    n_transitions = .N,
    n_with_did = sum(has_did, na.rm = TRUE),
    n_with_pol = sum(has_pol, na.rm = TRUE)
  ), by = sesso]
  data.table::setnames(by_gender, "sesso", "group_value", skip_absent = TRUE)
  by_gender[, group_by := "sesso"]

  # By age class
  by_age <- transitions[!is.na(cleta), .(
    group_by = "cleta",
    group_value = cleta,
    n_transitions = .N,
    n_with_did = sum(has_did, na.rm = TRUE),
    n_with_pol = sum(has_pol, na.rm = TRUE)
  ), by = cleta]
  data.table::setnames(by_age, "cleta", "group_value", skip_absent = TRUE)
  by_age[, group_by := "cleta"]

  # By education
  by_education <- transitions[!is.na(istruzione), .(
    group_by = "istruzione",
    group_value = istruzione,
    n_transitions = .N,
    n_with_did = sum(has_did, na.rm = TRUE),
    n_with_pol = sum(has_pol, na.rm = TRUE)
  ), by = istruzione]
  data.table::setnames(by_education, "istruzione", "group_value", skip_absent = TRUE)
  by_education[, group_by := "istruzione"]

  # Combine all groups
  demographics <- data.table::rbindlist(list(
    overall,
    by_gender[, .(group_by, group_value, n_transitions, n_with_did, n_with_pol)],
    by_age[, .(group_by, group_value, n_transitions, n_with_did, n_with_pol)],
    by_education[, .(group_by, group_value, n_transitions, n_with_did, n_with_pol)]
  ))

  # Calculate percentages
  demographics[, `:=`(
    pct_with_did = round(100 * n_with_did / n_transitions, 2),
    pct_with_pol = round(100 * n_with_pol / n_transitions, 2)
  )]

  return(demographics)
}


# 3. Create policy coverage by geography -----

#' Create policy coverage aggregates by geography
#'
#' Precomputes DID/POL coverage rates by geographic area and CPI.
#' This eliminates aggregation in Panel 3 (Geographic Map) of the dashboard.
#'
#' @param transitions data.table with transition records including:
#'   - has_did: Logical, whether transition has DID support
#'   - has_pol: Logical, whether transition has POL support
#'   - area: Geographic area
#'   - cpi_code: CPI code
#'   - cpi_name: CPI name
#'
#' @return data.table with columns:
#'   - geo_level: "area" or "cpi"
#'   - geo_code: Area or CPI code
#'   - geo_name: Area or CPI name
#'   - n_transitions: Total transitions
#'   - n_with_did: Number with DID
#'   - n_with_pol: Number with POL
#'   - did_coverage_pct: Percentage with DID
#'   - pol_coverage_pct: Percentage with POL
#'
#' @export
create_policy_coverage_by_geography <- function(transitions) {

  if (!data.table::is.data.table(transitions)) {
    stop("transitions must be a data.table")
  }

  # By area
  by_area <- transitions[!is.na(area), .(
    geo_level = "area",
    geo_code = area,
    geo_name = area,  # Area code = area name in this dataset
    n_transitions = .N,
    n_with_did = sum(has_did, na.rm = TRUE),
    n_with_pol = sum(has_pol, na.rm = TRUE)
  ), by = area]
  data.table::setnames(by_area, "area", "geo_code", skip_absent = TRUE)
  by_area[, `:=`(geo_level = "area", geo_name = geo_code)]

  # By CPI
  by_cpi <- transitions[!is.na(cpi_code), .(
    geo_level = "cpi",
    geo_code = cpi_code,
    geo_name = cpi_name[1],  # Take first non-NA name
    n_transitions = .N,
    n_with_did = sum(has_did, na.rm = TRUE),
    n_with_pol = sum(has_pol, na.rm = TRUE)
  ), by = .(cpi_code, cpi_name)]
  data.table::setnames(by_cpi, c("cpi_code", "cpi_name"), c("geo_code", "geo_name"), skip_absent = TRUE)
  by_cpi[, geo_level := "cpi"]

  # Combine
  geography <- data.table::rbindlist(list(
    by_area[, .(geo_level, geo_code, geo_name, n_transitions, n_with_did, n_with_pol)],
    by_cpi[, .(geo_level, geo_code, geo_name, n_transitions, n_with_did, n_with_pol)]
  ))

  # Calculate percentages
  geography[, `:=`(
    did_coverage_pct = round(100 * n_with_did / n_transitions, 2),
    pol_coverage_pct = round(100 * n_with_pol / n_transitions, 2)
  )]

  return(geography)
}


# 4. Create policy duration distributions -----

#' Create pre-filtered policy duration distributions
#'
#' Precomputes unemployment duration data filtered for box plot visualization.
#' Filters to unemployment spells >7 days and creates policy status labels.
#' This speeds up Panel 2 (Duration Box Plot) data preparation.
#'
#' @param transitions data.table with transition records including:
#'   - unemployment_duration: Duration of unemployment spell
#'   - has_did: Logical, whether transition has DID support
#'   - has_pol: Logical, whether transition has POL support
#'   - sesso: Gender
#'   - cleta: Age class
#'   - istruzione: Education level
#'
#' @return data.table with columns:
#'   - unemployment_duration: Duration in days
#'   - has_did: DID status
#'   - has_pol: POL status
#'   - did_label: "Con DID" or "Senza DID"
#'   - pol_label: "Con POL" or "Senza POL"
#'   - sesso: Gender
#'   - cleta: Age class
#'   - istruzione: Education level
#'
#' @export
create_policy_duration_distributions <- function(transitions) {

  if (!data.table::is.data.table(transitions)) {
    stop("transitions must be a data.table")
  }

  # Filter to unemployment spells >7 days (exclude job-to-job transitions)
  valid_data <- transitions[
    !is.na(unemployment_duration) & unemployment_duration > 7,
    .(
      unemployment_duration,
      has_did,
      has_pol,
      sesso,
      cleta,
      istruzione
    )
  ]

  # Create policy status labels
  valid_data[, `:=`(
    did_label = ifelse(has_did, "Con DID", "Senza DID"),
    pol_label = ifelse(has_pol, "Con POL", "Senza POL")
  )]

  # Sample if too large (for box plot performance)
  # Box plots don't benefit much from >10k points
  if (nrow(valid_data) > 50000) {
    cat("  Sampling duration data from", nrow(valid_data), "to 50000 rows for performance\n")
    valid_data <- valid_data[sample(.N, 50000)]
  }

  return(valid_data)
}
