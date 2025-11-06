# aggregations.R
# Functions for creating aggregated datasets for dashboard
# Author: Giampaolo Montaletti (giampaolo.montaletti@gmail.com)

# 1. Create person-level data -----

#' Create person-level summary data
#'
#' Merges demographics, transition statistics, career metrics, and cluster
#' assignments into a comprehensive person-level dataset.
#'
#' @param anag A data.table with person demographics (from extract_demographics())
#' @param transitions A data.table with all transitions (closed + open)
#' @param career_metrics A data.table with career metrics per person
#' @param clusters A list with cluster_assignments data.table (from cluster_trajectories())
#'
#' @return A data.table with one row per person containing:
#'   \itemize{
#'     \item Demographics: cf, sesso, eta, cleta, cpi_code, cpi_name,
#'       istruzione, professione, ateco_3digit
#'     \item Transition stats: n_transitions_total, n_transitions_closed,
#'       n_transitions_open, n_transitions_with_did, n_transitions_with_pol,
#'       median_unemployment_duration, did_coverage_rate, pol_coverage_rate
#'     \item Career metrics: employment_rate, employment_stability_index,
#'       job_turnover_rate, and other metrics from calculate_comprehensive_career_metrics()
#'     \item Cluster info: cluster, cluster_label_it
#'   }
#'
#' @details
#' This function creates the foundation dataset for person-level analysis in the
#' dashboard. It aggregates transition-level information and combines it with
#' career trajectory metrics and clustering results.
#'
#' Transition statistics include:
#' - Total transition count (closed + open)
#' - Counts by transition type
#' - DID/POL coverage rates (% of transitions with support)
#' - Median unemployment duration between jobs
#'
#' @examples
#' \dontrun{
#' person_data <- create_person_data(anag, transitions, career_metrics, clusters)
#' }
#'
#' @export
create_person_data <- function(anag, transitions, career_metrics, clusters) {

  if (!data.table::is.data.table(anag)) {
    stop("anag must be a data.table")
  }
  if (!data.table::is.data.table(transitions)) {
    stop("transitions must be a data.table")
  }
  if (!data.table::is.data.table(career_metrics)) {
    stop("career_metrics must be a data.table")
  }
  if (!is.list(clusters) || !"cluster_assignments" %in% names(clusters)) {
    stop("clusters must be a list with cluster_assignments element")
  }

  cat("Creating person-level summary data from transitions...\n")

  # Aggregate transition-level statistics per person
  person_transitions <- transitions[, .(
    n_transitions_total = .N,
    n_transitions_closed = sum(is_closed),
    n_transitions_open = sum(is_open),
    n_transitions_with_did = sum(has_did, na.rm = TRUE),
    n_transitions_with_pol = sum(has_pol, na.rm = TRUE),
    median_unemployment_duration = median(unemployment_duration, na.rm = TRUE)
  ), by = cf]

  # Calculate coverage rates
  person_transitions[, `:=`(
    did_coverage_rate = (n_transitions_with_did / n_transitions_total) * 100,
    pol_coverage_rate = (n_transitions_with_pol / n_transitions_total) * 100
  )]

  cat("  Aggregated transition statistics for", nrow(person_transitions), "individuals\n")

  # Merge all components: demographics + transition stats + career metrics + clusters
  person_data <- merge(anag, person_transitions, by = "cf", all.x = TRUE)
  person_data <- merge(person_data, career_metrics, by = "cf", all.x = TRUE)
  person_data <- merge(person_data, clusters$cluster_assignments, by = "cf", all.x = TRUE)

  cat("  Created person_data with", nrow(person_data), "rows and",
      ncol(person_data), "columns\n")

  return(person_data)
}


# 2. Create geographic summaries -----

#' Create geographic aggregates by CPI
#'
#' Aggregates person-level data by CPI (Centro Per l'Impiego) and demographic
#' groups, including transition counts from the transitions table.
#'
#' @param person_data A data.table with person-level data (from create_person_data())
#' @param transitions A data.table with all transitions (for transition counts)
#'
#' @return A data.table with CPI-level geographic aggregates
#'
#' @details
#' Each geographic aggregate contains:
#' - cpi_code: CPI identifier code
#' - cpi_name: CPI name (human-readable)
#' - sesso: Gender (M/F/Tutti)
#' - cleta: Age class (or "Tutti")
#' - istruzione: Education level (or "Tutti")
#' - n_persons: Number of individuals in the group
#' - median_employment_rate: Median employment rate
#' - median_stability: Median employment stability index
#' - median_turnover: Median job turnover rate
#' - n_transitions: Total number of transitions
#' - pct_female: Percentage of females (only in "Tutti" aggregates)
#' - pct_university: Percentage with university education (only in "Tutti" aggregates)
#'
#' Aggregates are created for:
#' 1. Each combination of CPI × demographics (sesso, cleta, istruzione)
#' 2. Overall totals with sesso="Tutti", cleta="Tutti", istruzione="Tutti"
#'
#' @examples
#' \dontrun{
#' geo_summary <- create_geo_summaries(person_data, transitions)
#' }
#'
#' @export
create_geo_summaries <- function(person_data, transitions) {

  if (!data.table::is.data.table(person_data)) {
    stop("person_data must be a data.table")
  }
  if (!data.table::is.data.table(transitions)) {
    stop("transitions must be a data.table")
  }

  cat("Creating geographic aggregates by CPI...\n")

  # CPI-based summaries -----
  cat("  Creating CPI-level aggregates...\n")

  # Detailed aggregates by CPI + demographics (aggregate by cpi_code only)
  geo_summary_cpi <- person_data[!is.na(cpi_code), .(
    n_persons = .N,
    median_employment_rate = median(employment_rate, na.rm = TRUE),
    median_stability = median(employment_stability_index, na.rm = TRUE),
    median_turnover = median(job_turnover_rate, na.rm = TRUE),
    pct_female = sum(sesso == "F", na.rm = TRUE) / .N * 100,
    pct_university = sum(istruzione == "universitaria", na.rm = TRUE) / .N * 100
  ), by = .(cpi_code, sesso, cleta, istruzione)]

  # Overall CPI aggregates (all demographics combined)
  geo_summary_cpi_total <- person_data[!is.na(cpi_code), .(
    sesso = "Tutti",
    cleta = "Tutti",
    istruzione = "Tutti",
    n_persons = .N,
    median_employment_rate = median(employment_rate, na.rm = TRUE),
    median_stability = median(employment_stability_index, na.rm = TRUE),
    median_turnover = median(job_turnover_rate, na.rm = TRUE),
    pct_female = sum(sesso == "F", na.rm = TRUE) / .N * 100,
    pct_university = sum(istruzione == "universitaria", na.rm = TRUE) / .N * 100
  ), by = .(cpi_code)]

  # Combine detailed and total
  geo_summary_cpi <- data.table::rbindlist(
    list(geo_summary_cpi, geo_summary_cpi_total),
    fill = TRUE
  )

  # Add transition counts - detailed level (by demographics)
  transition_counts_cpi_detailed <- transitions[!is.na(cpi_code),
    .(n_transitions = .N),
    by = .(cpi_code, sesso, cleta, istruzione)]

  # Add transition counts - "Tutti" level (all demographics combined)
  transition_counts_cpi_total <- transitions[!is.na(cpi_code),
    .(
      sesso = "Tutti",
      cleta = "Tutti",
      istruzione = "Tutti",
      n_transitions = .N
    ),
    by = cpi_code]

  # Combine both transition count levels
  transition_counts_cpi <- data.table::rbindlist(
    list(transition_counts_cpi_detailed, transition_counts_cpi_total),
    fill = TRUE
  )

  # Merge transition counts with geo_summary_cpi
  geo_summary_cpi <- merge(geo_summary_cpi, transition_counts_cpi,
                          by = c("cpi_code", "sesso", "cleta", "istruzione"),
                          all.x = TRUE)
  geo_summary_cpi[is.na(n_transitions), n_transitions := 0]

  # Add canonical CPI names from lookup table (prevents name duplication issues)
  shared_data_dir <- ifelse(Sys.getenv("SHARED_DATA_DIR") == "",
                            "~/Documents/funzioni/shared_data",
                            Sys.getenv("SHARED_DATA_DIR"))
  lookup_path <- file.path(shared_data_dir, "maps/comune_cpi_lookup.rds")

  if (file.exists(lookup_path)) {
    lookup <- readRDS(lookup_path)
    canonical_names <- unique(lookup[, .(cpi_code, cpi_name)])
    geo_summary_cpi <- merge(geo_summary_cpi, canonical_names, by = "cpi_code", all.x = TRUE)
  } else {
    warning("CPI lookup table not found at ", lookup_path, ". CPI names will be missing.")
    geo_summary_cpi[, cpi_name := NA_character_]
  }

  cat("    Created", nrow(geo_summary_cpi), "CPI-level aggregate rows\n")
  cat("    Unique CPI areas:", data.table::uniqueN(geo_summary_cpi$cpi_code), "\n")

  return(geo_summary_cpi)
}


# 3. Create monthly time series -----

#' Create monthly time series data
#'
#' Aggregates transitions by month and CPI/demographic dimensions for
#' time series analysis and visualization.
#'
#' @param transitions A data.table with all transitions containing columns:
#'   year, month, is_closed, is_open, has_did, has_pol, cpi_code, cpi_name,
#'   sesso, cleta, istruzione
#'
#' @return A data.table with monthly time series by CPI and demographics
#'
#' @details
#' Each monthly time series record contains:
#' - year_month: Date (first day of month)
#' - cpi_code: CPI identifier code
#' - cpi_name: CPI name (human-readable)
#' - sesso: Gender (M/F/Tutti)
#' - cleta: Age class (or "Tutti")
#' - istruzione: Education level (or "Tutti")
#' - n_transitions_closed: Count of closed transitions
#' - n_transitions_open: Count of open transitions
#' - n_transitions_total: Total transition count
#' - n_with_did: Count with DID support
#' - n_with_pol: Count with POL support
#'
#' Time series are created for:
#' 1. Each combination of CPI × month × demographics
#' 2. Overall monthly totals with "Tutti" for all demographic dimensions
#'
#' @examples
#' \dontrun{
#' monthly_ts <- create_monthly_timeseries(transitions)
#' }
#'
#' @export
create_monthly_timeseries <- function(transitions) {

  if (!data.table::is.data.table(transitions)) {
    stop("transitions must be a data.table")
  }

  required_cols <- c("year", "month", "is_closed", "is_open")
  missing_cols <- setdiff(required_cols, names(transitions))
  if (length(missing_cols) > 0) {
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }

  cat("Creating monthly time series data by CPI...\n")

  # Create year_month date column
  transitions[, year_month := as.Date(paste(year, month, "01", sep = "-"))]

  # CPI-based time series -----
  cat("  Creating CPI-level monthly time series...\n")

  monthly_timeseries_cpi <- transitions[!is.na(cpi_code), .(
    n_transitions_closed = sum(is_closed),
    n_transitions_open = sum(is_open),
    n_transitions_total = .N,
    n_with_did = sum(has_did, na.rm = TRUE),
    n_with_pol = sum(has_pol, na.rm = TRUE)
  ), by = .(year_month, cpi_code, cpi_name, sesso, cleta, istruzione)]

  # Overall CPI monthly summaries
  monthly_cpi_total <- transitions[!is.na(cpi_code), .(
    sesso = "Tutti",
    cleta = "Tutti",
    istruzione = "Tutti",
    n_transitions_closed = sum(is_closed),
    n_transitions_open = sum(is_open),
    n_transitions_total = .N,
    n_with_did = sum(has_did, na.rm = TRUE),
    n_with_pol = sum(has_pol, na.rm = TRUE)
  ), by = .(year_month, cpi_code, cpi_name)]

  # Combine
  monthly_timeseries_cpi <- data.table::rbindlist(
    list(monthly_timeseries_cpi, monthly_cpi_total),
    fill = TRUE
  )

  cat("    Created", nrow(monthly_timeseries_cpi), "CPI-level monthly rows\n")

  return(monthly_timeseries_cpi)
}


# 4. Create policy support summary -----

#' Create policy support effectiveness summary
#'
#' Aggregates transitions by CPI to evaluate the effectiveness of DID and POL active
#' labor market policies in reducing unemployment duration.
#'
#' @param transitions A data.table with all transitions containing columns:
#'   unemployment_duration, has_did, has_pol, cpi_code, cpi_name,
#'   sesso, cleta, istruzione
#'
#' @return A data.table with policy summary by CPI and demographics
#'
#' @details
#' Each policy summary record contains:
#' - cpi_code: CPI identifier code
#' - cpi_name: CPI name (human-readable)
#' - sesso: Gender (M/F/Tutti)
#' - cleta: Age class (or "Tutti")
#' - istruzione: Education level (or "Tutti")
#' - n_transitions: Total transition count
#' - n_with_did: Count with DID support
#' - n_with_pol: Count with POL support
#' - pct_with_did: Percentage with DID support
#' - pct_with_pol: Percentage with POL support
#' - median_unemp_duration_all: Median unemployment duration (all)
#' - median_unemp_duration_with_did: Median unemployment with DID
#' - median_unemp_duration_no_did: Median unemployment without DID
#' - median_unemp_duration_with_pol: Median unemployment with POL
#' - median_unemp_duration_no_pol: Median unemployment without POL
#'
#' This enables comparison of unemployment durations between supported and
#' non-supported transitions to assess policy effectiveness.
#'
#' @examples
#' \dontrun{
#' policy_summary <- create_policy_summary(transitions)
#' }
#'
#' @export
create_policy_summary <- function(transitions) {

  if (!data.table::is.data.table(transitions)) {
    stop("transitions must be a data.table")
  }

  required_cols <- c("unemployment_duration", "has_did", "has_pol")
  missing_cols <- setdiff(required_cols, names(transitions))
  if (length(missing_cols) > 0) {
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }

  cat("Creating policy support summary by CPI...\n")

  # Filter to transitions with unemployment duration (exclude NA)
  trans_with_unemp <- transitions[!is.na(unemployment_duration)]

  # CPI-based policy summary -----
  cat("  Creating CPI-level policy summary...\n")

  policy_summary_cpi <- trans_with_unemp[!is.na(cpi_code), .(
    n_transitions = .N,
    n_with_did = sum(has_did, na.rm = TRUE),
    n_with_pol = sum(has_pol, na.rm = TRUE),
    pct_with_did = sum(has_did, na.rm = TRUE) / .N * 100,
    pct_with_pol = sum(has_pol, na.rm = TRUE) / .N * 100,
    median_unemp_duration_all = median(unemployment_duration, na.rm = TRUE),
    median_unemp_duration_with_did = median(unemployment_duration[has_did], na.rm = TRUE),
    median_unemp_duration_no_did = median(unemployment_duration[!has_did], na.rm = TRUE),
    median_unemp_duration_with_pol = median(unemployment_duration[has_pol], na.rm = TRUE),
    median_unemp_duration_no_pol = median(unemployment_duration[!has_pol], na.rm = TRUE)
  ), by = .(cpi_code, cpi_name, sesso, cleta, istruzione)]

  cat("    Created CPI-level policy summary with", nrow(policy_summary_cpi), "rows\n")

  return(policy_summary_cpi)
}
