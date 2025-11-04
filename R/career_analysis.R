# career_analysis.R
# Functions for career metrics and trajectory analysis
# Author: Giampaolo Montaletti (giampaolo.montaletti@gmail.com)

# 1. Compute survival and comprehensive career metrics -----

#' Compute survival analysis and comprehensive career metrics
#'
#' Wrapper function that combines survival analysis and comprehensive career
#' metrics calculation using longworkR functions. This provides a complete
#' picture of employment stability and career trajectory for each person.
#'
#' @param dt A data.table containing employment spells. Must include columns:
#'   cf (person ID), COD_TIPOLOGIA_CONTRATTUALE (contract type), durata (duration),
#'   troncata (censoring indicator), retribuzione (salary), plus complexity
#'   variables like over_id, arco, prior, qualifica, ateco, datore.
#' @param survival_curves Optional. Pre-computed survival curves from
#'   estimate_contract_survival_optimized(). If NULL, will compute from data.
#'   Default is NULL.
#' @param metrics Character vector specifying which metrics to calculate. Options:
#'   "all", "stability", "employment", "progression", "complexity". Default is "all".
#' @param min_spell_duration Numeric. Minimum spell duration in days to include
#'   in analysis. Default is 1.
#' @param confidence_level Numeric. Confidence level for survival curves (0-1).
#'   Default is 0.95.
#'
#' @return A list with two elements:
#'   \itemize{
#'     \item survival_curves: Survival analysis results from estimate_contract_survival_optimized()
#'     \item career_metrics: Person-level career metrics from calculate_comprehensive_career_metrics()
#'   }
#'
#' @details
#' This function orchestrates two key longworkR analyses:
#'
#' 1. **Survival Analysis**: Uses estimate_contract_survival_optimized() to compute
#'    contract-specific survival curves, median survival times, and hazard rates.
#'    These results inform contract quality calculations.
#'
#' 2. **Career Metrics**: Uses calculate_comprehensive_career_metrics() to compute
#'    person-level metrics including:
#'    - Employment stability index
#'    - Employment rate
#'    - Job turnover rate
#'    - Career progression indicators
#'    - Trajectory complexity measures
#'
#' The survival curves are passed to the career metrics calculation to enable
#' data-driven contract quality assessment based on actual survival durations.
#'
#' @examples
#' \dontrun{
#' results <- compute_survival_and_metrics(dt, metrics = "all")
#' survival_curves <- results$survival_curves
#' career_metrics <- results$career_metrics
#' }
#'
#' @export
compute_survival_and_metrics <- function(dt,
                                        survival_curves = NULL,
                                        metrics = "all",
                                        min_spell_duration = 1,
                                        confidence_level = 0.95) {

  if (!data.table::is.data.table(dt)) {
    stop("Input must be a data.table")
  }

  required_cols <- c("cf", "COD_TIPOLOGIA_CONTRATTUALE", "durata", "troncata")
  missing_cols <- setdiff(required_cols, names(dt))
  if (length(missing_cols) > 0) {
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }

  cat("Computing survival analysis and career metrics...\n\n")

  # Step 1: Compute survival analysis if not provided
  if (is.null(survival_curves)) {
    cat("Step 1: Computing contract survival curves...\n")

    survival_curves <- longworkR::estimate_contract_survival_optimized(
      dt,
      contract_type_var = "COD_TIPOLOGIA_CONTRATTUALE",
      duration_var = "durata",
      censored_var = "troncata",
      confidence_level = confidence_level
    )

    cat("  Completed survival analysis for",
        length(unique(survival_curves$survival_table$group)), "contract types\n\n")

  } else {
    cat("Step 1: Using pre-computed survival curves\n\n")
  }

  # Step 2: Calculate comprehensive career metrics
  cat("Step 2: Calculating comprehensive career metrics...\n")

  # Determine complexity variables available in data
  complexity_vars <- c("over_id", "arco", "prior", "qualifica", "ateco", "datore")
  available_complexity_vars <- intersect(complexity_vars, names(dt))

  if (length(available_complexity_vars) == 0) {
    warning("No complexity variables found in data. Using basic metrics only.")
    available_complexity_vars <- NULL
  }

  career_metrics <- longworkR::calculate_comprehensive_career_metrics(
    dt,
    survival_data = survival_curves,
    metrics = metrics,
    id_column = "cf",
    output_format = "wide",
    contract_code_column = "COD_TIPOLOGIA_CONTRATTUALE",
    salary_column = if ("retribuzione" %in% names(dt)) "retribuzione" else NULL,
    complexity_variables = available_complexity_vars,
    min_spell_duration = min_spell_duration
  )

  cat("  Calculated metrics for", nrow(career_metrics), "individuals\n")

  # Verify key metrics are present
  key_metrics <- c("employment_rate", "employment_stability_index", "job_turnover_rate")
  present_metrics <- intersect(key_metrics, names(career_metrics))

  cat("\n")
  cat("  Key metrics computed:\n")
  for (metric in present_metrics) {
    cat("    -", metric, "\n")
  }

  if (length(present_metrics) < length(key_metrics)) {
    missing_metrics <- setdiff(key_metrics, present_metrics)
    warning("Some key metrics were not computed: ", paste(missing_metrics, collapse = ", "))
  }

  cat("\n")

  # Return both results
  return(list(
    survival_curves = survival_curves,
    career_metrics = career_metrics
  ))
}


# 2. Cluster career trajectories -----

#' Perform career trajectory clustering
#'
#' Wrapper function for longworkR::cluster_career_trajectories() that groups
#' individuals into career clusters based on their employment metrics.
#'
#' @param career_metrics A data.table containing career metrics from
#'   calculate_comprehensive_career_metrics(). Must include cf column.
#' @param n_clusters Integer. Number of clusters to create. If NULL, uses
#'   optimal cluster selection. Default is NULL.
#' @param features Character vector specifying which metric groups to use for
#'   clustering. Options: "stability", "employment", "progression", "complexity".
#'   Default is c("stability", "employment", "progression").
#' @param method Character string specifying clustering method. Options: "kmeans",
#'   "hierarchical", "dbscan". Default is "kmeans".
#' @param memory_fraction Numeric. Fraction of available memory to use (0-1).
#'   Default is 0.2.
#' @param verbose Logical. Print progress messages? Default is TRUE.
#'
#' @return A list with clustering results from cluster_career_trajectories():
#'   \itemize{
#'     \item cluster_assignments: Data.table with cf, cluster, cluster_label_it
#'     \item cluster_summary: Statistics for each cluster
#'     \item model: The fitted clustering model object
#'   }
#'
#' @details
#' This function identifies distinct career trajectory patterns by clustering
#' individuals based on their employment metrics. Common clusters include:
#' - Stable high-quality careers
#' - Precarious/unstable employment
#' - Career progressors
#' - Entry-level workers
#' - Mixed trajectories
#'
#' The function automatically handles memory constraints and can select the
#' optimal number of clusters using silhouette analysis or elbow method.
#'
#' @examples
#' \dontrun{
#' clusters <- cluster_trajectories(career_metrics, n_clusters = NULL)
#' cluster_assignments <- clusters$cluster_assignments
#' }
#'
#' @export
cluster_trajectories <- function(career_metrics,
                                n_clusters = NULL,
                                features = c("stability", "employment", "progression"),
                                method = "kmeans",
                                memory_fraction = 0.2,
                                verbose = TRUE) {

  if (!data.table::is.data.table(career_metrics)) {
    stop("career_metrics must be a data.table")
  }

  if (!"cf" %in% names(career_metrics)) {
    stop("career_metrics must contain 'cf' column")
  }

  if (verbose) {
    cat("Performing career trajectory clustering...\n")
  }

  # Call longworkR clustering function
  clusters <- longworkR::cluster_career_trajectories(
    career_metrics,
    n_clusters = n_clusters,
    features = features,
    method = method,
    memory_fraction = memory_fraction,
    verbose = verbose
  )

  if (verbose) {
    n_clusters_found <- data.table::uniqueN(clusters$cluster_assignments$cluster)
    cat("  Identified", n_clusters_found, "career clusters\n")

    # Show cluster sizes
    cluster_sizes <- clusters$cluster_assignments[, .N, by = cluster_label_it][order(-N)]
    cat("\n  Cluster sizes:\n")
    print(cluster_sizes)
    cat("\n")
  }

  return(clusters)
}
