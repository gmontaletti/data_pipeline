# _targets.R
# Comprehensive targets pipeline for longworkR dashboard preprocessing
# Author: Giampaolo Montaletti (giampaolo.montaletti@gmail.com)
# Date: 2025-10-23
#
# This pipeline orchestrates the complete 9-phase dashboard data preprocessing
# workflow using modular functions from R/ directory.

library(targets)
library(tarchetypes)

# Set target options
tar_option_set(
  packages = c("data.table", "fst", "devtools"),
  format = "qs"  # Default format for faster serialization, override for files
)

# Load custom functions from R/ folder
tar_source("R/")

# Load longworkR package from development source
devtools::load_all("/Users/giampaolomontaletti/Documents/funzioni/longworkR/")

# Define output directory
output_dir <- "output/dashboard"

# Define the pipeline
list(
  # ============================================================================
  # PHASE 1: Data Loading & Consolidation
  # ============================================================================
  # Load raw data and consolidate overlapping/adjacent employment spells

  tar_target(
    name = raw_data,
    command = load_and_consolidate_data(
      file.path(
        ifelse(Sys.getenv("SHARED_DATA_DIR") == "",
               "~/Documents/funzioni/shared_data",
               Sys.getenv("SHARED_DATA_DIR")),
        "raw/indice.fst"
      )
    ),
    format = "qs"
  ),

  tar_target(
    name = data_with_geo,
    command = add_geo_and_ateco(raw_data),
    format = "qs"
  ),

  # Phase 1.3: Recode obsolete CPI codes
  tar_target(
    name = data_with_geo_clean,
    command = {
      # Recode obsolete Codogno CPI code to current code
      # C816C530490 (404 transactions) → C816C000692 (current code)
      data_with_geo[cpi_code == "C816C530490", cpi_code := "C816C000692"]
      data_with_geo[cpi_code == "C816C000692", cpi_name := "CPI DELLA PROVINCIA DI LODI - UFFICIO PERIFERICO CODOGNO"]
      data_with_geo
    },
    format = "qs"
  ),

  # ============================================================================
  # PHASE 2: Load Classifiers
  # ============================================================================
  # Load contract types, professions, and sector classification lookup tables

  tar_target(
    name = classifiers,
    command = load_classifiers(use_mock = FALSE),
    format = "qs"
  ),

  # ============================================================================
  # PHASE 3: Compute Transitions (5 sub-phases)
  # ============================================================================
  # Extract demographics and compute both closed and open employment transitions

  # Phase 3.1: Extract person-level demographics
  tar_target(
    name = demographics,
    command = extract_demographics(data_with_geo_clean),
    format = "qs"
  ),

  # Phase 3.2: Compute closed transitions (employment → employment)
  tar_target(
    name = closed_transitions,
    command = compute_closed_transitions(data_with_geo_clean),
    format = "qs"
  ),

  # Phase 3.3: Compute open transitions (employment → unemployment)
  tar_target(
    name = open_transitions,
    command = compute_open_transitions(data_with_geo_clean, as.Date("2024-10-20")),
    format = "qs"
  ),

  # Phase 3.4: Extract unemployment spells with DID/POL
  tar_target(
    name = unemployment_spells,
    command = {
      data_with_geo_clean[arco == 0 & (!is.na(did_attribute) | !is.na(pol_attribute)), .(
        cf,
        unemp_start = inizio,
        unemp_end = fine,
        did_attribute,
        did_match_quality,
        pol_attribute,
        pol_match_quality
      )]
    },
    format = "qs"
  ),

  # Phase 3.5: Combine closed and open transitions
  tar_target(
    name = transitions_combined,
    command = rbindlist(list(closed_transitions, open_transitions), fill = TRUE),
    format = "qs"
  ),

  # Phase 3.6: Add DID/POL policy support flags to transitions
  tar_target(
    name = transitions_with_did_pol,
    command = add_did_pol_to_transitions(transitions_combined, unemployment_spells),
    format = "qs"
  ),

  # Phase 3.7: Add demographics to transitions
  tar_target(
    name = transitions,
    command = merge(transitions_with_did_pol, demographics[, .(cf, sesso, cleta, istruzione)], by = "cf", all.x = TRUE),
    format = "qs"
  ),

  # ============================================================================
  # PHASE 4: Create Transition Matrices
  # ============================================================================
  # Aggregate transitions into matrices by various dimensions

  tar_target(
    name = transition_matrices,
    command = create_transition_matrices(transitions),
    format = "qs"
  ),

  # ============================================================================
  # PHASE 4 (45-DAY LAG): Transition Matrices with 45+ Day Unemployment Gap
  # ============================================================================
  # Create transition matrices excluding rapid transitions (≤45 days unemployment)
  # This reveals more meaningful career transitions by filtering out immediate rehires

  # Filter transitions with unemployment duration > 45 days
  tar_target(
    name = transitions_8day,
    command = transitions[unemployment_duration > 45 | is.na(unemployment_duration)],
    format = "qs"
  ),

  # Create transition matrices for 45-day lag data
  tar_target(
    name = transition_matrices_8day,
    command = create_transition_matrices(transitions_8day),
    format = "qs"
  ),

  # ============================================================================
  # PHASE 4.5: Enrich Transition Matrices with Labels and Analysis
  # ============================================================================
  # Add human-readable labels, create visualizations, and compute statistics
  # for profession and sector transition matrices

  # Extract profession and sector matrices from transition_matrices
  tar_target(
    name = profession_matrix_raw,
    command = transition_matrices$profession,
    format = "qs"
  ),

  tar_target(
    name = sector_matrix_raw,
    command = transition_matrices$sector,
    format = "qs"
  ),

  # Enrich with human-readable labels from classifiers
  tar_target(
    name = profession_transitions_labeled,
    command = enrich_transition_matrix_with_labels(
      profession_matrix_raw,
      classifiers$professioni,
      code_col = "name",
      label_col = "nome_breve"
    ),
    format = "qs"
  ),

  tar_target(
    name = sector_transitions_labeled,
    command = enrich_transition_matrix_with_labels(
      sector_matrix_raw,
      classifiers$settori,
      code_col = "ateco",
      label_col = "nome"
    ),
    format = "qs"
  ),

  # Convert to R matrix format for matrix operations
  tar_target(
    name = profession_matrix,
    command = convert_transitions_to_matrix(profession_transitions_labeled),
    format = "qs"
  ),

  tar_target(
    name = sector_matrix,
    command = convert_transitions_to_matrix(sector_transitions_labeled),
    format = "qs"
  ),

  # Create network visualizations
  tar_target(
    name = profession_network_plot,
    command = create_transition_network_plot(
      profession_transitions_labeled,
      title = "Profession Transition Network",
      top_n = 50,
      min_transitions = 5
    ),
    format = "qs"
  ),

  tar_target(
    name = sector_network_plot,
    command = create_transition_network_plot(
      sector_transitions_labeled,
      title = "Economic Sector Transition Network",
      top_n = 50,
      min_transitions = 5
    ),
    format = "qs"
  ),

  # Compute summary statistics
  tar_target(
    name = profession_summary_stats,
    command = compute_transition_summary_stats(
      profession_transitions_labeled,
      top_k = 10
    ),
    format = "qs"
  ),

  tar_target(
    name = sector_summary_stats,
    command = compute_transition_summary_stats(
      sector_transitions_labeled,
      top_k = 10
    ),
    format = "qs"
  ),

  # ============================================================================
  # PHASE 4.5 (45-DAY LAG): Enrich 45-Day Lag Matrices with Labels and Analysis
  # ============================================================================
  # Process 45-day lag transition matrices with labels, visualizations, and statistics
  # These matrices exclude rapid job transitions (≤45 days) to focus on meaningful career changes

  # Extract profession and sector matrices from 45-day lag transition_matrices
  tar_target(
    name = profession_matrix_raw_8day,
    command = transition_matrices_8day$profession,
    format = "qs"
  ),

  tar_target(
    name = sector_matrix_raw_8day,
    command = transition_matrices_8day$sector,
    format = "qs"
  ),

  # Enrich with human-readable labels from classifiers
  tar_target(
    name = profession_transitions_labeled_8day,
    command = enrich_transition_matrix_with_labels(
      profession_matrix_raw_8day,
      classifiers$professioni,
      code_col = "name",
      label_col = "nome_breve"
    ),
    format = "qs"
  ),

  tar_target(
    name = sector_transitions_labeled_8day,
    command = enrich_transition_matrix_with_labels(
      sector_matrix_raw_8day,
      classifiers$settori,
      code_col = "ateco",
      label_col = "nome"
    ),
    format = "qs"
  ),

  # Convert to R matrix format for matrix operations
  tar_target(
    name = profession_matrix_8day,
    command = convert_transitions_to_matrix(profession_transitions_labeled_8day),
    format = "qs"
  ),

  tar_target(
    name = sector_matrix_8day,
    command = convert_transitions_to_matrix(sector_transitions_labeled_8day),
    format = "qs"
  ),

  # Create network visualizations for 8-day lag data
  tar_target(
    name = profession_network_plot_8day,
    command = create_transition_network_plot(
      profession_transitions_labeled_8day,
      title = "Profession Transition Network (8+ Day Gap)",
      top_n = 50,
      min_transitions = 5
    ),
    format = "qs"
  ),

  tar_target(
    name = sector_network_plot_8day,
    command = create_transition_network_plot(
      sector_transitions_labeled_8day,
      title = "Economic Sector Transition Network (8+ Day Gap)",
      top_n = 50,
      min_transitions = 5
    ),
    format = "qs"
  ),

  # Compute summary statistics for 8-day lag data
  tar_target(
    name = profession_summary_stats_8day,
    command = compute_transition_summary_stats(
      profession_transitions_labeled_8day,
      top_k = 10
    ),
    format = "qs"
  ),

  tar_target(
    name = sector_summary_stats_8day,
    command = compute_transition_summary_stats(
      sector_transitions_labeled_8day,
      top_k = 10
    ),
    format = "qs"
  ),

  # ============================================================================
  # PHASE 5: Career Metrics and Survival Analysis
  # ============================================================================
  # Compute survival curves and comprehensive career metrics

  tar_target(
    name = career_results,
    command = compute_survival_and_metrics(data_with_geo_clean),
    format = "qs"
  ),

  # Extract components from career analysis results
  tar_target(
    name = survival_curves,
    command = career_results$survival_curves,
    format = "qs"
  ),

  tar_target(
    name = career_metrics,
    command = career_results$career_metrics,
    format = "qs"
  ),

  # ============================================================================
  # PHASE 6: Person-Level Data with Career Clustering
  # ============================================================================
  # Cluster career trajectories and create comprehensive person-level dataset

  tar_target(
    name = career_clusters,
    command = cluster_trajectories(career_metrics),
    format = "qs"
  ),

  tar_target(
    name = person_data,
    command = create_person_data(
      demographics,
      transitions,
      career_metrics,
      career_clusters
    ),
    format = "qs"
  ),

  # ============================================================================
  # PHASE 7: Geographic Aggregations
  # ============================================================================
  # Create geographic summaries by area and CPI

  tar_target(
    name = geo_summaries,
    command = create_geo_summaries(person_data, transitions),
    format = "qs"
  ),

  # Extract geographic summary components
  tar_target(
    name = geo_summary,
    command = geo_summaries$area,
    format = "qs"
  ),

  tar_target(
    name = geo_summary_cpi,
    command = geo_summaries$cpi,
    format = "qs"
  ),

  # ============================================================================
  # PHASE 8: Time Series Aggregations
  # ============================================================================
  # Create monthly time series by area and CPI

  tar_target(
    name = monthly_data,
    command = create_monthly_timeseries(transitions),
    format = "qs"
  ),

  # Extract monthly time series components
  tar_target(
    name = monthly_timeseries,
    command = monthly_data$area,
    format = "qs"
  ),

  tar_target(
    name = monthly_timeseries_cpi,
    command = monthly_data$cpi,
    format = "qs"
  ),

  # ============================================================================
  # PHASE 9: Policy Summary
  # ============================================================================
  # Analyze DID/POL policy support effectiveness

  tar_target(
    name = policy_summary,
    command = create_policy_summary(transitions),
    format = "qs"
  ),

  # ============================================================================
  # PHASE 9.5: Policy Coverage Precomputations (for Dashboard Performance)
  # ============================================================================
  # Precompute policy coverage aggregates to eliminate on-the-fly computations
  # in the dashboard's "Copertura politiche" tab. This dramatically improves
  # performance by moving expensive aggregations from runtime to pipeline.

  # Monthly time series with DID/POL coverage rates
  tar_target(
    name = policy_timeseries_monthly,
    command = create_policy_timeseries_aggregates(transitions, freq = "month"),
    format = "qs"
  ),

  # Quarterly time series with DID/POL coverage rates
  tar_target(
    name = policy_timeseries_quarterly,
    command = create_policy_timeseries_aggregates(transitions, freq = "quarter"),
    format = "qs"
  ),

  # Coverage rates by demographics (gender, age, education)
  tar_target(
    name = policy_coverage_demographics,
    command = create_policy_coverage_by_demographics(transitions),
    format = "qs"
  ),

  # Coverage rates by geography (area and CPI)
  tar_target(
    name = policy_coverage_geography,
    command = create_policy_coverage_by_geography(transitions),
    format = "qs"
  ),

  # Pre-filtered unemployment duration data for box plots
  tar_target(
    name = policy_duration_data,
    command = create_policy_duration_distributions(transitions),
    format = "qs"
  ),

  # ============================================================================
  # PHASE 10: File Outputs
  # ============================================================================
  # Write final datasets to FST and RDS files for dashboard consumption

  # Write large transition dataset to FST (efficient columnar format)
  tar_target(
    name = output_transitions,
    command = {
      dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
      path <- file.path(output_dir, "transitions.fst")
      write_fst(transitions, path, compress = 85)
      path
    },
    format = "file"
  ),

  # Write person-level data to FST
  tar_target(
    name = output_person_data,
    command = {
      path <- file.path(output_dir, "person_data.fst")
      write_fst(person_data, path, compress = 85)
      path
    },
    format = "file"
  ),

  # Write monthly time series to FST (area aggregation)
  tar_target(
    name = output_monthly_timeseries,
    command = {
      path <- file.path(output_dir, "monthly_timeseries.fst")
      write_fst(monthly_timeseries, path, compress = 85)
      path
    },
    format = "file"
  ),

  # Write monthly time series to FST (CPI aggregation)
  tar_target(
    name = output_monthly_timeseries_cpi,
    command = {
      path <- file.path(output_dir, "monthly_timeseries_cpi.fst")
      write_fst(monthly_timeseries_cpi, path, compress = 85)
      path
    },
    format = "file"
  ),

  # Write smaller lookup and summary objects to RDS
  tar_target(
    name = output_rds_files,
    command = {
      # Ensure output directory exists
      dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

      # Save all RDS files
      saveRDS(classifiers, file.path(output_dir, "classifiers.rds"))
      saveRDS(survival_curves, file.path(output_dir, "survival_curves.rds"))
      saveRDS(transition_matrices, file.path(output_dir, "transition_matrices.rds"))
      saveRDS(geo_summary, file.path(output_dir, "geo_summary.rds"))
      saveRDS(geo_summary_cpi, file.path(output_dir, "geo_summary_cpi.rds"))
      saveRDS(policy_summary, file.path(output_dir, "policy_summary.rds"))

      # Save precomputed policy aggregates for dashboard performance
      saveRDS(policy_timeseries_monthly, file.path(output_dir, "policy_timeseries_monthly.rds"))
      saveRDS(policy_timeseries_quarterly, file.path(output_dir, "policy_timeseries_quarterly.rds"))
      saveRDS(policy_coverage_demographics, file.path(output_dir, "policy_coverage_demographics.rds"))
      saveRDS(policy_coverage_geography, file.path(output_dir, "policy_coverage_geography.rds"))
      saveRDS(policy_duration_data, file.path(output_dir, "policy_duration_data.rds"))

      # Save profession and sector transition files (separate) - both formats
      saveRDS(profession_transitions_labeled, file.path(output_dir, "profession_transitions.rds"))
      saveRDS(sector_transitions_labeled, file.path(output_dir, "sector_transitions.rds"))
      saveRDS(profession_matrix, file.path(output_dir, "profession_matrix.rds"))
      saveRDS(sector_matrix, file.path(output_dir, "sector_matrix.rds"))

      # Save profession and sector summary statistics
      saveRDS(profession_summary_stats, file.path(output_dir, "profession_summary_stats.rds"))
      saveRDS(sector_summary_stats, file.path(output_dir, "sector_summary_stats.rds"))

      # Save 8-day lag transition matrices and statistics
      saveRDS(transition_matrices_8day, file.path(output_dir, "transition_matrices_8day.rds"))
      saveRDS(profession_transitions_labeled_8day, file.path(output_dir, "profession_transitions_8day.rds"))
      saveRDS(sector_transitions_labeled_8day, file.path(output_dir, "sector_transitions_8day.rds"))
      saveRDS(profession_matrix_8day, file.path(output_dir, "profession_matrix_8day.rds"))
      saveRDS(sector_matrix_8day, file.path(output_dir, "sector_matrix_8day.rds"))
      saveRDS(profession_summary_stats_8day, file.path(output_dir, "profession_summary_stats_8day.rds"))
      saveRDS(sector_summary_stats_8day, file.path(output_dir, "sector_summary_stats_8day.rds"))

      # Return vector of file paths for targets to track
      c(
        file.path(output_dir, "classifiers.rds"),
        file.path(output_dir, "survival_curves.rds"),
        file.path(output_dir, "transition_matrices.rds"),
        file.path(output_dir, "geo_summary.rds"),
        file.path(output_dir, "geo_summary_cpi.rds"),
        file.path(output_dir, "policy_summary.rds"),
        file.path(output_dir, "policy_timeseries_monthly.rds"),
        file.path(output_dir, "policy_timeseries_quarterly.rds"),
        file.path(output_dir, "policy_coverage_demographics.rds"),
        file.path(output_dir, "policy_coverage_geography.rds"),
        file.path(output_dir, "policy_duration_data.rds"),
        file.path(output_dir, "profession_transitions.rds"),
        file.path(output_dir, "sector_transitions.rds"),
        file.path(output_dir, "profession_matrix.rds"),
        file.path(output_dir, "sector_matrix.rds"),
        file.path(output_dir, "profession_summary_stats.rds"),
        file.path(output_dir, "sector_summary_stats.rds"),
        file.path(output_dir, "transition_matrices_8day.rds"),
        file.path(output_dir, "profession_transitions_8day.rds"),
        file.path(output_dir, "sector_transitions_8day.rds"),
        file.path(output_dir, "profession_matrix_8day.rds"),
        file.path(output_dir, "sector_matrix_8day.rds"),
        file.path(output_dir, "profession_summary_stats_8day.rds"),
        file.path(output_dir, "sector_summary_stats_8day.rds")
      )
    },
    format = "file"
  ),

  # Write network visualization plots to PNG files
  tar_target(
    name = output_network_plots,
    command = {
      # Create plots subdirectory
      plots_dir <- file.path(output_dir, "plots")
      dir.create(plots_dir, recursive = TRUE, showWarnings = FALSE)

      # Save profession network plot
      prof_plot_path <- file.path(plots_dir, "profession_network.png")
      ggplot2::ggsave(
        filename = prof_plot_path,
        plot = profession_network_plot,
        width = 12,
        height = 10,
        dpi = 300,
        bg = "white"
      )

      # Save sector network plot
      sector_plot_path <- file.path(plots_dir, "sector_network.png")
      ggplot2::ggsave(
        filename = sector_plot_path,
        plot = sector_network_plot,
        width = 12,
        height = 10,
        dpi = 300,
        bg = "white"
      )

      # Save 8-day lag profession network plot
      prof_plot_path_8day <- file.path(plots_dir, "profession_network_8day.png")
      ggplot2::ggsave(
        filename = prof_plot_path_8day,
        plot = profession_network_plot_8day,
        width = 12,
        height = 10,
        dpi = 300,
        bg = "white"
      )

      # Save 8-day lag sector network plot
      sector_plot_path_8day <- file.path(plots_dir, "sector_network_8day.png")
      ggplot2::ggsave(
        filename = sector_plot_path_8day,
        plot = sector_network_plot_8day,
        width = 12,
        height = 10,
        dpi = 300,
        bg = "white"
      )

      # Return vector of file paths for targets to track
      c(prof_plot_path, sector_plot_path, prof_plot_path_8day, sector_plot_path_8day)
    },
    format = "file"
  ),

  # ============================================================================
  # PHASE 11: Validation and Summary Report
  # ============================================================================
  # Generate comprehensive validation report of pipeline execution

  tar_target(
    name = validation_report,
    command = {
      cat("\n")
      cat("==========================================================\n")
      cat("   Dashboard Data Preprocessing Pipeline - Validation\n")
      cat("==========================================================\n\n")

      cat("PHASE 1: Data Loading & Consolidation\n")
      cat("--------------------------------------\n")
      cat("  Raw records loaded:", nrow(data_with_geo), "\n")
      cat("  Unique individuals:", data_with_geo[, uniqueN(cf)], "\n")
      cat("  Employment spells:", data_with_geo[, .N], "\n")
      cat("  Date range:",
          format(data_with_geo[, min(inizio, na.rm = TRUE)], "%Y-%m-%d"), "to",
          format(data_with_geo[, max(fine, na.rm = TRUE)], "%Y-%m-%d"), "\n\n")

      cat("PHASE 2: Classifiers\n")
      cat("--------------------\n")
      cat("  Contract types:", nrow(classifiers$tipi_contratto), "\n")
      cat("  Professions:", nrow(classifiers$professioni), "\n")
      cat("  Sectors:", nrow(classifiers$settori), "\n\n")

      cat("PHASE 3: Transitions\n")
      cat("--------------------\n")
      cat("  Total transitions:", nrow(transitions), "\n")
      cat("  Closed transitions (emp→emp):",
          transitions[, sum(is_closed, na.rm = TRUE)], "\n")
      cat("  Open transitions (emp→unemp):",
          transitions[, sum(is_open, na.rm = TRUE)], "\n")
      cat("  With DID support:",
          transitions[, sum(has_did, na.rm = TRUE)], "\n")
      cat("  With POL support:",
          transitions[, sum(has_pol, na.rm = TRUE)], "\n")
      cat("  Average unemployment duration:",
          round(transitions[, mean(unemployment_duration, na.rm = TRUE)], 1),
          "days\n\n")

      cat("PHASE 4: Transition Matrices\n")
      cat("----------------------------\n")
      cat("  Matrix types created:", length(transition_matrices), "\n\n")

      cat("PHASE 4.5: Profession & Sector Transition Analysis\n")
      cat("---------------------------------------------------\n")
      cat("  Profession transitions:\n")
      cat("    Total transition pairs:", nrow(profession_transitions_labeled), "\n")
      cat("    Unique professions (origin):",
          profession_transitions_labeled[, uniqueN(from)], "\n")
      cat("    Unique professions (destination):",
          profession_transitions_labeled[, uniqueN(to)], "\n")
      cat("    Total transitions:", sum(profession_transitions_labeled$N), "\n")
      cat("    Mobility index:", profession_summary_stats$mobility_index, "\n")
      cat("    Top 3 profession transitions:\n")
      for (i in 1:min(3, nrow(profession_summary_stats$top_transitions))) {
        trans <- profession_summary_stats$top_transitions[i]
        cat(sprintf("      %s → %s: %d (%.1f%%)\n",
                    ifelse(is.na(trans$from_label), trans$from, trans$from_label),
                    ifelse(is.na(trans$to_label), trans$to, trans$to_label),
                    trans$N,
                    trans$percentage))
      }
      cat("\n")
      cat("  Sector transitions:\n")
      cat("    Total transition pairs:", nrow(sector_transitions_labeled), "\n")
      cat("    Unique sectors (origin):",
          sector_transitions_labeled[, uniqueN(from)], "\n")
      cat("    Unique sectors (destination):",
          sector_transitions_labeled[, uniqueN(to)], "\n")
      cat("    Total transitions:", sum(sector_transitions_labeled$N), "\n")
      cat("    Mobility index:", sector_summary_stats$mobility_index, "\n")
      cat("    Top 3 sector transitions:\n")
      for (i in 1:min(3, nrow(sector_summary_stats$top_transitions))) {
        trans <- sector_summary_stats$top_transitions[i]
        cat(sprintf("      %s → %s: %d (%.1f%%)\n",
                    ifelse(is.na(trans$from_label), trans$from, trans$from_label),
                    ifelse(is.na(trans$to_label), trans$to, trans$to_label),
                    trans$N,
                    trans$percentage))
      }
      cat("\n")
      cat("  Matrix formats:\n")
      cat("    Profession matrix dimensions:", nrow(profession_matrix), "x", ncol(profession_matrix), "\n")
      cat("    Sector matrix dimensions:", nrow(sector_matrix), "x", ncol(sector_matrix), "\n")
      cat("    Matrix sparsity (profession):",
          round(100 * sum(profession_matrix == 0) / length(profession_matrix), 1), "% zeros\n")
      cat("    Matrix sparsity (sector):",
          round(100 * sum(sector_matrix == 0) / length(sector_matrix), 1), "% zeros\n")
      cat("\n")

      cat("PHASE 4.5 (8-DAY LAG): 8+ Day Gap Transition Analysis\n")
      cat("-----------------------------------------------------\n")
      cat("  Transitions with 8+ day gap:", nrow(transitions_8day), "\n")
      cat("  Percentage of total transitions:",
          round(100 * nrow(transitions_8day) / nrow(transitions), 1), "%\n")
      cat("  Profession transitions (8-day):\n")
      cat("    Total transition pairs:", nrow(profession_transitions_labeled_8day), "\n")
      cat("    Unique professions (origin):",
          profession_transitions_labeled_8day[, uniqueN(from)], "\n")
      cat("    Unique professions (destination):",
          profession_transitions_labeled_8day[, uniqueN(to)], "\n")
      cat("    Total transitions:", sum(profession_transitions_labeled_8day$N), "\n")
      cat("    Mobility index:", profession_summary_stats_8day$mobility_index, "\n")
      cat("    Top 3 profession transitions (8-day):\n")
      for (i in 1:min(3, nrow(profession_summary_stats_8day$top_transitions))) {
        trans <- profession_summary_stats_8day$top_transitions[i]
        cat(sprintf("      %s → %s: %d (%.1f%%)\n",
                    ifelse(is.na(trans$from_label), trans$from, trans$from_label),
                    ifelse(is.na(trans$to_label), trans$to, trans$to_label),
                    trans$N,
                    trans$percentage))
      }
      cat("\n")
      cat("  Sector transitions (8-day):\n")
      cat("    Total transition pairs:", nrow(sector_transitions_labeled_8day), "\n")
      cat("    Unique sectors (origin):",
          sector_transitions_labeled_8day[, uniqueN(from)], "\n")
      cat("    Unique sectors (destination):",
          sector_transitions_labeled_8day[, uniqueN(to)], "\n")
      cat("    Total transitions:", sum(sector_transitions_labeled_8day$N), "\n")
      cat("    Mobility index:", sector_summary_stats_8day$mobility_index, "\n")
      cat("    Top 3 sector transitions (8-day):\n")
      for (i in 1:min(3, nrow(sector_summary_stats_8day$top_transitions))) {
        trans <- sector_summary_stats_8day$top_transitions[i]
        cat(sprintf("      %s → %s: %d (%.1f%%)\n",
                    ifelse(is.na(trans$from_label), trans$from, trans$from_label),
                    ifelse(is.na(trans$to_label), trans$to, trans$to_label),
                    trans$N,
                    trans$percentage))
      }
      cat("\n")
      cat("  Matrix formats (8-day):\n")
      cat("    Profession matrix dimensions:", nrow(profession_matrix_8day), "x", ncol(profession_matrix_8day), "\n")
      cat("    Sector matrix dimensions:", nrow(sector_matrix_8day), "x", ncol(sector_matrix_8day), "\n")
      cat("    Matrix sparsity (profession):",
          round(100 * sum(profession_matrix_8day == 0) / length(profession_matrix_8day), 1), "% zeros\n")
      cat("    Matrix sparsity (sector):",
          round(100 * sum(sector_matrix_8day == 0) / length(sector_matrix_8day), 1), "% zeros\n")
      cat("\n")

      cat("PHASE 5: Career Metrics & Survival\n")
      cat("----------------------------------\n")
      cat("  Individuals with metrics:", nrow(career_metrics), "\n")
      cat("  Contract types in survival:",
          length(unique(survival_curves$survival_table$group)), "\n")
      cat("  Average employment stability:",
          round(career_metrics[, mean(employment_stability_index, na.rm = TRUE)], 3), "\n\n")

      cat("PHASE 6: Career Clustering\n")
      cat("--------------------------\n")
      cat("  Individuals clustered:", nrow(person_data), "\n")
      cat("  Career clusters identified:",
          person_data[, uniqueN(cluster_label_it)], "\n")
      cat("  Cluster distribution:\n")
      cluster_dist <- person_data[, .N, by = cluster_label_it][order(-N)]
      for (i in 1:min(5, nrow(cluster_dist))) {
        cat(sprintf("    %s: %d (%.1f%%)\n",
                    cluster_dist[i, cluster_label_it],
                    cluster_dist[i, N],
                    100 * cluster_dist[i, N] / nrow(person_data)))
      }
      cat("\n")

      cat("PHASE 7: Geographic Aggregations\n")
      cat("--------------------------------\n")
      cat("  Areas:", geo_summary[, uniqueN(area)], "\n")
      cat("  CPI zones:", geo_summary_cpi[, uniqueN(cpi_code)], "\n\n")

      cat("PHASE 8: Time Series\n")
      cat("--------------------\n")
      cat("  Monthly periods (area):", nrow(monthly_timeseries), "\n")
      cat("  Monthly periods (CPI):", nrow(monthly_timeseries_cpi), "\n")
      if (nrow(monthly_timeseries) > 0 && "month" %in% names(monthly_timeseries)) {
        min_month <- min(monthly_timeseries$month, na.rm = TRUE)
        max_month <- max(monthly_timeseries$month, na.rm = TRUE)
        cat("  Time range:",
            format(min_month, "%Y-%m"), "to",
            format(max_month, "%Y-%m"), "\n")
      }
      cat("\n")

      cat("PHASE 9: Policy Summary\n")
      cat("-----------------------\n")
      cat("  Policy groups:", nrow(policy_summary), "\n\n")

      cat("PHASE 10: Output Files\n")
      cat("----------------------\n")
      cat("  Output directory:", output_dir, "\n")
      cat("  Transitions file:", file.exists(output_transitions), "\n")
      cat("  Person data file:", file.exists(output_person_data), "\n")
      cat("  Monthly timeseries (area):",
          file.exists(output_monthly_timeseries), "\n")
      cat("  Monthly timeseries (CPI):",
          file.exists(output_monthly_timeseries_cpi), "\n")
      cat("  RDS files:", all(file.exists(output_rds_files)), "\n")
      cat("  Network plots:", all(file.exists(output_network_plots)), "\n")
      cat("    - Profession network: plots/profession_network.png\n")
      cat("    - Sector network: plots/sector_network.png\n")
      cat("    - Profession network (8-day): plots/profession_network_8day.png\n")
      cat("    - Sector network (8-day): plots/sector_network_8day.png\n")

      # Calculate total output size
      all_files <- c(output_transitions, output_person_data,
                     output_monthly_timeseries, output_monthly_timeseries_cpi,
                     output_rds_files, output_network_plots)
      total_size_mb <- sum(file.info(all_files)$size) / 1024^2
      cat("  Total output size:", round(total_size_mb, 1), "MB\n\n")

      cat("==========================================================\n")
      cat("  Pipeline completed successfully!\n")
      cat("==========================================================\n\n")

      # Return summary statistics for programmatic access
      list(
        success = TRUE,
        timestamp = Sys.time(),
        data_summary = list(
          n_rows = nrow(data_with_geo),
          n_individuals = data_with_geo[, uniqueN(cf)],
          n_transitions = nrow(transitions),
          n_persons_with_metrics = nrow(person_data),
          n_clusters = person_data[, uniqueN(cluster_label_it)]
        ),
        transition_summary = list(
          closed_transitions = transitions[, sum(is_closed, na.rm = TRUE)],
          open_transitions = transitions[, sum(is_open, na.rm = TRUE)],
          with_did = transitions[, sum(has_did, na.rm = TRUE)],
          with_pol = transitions[, sum(has_pol, na.rm = TRUE)]
        ),
        output_summary = list(
          output_dir = output_dir,
          total_size_mb = round(total_size_mb, 1),
          files_created = length(all_files)
        )
      )
    }
  )
)
