# _targets.R
# Comprehensive targets pipeline for employment data preprocessing
# Author: Giampaolo Montaletti (giampaolo.montaletti@gmail.com)
# Date: 2025-01-05
#
# Pipeline Structure:
# Phase 0-1: Load raw data and prepare (harmonize, standardize)
# Phase 2: Consolidate FULL dataset (vecshift + longworkR) BEFORE filtering
# Phase 3: Fork by filtering consolidated data (residence/workplace)
# Phase 4-12: Process RESIDENCE BRANCH ONLY (for now)
#
# Note: Workplace branch infrastructure ready but not active

library(targets)
library(tarchetypes)

# Set target options
tar_option_set(
  packages = c("data.table", "fst", "devtools"),
  format = "qs"  # Default format for faster serialization
)

# Load custom functions from R/ folder
tar_source("R/")

# ============================================================================
# CONFIGURATION - Test Mode Settings
# ============================================================================
# Set test_mode=TRUE for fast testing with sample data (10K CFs by default)
# Set test_mode=FALSE for full production pipeline

test_mode <- TRUE          # Toggle test mode (TRUE = use sample, FALSE = use full data)
sample_size <- 10000      # Number of CFs to sample (only used if test_mode=TRUE)
sample_seed <- 42         # Random seed for reproducible sampling

# Load development packages
devtools::load_all("/Users/giampaolomontaletti/Documents/funzioni/longworkR/")
devtools::load_all("/Users/giampaolomontaletti/Documents/funzioni/vecshift/")

# Define shared data directory
shared_data_dir <- ifelse(Sys.getenv("SHARED_DATA_DIR") == "",
                          "~/Documents/funzioni/shared_data",
                          Sys.getenv("SHARED_DATA_DIR"))

# Define output directories
output_dir <- "output/dashboard"
output_dir_workplace <- "output/dashboard_workplace"

# Define the pipeline
list(
  # ============================================================================
  # PHASE -1: Generate Sample Data (Only Runs When test_mode=TRUE)
  # ============================================================================
  # Creates stratified sample of CFs and generates sample data files
  # Sample files cached in shared_data/sample/ for reuse

  # Generate stratified sample of CFs
  tar_target(
    name = sample_cf_list,
    command = if (test_mode) {
      create_stratified_sample(
        rap_path = file.path(shared_data_dir, "raw/rap.fst"),
        n_cf = sample_size,
        seed = sample_seed
      )
    } else {
      NULL  # Skip sampling when test_mode=FALSE
    },
    format = "qs"
  ),

  # Create rap_sample.fst
  tar_target(
    name = sample_rap_file,
    command = if (test_mode) {
      filter_contracts_to_sample(
        rap_path = file.path(shared_data_dir, "raw/rap.fst"),
        sampled_cf = sample_cf_list,
        output_path = file.path(shared_data_dir, "sample/rap_sample.fst")
      )
    } else {
      NULL  # Skip when test_mode=FALSE
    },
    format = "file"
  ),

  # Create did_sample.fst
  tar_target(
    name = sample_did_file,
    command = if (test_mode) {
      filter_did_to_sample(
        did_path = file.path(shared_data_dir, "raw/did.fst"),
        sampled_cf = sample_cf_list,
        output_path = file.path(shared_data_dir, "sample/did_sample.fst")
      )
    } else {
      NULL  # Skip when test_mode=FALSE
    },
    format = "file"
  ),

  # Create pol_sample.fst
  tar_target(
    name = sample_pol_file,
    command = if (test_mode) {
      filter_pol_to_sample(
        pol_path = file.path(shared_data_dir, "raw/pol.fst"),
        sampled_cf = sample_cf_list,
        output_path = file.path(shared_data_dir, "sample/pol_sample.fst")
      )
    } else {
      NULL  # Skip when test_mode=FALSE
    },
    format = "file"
  ),

  # ============================================================================
  # PHASE 0: Load Raw Data
  # ============================================================================
  # Load truly raw data files before any processing
  # Uses test_mode to switch between full data and sample data

  tar_target(
    name = raw_contracts,
    command = load_raw_contracts(test_mode = test_mode),
    format = "fst"  # FST preserves IDate type correctly
  ),

  tar_target(
    name = raw_did,
    command = load_raw_did(test_mode = test_mode),
    format = "fst"  # FST preserves IDate type correctly
  ),

  tar_target(
    name = raw_pol,
    command = load_raw_pol(test_mode = test_mode),
    format = "fst"  # FST preserves IDate type correctly
  ),

  # ============================================================================
  # PHASE 1: Prepare Data (BEFORE CONSOLIDATION AND FILTERING)
  # ============================================================================
  # Load classifiers, harmonize codes, standardize education
  # NO geographic filtering yet - that happens in Phase 3

  tar_target(
    name = classifiers,
    command = load_classifiers(use_mock = FALSE),
    format = "qs"
  ),

  # Prepare contracts: harmonize, filter to Lombardia-connected, standardize
  # This keeps contracts where EITHER residence OR workplace is in Lombardia
  # Then later filtering (Phase 3) will split into residence vs workplace branches
  tar_target(
    name = contracts_prepared_unfiltered,
    command = prepare_contracts(raw_contracts, classifiers),
    format = "fst"  # FST preserves IDate type correctly
  ),

  # ============================================================================
  # PHASE 2: Consolidate FULL Dataset (BEFORE FILTERING)
  # ============================================================================
  # Apply vecshift → unemployment → DID/POL → longworkR → geo enrichment
  # This runs ONCE on the complete dataset, preserving both location columns
  # Geographic inheritance for unemployment spells happens in filter_by_location()

  tar_target(
    name = data_consolidated,
    command = consolidate_and_enrich(
      contracts_prepared_unfiltered,
      raw_did,
      raw_pol,
      min_date = as.IDate("2021-01-01"),
      max_date = as.IDate("2024-12-31")
    ),
    format = "fst"  # FST preserves IDate type correctly
  ),

  # Recode obsolete CPI codes
  tar_target(
    name = data_consolidated_clean,
    command = {
      data.table::setDT(data_consolidated)  # Ensure it's data.table after FST load
      # Recode obsolete Codogno CPI code to current code
      data_consolidated[cpi_code == "C816C530490", cpi_code := "C816C000692"]
      data_consolidated[cpi_code == "C816C000692",
                       cpi_name := "CPI DELLA PROVINCIA DI LODI - UFFICIO PERIFERICO CODOGNO"]
      data_consolidated
    },
    format = "fst"  # FST preserves IDate type correctly
  ),

  # ============================================================================
  # PHASE 3: FORK - Filter Consolidated Data by Location
  # ============================================================================
  # Filter with geographic inheritance for unemployment spells (Option A)
  # Unemployment spells with NA location inherit from previous employment spell

  tar_target(
    name = data_residence,
    command = filter_by_location(
      data_consolidated_clean,
      filter_by = "residence",
      territoriale = classifiers$territoriale
    ),
    format = "fst"  # FST preserves IDate type correctly
  ),

  # Workplace branch
  tar_target(
    name = data_workplace,
    command = filter_by_location(
      data_consolidated_clean,
      filter_by = "workplace",
      territoriale = classifiers$territoriale
    ),
    format = "fst"  # FST preserves IDate type correctly
  ),

  # ============================================================================
  # PHASE 4-12: RESIDENCE BRANCH - Full Pipeline
  # ============================================================================
  # Extract demographics and compute transitions for residence-based data

  tar_target(
    name = demographics,
    command = {
      data.table::setDT(data_residence)  # Ensure it's data.table after FST load
      extract_demographics(data_residence)
    },
    format = "qs"
  ),

  tar_target(
    name = closed_transitions,
    command = {
      data.table::setDT(data_residence)  # Ensure it's data.table after FST load
      compute_closed_transitions(data_residence)
    },
    format = "qs"
  ),

  tar_target(
    name = open_transitions,
    command = {
      data.table::setDT(data_residence)  # Ensure it's data.table after FST load
      compute_open_transitions(data_residence, data.table::as.IDate("2024-12-31"))
    },
    format = "qs"
  ),

  tar_target(
    name = unemployment_spells,
    command = {
      data.table::setDT(data_residence)  # Ensure it's data.table after FST load
      data_residence[arco == 0 & (!is.na(did_attribute) | !is.na(pol_attribute)), .(
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

  tar_target(
    name = transitions_combined,
    command = rbindlist(list(closed_transitions, open_transitions), fill = TRUE),
    format = "qs"
  ),

  tar_target(
    name = transitions_with_did_pol,
    command = add_did_pol_to_transitions(transitions_combined, unemployment_spells),
    format = "qs"
  ),

  tar_target(
    name = transitions,
    command = merge(transitions_with_did_pol,
                    demographics[, .(cf, sesso, cleta, istruzione)],
                    by = "cf", all.x = TRUE),
    format = "qs"
  ),

  # Transition matrices
  tar_target(
    name = transition_matrices,
    command = create_transition_matrices(transitions),
    format = "qs"
  ),

  # 45-day lag transitions
  tar_target(
    name = transitions_45day,
    command = transitions[unemployment_duration > 45 | is.na(unemployment_duration)],
    format = "qs"
  ),

  tar_target(
    name = transition_matrices_45day,
    command = create_transition_matrices(transitions_45day),
    format = "qs"
  ),

  # Enrich matrices with labels (profession and sector)
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

  tar_target(
    name = profession_transitions_labeled,
    command = enrich_transition_matrix_with_labels(
      profession_matrix_raw,
      classifiers$professioni,
      code_col = "name",
      label_col = "label"
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

  tar_target(
    name = profession_network_plot,
    command = create_transition_network_plot(
      profession_transitions_labeled,
      title = "Profession Transition Network (Residence-Based)",
      top_n = 50,
      min_transitions = 5
    ),
    format = "qs"
  ),

  tar_target(
    name = sector_network_plot,
    command = create_transition_network_plot(
      sector_transitions_labeled,
      title = "Economic Sector Transition Network (Residence-Based)",
      top_n = 50,
      min_transitions = 5
    ),
    format = "qs"
  ),

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

  # 45-day lag enrichment
  tar_target(
    name = profession_matrix_raw_45day,
    command = transition_matrices_45day$profession,
    format = "qs"
  ),

  tar_target(
    name = sector_matrix_raw_45day,
    command = transition_matrices_45day$sector,
    format = "qs"
  ),

  tar_target(
    name = profession_transitions_labeled_45day,
    command = enrich_transition_matrix_with_labels(
      profession_matrix_raw_45day,
      classifiers$professioni,
      code_col = "name",
      label_col = "label"
    ),
    format = "qs"
  ),

  tar_target(
    name = sector_transitions_labeled_45day,
    command = enrich_transition_matrix_with_labels(
      sector_matrix_raw_45day,
      classifiers$settori,
      code_col = "ateco",
      label_col = "nome"
    ),
    format = "qs"
  ),

  tar_target(
    name = profession_matrix_45day,
    command = convert_transitions_to_matrix(profession_transitions_labeled_45day),
    format = "qs"
  ),

  tar_target(
    name = sector_matrix_45day,
    command = convert_transitions_to_matrix(sector_transitions_labeled_45day),
    format = "qs"
  ),

  tar_target(
    name = profession_network_plot_45day,
    command = create_transition_network_plot(
      profession_transitions_labeled_45day,
      title = "Profession Transition Network (45+ Day Gap)",
      top_n = 50,
      min_transitions = 5
    ),
    format = "qs"
  ),

  tar_target(
    name = sector_network_plot_45day,
    command = create_transition_network_plot(
      sector_transitions_labeled_45day,
      title = "Sector Transition Network (45+ Day Gap)",
      top_n = 50,
      min_transitions = 5
    ),
    format = "qs"
  ),

  tar_target(
    name = profession_summary_stats_45day,
    command = compute_transition_summary_stats(
      profession_transitions_labeled_45day,
      top_k = 10
    ),
    format = "qs"
  ),

  tar_target(
    name = sector_summary_stats_45day,
    command = compute_transition_summary_stats(
      sector_transitions_labeled_45day,
      top_k = 10
    ),
    format = "qs"
  ),

  # Career metrics and survival
  tar_target(
    name = career_results,
    command = {
      data.table::setDT(data_residence)  # Ensure it's data.table after FST load
      compute_survival_and_metrics(data_residence)
    },
    format = "qs"
  ),

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

  # Career clustering
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

  # Geographic aggregations
  # Geographic summaries (CPI-based only)
  tar_target(
    name = geo_summary_cpi,
    command = create_geo_summaries(person_data, transitions),
    format = "qs"
  ),

  # Time series (CPI-based only)
  tar_target(
    name = monthly_timeseries_cpi,
    command = create_monthly_timeseries(transitions),
    format = "qs"
  ),

  # Policy summary
  tar_target(
    name = policy_summary,
    command = create_policy_summary(transitions),
    format = "qs"
  ),

  # Policy coverage precomputations
  tar_target(
    name = policy_timeseries_monthly,
    command = create_policy_timeseries_aggregates(transitions, freq = "month"),
    format = "qs"
  ),

  tar_target(
    name = policy_timeseries_quarterly,
    command = create_policy_timeseries_aggregates(transitions, freq = "quarter"),
    format = "qs"
  ),

  tar_target(
    name = policy_coverage_demographics,
    command = create_policy_coverage_by_demographics(transitions),
    format = "qs"
  ),

  tar_target(
    name = policy_coverage_geography,
    command = create_policy_coverage_by_geography(transitions),
    format = "qs"
  ),

  tar_target(
    name = policy_duration_data,
    command = create_policy_duration_distributions(transitions),
    format = "qs"
  ),

  # ============================================================================
  # WORKPLACE BRANCH: Full Transition Pipeline
  # ============================================================================
  # Process workplace-filtered data to create transition matrices for
  # professions and sectors (people working in Lombardy)

  # 1. Extract demographics -----
  tar_target(
    name = demographics_workplace,
    command = {
      data.table::setDT(data_workplace)
      extract_demographics(data_workplace)
    },
    format = "qs"
  ),

  # 2. Compute transitions -----
  tar_target(
    name = closed_transitions_workplace,
    command = {
      data.table::setDT(data_workplace)
      compute_closed_transitions(data_workplace)
    },
    format = "qs"
  ),

  tar_target(
    name = open_transitions_workplace,
    command = {
      data.table::setDT(data_workplace)
      compute_open_transitions(data_workplace, data.table::as.IDate("2024-12-31"))
    },
    format = "qs"
  ),

  tar_target(
    name = unemployment_spells_workplace,
    command = {
      data.table::setDT(data_workplace)
      data_workplace[arco == 0 & (!is.na(did_attribute) | !is.na(pol_attribute)), .(
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

  # 3. Combine and enrich transitions -----
  tar_target(
    name = transitions_combined_workplace,
    command = rbindlist(list(closed_transitions_workplace, open_transitions_workplace), fill = TRUE),
    format = "qs"
  ),

  tar_target(
    name = transitions_with_did_pol_workplace,
    command = add_did_pol_to_transitions(transitions_combined_workplace, unemployment_spells_workplace),
    format = "qs"
  ),

  tar_target(
    name = transitions_workplace,
    command = merge(transitions_with_did_pol_workplace,
                    demographics_workplace[, .(cf, sesso, cleta, istruzione)],
                    by = "cf", all.x = TRUE),
    format = "qs"
  ),

  # 4. Transition matrices (standard) -----
  tar_target(
    name = transition_matrices_workplace,
    command = create_transition_matrices(transitions_workplace),
    format = "qs"
  ),

  # 5. Transition matrices (45-day lag) -----
  tar_target(
    name = transitions_45day_workplace,
    command = transitions_workplace[unemployment_duration > 45 | is.na(unemployment_duration)],
    format = "qs"
  ),

  tar_target(
    name = transition_matrices_45day_workplace,
    command = create_transition_matrices(transitions_45day_workplace),
    format = "qs"
  ),

  # 6. Enrich matrices with labels (standard) -----
  tar_target(
    name = profession_matrix_raw_workplace,
    command = transition_matrices_workplace$profession,
    format = "qs"
  ),

  tar_target(
    name = sector_matrix_raw_workplace,
    command = transition_matrices_workplace$sector,
    format = "qs"
  ),

  tar_target(
    name = profession_transitions_labeled_workplace,
    command = enrich_transition_matrix_with_labels(
      profession_matrix_raw_workplace,
      classifiers$professioni,
      code_col = "name",
      label_col = "label"
    ),
    format = "qs"
  ),

  tar_target(
    name = sector_transitions_labeled_workplace,
    command = enrich_transition_matrix_with_labels(
      sector_matrix_raw_workplace,
      classifiers$settori,
      code_col = "ateco",
      label_col = "nome"
    ),
    format = "qs"
  ),

  tar_target(
    name = profession_matrix_workplace,
    command = convert_transitions_to_matrix(profession_transitions_labeled_workplace),
    format = "qs"
  ),

  tar_target(
    name = sector_matrix_workplace,
    command = convert_transitions_to_matrix(sector_transitions_labeled_workplace),
    format = "qs"
  ),

  # 7. Network plots and summary stats (standard) -----
  tar_target(
    name = profession_network_plot_workplace,
    command = create_transition_network_plot(
      profession_transitions_labeled_workplace,
      title = "Profession Transition Network (Workplace-Based)",
      top_n = 50,
      min_transitions = 5
    ),
    format = "qs"
  ),

  tar_target(
    name = sector_network_plot_workplace,
    command = create_transition_network_plot(
      sector_transitions_labeled_workplace,
      title = "Economic Sector Transition Network (Workplace-Based)",
      top_n = 50,
      min_transitions = 5
    ),
    format = "qs"
  ),

  tar_target(
    name = profession_summary_stats_workplace,
    command = compute_transition_summary_stats(
      profession_transitions_labeled_workplace,
      top_k = 10
    ),
    format = "qs"
  ),

  tar_target(
    name = sector_summary_stats_workplace,
    command = compute_transition_summary_stats(
      sector_transitions_labeled_workplace,
      top_k = 10
    ),
    format = "qs"
  ),

  # 8. Enrich matrices with labels (45-day lag) -----
  tar_target(
    name = profession_matrix_raw_45day_workplace,
    command = transition_matrices_45day_workplace$profession,
    format = "qs"
  ),

  tar_target(
    name = sector_matrix_raw_45day_workplace,
    command = transition_matrices_45day_workplace$sector,
    format = "qs"
  ),

  tar_target(
    name = profession_transitions_labeled_45day_workplace,
    command = enrich_transition_matrix_with_labels(
      profession_matrix_raw_45day_workplace,
      classifiers$professioni,
      code_col = "name",
      label_col = "label"
    ),
    format = "qs"
  ),

  tar_target(
    name = sector_transitions_labeled_45day_workplace,
    command = enrich_transition_matrix_with_labels(
      sector_matrix_raw_45day_workplace,
      classifiers$settori,
      code_col = "ateco",
      label_col = "nome"
    ),
    format = "qs"
  ),

  tar_target(
    name = profession_matrix_45day_workplace,
    command = convert_transitions_to_matrix(profession_transitions_labeled_45day_workplace),
    format = "qs"
  ),

  tar_target(
    name = sector_matrix_45day_workplace,
    command = convert_transitions_to_matrix(sector_transitions_labeled_45day_workplace),
    format = "qs"
  ),

  # 9. Network plots and summary stats (45-day lag) -----
  tar_target(
    name = profession_network_plot_45day_workplace,
    command = create_transition_network_plot(
      profession_transitions_labeled_45day_workplace,
      title = "Profession Transition Network (Workplace, 45+ Day Gap)",
      top_n = 50,
      min_transitions = 5
    ),
    format = "qs"
  ),

  tar_target(
    name = sector_network_plot_45day_workplace,
    command = create_transition_network_plot(
      sector_transitions_labeled_45day_workplace,
      title = "Sector Transition Network (Workplace, 45+ Day Gap)",
      top_n = 50,
      min_transitions = 5
    ),
    format = "qs"
  ),

  tar_target(
    name = profession_summary_stats_45day_workplace,
    command = compute_transition_summary_stats(
      profession_transitions_labeled_45day_workplace,
      top_k = 10
    ),
    format = "qs"
  ),

  tar_target(
    name = sector_summary_stats_45day_workplace,
    command = compute_transition_summary_stats(
      sector_transitions_labeled_45day_workplace,
      top_k = 10
    ),
    format = "qs"
  ),

  # ============================================================================
  # PHASE 10: File Outputs
  # ============================================================================
  # Write final datasets to FST and RDS files for dashboard consumption

  # Write large transition dataset to FST
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

      # Save 45-day lag transition matrices and statistics
      saveRDS(transition_matrices_45day, file.path(output_dir, "transition_matrices_45day.rds"))
      saveRDS(profession_transitions_labeled_45day, file.path(output_dir, "profession_transitions_45day.rds"))
      saveRDS(sector_transitions_labeled_45day, file.path(output_dir, "sector_transitions_45day.rds"))
      saveRDS(profession_matrix_45day, file.path(output_dir, "profession_matrix_45day.rds"))
      saveRDS(sector_matrix_45day, file.path(output_dir, "sector_matrix_45day.rds"))
      saveRDS(profession_summary_stats_45day, file.path(output_dir, "profession_summary_stats_45day.rds"))
      saveRDS(sector_summary_stats_45day, file.path(output_dir, "sector_summary_stats_45day.rds"))

      # Return indicator
      "rds_files_saved"
    },
    format = "qs"
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

      # Save 45-day lag profession network plot
      prof_plot_path_45day <- file.path(plots_dir, "profession_network_45day.png")
      ggplot2::ggsave(
        filename = prof_plot_path_45day,
        plot = profession_network_plot_45day,
        width = 12,
        height = 10,
        dpi = 300,
        bg = "white"
      )

      # Save 45-day lag sector network plot
      sector_plot_path_45day <- file.path(plots_dir, "sector_network_45day.png")
      ggplot2::ggsave(
        filename = sector_plot_path_45day,
        plot = sector_network_plot_45day,
        width = 12,
        height = 10,
        dpi = 300,
        bg = "white"
      )

      # Return vector of file paths for targets to track
      c(prof_plot_path, sector_plot_path, prof_plot_path_45day, sector_plot_path_45day)
    },
    format = "file"
  ),

  # ============================================================================
  # WORKPLACE BRANCH: File Outputs
  # ============================================================================
  # Write workplace-based transition matrices and visualizations

  # Write large transition dataset to FST
  tar_target(
    name = output_transitions_workplace,
    command = {
      dir.create(output_dir_workplace, recursive = TRUE, showWarnings = FALSE)
      path <- file.path(output_dir_workplace, "transitions.fst")
      write_fst(transitions_workplace, path, compress = 85)
      path
    },
    format = "file"
  ),

  # Write smaller lookup and summary objects to RDS
  tar_target(
    name = output_rds_files_workplace,
    command = {
      # Ensure output directory exists
      dir.create(output_dir_workplace, recursive = TRUE, showWarnings = FALSE)

      # Save all RDS files
      saveRDS(classifiers, file.path(output_dir_workplace, "classifiers.rds"))
      saveRDS(transition_matrices_workplace, file.path(output_dir_workplace, "transition_matrices.rds"))

      # Save profession and sector transition files (separate) - both formats
      saveRDS(profession_transitions_labeled_workplace, file.path(output_dir_workplace, "profession_transitions.rds"))
      saveRDS(sector_transitions_labeled_workplace, file.path(output_dir_workplace, "sector_transitions.rds"))
      saveRDS(profession_matrix_workplace, file.path(output_dir_workplace, "profession_matrix.rds"))
      saveRDS(sector_matrix_workplace, file.path(output_dir_workplace, "sector_matrix.rds"))

      # Save profession and sector summary statistics
      saveRDS(profession_summary_stats_workplace, file.path(output_dir_workplace, "profession_summary_stats.rds"))
      saveRDS(sector_summary_stats_workplace, file.path(output_dir_workplace, "sector_summary_stats.rds"))

      # Save 45-day lag transition matrices and statistics
      saveRDS(transition_matrices_45day_workplace, file.path(output_dir_workplace, "transition_matrices_45day.rds"))
      saveRDS(profession_transitions_labeled_45day_workplace, file.path(output_dir_workplace, "profession_transitions_45day.rds"))
      saveRDS(sector_transitions_labeled_45day_workplace, file.path(output_dir_workplace, "sector_transitions_45day.rds"))
      saveRDS(profession_matrix_45day_workplace, file.path(output_dir_workplace, "profession_matrix_45day.rds"))
      saveRDS(sector_matrix_45day_workplace, file.path(output_dir_workplace, "sector_matrix_45day.rds"))
      saveRDS(profession_summary_stats_45day_workplace, file.path(output_dir_workplace, "profession_summary_stats_45day.rds"))
      saveRDS(sector_summary_stats_45day_workplace, file.path(output_dir_workplace, "sector_summary_stats_45day.rds"))

      # Return indicator
      "rds_files_saved_workplace"
    },
    format = "qs"
  ),

  # Write network visualization plots to PNG files
  tar_target(
    name = output_network_plots_workplace,
    command = {
      # Create plots subdirectory
      plots_dir <- file.path(output_dir_workplace, "plots")
      dir.create(plots_dir, recursive = TRUE, showWarnings = FALSE)

      # Save profession network plot
      prof_plot_path <- file.path(plots_dir, "profession_network.png")
      ggplot2::ggsave(
        filename = prof_plot_path,
        plot = profession_network_plot_workplace,
        width = 12,
        height = 10,
        dpi = 300,
        bg = "white"
      )

      # Save sector network plot
      sector_plot_path <- file.path(plots_dir, "sector_network.png")
      ggplot2::ggsave(
        filename = sector_plot_path,
        plot = sector_network_plot_workplace,
        width = 12,
        height = 10,
        dpi = 300,
        bg = "white"
      )

      # Save 45-day lag profession network plot
      prof_plot_path_45day <- file.path(plots_dir, "profession_network_45day.png")
      ggplot2::ggsave(
        filename = prof_plot_path_45day,
        plot = profession_network_plot_45day_workplace,
        width = 12,
        height = 10,
        dpi = 300,
        bg = "white"
      )

      # Save 45-day lag sector network plot
      sector_plot_path_45day <- file.path(plots_dir, "sector_network_45day.png")
      ggplot2::ggsave(
        filename = sector_plot_path_45day,
        plot = sector_network_plot_45day_workplace,
        width = 12,
        height = 10,
        dpi = 300,
        bg = "white"
      )

      # Return vector of file paths for targets to track
      c(prof_plot_path, sector_plot_path, prof_plot_path_45day, sector_plot_path_45day)
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
      # Ensure FST-loaded data is converted to data.table
      data.table::setDT(data_consolidated_clean)
      data.table::setDT(data_residence)
      data.table::setDT(data_workplace)
      data.table::setDT(transitions)

      cat("\n")
      cat("==========================================================\n")
      cat("   Dashboard Data Preprocessing Pipeline - Validation\n")
      cat("==========================================================\n\n")

      cat("PHASE 1: Data Preparation\n")
      cat("-------------------------\n")
      cat("  Raw contracts loaded:", nrow(raw_contracts), "\n")
      cat("  Prepared (unfiltered):", nrow(contracts_prepared_unfiltered), "\n\n")

      cat("PHASE 2: Consolidation (BEFORE filtering)\n")
      cat("------------------------------------------\n")
      cat("  Consolidated records:", nrow(data_consolidated_clean), "\n")
      cat("  Unique individuals:", data_consolidated_clean[, uniqueN(cf)], "\n")
      cat("  Date range:",
          format(data_consolidated_clean[, min(inizio, na.rm = TRUE)], "%Y-%m-%d"), "to",
          format(data_consolidated_clean[, max(fine, na.rm = TRUE)], "%Y-%m-%d"), "\n\n")

      cat("PHASE 3: Geographic Filtering\n")
      cat("------------------------------\n")
      cat("  Residence-filtered:", nrow(data_residence), "\n")
      cat("  Workplace-filtered:", nrow(data_workplace), "\n\n")

      cat("PHASE 4+: Residence Branch Processing\n")
      cat("--------------------------------------\n")
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

      cat("Transition Matrices (Residence)\n")
      cat("--------------------------------\n")
      cat("  Matrix types created:", length(transition_matrices), "\n")
      cat("  45-day lag transitions:", nrow(transitions_45day), "\n\n")

      cat("Career Analysis (Residence)\n")
      cat("---------------------------\n")
      cat("  Individuals with metrics:", nrow(career_metrics), "\n")
      cat("  Career clusters:", person_data[, uniqueN(cluster_label_it)], "\n\n")

      cat("Output Files (Residence)\n")
      cat("------------------------\n")
      cat("  Output directory:", output_dir, "\n")
      cat("  Transitions file:", file.exists(output_transitions), "\n")
      cat("  Person data file:", file.exists(output_person_data), "\n")
      cat("  Network plots: 4 PNG files generated\n\n")

      # Workplace branch statistics
      data.table::setDT(transitions_workplace)

      cat("PHASE 4+: Workplace Branch Processing\n")
      cat("--------------------------------------\n")
      cat("  Total transitions:", nrow(transitions_workplace), "\n")
      cat("  Closed transitions (emp→emp):",
          transitions_workplace[, sum(is_closed, na.rm = TRUE)], "\n")
      cat("  Open transitions (emp→unemp):",
          transitions_workplace[, sum(is_open, na.rm = TRUE)], "\n")
      cat("  With DID support:",
          transitions_workplace[, sum(has_did, na.rm = TRUE)], "\n")
      cat("  With POL support:",
          transitions_workplace[, sum(has_pol, na.rm = TRUE)], "\n")
      cat("  Average unemployment duration:",
          round(transitions_workplace[, mean(unemployment_duration, na.rm = TRUE)], 1),
          "days\n\n")

      cat("Transition Matrices (Workplace)\n")
      cat("--------------------------------\n")
      cat("  Matrix types created:", length(transition_matrices_workplace), "\n")
      cat("  45-day lag transitions:", nrow(transitions_45day_workplace), "\n\n")

      cat("Output Files (Workplace)\n")
      cat("------------------------\n")
      cat("  Output directory:", output_dir_workplace, "\n")
      cat("  Transitions file:", file.exists(output_transitions_workplace), "\n")
      cat("  Network plots: 4 PNG files generated\n\n")

      cat("==========================================================\n")
      cat("  ✓ Pipeline completed successfully!\n")
      cat("  ✓ Both residence and workplace branches processed\n")
      cat("==========================================================\n\n")

      # Return summary
      list(
        success = TRUE,
        timestamp = Sys.time(),
        consolidation_before_filtering = TRUE,
        data_summary = list(
          n_raw = nrow(raw_contracts),
          n_consolidated = nrow(data_consolidated_clean),
          n_residence = nrow(data_residence),
          n_workplace = nrow(data_workplace),
          n_transitions_residence = nrow(transitions),
          n_transitions_workplace = nrow(transitions_workplace)
        )
      )
    }
  )
)
