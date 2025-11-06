# sampling.R
# Functions for creating stratified test samples of the raw data
# Author: Giampaolo Montaletti

# 1. Stratified Sampling -----

#' Create Stratified Sample of Codice Fiscale by Province
#'
#' Samples unique CFs from rap.fst proportionally across Lombard provinces
#' based on COMUNE_LAVORATORE (residence). This ensures geographic diversity
#' in test samples.
#'
#' @param rap_path Full path to rap.fst file
#' @param n_cf Number of unique CFs to sample (default 10000)
#' @param seed Random seed for reproducibility (default 42)
#'
#' @return Character vector of sampled Codice Fiscale values
#'
#' @examples
#' sampled <- create_stratified_sample(
#'   rap_path = "~/shared_data/raw/rap.fst",
#'   n_cf = 10000,
#'   seed = 42
#' )
create_stratified_sample <- function(rap_path, n_cf = 10000, seed = 42) {

  cat("Creating stratified sample of", n_cf, "CFs...\n")

  # Load required packages
  require(data.table)
  require(fst)

  # Set seed for reproducibility
  set.seed(seed)

  # Load classifiers to get Lombard comuni with Belfiore codes
  source("R/classifiers.R")
  classifiers <- load_classifiers(use_mock = FALSE)

  # Get Lombard comuni from territoriale classifier
  # COD_COMUNE contains Belfiore codes that match COMUNE_LAVORATORE
  lombardia_lookup <- classifiers$territoriale[DES_REGIONE_PAUT == "LOMBARDIA",
                                               .(COD_COMUNE, COD_PROVINCIA, DES_PROVINCIA)]
  setDT(lombardia_lookup)

  cat("Lombard provinces found:", length(unique(lombardia_lookup$DES_PROVINCIA)), "\n")
  cat("Provinces:", paste(unique(lombardia_lookup$DES_PROVINCIA), collapse=", "), "\n")

  # Load rap.fst - only columns needed for sampling
  cat("\nLoading rap.fst to identify CF distribution...\n")
  rap <- read_fst(
    rap_path,
    columns = c("cf", "COMUNE_LAVORATORE"),
    as.data.table = TRUE
  )

  cat("Total contracts in rap.fst:", nrow(rap), "\n")

  # Merge with Lombard comuni to get province info and filter
  rap <- merge(rap, lombardia_lookup,
               by.x = "COMUNE_LAVORATORE",
               by.y = "COD_COMUNE",
               all.x = FALSE)  # Keep only Lombardia contracts

  cat("Contracts in Lombardia:", nrow(rap), "\n")

  # Get unique CFs per province
  cf_by_province <- rap[, .(cf_list = list(unique(cf))), by = DES_PROVINCIA]
  cf_by_province[, n_cf := sapply(cf_list, length)]

  cat("\nCF distribution by province:\n")
  print(cf_by_province[, .(DES_PROVINCIA, n_cf)])

  # Calculate total CFs
  total_cf <- sum(cf_by_province$n_cf)
  cat("\nTotal unique CFs in Lombardia:", total_cf, "\n")

  # Calculate sample size per province (proportional stratification)
  cf_by_province[, sample_n := pmax(1, round(n_cf / total_cf * n_cf))]

  # Adjust if total doesn't match exactly (due to rounding)
  while (sum(cf_by_province$sample_n) != n_cf) {
    if (sum(cf_by_province$sample_n) < n_cf) {
      # Add 1 to largest province
      cf_by_province[which.max(n_cf), sample_n := sample_n + 1]
    } else {
      # Subtract 1 from largest province with sample_n > 1
      cf_by_province[which.max(ifelse(sample_n > 1, n_cf, 0)), sample_n := sample_n - 1]
    }
  }

  cat("\nTarget sample sizes by province:\n")
  print(cf_by_province[, .(DES_PROVINCIA, n_cf, sample_n, pct = round(sample_n/n_cf*100, 2))])

  # Perform stratified sampling
  sampled_cf <- cf_by_province[, {
    cf_vec <- unlist(cf_list)
    if (length(cf_vec) <= sample_n) {
      list(sampled = cf_vec)  # Take all if province is small
    } else {
      list(sampled = sample(cf_vec, size = sample_n, replace = FALSE))
    }
  }, by = DES_PROVINCIA]

  # Extract vector of sampled CFs
  sampled_cf_vector <- sampled_cf$sampled

  cat("\nFinal sample: ", length(sampled_cf_vector), "unique CFs\n")
  cat("Sample created successfully!\n\n")

  return(sampled_cf_vector)
}


# 2. Filter Data Functions -----

#' Filter RAP Contracts to Sampled CFs
#'
#' Loads full rap.fst and filters to only contracts belonging to sampled CFs.
#' Saves result as rap_sample.fst.
#'
#' @param rap_path Full path to source rap.fst
#' @param sampled_cf Character vector of Codice Fiscale to keep
#' @param output_path Full path where to save rap_sample.fst
#'
#' @return Path to output file (for targets tracking)
filter_contracts_to_sample <- function(rap_path, sampled_cf, output_path) {

  cat("Filtering RAP contracts to", length(sampled_cf), "sampled CFs...\n")

  require(data.table)
  require(fst)

  # Create output directory if needed
  output_dir <- dirname(output_path)
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
    cat("Created directory:", output_dir, "\n")
  }

  # Load full rap.fst
  cat("Loading rap.fst...\n")
  rap <- read_fst(rap_path, as.data.table = TRUE)
  cat("Total contracts:", nrow(rap), "\n")

  # Filter to sampled CFs
  rap_sample <- rap[cf %in% sampled_cf]
  cat("Contracts for sampled CFs:", nrow(rap_sample), "\n")
  cat("Reduction ratio:", round(nrow(rap_sample) / nrow(rap) * 100, 2), "%\n")

  # Save as FST with compression
  cat("Writing to", output_path, "...\n")
  write_fst(rap_sample, output_path, compress = 85)

  cat("rap_sample.fst created successfully!\n\n")

  return(output_path)
}


#' Filter DID Data to Sampled CFs
#'
#' Loads full did.fst and filters to only records belonging to sampled CFs.
#' Saves result as did_sample.fst.
#'
#' @param did_path Full path to source did.fst
#' @param sampled_cf Character vector of cf values to keep
#' @param output_path Full path where to save did_sample.fst
#'
#' @return Path to output file (for targets tracking)
filter_did_to_sample <- function(did_path, sampled_cf, output_path) {

  cat("Filtering DID data to", length(sampled_cf), "sampled CFs...\n")

  require(data.table)
  require(fst)

  # Create output directory if needed
  output_dir <- dirname(output_path)
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  # Load did.fst
  cat("Loading did.fst...\n")
  did <- read_fst(did_path, as.data.table = TRUE)
  cat("Total DID records:", nrow(did), "\n")

  # Filter to sampled CFs
  did_sample <- did[cf %in% sampled_cf]
  cat("DID records for sampled CFs:", nrow(did_sample), "\n")
  cat("Reduction ratio:", round(nrow(did_sample) / nrow(did) * 100, 2), "%\n")

  # Save
  cat("Writing to", output_path, "...\n")
  write_fst(did_sample, output_path, compress = 85)

  cat("did_sample.fst created successfully!\n\n")

  return(output_path)
}


#' Filter POL Data to Sampled CFs
#'
#' Loads full pol.fst and filters to only records belonging to sampled CFs.
#' Saves result as pol_sample.fst.
#'
#' @param pol_path Full path to source pol.fst
#' @param sampled_cf Character vector of cf values to keep
#' @param output_path Full path where to save pol_sample.fst
#'
#' @return Path to output file (for targets tracking)
filter_pol_to_sample <- function(pol_path, sampled_cf, output_path) {

  cat("Filtering POL data to", length(sampled_cf), "sampled CFs...\n")

  require(data.table)
  require(fst)

  # Create output directory if needed
  output_dir <- dirname(output_path)
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  # Load pol.fst
  cat("Loading pol.fst...\n")
  pol <- read_fst(pol_path, as.data.table = TRUE)
  cat("Total POL records:", nrow(pol), "\n")

  # Filter to sampled CFs
  pol_sample <- pol[cf %in% sampled_cf]
  cat("POL records for sampled CFs:", nrow(pol_sample), "\n")
  cat("Reduction ratio:", round(nrow(pol_sample) / nrow(pol) * 100, 2), "%\n")

  # Save
  cat("Writing to", output_path, "...\n")
  write_fst(pol_sample, output_path, compress = 85)

  cat("pol_sample.fst created successfully!\n\n")

  return(output_path)
}
