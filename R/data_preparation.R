# 1. Raw data loading functions -----

#' Load raw contract data
#'
#' @param test_mode Logical. If TRUE, loads from sample directory (default: FALSE)
#' @return data.table with raw contract data
#' @export
load_raw_contracts <- function(test_mode = FALSE) {

  # Determine file path based on test_mode
  shared_data <- Sys.getenv("SHARED_DATA_DIR")

  if (test_mode) {
    rap_path <- file.path(shared_data, "sample", "rap_sample.fst")
    cat("[TEST MODE] Loading sample contracts from:", rap_path, "\n")
  } else {
    rap_path <- file.path(shared_data, "raw", "rap.fst")
    cat("Loading raw contracts from:", rap_path, "\n")
  }

  if (!file.exists(rap_path)) {
    stop(sprintf("Contract file not found: %s\nPlease ensure the file is available.", rap_path))
  }

  dt <- fst::read_fst(rap_path, as.data.table = TRUE)
  cat(sprintf("  Loaded %s contracts\n", format(nrow(dt), big.mark = ",")))

  return(dt)
}


#' Load raw DID (unemployment declaration) data
#'
#' @param test_mode Logical. If TRUE, loads from sample directory (default: FALSE)
#' @return data.table with filtered DID data
#' @export
load_raw_did <- function(test_mode = FALSE) {

  # Determine file path based on test_mode
  shared_data <- Sys.getenv("SHARED_DATA_DIR")

  if (test_mode) {
    did_path <- file.path(shared_data, "sample", "did_sample.fst")
    cat("[TEST MODE] Loading sample DID data from:", did_path, "\n")
  } else {
    did_path <- file.path(shared_data, "raw", "did.fst")
    cat("Loading raw DID data from:", did_path, "\n")
  }

  if (!file.exists(did_path)) {
    stop(sprintf("DID file not found: %s\nPlease ensure the file is available.", did_path))
  }

  did <- fst::read_fst(did_path, as.data.table = TRUE)

  # Filter to valid DID statuses and ensure proper IDate class
  did <- did[STATO_DID %in% c("Inserita", "Convalidata", "Sospesa"),
             .(cf, DATA_EVENTO = as.IDate(DATA_EVENTO), DID_NASPI)]

  cat(sprintf("  Loaded %s DID records (filtered to valid statuses)\n",
              format(nrow(did), big.mark = ",")))

  return(did)
}


#' Load raw POL (active labor market policy) data
#'
#' @param test_mode Logical. If TRUE, loads from sample directory (default: FALSE)
#' @return data.table with aggregated policy data
#' @export
load_raw_pol <- function(test_mode = FALSE) {

  # Determine file path based on test_mode
  shared_data <- Sys.getenv("SHARED_DATA_DIR")

  if (test_mode) {
    pol_path <- file.path(shared_data, "sample", "pol_sample.fst")
    cat("[TEST MODE] Loading sample POL data from:", pol_path, "\n")
  } else {
    pol_path <- file.path(shared_data, "raw", "pol.fst")
    cat("Loading raw POL data from:", pol_path, "\n")
  }

  if (!file.exists(pol_path)) {
    stop(sprintf("POL file not found: %s\nPlease ensure the file is available.", pol_path))
  }

  pol <- fst::read_fst(pol_path, as.data.table = TRUE)

  # Aggregate policies by person and date range, ensuring proper IDate class
  data.table::setkey(pol, cf, DATA_INIZIO)
  pol <- pol[!is.na(DATA_INIZIO), .(cf, DATA_INIZIO, DATA_FINE, DESCRIZIONE)]
  pol <- pol[, .(descrizione = paste(DESCRIZIONE, collapse = " "),
                 DATA_INIZIO = as.IDate(DATA_INIZIO[1]),
                 DATA_FINE = as.IDate(DATA_FINE[1])),
             by = .(cf, DATA_INIZIO, DATA_FINE)]

  # Remove the grouping columns since they're duplicated
  pol <- unique(pol[, .(cf, DATA_INIZIO, DATA_FINE, descrizione)])

  cat(sprintf("  Loaded %s POL records (aggregated by person-date)\n",
              format(nrow(pol), big.mark = ",")))

  return(pol)
}


# 2. Contract harmonization functions -----

#' Harmonize contract type codes
#'
#' Maps obsolete/similar contract codes to standard equivalents
#'
#' @param dt data.table with COD_TIPOLOGIA_CONTRATTUALE column
#' @param tipo_contratto Classifier table with contract type mappings
#' @return data.table with harmonized contract codes
#' @export
harmonize_contract_codes <- function(dt, tipo_contratto) {
  cat("Harmonizing contract type codes...\n")

  n_before <- length(unique(dt$COD_TIPOLOGIA_CONTRATTUALE))

  # Create harmonization mapping
  harmonization_map <- tipo_contratto[, .(COD_TIPOLOGIA_CONTRATTUALE, TIPOLOGIA_CONTRATTUALE)]

  # Merge to apply harmonization
  dt <- merge(dt, harmonization_map, by = "COD_TIPOLOGIA_CONTRATTUALE", all.x = TRUE)
  data.table::setDT(dt)  # Ensure dt is data.table after merge (FST loading can affect class)
  dt[, COD_TIPOLOGIA_CONTRATTUALE := NULL]
  data.table::setnames(dt, "TIPOLOGIA_CONTRATTUALE", "COD_TIPOLOGIA_CONTRATTUALE")

  # Add labels
  etichette_contratti <- tipo_contratto[COD_TIPOLOGIA_CONTRATTUALE == TIPOLOGIA_CONTRATTUALE,
                                        .(COD_TIPOLOGIA_CONTRATTUALE = TIPOLOGIA_CONTRATTUALE,
                                          DES_TIPOCONTRATTI)]
  dt <- merge(dt, etichette_contratti, by = "COD_TIPOLOGIA_CONTRATTUALE", all.x = TRUE)
  data.table::setDT(dt)  # Ensure dt is data.table after merge

  n_after <- length(unique(dt$COD_TIPOLOGIA_CONTRATTUALE))
  cat(sprintf("  Contract types: %d → %d (harmonized)\n", n_before, n_after))

  return(dt)
}


#' Filter by geographic location (residence or workplace)
#'
#' @param dt data.table with location columns
#' @param filter_by Either "residence" (COMUNE_LAVORATORE) or "workplace" (COMUNE_SEDE_LAVORO)
#' @param territoriale Geographic lookup table
#' @return data.table filtered to Lombardia region
#' @export
filter_by_location <- function(dt, filter_by = c("residence", "workplace"), territoriale) {
  filter_by <- match.arg(filter_by)

  cat(sprintf("Filtering by %s location...\n", filter_by))

  # Determine which location column to use
  location_col <- switch(filter_by,
    residence = "COMUNE_LAVORATORE",
    workplace = "COMUNE_SEDE_LAVORO"
  )

  n_before <- nrow(dt)

  # CRITICAL FIX: Inherit geography for spells with NA location
  # Any spell with NA geography inherits from previous spell (typically unemployment spells)
  dt <- data.table::copy(dt)  # Avoid modifying by reference
  data.table::setorder(dt, cf, inizio)

  # Use standard R functions to avoid NSE issues
  n_na_before <- sum(is.na(dt[[location_col]]))

  if (n_na_before > 0) {
    # Create a vector of previous locations per person
    prev_vals <- rep(NA_character_, nrow(dt))

    # For each person, shift their location values by 1
    for (person in unique(dt$cf)) {
      rows <- which(dt$cf == person)
      vals <- dt[[location_col]][rows]
      prev_vals[rows] <- c(NA, vals[-length(vals)])
    }

    # Fill NA locations with previous location
    na_mask <- is.na(dt[[location_col]])
    dt[[location_col]][na_mask] <- prev_vals[na_mask]

    n_na_after <- sum(is.na(dt[[location_col]]))

    cat(sprintf("  Inherited geography for %s spells (%s still have NA)\n",
                format(n_na_before - n_na_after, big.mark = ","),
                format(n_na_after, big.mark = ",")))
  }

  # Merge with Lombardia comuni to filter
  lombardia_comuni <- unique(territoriale[DES_REGIONE_PAUT == "LOMBARDIA",
                                          .(COD_COMUNE, DES_PROVINCIA)])

  dt <- merge(dt, lombardia_comuni,
              by.x = location_col,
              by.y = "COD_COMUNE",
              all.x = FALSE)  # Keep only matching records
  data.table::setDT(dt)  # Ensure dt is data.table after merge

  n_after <- nrow(dt)
  pct_retained <- 100 * n_after / n_before

  cat(sprintf("  Filtered by %s: %s → %s rows (%.1f%% retained)\n",
              filter_by,
              format(n_before, big.mark = ","),
              format(n_after, big.mark = ","),
              pct_retained))

  return(dt)
}


#' Standardize education level variable
#'
#' @param dt data.table with LIV_ISTRUZIONE_LAV column
#' @return data.table with istruzione factor variable
#' @export
standardize_education <- function(dt) {
  cat("Standardizing education levels...\n")

  dt[, istruzione := data.table::fcase(
    (is.na(LIV_ISTRUZIONE_LAV) | LIV_ISTRUZIONE_LAV %in% c(0, 10, 20)),
    "licenza media o inferiore",

    LIV_ISTRUZIONE_LAV %in% c(30, 40),
    "diploma",

    default = "universitaria"
  )]

  cat(sprintf("  Education distribution:\n"))
  print(dt[, .N, istruzione][order(istruzione)])

  return(dt)
}


# 3. Main preparation function -----

#' Prepare contract data WITHOUT geographic filtering
#'
#' Main function that orchestrates all preparation steps:
#' 1. Harmonize contract codes
#' 2. Standardize education levels
#' 3. Select and rename relevant columns (preserves both location columns)
#'
#' NOTE: Geographic filtering should happen AFTER consolidation for efficiency.
#' Use filter_by_location() after consolidation to create residence/workplace branches.
#'
#' @param raw_contracts Raw contract data from load_raw_contracts()
#' @param classifiers List containing tipo_contratto and territoriale
#' @return data.table with prepared contract data (NOT filtered by location)
#' @export
prepare_contracts <- function(raw_contracts, classifiers) {

  cat("=" %+% rep("=", 50) %+% "\n")
  cat("PREPARING CONTRACT DATA (no geographic filtering yet)\n")
  cat("=" %+% rep("=", 50) %+% "\n")

  dt <- data.table::copy(raw_contracts)

  # Step 1: Harmonize contract codes
  dt <- harmonize_contract_codes(dt, classifiers$tipo_contratto)

  # Step 2: Filter to Lombardia (either residence OR workplace in Lombardia)
  # This keeps all contracts relevant to Lombardia without choosing which filter yet
  cat("Filtering to contracts with Lombardia connection...\n")

  lombardia_comuni <- unique(classifiers$territoriale[
    DES_REGIONE_PAUT == "LOMBARDIA",
    .(COD_COMUNE)
  ])

  n_before <- nrow(dt)

  # Keep if EITHER residence OR workplace is in Lombardia
  dt <- dt[COMUNE_LAVORATORE %in% lombardia_comuni$COD_COMUNE |
           COMUNE_SEDE_LAVORO %in% lombardia_comuni$COD_COMUNE]

  n_after <- nrow(dt)
  pct_retained <- 100 * n_after / n_before

  cat(sprintf("  Filtered to Lombardia connection: %s → %s rows (%.1f%% retained)\n",
              format(n_before, big.mark = ","),
              format(n_after, big.mark = ","),
              pct_retained))

  # Step 3: Add geographic information (provincia) for both locations
  cat("Adding provincia information...\n")

  # Add provincia for workplace
  prov_lookup <- unique(classifiers$territoriale[,
    .(COD_COMUNE, DES_PROVINCIA_SEDE = DES_PROVINCIA)
  ])

  dt <- merge(dt, prov_lookup,
              by.x = "COMUNE_SEDE_LAVORO",
              by.y = "COD_COMUNE",
              all.x = TRUE)
  data.table::setDT(dt)  # Ensure dt is data.table after merge

  # Step 4: Standardize education
  dt <- standardize_education(dt)

  # Step 5: Select relevant columns
  cat("Selecting final column set...\n")
  dt <- dt[, .(
    cf,
    inizio = INIZIO,  # Lowercase for vecshift compatibility
    fine = FINE,      # Lowercase for vecshift compatibility
    prior = data.table::fcase(COD_TIPO_ORARIO == "F", 1, default = 0),
    qualifica = COD_QUALIFICA_PROF_ISTAT_3DGT,
    ateco = as.character(ATECO_GRUPPO),
    ore = ORE_SETTIM_MEDIE,
    retribuzione = RETRIBUZIONE,
    COD_TIPOLOGIA_CONTRATTUALE,
    DES_TIPOCONTRATTI,
    id = ID_RAPPORTO,
    eta = ETA_LAV_INIZIO,
    sesso = SESSO_LAV,
    istruzione,
    datore = cfd,
    provincia = DES_PROVINCIA_SEDE,  # Use workplace provincia initially
    COMUNE_LAVORATORE,
    COMUNE_SEDE_LAVORO,
    troncata
  )]

  # Set key for efficient processing
  data.table::setkey(dt, cf, inizio, fine)

  cat(sprintf("\n✓ Preparation complete: %s contracts\n",
              format(nrow(dt), big.mark = ",")))
  cat("=" %+% rep("=", 50) %+% "\n\n")

  return(dt)
}


# 4. Vecshift consolidation functions -----

#' Apply vecshift consolidation to handle overlapping employment periods
#'
#' @param prepared_data Prepared contract data from prepare_contracts()
#' @return data.table with consolidated (non-overlapping) employment spells
#' @export
apply_vecshift_consolidation <- function(prepared_data) {
  cat("=" %+% rep("=", 50) %+% "\n")
  cat("APPLYING VECSHIFT CONSOLIDATION\n")
  cat("=" %+% rep("=", 50) %+% "\n")

  # Assess data quality first
  cat("Assessing data quality...\n")
  quality_report <- vecshift::assess_data_quality(prepared_data)
  print(quality_report)

  # Apply vecshift consolidation
  cat("\nConsolidating overlapping spells...\n")
  n_before <- nrow(prepared_data)

  consolidated <- vecshift::vecshift(prepared_data)

  n_after <- nrow(consolidated)
  pct_reduction <- 100 * (1 - n_after / n_before)

  cat(sprintf("  Consolidation: %s → %s rows (%.1f%% reduction)\n",
              format(n_before, big.mark = ","),
              format(n_after, big.mark = ","),
              pct_reduction))

  cat("\n✓ Consolidation complete\n")
  cat("=" %+% rep("=", 50) %+% "\n\n")

  return(consolidated)
}


#' Add unemployment periods between employment spells (pipeline wrapper)
#'
#' @param consolidated_data Data from apply_vecshift_consolidation()
#' @param min_date Minimum date for unemployment periods (e.g., "2021-01-01")
#' @param max_date Maximum date for unemployment periods (e.g., "2024-12-31")
#' @param min_duration Minimum duration in days to create unemployment spell (default: 1)
#' @param add_tail Add unemployment at end if last job ended before max_date (default: TRUE)
#' @return data.table with both employment and unemployment spells
#' @export
pipeline_add_unemployment <- function(consolidated_data,
                                      min_date = as.IDate("2021-01-01"),
                                      max_date = as.IDate("2024-12-31"),
                                      min_duration = 1L,
                                      add_tail = TRUE) {

  cat("=" %+% rep("=", 50) %+% "\n")
  cat("ADDING UNEMPLOYMENT PERIODS\n")
  cat("=" %+% rep("=", 50) %+% "\n")

  cat(sprintf("  Date range: %s to %s\n", min_date, max_date))
  cat(sprintf("  Minimum gap duration: %d days\n", min_duration))
  cat(sprintf("  Add tail unemployment: %s\n", add_tail))

  n_before <- nrow(consolidated_data)

  # Add unemployment periods for gaps
  dt_with_unemployment <- vecshift::add_unemployment_periods(
    consolidated_data,
    min_date = min_date,
    max_date = max_date,
    min_duration = min_duration,
    add_tail = add_tail
  )

  n_after <- nrow(dt_with_unemployment)
  n_unemployment <- n_after - n_before

  cat(sprintf("  Added %s unemployment spells\n", format(n_unemployment, big.mark = ",")))
  cat(sprintf("  Total spells: %s\n", format(n_after, big.mark = ",")))

  cat("\n✓ Unemployment periods added\n")
  cat("=" %+% rep("=", 50) %+% "\n\n")

  return(dt_with_unemployment)
}


#' Merge original contract attributes back after consolidation (pipeline wrapper)
#'
#' @param data_with_unemployment Data with employment and unemployment spells
#' @param prepared_data Original prepared data with all columns
#' @return data.table with full attributes
#' @export
pipeline_merge_attributes <- function(data_with_unemployment, prepared_data) {
  cat("Merging original contract attributes...\n")

  # Select columns to merge (everything except date and id columns already present)
  merge_cols <- c("id", "qualifica", "ateco", "ore", "retribuzione",
                  "COD_TIPOLOGIA_CONTRATTUALE", "DES_TIPOCONTRATTI",
                  "eta", "sesso", "istruzione", "datore", "provincia",
                  "COMUNE_LAVORATORE", "COMUNE_SEDE_LAVORO", "troncata")

  # Merge attributes (only for employment spells, not unemployment)
  dt_merged <- merge(
    data_with_unemployment,
    prepared_data[, ..merge_cols],
    by = "id",
    all.x = TRUE
  )
  data.table::setDT(dt_merged)  # Ensure dt_merged is data.table after merge

  # Ensure date columns remain IDate after merge
  dt_merged[, inizio := as.IDate(inizio)]
  dt_merged[, fine := as.IDate(fine)]

  # Set prior=2 for intermittent contracts (A.05.02)
  dt_merged[COD_TIPOLOGIA_CONTRATTUALE == "A.05.02", prior := 2]

  cat(sprintf("  Attributes merged successfully\n"))

  return(dt_merged)
}


# 5. External event matching functions -----

#' Match DID and POL external events to unemployment spells (pipeline wrapper)
#'
#' Uses vecshift::add_external_events() to match DID/POL records
#' with unemployment periods, creating synthetic spells if needed
#'
#' @param data_with_unemployment Data from pipeline_add_unemployment()
#' @param did_data DID data from load_raw_did()
#' @param pol_data POL data from load_raw_pol()
#' @return data.table with DID/POL flags on unemployment spells
#' @export
pipeline_match_events <- function(data_with_unemployment, did_data, pol_data) {

  cat("=" %+% rep("=", 50) %+% "\n")
  cat("MATCHING EXTERNAL EVENTS (DID/POL)\n")
  cat("=" %+% rep("=", 50) %+% "\n")

  # Work directly with data_with_unemployment (no copy to avoid reference issues)
  dt <- data_with_unemployment
  data.table::setDT(dt)  # Ensure dt is data.table

  # WORKAROUND: Convert IDate to base R Date to avoid IDate storage mode bug in vecshift
  # This sacrifices IDate efficiency but ensures compatibility with vecshift operations
  cat("  WORKAROUND: Converting IDate → Date for vecshift compatibility...\n")
  dt[, inizio := as.Date(inizio)]
  dt[, fine := as.Date(fine)]

  # Prepare DID data (point events - same start and end date)
  cat("Processing DID events...\n")

  # Create new data.table with renamed columns for vecshift
  # WORKAROUND: Use Date instead of IDate to avoid storage mode bug
  did_events <- data.table::data.table(
    cf = did_data$cf,
    event_start = as.Date(did_data$DATA_EVENTO),
    event_end = as.Date(did_data$DATA_EVENTO),
    DID_NASPI = did_data$DID_NASPI,
    event_name = "did",
    id_evento = seq_len(nrow(did_data))
  )

  # Add DID events
  cat("  Matching DID to unemployment spells...\n")
  dt <- vecshift::add_external_events(
    dt,
    external_events = did_events,
    event_matching_strategy = "overlap",
    create_synthetic_unemployment = TRUE,
    synthetic_unemployment_duration = 730L  # 2 years
  )

  n_did_matched <- sum(!is.na(dt$did_attribute))
  cat(sprintf("  ✓ Matched %s DID events\n", format(n_did_matched, big.mark = ",")))

  # Convert back to IDate after DID matching
  cat("  Converting Date → IDate after DID matching...\n")
  dt[, inizio := data.table::as.IDate(inizio)]
  dt[, fine := data.table::as.IDate(fine)]

  # Prepare POL data (interval events with start and end dates)
  cat("\nProcessing POL events...\n")

  # WORKAROUND: Convert back to Date for POL matching
  dt[, inizio := as.Date(inizio)]
  dt[, fine := as.Date(fine)]

  # Create new data.table with renamed columns for vecshift
  # Cap end dates at max observation date
  # WORKAROUND: Use Date instead of IDate to avoid storage mode bug
  pol_events <- data.table::data.table(
    cf = pol_data$cf,
    event_start = as.Date(pol_data$DATA_INIZIO),
    event_end = as.Date(pmin(pol_data$DATA_FINE, as.Date("2024-12-31"))),
    descrizione = pol_data$descrizione,
    event_name = "pol",
    id_evento = seq_len(nrow(pol_data))
  )

  # Add POL events
  cat("  Matching POL to unemployment spells...\n")
  dt <- vecshift::add_external_events(
    dt,
    external_events = pol_events,
    event_matching_strategy = "overlap",
    create_synthetic_unemployment = TRUE,
    synthetic_unemployment_duration = 730L  # 2 years
  )

  n_pol_matched <- sum(!is.na(dt$pol_attribute))
  cat(sprintf("  ✓ Matched %s POL events\n", format(n_pol_matched, big.mark = ",")))

  # Convert back to IDate after all vecshift operations
  cat("\n  Converting Date → IDate (final conversion)...\n")
  dt[, inizio := data.table::as.IDate(inizio)]
  dt[, fine := data.table::as.IDate(fine)]

  cat("\n✓ External event matching complete\n")
  cat("=" %+% rep("=", 50) %+% "\n\n")

  return(dt)
}


# 6. Helper operator -----

# String concatenation operator for cleaner output formatting
`%+%` <- function(a, b) paste0(a, b)
