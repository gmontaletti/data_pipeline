# 1. Raw data loading functions -----

#' Load raw contract data
#'
#' Loads employment contract data from either PostgreSQL database or FST file.
#' When source="database", fetches from staging.mv_rapporti_cleaned and applies
#' column mapping to convert lowercase names to UPPERCASE format.
#'
#' @param test_mode Logical. If TRUE, loads from sample directory (default: FALSE).
#'   Only applies when source="fst".
#' @param source Data source: "auto" (prefer database if available), "database",
#'   or "fst". Default is "auto".
#' @return data.table with raw contract data in UPPERCASE column format
#' @export
load_raw_contracts <- function(test_mode = FALSE,
                               source = c("auto", "database", "fst")) {
  source <- match.arg(source)

  # Auto-detect: prefer database if available, fallback to FST

  if (source == "auto") {
    source <- if (db_available()) "database" else "fst"
    cat(sprintf("Data source auto-detected: %s\n", source))
  }

  # Database source: fetch from PostgreSQL and map columns
  if (source == "database") {
    cat("Loading raw contracts from PostgreSQL database...\n")

    # Ensure MV exists (does not create, just verifies)
    ensure_materialized_view(refresh = FALSE)

    # Fetch data (lowercase columns)
    dt <- fetch_from_mv()

    # Map to UPPERCASE format expected by prepare_contracts()
    dt <- map_db_columns(dt)

    cat(sprintf("  Loaded %s contracts from database\n",
                format(nrow(dt), big.mark = ",")))
    return(dt)
  }

  # FST source: load from file (existing logic)
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

  # Check if FST file has lowercase columns (from rapporti pipeline)
  # and map them if needed
  if ("inizio" %in% names(dt) && !"INIZIO" %in% names(dt)) {
    cat("  FST file has lowercase columns, applying column mapping...\n")
    dt <- map_db_columns(dt)
  }

  cat(sprintf("  Loaded %s contracts\n", format(nrow(dt), big.mark = ",")))

  return(dt)
}


#' Map database column names to expected format
#'
#' Converts lowercase columns from staging.mv_rapporti_cleaned to
#' UPPERCASE format expected by prepare_contracts().
#'
#' Handles:
#' - Column renaming: lowercase → UPPERCASE
#' - Derived columns: prior (0/1) → COD_TIPO_ORARIO (F/P)
#' - ID mapping: id → ID_RAPPORTO, datore → cfd
#'
#' @param dt data.table from fetch_from_mv() or FST file
#' @return data.table with renamed/mapped columns
#' @export
map_db_columns <- function(dt) {
  cat("Mapping database column names to pipeline format...\n")

  n_before <- ncol(dt)

  # Column renaming: lowercase → UPPERCASE
  # Using skip_absent=TRUE to handle partial column sets
  data.table::setnames(dt, old = c(
    "inizio", "fine", "qualifica", "ateco_gruppo", "ore",
    "retribuzione", "cod_tipologia_contrattuale",
    "ini_cod_tipologia_contrattuale", "eta", "sesso",
    "liv_istruzione_lav", "comune_lavoratore", "comune_sede_lavoro"
  ), new = c(
    "INIZIO", "FINE", "COD_QUALIFICA_PROF_ISTAT_3DGT", "ATECO_GRUPPO", "ORE_SETTIM_MEDIE",
    "RETRIBUZIONE", "COD_TIPOLOGIA_CONTRATTUALE",
    "INI_COD_TIPOLOGIA_CONTRATTUALE", "ETA_LAV_INIZIO", "SESSO_LAV",
    "LIV_ISTRUZIONE_LAV", "COMUNE_LAVORATORE", "COMUNE_SEDE_LAVORO"
  ), skip_absent = TRUE)

  # Derived column handling:


  # 1. prior (0/1) → COD_TIPO_ORARIO (F/P)
  if ("prior" %in% names(dt) && !"COD_TIPO_ORARIO" %in% names(dt)) {
    dt[, COD_TIPO_ORARIO := data.table::fifelse(prior == 1, "F", "P")]
    cat("  Created COD_TIPO_ORARIO from prior\n")
  }

  # 2. datore (numeric ID) → cfd
  if ("datore" %in% names(dt) && !"cfd" %in% names(dt)) {
    data.table::setnames(dt, "datore", "cfd")
    cat("  Renamed datore → cfd\n")
  }

  # 3. id (row number) → ID_RAPPORTO
  if ("id" %in% names(dt) && !"ID_RAPPORTO" %in% names(dt)) {
    data.table::setnames(dt, "id", "ID_RAPPORTO")
    cat("  Renamed id → ID_RAPPORTO\n")
  }

  # Convert dates to IDate for consistency
  if ("INIZIO" %in% names(dt)) {
    dt[, INIZIO := data.table::as.IDate(INIZIO)]
  }
  if ("FINE" %in% names(dt)) {
    dt[, FINE := data.table::as.IDate(FINE)]
  }

  cat(sprintf("  Mapped %d columns\n", ncol(dt)))

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
             list(cf, DATA_EVENTO = as.IDate(DATA_EVENTO), DID_NASPI)]

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
  pol <- pol[!is.na(DATA_INIZIO), list(cf, DATA_INIZIO, DATA_FINE, DESCRIZIONE)]
  pol <- pol[, list(descrizione = paste(DESCRIZIONE, collapse = " "),
                    DATA_INIZIO = as.IDate(DATA_INIZIO[1]),
                    DATA_FINE = as.IDate(DATA_FINE[1])),
             by = list(cf, DATA_INIZIO, DATA_FINE)]

  # Remove the grouping columns since they're duplicated
  pol <- unique(pol[, list(cf, DATA_INIZIO, DATA_FINE, descrizione)])

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
  harmonization_map <- tipo_contratto[, list(COD_TIPOLOGIA_CONTRATTUALE, TIPOLOGIA_CONTRATTUALE)]

  # Merge to apply harmonization
  dt <- merge(dt, harmonization_map, by = "COD_TIPOLOGIA_CONTRATTUALE", all.x = TRUE)
  data.table::setDT(dt)  # Ensure dt is data.table after merge (FST loading can affect class)
  dt[, COD_TIPOLOGIA_CONTRATTUALE := NULL]
  data.table::setnames(dt, "TIPOLOGIA_CONTRATTUALE", "COD_TIPOLOGIA_CONTRATTUALE")

  # Add labels
  etichette_contratti <- tipo_contratto[COD_TIPOLOGIA_CONTRATTUALE == TIPOLOGIA_CONTRATTUALE,
                                        list(COD_TIPOLOGIA_CONTRATTUALE = TIPOLOGIA_CONTRATTUALE,
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

  # OPTIMIZATION 1: Work directly on dt (no copy needed - filtering creates new object)
  # Ensure proper sorting for shift() operation
  data.table::setorder(dt, cf, inizio)

  # OPTIMIZATION 2: Use data.table's vectorized shift() for geographic inheritance
  # This replaces the split() + for-loop approach with C-level optimized code
  n_na_before <- sum(is.na(dt[[location_col]]))

  if (n_na_before > 0) {
    cat(sprintf("  Inheriting geography for %s NA spells...\n",
                format(n_na_before, big.mark = ",")))

    # SOLUTION: Use collapse::flag() which respects grouping differently than data.table::shift()
    # collapse package is already loaded (dependency of vecshift)
    # flag() performs grouped lagging without the problematic 'by' parameter syntax

    # Get lagged values using collapse::flag (groups by cf, respects sort order)
    prev_vals <- collapse::flag(dt[[location_col]], n = 1, g = dt$cf, t = dt$inizio)

    # Fill NA locations with previous location
    na_mask <- is.na(dt[[location_col]])
    if (sum(na_mask) > 0) {
      data.table::set(dt, i = which(na_mask), j = location_col, value = prev_vals[na_mask])
    }

    n_na_after <- sum(is.na(dt[[location_col]]))

    cat(sprintf("  Inherited geography for %s spells (%s still have NA)\n",
                format(n_na_before - n_na_after, big.mark = ","),
                format(n_na_after, big.mark = ",")))
  }

  # OPTIMIZATION 3: Filter to Lombardia and add provincia in one merge operation
  # Extract Lombardia comuni with their provincia
  lombardia_lookup <- unique(territoriale[DES_REGIONE_PAUT == "LOMBARDIA",
                                          list(COD_COMUNE, DES_PROVINCIA)])

  # Merge to filter AND add provincia (single operation, keeps only matching records)
  dt <- merge(dt, lombardia_lookup,
              by.x = location_col,
              by.y = "COD_COMUNE",
              all.x = FALSE,  # Keep only Lombardia records
              all.y = FALSE)

  # Ensure result is data.table (merge can sometimes return data.frame)
  data.table::setDT(dt)

  n_after <- nrow(dt)
  pct_retained <- 100 * n_after / n_before

  cat(sprintf("  Filtered by %s: %s → %s rows (%.1f%% retained)\n",
              filter_by,
              format(n_before, big.mark = ","),
              format(n_after, big.mark = ","),
              pct_retained))

  # OPTIMIZATION 5: Set key for downstream performance (if columns exist)
  key_cols <- c("cf", "inizio", "fine")
  existing_cols <- key_cols[key_cols %in% names(dt)]
  if (length(existing_cols) > 0) {
    data.table::setkeyv(dt, existing_cols)
  }

  return(dt)
}


# 2. Post-vecshift geographic filtering -----

#' Filter vecshift output by geography (before adding unemployment)
#'
#' CRITICAL FIX: Vecshift drops all columns except cf, inizio, fine, prior, id, arco, durata.
#' This function needs contracts_prepared_unfiltered to access COMUNE_LAVORATORE and COMUNE_SEDE_LAVORO.
#' It joins back to prepared data via the 'id' column to get location information for filtering.
#'
#' @param dt data.table from vecshift (employment only, minimal skeleton)
#' @param filter_by "residence" or "workplace"
#' @param territoriale Geographic lookup table from classifiers
#' @param contracts_prepared_unfiltered Original prepared data with location columns
#' @return Filtered data.table with only Lombardia records (still minimal skeleton)
#' @export
filter_vecshift_by_geography <- function(dt, filter_by = c("residence", "workplace"),
                                          territoriale, contracts_prepared_unfiltered) {
  filter_by <- match.arg(filter_by)

  cat(sprintf("Filtering vecshift output by %s location...\n", filter_by))

  location_col <- switch(filter_by,
    residence = "COMUNE_LAVORATORE",
    workplace = "COMUNE_SEDE_LAVORO")

  # Get Lombardia comuni
  lombardia_comuni <- unique(territoriale[
    DES_REGIONE_PAUT == "LOMBARDIA", list(COD_COMUNE)])

  n_before <- nrow(dt)

  # CRITICAL FIX: Join back to original prepared data to get location column
  # vecshift output only has: cf, inizio, fine, prior, id, arco, durata
  # We need to get location from contracts_prepared_unfiltered via 'id' column

  # DIAGNOSTIC: Check if 'id' column exists in contracts_prepared_unfiltered
  if (!"id" %in% names(contracts_prepared_unfiltered)) {
    stop("ERROR: 'id' column not found in contracts_prepared_unfiltered.\n",
         "Available columns: ", paste(names(contracts_prepared_unfiltered), collapse = ", "),
         "\nThis is likely because ID_RAPPORTO was not present in the raw data.")
  }

  # Extract just id and location from prepared data
  location_lookup <- contracts_prepared_unfiltered[, list(id,
                                                           COMUNE_LAVORATORE,
                                                           COMUNE_SEDE_LAVORO)]

  # Merge location info to vecshift skeleton
  dt_with_location <- merge(dt, location_lookup, by = "id", all.x = TRUE)
  data.table::setDT(dt_with_location)

  # Now filter by the selected location column
  dt_filtered <- dt_with_location[get(location_col) %chin% lombardia_comuni$COD_COMUNE]

  # Drop location columns to return to minimal skeleton format
  # (they'll be merged back later in merge_attributes_only)
  dt_filtered[, c("COMUNE_LAVORATORE", "COMUNE_SEDE_LAVORO") := NULL]

  n_after <- nrow(dt_filtered)
  pct_retained <- 100 * n_after / n_before

  cat(sprintf("  Filtered by %s: %s -> %s rows (%.1f%% retained)\n",
              filter_by,
              format(n_before, big.mark = ","),
              format(n_after, big.mark = ","),
              pct_retained))

  # Set key for downstream performance
  data.table::setkey(dt_filtered, cf, inizio, fine)

  return(dt_filtered)
}


#' Zero working hours for excluded contract types
#'
#' Sets working hours to 0 for contract types that should not contribute
#' to FTE calculations: tirocinio (internship), intermittente (on-call),
#' and parasubordinati (co.co.co).
#'
#' @param dt data.table with COD_TIPOLOGIA_CONTRATTUALE and ORE_SETTIM_MEDIE columns
#' @return data.table with modified ORE_SETTIM_MEDIE
#' @export
zero_hours_for_excluded_contracts <- function(dt) {
  # Contract codes to exclude from FTE calculations:
  # G.03.00 = Tirocinio (internship)
  # A.05.02 = Lavoro Intermittente (on-call)
  # C.01.00 = Collaborazione Coordinata Continuativa (co.co.co/parasubordinati)
  excluded_codes <- c("G.03.00", "A.05.02", "C.01.00")

  n_affected <- sum(dt$COD_TIPOLOGIA_CONTRATTUALE %in% excluded_codes, na.rm = TRUE)

  dt[COD_TIPOLOGIA_CONTRATTUALE %in% excluded_codes, ORE_SETTIM_MEDIE := 0]

  cat(sprintf("  Set working hours to 0 for %s contracts (tirocinio/intermittente/parasubordinati)\n",
              format(n_affected, big.mark = ",")))

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

  # Step 2: Zero working hours for excluded contract types
  dt <- zero_hours_for_excluded_contracts(dt)

  # Step 3: Filter to Lombardia (either residence OR workplace in Lombardia)
  # This keeps all contracts relevant to Lombardia without choosing which filter yet
  cat("Filtering to contracts with Lombardia connection...\n")

  lombardia_comuni <- unique(classifiers$territoriale[
    DES_REGIONE_PAUT == "LOMBARDIA",
    list(COD_COMUNE)
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

  # Step 4: Add geographic information (provincia) for both locations
  cat("Adding provincia information...\n")

  # Add provincia for workplace
  prov_lookup <- unique(classifiers$territoriale[,
    list(COD_COMUNE, DES_PROVINCIA_SEDE = DES_PROVINCIA)
  ])

  dt <- merge(dt, prov_lookup,
              by.x = "COMUNE_SEDE_LAVORO",
              by.y = "COD_COMUNE",
              all.x = TRUE)
  data.table::setDT(dt)  # Ensure dt is data.table after merge

  # Step 5: Standardize education
  dt <- standardize_education(dt)

  # Step 6: Select relevant columns
  cat("Selecting final column set...\n")

  # CRITICAL FIX: Check if ID_RAPPORTO exists, if not create it
  if (!"ID_RAPPORTO" %in% names(dt)) {
    cat("  WARNING: ID_RAPPORTO not found in raw data, creating sequential IDs...\n")
    dt[, ID_RAPPORTO := .I]  # Create sequential row IDs
  }

  # CRITICAL FIX: Check if troncata exists, if not create it
  if (!"troncata" %in% names(dt)) {
    cat("  WARNING: troncata column not found, setting to FALSE...\n")
    dt[, troncata := FALSE]
  }

  # CRITICAL FIX: Check if cfd exists, if not create it as NA
  if (!"cfd" %in% names(dt)) {
    cat("  WARNING: cfd (employer ID) column not found, setting to NA...\n")
    dt[, cfd := NA_character_]
  }

  dt <- dt[, list(
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

  # Apply vecshift consolidation
  cat("Consolidating overlapping spells...\n")
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
#' @param min_date Minimum date for filtering DID/POL events (default: NULL = no filter)
#' @param max_date Maximum date for filtering DID/POL events (default: NULL = no filter)
#' @return data.table with DID/POL flags on unemployment spells
#' @export
pipeline_match_events <- function(data_with_unemployment, did_data, pol_data,
                                   min_date = NULL, max_date = NULL) {

  cat("=" %+% rep("=", 50) %+% "\n")
  cat("MATCHING EXTERNAL EVENTS (DID/POL)\n")
  cat("=" %+% rep("=", 50) %+% "\n")

  # Work directly with data_with_unemployment (no copy to avoid reference issues)
  dt <- data_with_unemployment
  data.table::setDT(dt)  # Ensure dt is data.table
  data.table::setDT(did_data)
  data.table::setDT(pol_data)

  # Filter DID/POL data by date range to reduce memory usage
  if (!is.null(min_date)) {
    n_did_before <- nrow(did_data)
    n_pol_before <- nrow(pol_data)
    did_data <- did_data[DATA_EVENTO >= min_date]
    pol_data <- pol_data[DATA_INIZIO >= min_date | DATA_FINE >= min_date]
    cat(sprintf("  Date filter (>= %s): DID %s → %s, POL %s → %s\n",
                min_date,
                format(n_did_before, big.mark = ","),
                format(nrow(did_data), big.mark = ","),
                format(n_pol_before, big.mark = ","),
                format(nrow(pol_data), big.mark = ",")))
  }

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

  # MEMORY OPTIMIZATION: Batch DID matching (same pattern as POL)
  # Pre-filter DID to CFs in employment data
  cfs_in_dt <- as.character(unique(dt$cf))  # Ensure character for %chin%
  n_did_before <- nrow(did_events)
  did_events <- did_events[cf %chin% cfs_in_dt]
  cat(sprintf("  CF filter: DID %s → %s (%.1f%% reduction)\n",
              format(n_did_before, big.mark = ","),
              format(nrow(did_events), big.mark = ","),
              100 * (n_did_before - nrow(did_events)) / n_did_before))

  # Process DID in batches
  batch_size_did <- 50000L  # CFs per batch
  cf_batches_did <- split(cfs_in_dt, ceiling(seq_along(cfs_in_dt) / batch_size_did))
  n_batches_did <- length(cf_batches_did)

  cat(sprintf("  Processing %d batches of ~%s CFs each...\n",
              n_batches_did, format(batch_size_did, big.mark = ",")))

  result_list_did <- vector("list", n_batches_did)

  for (i in seq_len(n_batches_did)) {
    batch_cfs <- as.character(cf_batches_did[[i]])  # Ensure character for %chin%
    dt_batch <- dt[cf %chin% batch_cfs]
    did_batch <- did_events[cf %chin% batch_cfs]

    if (nrow(did_batch) > 0) {
      dt_batch <- vecshift::add_external_events(
        dt_batch,
        external_events = did_batch,
        event_matching_strategy = "overlap",
        create_synthetic_unemployment = TRUE,
        synthetic_unemployment_duration = 730L  # 2 years
      )
    } else {
      # No DID for this batch - initialize columns
      dt_batch[, `:=`(did_attribute = NA_character_, did_match_quality = NA_character_)]
    }

    result_list_did[[i]] <- dt_batch

    if (i %% 10 == 0 || i == n_batches_did) {
      cat(sprintf("    Batch %d/%d complete\n", i, n_batches_did))
      gc()
    }
  }

  dt <- data.table::rbindlist(result_list_did, use.names = TRUE, fill = TRUE)
  rm(result_list_did, did_events, cf_batches_did)
  gc()

  n_did_matched <- sum(!is.na(dt$did_attribute))
  cat(sprintf("  ✓ Matched %s DID events\n", format(n_did_matched, big.mark = ",")))

  # Convert back to IDate after DID matching
  cat("  Converting Date → IDate after DID matching...\n")
  dt[, inizio := data.table::as.IDate(inizio)]
  dt[, fine := data.table::as.IDate(fine)]

  # Prepare POL data (interval events with start and end dates)
  cat("\nProcessing POL events...\n")

  # MEMORY OPTIMIZATION: Two-phase batched processing
  # Phase A: Process POL for CFs in employment data (batched vecshift)
  # Phase B: Create synthetic unemployment for POL-only CFs

  # MEMORY OPTIMIZATION: Identify POL-only CFs BEFORE filtering (avoid full copy)
  cfs_in_dt <- as.character(unique(dt$cf))  # Ensure character for %chin%
  cfs_only_in_pol <- as.character(setdiff(unique(pol_data$cf), cfs_in_dt))  # Ensure character
  pol_only <- pol_data[cf %chin% cfs_only_in_pol]  # Keep only POL-only records
  cat(sprintf("  POL-only CFs identified: %s (kept %s records for Phase B)\n",
              format(length(cfs_only_in_pol), big.mark = ","),
              format(nrow(pol_only), big.mark = ",")))

  # 1. Pre-filter POL to CFs in employment data
  n_pol_before <- nrow(pol_data)
  pol_data <- pol_data[cf %chin% cfs_in_dt]
  cat(sprintf("  CF filter: POL %s → %s (%.1f%% reduction)\n",
              format(n_pol_before, big.mark = ","),
              format(nrow(pol_data), big.mark = ","),
              100 * (n_pol_before - nrow(pol_data)) / n_pol_before))

  # 2. Process in batches (Phase A)
  batch_size <- 50000L  # CFs per batch (reduced from 100k for memory efficiency)
  cf_batches <- split(cfs_in_dt, ceiling(seq_along(cfs_in_dt) / batch_size))
  n_batches <- length(cf_batches)

  cat(sprintf("  Processing %d batches of ~%s CFs each...\n",
              n_batches, format(batch_size, big.mark = ",")))

  result_list <- vector("list", n_batches)

  for (i in seq_len(n_batches)) {
    batch_cfs <- as.character(cf_batches[[i]])  # Ensure character for %chin%
    dt_batch <- dt[cf %chin% batch_cfs]
    pol_batch <- pol_data[cf %chin% batch_cfs]

    if (nrow(pol_batch) > 0) {
      # Convert to Date for vecshift
      dt_batch[, inizio := as.Date(inizio)]
      dt_batch[, fine := as.Date(fine)]

      pol_events <- data.table::data.table(
        cf = pol_batch$cf,
        event_start = as.Date(pol_batch$DATA_INIZIO),
        event_end = as.Date(pmin(pol_batch$DATA_FINE, as.Date("2024-12-31"))),
        descrizione = pol_batch$descrizione,
        event_name = "pol",
        id_evento = seq_len(nrow(pol_batch))
      )

      dt_batch <- vecshift::add_external_events(
        dt_batch,
        external_events = pol_events,
        event_matching_strategy = "overlap",
        create_synthetic_unemployment = TRUE,
        synthetic_unemployment_duration = 730L
      )

      # Convert back to IDate
      dt_batch[, inizio := data.table::as.IDate(inizio)]
      dt_batch[, fine := data.table::as.IDate(fine)]
    } else {
      # No POL for this batch - initialize columns
      dt_batch[, `:=`(pol_attribute = NA_character_, pol_match_quality = NA_character_)]
    }

    result_list[[i]] <- dt_batch

    if (i %% 10 == 0) {
      cat(sprintf("    Batch %d/%d complete\n", i, n_batches))
      gc()
    }
  }

  dt <- data.table::rbindlist(result_list, use.names = TRUE, fill = TRUE)
  rm(result_list)
  gc()

  # Phase B: Create synthetic unemployment for POL-only CFs
  # These CFs have POL participation but no employment contracts
  # (pol_only and cfs_only_in_pol already computed above to avoid full data copy)

  if (nrow(pol_only) > 0) {
    cat(sprintf("  Phase B: Creating synthetic unemployment for %s POL-only CFs...\n",
                format(length(cfs_only_in_pol), big.mark = ",")))

    # Create one synthetic unemployment spell per CF covering their POL period
    synthetic_pol <- pol_only[, list(
      inizio = min(DATA_INIZIO),
      fine = pmin(max(DATA_FINE), as.IDate("2024-12-31")),
      pol_attribute = paste(unique(descrizione), collapse = "; ")
    ), by = cf]

    # Add required columns
    synthetic_pol[, `:=`(
      arco = 0L,
      durata = as.integer(fine - inizio),
      pol_match_quality = "synthetic_pol_only",
      did_attribute = NA_character_,
      did_match_quality = NA_character_
    )]

    # Fill missing columns from dt with NA
    missing_cols <- setdiff(names(dt), names(synthetic_pol))
    for (col in missing_cols) {
      synthetic_pol[, (col) := NA]
    }

    dt <- data.table::rbindlist(list(dt, synthetic_pol), use.names = TRUE, fill = TRUE)
    cat(sprintf("  ✓ Added %s synthetic unemployment spells for POL-only CFs\n",
                format(nrow(synthetic_pol), big.mark = ",")))
  }

  n_pol_matched <- sum(!is.na(dt$pol_attribute))
  cat(sprintf("  ✓ Total POL matched: %s\n", format(n_pol_matched, big.mark = ",")))

  # Convert back to IDate after all vecshift operations
  cat("\n  Converting Date → IDate (final conversion)...\n")
  dt[, inizio := data.table::as.IDate(inizio)]
  dt[, fine := data.table::as.IDate(fine)]

  cat("\n✓ External event matching complete\n")
  cat("=" %+% rep("=", 50) %+% "\n\n")

  return(dt)
}


#' Match DID and POL events with memory-safe mode
#'
#' Memory-optimized version that enables vecshift's memory_safe mode and
#' uses smaller batch sizes (25k CFs instead of 50k).
#'
#' @param data_with_unemployment Data from pipeline_add_unemployment()
#' @param did_data DID data from load_raw_did()
#' @param pol_data POL data from load_raw_pol()
#' @param min_date Minimum date for filtering DID/POL events
#' @param max_date Maximum date for filtering DID/POL events
#' @return data.table with DID/POL flags on unemployment spells
#' @export
pipeline_match_events_memory_safe <- function(data_with_unemployment, did_data, pol_data,
                                               min_date = NULL, max_date = NULL) {

  cat("=" %+% rep("=", 50) %+% "\n")
  cat("MATCHING EXTERNAL EVENTS (DID/POL) - MEMORY SAFE MODE\n")
  cat("=" %+% rep("=", 50) %+% "\n")

  # Work directly with data_with_unemployment
  dt <- data_with_unemployment
  data.table::setDT(dt)
  data.table::setDT(did_data)
  data.table::setDT(pol_data)

  # Filter DID/POL data by date range
  if (!is.null(min_date)) {
    n_did_before <- nrow(did_data)
    n_pol_before <- nrow(pol_data)
    did_data <- did_data[DATA_EVENTO >= min_date]
    pol_data <- pol_data[DATA_INIZIO >= min_date | DATA_FINE >= min_date]
    cat(sprintf("  Date filter (>= %s): DID %s → %s, POL %s → %s\n",
                min_date,
                format(n_did_before, big.mark = ","),
                format(nrow(did_data), big.mark = ","),
                format(n_pol_before, big.mark = ","),
                format(nrow(pol_data), big.mark = ",")))
  }

  # Convert IDate → Date for vecshift compatibility
  cat("  Converting IDate → Date for vecshift compatibility...\n")
  dt[, inizio := as.Date(inizio)]
  dt[, fine := as.Date(fine)]

  # Prepare DID data
  cat("\nProcessing DID events (memory-safe mode)...\n")

  did_events <- data.table::data.table(
    cf = did_data$cf,
    event_start = as.Date(did_data$DATA_EVENTO),
    event_end = as.Date(did_data$DATA_EVENTO),
    DID_NASPI = did_data$DID_NASPI,
    event_name = "did",
    id_evento = seq_len(nrow(did_data))
  )

  # MEMORY OPTIMIZATION: Pre-filter and batch with smaller size
  cfs_in_dt <- as.character(unique(dt$cf))
  n_did_before <- nrow(did_events)
  did_events <- did_events[cf %chin% cfs_in_dt]
  cat(sprintf("  CF filter: DID %s → %s (%.1f%% reduction)\n",
              format(n_did_before, big.mark = ","),
              format(nrow(did_events), big.mark = ","),
              100 * (n_did_before - nrow(did_events)) / n_did_before))

  # Process DID in smaller batches (25k instead of 50k)
  batch_size_did <- 25000L  # Reduced from 50k for memory safety
  cf_batches_did <- split(cfs_in_dt, ceiling(seq_along(cfs_in_dt) / batch_size_did))
  n_batches_did <- length(cf_batches_did)

  cat(sprintf("  Processing %d batches of ~%s CFs each (memory-safe)...\n",
              n_batches_did, format(batch_size_did, big.mark = ",")))

  result_list_did <- vector("list", n_batches_did)

  for (i in seq_len(n_batches_did)) {
    batch_cfs <- as.character(cf_batches_did[[i]])
    dt_batch <- dt[cf %chin% batch_cfs]
    did_batch <- did_events[cf %chin% batch_cfs]

    if (nrow(did_batch) > 0) {
      # Use memory_safe mode with smaller chunks
      dt_batch <- vecshift::add_external_events(
        dt_batch,
        external_events = did_batch,
        event_matching_strategy = "overlap",
        create_synthetic_unemployment = TRUE,
        synthetic_unemployment_duration = 730L,
        memory_safe = TRUE,   # MEMORY OPTIMIZATION
        chunk_size = 5000L    # Smaller chunks (default is 10000)
      )
    } else {
      dt_batch[, `:=`(did_attribute = NA_character_, did_match_quality = NA_character_)]
    }

    result_list_did[[i]] <- dt_batch

    # More frequent gc() for memory safety
    if (i %% 5 == 0 || i == n_batches_did) {
      cat(sprintf("    Batch %d/%d complete\n", i, n_batches_did))
      gc()
    }
  }

  dt <- data.table::rbindlist(result_list_did, use.names = TRUE, fill = TRUE)
  rm(result_list_did, did_events, cf_batches_did)
  gc()

  n_did_matched <- sum(!is.na(dt$did_attribute))
  cat(sprintf("  ✓ Matched %s DID events\n", format(n_did_matched, big.mark = ",")))

  # Convert back to IDate
  dt[, inizio := data.table::as.IDate(inizio)]
  dt[, fine := data.table::as.IDate(fine)]

  # Process POL events
  cat("\nProcessing POL events (memory-safe mode)...\n")

  # Identify POL-only CFs BEFORE filtering
  cfs_in_dt <- as.character(unique(dt$cf))
  cfs_only_in_pol <- as.character(setdiff(unique(pol_data$cf), cfs_in_dt))
  pol_only <- pol_data[cf %chin% cfs_only_in_pol]
  cat(sprintf("  POL-only CFs identified: %s (kept %s records for Phase B)\n",
              format(length(cfs_only_in_pol), big.mark = ","),
              format(nrow(pol_only), big.mark = ",")))

  # Pre-filter POL to CFs in employment data
  n_pol_before <- nrow(pol_data)
  pol_data <- pol_data[cf %chin% cfs_in_dt]
  cat(sprintf("  CF filter: POL %s → %s (%.1f%% reduction)\n",
              format(n_pol_before, big.mark = ","),
              format(nrow(pol_data), big.mark = ","),
              100 * (n_pol_before - nrow(pol_data)) / n_pol_before))

  # Process POL in smaller batches
  batch_size <- 25000L  # Reduced from 50k
  cf_batches <- split(cfs_in_dt, ceiling(seq_along(cfs_in_dt) / batch_size))
  n_batches <- length(cf_batches)

  cat(sprintf("  Processing %d batches of ~%s CFs each (memory-safe)...\n",
              n_batches, format(batch_size, big.mark = ",")))

  result_list <- vector("list", n_batches)

  for (i in seq_len(n_batches)) {
    batch_cfs <- as.character(cf_batches[[i]])
    dt_batch <- dt[cf %chin% batch_cfs]
    pol_batch <- pol_data[cf %chin% batch_cfs]

    if (nrow(pol_batch) > 0) {
      dt_batch[, inizio := as.Date(inizio)]
      dt_batch[, fine := as.Date(fine)]

      pol_events <- data.table::data.table(
        cf = pol_batch$cf,
        event_start = as.Date(pol_batch$DATA_INIZIO),
        event_end = as.Date(pmin(pol_batch$DATA_FINE, as.Date("2024-12-31"))),
        descrizione = pol_batch$descrizione,
        event_name = "pol",
        id_evento = seq_len(nrow(pol_batch))
      )

      # Use memory_safe mode
      dt_batch <- vecshift::add_external_events(
        dt_batch,
        external_events = pol_events,
        event_matching_strategy = "overlap",
        create_synthetic_unemployment = TRUE,
        synthetic_unemployment_duration = 730L,
        memory_safe = TRUE,   # MEMORY OPTIMIZATION
        chunk_size = 5000L    # Smaller chunks
      )

      dt_batch[, inizio := data.table::as.IDate(inizio)]
      dt_batch[, fine := data.table::as.IDate(fine)]
    } else {
      dt_batch[, `:=`(pol_attribute = NA_character_, pol_match_quality = NA_character_)]
    }

    result_list[[i]] <- dt_batch

    # More frequent gc()
    if (i %% 5 == 0 || i == n_batches) {
      cat(sprintf("    Batch %d/%d complete\n", i, n_batches))
      gc()
    }
  }

  dt <- data.table::rbindlist(result_list, use.names = TRUE, fill = TRUE)
  rm(result_list)
  gc()

  # Phase B: Synthetic unemployment for POL-only CFs
  if (nrow(pol_only) > 0) {
    cat(sprintf("  Phase B: Creating synthetic unemployment for %s POL-only CFs...\n",
                format(length(cfs_only_in_pol), big.mark = ",")))

    synthetic_pol <- pol_only[, list(
      inizio = min(DATA_INIZIO),
      fine = pmin(max(DATA_FINE), as.IDate("2024-12-31")),
      pol_attribute = paste(unique(descrizione), collapse = "; ")
    ), by = cf]

    synthetic_pol[, `:=`(
      arco = 0L,
      durata = as.integer(fine - inizio),
      pol_match_quality = "synthetic_pol_only",
      did_attribute = NA_character_,
      did_match_quality = NA_character_
    )]

    missing_cols <- setdiff(names(dt), names(synthetic_pol))
    for (col in missing_cols) {
      synthetic_pol[, (col) := NA]
    }

    dt <- data.table::rbindlist(list(dt, synthetic_pol), use.names = TRUE, fill = TRUE)
    cat(sprintf("  ✓ Added %s synthetic unemployment spells for POL-only CFs\n",
                format(nrow(synthetic_pol), big.mark = ",")))
  }

  n_pol_matched <- sum(!is.na(dt$pol_attribute))
  cat(sprintf("  ✓ Total POL matched: %s\n", format(n_pol_matched, big.mark = ",")))

  # Final IDate conversion
  dt[, inizio := data.table::as.IDate(inizio)]
  dt[, fine := data.table::as.IDate(fine)]

  cat("\n✓ External event matching complete (memory-safe mode)\n")
  cat("=" %+% rep("=", 50) %+% "\n\n")

  return(dt)
}


# 6. Helper operator -----

# String concatenation operator for cleaner output formatting
`%+%` <- function(a, b) paste0(a, b)
