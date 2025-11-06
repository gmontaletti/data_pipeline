# transitions.R
# Functions for computing employment transitions and unemployment periods
# Author: Giampaolo Montaletti (giampaolo.montaletti@gmail.com)

# 1. Extract demographics -----

#' Extract person-level demographics from employment spells
#'
#' Aggregates person-level demographic information by taking the mode (most frequent)
#' value across all employment spells for each individual.
#'
#' @param dt A data.table containing employment spells with demographic variables
#'
#' @return A data.table with one row per person (cf) containing:
#'   \itemize{
#'     \item cf: Person identifier
#'     \item sesso: Gender (mode value)
#'     \item eta: Age (mode value)
#'     \item cpi_code: CPI area code (mode value)
#'     \item cpi_name: CPI area name (mode value)
#'     \item istruzione: Education level (mode value)
#'     \item professione: Profession code (mode from qualifica)
#'     \item ateco_3digit: Economic sector (mode value)
#'     \item cleta: Age class ("15-35", "35-55", "over 55")
#'   }
#'
#' @details
#' The function uses fmode() to compute the most frequent value for each demographic
#' variable across a person's employment history. Only spells with non-NA gender values
#' are considered to ensure data quality.
#'
#' Age classes are created as:
#' - "15-35": Under 35 years old
#' - "35-55": 35 to 54 years old
#' - "over 55": 55 years and older
#'
#' @examples
#' \dontrun{
#' anag <- extract_demographics(dt)
#' }
#'
#' @export
extract_demographics <- function(dt) {

  if (!data.table::is.data.table(dt)) {
    stop("Input must be a data.table")
  }

  required_cols <- c("cf", "sesso")
  missing_cols <- setdiff(required_cols, names(dt))
  if (length(missing_cols) > 0) {
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }

  cat("Extracting person-level demographics...\n")

  # Helper function for mode (most frequent value)
  fmode <- function(x) {
    ux <- unique(x)
    ux[which.max(tabulate(match(x, ux)))]
  }

  # Extract demographics (mode values per person from spells with non-NA sesso)
  anag <- dt[!is.na(sesso), .(
    sesso = fmode(sesso),
    eta = if ("eta" %in% names(dt)) fmode(eta) else NA_real_,
    cpi_code = if ("cpi_code" %in% names(dt)) fmode(cpi_code) else NA_character_,
    cpi_name = if ("cpi_name" %in% names(dt)) fmode(cpi_name) else NA_character_,
    istruzione = if ("istruzione" %in% names(dt)) fmode(istruzione) else NA_character_,
    professione = if ("qualifica" %in% names(dt)) fmode(qualifica) else NA_character_,
    ateco_3digit = if ("ateco_3digit" %in% names(dt)) fmode(ateco_3digit) else NA_character_
  ), by = cf]

  # Create age classes
  if ("eta" %in% names(anag) && !all(is.na(anag$eta))) {
    anag[, cleta := data.table::fcase(
      eta < 35, "15-35",
      eta >= 35 & eta < 55, "35-55",
      default = "over 55"
    )]
  } else {
    anag[, cleta := NA_character_]
  }

  cat("  Extracted demographics for", nrow(anag), "individuals\n")

  return(anag)
}


# 2. Compute closed transitions -----

#' Compute closed transitions (Employment → Employment)
#'
#' Creates a dataset of employment-to-employment transitions, tracking job changes
#' and the unemployment duration between consecutive employment spells.
#'
#' @param dt A data.table containing employment spells. Must include columns:
#'   cf, arco, inizio, fine, COD_TIPOLOGIA_CONTRATTUALE, qualifica, ateco_3digit
#'
#' @return A data.table with one row per closed transition containing:
#'   \itemize{
#'     \item cf: Person identifier
#'     \item transition_date: Date when previous employment ended
#'     \item from_contract: Contract type code of origin job
#'     \item to_contract: Contract type code of destination job
#'     \item from_profession: Profession code of origin job
#'     \item to_profession: Profession code of destination job
#'     \item from_sector: ATECO sector of origin job
#'     \item to_sector: ATECO sector of destination job
#'     \item unemployment_duration: Days between job end and next job start
#'     \item cpi_code: CPI area code
#'     \item cpi_name: CPI area name
#'     \item month: Month of transition
#'     \item year: Year of transition
#'     \item is_closed: TRUE (indicator for closed transitions)
#'     \item is_open: FALSE (indicator for open transitions)
#'   }
#'
#' @details
#' A closed transition occurs when a person moves from one employment spell to
#' another, with a measurable unemployment period in between. The function:
#' - Filters to employment spells only (arco > 0)
#' - Orders by person and start date
#' - Computes next employment spell characteristics using shift()
#' - Calculates unemployment duration as time gap between spells
#' - Handles geographic information for unemployment periods
#'
#' @examples
#' \dontrun{
#' closed_trans <- compute_closed_transitions(dt)
#' }
#'
#' @export
compute_closed_transitions <- function(dt) {

  if (!data.table::is.data.table(dt)) {
    stop("Input must be a data.table")
  }

  required_cols <- c("cf", "arco", "inizio", "fine", "COD_TIPOLOGIA_CONTRATTUALE")
  missing_cols <- setdiff(required_cols, names(dt))
  if (length(missing_cols) > 0) {
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }

  cat("Computing closed transitions (employment -> employment)...\n")

  # Filter to EMPLOYMENT spells only (remove unemployment arco==0)
  dt_employment <- dt[arco > 0]
  data.table::setorder(dt_employment, cf, inizio)

  # For unemployment spells (NA cpi), inherit geography from previous employment spell
  if ("cpi_code" %in% names(dt_employment)) {
    dt_employment[, prev_cpi_code := data.table::shift(cpi_code, type = "lag", fill = NA), by = cf]
    dt_employment[is.na(cpi_code), cpi_code := prev_cpi_code]
    dt_employment[, prev_cpi_code := NULL]
  }

  if ("cpi_name" %in% names(dt_employment)) {
    dt_employment[, prev_cpi_name := data.table::shift(cpi_name, type = "lag", fill = NA), by = cf]
    dt_employment[is.na(cpi_name), cpi_name := prev_cpi_name]
    dt_employment[, prev_cpi_name := NULL]
  }

  # Shift to get next employment spell
  dt_employment[, `:=`(
    next_contract = data.table::shift(COD_TIPOLOGIA_CONTRATTUALE, type = "lead"),
    next_profession = data.table::shift(qualifica, type = "lead"),
    next_ateco = data.table::shift(ateco, type = "lead"),
    next_ateco_3digit = data.table::shift(ateco_3digit, type = "lead"),
    next_inizio = data.table::shift(inizio, type = "lead"),
    next_retribuzione = data.table::shift(retribuzione, type = "lead"),
    is_last_employment = is.na(data.table::shift(inizio, type = "lead"))
  ), by = cf]

  # Create closed transitions data.table
  closed_transitions <- dt_employment[!is.na(next_contract), .(
    cf,
    transition_date = fine,
    from_contract = COD_TIPOLOGIA_CONTRATTUALE,
    to_contract = next_contract,
    from_profession = qualifica,
    to_profession = next_profession,
    from_sector = ateco_3digit,
    to_sector = next_ateco_3digit,
    unemployment_duration = as.numeric(next_inizio - fine),
    from_salary = retribuzione,
    to_salary = next_retribuzione,
    cpi_code,
    cpi_name,
    month = data.table::month(fine),
    year = data.table::year(fine),
    is_closed = TRUE,
    is_open = FALSE
  )]

  cat("  Created", nrow(closed_transitions), "closed transitions\n")

  return(closed_transitions)
}


# 3. Compute open transitions -----

#' Compute open transitions (Employment → Unemployment at observation end)
#'
#' Creates a dataset of transitions from employment into ongoing unemployment,
#' representing workers whose last employment spell ended before the observation
#' end date with no subsequent employment.
#'
#' @param dt A data.table containing employment spells. Must include columns:
#'   cf, arco, inizio, fine, troncata, COD_TIPOLOGIA_CONTRATTUALE, qualifica,
#'   ateco_3digit
#' @param observation_end Date representing the end of the observation period.
#'   Default is as.Date("2024-10-20").
#'
#' @return A data.table with one row per open transition containing:
#'   \itemize{
#'     \item cf: Person identifier
#'     \item transition_date: Date when last employment ended
#'     \item from_contract: Contract type code of last job
#'     \item to_contract: NA (no next job)
#'     \item from_profession: Profession code of last job
#'     \item to_profession: NA (no next job)
#'     \item from_sector: ATECO sector of last job
#'     \item to_sector: NA (no next sector)
#'     \item unemployment_duration: Days from job end to observation end
#'     \item cpi_code: CPI area code
#'     \item cpi_name: CPI area name
#'     \item month: Month of transition
#'     \item year: Year of transition
#'     \item is_closed: FALSE (indicator for closed transitions)
#'     \item is_open: TRUE (indicator for open transitions)
#'   }
#'
#' @details
#' An open transition occurs when a person's last employment spell ended and
#' they have not found new employment by the observation end date. The function:
#' - Identifies last employment spell per person (using lead shift)
#' - Filters to non-censored spells (troncata == FALSE)
#' - Calculates unemployment duration to observation end
#'
#' @examples
#' \dontrun{
#' open_trans <- compute_open_transitions(dt, observation_end = as.Date("2024-10-20"))
#' }
#'
#' @export
compute_open_transitions <- function(dt, observation_end = data.table::as.IDate("2024-10-20")) {

  if (!data.table::is.data.table(dt)) {
    stop("Input must be a data.table")
  }

  required_cols <- c("cf", "arco", "inizio", "fine", "troncata", "COD_TIPOLOGIA_CONTRATTUALE")
  missing_cols <- setdiff(required_cols, names(dt))
  if (length(missing_cols) > 0) {
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }

  cat("Computing open transitions (employment -> ongoing unemployment)...\n")

  # Filter to employment spells
  dt_employment <- dt[arco > 0]
  data.table::setorder(dt_employment, cf, inizio)

  # Identify last employment spell
  dt_employment[, is_last_employment := is.na(data.table::shift(inizio, type = "lead")), by = cf]

  # Last employment spell + not censored + no next job
  open_transitions <- dt_employment[is_last_employment == TRUE & troncata == FALSE, .(
    cf,
    transition_date = fine,
    from_contract = COD_TIPOLOGIA_CONTRATTUALE,
    to_contract = NA_character_,
    from_profession = qualifica,
    to_profession = NA_character_,
    from_sector = ateco_3digit,
    to_sector = NA_character_,
    unemployment_duration = as.numeric(observation_end - fine),
    from_salary = retribuzione,
    to_salary = NA_real_,
    cpi_code,
    cpi_name,
    month = data.table::month(fine),
    year = data.table::year(fine),
    is_closed = FALSE,
    is_open = TRUE
  )]

  cat("  Created", nrow(open_transitions), "open transitions\n")

  return(open_transitions)
}


# 4. Add DID/POL to transitions -----

#' Add DID/POL support information to transitions
#'
#' Matches unemployment spells containing DID (Dote Impresa) and POL (Politiche
#' Attive) support attributes to transition periods, enriching transitions with
#' policy support information.
#'
#' @param transitions A data.table of transitions (combined closed + open)
#' @param dt_unemployment A data.table of unemployment spells with DID/POL attributes.
#'   If NULL, extracts from the original dt. Must contain columns: cf, inizio, fine,
#'   did_attribute, did_match_quality, pol_attribute, pol_match_quality
#'
#' @return The transitions data.table with added columns:
#'   \itemize{
#'     \item did_active: Maximum DID attribute value during unemployment period
#'     \item did_quality: Match quality for DID
#'     \item pol_active: Maximum POL attribute value during unemployment period
#'     \item pol_quality: Match quality for POL
#'     \item has_did: Boolean flag (TRUE if DID was active)
#'     \item has_pol: Boolean flag (TRUE if POL was active)
#'   }
#'
#' @details
#' The function performs a non-equi join to match unemployment spell records
#' (arco == 0) with DID/POL attributes to transition unemployment periods. For
#' each transition:
#' - The unemployment period is from transition_date to (transition_date + unemployment_duration)
#' - Any unemployment spell overlapping this period contributes DID/POL information
#' - Multiple matches are aggregated (max for attributes, first for quality)
#'
#' @examples
#' \dontrun{
#' transitions <- add_did_pol_to_transitions(transitions, dt_unemployment)
#' }
#'
#' @export
add_did_pol_to_transitions <- function(transitions, dt_unemployment = NULL) {

  if (!data.table::is.data.table(transitions)) {
    stop("transitions must be a data.table")
  }

  required_cols <- c("cf", "transition_date", "unemployment_duration")
  missing_cols <- setdiff(required_cols, names(transitions))
  if (length(missing_cols) > 0) {
    stop("Missing required columns in transitions: ", paste(missing_cols, collapse = ", "))
  }

  cat("Adding DID/POL information from unemployment spells...\n")

  # Validate or extract unemployment data
  if (is.null(dt_unemployment)) {
    stop("dt_unemployment parameter is required. Extract unemployment spells with ",
         "DID/POL attributes from the original data.")
  }

  if (!data.table::is.data.table(dt_unemployment)) {
    stop("dt_unemployment must be a data.table")
  }

  if (nrow(dt_unemployment) == 0) {
    warning("No unemployment spells with DID/POL attributes found")
    # Create empty columns
    transitions[, `:=`(
      did_active = NA_real_,
      did_quality = NA_character_,
      pol_active = NA_real_,
      pol_quality = NA_character_,
      has_did = FALSE,
      has_pol = FALSE
    )]
    return(transitions)
  }

  # Set keys for joining
  data.table::setkey(transitions, cf, transition_date)
  data.table::setkey(dt_unemployment, cf, unemp_start)

  # For each transition, find overlapping unemployment spells
  transitions[, `:=`(
    unemp_period_start = transition_date,
    unemp_period_end = transition_date + unemployment_duration
  )]

  # Non-equi join: find unemployment spells overlapping with transition unemployment period
  matched <- dt_unemployment[transitions, on = .(
    cf,
    unemp_start <= unemp_period_end,
    unemp_end >= unemp_period_start
  ), nomatch = NULL]

  # Aggregate by transition (in case multiple unemployment records match)
  matched_agg <- matched[, .(
    did_active = max(did_attribute, na.rm = TRUE),
    did_quality = data.table::first(did_match_quality),
    pol_active = max(pol_attribute, na.rm = TRUE),
    pol_quality = data.table::first(pol_match_quality)
  ), by = .(cf, transition_date)]

  # Merge back to transitions
  transitions <- merge(transitions, matched_agg, by = c("cf", "transition_date"), all.x = TRUE)
  transitions[, `:=`(unemp_period_start = NULL, unemp_period_end = NULL)]

  # Create boolean flags
  transitions[, `:=`(
    has_did = !is.na(did_active) & did_active > 0,
    has_pol = !is.na(pol_active) & pol_active > 0
  )]

  # Count matches
  n_with_did <- sum(transitions$has_did, na.rm = TRUE)
  n_with_pol <- sum(transitions$has_pol, na.rm = TRUE)

  cat("  Matched DID to", n_with_did, "transitions\n")
  cat("  Matched POL to", n_with_pol, "transitions\n")

  return(transitions)
}


# 5. Create transition matrices -----

#' Create transition matrices from transitions table
#'
#' Aggregates closed transitions into frequency matrices showing transitions
#' between contract types, professions, and economic sectors.
#'
#' @param transitions A data.table containing transitions with columns:
#'   is_closed, from_contract, to_contract, from_profession, to_profession,
#'   from_sector, to_sector
#'
#' @return A list with three data.tables:
#'   \itemize{
#'     \item contract: Transitions between contract types (from, to, N)
#'     \item profession: Transitions between professions (from, to, N)
#'     \item sector: Transitions between economic sectors (from, to, N)
#'   }
#'
#' @details
#' Each matrix is a simple edge list with three columns:
#' - from: Origin state code
#' - to: Destination state code
#' - N: Number of transitions observed
#'
#' Only closed transitions (employment → employment) are included in the matrices.
#' Open transitions (employment → unemployment) are excluded since they have no
#' destination state.
#'
#' @examples
#' \dontrun{
#' matrices <- create_transition_matrices(transitions)
#' contract_matrix <- matrices$contract
#' }
#'
#' @export
create_transition_matrices <- function(transitions) {

  if (!data.table::is.data.table(transitions)) {
    stop("transitions must be a data.table")
  }

  required_cols <- c("is_closed", "from_contract", "to_contract")
  missing_cols <- setdiff(required_cols, names(transitions))
  if (length(missing_cols) > 0) {
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }

  cat("Generating transition matrices from transitions table...\n")

  # Simple aggregation by (from, to) pairs - only closed transitions
  contract_matrix <- transitions[is_closed == TRUE, .N, by = .(from_contract, to_contract)]
  data.table::setnames(contract_matrix, c("from", "to", "N"))

  profession_matrix <- transitions[is_closed == TRUE, .N, by = .(from_profession, to_profession)]
  data.table::setnames(profession_matrix, c("from", "to", "N"))

  sector_matrix <- transitions[is_closed == TRUE, .N, by = .(from_sector, to_sector)]
  data.table::setnames(sector_matrix, c("from", "to", "N"))

  cat("  Contract matrix:", nrow(contract_matrix), "transition pairs\n")
  cat("  Profession matrix:", nrow(profession_matrix), "transition pairs\n")
  cat("  Sector matrix:", nrow(sector_matrix), "transition pairs\n")

  transition_matrices <- list(
    contract = contract_matrix,
    profession = profession_matrix,
    sector = sector_matrix
  )

  return(transition_matrices)
}
