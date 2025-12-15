# db_functions.R
# Database connection and data fetching utilities
# Author: Giampaolo Montaletti (giampaolo.montaletti@gmail.com)
#
# Provides PostgreSQL connectivity for loading employment data from
# the staging.mv_rapporti_cleaned materialized view.

# 1. PostgreSQL connection -----

#' Check if PostgreSQL connection is available
#'
#' Verifies that all required environment variables for database
#' connection are configured.
#'
#' @return Logical indicating if DB credentials are configured
#' @export
db_available <- function() {
  all(nzchar(c(
    Sys.getenv("POSTGRES_HOST"),
    Sys.getenv("POSTGRES_USER"),
    Sys.getenv("POSTGRES_PASSWORD"),
    Sys.getenv("POSTGRES_DB")
  )))
}


#' Get PostgreSQL connection using RPostgres
#'
#' Connects to PostgreSQL using credentials from .Renviron
#'
#' @return DBI connection object
#' @export
get_pg_connection <- function() {
  if (!db_available()) {
    stop("PostgreSQL credentials not configured. ",
         "Please set POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, ",
         "and POSTGRES_DB in .Renviron")
  }

  DBI::dbConnect(
    RPostgres::Postgres(),
    host = Sys.getenv("POSTGRES_HOST"),
    port = as.integer(Sys.getenv("POSTGRES_PORT", "5432")),
    dbname = Sys.getenv("POSTGRES_DB"),
    user = Sys.getenv("POSTGRES_USER"),
    password = Sys.getenv("POSTGRES_PASSWORD")
  )
}


# 2. Materialized view management -----

#' Check if materialized view exists
#'
#' @param con DBI connection
#' @param schema Schema name
#' @param mv_name Materialized view name
#' @return Logical
#' @export
mv_exists <- function(con, schema = "staging", mv_name = "mv_rapporti_cleaned") {
  query <- sprintf(
    "SELECT EXISTS (
      SELECT 1 FROM pg_matviews
      WHERE schemaname = '%s' AND matviewname = '%s'
    )",
    schema, mv_name
  )
  result <- DBI::dbGetQuery(con, query)
  return(result[[1]])
}


#' Verify or refresh materialized view
#'
#' Checks if the materialized view exists. Optionally refreshes if requested.
#' Note: This function does NOT create the MV - it should already exist
#' (created by rapporti_2025_11 pipeline).
#'
#' @param con DBI connection (optional, will create if NULL)
#' @param refresh Logical, whether to refresh existing MV
#' @return TRUE on success
#' @export
ensure_materialized_view <- function(con = NULL, refresh = FALSE) {

  close_con <- FALSE
  if (is.null(con)) {
    con <- get_pg_connection()
    close_con <- TRUE
  }

  on.exit({
    if (close_con) DBI::dbDisconnect(con)
  })

  exists <- mv_exists(con)

  if (!exists) {
    stop("Materialized view staging.mv_rapporti_cleaned does not exist. ",
         "Please run the rapporti_2025_11 pipeline first to create it.")
  }

  if (refresh) {
    cat("Refreshing materialized view...\n")
    DBI::dbExecute(con, "REFRESH MATERIALIZED VIEW staging.mv_rapporti_cleaned;")
    cat("Materialized view refreshed.\n")
  } else {
    cat("Materialized view staging.mv_rapporti_cleaned exists.\n")
  }

  return(TRUE)
}


# 3. Data fetching -----

#' Fetch data from materialized view
#'
#' Queries the materialized view and returns a data.table.
#' Note: Column names are lowercase as output by the MV.
#' Use map_db_columns() to convert to pipeline format.
#'
#' @param con DBI connection (optional, will create if NULL)
#' @return data.table with employment records (lowercase column names)
#' @export
fetch_from_mv <- function(con = NULL) {

  close_con <- FALSE
  if (is.null(con)) {
    con <- get_pg_connection()
    close_con <- TRUE
  }

  on.exit({
    if (close_con) DBI::dbDisconnect(con)
  })

  cat("Fetching data from staging.mv_rapporti_cleaned...\n")
  start_time <- Sys.time()

  result <- DBI::dbGetQuery(con, "SELECT * FROM staging.mv_rapporti_cleaned")
  data.table::setDT(result)

  elapsed <- difftime(Sys.time(), start_time, units = "secs")

  cat(sprintf("  Retrieved %s records in %.1f seconds\n",
              format(nrow(result), big.mark = ","),
              as.numeric(elapsed)))

  # Set key for efficient operations
  data.table::setkey(result, cf, inizio, fine)

  return(result)
}
