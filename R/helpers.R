# Helper functions for longworkR data processing pipeline

#' Create consolidation comparison report
#'
#' @param stages List of data.tables from different consolidation stages
#' @return Summary data.table
compare_consolidation_stages <- function(stages) {
  data.table::data.table(
    stage = names(stages),
    records = sapply(stages, nrow),
    unique_persons = sapply(stages, function(x) data.table::uniqueN(x$cf))
  )
}

#' Extract employment statistics
#'
#' @param data Consolidated employment data
#' @return List of statistics
employment_statistics <- function(data) {
  list(
    total_records = nrow(data),
    unique_persons = data.table::uniqueN(data$cf),
    total_employment_days = sum(data$durata[data$arco == 1], na.rm = TRUE),
    total_unemployment_days = sum(data$durata[data$arco == 0], na.rm = TRUE),
    avg_employment_duration = mean(data$durata[data$arco == 1], na.rm = TRUE),
    avg_unemployment_duration = mean(data$durata[data$arco == 0], na.rm = TRUE)
  )
}
