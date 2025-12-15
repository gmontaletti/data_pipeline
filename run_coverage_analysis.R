# run_coverage_analysis.R
# Run annual coverage estimation for workplace branch
# Author: Giampaolo Montaletti (giampaolo.montaletti@gmail.com)

library(targets)

cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("RUNNING ANNUAL COVERAGE ANALYSIS FOR WORKPLACE BRANCH\n")
cat(rep("=", 70), "\n\n", sep = "")

# Check if data_workplace exists
if (!tar_exist_objects("data_workplace")) {
  cat("✗ Error: data_workplace target not found\n")
  cat("\nThe workplace branch data must be created first.\n")
  cat("Run one of the following:\n\n")
  cat("  1. Full pipeline:           tar_make()\n")
  cat("  2. Workplace data only:     tar_make(data_workplace)\n")
  cat("  3. Coverage analysis only:  tar_make(annual_coverage_workplace)\n\n")
  quit(status = 1)
}

# Run coverage analysis targets
cat("Running coverage analysis targets...\n\n")

tar_make(c(
  annual_coverage_workplace,
  output_annual_coverage_workplace
))

# Check if successful
if (tar_exist_objects("annual_coverage_workplace")) {
  cat("\n")
  cat(rep("=", 70), "\n", sep = "")
  cat("✓ COVERAGE ANALYSIS COMPLETE\n")
  cat(rep("=", 70), "\n\n", sep = "")

  # Load and summarize results
  tar_load(annual_coverage_workplace)

  cat("Summary of Annual Coverage:\n")
  cat(sprintf("  Total records: %s\n",
              format(nrow(annual_coverage_workplace), big.mark = ",")))

  # Summary by year
  yearly_summary <- annual_coverage_workplace[, .(
    total_coverage = sum(total_coverage),
    n_comuni = data.table::uniqueN(COMUNE_SEDE_LAVORO),
    n_sectors = data.table::uniqueN(ateco),
    total_workers = sum(n_workers),
    total_contracts = sum(n_contracts)
  ), by = year]

  data.table::setorder(yearly_summary, year)

  cat("\nCoverage by Year:\n")
  print(yearly_summary)

  # Output file location
  output_file <- tar_read(output_annual_coverage_workplace)
  cat(sprintf("\n✓ Results saved to: %s\n", output_file))

} else {
  cat("\n✗ Coverage analysis failed\n")
  cat("Check error messages above for details.\n\n")
  quit(status = 1)
}
