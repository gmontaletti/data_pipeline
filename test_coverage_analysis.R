# test_coverage_analysis.R
# Test script for annual coverage estimation
# Author: Giampaolo Montaletti (giampaolo.montaletti@gmail.com)

library(data.table)
library(targets)

# Load functions
cat("Loading coverage analysis functions...\n")
source("R/coverage_analysis.R")

# Option 1: Load from existing targets pipeline
cat("\nAttempting to load workplace branch data from targets...\n")

# Try to load the enriched workplace data
tryCatch({
  # Load from targets cache
  tar_load(data_enriched_workplace)

  cat(sprintf("✓ Loaded workplace data: %s rows, %s individuals\n",
              format(nrow(data_enriched_workplace), big.mark = ","),
              format(data_enriched_workplace[, uniqueN(cf)], big.mark = ",")))

  # Filter to employment spells only (arco != 0)
  workplace_employment <- data_enriched_workplace[arco != 0]

  cat(sprintf("  Employment spells only: %s rows\n",
              format(nrow(workplace_employment), big.mark = ",")))

  # Run coverage estimation
  cat("\n" %+% rep("=", 70) %+% "\n")
  cat("RUNNING COVERAGE ESTIMATION\n")
  cat(rep("=", 70) %+% "\n")

  coverage_summary <- estimate_annual_coverage(workplace_employment)

  # Save results
  output_dir <- "output/dashboard_workplace"
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  output_file <- file.path(output_dir, "annual_coverage_by_year_comune_sector.fst")
  cat(sprintf("\nSaving results to: %s\n", output_file))
  fst::write_fst(coverage_summary, output_file, compress = 85)

  cat(sprintf("✓ Saved %s records\n", format(nrow(coverage_summary), big.mark = ",")))

  # Print summary by year
  cat("\n" %+% rep("=", 70) %+% "\n")
  cat("COVERAGE SUMMARY BY YEAR\n")
  cat(rep("=", 70) %+% "\n\n")

  yearly_summary <- coverage_summary[, .(
    total_coverage = sum(total_coverage),
    n_comuni = uniqueN(COMUNE_SEDE_LAVORO),
    n_sectors = uniqueN(ateco),
    n_workers = sum(n_workers),
    mean_coverage_per_combo = mean(total_coverage)
  ), by = year]

  setorder(yearly_summary, year)
  print(yearly_summary)

  cat("\n✓ Coverage analysis complete!\n")

}, error = function(e) {
  cat("✗ Error loading workplace data from targets:\n")
  cat(sprintf("  %s\n", e$message))
  cat("\nPlease ensure the pipeline has been run with:\n")
  cat("  tar_make()\n")
  cat("\nOr run specific targets:\n")
  cat("  tar_make(data_enriched_workplace)\n")
})

# Option 2: Create simple test cases for validation
cat("\n" %+% rep("=", 70) %+% "\n")
cat("VALIDATION TESTS\n")
cat(rep("=", 70) %+% "\n\n")

# Test case 1: Full-year full-time contract
cat("Test 1: Full-year full-time contract (should have coverage ≈ 1)\n")
test1 <- data.table(
  cf = "TEST001",
  inizio = as.IDate("2023-01-01"),
  fine = as.IDate("2023-12-31"),
  prior = 1,  # Full-time
  ore = 40,   # 40 hours/week
  ateco = "01",
  COMUNE_SEDE_LAVORO = "TEST_COMUNE",
  arco = 1
)

result1 <- estimate_annual_coverage(test1)
cat(sprintf("  Expected: ≈1.0, Got: %.3f\n", result1$total_coverage))
cat(sprintf("  %s\n\n", ifelse(abs(result1$total_coverage - 1.0) < 0.01, "✓ PASS", "✗ FAIL")))

# Test case 2: Half-year full-time contract
cat("Test 2: Half-year full-time contract (should have coverage ≈ 0.5)\n")
test2 <- data.table(
  cf = "TEST002",
  inizio = as.IDate("2023-01-01"),
  fine = as.IDate("2023-06-30"),
  prior = 1,  # Full-time
  ore = 40,
  ateco = "02",
  COMUNE_SEDE_LAVORO = "TEST_COMUNE",
  arco = 1
)

result2 <- estimate_annual_coverage(test2)
expected2 <- (181 / 365) * 1.0  # 181 days in first half of 2023
cat(sprintf("  Expected: ≈%.3f, Got: %.3f\n", expected2, result2$total_coverage))
cat(sprintf("  %s\n\n", ifelse(abs(result2$total_coverage - expected2) < 0.01, "✓ PASS", "✗ FAIL")))

# Test case 3: Full-year part-time contract (20 hours/week)
cat("Test 3: Full-year part-time (20h/week) contract (should have coverage ≈ 0.5)\n")
test3 <- data.table(
  cf = "TEST003",
  inizio = as.IDate("2023-01-01"),
  fine = as.IDate("2023-12-31"),
  prior = 0,  # Part-time
  ore = 20,
  ateco = "03",
  COMUNE_SEDE_LAVORO = "TEST_COMUNE",
  arco = 1
)

result3 <- estimate_annual_coverage(test3)
cat(sprintf("  Expected: ≈0.5, Got: %.3f\n", result3$total_coverage))
cat(sprintf("  %s\n\n", ifelse(abs(result3$total_coverage - 0.5) < 0.01, "✓ PASS", "✗ FAIL")))

# Test case 4: Multi-year contract
cat("Test 4: Contract spanning two years\n")
test4 <- data.table(
  cf = "TEST004",
  inizio = as.IDate("2022-07-01"),
  fine = as.IDate("2023-06-30"),
  prior = 1,  # Full-time
  ore = 40,
  ateco = "04",
  COMUNE_SEDE_LAVORO = "TEST_COMUNE",
  arco = 1
)

result4 <- estimate_annual_coverage(test4)
cat(sprintf("  Should create 2 records (one for 2022, one for 2023)\n"))
cat(sprintf("  Got: %d records\n", nrow(result4)))
cat(sprintf("  2022 coverage: %.3f\n", result4[year == 2022, total_coverage]))
cat(sprintf("  2023 coverage: %.3f\n", result4[year == 2023, total_coverage]))
cat(sprintf("  Total: %.3f (should be ≈1.0)\n", sum(result4$total_coverage)))
cat(sprintf("  %s\n\n", ifelse(abs(sum(result4$total_coverage) - 1.0) < 0.01 & nrow(result4) == 2,
                                "✓ PASS", "✗ FAIL")))

cat("All validation tests complete!\n")
