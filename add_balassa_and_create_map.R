# add_balassa_and_create_map.R
# Script to add Balassa index to coverage data and create specialization map
# Author: Giampaolo Montaletti (giampaolo.montaletti@gmail.com)

library(data.table)
library(fst)
library(sf)
library(targets)

# Load functions
source("R/balassa_specialization.R")

cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("ADD BALASSA INDEX AND CREATE SPECIALIZATION MAP\n")
cat(rep("=", 70), "\n\n", sep = "")

# 1. Load coverage data -----
cat("STEP 1: Loading coverage data\n")
cat(rep("-", 70), "\n", sep = "")

coverage_file <- "output/dashboard_workplace/annual_coverage_by_year_comune_sector.fst"

if (!file.exists(coverage_file)) {
  stop("Coverage file not found: ", coverage_file, "\n",
       "Please run the coverage analysis first with: Rscript run_coverage_analysis.R")
}

coverage_data <- read_fst(coverage_file, as.data.table = TRUE)

cat(sprintf("Loaded coverage data: %s records\n", format(nrow(coverage_data), big.mark = ",")))
cat("Columns:", paste(names(coverage_data), collapse = ", "), "\n")
cat(sprintf("Years: %d to %d\n", min(coverage_data$year), max(coverage_data$year)))

# 2. Calculate Balassa Index -----
cat("\nSTEP 2: Calculating Balassa Index\n")
cat(rep("-", 70), "\n", sep = "")

# Check if already exists
if ("balassa_index" %in% names(coverage_data)) {
  cat("Balassa index already exists. Recalculating...\n")
  coverage_data[, balassa_index := NULL]
}

coverage_data <- calculate_balassa_index(coverage_data)

# 3. Update FST file -----
cat("\nSTEP 3: Updating coverage file with Balassa index\n")
cat(rep("-", 70), "\n", sep = "")

write_fst(coverage_data, coverage_file, compress = 85)
cat(sprintf("Updated file: %s\n", coverage_file))
cat(sprintf("File size: %.2f MB\n", file.size(coverage_file) / 1024^2))

# 4. Load classifiers for sector labels -----
cat("\nSTEP 4: Loading classifiers\n")
cat(rep("-", 70), "\n", sep = "")

tar_load(classifiers)
cat("Classifiers loaded\n")

# 5. Create specialization sf dataframe for 2024 -----
cat("\nSTEP 5: Creating specialization sf dataframe for 2024\n")
cat(rep("-", 70), "\n", sep = "")

comuni_spec_sf <- create_specialization_sf(
  coverage_data,
  year_filter = 2024,
  classifiers = classifiers,
  crs_output = "EPSG:4326"  # WGS84 for web mapping
)

# 6. Save specialization sf dataframe -----
cat("\nSTEP 6: Saving specialization sf dataframe\n")
cat(rep("-", 70), "\n", sep = "")

output_dir <- "output/dashboard_workplace"
output_file <- file.path(output_dir, "comuni_specialization_2024.rds")

saveRDS(comuni_spec_sf, output_file)
cat(sprintf("Saved: %s\n", output_file))
cat(sprintf("File size: %.2f MB\n", file.size(output_file) / 1024^2))

# 7. Create summary report -----
cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("SUMMARY REPORT\n")
cat(rep("=", 70), "\n\n", sep = "")

cat("Files created:\n")
cat(sprintf("  1. %s (updated with Balassa index)\n", coverage_file))
cat(sprintf("  2. %s (sf dataframe for dashboard)\n", output_file))

cat("\nBalassa Index Statistics:\n")
cat(sprintf("  Records with RCA > 1 (specialized): %s (%.1f%%)\n",
            format(sum(coverage_data$balassa_index > 1, na.rm = TRUE), big.mark = ","),
            100 * mean(coverage_data$balassa_index > 1, na.rm = TRUE)))
cat(sprintf("  Records with RCA > 1.5 (highly specialized): %s (%.1f%%)\n",
            format(sum(coverage_data$balassa_index > 1.5, na.rm = TRUE), big.mark = ","),
            100 * mean(coverage_data$balassa_index > 1.5, na.rm = TRUE)))
cat(sprintf("  Records with RCA > 2 (very highly specialized): %s (%.1f%%)\n",
            format(sum(coverage_data$balassa_index > 2, na.rm = TRUE), big.mark = ","),
            100 * mean(coverage_data$balassa_index > 2, na.rm = TRUE)))

cat("\n2024 Specialization Summary:\n")
comuni_2024 <- coverage_data[year == 2024]
cat(sprintf("  Total FTE in 2024: %.0f\n", sum(comuni_2024$total_coverage)))
cat(sprintf("  Comuni: %d\n", uniqueN(comuni_2024$COMUNE_SEDE_LAVORO)))
cat(sprintf("  Sectors: %d\n", uniqueN(comuni_2024$ateco)))

cat("\nTop 5 Most Specialized Combinations (2024):\n")
top5 <- head(comuni_2024[order(-balassa_index)], 5)
print(top5[, .(COMUNE_SEDE_LAVORO, ateco, total_coverage, n_workers, balassa_index = round(balassa_index, 2))])

cat("\n")
cat(rep("=", 70), "\n", sep = "")
cat("PROCESS COMPLETE\n")
cat(rep("=", 70), "\n\n", sep = "")

cat("To visualize the specialization map, load the sf dataframe:\n")
cat("  comuni_sf <- readRDS('", output_file, "')\n", sep = "")
cat("  library(sf)\n")
cat("  library(ggplot2)\n")
cat("  ggplot(comuni_sf) + geom_sf(aes(fill = n_specializations))\n")
