#!/bin/bash

echo "=========================================="
echo "longworkR Data Pipeline Update"
echo "=========================================="
echo ""

cd ~/Documents/funzioni/data_pipeline

echo "Starting pipeline execution..."
echo ""

Rscript -e "
  library(targets)
  tar_make()

  # Print validation report
  cat('\n\n')
  cat('==========================================\n')
  cat('VALIDATION REPORT\n')
  cat('==========================================\n')

  if (tar_exist_objects('validation_report')) {
    report <- tar_read(validation_report)
    print(report\$summary)
  } else {
    cat('Validation report not available yet.\n')
  }

  cat('\n\nPipeline completed successfully!\n')
  cat('Output files in: output/\n')
  cat('\nRestart dashboards to see updated data.\n')
"

echo ""
echo "=========================================="
echo "Pipeline execution completed"
echo "=========================================="
