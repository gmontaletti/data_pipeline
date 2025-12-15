# Install MONECA package from GitHub
if (!require('devtools', quietly = TRUE)) {
  install.packages('devtools', repos='https://cloud.r-project.org')
}

devtools::install_github('gmontaletti/MONECA')

# Verify installation
if (require('MONECA', quietly = TRUE)) {
  cat("MONECA installed successfully\n")
  cat("Version:", as.character(packageVersion("MONECA")), "\n")
} else {
  stop("MONECA installation failed")
}
