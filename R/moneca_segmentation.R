# MONECA Labor Market Segmentation
#
# This module implements MONECA clustering on sector mobility matrices
# to identify labor market segments and generate naming templates for
# manual interpretation.
#
# Author: Giampaolo Montaletti
# Email: giampaolo.montaletti@gmail.com
# GitHub: github.com/gmontaletti
# ORCID: https://orcid.org/0009-0002-5327-1122

# 1. Matrix Preparation -----

#' Prepare Mobility Matrix for MONECA
#'
#' Adds row/column margin totals to a mobility matrix for MONECA analysis.
#' MONECA functions (moneca_fast, find_optimal_cutoff) require matrices
#' WITH margins (row/column totals in the last row/column).
#'
#' @param mobility_matrix R matrix (n x n) with sector labels as row/column names
#'
#' @return Matrix with added margins (row/column totals)
#'
#' @export
prepare_matrix_for_moneca <- function(mobility_matrix) {
  addmargins(mobility_matrix)
}

# 2. MONECA Clustering -----

#' Cluster Labor Market Using MONECA Fast
#'
#' Applies MONECA hierarchical network segmentation to a sector mobility matrix
#' using a pre-computed optimal cutoff threshold. This is significantly faster
#' than auto-tuning (5-7x speedup) while maintaining optimal performance.
#'
#' @param mobility_matrix R matrix with row and column totals in the last row/column.
#'   IMPORTANT: This matrix MUST include margins (use prepare_matrix_for_moneca()
#'   or addmargins() before passing to this function). The moneca_fast() function
#'   requires matrices with margins to calculate relative risk.
#' @param cut_off Numeric. Minimum relative risk threshold (from cutoff analysis)
#' @param segment_levels Integer. Number of hierarchical levels (default: 3)
#' @param verbose Logical. Print progress messages (default: TRUE)
#'
#' @return List with components:
#'   - moneca_object: Full MONECA output (hierarchy, parameters, quality)
#'   - membership: data.table with sector, segment_id, segment_size
#'   - n_segments: Number of identified segments
#'
#' @export
cluster_labor_market <- function(mobility_matrix,
                                  cut_off,
                                  segment_levels = 3,
                                  verbose = TRUE) {

  # 1. Validate inputs -----

  # Check cutoff parameter
  if (is.null(cut_off)) {
    stop("cut_off parameter is NULL. Please provide a numeric cutoff value.")
  }

  if (!is.numeric(cut_off) || length(cut_off) != 1) {
    stop("cut_off must be a single numeric value. Got: ",
         class(cut_off), " of length ", length(cut_off))
  }

  if (is.na(cut_off) || !is.finite(cut_off)) {
    stop("cut_off must be a finite numeric value. Got: ", cut_off)
  }

  # Check matrix parameter
  if (is.null(mobility_matrix)) {
    stop("mobility_matrix parameter is NULL. Please provide a valid matrix.")
  }

  if (!is.matrix(mobility_matrix)) {
    stop("mobility_matrix must be a matrix. Got: ", class(mobility_matrix)[1])
  }

  if (nrow(mobility_matrix) != ncol(mobility_matrix)) {
    stop("mobility_matrix must be square. Got dimensions: ",
         nrow(mobility_matrix), " x ", ncol(mobility_matrix))
  }

  # Check for and handle NAs
  if (any(is.na(mobility_matrix))) {
    n_na <- sum(is.na(mobility_matrix))
    if (verbose) {
      warning("Matrix contains ", n_na, " NA values. Replacing with 0.")
    }
    mobility_matrix[is.na(mobility_matrix)] <- 0
  }

  # 2. Run MONECA clustering -----

  if (verbose) {
    cat("Starting MONECA fast clustering\n")
    cat("Matrix dimensions:", nrow(mobility_matrix), "x", ncol(mobility_matrix), "\n")
    cat("Using cutoff:", cut_off, "\n")
    cat("Segment levels:", segment_levels, "\n")
  }

  # Run MONECA fast (no auto-tuning)
  moneca_result <- moneca::moneca_fast(
    mx = mobility_matrix,
    segment.levels = segment_levels,
    cut.off = cut_off,
    mode = "symmetric",
    delete.upper.tri = TRUE,
    small.cell.reduction = 0,
    progress = verbose,
    auto_tune = FALSE
  )

  # Extract segment membership
  membership_result <- extract_segment_membership(moneca_result, verbose = verbose)

  if (verbose) {
    cat("Clustering complete:\n")
    cat("  - Segments identified:", membership_result$n_segments, "\n")
  }

  return(list(
    moneca_object = moneca_result,
    membership = membership_result$membership,
    n_segments = membership_result$n_segments
  ))
}

# 3. Segment Membership Extraction -----

#' Extract Segment Membership from MONECA Object
#'
#' Converts MONECA clustering results into a clean sector-to-segment mapping
#' data.table with segment sizes.
#'
#' @param moneca_object Output from moneca::moneca()
#' @param level Integer. Hierarchy level to extract (default: 1, finest level)
#' @param verbose Logical. Print extraction summary (default: FALSE)
#'
#' @return List with:
#'   - membership: data.table(sector, segment_id, segment_size)
#'   - n_segments: Number of segments at this level
#'
#' @export
extract_segment_membership <- function(moneca_object,
                                       level = 1,
                                       verbose = FALSE) {

  # Extract membership using MONECA's function
  # segment.membership() returns a data.frame with columns: name, membership
  membership_raw <- moneca::segment.membership(moneca_object, level = level)

  # Convert to data.table with clean structure
  membership_dt <- data.table::data.table(
    sector = membership_raw$name,
    segment_id = as.character(membership_raw$membership)
  )

  # Add segment sizes
  membership_dt[, segment_size := .N, by = segment_id]

  # Order by segment_id and sector
  data.table::setorder(membership_dt, segment_id, sector)

  n_segments <- data.table::uniqueN(membership_dt$segment_id)

  if (verbose) {
    cat("Extracted membership for", nrow(membership_dt), "sectors\n")
    cat("Number of segments:", n_segments, "\n")
    cat("Segment size range:", min(membership_dt$segment_size), "-",
        max(membership_dt$segment_size), "sectors\n")
  }

  return(list(
    membership = membership_dt,
    n_segments = n_segments
  ))
}

# 4. Segment Composition Analysis -----

#' Analyze Segment Composition
#'
#' Computes detailed statistics about each segment including sector lists,
#' transition volumes, and composition percentages. This information is used
#' to generate the naming template.
#'
#' @param membership data.table with sector, segment_id from extract_segment_membership()
#' @param mobility_matrix Original sector mobility matrix WITHOUT margins.
#'   Pass the original matrix, not the one with margins added.
#' @param transitions_data Optional. Full transitions data.table for demographic analysis.
#'   Should contain: from_sector, to_sector, and demographic columns
#' @param classifiers Optional. List with ateco classifier for sector labels
#'
#' @return data.table with one row per segment containing:
#'   - segment_id: Segment identifier
#'   - n_sectors: Number of sectors in segment
#'   - sectors: List column with sector codes
#'   - sector_labels: List column with sector names (if classifiers provided)
#'   - total_transitions: Total transition volume within and from segment
#'   - avg_salary: Average salary in segment (if transitions_data provided)
#'   - dominant_sectors: Top 5 sectors by transition volume
#'
#' @export
analyze_segment_composition <- function(membership,
                                        mobility_matrix,
                                        transitions_data = NULL,
                                        classifiers = NULL) {

  # Compute within-segment and outgoing transition volumes
  membership_dt <- data.table::copy(membership)

  # Extract transitions involving each segment
  transition_volumes <- data.table::data.table()

  for (seg_id in unique(membership_dt$segment_id)) {
    seg_sectors <- membership_dt[segment_id == seg_id, sector]

    # Sum all transitions from this segment's sectors
    seg_matrix <- mobility_matrix[seg_sectors, , drop = FALSE]
    total_trans <- sum(seg_matrix, na.rm = TRUE)

    # Within-segment transitions
    within_matrix <- mobility_matrix[seg_sectors, seg_sectors, drop = FALSE]
    within_trans <- sum(within_matrix, na.rm = TRUE)

    # Compute sector shares within segment
    sector_shares <- data.table::data.table(
      sector = seg_sectors,
      volume = rowSums(seg_matrix, na.rm = TRUE)
    )
    data.table::setorder(sector_shares, -volume)

    transition_volumes <- data.table::rbindlist(list(
      transition_volumes,
      data.table::data.table(
        segment_id = seg_id,
        n_sectors = length(seg_sectors),
        sectors = list(seg_sectors),
        total_transitions = total_trans,
        within_transitions = within_trans,
        cohesion = within_trans / total_trans,
        dominant_sectors = list(sector_shares$sector[1:min(5, nrow(sector_shares))]),
        sector_volumes = list(sector_shares$volume[1:min(5, nrow(sector_shares))])
      )
    ), fill = TRUE)
  }

  # Add sector labels if classifiers provided
  if (!is.null(classifiers) && "ateco" %in% names(classifiers)) {
    ateco_lookup <- classifiers$ateco[, .(ateco_3digit, des_ateco)]
    data.table::setnames(ateco_lookup, "des_ateco", "sector_label")

    transition_volumes[, sector_labels := lapply(sectors, function(s) {
      labels <- ateco_lookup[ateco_3digit %in% s, sector_label]
      if (length(labels) == 0) return(s)  # fallback to codes
      return(labels)
    })]

    transition_volumes[, dominant_labels := lapply(dominant_sectors, function(s) {
      labels <- ateco_lookup[ateco_3digit %in% s, sector_label]
      if (length(labels) == 0) return(s)
      return(labels)
    })]
  }

  # Add demographic info if transitions_data provided
  if (!is.null(transitions_data) && "from_sector" %in% names(transitions_data)) {
    demo_stats <- transitions_data[, .(
      avg_salary = mean(from_salary, na.rm = TRUE),
      median_age = median(cleta, na.rm = TRUE),
      pct_female = mean(sesso == "F", na.rm = TRUE) * 100
    ), by = from_sector]

    data.table::setnames(demo_stats, "from_sector", "sector")

    # Merge with membership
    membership_demo <- merge(membership_dt, demo_stats, by = "sector", all.x = TRUE)

    # Aggregate by segment
    segment_demo <- membership_demo[, .(
      segment_avg_salary = mean(avg_salary, na.rm = TRUE),
      segment_median_age = mean(median_age, na.rm = TRUE),
      segment_pct_female = mean(pct_female, na.rm = TRUE)
    ), by = segment_id]

    transition_volumes <- merge(transition_volumes, segment_demo, by = "segment_id", all.x = TRUE)
  }

  data.table::setorder(transition_volumes, segment_id)

  return(transition_volumes)
}

# 5. Naming Template Generation -----

#' Generate Naming Template File
#'
#' Creates an R script template in reference/ with segment composition as comments
#' and empty strings for user to fill in meaningful segment names.
#'
#' @param composition data.table from analyze_segment_composition()
#' @param output_path Character. Full path where template file should be saved
#' @param analysis_type Character. Description added to template header (e.g., "standard", "45-day lag")
#'
#' @return Character. Path to created template file
#'
#' @export
generate_naming_template <- function(composition,
                                     output_path,
                                     analysis_type = "standard") {

  # Ensure output directory exists
  output_dir <- dirname(output_path)
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  # Start building template content
  lines <- c(
    "# MONECA Labor Market Segment Names",
    paste("#", analysis_type, "mobility matrix segmentation"),
    "#",
    "# INSTRUCTIONS:",
    "# 1. Review the sector composition for each segment below",
    "# 2. Replace the empty strings with meaningful segment names",
    "# 3. Consider: dominant sectors, cohesion, transition patterns, demographics",
    "# 4. Save this file and rerun the pipeline",
    "#",
    paste("# Generated:", Sys.time()),
    paste("# Total segments:", nrow(composition)),
    "",
    "# Define segment names -----",
    "segment_names <- c("
  )

  # Add entry for each segment
  for (i in seq_len(nrow(composition))) {
    seg <- composition[i]

    # Segment header comment
    lines <- c(lines, "")
    lines <- c(lines, paste0("  # Segment ", seg$segment_id, " (", seg$n_sectors, " sectors)"))
    lines <- c(lines, paste0("  # Total transitions: ", format(seg$total_transitions, big.mark=",")))
    lines <- c(lines, paste0("  # Cohesion (within-segment): ", sprintf("%.1f%%", seg$cohesion * 100)))

    # Add demographic info if available
    if ("segment_avg_salary" %in% names(seg)) {
      lines <- c(lines, paste0("  # Avg salary: €", format(round(seg$segment_avg_salary), big.mark=",")))
      lines <- c(lines, paste0("  # Median age: ", round(seg$segment_median_age, 1), " years"))
      lines <- c(lines, paste0("  # Female: ", sprintf("%.1f%%", seg$segment_pct_female)))
    }

    lines <- c(lines, "  # Top sectors by volume:")

    # List dominant sectors with labels if available
    if ("dominant_labels" %in% names(seg) && !is.null(seg$dominant_labels[[1]])) {
      dom_labels <- seg$dominant_labels[[1]]
      dom_volumes <- seg$sector_volumes[[1]]

      for (j in seq_along(dom_labels)) {
        pct <- (dom_volumes[j] / seg$total_transitions) * 100
        lines <- c(lines, sprintf("  #   %d. %s (%.1f%%)", j, dom_labels[j], pct))
      }
    } else if ("dominant_sectors" %in% names(seg)) {
      dom_sectors <- seg$dominant_sectors[[1]]
      dom_volumes <- seg$sector_volumes[[1]]

      for (j in seq_along(dom_sectors)) {
        pct <- (dom_volumes[j] / seg$total_transitions) * 100
        lines <- c(lines, sprintf("  #   %d. %s (%.1f%%)", j, dom_sectors[j], pct))
      }
    }

    # Add the actual name assignment (empty string for user to fill)
    if (i < nrow(composition)) {
      lines <- c(lines, paste0("  \"", seg$segment_id, "\" = \"\","))
    } else {
      lines <- c(lines, paste0("  \"", seg$segment_id, "\" = \"\""))
    }
  }

  # Close the vector
  lines <- c(lines, ")", "")

  # Add validation footer
  lines <- c(lines,
    "# Validation -----",
    "# Check that all segments are named (no empty strings)",
    "if (any(segment_names == \"\")) {",
    "  warning(\"Some segments still need names: \",",
    "          paste(names(segment_names)[segment_names == \"\"], collapse=\", \"))",
    "  # Return NULL to signal incomplete",
    "  segment_names <- NULL",
    "}",
    ""
  )

  # Write to file
  writeLines(lines, output_path)

  cat("Template created at:", output_path, "\n")
  cat("Please edit this file to add meaningful segment names.\n")

  return(output_path)
}

# 6. Load and Apply Segment Names -----

#' Apply Segment Names from Template
#'
#' Loads the naming template file (after user has filled it) and applies
#' names to the segment membership data.table.
#'
#' @param template_path Character. Path to naming template R script
#' @param membership data.table with sector, segment_id
#' @param allow_incomplete Logical. If FALSE (default), returns NULL if any
#'   names are empty strings. If TRUE, uses segment_id for unnamed segments.
#'
#' @return data.table with sector, segment_id, segment_name columns,
#'   or NULL if template is incomplete and allow_incomplete=FALSE
#'
#' @export
apply_segment_names <- function(template_path,
                                membership,
                                allow_incomplete = FALSE) {

  if (!file.exists(template_path)) {
    stop("Template file not found at: ", template_path)
  }

  # Source the template file
  env <- new.env()
  source(template_path, local = env)

  # Check if segment_names was created (validation passed)
  if (is.null(env$segment_names)) {
    if (allow_incomplete) {
      warning("Template incomplete. Using segment IDs as names.")
      membership_dt <- data.table::copy(membership)
      membership_dt[, segment_name := paste("Segment", segment_id)]
      return(membership_dt)
    } else {
      warning("Template incomplete. Please fill all segment names.")
      return(NULL)
    }
  }

  # Create lookup table
  name_lookup <- data.table::data.table(
    segment_id = names(env$segment_names),
    segment_name = as.character(env$segment_names)
  )

  # Merge with membership
  membership_dt <- data.table::copy(membership)
  membership_dt <- merge(membership_dt, name_lookup, by = "segment_id", all.x = TRUE)

  # Check for any missing names
  if (any(is.na(membership_dt$segment_name))) {
    warning("Some segments don't have names in template. Using segment_id.")
    membership_dt[is.na(segment_name), segment_name := paste("Segment", segment_id)]
  }

  cat("Applied segment names to", nrow(membership_dt), "sectors\n")
  cat("Segments:", paste(unique(membership_dt$segment_name), collapse=", "), "\n")

  return(membership_dt)
}

# 7. Utility Functions -----

#' Check if Naming Template is Complete
#'
#' Quick check to see if user has filled the template without loading it.
#'
#' @param template_path Character. Path to naming template
#'
#' @return Logical. TRUE if complete, FALSE if any names are empty
#'
#' @export
is_template_complete <- function(template_path) {
  if (!file.exists(template_path)) return(FALSE)

  env <- new.env()
  tryCatch({
    source(template_path, local = env)
    return(!is.null(env$segment_names))
  }, error = function(e) {
    warning("Error checking template: ", e$message)
    return(FALSE)
  })
}
