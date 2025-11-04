# transition_enrichment.R
# Functions for enriching transition matrices with labels, visualizations, and statistics
# Author: Giampaolo Montaletti (giampaolo.montaletti@gmail.com)

# 1. Enrich transition matrix with human-readable labels -----

#' Enrich transition matrix with human-readable labels
#'
#' Merges classifier lookup tables with transition matrices to add descriptive
#' labels for both origin and destination nodes.
#'
#' @param transition_matrix A data.table with columns: from, to, N (transition counts)
#' @param classifier A data.table with lookup labels. Must contain a code column
#'   and a description column.
#' @param code_col Character string specifying the name of the code column in
#'   classifier. Default is "qualifica" for professions.
#' @param label_col Character string specifying the name of the label/description
#'   column in classifier. Default is "descrizione".
#'
#' @return A data.table with original columns plus:
#'   \itemize{
#'     \item from_label: Human-readable label for origin state
#'     \item to_label: Human-readable label for destination state
#'   }
#'
#' @details
#' The function performs left joins to preserve all transitions even if some
#' codes lack labels. Missing labels will appear as NA in the output.
#'
#' @examples
#' \dontrun{
#' prof_labeled <- enrich_transition_matrix_with_labels(
#'   profession_matrix,
#'   classifiers$professioni,
#'   code_col = "qualifica",
#'   label_col = "descrizione"
#' )
#' }
#'
#' @export
enrich_transition_matrix_with_labels <- function(transition_matrix,
                                                   classifier,
                                                   code_col = "qualifica",
                                                   label_col = "descrizione") {

  if (!data.table::is.data.table(transition_matrix)) {
    stop("transition_matrix must be a data.table")
  }

  if (!data.table::is.data.table(classifier)) {
    stop("classifier must be a data.table")
  }

  required_cols <- c("from", "to", "N")
  missing_cols <- setdiff(required_cols, names(transition_matrix))
  if (length(missing_cols) > 0) {
    stop("Missing required columns in transition_matrix: ",
         paste(missing_cols, collapse = ", "))
  }

  if (!code_col %in% names(classifier)) {
    stop("Code column '", code_col, "' not found in classifier")
  }

  if (!label_col %in% names(classifier)) {
    stop("Label column '", label_col, "' not found in classifier")
  }

  # Create a copy to avoid modifying the original
  enriched <- data.table::copy(transition_matrix)

  # Select only the needed columns from classifier to avoid naming conflicts
  # This prevents issues when classifier has multiple columns with similar names
  classifier_subset <- classifier[, .SD, .SDcols = c(code_col, label_col)]
  data.table::setnames(classifier_subset,
                       old = c(code_col, label_col),
                       new = c("code", "label"))

  # Merge labels for "from" nodes
  classifier_from <- data.table::copy(classifier_subset)
  data.table::setnames(classifier_from, c("code", "label"), c("from", "from_label"))

  enriched <- merge(enriched, classifier_from, by = "from", all.x = TRUE)

  # Merge labels for "to" nodes
  classifier_to <- data.table::copy(classifier_subset)
  data.table::setnames(classifier_to, c("code", "label"), c("to", "to_label"))

  enriched <- merge(enriched, classifier_to, by = "to", all.x = TRUE)

  # Reorder columns for clarity
  data.table::setcolorder(enriched, c("from", "from_label", "to", "to_label", "N"))

  return(enriched)
}


# 2. Create network visualization for transition matrix -----

#' Create network visualization for transition matrix
#'
#' Generates a ggraph network plot showing transitions between states with
#' colorblind-friendly styling.
#'
#' @param transition_matrix A data.table with columns: from, to, N, and optionally
#'   from_label, to_label for node labels.
#' @param title Character string for plot title. Default is "Transition Network".
#' @param top_n Integer specifying how many top transitions to display. If NULL,
#'   shows all transitions. Default is 50.
#' @param min_transitions Integer specifying minimum transition count to include.
#'   Default is 1 (show all).
#' @param layout Character string specifying ggraph layout algorithm. Default is
#'   "fr" (Fruchterman-Reingold).
#' @param use_labels Logical. If TRUE and labels are available, use them for node
#'   names. Default is TRUE.
#' @param exclude_self_loops Logical. If TRUE, excludes transitions where from == to
#'   (self-transitions representing job stability). Default is TRUE to show career
#'   mobility patterns between different states.
#'
#' @return A ggplot object showing the transition network
#'
#' @details
#' The function creates a directed network graph where:
#' - Node size reflects total transitions (in + out)
#' - Edge width reflects transition frequency
#' - Edge transparency reflects relative frequency
#' - Colorblind-friendly palette from viridis
#'
#' Large transition matrices are filtered to show only the most frequent transitions
#' to improve readability.
#'
#' @examples
#' \dontrun{
#' plot <- create_transition_network_plot(
#'   profession_matrix_labeled,
#'   title = "Profession Transition Network",
#'   top_n = 50
#' )
#' print(plot)
#' }
#'
#' @export
create_transition_network_plot <- function(transition_matrix,
                                            title = "Transition Network",
                                            top_n = 50,
                                            min_transitions = 1,
                                            layout = "fr",
                                            use_labels = TRUE,
                                            exclude_self_loops = TRUE) {

  if (!requireNamespace("ggraph", quietly = TRUE)) {
    stop("Package 'ggraph' is required but not installed")
  }

  if (!requireNamespace("igraph", quietly = TRUE)) {
    stop("Package 'igraph' is required but not installed")
  }

  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    stop("Package 'ggplot2' is required but not installed")
  }

  if (!data.table::is.data.table(transition_matrix)) {
    stop("transition_matrix must be a data.table")
  }

  required_cols <- c("from", "to", "N")
  missing_cols <- setdiff(required_cols, names(transition_matrix))
  if (length(missing_cols) > 0) {
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }

  # Filter transitions
  if (exclude_self_loops) {
    filtered <- transition_matrix[N >= min_transitions & from != to]
  } else {
    filtered <- transition_matrix[N >= min_transitions]
  }

  # Take top N if specified
  if (!is.null(top_n) && nrow(filtered) > top_n) {
    filtered <- filtered[order(-N)][1:top_n]
  }

  if (nrow(filtered) == 0) {
    stop("No transitions remaining after filtering")
  }

  # Prepare node names (use labels if available and requested)
  if (use_labels && "from_label" %in% names(filtered) && "to_label" %in% names(filtered)) {
    # Create node lookup table
    nodes_from <- filtered[, .(name = from, label = from_label)]
    nodes_to <- filtered[, .(name = to, label = to_label)]
    nodes <- unique(rbind(nodes_from, nodes_to))

    # Use label if available, otherwise use code
    nodes[is.na(label) | label == "", label := name]

    # Create network data with labels
    edges <- data.table::copy(filtered)
    edges <- merge(edges, nodes[, .(from = name, from_display = label)], by = "from")
    edges <- merge(edges, nodes[, .(to = name, to_display = label)], by = "to")

  } else {
    edges <- data.table::copy(filtered)
    edges[, `:=`(from_display = from, to_display = to)]
  }

  # Create igraph object
  graph <- igraph::graph_from_data_frame(
    d = edges[, .(from = from_display, to = to_display, weight = N)],
    directed = TRUE
  )

  # Calculate node sizes based on total transitions (in + out)
  in_degree <- edges[, .(in_trans = sum(N)), by = to_display]
  out_degree <- edges[, .(out_trans = sum(N)), by = from_display]

  data.table::setnames(in_degree, "to_display", "name")
  data.table::setnames(out_degree, "from_display", "name")

  node_sizes <- merge(
    in_degree,
    out_degree,
    by = "name",
    all = TRUE
  )
  node_sizes[is.na(in_trans), in_trans := 0]
  node_sizes[is.na(out_trans), out_trans := 0]
  node_sizes[, total_trans := in_trans + out_trans]

  # Create the plot
  p <- ggraph::ggraph(graph, layout = layout) +
    ggraph::geom_edge_link(
      ggplot2::aes(
        width = weight,
        alpha = weight
      ),
      arrow = ggplot2::arrow(length = ggplot2::unit(3, 'mm')),
      end_cap = ggraph::circle(3, 'mm'),
      color = "gray40"
    ) +
    ggraph::geom_node_point(
      ggplot2::aes(size = igraph::degree(graph)),
      color = "#0072B2",  # Colorblind-friendly blue
      alpha = 0.8
    ) +
    ggraph::geom_node_text(
      ggplot2::aes(label = name),
      repel = TRUE,
      size = 3,
      fontface = "bold"
    ) +
    ggraph::scale_edge_width(
      range = c(0.5, 3),
      name = "Transitions"
    ) +
    ggraph::scale_edge_alpha(
      range = c(0.3, 0.9),
      guide = "none"
    ) +
    ggplot2::scale_size_continuous(
      range = c(3, 12),
      name = "Degree"
    ) +
    ggplot2::labs(
      title = title,
      subtitle = paste0("Showing ", nrow(filtered), " transition pairs")
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(
      legend.position = "right",
      plot.title = ggplot2::element_text(face = "bold", size = 14),
      plot.subtitle = ggplot2::element_text(size = 10),
      panel.grid = ggplot2::element_blank(),
      axis.title = ggplot2::element_blank(),
      axis.text = ggplot2::element_blank(),
      axis.ticks = ggplot2::element_blank()
    )

  return(p)
}


# 3. Compute summary statistics for transition matrix -----

#' Compute summary statistics for transition matrix
#'
#' Calculates aggregate metrics describing mobility patterns in a transition
#' matrix including top transitions, node statistics, and mobility indices.
#'
#' @param transition_matrix A data.table with columns: from, to, N. Optionally
#'   from_label, to_label for human-readable output.
#' @param top_k Integer specifying how many top transitions to include in output.
#'   Default is 10.
#'
#' @return A list with summary statistics:
#'   \itemize{
#'     \item total_transitions: Total number of transitions
#'     \item unique_origins: Number of unique origin states
#'     \item unique_destinations: Number of unique destination states
#'     \item unique_pairs: Number of unique origin-destination pairs
#'     \item top_transitions: Data.table of top K most frequent transitions
#'     \item mobility_index: Measure of transition diversity (entropy-based)
#'     \item self_transition_rate: Proportion of transitions where from == to
#'     \item avg_transitions_per_pair: Mean transition frequency
#'   }
#'
#' @details
#' The mobility index is calculated using normalized Shannon entropy:
#' \deqn{H = -\sum p_i \log(p_i) / \log(n)}
#' where p_i is the proportion of transition pair i, and n is the number of pairs.
#' Values range from 0 (all transitions concentrate in one pair) to 1 (perfectly
#' uniform distribution).
#'
#' @examples
#' \dontrun{
#' stats <- compute_transition_summary_stats(profession_matrix_labeled, top_k = 10)
#' print(stats$top_transitions)
#' }
#'
#' @export
compute_transition_summary_stats <- function(transition_matrix, top_k = 10) {

  if (!data.table::is.data.table(transition_matrix)) {
    stop("transition_matrix must be a data.table")
  }

  required_cols <- c("from", "to", "N")
  missing_cols <- setdiff(required_cols, names(transition_matrix))
  if (length(missing_cols) > 0) {
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }

  # Basic counts
  total_transitions <- sum(transition_matrix$N)
  unique_origins <- transition_matrix[, data.table::uniqueN(from)]
  unique_destinations <- transition_matrix[, data.table::uniqueN(to)]
  unique_pairs <- nrow(transition_matrix)

  # Top K transitions
  top_transitions <- data.table::copy(transition_matrix)
  top_transitions <- top_transitions[order(-N)][1:min(top_k, nrow(top_transitions))]
  top_transitions[, percentage := round(100 * N / total_transitions, 2)]

  # Self-transition rate (from == to)
  self_transitions <- transition_matrix[from == to, sum(N)]
  self_transition_rate <- round(self_transitions / total_transitions, 4)

  # Average transitions per pair
  avg_transitions_per_pair <- round(mean(transition_matrix$N), 2)

  # Mobility index (normalized Shannon entropy)
  transition_matrix[, p := N / sum(N)]
  entropy <- -sum(transition_matrix$p * log(transition_matrix$p))
  max_entropy <- log(nrow(transition_matrix))
  mobility_index <- if (max_entropy > 0) {
    round(entropy / max_entropy, 4)
  } else {
    0
  }

  # Return results
  results <- list(
    total_transitions = total_transitions,
    unique_origins = unique_origins,
    unique_destinations = unique_destinations,
    unique_pairs = unique_pairs,
    top_transitions = top_transitions[, .(from, to, from_label, to_label, N, percentage)],
    mobility_index = mobility_index,
    self_transition_rate = self_transition_rate,
    avg_transitions_per_pair = avg_transitions_per_pair
  )

  return(results)
}


# 4. Convert transition data.table to R matrix -----

#' Convert Transition Data.table to R Matrix
#'
#' Converts transition data in data.table format (from, to, N) to a standard
#' R matrix suitable for matrix operations and compatibility with base R functions.
#' Uses longworkR's internal matrix conversion function for consistency.
#'
#' @param transition_dt A data.table with columns: from, to, N (transition counts).
#'   If from_label and to_label columns are present, they will be used as
#'   row/column names instead of codes.
#'
#' @return An R matrix where:
#'   \itemize{
#'     \item Row names: origin state labels (from_label) or codes (from)
#'     \item Column names: destination state labels (to_label) or codes (to)
#'     \item Values: transition counts (N)
#'     \item Dimensions: n x n where n is the number of unique states
#'   }
#'
#' @details
#' This function prepares transition data for use with:
#' - Base R matrix operations (rowSums, colSums, matrix multiplication)
#' - Standard heatmap visualizations
#' - Matrix-based statistical analyses
#' - longworkR's plot_transitions_heatmap() with input_format = "matrix"
#'
#' The resulting matrix includes all states that appear in either from or to
#' columns, with zeros for transitions that don't occur.
#'
#' If from_label and to_label columns are present in the input data, these
#' human-readable labels will be used as row and column names, making the
#' matrix more interpretable. Duplicate labels are made unique by appending
#' the code in parentheses.
#'
#' @examples
#' \dontrun{
#' # Convert labeled transitions to matrix (with labels as dimnames)
#' prof_matrix <- convert_transitions_to_matrix(profession_transitions_labeled)
#'
#' # Matrix operations
#' total_transitions <- sum(prof_matrix)
#' out_degree <- rowSums(prof_matrix)  # Total outgoing transitions
#' in_degree <- colSums(prof_matrix)   # Total incoming transitions
#'
#' # Row/column names are human-readable labels
#' head(rownames(prof_matrix))
#' # [1] "Membri_organismi_legislativi" "Dirigenti_pubblica_amministrazione" ...
#'
#' # Visualize with base R
#' heatmap(prof_matrix, scale = "none")
#'
#' # Or use longworkR
#' plot_transitions_heatmap(prof_matrix, input_format = "matrix")
#' }
#'
#' @export
convert_transitions_to_matrix <- function(transition_dt) {

  # Validate input
  if (!data.table::is.data.table(transition_dt)) {
    stop("Input must be a data.table")
  }

  required_cols <- c("from", "to", "N")
  missing_cols <- setdiff(required_cols, names(transition_dt))
  if (length(missing_cols) > 0) {
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }

  # Prepare data for longworkR's internal conversion
  # The internal function expects a column named "weight" instead of "N"
  dt_for_conversion <- data.table::copy(transition_dt)
  data.table::setnames(dt_for_conversion, "N", "weight")

  # Use longworkR's internal matrix conversion function
  # This ensures consistency with other longworkR functions
  transition_matrix <- longworkR:::.convert_to_matrix(dt_for_conversion, format = "data.table")

  # If labels are available, use them as row/column names
  has_labels <- all(c("from_label", "to_label") %in% names(transition_dt))

  if (has_labels) {
    # Create code-to-label mapping from both from and to sides
    from_mapping <- unique(transition_dt[, .(code = from, label = from_label)])
    to_mapping <- unique(transition_dt[, .(code = to, label = to_label)])

    # Combine and remove NAs
    code_label_map <- unique(data.table::rbindlist(list(from_mapping, to_mapping)))
    code_label_map <- code_label_map[!is.na(label)]

    # Remove duplicate codes, keeping first occurrence
    code_label_map <- code_label_map[!duplicated(code_label_map$code)]

    # Get codes in same order as matrix dimensions (which uses sorted codes)
    all_codes <- sort(unique(c(transition_dt$from, transition_dt$to)))

    # Create named vector for easy lookup
    label_lookup <- code_label_map$label
    names(label_lookup) <- code_label_map$code

    # Map codes to labels
    labels <- character(length(all_codes))
    for (i in seq_along(all_codes)) {
      code <- all_codes[i]
      if (code %in% names(label_lookup)) {
        labels[i] <- label_lookup[code]
      } else {
        labels[i] <- code  # Fallback to code if no label
      }
    }

    # Make labels unique if there are duplicates
    # Append code in parentheses for duplicates
    if (any(duplicated(labels))) {
      dup_indices <- which(duplicated(labels) | duplicated(labels, fromLast = TRUE))
      for (idx in dup_indices) {
        labels[idx] <- paste0(labels[idx], " (", all_codes[idx], ")")
      }
    }

    # Apply labels as row and column names
    rownames(transition_matrix) <- labels
    colnames(transition_matrix) <- labels
  }

  return(transition_matrix)
}
