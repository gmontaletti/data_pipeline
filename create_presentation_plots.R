# Create Presentation Plots for Data Pipeline
# Author: Giampaolo Montaletti
# Description: Generate 9 accessible, colorblind-friendly visualizations
#              for Quarto reveal.js presentation

# 1. Setup -----

# Load required packages
library(ggplot2)
library(ggraph)
library(igraph)
library(viridis)
library(dplyr)
library(tidyr)
library(lubridate)
library(patchwork)

# Set random seed for reproducibility
set.seed(42)

# Create output directory
output_dir <- "presentazione_grafici"
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# Define global plot settings
plot_width <- 12
plot_height <- 8
plot_dpi <- 300

# Custom theme for presentations
theme_presentation <- function() {
  theme_minimal() +
    theme(
      text = element_text(size = 12, family = "sans"),
      plot.title = element_text(size = 16, face = "bold", hjust = 0.5),
      plot.subtitle = element_text(size = 12, hjust = 0.5),
      axis.title = element_text(size = 12),
      axis.text = element_text(size = 10),
      legend.title = element_text(size = 11),
      legend.text = element_text(size = 10),
      panel.grid.minor = element_blank(),
      plot.background = element_rect(fill = "white", color = NA),
      panel.background = element_rect(fill = "white", color = NA)
    )
}

# Colorblind-friendly palette
cb_palette <- c("#0072B2", "#E69F00", "#009E73", "#F0E442", "#CC79A7",
                "#56B4E9", "#D55E00", "#999999")

# 2. Plot 1: Vecshift Timeline (Overlap Management) -----

# Simulate contract data with overlaps
contracts_raw <- data.frame(
  contract_id = c("A", "B", "C"),
  start = as.Date(c("2024-01-10", "2024-03-01", "2024-04-10")),
  end = as.Date(c("2024-04-30", "2024-05-15", "2024-06-20")),
  y_position = c(3, 2, 1)
)

# Create overlap periods
overlaps <- data.frame(
  period = c("Overlap 2 contracts (A+B)", "Overlap 3 contracts (A+B+C)", "Overlap 2 contracts (B+C)"),
  start = as.Date(c("2024-03-01", "2024-04-10", "2024-05-01")),
  end = as.Date(c("2024-04-10", "2024-04-30", "2024-05-15")),
  arco = c(2, 3, 2),
  y_position = c(0.5, 0, -0.5)
)

p1 <- ggplot() +
  # Original contracts (BEFORE consolidation)
  geom_segment(data = contracts_raw,
               aes(x = start, xend = end, y = y_position, yend = y_position,
                   color = contract_id),
               linewidth = 8, alpha = 0.7) +
  geom_text(data = contracts_raw,
            aes(x = start + (end - start)/2, y = y_position,
                label = paste("Contract", contract_id)),
            color = "white", fontface = "bold", size = 4) +

  # Overlap periods (AFTER consolidation)
  geom_segment(data = overlaps,
               aes(x = start, xend = end, y = y_position, yend = y_position),
               color = "#D55E00", linewidth = 6, alpha = 0.8) +
  geom_text(data = overlaps,
            aes(x = start + (end - start)/2, y = y_position - 0.3,
                label = paste("arco =", arco)),
            color = "#D55E00", fontface = "bold", size = 3.5) +

  # Add horizontal line to separate sections
  geom_hline(yintercept = 0.75, linetype = "dashed", color = "gray50", linewidth = 0.5) +

  # Annotations
  annotate("text", x = as.Date("2024-01-05"), y = 4,
           label = "BEFORE: Original Contracts",
           hjust = 0, fontface = "bold", size = 5) +
  annotate("text", x = as.Date("2024-01-05"), y = -1.5,
           label = "AFTER: Vecshift Consolidation",
           hjust = 0, fontface = "bold", size = 5) +

  scale_color_manual(values = cb_palette[1:3]) +
  scale_x_date(date_breaks = "1 month", date_labels = "%b %Y") +
  labs(
    title = "Vecshift: Longitudinal Reconstruction with Overlap Management",
    subtitle = "Identifying concurrent contracts (arco field)",
    x = "Time",
    y = "",
    color = "Contract ID"
  ) +
  theme_presentation() +
  theme(
    axis.text.y = element_blank(),
    axis.ticks.y = element_blank(),
    panel.grid.major.y = element_blank()
  )

ggsave(file.path(output_dir, "01_vecshift_timeline.png"),
       plot = p1, width = plot_width, height = plot_height, dpi = plot_dpi)

cat("Plot 1: Vecshift timeline saved\n")

# 3. Plot 2: LongworkR Gap Consolidation -----

# Simulate employment periods with gaps
employment_periods <- data.frame(
  period_id = 1:4,
  start = c(0, 15, 50, 90),
  end = c(10, 40, 80, 120),
  y = 1
)

# Calculate gaps
gaps <- data.frame(
  gap_id = 1:3,
  start = c(10, 40, 80),
  end = c(15, 50, 90),
  duration = c(5, 10, 10),
  consolidated = c(TRUE, FALSE, TRUE),
  y = 0.5
)

gaps$label <- ifelse(gaps$consolidated,
                     paste0(gaps$duration, " days\n(Consolidated)"),
                     paste0(gaps$duration, " days\n(NOT consolidated)"))

p2 <- ggplot() +
  # Employment periods
  geom_segment(data = employment_periods,
               aes(x = start, xend = end, y = y, yend = y),
               color = cb_palette[3], linewidth = 10, alpha = 0.8) +
  geom_text(data = employment_periods,
            aes(x = start + (end - start)/2, y = y + 0.15,
                label = paste("Period", period_id)),
            color = cb_palette[3], fontface = "bold", size = 4) +

  # Gaps
  geom_segment(data = gaps,
               aes(x = start, xend = end, y = y, yend = y,
                   color = consolidated),
               linewidth = 6, alpha = 0.7,
               arrow = arrow(angle = 90, ends = "both", length = unit(0.15, "inches"))) +
  geom_text(data = gaps,
            aes(x = start + (end - start)/2, y = y - 0.2,
                label = label, color = consolidated),
            fontface = "bold", size = 3.5) +

  # Threshold line at 8 days
  geom_vline(xintercept = c(10 + 8, 40 + 8, 80 + 8),
             linetype = "dotted", color = "gray40", linewidth = 0.8) +

  annotate("text", x = 18, y = 1.4,
           label = "8-day threshold",
           color = "gray40", fontface = "italic", size = 3.5, angle = 90) +

  scale_color_manual(values = c("FALSE" = "#D55E00", "TRUE" = "#009E73"),
                     labels = c("FALSE" = "NOT consolidated", "TRUE" = "Consolidated")) +
  scale_x_continuous(breaks = seq(0, 120, 20)) +
  labs(
    title = "LongworkR: Gap Consolidation with 8-Day Threshold",
    subtitle = "Short gaps (<8 days) are consolidated into continuous employment spells",
    x = "Days",
    y = "",
    color = "Status"
  ) +
  theme_presentation() +
  theme(
    axis.text.y = element_blank(),
    axis.ticks.y = element_blank(),
    panel.grid.major.y = element_blank(),
    legend.position = "bottom"
  )

ggsave(file.path(output_dir, "02_longworkr_gaps.png"),
       plot = p2, width = plot_width, height = plot_height, dpi = plot_dpi)

cat("Plot 2: LongworkR gaps saved\n")

# 4. Plot 3: Person-Level Metrics Dashboard -----

# Simulate person metrics
metrics_data <- data.frame(
  metric = c("Employment\nRate", "Stability\nIndex", "Job Turnover\nRate",
             "N. Transitions", "DID Coverage\nRate"),
  individual = c(0.75, 0.62, 0.15, 0.25, 0.40),
  population_avg = c(0.68, 0.55, 0.20, 0.35, 0.33)
) %>%
  pivot_longer(cols = c(individual, population_avg),
               names_to = "group", values_to = "value") %>%
  mutate(
    metric = factor(metric, levels = c("Employment\nRate", "Stability\nIndex",
                                       "Job Turnover\nRate", "N. Transitions",
                                       "DID Coverage\nRate")),
    group = factor(group, levels = c("individual", "population_avg"),
                   labels = c("Individual", "Population Average"))
  )

p3 <- ggplot(metrics_data, aes(x = metric, y = value, fill = group)) +
  geom_col(position = position_dodge(width = 0.8), width = 0.7, alpha = 0.9) +
  geom_text(aes(label = sprintf("%.2f", value)),
            position = position_dodge(width = 0.8),
            vjust = -0.5, size = 4, fontface = "bold") +
  scale_fill_manual(values = c("Individual" = cb_palette[1],
                                "Population Average" = cb_palette[2])) +
  scale_y_continuous(limits = c(0, 1), breaks = seq(0, 1, 0.2)) +
  labs(
    title = "Person-Level Career Metrics",
    subtitle = "Comparing individual trajectory with population benchmarks",
    x = "Metric",
    y = "Value",
    fill = "Group"
  ) +
  theme_presentation() +
  theme(legend.position = "bottom")

ggsave(file.path(output_dir, "03_person_metrics.png"),
       plot = p3, width = plot_width, height = plot_height, dpi = plot_dpi)

cat("Plot 3: Person metrics saved\n")

# 5. Plot 4: Contract Duration Distribution -----

# Simulate realistic contract durations (log-normal distribution)
n_contracts <- 10000
duration_data <- data.frame(
  duration_days = rlnorm(n_contracts, meanlog = log(180), sdlog = 0.9),
  contract_type = sample(c("Closed", "Open/Truncated"), n_contracts,
                         replace = TRUE, prob = c(0.7, 0.3))
) %>%
  filter(duration_days <= 3650) %>%  # Cap at 10 years
  mutate(duration_years = duration_days / 365)

# Calculate quartiles
quartiles <- quantile(duration_data$duration_days, probs = c(0.25, 0.5, 0.75))

p4 <- ggplot(duration_data, aes(x = duration_days, fill = contract_type)) +
  geom_histogram(bins = 60, alpha = 0.7, position = "identity") +
  geom_vline(xintercept = quartiles[2], linetype = "solid",
             color = "#D55E00", linewidth = 1.2) +
  geom_vline(xintercept = c(quartiles[1], quartiles[3]), linetype = "dashed",
             color = "#D55E00", linewidth = 0.8) +
  annotate("text", x = quartiles[2] + 150, y = Inf,
           label = paste0("Median: ", round(quartiles[2]), " days"),
           color = "#D55E00", fontface = "bold", size = 4, vjust = 1.5) +
  annotate("text", x = quartiles[1] - 100, y = Inf,
           label = "Q1", color = "#D55E00", size = 3.5, vjust = 1.5) +
  annotate("text", x = quartiles[3] + 100, y = Inf,
           label = "Q3", color = "#D55E00", size = 3.5, vjust = 1.5) +
  scale_fill_manual(values = c("Closed" = cb_palette[1],
                                "Open/Truncated" = cb_palette[5])) +
  scale_x_continuous(breaks = seq(0, 3650, 365),
                     labels = paste0(seq(0, 10, 1), "y")) +
  labs(
    title = "Contract Duration Distribution",
    subtitle = sprintf("N = %s contracts | Median = %d days (%.1f months)",
                       format(nrow(duration_data), big.mark = ","),
                       round(quartiles[2]),
                       round(quartiles[2]/30, 1)),
    x = "Duration",
    y = "Number of Contracts",
    fill = "Contract Type"
  ) +
  theme_presentation() +
  theme(legend.position = "bottom")

ggsave(file.path(output_dir, "04_contract_durations.png"),
       plot = p4, width = plot_width, height = plot_height, dpi = plot_dpi)

cat("Plot 4: Contract durations saved\n")

# 6. Plot 5: FTE Composition Examples -----

# Simulate FTE composition for 3 workers
fte_examples <- data.frame(
  worker = rep(c("Worker A:\nFull-time, 12 months",
                 "Worker B:\nPart-time 50%, 12 months",
                 "Worker C:\nFull-time, 6 months"), each = 2),
  component = rep(c("Weekly Effort", "Annual Coverage"), 3),
  value = c(1.0, 1.0,    # Worker A
            0.5, 1.0,    # Worker B
            1.0, 0.5)    # Worker C
) %>%
  mutate(
    worker = factor(worker, levels = c("Worker A:\nFull-time, 12 months",
                                       "Worker B:\nPart-time 50%, 12 months",
                                       "Worker C:\nFull-time, 6 months")),
    component = factor(component, levels = c("Annual Coverage", "Weekly Effort"))
  )

# Calculate final FTE for annotations
fte_totals <- data.frame(
  worker = c("Worker A:\nFull-time, 12 months",
             "Worker B:\nPart-time 50%, 12 months",
             "Worker C:\nFull-time, 6 months"),
  fte = c(1.0, 0.5, 0.5)
)

p5 <- ggplot(fte_examples, aes(x = worker, y = value, fill = component)) +
  geom_col(position = "identity", alpha = 0.8, width = 0.6) +
  geom_text(data = fte_totals,
            aes(x = worker, y = 1.15, label = paste0("FTE = ", fte), fill = NULL),
            fontface = "bold", size = 5, color = "#D55E00") +
  annotate("text", x = 0.5, y = 1.05,
           label = "FTE = Weekly Effort × Annual Coverage",
           hjust = 0, fontface = "italic", size = 4, color = "gray30") +
  scale_fill_manual(values = c("Weekly Effort" = cb_palette[1],
                                "Annual Coverage" = cb_palette[3])) +
  scale_y_continuous(limits = c(0, 1.25), breaks = seq(0, 1, 0.25)) +
  labs(
    title = "Full-Time Equivalent (FTE) Composition",
    subtitle = "Three examples of FTE calculation from weekly effort and annual coverage",
    x = "",
    y = "Value",
    fill = "Component"
  ) +
  theme_presentation() +
  theme(
    legend.position = "bottom",
    axis.text.x = element_text(size = 10, lineheight = 0.9)
  )

ggsave(file.path(output_dir, "05_fte_composition.png"),
       plot = p5, width = plot_width, height = plot_height, dpi = plot_dpi)

cat("Plot 5: FTE composition saved\n")

# 7. Plot 6: FTE Evolution by Sector Over Time -----

# Simulate FTE time series by sector
years <- 2019:2024
sectors <- c("Manufacturing", "Retail", "Services", "Construction", "ICT", "Healthcare")

fte_timeseries <- expand.grid(
  year = years,
  sector = sectors
) %>%
  mutate(
    base_fte = case_when(
      sector == "Manufacturing" ~ 120,
      sector == "Retail" ~ 95,
      sector == "Services" ~ 150,
      sector == "Construction" ~ 70,
      sector == "ICT" ~ 45,
      sector == "Healthcare" ~ 85
    ),
    # Add trends and COVID impact
    trend_factor = case_when(
      sector == "ICT" ~ 1 + (year - 2019) * 0.15,  # Strong growth
      sector == "Healthcare" ~ 1 + (year - 2019) * 0.08,  # Moderate growth
      sector == "Construction" ~ 1 + (year - 2019) * 0.05,  # Slight growth
      sector == "Services" ~ ifelse(year == 2020, 0.75, 1 + (year - 2019) * 0.03),  # COVID impact
      sector == "Retail" ~ ifelse(year == 2020, 0.80, 1 + (year - 2019) * 0.02),  # COVID impact
      sector == "Manufacturing" ~ 1 + (year - 2019) * 0.01  # Stable
    ),
    fte_thousands = base_fte * trend_factor + rnorm(n(), 0, 3)
  ) %>%
  mutate(sector = factor(sector, levels = sectors))

p6 <- ggplot(fte_timeseries, aes(x = year, y = fte_thousands, color = sector)) +
  geom_line(linewidth = 1.5, alpha = 0.9) +
  geom_point(size = 3, alpha = 0.8) +
  scale_color_manual(values = cb_palette[1:6]) +
  scale_x_continuous(breaks = years) +
  scale_y_continuous(labels = function(x) paste0(x, "k")) +
  labs(
    title = "FTE Evolution by Sector (2019-2024)",
    subtitle = "Total full-time equivalent employment across major economic sectors",
    x = "Year",
    y = "FTE (thousands)",
    color = "Sector"
  ) +
  theme_presentation() +
  theme(legend.position = "right")

ggsave(file.path(output_dir, "06_fte_sectors_time.png"),
       plot = p6, width = plot_width, height = plot_height, dpi = plot_dpi)

cat("Plot 6: FTE time series saved\n")

# 8. Plot 7: Sector Transition Network -----

# Simulate sector transition matrix
sectors_network <- c("Manufacturing", "Retail", "Hospitality", "Construction",
                     "ICT", "Healthcare", "Finance", "Education", "Public Admin", "Other")
n_sectors <- length(sectors_network)

# Create transition matrix with realistic patterns
set.seed(123)
transition_matrix <- matrix(0, n_sectors, n_sectors)
diag(transition_matrix) <- runif(n_sectors, 800, 1500)  # High persistence

# Add off-diagonal transitions (asymmetric)
for (i in 1:n_sectors) {
  for (j in 1:n_sectors) {
    if (i != j) {
      # Some sectors have higher mobility
      if (i %in% c(2, 3, 10) || j %in% c(2, 3, 10)) {  # Retail, Hospitality, Other
        transition_matrix[i, j] <- rpois(1, lambda = 30)
      } else {
        transition_matrix[i, j] <- rpois(1, lambda = 15)
      }
    }
  }
}

# Convert to edge list
edges_network <- expand.grid(from = 1:n_sectors, to = 1:n_sectors) %>%
  mutate(
    weight = as.vector(transition_matrix),
    from_sector = sectors_network[from],
    to_sector = sectors_network[to]
  ) %>%
  filter(weight > 0, from != to) %>%
  filter(weight > 20)  # Keep only significant transitions

# Create node data with macro-categories
nodes_network <- data.frame(
  name = sectors_network,
  category = c("Industry", "Services", "Services", "Industry",
               "Services", "Services", "Services", "Services", "Public", "Other"),
  total_flow = sapply(1:n_sectors, function(i) {
    sum(transition_matrix[i, ]) + sum(transition_matrix[, i])
  })
)

# Create igraph object
g <- graph_from_data_frame(edges_network[, c("from_sector", "to_sector", "weight")],
                           directed = TRUE,
                           vertices = nodes_network)

# Create network plot
set.seed(42)
p7 <- ggraph(g, layout = 'fr') +
  geom_edge_link(aes(width = weight, alpha = weight),
                 arrow = arrow(length = unit(3, 'mm'), type = "closed"),
                 end_cap = circle(8, 'mm'),
                 start_cap = circle(8, 'mm'),
                 color = "gray50") +
  geom_node_point(aes(size = total_flow, color = category), alpha = 0.9) +
  geom_node_text(aes(label = name), size = 3.5, fontface = "bold",
                 repel = TRUE, bg.color = "white", bg.r = 0.1) +
  scale_edge_width(range = c(0.3, 3), guide = "none") +
  scale_edge_alpha(range = c(0.3, 0.8), guide = "none") +
  scale_size_continuous(range = c(8, 20), guide = "none") +
  scale_color_manual(values = cb_palette[c(1, 3, 2, 8)]) +
  labs(
    title = "Sector Transition Network",
    subtitle = "Node size = total mobility | Edge width = transition frequency",
    color = "Sector Category"
  ) +
  theme_void() +
  theme(
    plot.title = element_text(size = 16, face = "bold", hjust = 0.5),
    plot.subtitle = element_text(size = 12, hjust = 0.5),
    legend.position = "bottom",
    legend.title = element_text(size = 11),
    legend.text = element_text(size = 10),
    plot.background = element_rect(fill = "white", color = NA)
  )

ggsave(file.path(output_dir, "07_sector_network.png"),
       plot = p7, width = plot_width, height = plot_height, dpi = plot_dpi)

cat("Plot 7: Sector network saved\n")

# 9. Plot 8: Transition Matrix Heatmap -----

# Use same transition matrix from plot 7, but subset to 10x10
sectors_heatmap <- sectors_network[1:10]
transition_heatmap <- transition_matrix[1:10, 1:10]

# Convert to long format
heatmap_data <- expand.grid(
  from = sectors_heatmap,
  to = sectors_heatmap
) %>%
  mutate(
    from = factor(from, levels = rev(sectors_heatmap)),
    to = factor(to, levels = sectors_heatmap),
    value = as.vector(transition_heatmap),
    is_diagonal = from == to,
    label = ifelse(value > 100, as.character(round(value)), "")
  )

p8 <- ggplot(heatmap_data, aes(x = to, y = from, fill = value)) +
  geom_tile(color = "white", linewidth = 0.5) +
  geom_text(aes(label = label), size = 3, fontface = "bold", color = "white") +
  geom_tile(data = filter(heatmap_data, is_diagonal),
            color = "#D55E00", linewidth = 1.5, fill = NA) +
  scale_fill_viridis(option = "plasma", trans = "log10",
                     breaks = c(10, 100, 1000),
                     labels = c("10", "100", "1000")) +
  labs(
    title = "Sector Transition Matrix Heatmap",
    subtitle = "Diagonal highlighted = persistence in same sector | Color intensity = transition frequency (log scale)",
    x = "Destination Sector",
    y = "Origin Sector",
    fill = "Transitions"
  ) +
  theme_presentation() +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1, size = 9),
    axis.text.y = element_text(size = 9),
    legend.position = "right",
    panel.grid = element_blank()
  )

ggsave(file.path(output_dir, "08_transition_heatmap.png"),
       plot = p8, width = plot_width, height = plot_height, dpi = plot_dpi)

cat("Plot 8: Transition heatmap saved\n")

# 10. Plot 9: Standard vs 45-Day Lag Comparison -----

# Simulate comparison data
comparison_data <- data.frame(
  category = c("All Transitions", "Significant Transitions\n(>45 days unemployment)",
               "Rapid Rehires\n(<45 days)"),
  Standard = c(120, 75, 45),
  Lag45day = c(75, 75, 0)
) %>%
  pivot_longer(cols = c(Standard, Lag45day),
               names_to = "filter", values_to = "n_transitions") %>%
  mutate(filter = ifelse(filter == "Lag45day", "45-day Lag", filter)) %>%
  mutate(
    category = factor(category, levels = c("All Transitions",
                                           "Significant Transitions\n(>45 days unemployment)",
                                           "Rapid Rehires\n(<45 days)")),
    filter = factor(filter, levels = c("Standard", "45-day Lag"))
  )

p9 <- ggplot(comparison_data, aes(x = category, y = n_transitions, fill = filter)) +
  geom_col(position = position_dodge(width = 0.8), width = 0.7, alpha = 0.9) +
  geom_text(aes(label = paste0(n_transitions, "k")),
            position = position_dodge(width = 0.8),
            vjust = -0.5, size = 4.5, fontface = "bold") +
  geom_segment(aes(x = 0.7, xend = 1.3, y = 125, yend = 125),
               arrow = arrow(length = unit(0.2, "inches"), ends = "both", type = "closed"),
               color = "#D55E00", linewidth = 1) +
  annotate("text", x = 1, y = 130,
           label = "45k rapid rehires\nexcluded",
           color = "#D55E00", fontface = "bold", size = 4) +
  scale_fill_manual(values = c("Standard" = cb_palette[1],
                                "45-day Lag" = cb_palette[2])) +
  scale_y_continuous(limits = c(0, 140), breaks = seq(0, 120, 20),
                     labels = function(x) paste0(x, "k")) +
  labs(
    title = "Standard vs 45-Day Lag Filter Comparison",
    subtitle = "Isolating meaningful career transitions by excluding rapid rehires",
    x = "Transition Category",
    y = "Number of Transitions (thousands)",
    fill = "Analysis Type"
  ) +
  theme_presentation() +
  theme(
    legend.position = "bottom",
    axis.text.x = element_text(size = 10, lineheight = 0.9)
  )

ggsave(file.path(output_dir, "09_standard_vs_45day.png"),
       plot = p9, width = plot_width, height = plot_height, dpi = plot_dpi)

cat("Plot 9: Standard vs 45-day comparison saved\n")

# 11. Summary Report -----

cat("\n=== PRESENTATION PLOTS CREATION COMPLETE ===\n\n")
cat("All 9 plots have been saved to:", output_dir, "\n\n")

plot_descriptions <- data.frame(
  File = c(
    "01_vecshift_timeline.png",
    "02_longworkr_gaps.png",
    "03_person_metrics.png",
    "04_contract_durations.png",
    "05_fte_composition.png",
    "06_fte_sectors_time.png",
    "07_sector_network.png",
    "08_transition_heatmap.png",
    "09_standard_vs_45day.png"
  ),
  Description = c(
    "Timeline showing vecshift overlap management (arco field)",
    "Gap consolidation with 8-day threshold visualization",
    "Person-level metrics comparison (individual vs population)",
    "Contract duration distribution with quartiles",
    "FTE composition examples (weekly effort × annual coverage)",
    "FTE evolution by sector over time (2019-2024)",
    "Network graph of sector transitions (ggraph)",
    "Heatmap of transition matrix with diagonal emphasis",
    "Comparison of standard vs 45-day lag filter impact"
  )
)

print(plot_descriptions)

cat("\n")
cat("Technical specifications:\n")
cat("- Resolution: 300 DPI\n")
cat("- Dimensions: 12 × 8 inches (16:9 aspect ratio)\n")
cat("- Color palette: Colorblind-friendly (viridis + custom CB palette)\n")
cat("- Format: PNG\n")
cat("- Theme: Custom presentation theme based on theme_minimal()\n")
cat("\nAll plots are ready for embedding in Quarto reveal.js presentations.\n")
