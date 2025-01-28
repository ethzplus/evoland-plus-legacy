#############################################################################
## Transition_identification: Using land use data from calibration (historic) periods to identify
## LULC transitions to be modeled in future simulations
##
## Date: 28/09/2022
## Author: Ben Black (Modified version)
#############################################################################

### =========================================================================
### A- Preparation
### =========================================================================

# Variables defined externally (not in this script):
# LULC_aggregation_path (path to LULC_classes.xlsx)
# Step_length (numeric; duration of time steps, e.g. 4)
# Inclusion_thres (numeric; minimum % threshold for transition inclusion)

# Historic LULC data folder path
LULC_folder <- "Data/Historic_LULC"

# Extract years from LULC filenames
LULC_years <- gsub(".*?([0-9]+).*", "\\1", list.files(LULC_folder, full.names = FALSE, pattern = ".tif"))

# Read aggregation scheme externally provided
Aggregation_scheme <- read_excel(LULC_aggregation_path)

# Prepare LULC classes
LULC_classes <- data.frame(label = unique(Aggregation_scheme$description_edited))
LULC_classes$value <- sapply(
  LULC_classes$label,
  function(x) {
    unique(Aggregation_scheme[Aggregation_scheme$description_edited == x, "ID"])
  }
)

# Directory for transition rate tables
trans_rates_dir <- "Data/Transition_tables/raw_trans_tables"
dir.create(trans_rates_dir, recursive = TRUE, showWarnings = FALSE)

### =========================================================================
### B- Calculate historic areal change for LULC classes
### =========================================================================

# Load LULC rasters with terra
LULC_rasters <- lapply(list.files(LULC_folder, full.names = TRUE, pattern = ".tif"), terra::rast)
names(LULC_rasters) <- LULC_years

# Create a levels (RAT) data frame for terra rasters
levels_df <- data.frame(ID = Aggregation_scheme$ID, lulc_name = Aggregation_scheme$description_edited)

# Frequency function for terra rasters
frequ <- function(raster) {
  freq_table <- terra::freq(raster, bylayer = FALSE)
  as.matrix(freq_table)
}

# Compute area (pixel counts) for each LULC raster
LULC_areas <- lapply(LULC_rasters, frequ)

# Merge area tables by LULC class value
LULC_areal_change <- Reduce(function(x, y) merge(x, y, by = "value"), LULC_areas)
names(LULC_areal_change)[2:length(LULC_rasters) + 1] <- names(LULC_areas)

# Add LULC class names
RAT <- levels_df
RAT$Pixel_value <- RAT$ID
LULC_areal_change$LULC_class <- sapply(
  LULC_areal_change$value,
  function(x) RAT[RAT$Pixel_value == x, "lulc_name"]
)

# Save area change table
write.csv(LULC_areal_change, file.path(trans_rates_dir, "LULC_historic_areal_change.csv"), row.names = FALSE)

### =========================================================================
### C- Preparing Transition tables for each calibration period
### =========================================================================

# Create list of LULC year pairs for transitions
LULC_change_periods <- vector("list", length(LULC_years) - 1)
for (i in seq_along(LULC_change_periods)) {
  LULC_change_periods[[i]] <- c(LULC_years[i], LULC_years[i + 1])
}
names(LULC_change_periods) <- sapply(LULC_change_periods, function(x) paste(x[1], x[2], sep = "_"))

# Function to produce single-step and multi-step transition tables
lulcc.periodictransmatrices <- function(Raster_combo, Raster_stack, period_name, Step_length) {
  # Extract rasters by year
  r1 <- Raster_stack[[grep(Raster_combo[1], names(Raster_stack))]]
  r2 <- Raster_stack[[grep(Raster_combo[2], names(Raster_stack))]]

  # Produce transition matrix of areal changes for the whole period
  trans_rates_for_period <- terra::crosstab(c(r1, r2), long = FALSE)

  # Convert to percentage changes
  sum01 <- apply(trans_rates_for_period, MARGIN = 1, FUN = sum)
  percchange <- sweep(trans_rates_for_period, MARGIN = 1, STATS = sum01, FUN = "/")
  options(scipen = 999)
  perchange_for_period <- as.data.frame(percchange)

  # Adjust column names to Dinamica format
  colnames(perchange_for_period) <- c("From*", "To*", "Rate")

  # Convert columns to numeric
  perchange_for_period$`From*` <- as.numeric(as.character(perchange_for_period$`From*`))
  perchange_for_period$`To*` <- as.numeric(as.character(perchange_for_period$`To*`))

  # Remove persistence and zero rows
  perchange_for_period <- perchange_for_period[perchange_for_period$From. != perchange_for_period$To., ]
  perchange_for_period <- perchange_for_period[perchange_for_period$Rate != 0, ]

  # Sort by initial class
  perchange_for_period <- perchange_for_period[order(perchange_for_period$From.), ]

  # Save single-step table
  write_csv(perchange_for_period, file.path(trans_rates_dir, paste0("Calibration_", period_name, "_singlestep_trans_table.csv")))

  # Multi-step calculation
  Period_length <- as.numeric(Raster_combo[2]) - as.numeric(Raster_combo[1])
  Num_steps <- ceiling(Period_length / Step_length)

  perchange_for_period_multistep <- perchange_for_period
  perchange_for_period_multistep$Rate <- perchange_for_period_multistep$Rate / Num_steps

  # Save multi-step table
  write_csv(perchange_for_period_multistep, file.path(trans_rates_dir, paste0("Calibration_", period_name, "_multistep_trans_table.csv")))
}


# Run function over each period
mapply(lulcc.periodictransmatrices,
  Raster_combo = LULC_change_periods,
  period_name = names(LULC_change_periods),
  MoreArgs = list(Raster_stack = LULC_rasters, Step_length = Step_length),
  SIMPLIFY = FALSE
)


### =========================================================================
### C- Subsetting to Viable Transitions
### =========================================================================

# Load single-step tables and subset by threshold
Calibration_singlestep_tables <- lapply(list.files(trans_rates_dir, full.names = TRUE, pattern = "singlestep"), read.csv)
names(Calibration_singlestep_tables) <- gsub(
  "Calibration_|_singlestep_trans_table.csv", "",
  list.files(trans_rates_dir, full.names = FALSE, pattern = "singlestep")
)

Viable_transitions_by_period_SS <- lapply(Calibration_singlestep_tables, function(x) {
  x$Initial_class <- sapply(x$From., function(y) LULC_classes[LULC_classes$value == y, "label"])
  x$Final_class <- sapply(x$To., function(y) LULC_classes[LULC_classes$value == y, "label"])
  x$Trans_name <- paste(x$Initial_class, x$Final_class, sep = "_")

  # Subset by inclusion threshold
  x <- x[x$Rate * 100 >= Inclusion_thres, ]

  # Subset by transitions from non-static classes
  x <- x[x$Initial_class != "Static", ]

  x$Trans_ID <- sprintf("%02d", 1:nrow(x))
  x
})

# Save viable transitions list
saveRDS(Viable_transitions_by_period_SS, "Tools/Viable_transitions_lists.rds")

### =========================================================================
### D- Combining Transition Rates for Calibration Periods
### =========================================================================

# Single-step transitions over time
Trans_tables_bound_SS <- data.table::rbindlist(Viable_transitions_by_period_SS, idcol = "Period")
Trans_tables_bound_SS$Trans_ID <- NULL
Trans_table_time_SS <- tidyr::pivot_wider(Trans_tables_bound_SS, names_from = "Period", values_from = "Rate")
write.csv(Trans_table_time_SS, "Data/Transition_tables/trans_rates_table_calibration_periods_SS.csv")

# Multi-step transitions
Calibration_multistep_tables <- lapply(list.files(trans_rates_dir, full.names = TRUE, pattern = "multistep"), read.csv)
names(Calibration_multistep_tables) <- gsub(
  "Calibration_|_multistep_trans_table.csv", "",
  list.files(trans_rates_dir, full.names = FALSE, pattern = "multistep")
)

Viable_transitions_by_period_MS <- lapply(Calibration_multistep_tables, function(x) {
  x$Initial_class <- sapply(x$From., function(y) LULC_classes[LULC_classes$value == y, "label"])
  x$Final_class <- sapply(x$To., function(y) LULC_classes[LULC_classes$value == y, "label"])
  x$Trans_name <- paste(x$Initial_class, x$Final_class, sep = "_")

  # Remove transitions from static class
  x <- x[x$Initial_class != "Static", ]

  x$Trans_ID <- sprintf("%02d", 1:nrow(x))
  x
})

# Use only transitions that appeared in single-step final table
single_step_table <- read.csv("Data/Transition_tables/trans_rates_table_calibration_periods_SS.csv")
trans_names <- single_step_table[!is.na(single_step_table[[length(single_step_table)]]), "Trans_name"] # last calibration column for filtering

Viable_transitions_by_period_MS <- lapply(Viable_transitions_by_period_MS, function(x) {
  x[x$Trans_name %in% trans_names, ]
})

# Save multi-step viable transitions individually
mapply(
  function(trans_table, table_name) {
    trans_table[, c("Trans_ID", "Trans_name", "Initial_class", "Final_class")] <- NULL
    write_csv(trans_table, file = file.path(trans_rates_dir, paste0("Calibration_", table_name, "_viable_trans.csv")))
  },
  trans_table = Viable_transitions_by_period_MS,
  table_name = names(Viable_transitions_by_period_MS)
)

# Merge multi-step transitions over time
Trans_tables_bound_MS <- data.table::rbindlist(Viable_transitions_by_period_MS, idcol = "Period")
Trans_tables_bound_MS$Trans_ID <- NULL
Trans_table_time_MS <- tidyr::pivot_wider(Trans_tables_bound_MS, names_from = "Period", values_from = "Rate")
write.csv(Trans_table_time_MS, "Data/Transition_tables/trans_rates_table_calibration_periods_MS.csv")
