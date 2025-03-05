#' Transition identification
#'
#' Transition_identification: Using land use data from calibration (historic) periods to identify
#' LULC transitions to be modeled in future simulations
#'
#' @author Ben Black

transition_identification <- function(config = get_config()) {
  ### =========================================================================
  ### A- Preparation
  ### =========================================================================

  # Extract years from LULC filenames
  lulc_years <- gsub(
    ".*?([0-9]+).*",
    "\\1",
    list.files(config[["historic_lulc_basepath"]], full.names = FALSE, pattern = ".tif$")
  )

  # Read aggregation scheme externally provided
  agg_scheme <- readxl::read_excel(config[["LULC_aggregation_path"]])

  # Prepare LULC classes
  lulc_classes <- data.frame(label = unique(agg_scheme$Class_abbreviation))
  lulc_classes$value <- sapply(
    lulc_classes$label,
    function(x) {
      unique(agg_scheme[
        agg_scheme$Class_abbreviation == x,
        "Aggregated_ID"
      ])
    }
  )

  # Directory for transition rate tables
  ensure_dir(config[["trans_rates_raw_dir"]])

  ### =========================================================================
  ### B- Calculate historic areal change for LULC classes
  ### =========================================================================

  # Load LULC rasters with terra
  lulc_rasters <- lapply(
    list.files(config[["historic_lulc_basepath"]], full.names = TRUE, pattern = ".tif$"),
    terra::rast,
    raw = TRUE
  )
  names(lulc_rasters) <- lulc_years

  # Compute area (pixel counts) for each LULC raster
  lulc_areas <- lapply(lulc_rasters, terra::freq, bylayer = FALSE)

  # Merge area tables by LULC class value
  lulc_areal_change <- Reduce(function(x, y) merge(x, y, by = "value"), lulc_areas)
  names(lulc_areal_change)[2:5] <- names(lulc_areas)

  # Add LULC numeric values (Aggregated_ID) to the table
  lulc_areal_change |>
    dplyr::left_join(
      dplyr::distinct(
        agg_scheme,
        value = Class_abbreviation,
        numeric_value = Aggregated_ID
      ),
      by = "value"
    ) |>
    dplyr::rename(
      # somewhere along the way, the addition of a RAT must have changed the frequency table
      LULC_class = value,
      value = numeric_value
    ) |>
    write.csv(
      file.path(config[["trans_rates_raw_dir"]], "lulc_historic_areal_change.csv"),
      row.names = FALSE
    )

  ### =========================================================================
  ### C- Preparing Transition tables for each calibration period
  ### =========================================================================

  # Dinamica produces tables of net transition rates (i.e. percentage of land that will
  # change from one state to another) These net rates of change are then used to
  # calculate gross rates of change during the CA allocation process by first
  # calculating areal change based on the current simulated landscape and then dividing
  # to number of cells that must transition

  # The net transition rate tables can be calculated for single steps i.e. the
  # difference between two initial and final historical landscape maps Or as Multi-step
  # rates by dividing the single step by a specificed number of time steps. given that
  # we want to model 5 year time steps for the future I have produced multi-step
  # transition rate tables for the 3 historical periods using 2 time steps for each
  # period which is a simplifaction given that the first two periods (1985-1997;
  # 1997-2009) are seperated by 12 years and the final period (2009-2018) 9 years.

  # Transition tables produced by Dinamica contain all possible transitions so they need
  # to be subset to only transitions we want to model

  # Importantly Dinamica only accepts net transition rate tables in a very specific
  # format So in preparing tables for each scenarios future time points then we need to
  # stick to this

  # The order of the rows in the table is also crucial because the probability maps
  # produced for each transition need to be named according to their row number so the
  # value is associated correctly in allocation.

  # create a list combining the correct LULC years for each transition period
  lulc_change_periods <- c()
  for (i in 1:(length(lulc_years) - 1)) {
    lulc_change_periods[[i]] <- c(lulc_years[i], lulc_years[i + 1])
  }
  names(lulc_change_periods) <- sapply(
    lulc_change_periods, function(x) paste(x[1], x[2], sep = "_")
  )

  # Run function over each period
  mapply(
    lulcc_periodictransmatrices,
    raster_combo = lulc_change_periods,
    period_name = names(lulc_change_periods),
    MoreArgs = list(
      raster_stack = lulc_rasters,
      step_length = config[["step_length"]],
      trans_rates_dir = config[["trans_rates_raw_dir"]]
    ),
    SIMPLIFY = FALSE
  )

  ### =========================================================================
  ### C- Subsetting to Viable Transitions
  ### =========================================================================

  # Load single-step net transition tables produced for historic periods
  calibration_singlestep_tables <- lapply(
    list.files(config[["trans_rates_raw_dir"]], full.names = TRUE, pattern = "singlestep"), read.csv
  )
  names(calibration_singlestep_tables) <- gsub(
    "calibration_|_singlestep_trans_table.csv", "",
    list.files(config[["trans_rates_raw_dir"]], full.names = FALSE, pattern = "singlestep")
  )

  viable_trans_by_period_ss <- lapply(calibration_singlestep_tables, function(x) {
    x$Initial_class <- sapply(
      x$From., function(y) lulc_classes[lulc_classes$value == y, "label"]
    )
    x$Final_class <- sapply(
      x$To., function(y) lulc_classes[lulc_classes$value == y, "label"]
    )
    x$Trans_name <- paste(x$Initial_class, x$Final_class, sep = "_")

    # Subset by inclusion threshold
    x <- x[x$Rate * 100 >= config[["inclusion_thres"]], ]

    # Subset by transitions from non-static classes
    x <- x[x$Initial_class != "Static", ]

    x$Trans_ID <- sprintf("%02d", seq_len(nrow(x)))
    return(x)
  })

  # Save viable transitions list
  saveRDS(viable_trans_by_period_ss, config[["viable_transitions_lists"]])

  ### =========================================================================
  ### D- Combining Transition Rates for Calibration Periods
  ### =========================================================================

  # Single-step transitions over time
  trans_tables_bound_ss <- data.table::rbindlist(
    viable_trans_by_period_ss,
    idcol = "Period"
  )
  trans_tables_bound_ss$Trans_ID <- NULL
  trans_table_time_ss <- tidyr::pivot_wider(
    trans_tables_bound_ss,
    names_from = "Period", values_from = "Rate"
  )
  write.csv(
    trans_table_time_ss,
    file.path(
      config[["trans_rates_raw_dir"]],
      "trans_rates_table_calibration_periods_SS.csv"
    )
  )

  # Repeat for the multi-step transition matrices

  # Load multi-step net transition tables produced for historic periods
  calibration_multistep_tables <- lapply(
    list.files(
      config[["trans_rates_raw_dir"]],
      full.names = TRUE, pattern = "multistep"
    ), read.csv
  )
  names(calibration_multistep_tables) <- gsub(
    "calibration_|_multistep_trans_table.csv", "",
    list.files(
      config[["trans_rates_raw_dir"]],
      full.names = FALSE, pattern = "multistep"
    )
  )

  viable_trans_by_period_ms <- lapply(calibration_multistep_tables, function(x) {
    x$Initial_class <- sapply(
      x$From., function(y) lulc_classes[lulc_classes$value == y, "label"]
    )
    x$Final_class <- sapply(
      x$To., function(y) lulc_classes[lulc_classes$value == y, "label"]
    )
    x$Trans_name <- paste(x$Initial_class, x$Final_class, sep = "_")

    # Remove transitions from static class
    x <- x[x$Initial_class != "Static", ]

    x$Trans_ID <- sprintf("%02d", seq_len(nrow(x)))
    return(x)
  })

  # because the same inclusion threshold does not apply to the multi-step rates
  # instead subset by the trans names in the single step equivalent table
  single_step_table <- read.csv(
    file.path(
      config[["trans_rates_raw_dir"]], "trans_rates_table_calibration_periods_SS.csv"
    )
  )
  trans_names <- single_step_table[
    !is.na(single_step_table[[length(single_step_table)]]), "Trans_name"
  ] # last calibration column for filtering

  viable_trans_by_period_ms <- lapply(viable_trans_by_period_ms, function(x) {
    x[x$Trans_name %in% trans_names, ]
  })

  # Save multi-step viable transitions individually
  mapply(
    function(trans_table, table_name) {
      trans_table[, c("Trans_ID", "Trans_name", "Initial_class", "Final_class")] <- NULL
      readr::write_csv(
        trans_table,
        file = file.path(
          config[["trans_rates_raw_dir"]],
          paste0("calibration_", table_name, "_viable_trans.csv")
        )
      )
    },
    trans_table = viable_trans_by_period_ms,
    table_name = names(viable_trans_by_period_ms)
  )

  # Merge multi-step transitions over time
  trans_tables_bound_ms <- data.table::rbindlist(viable_trans_by_period_ms, idcol = "Period")
  trans_tables_bound_ms$Trans_ID <- NULL
  trans_table_time_ms <- tidyr::pivot_wider(
    trans_tables_bound_ms,
    names_from = "Period", values_from = "Rate"
  )
  write.csv(
    trans_table_time_ms,
    file.path(
      config[["trans_rates_raw_dir"]], "trans_rates_table_calibration_periods_MS.csv"
    )
  )
}

# instantiate function to produce raw, single step and multistep
# net percentage transition tables and save each to file
lulcc_periodictransmatrices <- function(
    raster_combo,
    raster_stack,
    period_name,
    step_length,
    trans_rates_dir) {
  # Extract rasters by year, drop RATs and rename
  r1 <- raster_stack[[grep(raster_combo[1], names(raster_stack))]]
  levels(r1) <- NULL
  names(r1) <- "from_lulc"

  r2 <- raster_stack[[grep(raster_combo[2], names(raster_stack))]]
  levels(r2) <- NULL
  names(r2) <- "to_lulc"

  # Produce transition matrix of areal changes for the whole period
  # Remove RATs from both rasters

  # Now do the crosstab with numerical values
  trans_rates_for_period <- terra::crosstab(c(r1, r2), long = FALSE)

  # Convert to percentage changes
  sum01 <- apply(trans_rates_for_period, MARGIN = 1, FUN = sum)
  percchange <- sweep(trans_rates_for_period, MARGIN = 1, STATS = sum01, FUN = "/")
  perchange_for_period <- as.data.frame(percchange)

  # Convert columns to numeric
  perchange_for_period$from_lulc <- as.numeric(as.character(perchange_for_period$from_lulc))
  perchange_for_period$to_lulc <- as.numeric(as.character(perchange_for_period$to_lulc))

  # Remove persistence and zero rows
  perchange_for_period <- perchange_for_period[
    perchange_for_period$from_lulc != perchange_for_period$to_lulc,
  ]
  perchange_for_period <- perchange_for_period[
    perchange_for_period$Freq != 0,
  ]

  # Sort by initial class
  perchange_for_period <- perchange_for_period[order(perchange_for_period$from_lulc), ]

  # alter column names to match Dinamica trans tables
  colnames(perchange_for_period) <- c("From*", "To*", "Rate")
  # Save single-step table
  readr::write_csv(
    perchange_for_period,
    file.path(trans_rates_dir, paste0("calibration_", period_name, "_singlestep_trans_table.csv"))
  )

  # Multi-step calculation
  period_length <- as.numeric(raster_combo[2]) - as.numeric(raster_combo[1])
  num_steps <- ceiling(period_length / step_length)

  perchange_for_period_multistep <- perchange_for_period
  perchange_for_period_multistep$Rate <- perchange_for_period_multistep$Rate / num_steps

  # Save multi-step table
  readr::write_csv(
    perchange_for_period_multistep, file.path(
      trans_rates_dir, paste0("calibration_", period_name, "_multistep_trans_table.csv")
    )
  )
}
