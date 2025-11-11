#' transition_dataset_creation
#'
#' Script for gathering the layers of LULC (dependent variable) for each
#' historic period then separating into viable LULC transitions at the scale of
#' Peru and regions
#'
#' @param config list of configuration parameters
#' @author Ben Black
#' @export

transition_dataset_prep <- function(
  config = get_config(),
  refresh_cache = FALSE,
  mask_raster_path = list.files(
    config[["reg_dir"]],
    pattern = "regions.tif$",
    full.names = TRUE
  ),
  rows_per_block = 1000
) {
  ### =========================================================================
  ### A- Preparation
  ### =========================================================================

  # create save dirs
  purrr::walk(
    file.path(
      config[["trans_pre_pred_filter_dir"]],
      config[["data_periods"]]
    ),
    ensure_dir
  )

  ### =========================================================================
  ### B- Load data and filter for each time period
  ### =========================================================================

  # The predictor data table is used to identify the file names of variables that
  # are to be included in the stack for each time period

  # Load existing DEM raster and predictor YAML
  pred_yaml_file <- config[["pred_table_path"]]
  pred_table <- yaml::yaml.load_file(pred_yaml_file)

  # Create data frame of LULC file paths for each period and combine with the regions path
  lulc_raster_paths <- data.frame(matrix(ncol = 2, nrow = 4))
  colnames(lulc_raster_paths) <- c("File_name", "Layer_name")
  lulc_raster_paths["File_name"] <- as.data.frame(
    list.files(
      config[["aggregated_lulc_dir"]],
      pattern = ".tif$",
      full.names = TRUE
    )
  )

  # extract everything that begins with / and runs to the end of the string.
  lulc_raster_paths[["Layer_name"]] <-
    lulc_raster_paths[["File_name"]] |>
    stringr::str_extract("(?<=/)[^/]*$") |>
    stringr::str_remove(".tif$")

  # Create regexes for LULC periods
  lulc_period_regexes <- lapply(config[["data_periods"]], function(x) {
    stringr::str_replace(x, pattern = "_", "|")
  })
  names(lulc_period_regexes) <- config[["data_periods"]]

  # Filter LULC files by period
  lulc_paths_by_period <- lapply(lulc_period_regexes, function(x) {
    dplyr::filter(lulc_raster_paths, grepl(x, File_name))
  })

  # Change layer names to Initial and Final for easier splitting later
  lulc_paths_by_period <- lapply(lulc_paths_by_period, function(x) {
    x$Layer_name[1] <- "Initial_class"
    x$Layer_name[2] <- "Final_class"
    return(x)
  })

  # NOTE: We no longer add region_path here since we'll use pre-extracted regions
  combined_paths_by_period <- lulc_paths_by_period

  # read in all rasters in the list to check compatibility before stacking
  rasters_by_periods <- purrr::map(combined_paths_by_period, function(x) {
    raster_list <- purrr::map(x$File_name, function(raster_file_name) {
      r <- terra::rast(raster_file_name)
    })
    names(raster_list) <- x$Layer_name
    purrr::compact(raster_list) # drop NULLs
  })
  names(rasters_by_periods) <- config[["data_periods"]]

  ### =========================================================================
  ### C- Confirm Raster compatibility for stacking
  ### =========================================================================

  # Use a function to test rasters in the list against an 'exemplar'
  # which has the extent, crs and resolution that we want
  # in this case the Ref_grid file used for re-sampling some of the predictors.

  purrr::walk(
    rasters_by_periods,
    lulcc.TestRasterCompatibility,
    exemplar_raster = terra::rast(config[["ref_grid_path"]])
  )

  # Create SpatRaster stacks for each time period.
  rasterstacks_by_periods <- mapply(
    function(raster_list, period_name) {
      # Combine layers into a single SpatRaster
      raster_stack_for_period <- terra::rast(raster_list)
    },
    raster_list = rasters_by_periods,
    period_name = names(rasters_by_periods),
    SIMPLIFY = FALSE
  )

  rm(rasters_by_periods)

  ### =========================================================================
  ### C.1- Load valid cell data (SAME SOURCE AS PREDICTORS)
  ### =========================================================================

  # Use the SAME cached valid cell data as the predictor script
  valid_ids_parquet <- file.path(
    config[["data_basepath"]],
    "spatial_reference_grid",
    "valid_cell_ids_regions.parquet"
  )

  if (file.exists(valid_ids_parquet) && !refresh_cache) {
    cat("✓ Found existing valid_cell_ids.parquet, using...\n")
    valid_cell_data <- valid_ids_parquet # Store path, not data

    # Get summary stats without loading into memory
    ds <- arrow::open_dataset(valid_cell_data)
    n_cells <- ds %>% count() %>% collect() %>% pull(n)
    n_regions <- ds %>% distinct(region) %>% count() %>% collect() %>% pull(n)

    cat(sprintf(
      "\n✓ Found %s valid cells across %d regions\n\n",
      format(n_cells, big.mark = ","),
      n_regions
    ))
  } else {
    cat("✓ Extracting valid cells with region values from mask raster...\n")
    valid_cell_data <- extract_valid_cells_with_region(
      mask_raster_path,
      rows_per_block,
      output_path = valid_ids_parquet,
      keep_in_memory = FALSE # Keep on disk to save memory
    )
    cat(sprintf(
      "\n✓ Results saved to: %s\n\n",
      basename(valid_cell_data)
    ))
  }

  periods <- config[["data_periods"]]
  # Subset to specific period if needed (remove this line to process all)
  periods <- periods[3]

  # Apply function to prepare parquet files for each period
  message("Starting data extraction for each transition period")
  purrr::walk(
    periods,
    process_period_transitions,
    config = config,
    rasterstacks_by_periods = rasterstacks_by_periods,
    valid_cell_data = valid_cell_data
  )
  message("Preparation of transition datasets complete")
}

#' Efficiently process transitions for one period and write partitioned parquets by region
#' (one row per cell, one column per transition, partitioned by region)
#'
#' @param period Character period label (e.g. "2020_2030")
#' @param rasterstacks_by_periods Named list of terra rasters for each period
#' @param config Named list of configuration parameters
#' @param valid_cell_data Data frame with cell_id and region columns
#' @param chunk_size Number of cells to process per chunk (default 1e6)
#'
#' @return NULL (writes partitioned parquet files)
process_period_transitions <- function(
  period,
  rasterstacks_by_periods,
  config,
  valid_cell_data,
  chunk_size = 1e6
) {
  library(terra)
  library(arrow)

  message("\n=== Processing transitions for period: ", period, " ===")

  # ---- Load rasters ----
  r_stack <- rasterstacks_by_periods[[paste(period)]]
  init_r <- r_stack$Initial_class
  fin_r <- r_stack$Final_class

  regionalization <- isTRUE(config[["regionalization"]])

  viable_trans_list <- readRDS(config[["viable_transitions_lists"]])[[paste(
    period
  )]]
  trans_names <- viable_trans_list$Trans_name

  # ---- Prepare output ----
  out_dir <- file.path(config[["trans_pre_pred_filter_dir"]], period)
  if (!dir.exists(out_dir)) {
    dir.create(out_dir, recursive = TRUE)
  }

  n_valid <- nrow(valid_cell_data)
  n_chunks <- ceiling(n_valid / chunk_size)
  regions <- sort(unique(valid_cell_data$region))

  message(
    "Valid cells: ",
    format(n_valid, big.mark = ","),
    " (",
    n_chunks,
    " chunks of ",
    format(chunk_size, big.mark = ","),
    ")"
  )
  message("Regions: ", paste(regions, collapse = ", "))
  message("→ Writing to: ", out_dir)

  # Initialize writer storage (region -> writer)
  writers <- list()
  sinks <- list()
  schema <- NULL

  # ---- Chunk processing ----
  for (chunk_idx in seq_len(n_chunks)) {
    start_idx <- (chunk_idx - 1) * chunk_size + 1
    end_idx <- min(chunk_idx * chunk_size, n_valid)
    chunk_rows <- valid_cell_data[start_idx:end_idx, , drop = FALSE]

    cat(sprintf("\r  Processing chunk %d/%d...", chunk_idx, n_chunks))

    # Extract raster values using pre-identified cell IDs
    chunk_ids <- chunk_rows$cell_id
    init_vals <- init_r[chunk_ids]
    fin_vals <- fin_r[chunk_ids]

    # Use pre-extracted region values from valid_cell_data
    region_vals <- chunk_rows$region

    # ---- Build transition columns ----
    trans_mat <- matrix(
      NA_integer_,
      nrow = length(chunk_ids),
      ncol = nrow(viable_trans_list)
    )
    colnames(trans_mat) <- trans_names

    for (i in seq_len(nrow(viable_trans_list))) {
      from_class <- viable_trans_list$From.[i]
      to_class <- viable_trans_list$To.[i]

      # Transition logic:
      #   1 → cell was from_class and changed to to_class
      #   0 → cell was from_class but did NOT change to to_class
      #   NA → cell was not from_class at all
      trans_mat[, i] <- ifelse(
        init_vals == from_class,
        ifelse(fin_vals == to_class, 1L, 0L),
        NA_integer_
      )
    }

    # ---- Assemble chunk ----
    chunk_df <- data.frame(
      cell_id = as.numeric(chunk_ids),
      region = as.integer(region_vals),
      stringsAsFactors = FALSE
    )
    chunk_df <- cbind(chunk_df, as.data.frame(trans_mat))

    # ---- Split by region and write to partitions ----
    for (region in unique(chunk_df$region)) {
      region_chunk <- chunk_df[chunk_df$region == region, , drop = FALSE]

      # Create writer for this region on first encounter
      if (is.null(writers[[as.character(region)]])) {
        # Create schema from first chunk
        if (is.null(schema)) {
          tbl <- arrow::Table$create(region_chunk)
          schema <- tbl$schema
        }

        # Create partition directory
        region_dir <- file.path(out_dir, sprintf("region=%d", region))
        ensure_dir(region_dir)

        # Create output file path
        region_file <- file.path(
          region_dir,
          paste0("transitions_", period, ".parquet")
        )

        # Create writer
        sinks[[as.character(region)]] <- arrow::FileOutputStream$create(
          region_file
        )
        writers[[as.character(region)]] <- arrow::ParquetFileWriter$create(
          schema = schema,
          sink = sinks[[as.character(region)]],
          properties = arrow::ParquetWriterProperties$create(
            names(schema),
            compression = "zstd",
            compression_level = 9
          )
        )
      }

      # Write region chunk to appropriate partition
      writers[[as.character(region)]]$WriteTable(
        arrow::Table$create(region_chunk),
        chunk_size = nrow(region_chunk)
      )

      rm(region_chunk)
    }

    rm(chunk_df, trans_mat, init_vals, fin_vals)
    gc(verbose = FALSE)
  }

  cat("\n")

  # ---- Close writers ----
  cat("  Closing writers...\n")
  for (region in names(writers)) {
    writers[[region]]$Close()
    sinks[[region]]$close()
  }

  message("✓ Transition parquet writing complete.")
  message(sprintf("  Location: %s", out_dir))
  message(sprintf("  Partitions: %d regions", length(regions)))
  message("=== Completed all transitions for period ", period, " ===\n")

  # ---- Sanity check ----
  message("Performing sanity check on written files...")
  parquet_check <- sanity_check_transitions_partitioned(
    transitions_dir = out_dir,
    regions_json_path = if (regionalization) {
      file.path(config[["reg_dir"]], "regions.json")
    } else {
      NULL
    }
  )

  # save results
  sanity_output_path <- file.path(
    out_dir,
    paste0("check_transitions_", period, ".csv")
  )
  readr::write_csv(parquet_check, sanity_output_path)
  message("✓ Sanity check complete\n")
}


#' Sanity check for partitioned transition parquet files
#'
#' @param transitions_dir Directory containing partitioned transition files
#' @param regions_json_path Optional path to regions.json file
#' @return Data frame with sanity check results
sanity_check_transitions_partitioned <- function(
  transitions_dir,
  regions_json_path = NULL
) {
  library(arrow)

  # Open the partitioned dataset
  ds <- arrow::open_dataset(
    transitions_dir,
    partitioning = arrow::hive_partition(region = arrow::int32())
  )

  # Collect basic statistics
  results <- list()

  # Count total rows
  results$total_rows <- ds |>
    dplyr::count() |>
    dplyr::collect() |>
    dplyr::pull(n)

  # Count rows per region
  results$rows_per_region <- ds |>
    dplyr::group_by(region) |>
    dplyr::count() |>
    dplyr::collect()

  # Check for duplicate cell_ids within regions
  duplicates <- ds |>
    dplyr::group_by(region, cell_id) |>
    dplyr::count() |>
    dplyr::filter(n > 1) |>
    dplyr::collect()

  results$has_duplicates <- nrow(duplicates) > 0
  results$n_duplicates <- nrow(duplicates)

  # Get transition column names (exclude cell_id and region)
  schema_names <- names(ds$schema)
  trans_cols <- setdiff(schema_names, c("cell_id", "region"))

  results$n_transition_cols <- length(trans_cols)

  # Check for NAs in transition columns
  na_counts <- ds |>
    dplyr::select(dplyr::all_of(trans_cols)) |>
    dplyr::summarise(dplyr::across(dplyr::everything(), ~ sum(is.na(.)))) |>
    dplyr::collect()

  results$na_summary <- data.frame(
    transition = names(na_counts),
    n_na = as.numeric(na_counts[1, ]),
    stringsAsFactors = FALSE
  )

  # Create summary data frame
  summary_df <- data.frame(
    metric = c(
      "Total rows",
      "Number of regions",
      "Has duplicates",
      "Number of duplicates",
      "Number of transitions"
    ),
    value = c(
      results$total_rows,
      nrow(results$rows_per_region),
      results$has_duplicates,
      results$n_duplicates,
      results$n_transition_cols
    ),
    stringsAsFactors = FALSE
  )

  return(summary_df)
}
