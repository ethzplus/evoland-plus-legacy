#' Create predictor parquets for large sparse rasters
#' @param config Project configuration
#' @param refresh_cache If TRUE, re-extract valid cell IDs from mask
#' @param mask_raster_path Path to mask raster
#' @param output_dir Output directory for parquet files
#' @param rows_per_block Number of raster rows to read per block
#' @param chunk_size Number of cells to process per write operation
#' @return List with valid cell IDs, number of cells, and time periods
create_predictor_parquets <- function(
  config = get_config(),
  refresh_cache = FALSE,
  mask_raster_path = config[["ref_grid_path"]],
  output_dir = file.path(config[["predictors_prepped_dir"]], "parquet_data"),
  rows_per_block = 1000,
  chunk_size = 5e6
) {
  pred_yaml_file <- config[["pred_table_path"]]
  pred_config <- yaml::yaml.load_file(pred_yaml_file)

  # Create directory structure
  ensure_dir(output_dir)
  ensure_dir(file.path(output_dir, "static"))
  ensure_dir(file.path(output_dir, "dynamic"))

  message("Step 1: Extracting valid cell IDs\n")

  # Check if valid_cell_ids.rds exists
  valid_ids_path <- file.path(output_dir, "valid_cell_ids.rds")
  if (file.exists(valid_ids_path) && !refresh_cache) {
    cat("✓ Found existing valid_cell_ids.rds, loading...\n")
    valid_cell_ids <- readRDS(valid_ids_path)
    cat(sprintf(
      "\n✓ Loaded %s valid cells\n\n",
      format(length(valid_cell_ids), big.mark = ",")
    ))
  } else {
    cat(
      "✓ No existing valid_cell_ids.rds found, extracting from mask raster...\n"
    )
    # Extract valid cell IDs
    valid_cell_ids <- extract_valid_cells(mask_raster_path, rows_per_block)
    saveRDS(valid_cell_ids, valid_ids_path)
    cat(sprintf(
      "\n✓ Found %s valid cells\n\n",
      format(length(valid_cell_ids), big.mark = ",")
    ))
  }

  # Process static variables
  cat("Step 2: Processing static predictors\n")

  # Extract static variables from config
  static_rasters <- extract_static_variables(pred_config)

  # temporaily remove 'dist_to_airports' due to data issues
  static_rasters[["dist_to_airports"]] <- NULL
  static_rasters[["aspect"]] <- NULL
  static_rasters[["slope"]] <- NULL

  # subset to first 5 entries for testing
  static_rasters <- static_rasters[1:5]

  start_time <- Sys.time()
  if (length(static_rasters) > 0) {
    write_static_parquet(
      cell_ids = valid_cell_ids,
      static_rasters = static_rasters,
      output_path = file.path(
        output_dir,
        "static",
        "predictors_static.parquet"
      ),
      chunk_size = chunk_size
    )
  } else {
    cat("\nNo static variables found\n")
  }
  end_time <- Sys.time()
  cat(sprintf(
    "\nStatic processing time: %.2f minutes\n\n",
    as.numeric(difftime(end_time, start_time, units = "mins"))
  ))

  # Process dynamic variables
  cat("Step 3: Creating dynamic predictors by time period\n")

  # Reorganize config by grouping into a nested list time_period -> scenario -> variable
  period_structure <- reorganize_by_period(pred_config = pred_config)

  # Get vector of time periods
  time_periods <- names(period_structure)

  cat(sprintf(
    "\nFound %d time periods: %s\n\n",
    length(time_periods),
    paste(time_periods, collapse = ", ")
  ))

  # Process each time period
  for (period in time_periods) {
    cat(sprintf("Processing time period: %s\n", period))

    parquet_file <- file.path(
      output_dir,
      "dynamic",
      sprintf("period_%s.parquet", period)
    )

    write_dynamic_parquet_streaming(
      cell_ids = valid_cell_ids,
      period_data = period_structure[[period]],
      output_path = parquet_file,
      chunk_size = chunk_size
    )

    cat("\n")
  }

  # Create metadata
  create_metadata(pred_config, output_dir, time_periods)

  cat("✓ COMPLETE!\n")

  list_output_files(output_dir)

  return(invisible(list(
    valid_cell_ids = valid_cell_ids,
    n_cells = length(valid_cell_ids),
    time_periods = time_periods
  )))
}

#' Extract valid (non-NA) cell IDs from mask raster
#' using block processing to minimize memory usage
#' @param mask_raster_path Path to mask raster
#' @param rows_per_block Number of raster rows to read per block
#' @return Integer vector of valid cell IDs
extract_valid_cells <- function(
  mask_raster_path,
  rows_per_block
) {
  mask <- terra::rast(mask_raster_path)
  n_rows <- terra::nrow(mask)
  n_cols <- terra::ncol(mask)

  # Start read
  terra::readStart(mask)

  valid_cell_ids <- integer(0)
  n_blocks <- ceiling(n_rows / rows_per_block)

  for (block_idx in seq_len(n_blocks)) {
    start_row <- (block_idx - 1) * rows_per_block + 1
    n_rows_in_block <- min(rows_per_block, n_rows - start_row + 1)

    cat(sprintf(
      "\rProcessing rows %d-%d (block %d/%d)     ",
      start_row,
      start_row + n_rows_in_block - 1,
      block_idx,
      n_blocks
    ))

    mask_vals <- terra::readValues(
      mask,
      row = start_row,
      nrows = n_rows_in_block
    )
    start_cell <- (start_row - 1) * n_cols + 1
    end_cell <- start_cell + length(mask_vals) - 1
    block_cell_ids <- start_cell:end_cell
    non_na <- !is.na(mask_vals)
    valid_cell_ids <- c(valid_cell_ids, block_cell_ids[non_na])
  }

  cat("\n")
  terra::readStop(mask)

  return(valid_cell_ids)
}

#' Write static predictors using streaming approach
#' @param cell_ids Integer vector of valid cell IDs
#' @param static_rasters Named list of static raster file paths
#' @param output_path Output parquet file path
#' @param chunk_size Number of cells to process per write operation
#' @return NULL
write_static_parquet <- function(
  cell_ids,
  static_rasters,
  output_path,
  chunk_size
) {
  library(arrow)

  n_cells <- length(cell_ids)
  n_chunks <- ceiling(n_cells / chunk_size)

  cat(sprintf(
    "\nWriting %d variables in %d chunks...\n",
    length(static_rasters),
    n_chunks
  ))

  # Determine reference raster (for x/y coordinate lookup)
  ref_rast <- terra::rast(static_rasters[[1]])

  # Remove the pre-defined schema creation
  # Instead, we'll create it from the first chunk

  first_chunk <- TRUE
  writer <- NULL
  sink <- NULL

  for (chunk_idx in seq_len(n_chunks)) {
    start_idx <- (chunk_idx - 1) * chunk_size + 1
    end_idx <- min(chunk_idx * chunk_size, n_cells)
    chunk_cell_ids <- cell_ids[start_idx:end_idx]

    cat(sprintf(
      "\r  Chunk %d/%d (%s cells)     ",
      chunk_idx,
      n_chunks,
      format(length(chunk_cell_ids), big.mark = ",")
    ))

    # Compute coordinates for these cell IDs
    xy <- terra::xyFromCell(ref_rast, chunk_cell_ids)

    # Build chunk data frame
    chunk_df <- data.frame(
      cell_id = as.integer(chunk_cell_ids),
      x = xy[, 1],
      y = xy[, 2],
      stringsAsFactors = FALSE
    )

    # Add static variables
    for (var_name in names(static_rasters)) {
      r <- terra::rast(static_rasters[[var_name]])
      vals <- r[chunk_cell_ids]

      # Flatten if structured
      if (is.data.frame(vals) || is.list(vals)) {
        vals <- vals[[1]]
      }

      chunk_df[[var_name]] <- vals
    }

    # Create writer on first chunk (infer schema from data)
    if (first_chunk) {
      tbl <- arrow::Table$create(chunk_df)
      schema <- tbl$schema

      sink <- arrow::FileOutputStream$create(output_path)
      writer <- ParquetFileWriter$create(
        schema = schema,
        sink = sink,
        properties = ParquetWriterProperties$create(
          names(schema),
          compression = "zstd",
          compression_level = 9
        )
      )
      writer$WriteTable(tbl, chunk_size = nrow(chunk_df))
      first_chunk <- FALSE
    } else {
      writer$WriteTable(
        arrow::Table$create(chunk_df),
        chunk_size = nrow(chunk_df)
      )
    }

    rm(chunk_df, xy)
    gc(verbose = FALSE)
  }

  # Close writer
  writer$Close()
  sink$close()

  cat("\n✓ Static parquet created successfully with coordinates\n")
}


#' Write dynamic predictors using streaming approach (one scenario at a time)
#' to minimize memory usage
#' @param cell_ids Integer vector of valid cell IDs
#' @param period_data Named list of scenarios, each containing named list of variable raster paths
#' @param output_path Output parquet file path
#' @param chunk_size Number of cells to process per write operation
write_dynamic_parquet_streaming <- function(
  cell_ids,
  period_data,
  output_path,
  chunk_size
) {
  n_cells <- length(cell_ids)
  n_chunks <- ceiling(n_cells / chunk_size)
  scenarios <- names(period_data)

  cat(sprintf(
    "  %d scenarios: %s\n",
    length(scenarios),
    paste(scenarios, collapse = ", ")
  ))

  # Get variable names (assume all scenarios have same variables)
  var_names <- names(period_data[[scenarios[1]]])

  # Prepare schema
  schema_list <- list(
    cell_id = arrow::int64(),
    scenario = arrow::utf8()
  )
  for (var_name in var_names) {
    schema_list[[var_name]] <- arrow::float64()
  }
  schema <- do.call(arrow::schema, schema_list)

  # Open parquet writer
  sink <- arrow::FileOutputStream$create(output_path)
  writer <- arrow::ParquetFileWriter$create(
    sink,
    schema,
    properties = arrow::ParquetWriterProperties$create(
      compression = "zstd",
      compression_level = 9
    )
  )

  # Process each scenario separately to minimize memory
  for (scenario in scenarios) {
    cat(sprintf("  Scenario: %s\n", scenario))

    for (chunk_idx in seq_len(n_chunks)) {
      start_idx <- (chunk_idx - 1) * chunk_size + 1
      end_idx <- min(chunk_idx * chunk_size, n_cells)
      chunk_cell_ids <- cell_ids[start_idx:end_idx]

      cat(sprintf("\r    Chunk %d/%d     ", chunk_idx, n_chunks))

      # Build chunk for this scenario only
      chunk_df <- data.frame(
        cell_id = chunk_cell_ids,
        scenario = scenario,
        stringsAsFactors = FALSE
      )

      for (var_name in var_names) {
        r <- terra::rast(period_data[[scenario]][[var_name]])
        chunk_df[[var_name]] <- r[chunk_cell_ids]
      }

      # Write immediately
      writer$WriteTable(arrow::Table$create(chunk_df))

      rm(chunk_df)
      gc(verbose = FALSE)
    }
    cat("\n")
  }

  writer$Close()
  sink$close()

  cat(sprintf(
    "  ✓ Dynamic parquet created (%s rows)\n",
    format(n_cells * length(scenarios), big.mark = ",")
  ))
}

#' Reorganize dynamic config from flat variable list to period->scenario->variable
#' Uses base_name field to determine the column name in output parquet
#' @param pred_config Predictor configuration list
#' @return Nested list organized by period and scenario
reorganize_by_period <- function(pred_config) {
  period_structure <- list()

  # Iterate through all variables in config
  for (var_key in names(pred_config)) {
    var_config <- pred_config[[var_key]]

    # Skip static variables
    if (var_config$static_or_dynamic == "static") {
      next
    }

    # Get period and scenario for this variable
    period <- as.character(var_config$period)
    scenario <- if (
      is.null(var_config$scenario_variant) ||
        is.na(var_config$scenario_variant)
    ) {
      "baseline" # Default scenario name if none specified
    } else {
      as.character(var_config$scenario_variant)
    }

    raster_path <- var_config$path

    # Use base_name as the variable name for parquet columns
    # This ensures temp_2030_rcp45 and temp_2030_rcp85 both become 'temperature'
    var_name <- if (
      !is.null(var_config$base_name) && !is.na(var_config$base_name)
    ) {
      var_config$base_name
    } else {
      # Fallback to YAML key if base_name not provided
      var_key
    }

    # Initialize nested structure if needed
    if (is.null(period_structure[[period]])) {
      period_structure[[period]] <- list()
    }
    if (is.null(period_structure[[period]][[scenario]])) {
      period_structure[[period]][[scenario]] <- list()
    }

    # Check for duplicate variable names within same period/scenario
    if (!is.null(period_structure[[period]][[scenario]][[var_name]])) {
      stop(sprintf(
        "Duplicate base_name '%s' found for period '%s' and scenario '%s'. Check YAML entries: %s",
        var_name,
        period,
        scenario,
        var_key
      ))
    }

    # Add variable path using base_name as key
    period_structure[[period]][[scenario]][[var_name]] <- raster_path
  }

  return(period_structure)
}

#' Extract static variables from flat config
#' Uses base_name field to determine the column name in output parquet
#' @param pred_config Predictor configuration list
#' @return Named list of static variable paths
extract_static_variables <- function(pred_config) {
  static_rasters <- list()

  for (var_key in names(pred_config)) {
    var_config <- pred_config[[var_key]]

    if (var_config$static_or_dynamic == "static") {
      # Use base_name as the variable name for parquet columns
      var_name <- if (
        !is.null(var_config$base_name) && !is.na(var_config$base_name)
      ) {
        var_config$base_name
      } else {
        # Fallback to YAML key if base_name not provided
        var_key
      }

      # Check for duplicate variable names
      if (!is.null(static_rasters[[var_name]])) {
        stop(sprintf(
          "Duplicate base_name '%s' found for static variables. Check YAML entries: %s",
          var_name,
          var_key
        ))
      }

      # if there is no path entry then skip
      if (is.null(var_config$path) || is.na(var_config$path)) {
        next
      }

      rel_path <- file.path(config[["data_basepath"]], var_config$path)

      static_rasters[[var_name]] <- rel_path
    }
  }

  return(static_rasters)
}

#' Create metadata parquet file
#' @param pred_config Predictor configuration list
#' @param output_dir Output directory
#' @param time_periods Vector of time period names
create_metadata <- function(
  pred_config,
  output_dir,
  time_periods
) {
  # Static metadata
  static_rasters <- extract_static_variables(pred_config)

  if (length(static_rasters) > 0) {
    static_meta <- data.frame(
      file_name = "static/predictors_static.parquet",
      type = "static",
      period = NA_character_,
      n_variables = length(static_rasters),
      variables = paste(names(static_rasters), collapse = ","),
      stringsAsFactors = FALSE
    )
  } else {
    static_meta <- data.frame()
  }

  # Dynamic metadata
  if (length(time_periods) > 0) {
    dynamic_meta <- data.frame(
      file_name = file.path(
        "dynamic",
        paste0("period_", time_periods, ".parquet")
      ),
      type = "dynamic",
      period = time_periods,
      n_variables = NA_integer_,
      variables = NA_character_,
      stringsAsFactors = FALSE
    )

    # Get variable info for each period
    period_structure <- reorganize_by_period(pred_config)

    for (i in seq_along(time_periods)) {
      period <- time_periods[i]
      first_scenario <- period_structure[[period]][[names(period_structure[[
        period
      ]])[1]]]

      dynamic_meta$n_variables[i] <- length(first_scenario)
      dynamic_meta$variables[i] <- paste(names(first_scenario), collapse = ",")
    }

    metadata <- rbind(static_meta, dynamic_meta)
  } else {
    metadata <- static_meta
  }

  arrow::write_parquet(metadata, file.path(output_dir, "metadata.parquet"))

  cat("\n✓ Metadata created\n")
}

#' List all output files with sizes
#' @param output_dir Output directory path
list_output_files <- function(output_dir) {
  cat("\nCreated files:\n")
  files <- list.files(output_dir, recursive = TRUE, full.names = TRUE)

  total_size <- 0
  for (f in files) {
    size_mb <- file.info(f)$size / 1024^2
    total_size <- total_size + size_mb
    cat(sprintf(
      "  %s (%.1f MB)\n",
      gsub(paste0(output_dir, "/"), "", f),
      size_mb
    ))
  }

  cat(sprintf("\nTotal size: %.1f MB\n", total_size))
}
