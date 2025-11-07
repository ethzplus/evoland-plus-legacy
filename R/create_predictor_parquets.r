#' Create partitioned predictor parquets for large sparse rasters (streaming approach)
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
  mask_raster_path = list.files(
    config[["reg_dir"]],
    pattern = "regions.tif$",
    full.names = TRUE
  ),
  output_dir = file.path(config[["predictors_prepped_dir"]], "parquet_data"),
  rows_per_block = 1000,
  chunk_size = 2e6
) {
  pred_yaml_file <- config[["pred_table_path"]]
  pred_config <- yaml::yaml.load_file(pred_yaml_file)

  # Create directory structure
  ensure_dir(output_dir)
  ensure_dir(file.path(output_dir, "static"))
  ensure_dir(file.path(output_dir, "dynamic"))

  message("Step 1: Extracting valid cell IDs with region mapping\n")

  # Check if valid_cell_ids.rds exists
  valid_ids_path <- file.path(output_dir, "valid_cell_ids_regions.rds")
  if (file.exists(valid_ids_path) && !refresh_cache) {
    cat("✓ Found existing valid_cell_ids.rds, loading...\n")
    valid_cell_data <- readRDS(valid_ids_path)
    cat(sprintf(
      "\n✓ Loaded %s valid cells\n\n",
      format(nrow(valid_cell_data), big.mark = ",")
    ))
  } else {
    cat("✓ Extracting valid cells with region values from mask raster...\n")
    valid_cell_data <- extract_valid_cells_with_region(
      mask_raster_path,
      rows_per_block
    )
    saveRDS(valid_cell_data, valid_ids_path)
    cat(sprintf(
      "\n✓ Found %s valid cells across %d regions\n\n",
      format(nrow(valid_cell_data), big.mark = ","),
      length(unique(valid_cell_data$region))
    ))
  }

  # Process static variables (PARTITIONED BY REGION)
  cat("Step 2: Processing static predictors (partitioned by region)\n")

  static_rasters <- extract_static_variables(pred_config)

  # Temporarily remove problematic variables
  static_rasters[["dist_to_airports"]] <- NULL
  static_rasters[["aspect"]] <- NULL
  static_rasters[["slope"]] <- NULL

  start_time <- Sys.time()
  if (length(static_rasters) > 0) {
    write_static_parquet_streaming_partitioned(
      cell_data = valid_cell_data,
      static_rasters = static_rasters,
      output_dir = file.path(output_dir, "static"),
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

  # Process dynamic variables (PARTITIONED BY SCENARIO AND REGION)
  cat("Step 3: Creating dynamic predictors by time period (partitioned)\n")

  period_structure <- reorganize_by_period(pred_config = pred_config)

  # Subset to only 2018 (remove to process all periods)
  period_structure <- period_structure["2018_2022"]

  time_periods <- names(period_structure)

  cat(sprintf(
    "\nFound %d time periods: %s\n\n",
    length(time_periods),
    paste(time_periods, collapse = ", ")
  ))

  # Process each time period
  for (period in time_periods) {
    cat(sprintf("Processing time period: %s\n", period))

    output_subdir <- file.path(output_dir, "dynamic", period)
    ensure_dir(output_subdir)

    write_dynamic_parquet_streaming_partitioned(
      cell_data = valid_cell_data,
      period_data = period_structure[[period]],
      output_dir = output_subdir,
      chunk_size = chunk_size
    )

    cat("\n")
  }

  message("Step 4: Creating metadata file\n")
  # Create metadata
  create_metadata(pred_config, output_dir, time_periods)

  list_output_files(output_dir)

  message("\nStep 5: Computing NA counts for each dataset\n")
  # checking for NAs across datasets
  ds_static <- open_dataset(file.path(output_dir, "static"))
  na_static <- compute_na_counts_streaming(ds_static, "Static predictors")

  # save
  saveRDS(
    na_static,
    file.path(output_dir, "static", "na_counts_static_predictors.rds")
  )

  # loop over periods
  for (period in time_periods) {
    ds_dynamic <- open_dataset(
      file.path(
        config[["predictors_prepped_dir"]],
        "parquet_data",
        "dynamic",
        period
      ),
      partitioning = hive_partition(region = int32(), scenario = utf8())
    )
    na_dynamic <- compute_na_counts_streaming(ds_dynamic, "Dynamic predictors")
    # save
    saveRDS(
      na_dynamic,
      file.path(
        output_dir,
        "dynamic",
        period,
        sprintf("na_counts_dynamic_predictors_%s.rds", period)
      )
    )
  }

  message("\nAll steps completed successfully.\n")

  return(invisible(list(
    valid_cell_data = valid_cell_data,
    n_cells = nrow(valid_cell_data),
    time_periods = time_periods
  )))
}


#' Extract valid (non-NA) cell IDs from mask raster WITH region values
#' @param mask_raster_path Path to mask raster (should contain region values)
#' @param rows_per_block Number of raster rows to read per block
#' @return Data frame with cell_id and region columns
extract_valid_cells_with_region <- function(
  mask_raster_path,
  rows_per_block
) {
  mask <- terra::rast(mask_raster_path)
  n_rows <- terra::nrow(mask)
  n_cols <- terra::ncol(mask)

  terra::readStart(mask)

  cell_id_list <- list()
  region_list <- list()

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

    cell_id_list[[block_idx]] <- block_cell_ids[non_na]
    region_list[[block_idx]] <- mask_vals[non_na]
  }

  cat("\n")
  terra::readStop(mask)

  valid_cell_data <- data.frame(
    cell_id = unlist(cell_id_list),
    region = as.integer(unlist(region_list))
  )

  return(valid_cell_data)
}


#' Write static predictors with streaming and partitioning by region
#' @param cell_data Data frame with cell_id and region columns
#' @param static_rasters Named list of static raster file paths
#' @param output_dir Output directory for partitioned dataset
#' @param chunk_size Number of cells to process per write operation
#' @return NULL
write_static_parquet_streaming_partitioned <- function(
  cell_data,
  static_rasters,
  output_dir,
  chunk_size
) {
  library(arrow)

  n_cells <- nrow(cell_data)
  n_chunks <- ceiling(n_cells / chunk_size)

  cat(sprintf(
    "\nWriting %d variables in %d chunks (streaming, partitioned by region)...\n",
    length(static_rasters),
    n_chunks
  ))

  # Get unique regions and create writers for each
  regions <- sort(unique(cell_data$region))

  cat(sprintf(
    "  Found %d regions: %s\n",
    length(regions),
    paste(regions, collapse = ", ")
  ))

  # Initialize writer storage
  writers <- list()
  sinks <- list()
  schema <- NULL

  # Process chunks
  for (chunk_idx in seq_len(n_chunks)) {
    start_idx <- (chunk_idx - 1) * chunk_size + 1
    end_idx <- min(chunk_idx * chunk_size, n_cells)
    chunk_rows <- cell_data[start_idx:end_idx, , drop = FALSE]

    cat(sprintf(
      "\r  Chunk %d/%d (%s cells)     ",
      chunk_idx,
      n_chunks,
      format(nrow(chunk_rows), big.mark = ",")
    ))

    # Build chunk data frame with region
    chunk_df <- data.frame(
      cell_id = as.numeric(chunk_rows$cell_id),
      region = as.integer(chunk_rows$region),
      stringsAsFactors = FALSE
    )
    browser()
    # Add static variables
    for (var_name in names(static_rasters)) {
      r <- terra::rast(static_rasters[[var_name]])
      vals <- r[chunk_rows$cell_id]

      if (is.data.frame(vals) || is.list(vals)) {
        vals <- vals[[1]]
      }

      chunk_df[[var_name]] <- vals
    }

    # Split chunk by region and write to appropriate partition
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
        region_dir <- file.path(output_dir, sprintf("region=%d", region))
        ensure_dir(region_dir)

        # Create output file path
        region_file <- file.path(region_dir, "static_predictors.parquet")

        # Create writer
        sinks[[as.character(region)]] <- arrow::FileOutputStream$create(
          region_file
        )
        writers[[as.character(region)]] <- ParquetFileWriter$create(
          schema = schema,
          sink = sinks[[as.character(region)]],
          properties = ParquetWriterProperties$create(
            names(schema),
            compression = "zstd",
            compression_level = 9
          )
        )
      }

      # Write region chunk
      writers[[as.character(region)]]$WriteTable(
        arrow::Table$create(region_chunk),
        chunk_size = nrow(region_chunk)
      )

      rm(region_chunk)
    }

    rm(chunk_df)
    gc(verbose = FALSE)
  }

  cat("\n  Closing writers...\n")

  # Close all writers
  for (region in names(writers)) {
    writers[[region]]$Close()
    sinks[[region]]$close()
  }

  cat("✓ Static partitioned dataset created successfully\n")
  cat(sprintf("  Location: %s\n", output_dir))
  cat(sprintf("  Partitions: %d regions\n", length(regions)))
}


#' Write dynamic predictors with streaming and partitioning by scenario and region
#' @param cell_data Data frame with cell_id and region columns
#' @param period_data Named list of scenarios, each containing named list of variable raster paths
#' @param output_dir Output directory for partitioned dataset
#' @param chunk_size Number of cells to process per write operation
write_dynamic_parquet_streaming_partitioned <- function(
  cell_data,
  period_data,
  output_dir,
  chunk_size
) {
  library(arrow)

  n_cells <- nrow(cell_data)
  n_chunks <- ceiling(n_cells / chunk_size)
  scenarios <- names(period_data)
  regions <- sort(unique(cell_data$region))

  cat(sprintf(
    "  %d scenarios: %s\n",
    length(scenarios),
    paste(scenarios, collapse = ", ")
  ))
  cat(sprintf(
    "  %d regions: %s\n",
    length(regions),
    paste(regions, collapse = ", ")
  ))

  # Initialize writer storage (scenario -> region -> writer)
  writers <- list()
  sinks <- list()
  schema <- NULL

  for (scenario in scenarios) {
    cat(sprintf("  Scenario: %s\n", scenario))

    var_names <- names(period_data[[scenario]])
    cat(sprintf(
      "    %d variables: %s\n",
      length(var_names),
      paste(var_names, collapse = ", ")
    ))

    for (chunk_idx in seq_len(n_chunks)) {
      start_idx <- (chunk_idx - 1) * chunk_size + 1
      end_idx <- min(chunk_idx * chunk_size, n_cells)
      chunk_rows <- cell_data[start_idx:end_idx, , drop = FALSE]

      cat(sprintf("\r    Chunk %d/%d     ", chunk_idx, n_chunks))

      # Build chunk for this scenario
      chunk_df <- data.frame(
        cell_id = as.numeric(chunk_rows$cell_id),
        scenario = scenario,
        region = as.integer(chunk_rows$region),
        stringsAsFactors = FALSE
      )

      for (var_name in var_names) {
        r <- terra::rast(period_data[[scenario]][[var_name]])
        vals <- r[chunk_rows$cell_id]
        if (is.data.frame(vals) || is.list(vals)) {
          vals <- vals[[1]]
        }
        chunk_df[[var_name]] <- vals
      }

      # Split chunk by region and write to appropriate partition
      for (region in unique(chunk_df$region)) {
        region_chunk <- chunk_df[chunk_df$region == region, , drop = FALSE]

        # Create unique key for this scenario-region combination
        writer_key <- sprintf("%s_%d", scenario, region)

        # Create writer for this scenario-region combination on first encounter
        if (is.null(writers[[writer_key]])) {
          # Create schema from first chunk
          if (is.null(schema)) {
            tbl <- arrow::Table$create(region_chunk)
            schema <- tbl$schema
          }

          # Create nested partition directory (scenario=X/region=Y)
          scenario_dir <- file.path(
            output_dir,
            sprintf("scenario=%s", scenario)
          )
          region_dir <- file.path(scenario_dir, sprintf("region=%d", region))
          ensure_dir(region_dir)

          # Create output file path
          partition_file <- file.path(region_dir, "data.parquet")

          # Create writer
          sinks[[writer_key]] <- arrow::FileOutputStream$create(partition_file)
          writers[[writer_key]] <- ParquetFileWriter$create(
            schema = schema,
            sink = sinks[[writer_key]],
            properties = ParquetWriterProperties$create(
              names(schema),
              compression = "zstd",
              compression_level = 9
            )
          )
        }

        # Write region chunk to appropriate partition
        writers[[writer_key]]$WriteTable(
          arrow::Table$create(region_chunk),
          chunk_size = nrow(region_chunk)
        )

        rm(region_chunk)
      }

      rm(chunk_df)
      gc(verbose = FALSE)
    }
    cat("\n")
  }

  cat("  Closing writers...\n")

  # Close all writers
  for (writer_key in names(writers)) {
    writers[[writer_key]]$Close()
    sinks[[writer_key]]$close()
  }

  cat(sprintf(
    "  ✓ Dynamic partitioned dataset created\n"
  ))
  cat(sprintf("  Location: %s\n", output_dir))
  cat(sprintf(
    "  Partitions: %d scenarios × %d regions = %d total\n",
    length(scenarios),
    length(regions),
    length(scenarios) * length(regions)
  ))
}


#' Reorganize dynamic config from flat variable list to period->scenario->variable
#' @param pred_config Predictor configuration list
#' @return Nested list organized by period and scenario
reorganize_by_period <- function(pred_config) {
  period_structure <- list()

  for (var_key in names(pred_config)) {
    var_config <- pred_config[[var_key]]

    if (var_config$static_or_dynamic == "static") {
      next
    }

    period <- as.character(var_config$period)
    scenario <- if (
      is.null(var_config$scenario_variant) ||
        is.na(var_config$scenario_variant)
    ) {
      "baseline"
    } else {
      as.character(var_config$scenario_variant)
    }

    raster_path <- file.path(config[["data_basepath"]], var_config$path)

    var_name <- if (
      !is.null(var_config$base_name) && !is.na(var_config$base_name)
    ) {
      var_config$base_name
    } else {
      var_key
    }

    if (is.null(period_structure[[period]])) {
      period_structure[[period]] <- list()
    }
    if (is.null(period_structure[[period]][[scenario]])) {
      period_structure[[period]][[scenario]] <- list()
    }

    period_structure[[period]][[scenario]][[var_name]] <- raster_path
  }

  return(period_structure)
}


#' Extract static variables from flat config
#' @param pred_config Predictor configuration list
#' @return Named list of static variable paths
extract_static_variables <- function(pred_config) {
  static_rasters <- list()

  for (var_key in names(pred_config)) {
    var_config <- pred_config[[var_key]]

    if (var_config$static_or_dynamic == "static") {
      var_name <- if (
        !is.null(var_config$base_name) && !is.na(var_config$base_name)
      ) {
        var_config$base_name
      } else {
        var_key
      }

      if (!is.null(static_rasters[[var_name]])) {
        stop(sprintf(
          "Duplicate base_name '%s' found for static variables. Check YAML entries: %s",
          var_name,
          var_key
        ))
      }

      if (
        !is.null(var_config$grouping) &&
          var_config$grouping == "soil"
      ) {
        if (!grepl("100-200cm", var_name)) {
          next
        }
      }

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
  static_rasters <- extract_static_variables(pred_config)

  if (length(static_rasters) > 0) {
    static_meta <- data.frame(
      file_name = "static (partitioned by region)",
      type = "static",
      period = NA_character_,
      n_variables = length(static_rasters),
      variables = paste(names(static_rasters), collapse = ","),
      partitioning = "region",
      stringsAsFactors = FALSE
    )
  } else {
    static_meta <- data.frame()
  }

  if (length(time_periods) > 0) {
    dynamic_meta <- data.frame(
      file_name = file.path("dynamic", time_periods, "(partitioned)"),
      type = "dynamic",
      period = time_periods,
      n_variables = NA_integer_,
      variables = NA_character_,
      partitioning = "scenario, region",
      stringsAsFactors = FALSE
    )

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
  files <- files[!dir.exists(files)] # Exclude directories

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


#' Compute NA counts for each column in an Arrow dataset (streaming)
#' @param ds Arrow dataset
#' @param dataset_name Name of dataset (for messages)
#' @param sample_cols Optional vector of column names to restrict analysis
#' @return Data frame with NA counts and proportions per column
compute_na_counts_streaming <- function(
  ds,
  dataset_name = "dataset",
  sample_cols = NULL
) {
  message(sprintf("\n[INFO] Computing NA counts for %s...", dataset_name))

  # List all columns in dataset
  all_cols <- ds$schema$names

  # Optionally restrict to a subset (e.g. from collinearity results)
  if (!is.null(sample_cols)) {
    all_cols <- intersect(all_cols, sample_cols)
  }

  results <- map_dfr(all_cols, function(colname) {
    message(sprintf("  -> Checking column: %s", colname))

    # Summarise missing values for this column
    na_summary <- ds %>%
      summarise(
        na_count = sum(is.na(!!sym(colname))),
        total_rows = n()
      ) %>%
      collect()

    tibble(
      column = colname,
      na_count = na_summary$na_count,
      total_rows = na_summary$total_rows,
      na_proportion = na_summary$na_count / na_summary$total_rows
    )
  })

  message("[INFO] NA counting complete.")
  results %>% arrange(desc(na_proportion))
}
