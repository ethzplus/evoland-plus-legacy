#' transition_dataset_creation
#'
#' Script for gathering the layers of LULC (dependent variable) and predictors for each
#' historic period then separating into viable LULC transitions at the scale of
#' Peru and regions
#'
#' @param config list of configuration parameters
#'
#' @author Ben Black
#' @export

transition_dataset_prep <- function(config = get_config()) {
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

  # Collect file path for regional raster
  region_path <- data.frame(matrix(ncol = 2, nrow = 1))
  colnames(region_path) <- c("File_name", "Layer_name")
  region_path["File_name"] <- list.files(
    config[["reg_dir"]],
    pattern = ".tif$",
    full.names = TRUE
  )
  region_path["Layer_name"] <- "region"

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

  # Combine predictor paths with LULC (and region if needed)
  if (config[["regionalization"]]) {
    combined_paths_by_period <- lapply(lulc_paths_by_period, function(x) {
      rbind(x, region_path)
    })
  } else {
    combined_paths_by_period <- lulc_paths_by_period
  }

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
  ### C.1- Data extraction
  ### =========================================================================

  message("Starting data extraction for each transition period")
  # futures crash with an error on a raster::cellFromXY() call
  # i could not trace the error, so sequential computation it is for now.
  # furrr::future_walk(
  purrr::walk(
    config[["data_periods"]],
    process_period_transitions,
    config = config,
    rasterstacks_by_periods = rasterstacks_by_periods
  )
  message("Preparation of transition datasets complete")

  # testing values
  period <- config[["data_periods"]][3]

  # Check if valid_cell_ids.rds exists
  valid_ids_path <- file.path(
    config[["predictors_prepped_dir"]],
    "parquet_data",
    "valid_cell_ids.rds"
  )
  if (file.exists(valid_ids_path)) {
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

  process_period_transitions <- function(
    period,
    rasterstacks_by_periods,
    config,
    valid_cell_ids,
    chunk_size = 5e6 # tune this based on your RAM and I/O speed
  ) {
    library(terra)
    library(arrow)

    message("\n=== Processing transitions for period: ", period, " ===")

    # Load rasters
    r_stack <- rasterstacks_by_periods[[paste(period)]]
    init_r <- r_stack$Initial_class
    fin_r <- r_stack$Final_class

    regionalization <- isTRUE(config[["regionalization"]])
    region_r <- if (regionalization) r_stack$region else NULL

    viable_trans_list <- readRDS(config[["viable_transitions_lists"]])[[paste(
      period
    )]]

    out_dir <- file.path(config[["trans_pre_pred_filter_dir"]], period)
    ensure_dir(out_dir)

    n_valid <- length(valid_cell_ids)
    n_chunks <- ceiling(n_valid / chunk_size)
    message(
      "Valid cells: ",
      format(n_valid, big.mark = ","),
      " (",
      n_chunks,
      " chunks of ",
      format(chunk_size, big.mark = ","),
      ")"
    )

    for (i in seq_len(nrow(viable_trans_list))) {
      from_class <- viable_trans_list[i, "From."]
      to_class <- viable_trans_list[i, "To."]
      trans_name <- viable_trans_list[i, "Trans_name"]

      message("\n→ Transition: ", from_class, " → ", to_class)

      output_path <- file.path(
        out_dir,
        paste0(trans_name, "_transition.parquet")
      )
      cat("  Writing to: ", output_path, "\n")

      first_chunk <- TRUE
      writer <- NULL
      sink <- NULL

      for (chunk_idx in seq_len(n_chunks)) {
        cat(sprintf("\r  Processing chunk %d/%d...", chunk_idx, n_chunks))
        start_idx <- (chunk_idx - 1) * chunk_size + 1
        end_idx <- min(chunk_idx * chunk_size, n_valid)
        chunk_ids <- valid_cell_ids[start_idx:end_idx]

        # Read raster values directly by cell ID
        init_vals <- init_r[chunk_ids]
        fin_vals <- fin_r[chunk_ids]
        if (regionalization) {
          region_vals <- region_r[chunk_ids]
        }

        # Identify transition type
        trans_vals <- ifelse(
          init_vals == from_class & fin_vals == to_class,
          1L,
          ifelse(
            init_vals == from_class & fin_vals != to_class,
            0L,
            NA_integer_
          )
        )
        keep <- !is.na(trans_vals)
        if (!any(keep)) {
          cat(sprintf(
            "\r  Chunk %d/%d has no relevant transitions, skipping...",
            chunk_idx,
            n_chunks
          ))
          next
        }

        # Build minimal chunk table (no x/y)
        chunk_df <- data.frame(
          cell_id = as.integer(chunk_ids[keep]),
          transition = trans_vals[keep],
          stringsAsFactors = FALSE
        )
        if (regionalization) {
          chunk_df$region <- as.character(region_vals[keep])
        }

        # Parquet writing identical to your known-good function
        if (first_chunk) {
          tbl <- arrow::Table$create(chunk_df)
          schema <- tbl$schema

          sink <- arrow::FileOutputStream$create(output_path)
          writer <- arrow::ParquetFileWriter$create(
            schema = schema,
            sink = sink,
            properties = arrow::ParquetWriterProperties$create(
              names(schema),
              compression = "zstd",
              compression_level = 9
            )
          )
          print("writer created")
          print(writer)
          writer$WriteTable(tbl, chunk_size = nrow(chunk_df))
          first_chunk <- FALSE
        } else {
          writer$WriteTable(
            arrow::Table$create(chunk_df),
            chunk_size = nrow(chunk_df)
          )
        }

        rm(chunk_df, init_vals, fin_vals, trans_vals)
        gc(verbose = FALSE)

        cat(sprintf(
          "\r  Chunk %d/%d written (%s rows)",
          chunk_idx,
          n_chunks,
          format(sum(keep), big.mark = ",")
        ))
      }

      if (!is.null(writer)) {
        writer$Close()
        sink$close()
      }

      message("\n  ✓ Transition parquet written: ", basename(output_path))
    }

    message("\n=== All transitions for period ", period, " complete. ===\n")
  }

  # Loop over transition datasets splitting each into:
  # the transition result column
  # non-transition columns,
  # weight vector,
  # measure of class imbalance
  # number of units in the dataset
  trans_datasets_full <- lapply(binarized_trans_datasets, function(x) {
    lulcc.splitforcovselection(
      trans_dataset = x,
      covariate_ids = predictor_table$pred_name
    )
  })
  rm(binarized_trans_datasets)

  if (config[["regionalization"]]) {
    trans_datasets_regionalized <- lapply(
      binarized_trans_datasets_regionalized,
      function(x) {
        lulcc.splitforcovselection(
          trans_dataset = x,
          covariate_ids = predictor_table$pred_name
        )
      }
    )
    rm(binarized_trans_datasets_regionalized)

    #  Remove regional datasets without sufficient transitions
    trans_datasets_regionalized <- trans_datasets_regionalized[
      sapply(trans_datasets_regionalized, function(x) {
        sum(x[["trans_result"]] == 1)
      }) >
        5
    ]
  }

  # Save datasets
  sapply(names(trans_datasets_full), function(dataset_name) {
    full_save_path <- file.path(
      config[["trans_pre_pred_filter_dir"]],
      period,
      paste0(dataset_name, "_full_ch.rds")
    )
    saveRDS(trans_datasets_full[[paste(dataset_name)]], full_save_path)
  })

  if (config[["regionalization"]]) {
    sapply(names(trans_datasets_regionalized), function(dataset_name) {
      full_save_path <- file.path(
        config[["trans_pre_pred_filter_dir"]],
        period,
        paste0(dataset_name, "_regionalized.rds")
      )
      saveRDS(
        trans_datasets_regionalized[[paste(dataset_name)]],
        full_save_path
      )
    })
  }

  rm(trans_datasets_full, trans_datasets_regionalized)
  message("Transition Datasets for: ", period, " complete\n")

  gc()
}
