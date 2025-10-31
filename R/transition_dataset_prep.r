#' transition_dataset_creation
#'
#' Script for gathering the layers of LULC (dependent variable)  for each
#' historic period then separating into viable LULC transitions at the scale of
#' Peru and regions
#'
#' @param config list of configuration parameters
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
    pattern = "regions.tif$",
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

  # To speed up the extract identify the IDs of only valid cells (i.e. cells where ref_grid is not NA)
  # Check if valid_cell_ids.rds exists (prepared in predictors prep step)
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

  periods <- config[["data_periods"]]
  periods <- periods[3]

  # Apply function to prepare parquet files for each period
  message("Starting data extraction for each transition period")
  purrr::walk(
    periods,
    process_period_transitions,
    config = config,
    rasterstacks_by_periods = rasterstacks_by_periods,
    valid_cell_ids = valid_cell_ids
  )
  message("Preparation of transition datasets complete")
}

#' Efficiently process transitions for one period and write a single compact parquet
#' (one row per cell, one column per transition)
#'
#' @param period Character period label (e.g. "2020_2030")
#' @param rasterstacks_by_periods Named list of terra rasters for each period
#' @param config Named list of configuration parameters
#' @param valid_cell_ids Integer vector of valid cell IDs (pre-filtered mask)
#' @param chunk_size Number of cells to process per chunk (default 1e6)
#'
#' @return NULL (writes parquet file)
process_period_transitions <- function(
  period,
  rasterstacks_by_periods,
  config,
  valid_cell_ids,
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
  region_r <- if (regionalization) r_stack$region else NULL

  viable_trans_list <- readRDS(config[["viable_transitions_lists"]])[[paste(
    period
  )]]
  trans_names <- viable_trans_list$Trans_name

  # ---- Prepare output ----
  out_dir <- file.path(config[["trans_pre_pred_filter_dir"]], period)
  if (!dir.exists(out_dir)) {
    dir.create(out_dir, recursive = TRUE)
  }
  output_path <- file.path(
    out_dir,
    paste0("transitions_", period, ".parquet")
  )

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
  message("→ Writing to: ", output_path)

  first_chunk <- TRUE
  writer <- NULL
  sink <- NULL

  # ---- Chunk processing ----
  for (chunk_idx in seq_len(n_chunks)) {
    start_idx <- (chunk_idx - 1) * chunk_size + 1
    end_idx <- min(chunk_idx * chunk_size, n_valid)
    chunk_ids <- valid_cell_ids[start_idx:end_idx]

    cat(sprintf("\r  Processing chunk %d/%d...", chunk_idx, n_chunks))

    # Extract raster values
    init_vals <- init_r[chunk_ids]
    fin_vals <- fin_r[chunk_ids]

    if (regionalization) {
      region_vals <- region_r[chunk_ids]
      # Flatten if terra returns a data.frame or list
      if (is.data.frame(region_vals) || is.list(region_vals)) {
        region_vals <- unlist(region_vals, use.names = FALSE)
      }
      region_vals <- as.integer(region_vals)
    } else {
      region_vals <- rep(NA_integer_, length(chunk_ids))
    }

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
      region = if (regionalization) as.integer(region_vals) else NA_integer_
    )
    chunk_df <- cbind(chunk_df, as.data.frame(trans_mat))

    # ---- Write parquet (streaming) ----
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
      writer$WriteTable(tbl, chunk_size = nrow(tbl))
      first_chunk <- FALSE
    } else {
      writer$WriteTable(
        arrow::Table$create(chunk_df),
        chunk_size = nrow(chunk_df)
      )
    }

    rm(chunk_df, trans_mat, init_vals, fin_vals, region_vals)
    gc(verbose = FALSE)

    cat(sprintf(
      "\n  ✓ Chunk %d/%d written (%s rows)",
      chunk_idx,
      n_chunks,
      format(length(chunk_ids), big.mark = ",")
    ))
  }

  # ---- Close writer ----
  if (!is.null(writer)) {
    writer$Close()
    sink$close()
    message("\n✓ Parquet writing complete.")
  }

  message("\n✓ Transition parquet written: ", basename(output_path))
  message("=== Completed all transitions for period ", period, " ===\n")

  message("performing sanity check on written file...")
  parquet_check <- sanity_check_transitions(
    transitions_pq_path = output_path,
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
}

#' Sanity Check Transitions Parquet File (Memory Efficient)
#'
#' Summarizes the number of 1's (transitions) for each transition column
#' by region WITHOUT loading the entire dataset into memory
#'
#' @param transitions_pq_path Path to the transitions parquet file
#' @param regions_json_path Path to regions.json file (optional, for region labels)
#' @return Data frame with counts of transitions by transition_name and region
#' @export

library(arrow)
library(dplyr)
library(tidyr)
library(jsonlite)
library(purrr)

sanity_check_transitions <- function(
  transitions_pq_path,
  regions_json_path = NULL
) {
  message("========================================")
  message("Transitions Parquet Sanity Check")
  message("========================================\n")

  # Check file exists
  if (!file.exists(transitions_pq_path)) {
    stop(sprintf("File not found: %s", transitions_pq_path))
  }

  message(sprintf("Reading: %s\n", transitions_pq_path))

  # Open dataset
  ds <- arrow::open_dataset(transitions_pq_path)

  # Get all column names (excluding cell_id and region)
  all_cols <- names(ds$schema)
  transition_cols <- setdiff(all_cols, c("cell_id", "region"))

  message(sprintf("Found %d transition columns:", length(transition_cols)))
  message(paste(transition_cols, collapse = ", "))
  message("")

  # Check if region column exists
  has_regions <- "region" %in% all_cols

  if (!has_regions) {
    message("No 'region' column found - summarizing nationally\n")

    # Process each transition column without loading full data
    summary_list <- purrr::map(transition_cols, function(col) {
      message(sprintf("Processing: %s", col))

      # Count each value using Arrow aggregation
      counts <- ds %>%
        select(!!sym(col)) %>%
        group_by(!!sym(col)) %>%
        summarise(count = n()) %>%
        collect()

      # Extract counts
      n_ones <- counts %>% filter(!!sym(col) == 1) %>% pull(count)
      n_zeros <- counts %>% filter(!!sym(col) == 0) %>% pull(count)
      n_na <- counts %>% filter(is.na(!!sym(col))) %>% pull(count)

      # Handle cases where values don't exist
      if (length(n_ones) == 0) {
        n_ones <- 0
      }
      if (length(n_zeros) == 0) {
        n_zeros <- 0
      }
      if (length(n_na) == 0) {
        n_na <- 0
      }

      tibble::tibble(
        transition_name = col,
        region = NA_integer_,
        region_label = "National",
        n_ones = n_ones,
        n_zeros = n_zeros,
        n_na = n_na,
        total_rows = sum(counts$count)
      )
    })

    summary_df <- bind_rows(summary_list)
  } else {
    message("'region' column found - summarizing by region\n")

    # Load region labels if provided
    region_labels <- NULL
    if (!is.null(regions_json_path) && file.exists(regions_json_path)) {
      regions_info <- jsonlite::fromJSON(regions_json_path)
      region_labels <- setNames(regions_info$label, regions_info$value)
      message(sprintf(
        "Loaded region labels for %d regions\n",
        length(region_labels)
      ))
    }

    # Get unique regions (small query)
    unique_regions <- ds %>%
      select(region) %>%
      distinct() %>%
      collect() %>%
      pull(region) %>%
      sort()

    has_zero_region <- ds %>%
      select(region) %>%
      filter(region == 0) %>%
      head(1) %>%
      collect() %>%
      nrow() >
      0

    print(has_zero_region)

    message(sprintf(
      "Found %d unique regions: %s\n",
      length(unique_regions),
      paste(unique_regions, collapse = ", ")
    ))

    # Process each region x transition combination
    summary_list <- list()
    counter <- 0
    total_combos <- length(unique_regions) * length(transition_cols)

    for (reg in unique_regions) {
      region_label <- if (!is.null(region_labels)) {
        region_labels[as.character(reg)]
      } else {
        paste0("Region_", reg)
      }

      message(sprintf("\nProcessing region %d (%s)...", reg, region_label))

      # Process each transition for this region
      region_summaries <- purrr::map(transition_cols, function(col) {
        counter <<- counter + 1
        if (counter %% 10 == 0) {
          message(sprintf(
            "  Progress: %d/%d (%.1f%%)",
            counter,
            total_combos,
            100 * counter / total_combos
          ))
        }

        # Count values for this transition in this region using Arrow
        counts <- ds %>%
          filter(region == reg) %>%
          select(!!sym(col)) %>%
          group_by(!!sym(col)) %>%
          summarise(count = n()) %>%
          collect()

        # Extract counts
        n_ones <- counts %>% filter(!!sym(col) == 1) %>% pull(count)
        n_zeros <- counts %>% filter(!!sym(col) == 0) %>% pull(count)
        n_na <- counts %>% filter(is.na(!!sym(col))) %>% pull(count)

        # Handle cases where values don't exist
        if (length(n_ones) == 0) {
          n_ones <- 0
        }
        if (length(n_zeros) == 0) {
          n_zeros <- 0
        }
        if (length(n_na) == 0) {
          n_na <- 0
        }

        tibble::tibble(
          transition_name = col,
          region = reg,
          region_label = region_label,
          n_ones = n_ones,
          n_zeros = n_zeros,
          n_na = n_na,
          total_rows = sum(counts$count)
        )
      })

      summary_list[[as.character(reg)]] <- bind_rows(region_summaries)
    }

    summary_df <- bind_rows(summary_list)
  }

  # Add percentage columns
  summary_df <- summary_df %>%
    mutate(
      percent_ones = round(100 * n_ones / (n_ones + n_zeros), 2),
      percent_na = round(100 * n_na / total_rows, 2)
    )

  # Print summary
  message("\n========================================")
  message("SUMMARY")
  message("========================================\n")

  if (has_regions) {
    # Summary by region
    region_totals <- summary_df %>%
      group_by(region, region_label) %>%
      summarise(
        n_transitions = n(),
        total_ones = sum(n_ones),
        mean_ones = round(mean(n_ones), 1),
        max_ones = max(n_ones),
        min_ones = min(n_ones),
        .groups = "drop"
      )

    message("--- Summary by Region ---")
    print(region_totals)

    message("\n--- Top 10 Transition-Region Combinations by Count ---")
    top_combos <- summary_df %>%
      arrange(desc(n_ones)) %>%
      head(10) %>%
      select(transition_name, region_label, n_ones, percent_ones)
    print(top_combos)

    # Summary by transition (across all regions)
    message("\n--- Summary by Transition (all regions combined) ---")
    transition_totals <- summary_df %>%
      group_by(transition_name) %>%
      summarise(
        total_ones = sum(n_ones),
        mean_ones_per_region = round(mean(n_ones), 1),
        max_ones_in_region = max(n_ones),
        n_regions_with_data = sum(n_ones > 0),
        .groups = "drop"
      ) %>%
      arrange(desc(total_ones))
    print(head(transition_totals, 10))
  } else {
    # Summary national
    message("--- Transitions Summary (National) ---")
    print(
      summary_df %>%
        select(transition_name, n_ones, n_zeros, n_na, percent_ones) %>%
        arrange(desc(n_ones))
    )
  }

  # Check for any transitions with zero 1's
  zero_transitions <- summary_df %>%
    filter(n_ones == 0)

  if (nrow(zero_transitions) > 0) {
    message(
      "\n⚠️  WARNING: Found ",
      nrow(zero_transitions),
      " transition-region combinations with ZERO 1's:"
    )
    print(
      zero_transitions %>%
        select(transition_name, region_label, n_ones, n_zeros, n_na) %>%
        head(20)
    )
    if (nrow(zero_transitions) > 20) {
      message(sprintf("  ... and %d more", nrow(zero_transitions) - 20))
    }
  }

  # Check for transitions with all NAs
  all_na_transitions <- summary_df %>%
    filter(n_na == total_rows)

  if (nrow(all_na_transitions) > 0) {
    message(
      "\n⚠️  WARNING: Found ",
      nrow(all_na_transitions),
      " transition-region combinations with ALL NAs:"
    )
    print(all_na_transitions %>% select(transition_name, region_label, n_na))
  }

  # Overall statistics
  message("\n--- Overall Statistics ---")
  message(sprintf("Total transition-region combinations: %d", nrow(summary_df)))
  message(sprintf(
    "Combinations with data (n_ones > 0): %d (%.1f%%)",
    sum(summary_df$n_ones > 0),
    100 * sum(summary_df$n_ones > 0) / nrow(summary_df)
  ))
  message(sprintf(
    "Total transitions (1's) across all: %s",
    format(sum(summary_df$n_ones), big.mark = ",")
  ))

  message("\n========================================")
  message("Sanity check complete!")
  message("========================================\n")

  return(summary_df)
}
