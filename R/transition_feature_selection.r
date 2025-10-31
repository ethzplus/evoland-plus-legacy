#' Modularized Feature Selection for LULCC Transitions
#'
#' @author: Ben Black (adapted)

# testing parameters
period <- periods_to_process[3]
transition_name <- transitions_info$Trans_name[1]
region <- region_names[1]

#' Main orchestrator function for transition feature selection across all periods
#'
#' @param config Configuration list
#' @return Data frame with feature selection summary for all periods
#' @export
transition_feature_selection <- function(
  config = get_config(),
  sample_size = 3e5, # Optional: number of rows to sample for testing
  do_collinearity = TRUE, # Whether to perform collinearity filtering
  do_grrf = TRUE # Whether to perform GRRF feature selection
) {
  message("\n========================================")
  message("Starting Feature Selection Pipeline")
  message("========================================\n")

  # Plan parallel execution (multisession is cross-platform)
  future::plan(future::multisession)

  periods_to_process <- config[["data_periods"]]
  use_regions <- isTRUE(config[["regionalization"]])

  message(sprintf(
    "Processing %d periods",
    length(periods_to_process)
  ))
  message(sprintf(
    "Regionalization: %s\n",
    ifelse(use_regions, "ENABLED", "DISABLED")
  ))

  # create a debug directory for intermediate results
  debug_dir <- file.path(config[["feature_selection_dir"]], "debug_fs")
  ensure_dir(debug_dir)

  # Process each period (sequential to avoid memory issues with large datasets)
  results_list <- purrr::map(
    periods_to_process$data_period_name,
    function(period) {
      perform_feature_selection(
        period = period,
        use_regions = use_regions,
        config = config,
        debug_dir = debug_dir,
        save_debug = TRUE,
        sample_size = sample_size,
        do_collinearity = do_collinearity,
        do_grrf = do_grrf
      )
    }
  )

  # Combine all period results
  final_summary <- dplyr::bind_rows(results_list)

  # Save results
  output_path <- file.path(
    config[["feature_selection_dir"]],
    "feature_selection_summary.csv"
  )
  dir.create(
    config[["feature_selection_dir"]],
    recursive = TRUE,
    showWarnings = FALSE
  )
  readr::write_csv(final_summary, output_path)

  message("\n========================================")
  message("Feature Selection Complete")
  message("========================================")
  message(sprintf("Total transitions processed: %d", nrow(final_summary)))
  message(sprintf("Successful: %d", sum(final_summary$status == "success")))
  message(sprintf("Failed: %d", sum(final_summary$status != "success")))
  message(sprintf("Results saved to: %s\n", output_path))

  # Reset parallel plan
  future::plan(sequential)

  return(final_summary)
}


#' Perform feature selection for a single period
#'
#' @param period Period name (e.g., "2018_2022")
#' @param use_regions Boolean indicating if regionalization is active
#' @param config Configuration list
#' @param debug_dir Directory for debug outputs
#' @param save_debug Boolean indicating if debug outputs should be saved
#' @param sample_size Optional number of rows to sample for testing
#' @param do_collinearity Whether to perform collinearity filtering
#' @param do_grrf Whether to perform GRRF feature selection
#' @return Data frame with feature selection results for all transitions in period
perform_feature_selection <- function(
  period,
  use_regions,
  config,
  debug_dir,
  save_debug = TRUE,
  sample_size = 3e5, # Optional: number of rows to sample for testing
  do_collinearity = TRUE, # Whether to perform collinearity filtering
  do_grrf = TRUE # Whether to perform GRRF feature selection
) {
  message("\n########################################")
  message(sprintf("# PERIOD: %s", period))
  message("########################################\n")

  # --- Load metadata ---
  transitions_info <- readRDS(config[["viable_transitions_lists"]])[[period]]
  lulc_schema <- jsonlite::fromJSON(
    config[["LULC_aggregation_path"]],
    simplifyVector = FALSE
  )

  # get the values of class_name where nhood_class == FALSE
  nhood_lulcs <- lapply(lulc_schema, function(x) {
    if (isTRUE(x$nhood_class)) {
      return(x$class_name)
    } else {
      return(NA)
    }
  })
  nhood_lulcs <- nhood_lulcs[!is.na(nhood_lulcs)]

  # Load predictor table
  pred_table_raw <- yaml::yaml.load_file(config[["pred_table_path"]])

  # remove all entries without a path
  pred_table_raw <- pred_table_raw[sapply(pred_table_raw, function(x) {
    !is.null(x$path)
  })]

  # get period specific predictors
  period_preds <- pred_table_raw[sapply(pred_table_raw, function(x) {
    x$period == period
  })]

  # get static predictors
  static_preds <- pred_table_raw[sapply(pred_table_raw, function(x) {
    x$period == "all"
  })]

  # Combine predictor lists
  pred_table <- c(static_preds, period_preds)

  # Extract names by group type
  get_preds <- function(tbl, grouping) {
    purrr::map_chr(
      tbl,
      ~ if (.x$grouping == grouping) .x$base_name else NA_character_
    ) |>
      purrr::discard(is.na)
  }

  soil_preds <- get_preds(pred_table, "soil")
  nhood_preds <- get_preds(pred_table, "neighbourhood")

  # Soil groups: group by prefix before "_"
  soil_groups <- split(soil_preds, stringr::str_extract(soil_preds, "^[^_]+"))

  # Neighbourhood groups: group by presence of lulc name
  nhood_groups <- purrr::map(nhood_lulcs, function(lulc) {
    purrr::keep(nhood_preds, ~ stringr::str_detect(.x, lulc))
  })

  # Remove empty groups
  nhood_groups <- purrr::discard(nhood_groups, ~ length(.x) == 0)
  names(nhood_groups) <- nhood_lulcs[nhood_lulcs %in% names(nhood_groups)]

  # Suitability predictors (not soil or neighbourhood)
  suitability_preds <- purrr::map_chr(
    pred_table,
    ~ {
      if (
        .x$pred_category == "suitability" &&
          !.x$grouping %in% c("soil", "neighbourhood")
      ) {
        .x$base_name
      } else {
        NA_character_
      }
    }
  ) |>
    purrr::discard(is.na)

  # Build final vector
  pred_categories <- c(
    setNames(rep("suitability", length(suitability_preds)), suitability_preds),
    unlist(imap(soil_groups, ~ setNames(rep(.y, length(.x)), .x))),
    unlist(imap(nhood_groups, ~ setNames(rep(.y, length(.x)), .x)))
  )

  message(sprintf("Loaded %d viable transitions", nrow(transitions_info)))
  message(sprintf(
    "Loaded %d predictors for this period\n",
    length(pred_categories)
  ))

  # --- Set up file paths ---
  transitions_pq_path <- file.path(
    config[["trans_pre_pred_filter_dir"]],
    period,
    sprintf("transitions_%s.parquet", period)
  )
  static_preds_pq_path <- file.path(
    config[["predictors_prepped_dir"]],
    "parquet_data",
    "static",
    "predictors_static.parquet"
  )
  dynamic_preds_pq_path <- file.path(
    config[["predictors_prepped_dir"]],
    "parquet_data",
    "dynamic",
    sprintf("period_%s.parquet", period)
  )

  # Verify files exist
  stopifnot(
    file.exists(transitions_pq_path),
    file.exists(static_preds_pq_path),
    file.exists(dynamic_preds_pq_path)
  )

  # Open arrow datasets (lazy)
  message("Opening Arrow datasets...")
  ds_transitions <- arrow::open_dataset(transitions_pq_path)
  ds_static <- arrow::open_dataset(static_preds_pq_path)
  ds_dynamic <- arrow::open_dataset(dynamic_preds_pq_path)
  message("  Arrow datasets opened successfully\n")

  # --- Set up regions ---
  if (use_regions) {
    regions <- jsonlite::fromJSON(file.path(
      config[["reg_dir"]],
      "regions.json"
    ))
    region_names <- regions$label
    message(sprintf(
      "Processing %d regions: %s\n",
      length(region_names),
      paste(region_names, collapse = ", ")
    ))
  } else {
    region_names <- "National extent"
    message("Processing national extent (no regionalization)\n")
  }

  # --- Process all transitions in parallel ---
  message(sprintf(
    "Starting parallel processing of %d transitions...\n",
    nrow(transitions_info)
  ))

  transition_results <- purrr::map_dfr(
    transitions_info$Trans_name,
    function(trans_name) {
      purrr::map_dfr(region_names, function(region) {
        process_single_transition(
          transition_name = trans_name,
          region = region,
          use_regions = use_regions,
          ds_transitions = ds_transitions,
          ds_static = ds_static,
          ds_dynamic = ds_dynamic,
          pred_categories = pred_categories,
          period = period,
          config = config,
          debug_dir = debug_dir,
          save_debug = save_debug,
          sample_size = sample_size,
          do_collinearity = do_collinearity,
          do_grrf = do_grrf
        )
      })
    }
  )

  message(sprintf(
    "\nPeriod %s complete: %d transition-region combinations processed",
    period,
    nrow(transition_results)
  ))
  return(transition_results)
}

#' Process feature selection for a single transition-region (optimized)
#' @param transition_name Name of the transition
#' @param region Region name (if applicable)
#' @param use_regions Boolean indicating if regionalization is active
#' @param ds_transitions Arrow dataset for transitions
#' @param ds_static Arrow dataset for static predictors
#' @param ds_dynamic Arrow dataset for dynamic predictors
#' @param pred_categories Named vector of predictor categories
#' @param period Period name
#' @param config Configuration list
#' @param debug_dir Directory for debug outputs
#' @param save_debug Boolean indicating if debug outputs should be saved
#' @return Data frame with feature selection summary for the transition-region
process_single_transition <- function(
  transition_name,
  region,
  use_regions,
  ds_transitions,
  ds_static,
  ds_dynamic,
  pred_categories,
  period,
  config,
  debug_dir,
  save_debug = TRUE,
  sample_size = 3e5, # <-- new: default sample size = 100k
  do_collinearity = TRUE, # <-- new: toggle collinearity filtering
  do_grrf = TRUE # <-- new: toggle GRRF filtering
) {
  message(sprintf(
    "\n========================================\nStarting: %s | Region: %s\n========================================",
    transition_name,
    if (use_regions) region else "National"
  ))

  # Resolve region value if using regionalization
  region_value <- NULL
  if (use_regions) {
    regions <- jsonlite::fromJSON(file.path(
      config[["reg_dir"]],
      "regions.json"
    ))
    region_value <- as.integer(regions$value[match(region, regions$label)])
    stopifnot(length(region_value) == 1, !is.na(region_value))
  }

  # --- STEP 1: Load Transition Data ---
  trans_df <- load_transition_data(
    ds_transitions,
    transition_name,
    region_value,
    use_regions
  )

  if (nrow(trans_df) == 0) {
    return(create_summary_row(
      period,
      region,
      transition_name,
      0,
      0,
      length(pred_categories),
      status = "no_transition_data",
      debug_dir = debug_dir,
      save_debug = save_debug
    ))
  }

  # --- STEP 2: Stratified Sampling (before predictor loading) ---
  if (nrow(trans_df) > sample_size) {
    message(sprintf(
      "Performing stratified sampling to %d rows (preserving class balance)",
      sample_size
    ))

    prop_table <- prop.table(table(trans_df$response))
    n_1 <- round(
      sample_size * ifelse("1" %in% names(prop_table), prop_table["1"], 0)
    )
    n_0 <- sample_size - n_1

    set.seed(123)
    idx_1 <- which(trans_df$response == 1)
    idx_0 <- which(trans_df$response == 0)

    samp_1 <- if (length(idx_1) > n_1) sample(idx_1, n_1) else idx_1
    samp_0 <- if (length(idx_0) > n_0) sample(idx_0, n_0) else idx_0

    trans_df <- trans_df[c(samp_1, samp_0), , drop = FALSE]

    message(sprintf(
      "Sampled %d rows: %d transitions (1s), %d non-transitions (0s)",
      nrow(trans_df),
      sum(trans_df$response == 1),
      sum(trans_df$response == 0)
    ))
  } else {
    message("Dataset smaller than sample size — using full data")
  }

  # --- STEP 3: Load Predictor Data ---
  preds_df <- load_predictor_data(
    ds_static,
    ds_dynamic,
    unique(trans_df$cell_id),
    names(pred_categories)
  )

  # --- STEP 4: Validate and Join Data ---
  validated <- validate_and_join_data(
    trans_df,
    preds_df,
    min_observations = 100,
    min_transitions = 10
  )

  if (validated$status != "valid") {
    return(create_summary_row(
      period,
      region,
      transition_name,
      validated$n_observations,
      validated$n_transitions,
      length(names(pred_categories)),
      status = validated$status,
      debug_dir = debug_dir,
      save_debug = save_debug
    ))
  }

  joined <- validated$data

  # remove transition dataframes to free up memory
  rm(trans_df)
  rm(preds_df)
  gc()

  # Extract response and predictors (no unnecessary copies)
  response_vec <- joined$response
  predictor_cols <- setdiff(names(joined), c("cell_id", "response", "region"))

  message(sprintf(
    "Data validation successful: %d observations, %d transitions, %d predictors",
    validated$n_observations,
    validated$n_transitions,
    length(predictor_cols)
  ))

  # --- STEP 5: Compute Class Weights ---
  class_counts <- table(response_vec)
  weights <- setNames(max(class_counts) / class_counts, names(class_counts))
  weight_vec <- weights[as.character(response_vec)]

  # --- STEP 6: Feature Selection Pipeline (controlled by arguments) ---
  collin_selected_names <- predictor_cols

  # -- (a) Collinearity filtering --
  if (do_collinearity) {
    collin_selected_names <- tryCatch(
      {
        category_wise_collin_filter(
          joined = joined,
          predictor_cols = predictor_cols,
          response_vec = response_vec,
          categories = pred_categories[predictor_cols],
          weights = weight_vec,
          corcut = 0.7
        )
      },
      error = function(e) {
        warning(sprintf("Collinearity filtering failed: %s", e$message))
        character(0)
      }
    )

    if (length(collin_selected_names) == 0) {
      return(create_summary_row(
        period,
        region,
        transition_name,
        validated$n_observations,
        validated$n_transitions,
        length(predictor_cols),
        n_after_collinearity = 0,
        status = "collinearity_filtering_failed",
        debug_dir = debug_dir,
        save_debug = save_debug
      ))
    }
  } else {
    message("Skipping collinearity filtering (do_collinearity = FALSE)")
  }

  # -- (b) GRRF filtering --
  grrf_selected_names <- collin_selected_names
  if (do_grrf) {
    grrf_selected_names <- tryCatch(
      {
        grrff_filter(
          joined = joined,
          predictor_cols = collin_selected_names,
          response_vec = response_vec,
          weight_vector = weights,
          gamma = 0.5
        )
      },
      error = function(e) {
        warning(sprintf("GRRF selection failed: %s", e$message))
        character(0)
      }
    )

    if (length(grrf_selected_names) == 0) {
      return(create_summary_row(
        period,
        region,
        transition_name,
        validated$n_observations,
        validated$n_transitions,
        length(predictor_cols),
        length(collin_selected_names),
        n_after_grrf = 0,
        status = "grrf_failed",
        debug_dir = debug_dir,
        save_debug = save_debug
      ))
    }
  } else {
    message("Skipping GRRF filtering (do_grrf = FALSE)")
  }

  # --- STEP 7: Update Focal Lookup ---
  focal_preds <- grep("nhood", grrf_selected_names, value = TRUE)
  if (length(focal_preds) > 0) {
    update_focal_lookup(focal_preds, period, transition_name, region, config)
  }

  # --- STEP 8: Create summary ---
  create_summary_row(
    period,
    region,
    transition_name,
    validated$n_observations,
    validated$n_transitions,
    length(predictor_cols),
    length(collin_selected_names),
    length(grrf_selected_names),
    paste(grrf_selected_names, collapse = "; "),
    paste(focal_preds, collapse = "; "),
    status = "success",
    debug_dir = debug_dir,
    save_debug = save_debug
  )
}

#' Load transition data from the parquet file for a specific transition and region
#' @param ds_transitions Arrow dataset for transitions
#' @param transition_name Name of the transition
#' @param region_value Numeric region value (if applicable)
#' @param use_regions Boolean indicating if regionalization is active
#' @return Data frame with cell_id and response columns
load_transition_data <- function(
  ds_transitions,
  transition_name,
  region_value = NULL,
  use_regions = FALSE
) {
  message(sprintf("Loading transition data for: %s", transition_name))

  trans_query <- ds_transitions %>%
    dplyr::select(cell_id, region, dplyr::all_of(transition_name))

  if (use_regions && !is.null(region_value)) {
    message(sprintf("  Filtering for region value: %d", region_value))
    trans_query <- trans_query %>% dplyr::filter(region == region_value)
  }

  trans_query <- trans_query %>% dplyr::filter(!is.na(.data[[transition_name]]))

  trans_df <- tryCatch(
    {
      result <- trans_query %>% dplyr::collect()
      message(sprintf(
        "  Loaded %d rows for transition %s",
        nrow(result),
        transition_name
      ))
      result
    },
    error = function(e) {
      warning(sprintf(
        "Failed to read transitions for %s: %s",
        transition_name,
        e$message
      ))
      tibble::tibble(cell_id = integer())
    }
  )

  if (nrow(trans_df) == 0) {
    message(sprintf("  No data found for transition: %s", transition_name))
    return(tibble::tibble(cell_id = integer(), response = integer()))
  }

  trans_df %>% dplyr::select(cell_id, response = dplyr::all_of(transition_name))
}

#' Load predictor data (both static and dynamic) from the parquet file for specific cell IDs
#' @param ds_static Arrow dataset for static predictors
#' @param ds_dynamic Arrow dataset for dynamic predictors
#' @param cell_ids Vector of cell IDs to load
#' @param predictor_names Vector of predictor variable names to load
#' @return Data frame with cell_id and predictor columns
load_predictor_data <- function(
  ds_static,
  ds_dynamic,
  cell_ids,
  predictor_names
) {
  message(sprintf("Loading predictors for %d cells", length(cell_ids)))
  message(sprintf(
    "  Requesting %d predictor variables",
    length(predictor_names)
  ))

  if (length(predictor_names) == 0 || length(cell_ids) == 0) {
    warning("No predictor names or cell IDs provided")
    return(tibble::tibble(cell_id = integer()))
  }

  # --- Load Static Predictors ---
  static_schema_names <- names(ds_static$schema)
  static_pred_names <- intersect(predictor_names, static_schema_names)
  message(sprintf("  Found %d static predictors", length(static_pred_names)))

  static_df <- tibble::tibble(cell_id = integer())
  if (length(static_pred_names) > 0) {
    static_df <- tryCatch(
      {
        result <- ds_static %>%
          dplyr::filter(cell_id %in% !!cell_ids) %>%
          dplyr::select(cell_id, dplyr::all_of(static_pred_names)) %>%
          dplyr::collect()
        message(sprintf(
          "    Loaded %d rows of static predictors",
          nrow(result)
        ))
        result
      },
      error = function(e) {
        warning(sprintf("Failed to read static predictors: %s", e$message))
        tibble::tibble(cell_id = integer())
      }
    )
  }

  # --- Load Dynamic Predictors ---
  dynamic_schema_names <- names(ds_dynamic$schema)
  dynamic_pred_names <- intersect(predictor_names, dynamic_schema_names)
  message(sprintf("  Found %d dynamic predictors", length(dynamic_pred_names)))

  dynamic_df <- tibble::tibble(cell_id = integer())
  if (length(dynamic_pred_names) > 0) {
    dynamic_df <- tryCatch(
      {
        result <- ds_dynamic %>%
          dplyr::filter(cell_id %in% !!cell_ids) %>%
          dplyr::select(cell_id, dplyr::all_of(dynamic_pred_names)) %>%
          dplyr::collect()
        message(sprintf(
          "    Loaded %d rows of dynamic predictors",
          nrow(result)
        ))
        result
      },
      error = function(e) {
        warning(sprintf("Failed to read dynamic predictors: %s", e$message))
        tibble::tibble(cell_id = integer())
      }
    )
  }

  # --- Combine Static and Dynamic ---
  if (nrow(static_df) > 0 && nrow(dynamic_df) > 0) {
    message("  Combining static and dynamic predictors via full_join")
    combined_df <- dplyr::full_join(static_df, dynamic_df, by = "cell_id")
  } else if (nrow(static_df) > 0) {
    message("  Using only static predictors")
    combined_df <- static_df
  } else if (nrow(dynamic_df) > 0) {
    message("  Using only dynamic predictors")
    combined_df <- dynamic_df
  } else {
    warning("No predictor data loaded from either static or dynamic sources")
    return(tibble::tibble(cell_id = integer()))
  }

  message(sprintf(
    "  Final combined dataset: %d rows, %d columns",
    nrow(combined_df),
    ncol(combined_df)
  ))
  combined_df
}


#' Validate and prepare data for feature selection
#' @param trans_df Data frame with transition data (cell_id, response)
#' @param preds_df Data frame with predictor data (cell_id, predictors)
#' @param min_observations Minimum number of observations required
#' @param min_transitions Minimum number of transition events required
#' @return List with status, joined data, and diagnostics
validate_and_join_data <- function(
  trans_df,
  preds_df,
  min_observations = 100,
  min_transitions = 10
) {
  if (nrow(trans_df) == 0) {
    return(list(
      status = "no_transition_data",
      n_observations = 0,
      n_transitions = 0,
      data = NULL
    ))
  }

  if (nrow(preds_df) == 0) {
    return(list(
      status = "no_predictor_data",
      n_observations = nrow(trans_df),
      n_transitions = sum(trans_df$response, na.rm = TRUE),
      data = NULL
    ))
  }

  joined <- dplyr::inner_join(trans_df, preds_df, by = "cell_id")

  if (nrow(joined) == 0) {
    return(list(
      status = "no_join_results",
      n_observations = 0,
      n_transitions = 0,
      data = NULL
    ))
  }

  n_obs <- nrow(joined)
  n_trans <- sum(joined$response, na.rm = TRUE)

  if (n_obs < min_observations) {
    return(list(
      status = "insufficient_observations",
      n_observations = n_obs,
      n_transitions = n_trans,
      data = NULL
    ))
  }

  if (n_trans < min_transitions) {
    return(list(
      status = "insufficient_transitions",
      n_observations = n_obs,
      n_transitions = n_trans,
      data = NULL
    ))
  }

  list(
    status = "valid",
    n_observations = n_obs,
    n_transitions = n_trans,
    data = joined
  )
}


#' Wrapper function that performs collinearity-based filter selection for predictors in specific categories
#' @param joined Full joined data frame
#' @param predictor_cols Column names of predictors to filter
#' @param response_vec Response vector (binary 0/1)
#' @param categories Named vector (base_name of predictors) of predictor categories
#' @param weights Weight vector
#' @param corcut Correlation cutoff threshold
#' @return Character vector of selected predictor names after category-wise collinearity filtering
category_wise_collin_filter <- function(
  joined,
  predictor_cols,
  response_vec,
  categories,
  weights,
  corcut = 0.7
) {
  # Split predictor names by category
  pred_by_category <- split(predictor_cols, categories[predictor_cols])

  # Apply collinearity filtering to each category
  selected_by_category <- lapply(pred_by_category, function(pred_names) {
    collin_filter(
      joined = joined,
      predictor_cols = pred_names,
      response_vec = response_vec,
      weights = weights,
      corcut = corcut
    )
  })

  # Combine all selected predictor names
  unlist(selected_by_category, use.names = FALSE)
}


#' Filter covariates for LULCC models (optimized - binary only, returns names)
#'
#' @param joined Full joined data frame
#' @param predictor_cols Column names of predictors to filter
#' @param response_vec Response vector (binary 0/1)
#' @param weights Weight vector
#' @param corcut Correlation cutoff threshold
#' @return Character vector of selected predictor names
collin_filter <- function(
  joined,
  predictor_cols,
  response_vec,
  weights,
  corcut = 0
) {
  # If only one predictor, return it
  if (length(predictor_cols) == 1) {
    return(predictor_cols)
  }

  # Compute ranking scores (using GLM method - optimized)
  # Pre-allocate and vectorize where possible
  pvals <- vapply(
    predictor_cols,
    function(pred_name) {
      x <- joined[[pred_name]]

      # Remove rows with NA values in predictor
      na_idx <- is.na(x)
      if (all(na_idx)) {
        # If all values are NA, return worst possible p-value
        return(1.0)
      }

      x_clean <- x[!na_idx]
      y_clean <- response_vec[!na_idx]
      w_clean <- weights[!na_idx]

      # Check if we have enough data and variation
      if (length(unique(x_clean)) < 3 || length(x_clean) < 10) {
        return(1.0)
      }

      # Fit GLM with quadratic term
      tryCatch(
        {
          mdl <- glm(
            y_clean ~ poly(x_clean, degree = 2, simple = TRUE),
            family = binomial(link = "logit"),
            weights = w_clean
          )

          # Return minimum p-value for linear and quadratic terms
          coef_summary <- summary(mdl)$coefficients
          min(coef_summary[2:3, 4])
        },
        error = function(e) {
          # If GLM fails, return worst p-value
          1.0
        }
      )
    },
    numeric(1)
  )

  # Remove predictors that failed (p-value = 1.0 for all)
  valid_preds <- names(pvals[pvals < 1.0])

  if (length(valid_preds) == 0) {
    warning("No valid predictors after GLM fitting")
    return(character(0))
  }

  # Rank predictors by p-value (ascending)
  ranked_names <- names(sort(pvals[valid_preds]))

  # Compute correlation matrix for ranked predictors
  cor_mat <- abs(cor(
    joined[, ranked_names, drop = FALSE],
    use = "pairwise.complete.obs"
  ))

  # Iteratively remove correlated predictors
  selected <- character(0)

  if (all(cor_mat[cor_mat != 1] < corcut, na.rm = TRUE)) {
    selected <- ranked_names
  } else {
    remaining_names <- ranked_names

    while (length(remaining_names) > 0) {
      # Take first (best ranked) predictor
      current_pred <- remaining_names[1]
      selected <- c(selected, current_pred)

      if (length(remaining_names) == 1) {
        break
      }

      # Find predictors not highly correlated with current predictor
      current_cors <- cor_mat[current_pred, remaining_names[-1]]
      keep_idx <- which(current_cors <= corcut)

      if (length(keep_idx) == 0) {
        break
      }

      remaining_names <- remaining_names[-1][keep_idx]
      cor_mat <- cor_mat[remaining_names, remaining_names, drop = FALSE]

      # Check if remaining predictors are all below threshold
      if (
        length(remaining_names) > 1 &&
          all(cor_mat[cor_mat != 1] < corcut, na.rm = TRUE)
      ) {
        selected <- c(selected, remaining_names)
        break
      }
    }
  }

  selected
}

#' Guided Regularized Random Forest feature selection (optimized - returns names only)
#'
#' @param joined Full joined data frame
#' @param predictor_cols Column names of predictors to use
#' @param response_vec Response vector (binary 0/1)
#' @param weight_vector Class weights
#' @param gamma Importance coefficient (0-1)
#' @return Character vector of selected predictor names
grrff_filter <- function(
  joined,
  predictor_cols,
  response_vec,
  weight_vector,
  gamma
) {
  # Extract predictor data as matrix (more efficient for RF)
  cov_mat <- as.data.frame(joined[, predictor_cols, drop = FALSE])
  response_fac <- as.factor(response_vec)

  # Check for and handle missing values
  na_rows <- rowSums(is.na(cov_mat)) > 0

  if (any(na_rows)) {
    message(sprintf(
      "  Removing %d rows with missing values for GRRF",
      sum(na_rows)
    ))
    cov_mat <- cov_mat[!na_rows, , drop = FALSE]
    response_fac <- response_fac[!na_rows]

    # Check if we have enough data left
    if (nrow(cov_mat) < 100) {
      warning("Insufficient complete cases for GRRF after removing NAs")
      return(character(0))
    }
  }

  # Check for zero-variance predictors (after NA removal)
  zero_var <- vapply(cov_mat, function(x) length(unique(x)) <= 1, logical(1))
  if (any(zero_var)) {
    message(sprintf("  Removing %d zero-variance predictors", sum(zero_var)))
    cov_mat <- cov_mat[, !zero_var, drop = FALSE]
    predictor_cols <- predictor_cols[!zero_var]

    if (ncol(cov_mat) == 0) {
      warning(
        "No predictors with variance after removing zero-variance columns"
      )
      return(character(0))
    }
  }

  # Run non-regularized RF to get importance scores
  rf <- RRF::RRF(cov_mat, response_fac, flagReg = 0)

  # Normalize importance scores
  imp_raw <- rf$importance[, "MeanDecreaseGini"]
  imp_norm <- imp_raw / max(imp_raw)

  # Calculate penalty coefficients
  coef_reg <- (1 - gamma) + gamma * imp_norm

  # Run guided regularized RF
  mdl_rf <- RRF::RRF(
    cov_mat,
    response_fac,
    classwt = weight_vector,
    coefReg = coef_reg,
    flagReg = 1
  )

  # Extract predictors with positive importance
  rf_imp <- mdl_rf$importance[, "MeanDecreaseGini"]
  selected_names <- names(rf_imp[rf_imp > 0])

  # Return names sorted by importance (descending)
  selected_names[order(rf_imp[selected_names], decreasing = TRUE)]
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

#' Create a standardized summary row
create_summary_row <- function(
  period,
  region,
  transition_name,
  n_observations,
  n_transitions,
  n_initial_predictors,
  n_after_collinearity = 0,
  n_after_grrf = 0,
  selected_predictors = NA_character_,
  focal_predictors = NA_character_,
  status,
  save_debug,
  debug_dir
) {
  summary <- tibble::tibble(
    period = period,
    region = if (is.null(region) || is.na(region)) NA_character_ else region,
    transition = transition_name,
    n_observations = n_observations,
    n_transitions = n_transitions,
    n_initial_predictors = n_initial_predictors,
    n_after_collinearity = n_after_collinearity,
    n_after_grrf = n_after_grrf,
    selected_predictors = selected_predictors,
    focal_predictors = focal_predictors,
    status = status
  )

  if (save_debug) {
    debug_path <- file.path(
      debug_dir,
      sprintf("fs_summary_%s_%s.rds", transition_name, region)
    )
    saveRDS(
      summary,
      file = debug_path
    )
  }
  return(summary)
}


#' Update focal layer lookup
update_focal_lookup <- function(
  focal_preds,
  period,
  transition,
  region,
  config
) {
  lookup_path <- file.path(
    config[["preds_tools_dir"]],
    "neighbourhood_details_for_dynamic_updating",
    "focal_layer_lookup.rds"
  )

  if (!file.exists(lookup_path)) {
    warning("Focal layer lookup file not found")
    return(invisible(NULL))
  }

  focal_lookup <- readRDS(lookup_path) %>% dplyr::filter(period == !!period)

  focal_subset <- focal_lookup %>%
    dplyr::filter(grepl(paste(focal_preds, collapse = "|"), layer_name)) %>%
    dplyr::mutate(transition = transition, region = region)

  output_path <- if (!is.na(region)) {
    file.path(
      config[["preds_tools_dir"]],
      "neighbourhood_details_for_dynamic_updating",
      sprintf("%s_region_%s_focals_for_updating.rds", period, region)
    )
  } else {
    file.path(
      config[["preds_tools_dir"]],
      "neighbourhood_details_for_dynamic_updating",
      sprintf("%s_focals_for_updating.rds", period)
    )
  }

  if (file.exists(output_path)) {
    existing <- readRDS(output_path)
    focal_subset <- dplyr::bind_rows(existing, focal_subset)
  }

  saveRDS(focal_subset, output_path)
  invisible(NULL)
}
