#' Streamlined Feature Selection for LULCC Transitions
#'
#' Performs collinearity-based and GRRF embedded feature selection
#' directly on Parquet files without intermediate data structures
#'
#' @author: Ben Black (adapted)
#' @export

transition_feature_selection <- function(config = get_config()) {
  # Setup parallel processing
  future::plan(future::multisession)

  # Load model specifications
  model_specs <- readr::read_csv(config[["model_specs_path"]])

  # Filter for models requiring feature selection
  periods_to_process <- model_specs |>
    dplyr::filter(feature_selection_employed) |>
    dplyr::distinct(data_period_name, model_scale)

  # Process each period and scale combination
  results_list <- purrr::map2(
    periods_to_process$data_period_name,
    periods_to_process$model_scale,
    function(period, scale) {
      perform_feature_selection(
        period = period,
        scale = scale,
        config = config
      )
    }
  )

  # Combine all results into a single summary table
  final_summary <- dplyr::bind_rows(results_list)

  # Save final summary
  readr::write_csv(
    final_summary,
    file.path(config[["output_dir"]], "feature_selection_summary.csv")
  )

  message("Feature selection complete. Summary saved.")
  return(final_summary)
}


#' Perform feature selection for a single period and scale
#'
#' @param period Data period name
#' @param scale Model scale
#' @param config Configuration list
#' @return Data frame with selected features per transition

perform_feature_selection <- function(period, scale, config) {
  message(sprintf("Processing period: %s, scale: %s", period, scale))

  # Load transition definitions for this period
  transitions_info <- readRDS(config[["transitions_rds_path"]]) |>
    dplyr::filter(data_period_name == period)

  # Load predictor metadata from YAML
  pred_metadata <- yaml::read_yaml(config[["pred_metadata_path"]])[[period]]

  # Get predictor categories
  pred_categories <- setNames(
    pred_metadata$pred_category,
    pred_metadata$pred_name
  )

  # Paths to data
  transitions_pq_path <- file.path(
    config[["transitions_dir"]],
    sprintf("%s_%s_transitions.parquet", period, scale)
  )

  predictors_pq_path <- file.path(
    config[["predictors_dir"]],
    sprintf("%s_%s_predictors.parquet", period, scale)
  )

  # Process each transition
  transition_results <- furrr::future_map_dfr(
    transitions_info$transition_name,
    function(trans_name) {
      process_single_transition(
        transition_name = trans_name,
        transitions_pq_path = transitions_pq_path,
        predictors_pq_path = predictors_pq_path,
        pred_categories = pred_categories,
        pred_metadata = pred_metadata,
        period = period,
        scale = scale,
        config = config
      )
    },
    .options = furrr::furrr_options(seed = TRUE)
  )

  return(transition_results)
}


#' Process feature selection for a single transition
#'
#' @param transition_name Name of the transition column
#' @param transitions_pq_path Path to transitions parquet file
#' @param predictors_pq_path Path to predictors parquet file
#' @param pred_categories Named vector of predictor categories
#' @param pred_metadata List of predictor metadata
#' @param period Data period
#' @param scale Model scale
#' @param config Configuration list
#' @return Data frame row with selection results

process_single_transition <- function(
  transition_name,
  transitions_pq_path,
  predictors_pq_path,
  pred_categories,
  pred_metadata,
  period,
  scale,
  config
) {
  # Read and filter transition data (only valid observations: 0 or 1)
  trans_data <- arrow::read_parquet(
    transitions_pq_path,
    col_select = c("cell_id", transition_name)
  ) |>
    dplyr::filter(!is.na(.data[[transition_name]]))

  # Early exit if insufficient data
  if (nrow(trans_data) < 100 || sum(trans_data[[transition_name]]) < 10) {
    warning(sprintf("Insufficient data for transition: %s", transition_name))
    return(tibble::tibble(
      period = period,
      scale = scale,
      transition = transition_name,
      n_initial_predictors = length(pred_categories),
      n_after_collinearity = 0,
      n_after_grrf = 0,
      selected_predictors = NA_character_,
      status = "insufficient_data"
    ))
  }

  # Read predictor data and join
  pred_data <- arrow::read_parquet(
    predictors_pq_path,
    col_select = c("cell_id", names(pred_categories))
  ) |>
    dplyr::inner_join(trans_data, by = "cell_id") |>
    dplyr::select(-cell_id)

  # Extract response and predictors
  trans_result <- pred_data[[transition_name]]
  cov_data <- pred_data |>
    dplyr::select(-all_of(transition_name)) |>
    as.data.frame()

  # Calculate weights for this transition
  class_counts <- table(trans_result)
  weights <- setNames(
    max(class_counts) / class_counts,
    names(class_counts)
  )

  # Stage 1: Collinearity-based filtering
  collin_filtered <- tryCatch(
    {
      lulcc_filtersel_simple(
        transition_result = trans_result,
        cov_data = cov_data,
        categories = pred_categories[names(cov_data)],
        weights = weights[as.character(trans_result)],
        focal_categories = c("neighbourhood"),
        method = "GLM",
        corcut = 0.7
      )
    },
    error = function(e) {
      warning(sprintf(
        "Collinearity filtering failed for %s: %s",
        transition_name,
        e$message
      ))
      return(NULL)
    }
  )

  if (is.null(collin_filtered) || ncol(collin_filtered) == 0) {
    return(tibble::tibble(
      period = period,
      scale = scale,
      transition = transition_name,
      n_initial_predictors = ncol(cov_data),
      n_after_collinearity = 0,
      n_after_grrf = 0,
      selected_predictors = NA_character_,
      status = "collinearity_failed"
    ))
  }

  # Stage 2: GRRF embedded feature selection
  grrf_selected <- tryCatch(
    {
      lulcc_grrffeatselect(
        transition_result = trans_result,
        cov_data = collin_filtered,
        weight_vector = weights,
        gamma = 0.5
      )
    },
    error = function(e) {
      warning(sprintf(
        "GRRF filtering failed for %s: %s",
        transition_name,
        e$message
      ))
      return(NULL)
    }
  )

  if (is.null(grrf_selected) || nrow(grrf_selected) == 0) {
    return(tibble::tibble(
      period = period,
      scale = scale,
      transition = transition_name,
      n_initial_predictors = ncol(cov_data),
      n_after_collinearity = ncol(collin_filtered),
      n_after_grrf = 0,
      selected_predictors = NA_character_,
      status = "grrf_failed"
    ))
  }

  # Compile results
  selected_vars <- grrf_selected$var

  # Save the filtered dataset for this transition
  filtered_data <- list(
    transition_result = trans_result,
    cov_data = collin_filtered[, selected_vars, drop = FALSE],
    selected_predictors = selected_vars
  )

  save_path <- file.path(
    config[["filtered_data_dir"]],
    period,
    sprintf("%s_%s_filtered.rds", transition_name, scale)
  )
  dir.create(dirname(save_path), recursive = TRUE, showWarnings = FALSE)
  saveRDS(filtered_data, save_path)

  # Identify focal predictors for dynamic updating
  focal_preds <- grep("nhood", selected_vars, value = TRUE)

  if (length(focal_preds) > 0) {
    update_focal_lookup(
      focal_preds = focal_preds,
      period = period,
      scale = scale,
      transition = transition_name,
      config = config
    )
  }

  # Return summary
  tibble::tibble(
    period = period,
    scale = scale,
    transition = transition_name,
    n_initial_predictors = ncol(cov_data),
    n_after_collinearity = ncol(collin_filtered),
    n_after_grrf = length(selected_vars),
    selected_predictors = paste(selected_vars, collapse = "; "),
    focal_predictors = paste(focal_preds, collapse = "; "),
    status = "success"
  )
}


#' Simplified collinearity-based filter selection
#'
#' @param transition_result Binary vector of transition outcomes
#' @param cov_data Data frame of predictors
#' @param categories Named vector of predictor categories
#' @param weights Weight vector for observations
#' @param focal_categories Categories to treat as focal (nested filtering)
#' @param method Ranking method (GLM, COR.P, etc.)
#' @param corcut Correlation cutoff threshold
#' @return Filtered data frame of predictors

lulcc_filtersel_simple <- function(
  transition_result,
  cov_data,
  categories,
  weights,
  focal_categories = NULL,
  method = "GLM",
  corcut = 0.7
) {
  # Split covariates by category
  cov_by_category <- split.default(cov_data, categories)

  # Handle focal categories with nested filtering
  if (!is.null(focal_categories)) {
    for (focal_cat in focal_categories) {
      if (focal_cat %in% names(cov_by_category)) {
        focal_cov <- cov_by_category[[focal_cat]]

        # Split focal covariates by LULC type
        focal_groups <- split.default(
          focal_cov,
          sub("\\cov.*", "", names(focal_cov))
        )

        # Filter each group
        focal_filtered <- lapply(focal_groups, function(x) {
          lulcc_covfilter(
            cov_data = x,
            trans_result = transition_result,
            method = method,
            weights = weights,
            corcut = 0 # No correlation filtering within focal groups
          )
        })

        cov_by_category[[focal_cat]] <- do.call("cbind", focal_filtered)
      }
    }
  }

  # Filter each category
  filtered_categories <- lapply(cov_by_category, function(cat_data) {
    lulcc_covfilter(
      cov_data = cat_data,
      trans_result = transition_result,
      method = method,
      weights = weights,
      corcut = corcut
    )
  })

  # Combine filtered categories
  result <- do.call("cbind", filtered_categories)
  names(result) <- gsub("^.*\\.", "", names(result))

  return(result)
}


#' Update focal layer lookup table
#'
#' @param focal_preds Vector of focal predictor names
#' @param period Data period
#' @param scale Model scale
#' @param transition Transition name
#' @param config Configuration list

update_focal_lookup <- function(
  focal_preds,
  period,
  scale,
  transition,
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

  focal_lookup <- readRDS(lookup_path) |>
    dplyr::filter(period == !!period)

  # Match focal predictors
  focal_subset <- focal_lookup |>
    dplyr::filter(
      grepl(paste(focal_preds, collapse = "|"), layer_name)
    ) |>
    dplyr::mutate(
      transition = transition,
      scale = scale
    )

  # Append to period-specific lookup
  output_path <- file.path(
    config[["preds_tools_dir"]],
    "neighbourhood_details_for_dynamic_updating",
    sprintf("%s_%s_focals_for_updating.rds", period, scale)
  )

  if (file.exists(output_path)) {
    existing <- readRDS(output_path)
    focal_subset <- dplyr::bind_rows(existing, focal_subset)
  }

  saveRDS(focal_subset, output_path)

  invisible(NULL)
}


# Keep your existing helper functions (lulcc_covfilter, lulcc_grrffeatselect)
# with minimal modifications...

lulcc_covfilter <- function(
  cov_data,
  trans_result,
  method,
  weights,
  corcut = 0
) {
  # [Your existing implementation - no changes needed]
  # ... [paste your existing lulcc.covfilter code here]
}

lulcc_grrffeatselect <- function(
  transition_result,
  cov_data,
  weight_vector,
  gamma
) {
  # [Your existing implementation - no changes needed]
  # ... [paste your existing lulcc.grrffeatselect code here]
}
