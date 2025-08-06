#' Feature_selection
#'
#' Performing collinearity based and embedded feature selection with Guided
#' Regularized Random Forests
#'
#' @author: Ben Black
#'
#' @export

transition_feature_selection <- function(config = get_config()) {
  # Import model specifications table
  model_specs <- readr::read_csv(config[["model_specs_path"]])

  # Filter for models with feature selection required
  Filtering_required <-
    model_specs |>
    dplyr::filter(feature_selection_employed) |>
    dplyr::group_by(model_scale) |>
    dplyr::distinct(data_period_name)

  # Add tag column
  Filtering_required$tag <- paste0(
    Filtering_required$data_period_name,
    "_", Filtering_required$model_scale
  )

  # Split into named list
  Datasets_for_PS <-
    split(Filtering_required, seq_len(nrow(Filtering_required))) |>
    lapply(as.list)
  names(Datasets_for_PS) <- Filtering_required$tag

  # Loop through folders and create any that do not exist
  purrr::walk(
    list(
      config[["trans_pre_pred_filter_dir"]],
      config[["collinearity_dir"]],
      config[["grrf_dir"]],
      config[["trans_post_pred_filter_dir"]]
    ),
    ensure_dir
  )

  ### =========================================================================
  ### B- Perform Feature Selection
  ### =========================================================================

  # wrapper function to run feature selection over multiple scales and datasets
  lulcc.featureselection <- function(Dataset_details) {
    ### =========================================================================
    ### A- Preparation
    ### =========================================================================
    Data_period <- Dataset_details$data_period_name
    Dataset_scale <- Dataset_details$model_scale

    # Load the predictor data table that will be used to identify the categories of covariates
    # and perform collinearity testing seperately
    Predictor_table <- openxlsx::read.xlsx(config[["pred_table_path"]], sheet = Data_period)

    # vector file paths for the data for the period specified
    Data_paths <- list.files(
      file.path(config[["trans_pre_pred_filter_dir"]], Data_period),
      pattern = Dataset_scale,
      full.names = TRUE
    )
    names(Data_paths) <- sapply(Data_paths, basename)

    ### =========================================================================
    ### B- Stage 1: Collinearity based 'filter' feature selection
    ### =========================================================================
    collin_selection_results <- furrr::future_map(
      Data_paths,
      function(z) {
        gc()

        # Load dataset as terra SpatRaster if not already a data.frame/data.table
        # Assuming 'Trans_data' is a list as per previous scripts
        Trans_data <- readRDS(z)

        # Perform filter based feature selection
        Collin_filtered_data <- tryCatch(
          {
            lulcc.filtersel(
              transition_result = Trans_data$trans_result,
              cov_data = Trans_data$cov_data,
              categories = Predictor_table$CA_category[
                Predictor_table$Covariate_ID %in% names(Trans_data$cov_data)
              ],
              collin_weight_vector = Trans_data$collin_weights,
              embedded_weight_vector = Trans_data$embed_weights,
              focals = c("neighbourhood"),
              method = "GLM",
              corcut = 0.7
            )
          },
          error = function(e) {
            warning(paste("Collinearity filtering failed for dataset:", z, "\n", e))
            return(NULL)
          }
        )

        if (!is.null(Collin_filtered_data)) {
          # Save the result
          Dataset_name <- tools::file_path_sans_ext(basename(z))
          Save_dir <- file.path(config[["collinearity_dir"]], Data_period)
          dir.create(Save_dir, recursive = TRUE, showWarnings = FALSE)
          Save_path_collinearity <- file.path(
            Save_dir, paste0(Dataset_name, "_collin_filtered.rds")
          )
          saveRDS(Collin_filtered_data, Save_path_collinearity)

          gc()
          return(Save_path_collinearity)
        } else {
          return(NULL)
        }
      }
    ) # close loop over transition datasets

    # Remove NULL results (failed selections)
    collin_selection_results <- collin_selection_results[
      !sapply(collin_selection_results, is.null)
    ]

    message("Collinearity based covariate selection complete")

    ### =========================================================================
    ### C- Stage 2: GRRF Embedded feature selection
    ### =========================================================================

    GRRF_selection_results <- furrr::future_map(
      collin_selection_results,
      function(x) {
        # Load dataset
        Collin_filtered_data <- readRDS(x)

        GRRF_filtered_data <- tryCatch(
          {
            lulcc.grrffeatselect(
              transition_result = Collin_filtered_data$transition_result,
              cov_data = Collin_filtered_data$covdata_collinearity_filtered,
              weight_vector = Collin_filtered_data$embedded_weight_vector,
              gamma = 0.5
            )
          },
          error = function(e) {
            warning(paste("GRRF feature selection failed for dataset:", x, "\n", e))
            return(NULL)
          }
        )

        if (!is.null(GRRF_filtered_data)) {
          # Save the result
          Dataset_name <- tools::file_path_sans_ext(basename(x))
          Save_dir <- file.path(config[["grrf_dir"]], Data_period)
          dir.create(Save_dir, recursive = TRUE, showWarnings = FALSE)
          Save_path_grrf <- file.path(
            Save_dir, paste0(gsub("_collin_filtered", "", Dataset_name), "_GRRF_filtered.rds")
          )
          saveRDS(GRRF_filtered_data, Save_path_grrf)
          return(Save_path_grrf)
        } else {
          return(NULL)
        }
      }
    ) # close loop over collinearity selection results

    # Remove NULL results (failed selections)
    GRRF_selection_results <- GRRF_selection_results[
      !sapply(GRRF_selection_results, is.null)
    ]

    message("GRRF embedded covariate selection done")

    ### =========================================================================
    ### D- Summarize results of predictor selection procedures
    ### =========================================================================

    # Loop over the lists of results extracting names of remaining predictors
    Filtered_predictors <- lapply(GRRF_selection_results, function(x) {
      if (!is.null(x)) {
        list(
          collinearity_preds = colnames(readRDS(x)[["covdata_collinearity_filtered"]]),
          GRRF_preds = tryCatch(readRDS(x)[["var"]], error = function(e) {
            warning(paste("Failed to extract GRRF predictors from:", x, "\n", e))
            return(NULL)
          })
        )
      } else {
        NULL
      }
    })

    # Remove NULL entries
    Filtered_predictors <- Filtered_predictors[!sapply(Filtered_predictors, is.null)]

    # Rename with transition names
    names(Filtered_predictors) <- names(GRRF_selection_results)

    ### =========================================================================
    ### E- Subsetting datasets with results of predictor filtering
    ### =========================================================================

    furrr::future_walk(seq_along(Data_paths), function(i) {
      # Read pre-filtering data
      Pre_PS_dat <- readRDS(Data_paths[[i]])

      # Subset the cov_data component by the names of the remaining predictors following GRRF
      Pred_names <- Filtered_predictors[[i]][["GRRF_preds"]]

      # Check if Pred_names is not NULL and exists in cov_data
      if (!is.null(Pred_names)) {
        existing_preds <- Pred_names[Pred_names %in% names(Pre_PS_dat$cov_data)]
        if (length(existing_preds) > 0) {
          Pre_PS_dat$cov_data <- Pre_PS_dat$cov_data[, existing_preds, drop = FALSE]
          colnames(Pre_PS_dat$cov_data) <- existing_preds
        } else {
          warning(paste("No matching predictors found for dataset:", Data_paths[[i]]))
        }
      }

      # Assign to Post_PS_dat
      Post_PS_dat <- Pre_PS_dat

      # Save the filtered dataset
      Post_PS_dir <- file.path(config[["trans_post_pred_filter_dir"]], Data_period)
      dir.create(Post_PS_dir, recursive = TRUE, showWarnings = FALSE)
      Post_PS_dat_save_path <- file.path(Post_PS_dir, paste0(basename(Data_paths[[i]])))
      saveRDS(Post_PS_dat, Post_PS_dat_save_path)

      gc()
    })

    message("Transition datasets subsetted to filtered covariates")

    ### =========================================================================
    ### F- Identifying focal layers in final covariate selection for dynamic updating in simulations
    ### =========================================================================

    # Identify focal variables
    Focal_preds_remaining <- unique(unlist(lapply(Filtered_predictors, function(x) {
      grep("nhood", x[["GRRF_preds"]], value = TRUE)
    })))

    # Load focal layer look up table
    Focal_lookup <- readRDS(file.path(
      config[["preds_tools_dir"]], "neighbourhood_details_for_dynamic_updating",
      "focal_layer_lookup.rds"
    ))

    # Subset by the current data period
    Focal_lookup <- Focal_lookup[Focal_lookup$period == Data_period, ]

    # Subset the focal look up table by the list of focals required for the
    # transition models for this period
    Focal_subset <- Focal_lookup[
      grep(paste(Focal_preds_remaining, collapse = "|"), Focal_lookup$layer_name),
    ]

    # Save the Focal layer details for this period
    saveRDS(Focal_subset, file.path(
      config[["preds_tools_dir"]], "neighbourhood_details_for_dynamic_updating",
      paste0(Data_period, "_", Dataset_scale, "_focals_for_updating.rds")
    ))

    message("Focal layers identified for updating during simulation")

    # Return list of predictors for each dataset as this will also capture try errors
    return(Filtered_predictors)
  } # close wrapper function

  # Apply feature selection to all required datasets
  lapply(Datasets_for_PS, function(x) lulcc.featureselection(Dataset_details = x))

  message("Covariate selection complete")
}
