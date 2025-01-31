#############################################################################
## Feature_selection: Performing collinearity based and
## embedded feature selection with Guided Regularized Random Forests
## Date: 25-09-2021
## Author: Ben Black (Modified version)
#############################################################################

### =========================================================================
### A- Preparation
### =========================================================================
transition_feature_selection <- function() {
  # Import model specifications table
  model_specs <- readxl::read_excel(model_specs_path)

  # Filter for models with feature selection required
  Filtering_required <- model_specs %>%
    dplyr::filter(Feature_selection_employed == "TRUE") %>%
    dplyr::group_by(model_scale) %>%
    dplyr::distinct(data_period_name)

  # Add tag column
  Filtering_required$tag <- paste0(Filtering_required$data_period_name, "_", Filtering_required$model_scale)

  # Split into named list
  Datasets_for_PS <- split(Filtering_required, seq(nrow(Filtering_required)))
  Datasets_for_PS <- lapply(Datasets_for_PS, as.list)
  names(Datasets_for_PS) <- Filtering_required$tag

  # Set folder paths
  Pre_PS_folder <- "Data/Transition_datasets/Pre_predictor_filtering" # Pre Predictor selection datasets folder
  collinearity_folder_path <- "Results/Model_tuning/Predictor_selection/Collinearity_filtering"
  grrf_folder_path <- "Results/Model_tuning/Predictor_selection/GRRF_embedded_selection"
  Filtered_datasets_folder_path <- "Data/Transition_datasets/Post_predictor_filtering"
  PS_results_folder <- "Results/Model_tuning/Predictor_selection/Predictor_selection_summaries" # Predictor selection results folder

  # Loop through folders and create any that do not exist
  lapply(list(
    Pre_PS_folder,
    collinearity_folder_path,
    grrf_folder_path,
    Filtered_datasets_folder_path,
    PS_results_folder
  ), function(x) dir.create(x, recursive = TRUE, showWarnings = FALSE))

  # Predictor table file path (received from output_env, only uncomment here for testing)
  # pred_table_path <- "Tools/Predictor_table.xlsx"

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
    Predictor_table <- openxlsx::read.xlsx(pred_table_path, sheet = Data_period)

    # vector file paths for the data for the period specified
    Data_paths <- list.files(paste0(Pre_PS_folder, "/", Data_period), pattern = Dataset_scale, full.names = TRUE)
    names(Data_paths) <- sapply(Data_paths, function(x) basename(x))

    ### =========================================================================
    ### B- Stage 1: Collinearity based 'filter' feature selection
    ### =========================================================================

    future::plan(multisession, workers = parallel::availableCores() - 2)

    collin_selection_results <- future.apply::future_lapply(Data_paths, function(z) {
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
            categories = Predictor_table$CA_category[Predictor_table$Covariate_ID %in% names(Trans_data$cov_data)],
            collin_weight_vector = Trans_data$collin_weights,
            embedded_weight_vector = Trans_data$embed_weights,
            focals = c("Neighbourhood"),
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
        Save_dir <- file.path(collinearity_folder_path, Data_period)
        dir.create(Save_dir, recursive = TRUE, showWarnings = FALSE)
        Save_path_collinearity <- file.path(Save_dir, paste0(Dataset_name, "_collin_filtered.rds"))
        saveRDS(Collin_filtered_data, Save_path_collinearity)

        gc()
        return(Save_path_collinearity)
      } else {
        return(NULL)
      }
    }) # close loop over transition datasets

    # Remove NULL results (failed selections)
    collin_selection_results <- collin_selection_results[!sapply(collin_selection_results, is.null)]

    future::plan(sequential)

    cat("Collinearity based covariate selection complete \n")

    ### =========================================================================
    ### C- Stage 2: GRRF Embedded feature selection
    ### =========================================================================

    future::plan(multisession, workers = parallel::availableCores() - 2)

    GRRF_selection_results <- future.apply::future_lapply(collin_selection_results, function(x) {
      gc()

      # Load dataset
      Collin_filtered_data <- readRDS(x)

      GRRF_filtered_data <- tryCatch(
        {
          lulcc.grrffeatselect(
            transition_result = Collin_filtered_data$trans_result,
            cov_data = Collin_filtered_data$cov_data,
            weight_vector = Collin_filtered_data$embed_weights,
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
        Save_dir <- file.path(grrf_folder_path, Data_period)
        dir.create(Save_dir, recursive = TRUE, showWarnings = FALSE)
        Save_path_grrf <- file.path(Save_dir, paste0(gsub("_collin_filtered", "", Dataset_name), "_GRRF_filtered.rds"))
        saveRDS(GRRF_filtered_data, Save_path_grrf)

        gc()
        return(Save_path_grrf)
      } else {
        return(NULL)
      }
    }) # close loop over collinearity selection results

    # Remove NULL results (failed selections)
    GRRF_selection_results <- GRRF_selection_results[!sapply(GRRF_selection_results, is.null)]

    future::plan(sequential)

    cat("GRRF embedded covariate selection done \n")

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

    future::plan(multisession, workers = parallel::availableCores() - 2)

    future.apply::future_lapply(seq_along(Data_paths), function(i) {
      gc()

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
      Post_PS_dir <- file.path(Filtered_datasets_folder_path, Data_period)
      dir.create(Post_PS_dir, recursive = TRUE, showWarnings = FALSE)
      Post_PS_dat_save_path <- file.path(Post_PS_dir, paste0(basename(Data_paths[[i]])))
      saveRDS(Post_PS_dat, Post_PS_dat_save_path)

      gc()
    })

    future::plan(sequential)

    cat("Transition datasets subsetted to filtered covariates \n")

    ### =========================================================================
    ### F- Identifying focal layers in final covariate selection for dynamic updating in simulations
    ### =========================================================================

    # Identify focal variables
    Focal_preds_remaining <- unique(unlist(lapply(Filtered_predictors, function(x) {
      grep("nhood", x[["GRRF_preds"]], value = TRUE)
    })))

    # Load focal layer look up table
    Focal_lookup <- readRDS("Data/Preds/Tools/Neighbourhood_details_for_dynamic_updating/Focal_layer_lookup.rds")

    # Subset by the current data period
    Focal_lookup <- Focal_lookup[Focal_lookup$period == Data_period, ]

    # Subset the focal look up table by the list of focals required for the transition models for this period
    Focal_subset <- Focal_lookup[grep(paste(Focal_preds_remaining, collapse = "|"), Focal_lookup$layer_name), ]

    # Save the Focal layer details for this period
    saveRDS(Focal_subset, file.path(
      "Data/Preds/Tools/Neighbourhood_details_for_dynamic_updating",
      paste0(Data_period, "_", Dataset_scale, "_focals_for_updating.rds")
    ))

    cat("Focal layers identified for updating during simulation \n")

    # Return list of predictors for each dataset as this will also capture try errors
    return(Filtered_predictors)
  } # close wrapper function

  # Apply feature selection to all required datasets
  Filtering_overview <- lapply(Datasets_for_PS, function(x) lulcc.featureselection(Dataset_details = x))

  cat("Covariate selection complete \n")
}
