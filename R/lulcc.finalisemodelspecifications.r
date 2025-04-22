#' Finalize Model Specifications
#'
#' This function prepares configuration files for predicting land use land cover change.
#' It interactively gathers model parameters from the user and creates specification
#' files for prediction based on previously completed models.
#'
#' @author Ben Black
#'
#' @param config A list of configuration parameters
#'
#' @details
#' The function performs the following steps:
#' 1. Loads model specifications from the provided Excel file
#' 2. Filters for completed model specifications
#' 3. Prompts user to select model type, scale, feature selection option, and balance adjustment
#' 4. Creates a subset of model specifications based on user inputs
#' 5. Sets the "modelling_completed" flag to "N" for the prediction configurations
#' 6. Warns if any specified data periods are not included in the filtered model specifications
#' 7. Saves the prediction model specifications to "tools/predict_model_specs.xlsx"
#' 8. Creates a parameter grid for prediction by prompting user for parameter values
#' 9. Saves the prediction parameter grid to "tools/predict_param-grid.xlsx"
#'
#' @return No return value, called for side effects (creating configuration files)
#'
#' @export

lulcc.finalisemodelspecifications <- function(
    model_type = "rf",
    model_scale = "regionalized",
    feature_selection_employed = TRUE,
    balance_adjustment = TRUE,
    config = get_config()) {
  # Load model specifications
  model_specs <- readxl::read_excel(config[["model_specs_path"]])

  # Subset model specifications by user inputs
  predict_model_specs <- model_specs[
    model_specs$modelling_completed == "Y" &
      model_specs$model_type == model_type &
      model_specs$model_scale == model_scale &
      model_specs$feature_selection_employed == feature_selection_employed &
      model_specs$balance_adjustment == balance_adjustment,
  ]

  # change completion values to 'N'
  predict_model_specs$modelling_completed <- "N"

  data_periods <- config[["data_periods"]]
  # Print a warning that not all data periods are included in the predict model specs table
  if (!all(data_periods %in% unique(predict_model_specs$data_period_name))) {
    stop(
      "Warning the data period/s: ",
      paste(
        data_periods[which(!(data_periods %in% unique(predict_model_specs$data_period_name)))],
        sep = ","
      ),
      " do not have models of the desired specification completed for them
        (i.e. Errors may have occured in model fitting or evaluation,
        please check before continuing"
    )
  }

  # save prediction model specs
  openxlsx::write.xlsx(predict_model_specs, file = config[["predict_model_specs_path"]])

  # Load parameter grid
  param_grid <- readxl::read_excel(config[["param_grid_path"]], sheet = model_type)

  # create a duplicate parameter grid for prediction
  predict_param_grid <- data.frame(matrix(nrow = nrow(param_grid), ncol = ncol(param_grid)))
  colnames(predict_param_grid) <- colnames(param_grid)

  # fill grid
  for (i in colnames(param_grid)) {
    predict_param_grid[[i]] <- readline(prompt = paste0("Enter value for parameter ", i, ": "))
  }

  # save prediction parameter grid
  openxlsx::write.xlsx(
    predict_param_grid,
    file = config[["predict_param_grid_path"]],
    sheetName = model_type
  )
}
