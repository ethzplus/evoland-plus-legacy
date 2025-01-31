#' lulcc.finalisemodelspecifications
#'
#' Enter optimal model specification and parameters for prediction
#'
#' @param model_specs_path Character, file path for table of model specifications
#' @param param_grid_path Character, file path for grid of hyperparameters
#' @author Ben Black
#' @export

lulcc.finalisemodelspecifications <- function(model_specs_path, param_grid_path, data_periods) {
  # Load model specifications
  model_specs <- readxl::read_excel(model_specs_path)

  # filter for completed model specifcations
  model_specs <- model_specs[model_specs$Modelling_completed == "Y", ]

  # Use prompts to get specifications as input from user
  model_type <- readline(prompt = paste0(
    "Choose model type from: ",
    paste(unique(model_specs$Model_type), sep = "or"),
    ", Enter type: "
  ))
  model_scale <- readline(prompt = paste0(
    "Choose model scale from: ",
    paste(unique(model_specs$model_scale), sep = "or"),
    ", Enter scale: "
  ))
  feature_selection_employed <- readline(
    prompt = "Use transitions datasets that have undergone feature selection (TRUE or FALSE): "
  )
  balance_adjustment <- if (model_type == "rf") {
    readline(prompt = "Use Tree-wise downsampling to address class imbalance: ")
  } else {
    "FALSE"
  }

  # Subset model specifications by user inputs
  predict_model_specs <- model_specs[
    model_specs$Model_type == model_type &
      model_specs$model_scale == model_scale &
      model_specs$Feature_selection_employed == feature_selection_employed &
      model_specs$Balance_adjustment == balance_adjustment,
  ]


  # change completion values to 'N'
  predict_model_specs$Modelling_completed <- "N"

  # Print a warning that not all data periods are included in the predict model specs table
  if (all(data_periods %in% unique(predict_model_specs$data_period_name)) == FALSE) {
    cat(paste0(
      "Warning the data period/s: ",
      paste(
        data_periods[which(!(data_periods %in% unique(predict_model_specs$data_period_name)))],
        sep = ","
      ),
      " do not have models of the desired specification completed for them
           (i.e. Errors may have occured in model fitting or evaluation,
           please check before continuing"
    ))
  }

  # save prediction model specs
  openxlsx::write.xlsx(predict_model_specs, file = "tools/predict_model_specs.xlsx")

  # Load parameter grid
  param_grid <- readxl::read_excel(param_grid_path, sheet = model_type)

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
    file = "tools/predict_param-grid.xlsx",
    sheetName = model_type
  )
}
