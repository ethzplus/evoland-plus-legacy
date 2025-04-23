#' Transition Modeling for Land Use Land Cover Change
#'
#' This function coordinates the modeling and evaluation of land use and land cover
#' change (LULCC) transitions based on configurations specified in a model specification
#' table. It handles model preparation, fitting, evaluation, and state tracking.
#'
#' @author Ben Black
#'
#' @param config A list containing configuration parameters
#' @param ignore_excel_state Logical, whether to ignore the compeltion state in the Excel file
#'
#' @details
#' The function performs the following steps:
#' 1. Creates necessary directories for models and evaluation results
#' 2. Loads model specifications from an Excel file
#' 3. Filters out models that have already been completed
#' 4. For each model specification:
#'    - Prepares model settings based on configuration (balanced/unbalanced, filtered/unfiltered)
#'    - Loads appropriate transition datasets
#'    - Sets model parameters via `lulcc.setparams()`
#'    - Fits, evaluates, and saves models via `lulcc.fitevalsave()`
#'    - Updates the model specification table to mark completed models
#'    - Handles and reports errors that occur during modeling
#'
#' @note
#' - Models are processed in parallel using the `furrr` package
#' - The function tracks model completion status in an Excel file
#' - Error handling includes saving diagnostic information for failed models
#'
#' @return No explicit return value, operates through side effects (writing files)
#'
#' @export

transition_modelling <- function(config = get_config(), ignore_excel_state = FALSE) {
  ### =========================================================================
  ### A- Preparation
  ### =========================================================================

  ensure_dir(config[["transition_model_dir"]])
  ensure_dir(config[["transition_model_eval_dir"]])

  models_specs <- readr::read_csv(config[["model_specs_path"]])

  if (!ignore_excel_state) {
    # TODO avoid using excel for recording state
    models_specs <- dplyr::filter(models_specs, modelling_completed == "N")
  }

  # split into named list
  model_list <- lapply(split(models_specs, seq_len(nrow(models_specs))), as.list)
  names(model_list) <- models_specs$detail_model_tag

  # Instantiate wrapper function over process of modelling prep, fitting,
  # evaluation, saving and completeness checking
  lulcc.multispectransmodelling <- function(model_specs) {
    ### =========================================================================
    ### A- Prepare model specifications
    ### =========================================================================

    # vector model specifcations
    Data_period <- model_specs$data_period_name
    Model_type <- model_specs$model_type
    model_scale <- model_specs$model_scale
    Feature_selection_employed <- model_specs$feature_selection_employed
    Correct_balance <- model_specs$balance_adjustment

    message(
      "Conducting modelling under specification `", model_specs$detail_model_tag, "`"
    )

    # finalise folder paths
    FS_string <- ifelse(Feature_selection_employed, "filtered", "unfiltered")

    if (Correct_balance) {
      model_folder <- file.path(
        config[["transition_model_dir"]],
        paste0(toupper(Model_type), "_models"),
        paste0(model_scale, "_", FS_string)
      )
      eval_results_folder <- file.path(
        config[["transition_model_eval_dir"]],
        paste0(toupper(Model_type), "_model_evaluation_downsampled"),
        paste0(model_scale, "_", FS_string)
      )
    } else {
      model_folder <- file.path(
        config[["transition_model_dir"]],
        paste0(toupper(Model_type), "_models_non_adjusted"),
        paste0(model_scale, "_", FS_string)
      )
      eval_results_folder <- file.path(
        config[["transition_model_eval_dir"]],
        paste0(toupper(Model_type), "_model_evaluation_non_adjusted"),
        paste0(model_scale, "_", FS_string)
      )
    }

    # Get file paths of transition datasets for period
    Data_paths_for_period <-
      if (Feature_selection_employed) {
        list.files(file.path(
          config[["trans_post_pred_filter_dir"]], Data_period
        ), pattern = model_scale, full.names = TRUE)
      } else {
        list.files(file.path(
          config[["trans_pre_pred_filter_dir"]], Data_period
        ), pattern = model_scale, full.names = TRUE)
      }

    names(Data_paths_for_period) <- sapply(
      Data_paths_for_period, function(x) tools::file_path_sans_ext(basename(x))
    )

    ### =========================================================================
    ### B- Performing modelling
    ### =========================================================================

    # Now opening loop over datasets
    Modelling_outputs <- furrr::future_map(
      .x = Data_paths_for_period,
      .options = furrr::furrr_options(seed = TRUE, scheduling = FALSE),
      .f = function(Dataset_path) {
        message("Modelling transition: ", stringr::str_remove(basename(Dataset_path), ".rds"))

        # load dataset
        Trans_dataset <- readRDS(Dataset_path)
        Trans_name <- stringr::str_remove(basename(Dataset_path), ".rds")

        ### =========================================================================
        ### B.1 - Attach model parameters
        ### =========================================================================

        # Attach  a list of model parameters('model_settings')
        # for each type of model specifcied in the parameter grid
        Trans_dataset[["model_settings"]] <- lulcc.setparams(
          transition_result = Trans_dataset$trans_result,
          covariate_names = names(Trans_dataset$cov_data),
          model_name = Model_type,
          # Parameter tuning grid (all possible combinations will be evaluated)
          param_grid = config[["param_grid_path"]],
          weights = 1
        )

        message("Modelling parameters defined")

        ### =========================================================================
        ### B.2- Fit, evaluate and save models
        ### =========================================================================

        # Apply function for fitting and evaluating models and saving results
        # the output returned is a list of errors caught by try()
        Trans_model_capture <- lulcc.fitevalsave(
          Transition_dataset = Trans_dataset,
          Transition_name = Trans_name,
          replicatetype = "splitsample",
          reps = 5,
          balance_class = Correct_balance,
          Downsampling_bounds = list(lower = 0.05, upper = 60),
          Data_period = Data_period,
          model_folder = model_folder,
          eval_results_folder = eval_results_folder,
          Model_type = Model_type
        )

        gc()
        return(Trans_model_capture)
      }
    ) # close loop over trnasition datasets

    ### =========================================================================
    ### B.3- Update model specification table to reflect that this specification
    ### of models is complete
    ### =========================================================================

    # check for failures in the Modelling outputs
    Modelling_check <- unlist(Modelling_outputs)

    if (all(Modelling_check == "Success")) {
      # load model spec table and replace the values in the 'completed' column
      model_spec_table <- readr::read_csv(config[["model_specs_path"]])

      # find the correct row
      model_spec_table$modelling_completed[
        model_spec_table$detail_model_tag == model_specs$detail_model_tag
      ] <- "Y"

      readr::write_csv(
        model_spec_table,
        file = config[["model_specs_path"]]
      )

      message(
        "Model fitting and evaluation for:",
        model_specs$detail_model_tag, "completed without errors"
      )
    } else {
      # count number of errors
      Num_errors <- length(Modelling_check[Modelling_check != "Success"])

      # print error message
      warning(
        Num_errors,
        " errors occurred in model fitting and evaluation for the model specification: \n",
        model_specs$detail_model_tag,
        "\n please consult saved modelling output file: \n",
        paste0(
          eval_results_folder,
          model_specs$detail_model_tag,
          "_modelling_output_summary.rds"
        )
      )

      if (Num_errors < 10) {
        message(
          "As the number of errors was <10 the model spec table has been updated
           to indicate that this specification has been completed, the likely cause
           of error is transition dataset with insufficient number of transition
           instances (1's) or where feature selection has reduced to a single predictor variable"
        )
        # load model spec table and replace the values in the 'completed' column
        model_spec_table <- readr::read_csv(config[["model_specs_path"]])

        # find the correct row
        model_spec_table$Modelling_completed[
          model_spec_table$Detail_model_tag == model_specs$detail_model_tag
        ] <- "Y"

        # add a warning
        model_spec_table$Num_errors <- Num_errors

        # save
        readr::write_csv(
          model_spec_table,
          file = config[["model_specs_path"]]
        )
      }


      # save modelling outputs for user inspection
      saveRDS(
        Modelling_outputs,
        paste0(
          eval_results_folder,
          model_specs$detail_model_tag, "_modelling_output_summary.rds"
        )
      )
    }
  } # close wrapper function

  # loop wrapper function over list of models
  lapply(model_list, function(model) {
    lulcc.multispectransmodelling(model)
  })
}
