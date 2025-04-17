#############################################################################
## Trans_modelling: Fit and evaluate models of LULC transitions
## under multiple model specifications
## The results comparing the performance of different transition model
## specifications require manual interpretation as the choice of optimal model
## must balance numerous aspects: accuracy, overfitting, computation time etc.
## Date: 08-04-2022
## Author: Ben Black
#############################################################################
transition_modelling <- function(config = get_config()) {
  ### =========================================================================
  ### A- Preparation
  ### =========================================================================

  ensure_dir(config[["transition_model_dir"]])
  ensure_dir(config[["transition_model_eval_dir"]])
  # load table of model specifications
  # Import model specifications table
  model_specs <- readxl::read_excel(config[["model_specs_path"]])

  # Filter out models already completed
  # TODO avoid using excel for recording state
  models_specs <- model_specs[model_specs$modelling_completed == "N", ]

  # split into named list
  model_list <- lapply(split(models_specs, seq_len(nrow(models_specs))), as.list)
  names(model_list) <- models_specs$detail_model_tag

  # Instantiate wrapper function over process of modelling prep, fitting,
  # evaluation, saving and completeness checking
  lulcc.multispectransmodelling <- function(model_specs) {
    # model_specs <- model_list[[1]]
    ### =========================================================================
    ### A- Prepare model specifications
    ### =========================================================================

    # vector model specifcations
    Data_period <- model_specs$data_period
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
      model_folder <- paste0(
        config[["transition_model_dir"]], "/",
        toupper(Model_type), "_models", "/",
        model_scale, "_", FS_string, "/"
      )
      eval_results_folder <- paste0(
        config[["transition_model_eval_dir"]], "/",
        toupper(Model_type), "_model_evaluation_downsampled", "/",
        model_scale, "_", FS_string, "/"
      )
    } else {
      model_folder <- paste0(
        config[["transition_model_dir"]], "/",
        toupper(Model_type), "_models_non_adjusted", "/",
        model_scale, "_", FS_string, "/"
      )
      eval_results_folder <- paste0(
        config[["transition_model_eval_dir"]], "/",
        toupper(Model_type), "_model_evaluation_non_adjusted", "/",
        model_scale, "_", FS_string, "/"
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
      .options = furrr::furrr_options(seed = TRUE),
      .f = function(Dataset_path) {
        # Dataset_path <- Data_paths_for_period[[1]]
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
      model_spec_table <- readxl::read_excel(config[["model_specs_path"]])

      # find the correct row
      model_spec_table$modelling_completed[
        model_spec_table$detail_model_tag == model_specs$detail_model_tag
      ] <- "Y"

      openxlsx::write.xlsx(
        model_spec_table,
        file = config[["model_specs_path"]],
        overwrite = TRUE
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
        model_spec_table <- readxl::read_excel(config[["model_specs_path"]])

        # find the correct row
        model_spec_table$Modelling_completed[
          model_spec_table$Detail_model_tag == model_specs$detail_model_tag
        ] <- "Y"

        # add a warning
        model_spec_table$Num_errors <- Num_errors

        # save
        openxlsx::write.xlsx(
          model_spec_table,
          file = config[["model_specs_path"]],
          overwrite = TRUE
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

  ### =========================================================================
  ### C- Extract model objects and save in seperate folder structure
  ### =========================================================================

  # This process requires the user to have checked the model evaluation results
  # and determined which is the preferred model specification to use moving forward
  # with the LULCC modelling i.e model spec with the best performance

  # This code section is to extract the model objects for the desired specification
  # and save them in a seperate location and at the same time create a model lookup
  # table to be used by Dinamica when predicting transition potential
  # at future time points

  # vector model periods
  Model_periods <- unique(model_specs$data_period)

  lapply(Model_periods, function(Model_period) {
    # paste together path
    Model_folder_path <- paste0(
      "Data/Fitted_models/RF_models/regionalized_filtered/Period_",
      Model_period, "_rf_models"
    )

    # list model file paths
    model_paths <- as.list(list.files(
      Model_folder_path,
      recursive = TRUE, full.names = TRUE
    ))

    # rename
    names_w_dir <- sapply(
      as.list(
        list.files(Model_folder_path, recursive = TRUE, full.names = FALSE)
      ),
      function(x) {
        stringr::str_remove_all(stringr::str_split(x, "/")[[1]][2], "_rf-1.rds")
      }
    )

    # load viable trans_list for period
    Initial_LULC_classes <- unique(
      readRDS(config[["viable_transitions_lists"]])[[Model_period]][["Initial_class"]]
    )
    Initial_LULC_classes <- paste0(Initial_LULC_classes, "_")

    # replacing the "_" between LULC classes with a '.'
    names_w_dir <- sapply(
      names_w_dir,
      function(name) {
        Initial_class <- Initial_LULC_classes[
          sapply(Initial_LULC_classes, function(class) {
            grepl(class, name)
          }, simplify = TRUE)
        ]
        new_name <- gsub("^\\_|\\_$", ".", Initial_class)
        gsub(Initial_class, new_name, name)
      }
    )

    names(model_paths) <- names_w_dir

    extract_save_model <- function(model_file_path, model_name) {
      # load model
      model_object <- readRDS(model_file_path)

      # extract one of the model objects
      model_extract <- model_object[["model"]]@fits[["replicate_01"]][["rf-1"]]

      # create a folder path based on time period
      folder_path <- paste0("Data/Fitted_models/", Model_period)
      dir.create(folder_path, recursive = TRUE)

      # expand to file path using model name
      file_path <- paste0(
        folder_path, "/", model_name, ".", Model_period, ".", "RF.rds"
      )

      # save the model
      saveRDS(model_extract, file = file_path)

      # return the predictors
    }

    mapply(extract_save_model,
      model_file_path = model_paths,
      model_name = names(model_paths)
    )
  }) # close loop over periods
}
