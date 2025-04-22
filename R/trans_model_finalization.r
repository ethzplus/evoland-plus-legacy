#' Trans_model_finalization
#'
#' Re-fit LULCC transition models using optimal specifcation after calling
#' [lulcc.finalisemodelspecifications()]
#'
#' @param config A list of configuration parameters
#' @author Ben Black
#' @export

trans_model_finalization <- function(config = get_config()) {
  ### =========================================================================
  ### A- Preparation
  ### =========================================================================
  # Provide base folder paths for saving models for simulation
  model_base_folder <- config[["prediction_models_dir"]]
  ensure_dir(model_base_folder)

  # load table of model specifications
  models_specs <- readxl::read_excel(config[["predict_model_specs_path"]])

  # Filter for models already completed
  models_specs <- models_specs[models_specs$modelling_completed == "N", ]

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
    Data_period <- model_specs$data_period
    Model_type <- model_specs$model_type
    model_scale <- model_specs$model_scale
    Feature_selection_employed <- model_specs$feature_selection_employed
    Correct_balance <- model_specs$balance_adjustment

    message("Conducting modelling under specification ", model_specs$detail_model_tag)

    # finalise folder paths
    model_folder <- file.path(model_base_folder, Data_period)
    ensure_dir(model_folder)

    # Get file paths of transition datasets for period
    Data_paths_for_period <- if (Feature_selection_employed) {
      list.files(
        file.path(config[["trans_post_pred_filter_dir"]], Data_period),
        pattern = model_scale, full.names = TRUE
      )
    } else {
      list.files(
        file.path(config[["trans_post_pred_filter_dir"]], Data_period),
        pattern = model_scale, full.names = TRUE
      )
    }

    ### =========================================================================
    ### B- Performing modelling
    ### =========================================================================

    # Now opening loop over datasets
    Predict_model_paths <- furrr::future_map(
      .x = Data_paths_for_period,
      .options = furrr::furrr_options(seed = TRUE, scheduling = FALSE),
      .f = function(Dataset_path) {
        # load dataset
        Trans_dataset <- readRDS(Dataset_path)
        Trans_name <- stringr::str_remove_all(
          basename(Dataset_path),
          paste(c(".rds", paste0("_", model_scale)), collapse = "|")
        )

        ### =========================================================================
        ### B.1 - Attach model parameters
        ### =========================================================================

        # Attach  a list of model parameters('model_settings')
        # for each type of model specified in the parameter grid
        model_settings <- lulcc.setparams(
          transition_result = Trans_dataset$trans_result,
          covariate_names = names(Trans_dataset$cov_data),
          model_name = Model_type,
          # Parameter tuning grid (all possible combinations will be evaluated)
          param_grid = config[["predict_param_grid_path"]],
          weights = 1
        )

        Trans_dataset[["model_settings"]] <- model_settings
        rm(model_settings)

        ### =========================================================================
        ### B.2- Fit model
        ### =========================================================================

        Downsampling_bounds <- list(lower = 0.05, upper = 60)

        # Fit model
        mod <- try(lulcc.fitmodel(
          trans_result = Trans_dataset$trans_result, # transitions data
          cov_data = Trans_dataset$cov_data, # covariate data
          replicatetype = "none", # cross-validation strategy
          reps = 1, # Number of replicates
          mod_args = Trans_dataset$model_settings,
          path = NA,
          Downsampling = (
            Correct_balance &&
              Trans_dataset$imbalance_ratio <= Downsampling_bounds$lower |
              Trans_dataset$imbalance_ratio >= Downsampling_bounds$upper
          ) # utilise downsampling based on imbalance ratio
        ), TRUE) # Supply model arguments

        # extract only the part of the model that is needed (see below)
        model_extract <- mod@fits[["replicate_01"]][[1]]

        # save in final model location
        model_save_path <- paste0(
          model_folder, "/", Trans_name, ".", Data_period, ".", Model_type, ".rds"
        )
        saveRDS(model_extract, model_save_path)

        # return save path for model look up table
        return(model_save_path)
      }
    )

    ### =========================================================================
    ### B.3- Update model specification table to reflect that this specification
    ### of models is complete
    ### =========================================================================

    # load model spec table and replace the values in the 'completed' column
    model_spec_table <- readxl::read_excel(config[["predict_model_specs_path"]])

    # find the correct row
    model_spec_table$modelling_completed[
      model_spec_table$detail_model_tag == model_specs$detail_model_tag
    ] <- "Y"

    openxlsx::write.xlsx(
      model_spec_table,
      file = config[["predict_model_specs_path"]],
      overwrite = TRUE
    )

    message("Prediction model fitting finished \n")

    return(Predict_model_paths)
  } # close wrapper function

  # loop wrapper function over list of models
  # TODO why are we only fitting periods two and three?
  lapply(model_list[2:3], function(model) {
    lulcc.multispectransmodelling(model)
  })

  ### =========================================================================
  ### F- create a model look up table
  ### =========================================================================

  Model_periods <- unique(models_specs$data_period_name)

  # load list of viable transitions for each time period
  Viable_transitions_lists <- readRDS(config[["viable_transitions_lists"]])
  names(Viable_transitions_lists) <- Model_periods

  # create a df with info for each period
  Model_lookups <- lapply(Model_periods, function(x) {
    # Get file paths for models and uncertainty tables and convert to DF adding columns
    # for file_path, model_name, unc_table_path
    model_df <- data.frame(
      File_path = list.files(paste0(model_base_folder, "/", x), full.names = TRUE),
      # FIXME better not rely on the assumption that file order is deterministic
      Model_name = list.files(paste0(model_base_folder, "/", x), full.names = FALSE)
    )
    # Unc_table_path = list.files(paste0("Data/Uncertainty_tables/", x), full.names =
    # TRUE)) #adding uncertainty table as a list object if desired

    # model_region (split on 1st period)
    model_df$Region <- sapply(
      model_df$Model_name, function(x) stringr::str_split(x, "\\.")[[1]][1],
      simplify = TRUE
    )

    # Initial LULC class (split on 2nd period)
    model_df$Initial_LULC <- sapply(
      model_df$Model_name, function(x) stringr::str_split(x, "\\.")[[1]][2],
      simplify = TRUE
    )

    # Final LULC class (split on 3rd period)
    model_df$Final_LULC <- sapply(
      model_df$Model_name, function(x) stringr::str_split(x, "\\.")[[1]][3],
      simplify = TRUE
    )

    # transition names (concatenating Initial and Final_class)
    model_df$Trans_name <- sapply(
      model_df$Model_name, function(x) {
        Initial <- stringr::str_split(x, "\\.")[[1]][2]
        Final <- stringr::str_split(x, "\\.")[[1]][3]
        return(paste0(Initial, "_", Final))
      },
      simplify = TRUE
    )

    # Model_period (split on 4th period)
    model_df$Model_period <- sapply(
      model_df$Model_name, function(x) stringr::str_split(x, "\\.")[[1]][4],
      simplify = TRUE
    )

    # Model_type (split on 5th period)
    model_df$Model_type <- sapply(
      model_df$Model_name, function(x) stringr::str_split(x, "\\.")[[1]][5],
      simplify = TRUE
    )

    # reorder columns
    # add "Unc_table_path" to vector if included
    model_df <- model_df[, c(
      "Trans_name",
      "Region",
      "Initial_LULC",
      "Final_LULC",
      "Model_type",
      "Model_period",
      "Model_name",
      "File_path"
    )]

    # remove rows for persistence models which are not required by Dinamica
    model_df <- model_df[!model_df$Initial_LULC == model_df$Final_LULC, ]

    return(model_df)
  })
  names(Model_lookups) <- Model_periods
  # adding ID column using lists of viable transitions
  Model_lookups_with_ID <- mapply(
    function(Model_lookup, trans_table) {
      Model_lookup$Trans_ID <- sapply(
        Model_lookup$Trans_name,
        function(x) trans_table[trans_table$Trans_name == x, "Trans_ID"]
      )
      return(Model_lookup)
    },
    Model_lookup = Model_lookups,
    trans_table = Viable_transitions_lists,
    SIMPLIFY = FALSE
  )

  # save DFs for each periods as sheets in a xlsx.
  unlink(config[["model_lookup_path"]])
  mapply(
    function(model_table, model_name) {
      xlsx::write.xlsx(
        model_table,
        file = config[["model_lookup_path"]],
        sheet = model_name,
        row.names = FALSE,
        append = TRUE
      )
    },
    model_table = Model_lookups_with_ID,
    model_name = names(Model_lookups_with_ID)
  )
}
