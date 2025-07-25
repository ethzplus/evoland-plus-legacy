#' dinamica_trans_potent_calc
#'
#' Calculates the transition potential for land use/land cover (LULC) changes using
#' fitted statistical models. Supports both calibration and simulation modes, and can
#' optionally apply scenario-specific spatial interventions. Generates dynamic
#' predictors (e.g., population, neighbourhood), predicts transition probabilities for
#' each possible LULC transition, rescales probabilities, applies interventions if
#' required, and saves the resulting probability maps as raster files.
#'
#' @param config A list containing configuration parameters and file paths. Default:
#' result of `get_config()`.
#' @param simulation_num Integer. Simulation ID number to select simulation parameters.
#' @param time_step Integer. The current time step for which transition potentials are
#' calculated.
#'
#' @details
#' 1. Loads simulation parameters and configuration.
#' 1. Loads current LULC raster and layerizes it by class.
#' 1. Loads suitability and accessibility predictors, adapting to calibration or simulation mode.
#' 1. In simulation mode, generates dynamic predictors such as municipal population and neighbourhood statistics.
#' 1. Stacks all predictors and converts them to a dataframe for model prediction.
#' 1. Loads transition models and predicts transition probabilities for each transition and region.
#' 1. Rescales probabilities to ensure they sum to 1 for each cell.
#' 1. Optionally applies scenario-specific spatial interventions to the probabilities.
#' 1. Saves probability maps for each transition as raster files.
#'
#' @author Ben Black, Carlson Büth
#'
#' @seealso [lulcc.spatprobmanipulation()]
#'
#' @export

dinamica_trans_potent_calc <- function(
    config = get_config(),
    simulation_num = integer(),
    time_step = integer()) {
  # params <- get_simulation_params(
  #   ctrl_tbl_path = "2025-07-25_08h58m07s/simulation_control.csv",
  #   simulation_id = 1
  # )
  params <- get_simulation_params(simulation_id = simulation_num)

  if (grepl("y", params[["parallel_tpc.string"]], ignore.case = TRUE)) {
    stop("Removed support for parallelisation at this stage")
  }

  # implement spatial interventions
  Use_interventions <- params[["spatial_interventions.string"]]

  # check normalisation of transition probabilities
  Check_normalisation <- FALSE

  cat(paste0(
    "Starting transition potential calculation for ", params[["model_mode.string"]], ": ",
    params[["simulation_id.string"]], ", with scenario: ", params[["scenario_id.string"]],
    ", at time step: ", time_step, "\n"
  ))
  cat(paste0(" - loading maps from: ", params[["sim_results_path"]], "\n"))

  # Convert model mode into a string of the dates calibration period being used
  # this makes it easier to load files because they use this nomenclature

  calibration_periods <- unique(readr::read_csv(config[["model_specs_path"]])[["data_period_name"]])

  calibration_dates <- lapply(calibration_periods, function(period) {
    as.numeric(stringr::str_split(period, "_")[[1]])
  })

  period_tag <- if (grepl("calibration", params[["model_mode.string"]], ignore.case = TRUE)) {
    Period_log <- sapply(calibration_dates, function(x) {
      if (time_step > x[1] & time_step <= x[2]) {
        TRUE
      } else {
        FALSE
      }
    })
    if (all(Period_log == FALSE) == TRUE) {
      # find the closet year
      closest_year <- unlist(calibration_dates)[
        which.min(abs(unlist(calibration_dates) - time_step))
      ]
      # match year to period name
      Period_log <- calibration_periods[grepl(paste0(closest_year), calibration_periods)]
    } else {
      calibration_periods[Period_log == TRUE]
    }
  } else if (grepl("simulation", params[["model_mode.string"]], ignore.case = TRUE)) {
    calibration_periods[length(calibration_periods)]
  }
  # The last clause covers when calibration is occuring between 2009 and 2018
  # and when the model is in simulation mode

  # create folder for saving prediction probability maps
  prob_map_folder <-
    file.path("results", "pred_prob_maps", params[["simulation_id.string"]], time_step) |>
    ensure_dir()

  cat(paste0(" - creating directory for saving probability maps: ", prob_map_folder, "\n"))

  # B- Retrieve current LULC map and layerize ####

  # replace Dinamica escape character in file path with current time step
  # TODO this seems sketchy, because the logic at create_init_lulc_raster() constructs
  # the file path differently
  current_LULC_path <- file.path(
    params[["sim_results_path"]],
    paste0(time_step, ".tif")
  )

  # load current LULC map
  Current_LULC <- raster::raster(current_LULC_path)

  # load aggregation scheme
  LULC_rat <-
    config[["LULC_aggregation_path"]] |>
    readxl::read_excel() |>
    dplyr::select(tidyselect::all_of(c("Class_abbreviation", "Aggregated_ID"))) |>
    dplyr::distinct()

  cat(paste0("Layerizing current LULC map: ", current_LULC_path, "\n"))

  # layerize data (columns for each LULC class)
  LULC_data <- raster::layerize(Current_LULC)
  names(LULC_data) <- sapply(stringr::str_remove_all(names(LULC_data), "X"), function(y) {
    LULC_rat[LULC_rat$Aggregated_ID == y, "Class_abbreviation"]
  })

  # C- Load Suitability and Accessibility predictors ####

  # use model mode string to select correct folder

  # For calibration mode (matching on Period_tag)
  if (grepl("calibration", params[["model_mode.string"]], ignore.case = TRUE)) {
    cat("--- CALIBRATION MODE --- \n")
    # load sheet of predictor table for time point
    pred_details <- openxlsx::read.xlsx(config[["pred_table_path"]], sheet = paste(period_tag))

    # load layers as raster::stack
    SA_pred_stack <- raster::stack(pred_details$Prepared_data_path)

    # name layers in stack
    names(SA_pred_stack@layers) <- pred_details$Covariate_ID
  } # close if statement calibration

  # For simulation mode
  if (grepl("simulation", params[["model_mode.string"]], ignore.case = TRUE)) {
    cat("--- SIMULATION MODE --- \n")

    # load sheet of predictor table for time point
    pred_details <- openxlsx::read.xlsx(config[["pred_table_path"]], sheet = paste(time_step))

    # convert scenario column back to character vectors
    pred_details$Scenario_variant <- sapply(
      pred_details$Scenario_variant,
      function(x) unlist(strsplit(x, ","))
    )

    # first seperate static variables scenario = "All""
    preds_static <- pred_details[pred_details$Scenario_variant == "All", ]

    # then those relevant for the scenario variant
    # first the climate predictors based on the RCP designation
    preds_climate <- pred_details[grep(
      params[["climate_scenario.string"]],
      pred_details$Scenario_variant
    ), ]

    # Then the economic predictors
    preds_economic <- pred_details[grep(
      params[["econ_scenario.string"]],
      pred_details$Scenario_variant
    ), ]

    # bind static and scenario specific preds
    preds_scenario <- rbind(preds_static, preds_climate, preds_economic)

    # load layers as raster::stack
    SA_pred_stack <- raster::stack(preds_scenario$Prepared_data_path)

    # name layers in stack
    names(SA_pred_stack@layers) <- preds_scenario$Covariate_ID
  } # close simulation if statement

  cat(paste0("Loaded Suitability and Accessibility predictors \n"))

  # E- Generate dynamic predictors ####

  if (grepl("simulation", params[["model_mode.string"]], ignore.case = TRUE)) {
    # E.1- Dynamic predictors:  Municipal Population ####
    # for calibration the raster stacks already contain the dynamic predictor layers so there
    # is nothing to be done
    cat("Generating dynamic predictors: \n - Municipal population \n")

    # create population data layer
    # subset current LULC to just urban cells
    Urban_rast <- Current_LULC == 10

    # load canton shapefile
    Canton_shp <- raster::shapefile(
      "Data/Preds/Raw/CH_geoms/SHAPEFILE_LV95_LN02/swissBOUNDARIES3D_1_3_TLM_KANTONSGEBIET.shp"
    )

    # Zonal stats to get urban area per kanton
    Canton_urban_areas <- raster::extract(
      Urban_rast, Canton_shp,
      fun = sum, na.rm = TRUE, df = TRUE
    )

    # append Kanton ID
    Canton_urban_areas$Canton_num <- Canton_shp$KANTONSNUM

    # combine areas for cantons with multiple polygons
    Canton_urban_areas <-
      Canton_urban_areas |>
      dplyr::group_by(Canton_num) |>
      dplyr::summarise(dplyr::across(layer, sum))

    # load the municipality shape file
    Muni_shp <- raster::shapefile(
      "Data/Preds/Raw/CH_geoms/SHAPEFILE_LV95_LN02/swissBOUNDARIES3D_1_3_TLM_HOHEITSGEBIET.shp"
    )

    # filter out non-swiss municipalities
    Muni_shp <- Muni_shp[
      Muni_shp@data$ICC == "CH" & Muni_shp@data$OBJEKTART == "Gemeindegebiet",
    ]

    # Zonal stats to get number of Urban cells per Municipality polygon
    # sum is used as a function because urban cells = 1 all others = 0
    Muni_urban_areas <- raster::extract(Urban_rast, Muni_shp, fun = sum, na.rm = TRUE, df = TRUE)

    # append Kanton and Municipality IDs
    Muni_urban_areas$Canton_num <- Muni_shp@data[["KANTONSNUM"]]
    Muni_urban_areas$Muni_num <- Muni_shp$BFS_NUMMER
    Muni_urban_areas$Perc_urban <- 0

    # loop over kanton numbers and calculate municipality urban areas as a % of canton urban area
    for (i in Canton_urban_areas$Canton_num) {
      # vector kanton urban area
      Can_urban_area <- as.numeric(Canton_urban_areas[Canton_urban_areas$Canton_num == i, "layer"])

      # subset municipalities to this canton number
      munis_indices <- which(Muni_urban_areas$Canton_num == i)

      # loop over municipalities in the Kanton and calculate their urban areas as a % of
      # the Canton's total
      for (muni in munis_indices) {
        Muni_urban_areas$Perc_urban[muni] <- (
          Muni_urban_areas[muni, "layer"] / Can_urban_area
        ) * 100
      } # close inner loop
    } # close outer loop

    # estimate % of predicted cantonal population per municipality
    # Load list of cantonal population models
    pop_models <- readRDS(
      file.path(config[["preds_tools_dir"]], "dynamic_pop_models.rds")
    )

    # add predicted % pop results column
    Muni_urban_areas$Perc_pop <- 0

    # estimate % of pop per municipality
    # loop over unique polygon IDs rather than BFS numbers to take into account that
    # multiple polygons have the same BFS number
    for (i in Muni_urban_areas$ID) {
      # seperate canton specific model
      canton_model <- pop_models[[Muni_urban_areas[Muni_urban_areas$ID == i, "Canton_num"]]]

      # perform prediction
      Muni_urban_areas$Perc_pop[[i]] <- predict(
        canton_model,
        newdata = Muni_urban_areas[Muni_urban_areas$ID == i, ]
      )
    }

    # load correct sheet of future population predictions according to scenario
    Pop_prediction_table <- openxlsx::read.xlsx(
      "Data/Preds/Tools/Population_projections.xlsx",
      sheet = params[["pop_scenario.string"]]
    )

    # loop over unique kanton numbers, rescale the predicted population percentages
    # and calculate the estimated population per municipality as a % of the cantonal total

    # add results column
    Muni_urban_areas$Pop_est <- 0

    for (i in unique(Muni_urban_areas$Canton_num)) {
      # subset to predicted cantonal population percentages
      Canton_dat <- Muni_urban_areas[Muni_urban_areas$Canton_num == i, "Perc_urban"]

      # loop over the municipalites re-scaling the values
      Canton_preds_rescaled <- sapply(Canton_dat, function(y) {
        value <- y * 1 / sum(Canton_dat)
        # dividing by Zero introduces NA's so these must be converted back to zero
        value[is.na(value)] <- 0
        value
      }) # close inner loop

      # get the projected canton population value for this time point
      Canton_pop <- Pop_prediction_table[Pop_prediction_table$Canton_num == i, paste(time_step)]

      # loop over the rescaled values calculating the estimated population
      Muni_indices <- which(Muni_urban_areas$Canton_num == i)
      Muni_urban_areas$Pop_est[Muni_indices] <- sapply(Canton_preds_rescaled, function(x) {
        Canton_pop * x # % already expressed as decimal so no need to /100
      }) # close loop over municipalities
    } # close loop over cantons

    # add estimated population to @data table of polygons and then rasterize
    Muni_shp@data$Pop_est <- Muni_urban_areas$Pop_est
    Ref_grid <- raster::raster(config[["ref_grid_path"]])
    pop_raster <- raster::rasterize(
      x = Muni_shp,
      y = Ref_grid,
      field = "Pop_est",
      background = raster::NAvalue(Ref_grid)
    )

    # TODO: THIS MUST BE THE LAYER NAME IN THE CALIBRATION STACKS/MODELS
    names(pop_raster) <- "Muni_pop"

    # clean up
    rm(
      Canton_shp, Canton_urban_areas, Can_urban_area, canton_model, Muni_shp,
      Muni_urban_areas, munis_indices, pop_models, Pop_prediction_table,
      Urban_rast
    )

    # E.2- Dynamic predictors: Neighbourhood predictors ####

    cat(" - Neighbourhood predictors \n")

    # load matrices used to create focal layers
    Focal_matrices <- unlist(
      readRDS(
        "Data/Preds/Tools/Neighbourhood_matrices/ALL_matrices"
      ),
      recursive = FALSE
    )

    # adjust matrix names
    names(Focal_matrices) <- sapply(names(Focal_matrices), function(x) {
      stringr::str_split(x, "[.]")[[1]][2]
    })

    # Load details of focal layers required for the model set being utilised
    Required_focals_details <- readRDS(list.files(
      "Data/Preds/Tools/Neighbourhood_details_for_dynamic_updating",
      pattern = period_tag,
      full.names = TRUE
    ))

    # Loop over details of focal layers required creating a list of rasters from the
    # current LULC map
    Nhood_rasters <- list()
    for (i in seq_len(nrow(Required_focals_details))) {
      # vector active class names
      Active_class_name <- Required_focals_details[i, ]$active_lulc

      # get pixel values of active LULC class
      Active_class_value <- unlist(
        LULC_rat[LULC_rat$Class_abbreviation == Active_class_name, "Aggregated_ID"]
      )

      # subset LULC raster by all Active_class_value
      Active_class_raster_subset <- Current_LULC == Active_class_value

      # create focal layer using matrix
      Focal_layer <- raster::focal(
        x = Active_class_raster_subset,
        w = Focal_matrices[[Required_focals_details[i, ]$matrix_id]],
        na.rm = FALSE,
        pad = TRUE,
        padValue = 0,
        NAonly = FALSE
      )

      # create file path for saving this layer
      Focal_name <- paste(
        Active_class_name, "nhood",
        Required_focals_details[i, ]$matrix_id,
        sep = "_"
      )
      Nhood_rasters[[Focal_name]] <- Focal_layer
    }

    rm(
      Focal_matrices, Focal_layer, Focal_name, Required_focals_details,
      Active_class_raster_subset, Active_class_name, Active_class_value
    )
  } # close if statement for dynamic predictor prep

  # F- Combine LULC, SA_preds and Nhood_preds and extract to dataframe ####

  cat("Stacking LULC, SA_preds and Nhood_preds \n")

  # Stack all rasters
  # For calibration mode
  {
    if (grepl("calibration", params[["model_mode.string"]], ignore.case = TRUE)) {
      Trans_data_stack <- stack(LULC_data, SA_pred_stack)
      names(Trans_data_stack) <- c(names(LULC_data), names(SA_pred_stack@layers))
    } # close if statement calibration

    # For simulation mode
    # only stack the Nhood_rasters here because otherwise they were not included
    # in the upper stack function
    else if (grepl("simulation", params[["model_mode.string"]], ignore.case = TRUE)) {
      # load the raster of Bioregions
      Bioregion_rast <- raster::raster("Data/Bioreg_CH/Bioreg_raster.gri")
      names(Bioregion_rast) <- "Bioregion"
      Trans_data_stack <- raster::stack(
        LULC_data,
        SA_pred_stack,
        pop_raster,
        raster::stack(Nhood_rasters),
        Bioregion_rast
      )
      cat(" - Stacked all layers \n")
      names(Trans_data_stack) <- c(
        names(LULC_data),
        names(SA_pred_stack@layers),
        names(pop_raster),
        names(Nhood_rasters),
        names(Bioregion_rast)
      )
      cat(" - Renamed layers \n")
    } # close simulation if statement

    else {
      stop("Model mode not recognised!")
    }
  }

  # Convert Rasterstack to dataframe, because the LULC and Region layers have attribute
  # tables the function creates two columns for each: Pixel value and class name
  Trans_dataset <- raster::as.data.frame(Trans_data_stack)

  cat(" - Converted raster stack to dataframe \n")

  # add ID column to dataset
  Trans_dataset$ID <- row.names(Trans_dataset)

  # Get XY coordinates of cells
  xy_coordinates <- raster::coordinates(Trans_data_stack)

  # cbind XY coordinates to dataframe and seperate rows where all values = NA
  Trans_dataset <- cbind(Trans_dataset, xy_coordinates)

  # release memory
  rm(LULC_data, SA_pred_stack, Nhood_rasters, Trans_data_stack, xy_coordinates)

  # G- Run transition potential prediction for each transition ####

  cat("Running transition potential prediction for each transition \n")

  # load model look up
  Model_lookup <- xlsx::read.xlsx(
    config[["model_lookup_path"]],
    sheetName = period_tag
  )
  cat(" - Loaded model lookup table \n")

  # if statement to remove transitions if they are being implemented deterministically
  if (grepl("simulation", params[["model_mode.string"]], ignore.case = TRUE) &
    grepl("Y", params[["deterministic_trans.string"]], ignore.case = TRUE)) {
    # remove transitions with initial class == glacier
    cat(" - Removing transitions with initial class == glacier \n")
    Model_lookup <- Model_lookup[Model_lookup$Initial_LULC != "Glacier", ]
  } # close if statement

  # seperate Trans_dataset into complete cases for prediction and NAs
  complete_cases <- complete.cases(Trans_dataset)
  Trans_dataset_complete <- Trans_dataset[complete_cases, ]
  row.names(Trans_dataset_complete) <- Trans_dataset_complete$ID

  Trans_dataset_na <- Trans_dataset_na[!complete_cases, c("ID", "x", "y")]
  row.names(Trans_dataset_na) <- Trans_dataset_na$ID

  rm(Trans_dataset)

  # seperate ID, x/y and Initial_LULC cols to append results to
  Prediction_probs <- Trans_dataset_complete[
    ,
    c("ID", "x", "y", unique(Model_lookup$Initial_LULC), "Static")
  ]
  row.names(Prediction_probs) <- Prediction_probs$ID

  # add cols to both the complete and NA data to capture predict probabilities to each class
  Final_LULC_classes <- unique(Model_lookup$Final_LULC)
  for (i in Final_LULC_classes) {
    Prediction_probs[[paste0("Prob_", i)]] <- 0
    Trans_dataset_na[[paste0("Prob_", i)]] <- NA
  }

  cat(" - Created dataframe for storing prediction probabilities \n")

  {
    # Non_parallel TPC calculation:
    Non_par_start <- Sys.time()
    # loop over transitions
    for (i in seq_len(Model_lookup)) {
      Trans_ID <- Model_lookup[i, "Trans_ID"]
      Region <- Model_lookup[i, "Region"]
      Final_LULC <- Model_lookup[i, "Final_LULC"]
      Initial_LULC <- Model_lookup[i, "Initial_LULC"]

      # detailed status message
      # cat(paste0(" - predicting probabilities for transitions from ", Initial_LULC,
      #            " to ", Final_LULC, " within region: ", Region, "\n"))

      # load model
      Fitted_model <- readRDS(Model_lookup[i, "File_path"])

      # subset data for prediction
      pred_data <- Trans_dataset_complete[
        Trans_dataset_complete[Initial_LULC] == 1 &
          Trans_dataset_complete$Bioregion_Class_Names == Region,
      ]

      # predict using fitted model
      prob_predicts <- as.data.frame(predict(Fitted_model, pred_data, type = "prob"))
      names(prob_predicts)[[2]] <- paste0("Prob_", Final_LULC)

      # alternative method of replacing prob prediction values
      Prediction_probs[
        row.names(prob_predicts),
        paste0("Prob_", Final_LULC)
      ] <- prob_predicts[paste0("Prob_", Final_LULC)]
    } # close loop over Models
    Non_par_end <- Sys.time()
    Non_par_time <- Non_par_end - Non_par_start # sequential time = 2.937131 mins
    message(" - completed transition potential prediction in ", Non_par_time)
  } # Close non-parallel TPC chunk

  # G- Re-scale predictions ####

  cat("Re-scaling transition probabilities \n")

  # loop over rows and re-scale probability values so that they sum to 1
  # (by dividing by multiplying by 1 and then dividing by the sum of the vector)

  # Transition probability columns to re-scale
  Pred_prob_columns <- grep("Prob_", names(Prediction_probs), value = TRUE)

  # vector row indices with non-zero sums of transition probabilities
  Non_zero_indices <- which(rowSums(Prediction_probs[, Pred_prob_columns]) > 1)

  # Loop over rows performing re-scaling
  Prediction_probs[Non_zero_indices, Pred_prob_columns] <- as.data.frame(
    t(apply(Prediction_probs[Non_zero_indices, Pred_prob_columns],
      MARGIN = 1,
      FUN = function(x) {
        sapply(x, function(y) {
          value <- y * 1 / sum(x)
          # dividing by Zero introduces NA's so these must be converted back to zero
          value[is.na(value)] <- 0
          return(value)
        })
      }
    ))
  )

  # bind with background values
  Trans_dataset_na[setdiff(names(Prediction_probs), names(Trans_dataset_na))] <- NA
  Raster_prob_values <- rbind(Prediction_probs, Trans_dataset_na)

  # sort by ID
  Raster_prob_values <- Raster_prob_values[order(as.numeric(row.names(Raster_prob_values))), ]

  # FIXME is this commented out because it's not actually needed?
  # Save one copy of the raster probability values to be used to test
  # spatial interventions, this file will be created during the running of the model
  # to calibrate the Dinamica allocation parameters.
  # if (file.exists("Data/Exemplar_data/EXP_raster_prob_values.rds") == FALSE) {
  #   dir.create("Data/Exemplar_data")
  #   saveRDS(Raster_prob_values, "Data/Exemplar_data/EXP_raster_prob_values.rds")
  # }

  # H- Spatial manipulations of transition probabilities ####

  if (grepl("simulation", params[["model_mode.string"]], ignore.case = TRUE)) {
    # If statement to implement spatial interventions
    if (Use_interventions == "Y") {
      cat("Implementing spatial interventions \n")

      # load table of scenario interventions
      Interventions <- Interventions <- read.csv(config[["spat_ints_path"]])

      # Use function to perform manipulation of spatial transition probabilities
      # according to scenario-specific interventions
      Raster_prob_values <- lulcc.spatprobmanipulation(
        Interventions = Interventions,
        scenario_id = params[["scenario_id.string"]],
        Raster_prob_values = Raster_prob_values,
        Simulation_time_step = paste(time_step)
      )
    } # close if statement for spatial interventions
  } # close simulation if statement

  # I- Final rescaling ####

  cat("Performing final re-scaling \n")

  # vector row indices with non-zero sums of transition probabilities
  Non_zero_indices <- which(rowSums(Raster_prob_values[, Pred_prob_columns]) > 1)

  # Loop over rows performing re-scaling
  Raster_prob_values[Non_zero_indices, Pred_prob_columns] <- as.data.frame(
    t(apply(
      Raster_prob_values[Non_zero_indices, Pred_prob_columns],
      MARGIN = 1,
      FUN = function(x) {
        sapply(x, function(y) {
          value <- y * 1 / sum(x)
          # dividing by Zero introduces NA's so these must be converted back to zero
          value[is.na(value)] <- 0
          return(value)
        })
      }
    ))
  )

  # J- Save transition rasters ####

  cat("Saving transition rasters \n")

  # subset model_lookup table to unique trans ID
  Unique_trans <- Model_lookup[!duplicated(Model_lookup$Trans_ID), ]

  # Loop over unique trans using details to subset data and save Rasters
  for (i in seq_len(nrow(Unique_trans))) {
    Trans_ID <- Unique_trans[i, "Trans_ID"]
    cat(paste0(" - Preparing layer ", Trans_ID, "\n"))
    Final_LULC <- Unique_trans[i, "Final_LULC"]
    Initial_LULC <- Unique_trans[i, "Initial_LULC"]

    # get indices of non_class cells
    non_initial_indices <- na.omit(Raster_prob_values[Raster_prob_values[Initial_LULC] == 0, "ID"])

    # seperate Final class column
    Trans_raster_values <- Raster_prob_values[, c("ID", "x", "y", paste0("Prob_", Final_LULC))]

    # replace values of non-class cells with 0
    Trans_raster_values[non_initial_indices, paste0("Prob_", Final_LULC)] <- 0

    if (Check_normalisation) {
      # check that are Prob_ values are in [0, 1[ - otherwise warn
      for (col_name in paste0("Prob_", Final_LULC)) {
        col <- Trans_raster_values[, col_name]
        breaking <- FALSE
        if (any(col[is.finite(col)] < 0 | col[is.finite(col)] >= 1)) {
          # Raise warning for values outside [0, 1)
          warning("Raster warning: Probabilities (excluding NAs) are not in [0, 1[.")
          breaking <- TRUE
        } else if (any(is.na(col))) {
          # Raise warning for NAs
          warning("Raster warning: Probabilities contain NA values.")
          breaking <- TRUE
        }
        if (breaking) {
          break # Stop checking further columns
        }
      }
    }

    # rasterize and save using Initial and Final class names
    Prob_raster <- raster::rasterFromXYZ(
      Trans_raster_values[, c("x", "y", paste0("Prob_", Final_LULC))],
      crs = raster::crs(Current_LULC)
    )

    # vector file path for saving probability maps
    prob_map_path <- file.path(
      prob_map_folder,
      paste0(Trans_ID, "_probability_", Initial_LULC, "_to_", Final_LULC, ".tif")
    )

    raster::writeRaster(Prob_raster, prob_map_path, overwrite = TRUE)
  } # close loop over transitions

  # Return the probability map folder path as a string to
  # Dinamica to indicate completion
  # Note strings must be vectorized for 'outputString to work
  cat(paste0(
    "Probability maps saved to: ", prob_map_folder, " (class: ", class(prob_map_folder), ") \n"
  ))

  prob_map_folder
}
