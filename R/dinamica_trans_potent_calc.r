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
#' 1. Loads suitability and accessibility predictors, adapting to calibration or
#'    simulation mode.
#' 1. In simulation mode, generates dynamic predictors such as municipal population and
#'    neighbourhood statistics.
#' 1. Stacks all predictors and converts them to a dataframe for model prediction.
#' 1. Loads transition models and predicts transition probabilities for each transition
#'    and region.
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
  devtools::load_all()
  config <- get_config()
  setwd("2025-07-25_14h11m19s")
  simulation_num <- 1L
  time_step <- 2010
  params <- get_simulation_params(simulation_id = simulation_num)

  if (grepl("y", params[["parallel_tpc.string"]], ignore.case = TRUE)) {
    stop("Removed support for parallelisation at this stage")
  }

  # implement spatial interventions
  Use_interventions <- params[["spatial_interventions.string"]]

  # check normalisation of transition probabilities
  Check_normalisation <- FALSE

  message(
    "Starting transition potential calculation for ", params[["model_mode.string"]], ": ",
    params[["simulation_id.string"]], ", with scenario: ", params[["scenario_id.string"]],
    ", at time step: ", time_step
  )
  message(" - loading maps from: ", params[["sim_results_path"]], "\n")

  # Convert model mode into a string of the dates calibration period being used
  # this makes it easier to load files because they use this nomenclature

  calibration_periods <-
    config[["model_specs_path"]] |>
    readr::read_csv(col_types = "cccccllcc") |>
    purrr::pluck("data_period_name") |>
    unique()

  calibration_dates <- lapply(calibration_periods, function(period) {
    as.numeric(stringr::str_split(period, "_")[[1]])
  })

  period_tag <- if (!params[["is_simulation"]]) {
    Period_log <- sapply(calibration_dates, function(x) {
      time_step > x[1] & time_step <= x[2]
    })
    if (all(Period_log == FALSE)) {
      # find the closet year
      closest_year <- unlist(calibration_dates)[
        which.min(abs(unlist(calibration_dates) - time_step))
      ]
      # match year to period name
      Period_log <- calibration_periods[
        grepl(paste0(closest_year), calibration_periods)
      ]
    } else {
      calibration_periods[Period_log]
    }
  } else {
    # if simulation, take last calibration_period (presuming that vector is ordered)
    calibration_periods[length(calibration_periods)]
  }

  # create folder for saving prediction probability maps
  prob_map_folder <-
    fs::path("results", "pred_prob_maps", time_step) |>
    ensure_dir()

  message(" - creating directory for saving probability maps: ", prob_map_folder)

  # B- Retrieve current LULC map and layerize ####

  current_LULC_path <- fs::path(
    params[["sim_results_path"]],
    paste0(time_step, ".tif")
  )

  current_LULC_raster <- terra::rast(current_LULC_path)

  # load aggregation scheme (RAT = raster attribute table)
  lulc_rat <-
    config[["LULC_aggregation_path"]] |>
    readxl::read_excel() |>
    dplyr::select(tidyselect::all_of(c("Class_abbreviation", "Aggregated_ID"))) |>
    dplyr::distinct() |>
    dplyr::arrange(Aggregated_ID)

  message("Layerizing current LULC map: ", current_LULC_path)

  # layerize data - one column for each LULC class. depends
  lulc_data_tbl <-
    current_LULC_raster |>
    terra::segregate(lulc_rat$Aggregated_ID) |>
    terra::as.data.frame(cells = TRUE, na.rm = TRUE) |>
    tibble::as_tibble() |>
    rlang::set_names(c("id", lulc_rat$Class_abbreviation))

  # C- Load Suitability and Accessibility predictors ####

  prepared_data_tbl_path <- fs::path(
    config[["prepped_pred_stacks"]], paste0(period_tag, ".csv.gz")
  )
  if (fs::file_exists(prepared_data_tbl_path)) {
    prepared_data_tbl <- readr::read_csv(prepared_data_tbl_path)
  } else {
    t1 <- proc.time()
    prepared_data_metatbl <-
      config[["pred_table_path"]] |>
      readxl::read_excel(sheet = period_tag) |>
      dplyr::mutate(Prepared_data_path = fs::path(config[["data_basepath"]], Prepared_data_path))
    prepared_data_tbl <-
      purrr::map2(
        prepared_data_metatbl$Prepared_data_path,
        prepared_data_metatbl$Covariate_ID,
        function(path, name) {
          terra::rast(path) |>
            terra::as.data.frame(cells = TRUE, na.rm = TRUE) |>
            tibble::as_tibble() |>
            rlang::set_names(c("id", name))
        }
      ) |>
      purrr::reduce(\(x, y) dplyr::inner_join(x, y, by = "id"))
    readr::write_csv(prepared_data_tbl, prepared_data_tbl_path)
    t2 <- proc.time() - t1
    message(
      "Took", t2[["elapsed"]], "s to prepare data for period ", period_tag,
      "\n\tSaved at ", prepared_data_tbl_path
    )
  }

  message("Loaded Suitability and Accessibility predictors")

  # E- Generate dynamic predictors ####

  if (params[["is_simulation"]]) {
    stop("not implemented")
    pop_raster <- model_municip_pop()
    Nhood_rasters <- model_neigh_preds()
  }

  # F- Combine LULC, SA_preds and Nhood_preds and extract to dataframe ####

  message("Stacking LULC, SA_preds and Nhood_preds")
  if (config[["regionalization"]]) {
    bioregion_tbl <-
      fs::path(config[["bioreg_dir"]], "bioreg_raster.grd") |>
      terra::rast() |>
      terra::as.data.frame(cells = TRUE, na.rm = TRUE) |>
      tibble::as_tibble() |>
      rlang::set_names(c("id", "Bioregion"))
  }

  if (!params[["is_simulation"]]) {
    trans_dataset_list <- list(
      lulc_data_tbl,
      prepared_data_tbl,
      bioregion_tbl
    )
  } else {
    # For simulation mode only stack the Nhood_rasters here because otherwise they
    # were not included in the upper stack function
    stop("not implemented")

    # fixme this needs to become a list so we can iterate over its elements when making
    # a DF to keep memory pressure a bit lower
    trans_dataset_list <- list(
      lulc_data_tbl,
      prepared_data_tbl,
      pop_raster,
      raster::stack(Nhood_rasters),
      bioregion_tbl
    )
    message(" - Stacked all layers")
    names(trans_dataset_list) <- c(
      names(lulc_data_tbl),
      names(prepared_data_tbl@layers),
      names(pop_raster),
      names(Nhood_rasters),
      names(bioregion_tbl)
    )
    message(" - Renamed layers")
  }

  # Get XY coordinates of cells
  xy_coordinates <-
    config[["ref_grid_path"]] |>
    terra::rast() |>
    terra::as.data.frame(xy = TRUE, cell = TRUE, na.rm = FALSE) |>
    tibble::as_tibble() |>
    dplyr::transmute(
      id = as.integer(cell),
      x = as.integer(x),
      y = as.integer(y)
    )

  # reduce all previously made tables by inner joining into a single table
  trans_dataset_complete <-
    purrr::reduce(
      trans_dataset_list,
      function(x, y) {
        dplyr::inner_join(
          x,
          terra::as.data.frame(y, cells = TRUE, na.rm = TRUE),
          by = "id"
        )
      }
    ) |>
    dplyr::mutate(id = as.integer(id)) |>
    dplyr::left_join(xy_coordinates, by = "id")

  message(" - Converted raster stack to dataframe")


  # cbind XY coordinates to dataframe and seperate rows where all values = NA
  # trans_dataset_complete <- cbind(trans_dataset_complete, xy_coordinates)

  # release memory
  rm(lulc_data_tbl, prepared_data_tbl, Nhood_rasters, trans_dataset_list, xy_coordinates)

  # G- Run transition potential prediction for each transition ####

  message("Running transition potential prediction for each transition")

  # load model look up
  model_lookup <-
    readxl::read_excel(
      config[["model_lookup_path"]],
      sheet = period_tag
    ) |>
    dplyr::rename_with(tolower) |>
    dplyr::mutate(file_path = fs::path(config[["data_basepath"]], file_path))

  # remove transitions if they are being implemented deterministically
  if (
    params[["is_simulation"]] &&
      grepl("Y", params[["deterministic_trans.string"]], ignore.case = TRUE)
  ) {
    # remove transitions with initial class == glacier
    message(" - Removing transitions with initial class == glacier")
    model_lookup <- model_lookup[model_lookup$initial_lulc != "Glacier", ]
  }

  trans_dataset_na <- dplyr::anti_join(xy_coordinates, trans_dataset_complete, by = "id")

  # seperate ID, x/y and initial_lulc cols to append results to
  prediction_probs <- trans_dataset_complete[
    ,
    c("id", "x", "y", unique(model_lookup$initial_lulc), "static")
  ]
  # TODO remove? why would the row.names be important?
  # row.names(prediction_probs) <- prediction_probs$ID

  # add cols to both the complete and NA data to capture predict probabilities to each class
  for (lulc in unique(model_lookup$final_lulc)) {
    prediction_probs[[paste0("prob_", lulc)]] <- 0
    trans_dataset_na[[paste0("prob_", lulc)]] <- NA_real_
  }

  message(" - Created dataframe for storing prediction probabilities")

  # G2 Actual transition potential computation ####

  Non_par_start <- Sys.time()
  purrr::pmap(
    dplyr::select(model_lookup, trans_id, region, final_lulc, initial_lulc, file_path),
    function(tdc = trans_dataset_complete,
             trans_id, region, final_lulc, initial_lulc, file_path) {
      browser()
      fitted_model <- readRDS(file_path)
      pred_data <- tdc[
        tdc[[initial_lulc]] == 1 &
          tdc[["Bioregion"]] == region,
      ]
      prob_predicts <- as.data.frame(predict(fitted_model, pred_data, type = "prob"))
      names(prob_predicts)[[2]] <- paste0("prob_", final_lulc)
    }
  )
  # loop over transitions
  for (i in seq_len(nrow(model_lookup))) {
    # alternative method of replacing prob prediction values
    prediction_probs[
      row.names(prob_predicts),
      paste0("prob_", final_lulc)
    ] <- prob_predicts[paste0("prob_", final_lulc)]
  } # close loop over Models
  Non_par_end <- Sys.time()
  Non_par_time <- Non_par_end - Non_par_start # sequential time = 2.937131 mins
  message(" - completed transition potential prediction in ", Non_par_time)

  # G3- Re-scale predictions ####

  message("Re-scaling transition probabilities")

  # loop over rows and re-scale probability values so that they sum to 1
  # (by dividing by multiplying by 1 and then dividing by the sum of the vector)

  # Transition probability columns to re-scale
  Pred_prob_columns <- grep("prob_", names(prediction_probs), value = TRUE)

  # vector row indices with non-zero sums of transition probabilities
  Non_zero_indices <- which(rowSums(prediction_probs[, Pred_prob_columns]) > 1)

  # Loop over rows performing re-scaling
  prediction_probs[Non_zero_indices, Pred_prob_columns] <- as.data.frame(
    t(apply(prediction_probs[Non_zero_indices, Pred_prob_columns],
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
  trans_dataset_na[setdiff(names(prediction_probs), names(trans_dataset_na))] <- NA
  Raster_prob_values <- rbind(prediction_probs, trans_dataset_na)

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

  if (params[["is_simulation"]]) {
    # If statement to implement spatial interventions
    if (Use_interventions == "Y") {
      message("Implementing spatial interventions")

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

  message("Performing final re-scaling")

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

  message("Saving transition rasters")

  # subset model_lookup table to unique trans ID
  Unique_trans <- model_lookup[!duplicated(model_lookup$Trans_ID), ]

  # Loop over unique trans using details to subset data and save Rasters
  for (i in seq_len(nrow(Unique_trans))) {
    Trans_ID <- Unique_trans[i, "Trans_ID"]
    message(" - Preparing layer ", Trans_ID, "\n")
    final_lulc <- Unique_trans[i, "final_lulc"]
    initial_lulc <- Unique_trans[i, "initial_lulc"]

    # get indices of non_class cells
    non_initial_indices <- na.omit(Raster_prob_values[Raster_prob_values[initial_lulc] == 0, "ID"])

    # seperate Final class column
    Trans_raster_values <- Raster_prob_values[, c("ID", "x", "y", paste0("prob_", final_lulc))]

    # replace values of non-class cells with 0
    Trans_raster_values[non_initial_indices, paste0("prob_", final_lulc)] <- 0

    if (Check_normalisation) {
      # check that are Prob_ values are in [0, 1[ - otherwise warn
      for (col_name in paste0("prob_", final_lulc)) {
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
      Trans_raster_values[, c("x", "y", paste0("prob_", final_lulc))],
      crs = raster::crs(current_LULC_raster)
    )

    # vector file path for saving probability maps
    prob_map_path <- file.path(
      prob_map_folder,
      paste0(Trans_ID, "_probability_", initial_lulc, "_to_", final_lulc, ".tif")
    )

    raster::writeRaster(Prob_raster, prob_map_path, overwrite = TRUE)
  } # close loop over transitions

  # Return the probability map folder path as a string to
  # Dinamica to indicate completion
  # Note strings must be vectorized for 'outputString to work
  message(
    "Probability maps saved to: ", prob_map_folder, " (class: ", class(prob_map_folder), ") \n"
  )

  prob_map_folder
}



model_municip_pop <- function(current_LULC_raster, config, params) {
  # E.1- Dynamic predictors:  Municipal Population; for calibration the raster stacks
  # already contain the dynamic predictor layers so there is nothing to be done
  message("Generating dynamic predictors: - Municipal population")
  # create population data layer
  # subset current LULC to just urban cells
  Urban_rast <- current_LULC_raster == 10

  # load canton shapefile
  Canton_shp <- raster::shapefile(
    fs::path(config[["ch_geoms_path"]], "swissboundaries3d_1_5_tlm_kantonsgebiet.shp")
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
    fs::path(config[["ch_geoms_path"]], "swissboundaries3d_1_5_tlm_hoheitsgebiet.shp")
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
    fs::path(config[["preds_tools_dir"]], "population_projections.xlsx"),
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

  # why was this todo? THIS MUST BE THE LAYER NAME IN THE CALIBRATION STACKS/MODELS
  names(pop_raster) <- "Muni_pop"

  # clean up
  rm(
    Canton_shp, Canton_urban_areas, Can_urban_area, canton_model, Muni_shp,
    Muni_urban_areas, munis_indices, pop_models, Pop_prediction_table,
    Urban_rast
  )
}


model_neigh_preds <- function(config, period_tag) {
  # E.2- Dynamic predictors: Neighbourhood predictors

  message(" - Neighbourhood predictors")

  # load matrices used to create focal layers
  Focal_matrices <-
    fs::path(config[["preds_tools_dir"]], "neighbourhood_matrices", "all_matrices.rds") |>
    readRDS() |>
    unlist(recursive = FALSE)

  # adjust matrix names
  names(Focal_matrices) <- sapply(names(Focal_matrices), function(x) {
    stringr::str_split(x, "[.]")[[1]][2]
  })

  # Load details of focal layers required for the model set being utilised
  Required_focals_details <- readRDS(list.files(
    fs::path(config[["preds_tools_dir"]], "neighbourhood_details_for_dynamic_updating"),
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
      lulc_rat[lulc_rat$Class_abbreviation == Active_class_name, "Aggregated_ID"]
    )

    # subset LULC raster by all Active_class_value
    Active_class_raster_subset <- current_LULC_raster == Active_class_value

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
