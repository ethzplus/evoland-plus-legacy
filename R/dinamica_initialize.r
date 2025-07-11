#' Get Simulation Parameters
#'
#' This functionality is roughly equivalent to what used to be called
#' `Dinamica_initialize.r`. It is intended to be called from within a Dinamica
#' simulation to read parameters.
#' Date: 25-02-2022
#' Author: Ben Black

#' @export
default_ctrl_tbl_path <- function() {
  Sys.getenv(
    "EVOLAND_CTRL_TBL_PATH",
    unset = "simulation_control.csv"
  )
}

#' @export
get_control_table <- function(ctrl_tbl_path = default_ctrl_tbl_path()) {
  readr::read_csv(ctrl_tbl_path)
}

# get a table of those
#' @export
get_remaining_simulations <- function(ctrl_tbl_path = default_ctrl_tbl_path()) {
  get_control_table(ctrl_tbl_path) |>
    dplyr::filter(completed.string == "N") |>
    dplyr::relocate(simulation_num.)
}

# get parameters for a given simulation run (scenario) given a control table
# path and a single integer simulation id
#' @export
get_simulation_params <- function(
    ctrl_tbl_path = default_ctrl_tbl_path(),
    simulation_id = integer(),
    model_mode) {
  stopifnot(rlang::is_scalar_integerish(simulation_id))
  params <-
    get_control_table(ctrl_tbl_path) |>
    dplyr::filter(simulation_num. == simulation_id) |>
    as.list()

  params[["sim_results_path"]] <-
    fs::path("results", paste0("sim_id_", simulation_id)) |>
    ensure_dir()

  params[["initial_lulc_path"]] <-
    fs::path(
      params[["sim_results_path"]],
      paste0(
        "simulated_LULC_simID_", simulation_id,
        "_year_", params[["scenario_start.real"]],
        ".tif"
      )
    )

  config <- get_config()

  params[["params_folder_dinamica"]] <-
    ifelse(
      grepl("simulation", params[["model_mode.string"]]),
      file.path(
        config[["simulation_param_dir"]], params[["scenario_id.string"]],
        "Allocation_param_table_<v1>.csv"
      ),
      file.path(
        config[["calibration_param_dir"]], simulation_id, "Allocation_param_table_<v1>.csv"
      )
    )

  params
}

# To be used for a lookuptable in Dinamica
#' @export
get_simulation_timesteps <- function(config = get_config()) {
  steps <- seq.int(
    from = config[["scenario_start"]],
    to = config[["scenario_end"]],
    by = config[["step_length"]]
  )

  list(
    key = steps[-1],
    value = steps[-length(steps)]
  )
}

dinamica_initialize <- function(wpath, ctrl_tbl_path, simulation_num) {
  # A- Preparation ####

  # Enter name of Scenario to be tested as string or numeric (i.e. "BAU" etc.)
  scenario_id <- Simulation_table$scenario_id.string

  # Vector simulation ID
  simulation_id <- control_table$simulation_id.string

  # Vector name of Climate scenario
  Climate_ID <- Simulation_table$climate_scenario.string

  # Define model_mode: Calibration or Simulation
  model_mode <- Simulation_table$model_mode.string

  # Get start and end dates of scenario (numeric)
  scenario_start <- Simulation_table$scenario_start.real
  scenario_end <- Simulation_table$scenario_end.real

  # Enter duration of time step for modelling
  step_length <- Simulation_table$step_length.real

  # C- LULC map initialization + glacier conversion ####


  # use Simulation start time to select file path of initial LULC map
  Obs_LULC_paths <- list.files(
    file.path("Data", "Historic_LULC"),
    full.names = TRUE, pattern = ".gri"
  )

  # extract numerics
  Obs_LULC_years <- unique(as.numeric(gsub(".*?([0-9]+).*", "\\1", Obs_LULC_paths)))

  # if scenario_start year is <= 2020 then it probably hasn't been run before so we
  # need to create a copy of the initial LULC map to start the simulation with
  # vice versa if scenario_start year is >2020 then the scenario may have
  # been run previously or have been interrupted by an error so there is no need
  # to copy the start map because one will exist but this still needs to be checked

  if (scenario_start <= 2020) {
    # Identify start year
    LULC_start_year <- Obs_LULC_years[
      base::which.min(abs(Obs_LULC_years - scenario_start))
    ]

    # subset to correct LULC path and load
    Initial_LULC_raster <- raster::raster(
      Obs_LULC_paths[grep(LULC_start_year, Obs_LULC_paths)]
    )

    # convert raster to dataframe
    LULC_dat <- raster::as.data.frame(Initial_LULC_raster)

    # add ID column to dataset
    LULC_dat$ID <- seq.int(nrow(LULC_dat))

    # Get XY coordinates of cells
    xy_coordinates <- raster::coordinates(Initial_LULC_raster)

    # cbind XY coordinates to dataframe and seperate rows where all values = NA
    LULC_dat <- cbind(LULC_dat, xy_coordinates)

    # For the simulations in order for the transition rates for glaciers to be
    # accurate we need to make sure that the initial LULC map has the correct
    # number of glacier cells according to glacial modelling
    if (grepl("simulation", model_mode, ignore.case = TRUE)) {
      # load scenario specific glacier index
      Glacier_index <- readRDS(file = list.files(
        "Data/Glacial_change/Scenario_indices",
        full.names = TRUE,
        pattern = Climate_ID
      ))[, c("ID_loc", paste(scenario_start))]

      # seperate vector of cell IDs for glacier and non-glacer cells
      Non_glacier_IDs <- Glacier_index[Glacier_index[[paste(scenario_start)]] == 0, "ID_loc"]
      Glacier_IDs <- Glacier_index[Glacier_index[[paste(scenario_start)]] == 1, "ID_loc"]

      # replace the 1's and 0's with the correct LULC
      LULC_dat[LULC_dat$ID %in% Non_glacier_IDs, "Pixel_value"] <- 11
      LULC_dat[LULC_dat$ID %in% Glacier_IDs, "Pixel_value"] <- 19

      # 2nd step ensure that other glacial cells that do not match the glacier index
      # are also changed to static so that the transition rates calculate the
      # correct number of cell changes
      LULC_dat[
        which(LULC_dat$Pixel_value == 19 & !(LULC_dat$ID %in% Glacier_IDs)),
        "Pixel_value"
      ] <- 11

      # convert back to raster
      Initial_LULC_raster <- raster::rasterFromXYZ(LULC_dat[, c("x", "y", "Pixel_value")])
    } # close if statement for glacial modification

    # create a copy of the initial LULC raster files in the Simulation output folder so
    # that it can be called within Dinamica,
    # it should be named using the file path for simulated_LULC maps (see above) and the
    # Simulation start year
    raster::writeRaster(Initial_LULC_raster, save_raster_path, overwrite = TRUE, datatype = "INT1U")
  } # close if statement for copying initial LULC raster

  # D- Send allocation parameter table folder path ####

  # append the suffix necessary for Dinamica to alter strings (<v1>) to the file name
  if (grepl("simulation", model_mode, ignore.case = TRUE)) {
    Params_folder_Dinamica <- file.path(
      simulation_param_dir, scenario_id, "Allocation_param_table_<v1>.csv"
    )
  } else if (grepl("calibration", model_mode, ignore.case = TRUE)) {
    Params_folder_Dinamica <- file.path(
      calibration_param_dir, simulation_id, "Allocation_param_table_<v1>.csv"
    )
  }
}
