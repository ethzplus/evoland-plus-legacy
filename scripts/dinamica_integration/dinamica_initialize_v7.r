#############################################################################
## Dinamica_intialize: Initialize model run specifications in Dinamica
## Date: 25-02-2022
## Author: Ben Black
#############################################################################

### =========================================================================
### A- Preparation
### =========================================================================

# Install packages if they are not already installed
packs <- c(
  "data.table", "stringi", "stringr", "plyr", "readxl", "ggpubr",
  "rlist", "tidyverse", "rstatix", "Dinamica", "raster", "openxlsx"
)

new.packs <- packs[!(packs %in% installed.packages()[, "Package"])]

if (length(new.packs)) install.packages(new.packs)

# Load required packages
invisible(lapply(packs, require, character.only = TRUE))

### =========================================================================
### B- Receive model specifications
### =========================================================================

# receive working directory
wpath <- s2
setwd(wpath)

# send model tool vars to global environment
list2env(readRDS("Tools/Model_tool_vars.rds"), .GlobalEnv)
# FIXME actually just variables provided by evoland::get_config() hoisted into
# global env

# simulation number being performed
simulation_num <- v1

# load table of simulations
Control_table_path <- s1
Simulation_table <- read.csv(Control_table_path)[simulation_num, ]

# Enter name of Scenario to be tested as string or numeric (i.e. "BAU" etc.)
scenario_id <- Simulation_table$scenario_id.string

# Enter an ID for this run of the scenario (e.g V1)
simulation_id <- Simulation_table$simulation_id.string

# Define model_mode: Calibration or Simulation
model_mode <- Simulation_table$model_mode.string

# Get start and end dates of scenario (numeric)
scenario_start <- Simulation_table$scenario_start.real
scenario_end <- Simulation_table$scenario_end.real

# Enter duration of time step for modelling
step_length <- Simulation_table$step_length.real

# specify save location for simulated LULC maps (replace quoted section)
# folder path based upon Scenario and Simulation ID's
simulated_LULC_folder_path <- paste(wpath, "Results/Dinamica_simulated_LULC", scenario_id, simulation_id, sep = "/")

### =========================================================================
### C- Work Dir and model mode initialization
### =========================================================================

# send step length
outputDouble("step_length", step_length)

# Send Simulation ID
outputString("Sim_id", simulation_id)

# send Model mode
outputString("model_mode", model_mode)

### =========================================================================
### D- Model time step
### =========================================================================

# use start and end time to generate a lookup table of dates seperated by 5 years
model_time_steps <- list(
  Keys = c(seq(scenario_start, scenario_end - 5, step_length)),
  Values = c(seq((scenario_start + 5), (scenario_end), step_length))
)

# send Model time step table to Dinamica receiver: simulation_time_steps
outputLookupTable("simulation_time_steps", model_time_steps$Keys, model_time_steps$Values)

### =========================================================================
### E- LULC map initialization + glacier conversion
### =========================================================================

# Check if directory for saving LULC maps exists, if not create it.
# requires use of absolute paths
if (dir.exists(simulated_LULC_folder_path) == TRUE) {
  "LULC folder already exists"
} else {
  dir.create(simulated_LULC_folder_path, recursive = TRUE)
}

# Create relative file path for simulated LULC maps, building on folder path
# no need to include Dinamica's escape string because an R script is used to modify for the correct time step
simulated_LULC_file_path <- paste0(simulated_LULC_folder_path, "/", "simulated_LULC_scenario_", scenario_id, "_simID_", simulation_id, "_year_")

# send simulated LULC folder path to Dinamica receiver: sim_lulc_folder_path
outputString("sim_lulc_folder_path", simulated_LULC_file_path)

# use Simulation start time to select file path of initial LULC map
Obs_LULC_paths <- list.files("Data/Historic_LULC", full.names = TRUE, pattern = ".gri")

# extract numerics
Obs_LULC_years <- unique(as.numeric(gsub(".*?([0-9]+).*", "\\1", Obs_LULC_paths)))

# vector file path for saving raster
save_raster_path <- paste0(simulated_LULC_file_path, scenario_start, ".tif")

# if scenario_start year is <= 2020 then it probably hasn't been run before so we
# need to create a copy of the initial LULC map to start the simulation with
# vice versa if scenario_start year is >2020 then the scenario may have
# been run previously or have been interrupted by an error so there is no need
# to copy the start map because one will exist but this still needs to be checked

if (scenario_start <= 2020) {
  # Identify start year
  LULC_start_year <- Obs_LULC_years[base::which.min(abs(Obs_LULC_years - scenario_start))]

  # subset to correct LULC path and load
  Initial_LULC_raster <- raster(Obs_LULC_paths[grep(LULC_start_year, Obs_LULC_paths)])

  # convert raster to dataframe
  LULC_dat <- raster::as.data.frame(Initial_LULC_raster)

  # add ID column to dataset
  LULC_dat$ID <- seq.int(nrow(LULC_dat))

  # Get XY coordinates of cells
  xy_coordinates <- coordinates(Initial_LULC_raster)

  # cbind XY coordinates to dataframe and seperate rows where all values = NA
  LULC_dat <- cbind(LULC_dat, xy_coordinates)

  # For the simulations in order for the transition rates for glaciers to be
  # accurate we need to make sure that the initial LULC map has the correct
  # number of glacier cells according to glacial modelling
  if (grepl("simulation", model_mode, ignore.case = TRUE)) {
    # load scenario specific glacier index
    Glacier_index <- readRDS(file = list.files("Data/Glacial_change/Scenario_indices",
      full.names = TRUE,
      pattern = scenario_id
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
    LULC_dat[which(LULC_dat$Pixel_value == 19 & !(LULC_dat$ID %in% Glacier_IDs)), "Pixel_value"] <- 11

    # convert back to raster
    Initial_LULC_raster <- rasterFromXYZ(LULC_dat[, c("x", "y", "Pixel_value")])
  } # close if statement for glacial modification

  # create a copy of the initial LULC raster files in the Simulation output folder so that it can be called within Dinamica,
  # it should be named using the file path for simulated_LULC maps (see above) and the Simulation start year
  writeRaster(Initial_LULC_raster, save_raster_path, overwrite = TRUE, datatype = "INT1U")
} # close if statement for copying initial LULC raster

# send initial LULC map file path to Dinamica receiver:
outputString("initial_lulc_path", save_raster_path)

### =========================================================================
### F- Send allocation parameter table folder path
### =========================================================================

# append the suffix necessary for Dinamica to alter strings (<v1>) to the file name
if (grepl("simulation", model_mode, ignore.case = TRUE)) {
  Params_folder_Dinamica <- paste0(simulation_param_dir, "/", scenario_id, "/Allocation_param_table_<v1>.csv")
} else if (grepl("calibration", model_mode, ignore.case = TRUE)) {
  Params_folder_Dinamica <- paste0(calibration_param_dir, "/", simulation_id, "/Allocation_param_table_<v1>.csv")
}

# send folder path as string to Dinamica receiver: trans_matrix_folder_path
outputString("Allocation_params_folder_path", Params_folder_Dinamica)
