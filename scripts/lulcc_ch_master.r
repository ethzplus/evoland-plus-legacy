#############################################################################
## LULCC_CH_master: Master script for whole process of LULCC data preparation,
## statistical modelling and preparing model inputs required by Dinamica EGO
##
## Note: scripts must be sourced in the order presented in order to work,
## adjust model_specs table to:
## 1. control time periods to be modelled
## 2. Specify statistical modelling technique
## 3. Whether or not regionalized datasets and models should be created
##
## Date: 25-10-2022
## Author: Ben Black
#############################################################################

# presumed working dir: root of the repo
devtools::load_all("lulccfunspkg")

### =========================================================================
### Simulation control table prep
### =========================================================================

# vector save path
Sim_control_path <- "tools/simulation_control.csv"
# TODO the code I found here was missing some columns vs. the csv that's been committed
# to the repo.
# lulccfunspkg::simcontrolprep() |> 
#   readr::write_csv(Sim_control_path)

### =========================================================================
### Modelling set-up
### =========================================================================

# list objects required for modelling
Model_tool_vars <- list(
  LULC_aggregation_path = "tools/lulc_class_aggregation.xlsx", # LULC class aggregation table
  Model_specs_path = "tools/model_specs.xlsx", # model specifications table
  Param_grid_path = "tools/param-grid.xlsx", # model hyper parameter grids
  Pred_table_path = "tools/predictor_table.xlsx", # predictor table
  Spat_ints_path = "tools/spatial_interventions.csv", # spatial interventions table
  EI_ints_path = "tools/ei_interventions.xlsx", # EI interventions table
  Ref_grid_path = "data/ref_grid.grd", # spatial grid to standardise spatial data
  Calibration_param_dir = "data/allocation_parameters/calibration",
  Simulation_param_dir = "data/allocation_parameters/simulation",
  Trans_rate_table_dir = "data/transition_tables/prepared_trans_tables",
  Sim_control_path = Sim_control_path, # simulation control table
  Step_length = Step_length,
  Scenario_names = Scenario_names,
  Inclusion_thres = 0.5
)

# Import model specifications table
model_specs <- read.csv(Model_tool_vars$Model_specs_path)

# Vector data periods contained in model specifications table
Model_tool_vars$Data_periods <- unique(model_specs$Data_period_name)

# attach string to env. indicating whether regionalized datasets should be produced
if (any(grep(
  model_specs$Model_scale,
  pattern = "regionalized",
  ignore.case = TRUE
)) == TRUE) {
  Model_tool_vars$Regionalization <- TRUE
} else {
  Model_tool_vars$Regionalization <- FALSE
}


### =========================================================================
### Download and unpack data
### =========================================================================

# download raw predictor data using Zenodo API service to get URLs for file downloads
lulccfunspkg::fetch_zenodo_predictors(doi = "10.5281/zenodo.7590103")

### =========================================================================
### A- Prepare LULC/region data
### =========================================================================

# Prepare LULC data layers
source("scripts/preparation/lulc_data_prep.r", local = scripting_env)

# Prepare raster of Swiss Bioregions
source("scripts/preparation/region_prep.r", local = scripting_env)

### =========================================================================
### B- Prepare predictor data
### =========================================================================

# Start from a basic table of predictor names and details
# that cannot be created programmatically and expand this
# when data layers are created

# Prepare suitability and accessibility predictors
source("scripts/preparation/calibration_predictor_prep.r", local = scripting_env)

### =========================================================================
### C- Identify LULC transitions and create transition datasets
### =========================================================================

source("scripts/preparation/transition_identification.r", local = scripting_env)

### =========================================================================
### D- Create transition datasets
### =========================================================================

source("scripts/preparation/transition_dataset_prep.r", local = scripting_env)

### =========================================================================
### E- Predictor variable selection on LULCC transition datasets
### =========================================================================

source("scripts/preparation/transition_feature_selection.r", local = scripting_env)

### =========================================================================
### F- Statistical modelling of LULCC transition datasets
### =========================================================================

source("scripts/preparation/trans_modelling.r", local = scripting_env)

### =========================================================================
### G- Summarizing model validation results
### =========================================================================

# The results comparing the performance of different transition model
# specifications require manual interpretation as the choice of optimal model
# must balance numerous aspects: accuracy, overfitting, computation time etc.
source("scripts/preparation/transition_model_evaluation.r", local = scripting_env)

# adjust contents of model_specs table to only optimal specifcations
lulccfunspkg::lulcc.finalisemodelspecifications(
  Model_specs_path = Model_specs_path,
  Param_grid_path = Param_grid_path
)

### =========================================================================
### H- Re-fitting optimal model specifications on full data
### =========================================================================

source("scripts/preparation/trans_model_finalization.r", local = scripting_env)

### =========================================================================
### I- Prepare data for deterministic transitions (e.g glacier -> Non-glacier)
### =========================================================================

source("scripts/preparation/deterministic_trans_prep.r", local = scripting_env)

### =========================================================================
### I- Prepare tables of transition rates for scenarios
### =========================================================================

source("scripts/preparation/simulation_trans_tables_prep.r", local = scripting_env)

### =========================================================================
### J- Prepare predictor data for scenarios
### =========================================================================

source("scripts/preparation/simulation_predictor_prep.r", local = scripting_env)

### =========================================================================
### K- Calibrate allocation parameters for Dinamica
### =========================================================================

# 1. Estimate values for the allocation parameters and then apply random perturbation
# to generate sets of values to test with monte-carlo simulation
# 2. perform simulations with all parameter sets
# 3. Identify best performing parameter sets and save copies of tables
# to be used in scenario simulations

source("scripts/preparation/calibrate_allocation_parameters.r", local = scripting_env)

### =========================================================================
### L- Prepare scenario specific spatial interventions
### =========================================================================

source("scripts/preparation/spatial_interventions_prep.r", local = scripting_env)

### =========================================================================
### M- Run Dinamica simulations over scenarios
### =========================================================================

# #Perform pre-checks to make sure that all element required for Dinamica modelling
# #are prepared
Pre_check_result <- lulcc.modelprechecks(Control_table_path, Param_dir = Simulation_param_dir)

# Run the Dinamica simulation model
# Fail pre-check condition
if (Pre_check_result == FALSE) {
  print("Some elements required for modelling are not present/incorrect,
        consult the pre-check results object")
} else if (Pre_check_result == TRUE) {
  # save a temporary copy of the model.ego file to run
  print("Creating a copy of the Dinamica model using the current control table")
  Temp_model_path <- gsub(
    ".ego", paste0("_simulation_", Sys.Date(), ".ego"),
    "model/dinamica_models/lulcc_ch.ego"
  )
  writeLines(Model_text, Temp_model_path)

  # vector a path for saving the output text of this simulation
  # run which indicates any errors
  output_path <- paste0(Sim_log_dir, "/simulation_output_", Sys.Date(), ".txt")

  # set environment path for Dinamica log/debug files
  # create a temporary dir for storing the Dinamica output files
  # Logdir <- "Model/Dinamica_models/Model_log_files"
  # dir.create(Logdir)
  # Win_logdir <- paste0(getwd(), "/", Logdir)

  print("Starting to run model with Dinamica EGO")
  lulccfunspkg::check_dinamica()
  system2(
    command = lulccfunspkg::get_dinamica_path(),
    args = c("-disable-parallel-steps -log-level 7", Temp_model_path)
    # env = c(
    #   DINAMICA_EGO_7_LOG_PATH = Win_logdir
    # )
  )

  # because the simulations may fail without the system command returning an error
  # (if the error occurs in Dinamica) then check the simulation control table to see
  # if/how many simulations have failed
  Updated_control_tbl <- read.csv(Sim_control_path)

  if (any(Updated_control_tbl$Completed.string == "ERROR")) {
    message(
      length(which(Updated_control_tbl$Completed.string == "ERROR")),
      "of", nrow(Updated_control_tbl),
      "simulations have failed to run till completion, check log for details of errors"
    )
  } else {
    # Send completion message
    print("All simulations completed sucessfully")

    # Delete the temporary model file
    # unlink(Temp_model_path)
  }
} # close if statement running simulation
