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

lulccfunspkg::setup()

# Download and unpack data
lulccfunspkg::fetch_zenodo_predictors(doi = "10.5281/zenodo.7590103")

# A- Prepare LULC/region data
# Prepare LULC data layers
lulccfunspkg::lulc_data_prep()

# Prepare raster of Swiss Bioregions
lulccfunspkg::region_prep()

# B- Prepare predictor data
# Prepare suitability and accessibility predictors
lulccfunspkg::calibration_predictor_prep()

# C- Identify LULC transitions and create transition datasets
lulccfunspkg::transition_identification()

# D- Create transition datasets
lulccfunspkg::transition_dataset_prep()

# E- Predictor variable selection on LULCC transition datasets
lulccfunspkg::transition_feature_selection()

# F- Statistical modelling of LULCC transition datasets
# G- Summarizing model validation results
lulccfunspkg::transition_modelling()

# adjust contents of model_specs table to only optimal specifcations
lulccfunspkg::lulcc.finalisemodelspecifications(
  Model_specs_path = Model_specs_path,
  Param_grid_path = Param_grid_path
)

# H- Re-fitting optimal model specifications on full data
lulccfunspkg::trans_model_finalization()

# I- Prepare data for deterministic transitions (e.g glacier -> Non-glacier)
lulccfunspkg::deterministic_trans_prep()

# I- Prepare tables of transition rates for scenarios
lulccfunspkg::simulation_trans_tables_prep()

# J- Prepare predictor data for scenarios
lulccfunspkg::simulation_predictor_prep()

## K- Calibrate allocation parameters for Dinamica
lulccfunspkg::calibrate_allocation_parameters()

# L- Prepare scenario specific spatial interventions
lulccfunspkg::spatial_interventions_prep()

# M- Run Dinamica simulations over scenarios
lulccfunspkg::run_dinamica_sims()
