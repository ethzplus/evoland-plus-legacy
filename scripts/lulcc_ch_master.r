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

config <- lulccfunspkg::get_config()

#' Download and unpack data
#' FIXME this all-in-one download might be the wrong strategy:
#' - i'm unsure whether we're allowed to republish the data under CC04-BY
#' - the zenodo file is corrupted and needs to be replaced
#' - the zenodo file is pretty large, which increases risk of corruptinon during
#'   download. might want to segment into multiple smaller chunks
#' lulccfunspkg::fetch_zenodo_predictors()

# A- Prepare LULC/region data
# Prepare LULC data layers
lulccfunspkg::lulc_data_prep(config = config)

# Prepare raster of Swiss Bioregions
lulccfunspkg::region_prep(config = config)

# B- Prepare predictor data
# Prepare suitability and accessibility predictors
lulccfunspkg::calibration_predictor_prep(config = config)

# C- Identify LULC transitions and create transition datasets
lulccfunspkg::transition_identification(config = config)

# D- Create transition datasets
lulccfunspkg::transition_dataset_prep(config = config)

# E- Predictor variable selection on LULCC transition datasets
lulccfunspkg::transition_feature_selection(config = config)

# F- Statistical modelling of LULCC transition datasets
# G- Summarizing model validation results
lulccfunspkg::transition_modelling(config = config)

# adjust contents of model_specs table to only optimal specifcations
lulccfunspkg::lulcc.finalisemodelspecifications(
  model_specs_path = model_specs_path,
  param_grid_path = param_grid_path
)

# H- Re-fitting optimal model specifications on full data
lulccfunspkg::trans_model_finalization(config = config)

# I- Prepare data for deterministic transitions (e.g glacier -> Non-glacier)
lulccfunspkg::deterministic_trans_prep(config = config)

# I- Prepare tables of transition rates for scenarios
lulccfunspkg::simulation_trans_tables_prep(config = config)

# J- Prepare predictor data for scenarios
lulccfunspkg::simulation_predictor_prep(config = config)

## K- Calibrate allocation parameters for Dinamica
lulccfunspkg::calibrate_allocation_parameters(config = config)

# L- Prepare scenario specific spatial interventions
lulccfunspkg::spatial_interventions_prep(config = config)

# M- Run Dinamica simulations over scenarios
lulccfunspkg::run_dinamica_sims(config = config)
