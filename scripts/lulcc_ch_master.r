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
devtools::load_all()

config <- get_config()

#' Download and unpack data
#' FIXME this all-in-one download might be the wrong strategy:
#' - i'm unsure whether we're allowed to republish the data under CC04-BY
#' - the zenodo file is corrupted and needs to be replaced
#' - the zenodo file is pretty large, which increases risk of corruptinon during
#'   download. might want to segment into multiple smaller chunks
#' fetch_zenodo_predictors()

#' TODO where does the base / reference grid come from? why isn't it algorithmically
#' created? do we even need it, given that we need to know the extent of LULC observations?
#' init_base_grid()

# A- Prepare LULC/region data
# Prepare LULC data layers
lulc_data_prep(config = config)

#' Prepare raster of Swiss Bioregions
#' TODO why is this prepared? none of the produced files seem to be referenced
#' region_prep(config = config)

# B- Prepare predictor data
# Prepare suitability and accessibility predictors
calibration_predictor_prep(config = config)

# C- Identify LULC transitions and create transition datasets
transition_identification(config = config)

# D- Create transition datasets
transition_dataset_prep(config = config)

# E- Predictor variable selection on LULCC transition datasets
transition_feature_selection(config = config)

# F- Statistical modelling of LULCC transition datasets
transition_modelling(config = config)

# G- Summarizing model validation results
# TODO here, there was a reference to a "Transition_model_evaluation.R" script
# Without having seen it, its name implies that it was run on the comprehensive set of
# models that are calculated in the previous step. I _did_ find a file called
# "Model_evaluation.R" which I'm committing together with these lines as
# "transition_model_evaluation.r" with the slight adaptation of encapsulating its logic
# in a function of the same name.

# adjust contents of model_specs table to only optimal specifcations
lulcc.finalisemodelspecifications()

# H- Re-fitting optimal model specifications on full data
trans_model_finalization(config = config)

# I- Prepare data for deterministic transitions (e.g glacier -> Non-glacier)
deterministic_trans_prep(config = config)

# I- Prepare tables of transition rates for scenarios
simulation_trans_tables_prep(config = config)

# J- Prepare predictor data for scenarios
simulation_predictor_prep(config = config)

## K- Calibrate allocation parameters for Dinamica
calibrate_allocation_parameters(config = config)

# L- Prepare scenario specific spatial interventions
spatial_interventions_prep(config = config)

# M- Run Dinamica simulations over scenarios
run_dinamica_sims(config = config)
