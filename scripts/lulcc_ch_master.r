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

# Install packages
#install Dinamica seperately
install.packages("Model/dinamica_1.0.4.tar.gz", repos=NULL, type="source")

packs <- c("data.table","stringi","stringr", "raster", "tidyverse", "testthat",
           "sjmisc", "tictoc", "readxl", "parallel", "terra", "gridExtra",
           "extrafont", "purrr", "rlist", "glmnet", "gam", "mgcv",
           "randomForest", "RRF", "lightgbm", "ranger", "maxnet", "ROCR",
           "ecospat","caret", "ggpubr", "rstatix", "ggstatsplot", "tibble",
           "car", "tidyr", "PMCMRplus", "reshape2", "ggsignif", "ggthemes",
           "ggside", "ghibli", "gridtext", "grid", "tidyverse", "openxlsx",
           "ggpattern", "Dinamica", "slackr", "rstudioapi")

new.packs <- packs[!(packs %in% installed.packages()[, "Package"])]

if (length(new.packs)) install.packages(new.packs)

# Load required packages
invisible(lapply(packs, require, character.only = TRUE))

# Source custom functions
invisible(sapply(list.files("Scripts/Functions", pattern = ".R", full.names = TRUE, recursive = TRUE), source))

#Install Dinamica EGO using included installer (Windows)
system2(command = paste("E:\\LULCC_CH\\Model\\SetupDinamicaEGO-720.exe"))

# Create a seperate environment for storing output of sourced scripts
output_env <- new.env()

#Because the simulations take a long time to complete it can be useful to have R
#send a message to Slack if the run has been completed successfully
#To do this user must set up the 'slackr' package according to the vignette:
#vignette('webhook-setup', package = 'slackr')

#Create config file (only needs to be done once)
# create_config_file(token = 'xapp-1-A049C8Z8ALW-4318425761910-7988238c82da2316a2c87bee2ad1962d6e42792243edd7517b12cce53a73056f',
#   incoming_webhook_url = 'https://hooks.slack.com/services/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',
#   channel = '#general',
#   username = 'slackr',
#   icon_emoji = 'tada')

#Connect to Slack
slackr_setup(channel = '#general',
             incoming_webhook_url = 'https://hooks.slack.com/services/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')

#TO DO: Document how users should set up the various 'tools' tables that control
#the creation of transition datasets and the tp models.

# Import model specifications table
model_specs <- read_excel("Tools/model_specs.xlsx")

#attach data period names to env.
output_env$Data_periods <- unique(model_specs$Data_period_name)

#attach string to env. indicating whether regionalized datasets should be produced
if(any(grep(model_specs$model_scale,
        pattern = "regionalized",
        ignore.case = TRUE)) == TRUE){
output_env$Regionalization <- TRUE
} else{
output_env$Regionalization <- FALSE
}

#create table for controlling simulations
Simulation_control_table <- data.frame(matrix(ncol = 9, nrow = 0))
colnames(Simulation_control_table) <- c("Simulation_num.",
                                         "Scenario_ID.string",
                                         "Simulation_ID.string",
                                         "Model_mode.string",
                                         "Scenario_start.real",
                                         "Scenario_end.real",
                                         "Step_length.real",
                                         "Parallel_TPC.string",
                                         "Completed.string")

#User enter scenario names to model
#vector abbreviations of scenario's for folder/file naming
Scenario_names <- c("BAU", "EI_NAT", "EI_CUL", "EI_SOC", "GR_EX")

#User enter start and end dates for the scenarios
#either enter a single number value or a vector of values the same length as the number of scenarios
#earliest possible model start time is 1985 and end time is 2060,
#simulations begin from 2020 and we have initially agreed to use 5 year time steps

Scenario_start <- 2020
Scenario_end <- 2060
Step_length <- 5

#User enter number of runs to perform for each simulation
reps <- 2

#expand vector of scenario names according to number of repetitions and add to table
Scenario_IDs <- c(sapply(Scenario_names, function(x) rep(x, reps), simplify = TRUE))
Simulation_control_table[1:length(Scenario_IDs), "Scenario_ID.string"] <- Scenario_IDs

#fill other columns
Simulation_control_table$Simulation_ID.string <- rep(paste0("v", seq(1, reps, 1)), length(Scenario_names))
Simulation_control_table$Scenario_start.real <- if(length(unique(Scenario_start)) == 1){Scenario_start} else {c(rep(Scenario_start, length(Scenario_names)))}
Simulation_control_table$Scenario_end.real <- if(length(unique(Scenario_end)) == 1){Scenario_end} else {c(rep(Scenario_end, length(Scenario_names)))}
Simulation_control_table$Step_length.real <- Step_length
Simulation_control_table$Model_mode.string <- "Simulation"
Simulation_control_table$Simulation_num. <- seq(1, nrow(Simulation_control_table),1)
Simulation_control_table$Parallel_TPC.string <- "Y"
Simulation_control_table$Completed.string <- "N"

#save the table
write_csv(Simulation_control_table, "Tools/Simulation_control.csv")

### =========================================================================
### A- Prepare LULC/region data
### =========================================================================

#Prepare LULC data layers
source("Scripts/preparation/LULC_data_prep.R", local = output_env)

#Prepare raster of Swiss Bioregions
source("Scripts/preparation/Region_prep.R", local = output_env)

### =========================================================================
### B- Prepare predictor data
### =========================================================================

#Start from a basic table of predictor names and details
#that cannot be created programmatically and expand this
#when data layers are created

#Prepare suitability and accessibility predictors
#source("Scripts/preparation/SA_var_prep_v3.R", local = output_env)

### =========================================================================
### C- Identify LULC transitions and create transition datasets
### =========================================================================

source("Scripts/preparation/Transition_identification.R", local = output_env)

source("Scripts/preparation/Transition_dataset_prep.R", local = output_env)

### =========================================================================
### D- Predictor variable selection on LULCC transition datasets
### =========================================================================

source("Scripts/preparation/Transition_feature_selection.R", local = output_env)

### =========================================================================
### E- Statistical modelling of LULCC transition datasets
### =========================================================================

source("Scripts/preparation/Trans_modelling.R", local = output_env)

### =========================================================================
### F- Summarizing model validation results
### =========================================================================

source("Scripts/preparation/Transition_model_evaluation.R", local = output_env)

### =========================================================================
### G- Prepare tables of transition rates for scenarios
### =========================================================================

source("Scripts/preparation/Scenario_trans_rates_prep.R", local = output_env)

### =========================================================================
### H- Prepare predictor data for scenarios
### =========================================================================

source("Scripts/preparation/Scenario_data_prep.R", local = output_env)

### =========================================================================
### I- Calibrate allocation parameters for Dinamica
### =========================================================================

#1. Estimate values for the allocation parameters and then apply random perturbation
#to generate sets of values to test with monte-carlo simulation
#2. perform simulations with all parameter sets
#3. Identify best performing parameter sets and save copies of tables
#to be used in scenario simulations

source("Scripts/preparation/Calibrate_allocation_parameters.R", local = output_env)

### =========================================================================
### J- Run Dinamica simulations over scenarios
### =========================================================================

#Perform pre-check to make sure that all element required for Dinamica modelling
#are prepared
Control_table_path <- paste0(getwd(),"/Tools/Calibration_control.csv")
Pre_check_result <- lulcc.modelprechecks(Control_table_path)

#TO DO: change model name to simulation
#run the Dinamica simulation model
if(Pre_check_result == TRUE){

#Read in Model.ego file
Model_text <- try(readLines("Model/Dinamica_models/LULCC_CH.ego"))

#Replace dummy string for working directory path path
Model_text <- str_replace(Model_text, "=====WORK_DIR=====", getwd())

#Replace dummy string for control table file path
Model_text <- str_replace(Model_text, "=====TABLE_PATH=====", Control_table_path)

#save a temporary copy of the model.ego file to run
Temp_model_path <- gsub(".ego", paste0("_simulation_", Sys.Date(), ".ego"), "Model/Dinamica_models/LULCC_CH.ego")
writeLines(Model_text, Temp_model_path)

#Get path for the Dinamica console executable
#(matching on regex '&' string) to be version agnostic
DC_path <- list.files("C:/", recursive = TRUE, full.names = TRUE, pattern = ".*DinamicaConsole.*\\.exe")
DC_path <- gsub('(*/)\\1+', '\\1', DC_path) #remove instances of double "/"
DC_path <- gsub("/", "\\\\", DC_path) #replace "/" with "\\"

#vector a path for saving the output text of this simulation
#run which indicates any errors
output_path <- paste0("Results/Simulation_notifications/Simulation_output_", Sys.Date(), ".txt")

system2(command = paste(DC_path),
        args = c("-disable-parallel-steps",
             Temp_model_path),
       wait = TRUE,
       stdout= output_path,
       stderr = output_path)

}else{
  print("Some elements required for modelling are not present/incorrect,
        consult the pre-check results object")}

#Check to see if the output.txt file contains the pattern "ERROR"
#(case sensitive) which indicates that the system command has failed
#If TRUE then send a message through Slack
if(grepl("ERROR", paste(readLines(output_path), collapse = "|"), ignore.case = FALSE) == TRUE){
slackr_bot('Simulation has stopped because of error')
}else{
#Send completion message
slackr_bot('Simulation completed sucessfully')

#Delete the temporary model file
unlink(Temp_model_path)
}
