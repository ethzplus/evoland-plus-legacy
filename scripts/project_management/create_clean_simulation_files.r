#############################################################################
## create_clean_simulation_files: Script to programmatically create a
## clean directory of only the files necessary for runnnig LULCC simulations
## for testing on the HPC cluster
##
##
## Date: 27-08-2023
## Author: Ben Black
#############################################################################

#vector name of model directory
Model_dir <- "E:/LULCC_CH"
Sim_dir <- "E:/LULCC_CH_HPC"

dir.create(Sim_dir)

### =========================================================================
### Identifying files required for model simulations
### =========================================================================

# #vector of only the relevant sub-dirs in Model dir
# #containing files required for simulation
# sub_dirs <- c("Scripts",
#               "Tools",
#               "Data")
#
# #save time by excluding large sub-dirs that are unnecessary
# Trans_table_dirs <- list.dirs("Data/Transition_tables")
# Trans_table_dirs <- Trans_table_dirs[!grepl(paste("prepared_trans_tables", collapse="|"), Trans_table_dirs)][-1]
#
# Glacier_dirs <- list.dirs("Data/Glacial_change")
# Glacier_dirs <- Glacier_dirs[!grepl(paste("Scenario_indices", collapse="|"), Glacier_dirs)][-1]
#
# Script_dirs <- list.dirs("Scripts")
# Script_dirs <- Script_dirs[!grepl(paste(c("Dinamica_integration", "Functions"), collapse="|"), Script_dirs)][-1]
#
# exclusions <- c(Trans_table_dirs,
#                 Glacier_dirs,
#                 Script_dirs,
#                 "Data/Preds/Raw",
#                 "Exemplar_data",
#                 "Transition_datasets",
#                 "Transition_models/RF_models",
#                 "Agriculture_bio_areas",
#                 "Agriculture_usable_areas",
#                 "Historic_LULC/NOAS04_LULC",
#                 "Allocation_parameters/Calibration",
#                 "Data/Bioreg_CH"
#                 )
#
# #list all files
# All_file_paths <- list.files(paste0(Model_dir, "/", sub_dirs), recursive = TRUE, full.names = TRUE)
#
# #Remove exclusions
# Sim_file_paths <- All_file_paths[!grepl(paste(exclusions, collapse="|"), All_file_paths)]
#
# #add in the model file paths
# Model_files <- list.files("Model", full.names = TRUE, recursive = TRUE)
# Model_files <- Model_files[grepl(paste(c("LULCC_CH.ego", "LULCC_CH_ego_Submodels"), collapse="|"), Model_files)]
#
# #add in the Bioreg files
# Bioreg_files <- c("Data/Bioreg_CH/Bioreg_raster.gri", "Data/Bioreg_CH/Bioreg_raster.grd")
#
# #add in the municipality shape file
# Muni_files <- list.files("Data/Preds/Raw",
#             recursive = TRUE,
#            full.names = TRUE,
#            pattern = "HOHEITSGEBIET")
#
# #append vectors
# Sim_file_paths <- c(Sim_file_paths, Model_files, Bioreg_files, Muni_files)
#
# #save list to perform final manual removals
# File_list_path <- "sim_files_list.csv"
# write.csv(Sim_file_paths,
#           file = File_list_path,
#           row.names = FALSE,
#           col.names = NULL)

# I messed up and only including the predictor stacks and not layers.
# Create a list of the pred layer files and manually incorporate in the overall file list.
# Pred_layers <- list.files("Data/Preds/Prepared/Layers", full.names = TRUE, recursive = TRUE)
# write.csv(Pred_layers,
#           file = "Pred_layers.csv",
#           row.names = FALSE,
#           col.names = NULL)

### =========================================================================
### Copying files to clean directory
### =========================================================================

#Load in vector of required file paths
Sim_files <- unlist(read.csv2(File_list_path, header = FALSE))

#remove the original dir
Sim_files <- stringr::str_remove_all(Sim_files, "E:/LULCC_CH/")

#subset the neighbourhood layers to only those required for modelling

#identify nhood layers and remove from complete vector of file paths
All_nhood_paths <- Sim_files[grepl("nhood", Sim_files)]
Sim_files <- Sim_files[!grepl("nhood", Sim_files)]

#Load details of focal layers required for the models of the simulation period
Required_focals_details <- readRDS(list.files("Data/Preds/Tools/Neighbourhood_details_for_dynamic_updating", pattern = "2009_2018", full.names = TRUE))

#subset to only required paths
Required_nhood <- All_nhood_paths[All_nhood_paths %in% Required_focals_details$Prepared_data_path]

#append required nhood paths
Sim_files <- c(Sim_files, Required_nhood)

#create directories before moving files
sapply(dirname(Sim_files), function(x){
  if(dir.exists(paste0(Sim_dir, "/", x)) == FALSE){
    dir.create(paste0(Sim_dir, "/", x), recursive = TRUE)
  }
})

#copy files from private dir to public
file.copy(Sim_files,
          to = paste0(Sim_dir, "/", Sim_files),
          overwrite = TRUE)
