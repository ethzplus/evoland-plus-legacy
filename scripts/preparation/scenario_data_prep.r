# Set working directory
wpath<-"E:/LULCC_CH"
setwd(wpath)

#Vector packages for loading
packs<-c("foreach", "doMC", "data.table", "raster", "tidyverse",
         "testthat", "sjmisc", "tictoc", "doParallel",
         "lulcc", "pbapply", "stringr", "readr", "openxlsx", "randomForest", "Dinamica")

new.packs<-packs[!(packs %in% installed.packages()[,"Package"])]

if(length(new.packs)) install.packages(new.packs)

# Load required packages
invisible(lapply(packs, require, character.only = TRUE))

### =========================================================================
### B- Gather details of static variables
### =========================================================================

#load the predictor table sheet for the most recent calibration period
Predictor_table <- read.xlsx("E:/LULCC_CH/Data/Preds/Predictor_table.xlsx", sheet = "Period_2009_2018")

#filtering to static predictors
Static_preds <- Predictor_table[Predictor_table$Static_or_dynamic == "static",]
Static_preds$Scenario <- "All"

### =========================================================================
### B- Gather details of Dynamic variables
### =========================================================================

##TEMPORARILY PERFORMING THIS PROCESS FOR bau USING AGGREGATED CLIMATIC DATA
## ADAPT WHEN FUTURE DATA IS PRODUCED AT 5 YEAR INTERVALS

#upscale climatic data to 100m
#Load in the grid to use use for re-projecting the CRS and extent of covariate data
Ref_grid <- raster("Data/Ref_grid.gri")

#list the files of the raw climatic variables, pattern match on rcp45 because this is all that is need for BAU
Clim_var_paths <- list.files("E:/LULCC_CH/Data/Preds/Raw/Climatic/Future", recursive = TRUE, full.names = TRUE, pattern = "rcp45", ignore.case = TRUE)

Clim_var_names <- c("bio1", "bio12", "gdd0", "gdd3", "gdd5")
names(Clim_var_names) <- Predictor_table[Predictor_table$Predictor_category == "Climatic", "Covariate_ID"][2:6]

#subset to the required variables
Clim_var_paths <- grep(paste0("\\b(", paste(Clim_var_names, collapse="|"), ")\\b"), Clim_var_paths, value = TRUE)
names(Clim_var_paths) <- str_remove_all(str_remove_all(str_replace_all(Clim_var_paths, "Raw", "Prepared/Layers"), ".rData"), ".rds")
print(names(Clim_var_paths))

#NOTE THIS IS NOT WORKING FOR THE GDDX VARIABLES
#(BECAUSE THE RASTER HAS NO crs AND APPLYING ONE AND THEN PROJECTING
#GIVES A RASTER OF ALL NAs) SO INSTEAD i MANUALLY MOVED THE OLD GDDX LAYERS
#TO THE NEW FILE LOCATIONS
#loop over the files and ensure they are all standarised to the 100m grid
prepped_rasts <- mapply(function(file_path, save_path){

  #read raster
  if(grepl(".rData", file_path) == TRUE){
  load(file_path)
  crs(X) <- crs(Ref_grid) #necessary to set CRS
  #extent(X) <- extent(Ref_grid)
  prepped_rast <- projectRaster(X, to = Ref_grid) #reproject

  } else{
    rast <- readRDS(file_path)
    prepped_rast <- projectRaster(rast, to = Ref_grid)#reproject
    }
  #save
  dir.create(save_path, recursive = TRUE)
  writeRaster(prepped_rast, file = paste0(save_path, ".tif"), overwrite=TRUE)
  return(prepped_rast)
  }, file_path = Clim_var_paths,
  save_path = names(Clim_var_paths),
  SIMPLIFY = FALSE)

#create DF for capturing info
Dynamic_preds <- data.frame(matrix(ncol = length(colnames(Static_preds)), nrow=1))
colnames(Dynamic_preds) <- colnames(Static_preds)

#list files of aggregated climatic data
Prep_clim_vars_paths <- list.files("Data/Preds/Prepared/Simulation/SA_preds/Climatic", recursive = TRUE, full.names = TRUE)

#filter out tif.aux files and remove work dir
Prep_clim_vars_paths <- str_remove_all(grep(Prep_clim_vars_paths, pattern='.aux', invert=TRUE, value=TRUE), wpath)

#fill in column details
Dynamic_preds[c(1:length(Prep_clim_vars_paths)), "File_name"] <- Prep_clim_vars_paths
Dynamic_preds$Scenario <- "BAU"
Dynamic_preds$Static_or_dynamic <- "Dynamic"
Dynamic_preds$CA_category <- "Suitability"
Dynamic_preds$Predictor_category <- "Climatic"
Dynamic_preds$Original_resolution <- "25m"
Dynamic_preds$numeric_or_categorical <- "num"
Dynamic_preds$Prepared <- "Y"

#loop over the named vector of climate variable names to match based on the file name
Dynamic_preds$Covariate_ID <- sapply(Dynamic_preds$File_name, function(x){
  var_name <- names(Clim_var_names[sapply(paste0("\\b(", Clim_var_names, ")\\b"), function(name) grepl(name, x))])
  })

Dynamic_preds$Temporal_coverage <- sapply(Dynamic_preds$File_name, function(x) str_split(x, "/")[[1]][[8]])

### =========================================================================
### B- Add static/dynamic variable data to sheets  for future time points
### =========================================================================

#vector time steps for future predictions
#Simulation start time is 2020 and end time is 2060
#we have initially agreed to use 5 year time steps
Simulation_start <- 2020
Simulation_end <- 2060
Step_length <- 5

Time_steps <- seq(Simulation_start, Simulation_end, Step_length)

#loop over time steps binding static and dynamic preds
Combined_vars_for_time_steps <- sapply(Time_steps, function(sim_year){
  #subset dynamic variables by
  Dynamic_preds <- Dynamic_preds[sapply(Dynamic_preds$Temporal_coverage, function(data_period){
    dates <- as.numeric(str_split(data_period, "_")[[1]])
    between(sim_year, dates[1], dates[2])
  }),]
  Combined_vars <- rbind(Static_preds, Dynamic_preds)
}, simplify = FALSE)
names(Combined_vars_for_time_steps) <- Time_steps

#load predictor_table as workbook to add sheets
pred_workbook <- openxlsx::loadWorkbook(file = "E:/LULCC_CH/Data/Preds/Predictor_table.xlsx")

#loop over time steps adding sheets and adding the predictors to them
for(i in Time_steps){
#the try() is necessary in case sheets already exist
try(addWorksheet(pred_workbook, sheetName = i))
writeData(pred_workbook, sheet = paste(i), x = Combined_vars_for_time_steps[[paste(i)]])
}

#save workbook
openxlsx::saveWorkbook(pred_workbook, "E:/LULCC_CH/Data/Preds/Predictor_table.xlsx", overwrite = TRUE)

### =========================================================================
### B- create predictor variable stack for each scenario/time point
### =========================================================================

#To do: implement a check in the loop to make sure there are no duplicate
#predictors being included in the stacks

#vector scenario names
Scenario_names <- c("BAU", "BIOPRO", "DIV", "SHAD", "FUTEI")

#upper loop over time steps
sapply(Time_steps, function(sim_year){

  #load corresponding sheet of predictor table
  pred_details <- openxlsx::read.xlsx("Data/Preds/Predictor_table.xlsx", sheet = paste(sim_year))

  #loop over scenario names
  sapply(Scenario_names, function(scenario){

    #subset preds to scenario name and 'All' for static variables
    pred_details <- pred_details[pred_details$Scenario == scenario | pred_details$Scenario == "All",]

    #use file paths to stack
    pred_stack <- raster::stack(pred_details$File_name)

    #name layers
    names(pred_stack@layers) <- pred_details$Covariate_ID

    #save
    saveRDS(pred_stack, file = paste0("Data/Preds/Prepared/Stacks/Simulation/SA_preds/SA_pred_stacks/SA_pred_", scenario, "_", sim_year, ".rds"))
    }) #close loop over scenarios
}) #close loop over time steps


