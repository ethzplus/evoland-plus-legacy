#############################################################################
## Transition_table_prep: Creating tables of transition rates for future time points
## for use in LULCC allocation within Dinamica EGO
## Date: 22-09-2022
## Author: Ben Black
#############################################################################

### =========================================================================
### A- Preparation
### =========================================================================

# Set working directory
wpath<-"E:/LULCC_CH"
setwd(wpath)

#Vector packages for loading
packs<-c("data.table", "raster", "tidyverse",
         "lulcc", "stringr", "readr", "xlsx", "gdata")

# Load required packages
invisible(lapply(packs, require, character.only = TRUE))

#Dataframe of LULC labels and values
LULC_classes <- data.frame(label = c("Urban", "Static", "Open_Forest",
                                      "Closed_Forest","Shrubland", "Int_AG",
                                      "Alp_Past", "Grassland", "Perm_crops", "Glacier"),
                           value = c(10,11,12,13,14,15,16,17,18,19))

#Vector time periods for calibration
Periods <- c("1985_1997", "1997_2009", "2009_2018")

#The model lookup table specifies which transitions are modelled and
#can be used to subset the transition rates tables

#Load Model lookup tables for each period
Model_lookups <- lapply(Periods, function(Period){read.xlsx("Tools/Model_lookup.xlsx", sheetIndex = Period)})
names(Model_lookups) <- Periods

### =========================================================================
### C- Create folder structure for scenario specific trans tables
### =========================================================================

#use simulation control table to get names of Scenarios
Scenario_names <- unique(read.csv("Tools/Simulation_control.csv")[["Scenario_ID.string"]])

#base folder for creating scenario specific folders
base_trans_table_folder <- "Data/Transition_tables/prepared_trans_tables/"

#loop over scenario names creating folders for each in base folder
sapply(Scenario_names, function(x){
  dir.create(paste0(base_trans_table_folder,x), recursive = TRUE)
})

### =========================================================================
### D- Create time dependent naming structure for trans tables
### =========================================================================

#earliest possible model start time is 1985 and end time is 2060
#we have initially agreed to use 5 year time steps
Scenario_start <- 1985
Scenario_end <- 2060
Step_length <- 5

#vector sequence of time points and suffix
Time_steps <- seq(Scenario_start, Scenario_end, Step_length)

#These time points and suffixes can be used to name trans tables in a loop over scenarios

#instantiate function for saving transition tables across time steps
lulcc.savescenariotranstables <- function(Scenario_name, Time_steps, Base_folder, trans_table){
sapply(Time_steps, function(x){
  file_name <- paste0(Base_folder, "/", Scenario_name, "/", Scenario_name, "_trans_table_", x, ".csv")
  write_csv(trans_table, file = file_name)
})
}

### =========================================================================
### E- Calibration trans tables
### =========================================================================

#We already have tables of trans rates for each calibration period prepared but
#they need to be converted into tables for each time point

#seperate vector of time points into those relevant for each calibration period
Time_points_by_period <- list(Period_1985_1997 = Time_steps[Time_steps <= 1997],
             Period_1997_2009 = Time_steps[Time_steps >= 1997 & Time_steps <= 2009],
             Period_2009_2018 = Time_steps[Time_steps >= 2009 & Time_steps <= 2020])

#Load calibration trans_tables as a list with the same names
calibration_trans_tables <- lapply(list.files("Data/Transition_tables/raw_trans_tables", full.names = TRUE, pattern = "viable_trans"), read.csv)
names(calibration_trans_tables) <- names(Time_points_by_period)

#adjust column names in trans tables to include the '*' necessary for Dinamica to recognise them as key columns
calibration_trans_tables <- lapply(calibration_trans_tables, function(x){
  names(x)[names(x) == "From."] <- "From*"
  names(x)[names(x) == "To."] <- "To*"
  return(x)
})


#loop over both lists applying function lulcc.savescenariotranstables
mapply(function(trans_table_for_period, Time_points_by_period, base_trans_table_folder){
  lulcc.savescenariotranstables(Scenario_name = "CALIBRATION",
                              Time_steps = Time_points_by_period,
                              trans_table = trans_table_for_period,
                              Base_folder = base_trans_table_folder)},
       trans_table_for_period = calibration_trans_tables,
       Time_points_by_period = Time_points_by_period,
       base_trans_table_folder = base_trans_table_folder)


### =========================================================================
### F- Dummy trans tables for simulating BAU
### =========================================================================

#load back in the table of net transition rates for the different time periods
#in the calibration period
calibration_table <- read.csv("Data/Transition_tables/trans_rates_table_calibration_periods.csv")

#exclude any rows with NAs in the 2009_2018 columns as these are transitions
#that are not modelled in the future
calibration_table <- calibration_table[!is.na(calibration_table$X2009_2018),]

#calculate average net transition rate over historical period
calibration_table$Rate <- rowMeans(calibration_table[c("X1985_1997", "X1997_2009", "X2009_2018")], na.rm = TRUE)

#subset to just the columns necessary for Dinamica
Dummy_BAU_trans_table <- calibration_table[c("From.", "To.", "Rate")]
Dummy_BAU_trans_table$ID <- paste(Dummy_BAU_trans_table$From., Dummy_BAU_trans_table$To., sep = "_")

#extrapolating rates for future time steps

#rename period columns to single date to create an interval variable
names(calibration_table)[7:9] <- c("1997", "2009", "2018")

#pivot to long format
calibration_rates_long <- calibration_table %>% pivot_longer(cols = c("1997", "2009", "2018"),
                                                            names_to = "Year",
                                                            values_to = "Perc_rate")
calibration_rates_long$Year <- as.numeric(calibration_rates_long$Year)

#subset time steps to only those for simulations (>2020)
Simulation_steps <- Time_steps[Time_steps >=2020]

#create a df from these used for predicting
Pred_df <- as.data.frame(Simulation_steps)
names(Pred_df) <- "Year"

#create a duplicate table for storing extrapolated values with a column of
#unique Trans_IDs and a column for each simulation step
Extrap_calibration_rates <- data.frame(matrix(nrow = length(unique(calibration_rates_long$Trans_name)),
                                                                   ncol = length(Simulation_steps)+1))
colnames(Extrap_calibration_rates) <- c("Trans_name", Simulation_steps)
Extrap_calibration_rates$Trans_name <- unique(calibration_table$Trans_name)

#loop over unique trans_IDs, create a linear model for the perc_rate
#and use it to predict future time points

#upper loop over Trans_names
for(Name in unique(calibration_rates_long$Trans_name)){

#create model
Mod <-   lm(formula = Perc_rate ~ Year,
     data = calibration_rates_long[calibration_rates_long$Trans_name == Name,])

#predict
Pred <- predict(Mod, newdata = Pred_df)

#append to results df
Extrap_calibration_rates[Extrap_calibration_rates$Trans_name == Name, c(2:ncol(Extrap_calibration_rates))] <- Pred
}

#combine historical and extrapolated values
Combined_table <- cbind(calibration_table, Extrap_calibration_rates[,c(paste(Simulation_steps))])
Combined_table$Rate <- NULL

#save
write.xlsx(Combined_table, "Data/Transition_tables/Extrapolated_trans_rates.xlsx")

#ALTERNATIVE: Load in the BAU trans table produced by SR
Dummy_BAU_trans_table_SR <- read_csv("Data/Transition_tables/BAU_transition_SR.csv")
Dummy_BAU_trans_table_SR$ID <- paste(Dummy_BAU_trans_table_SR$`From*`, Dummy_BAU_trans_table_SR$`To*`, sep = "_")

#subset Sven-Eriks table
Output_table <- Dummy_BAU_trans_table_SR[which(Dummy_BAU_trans_table_SR$ID %in% Dummy_BAU_trans_table$ID),]
Output_table <- Output_table[order(Output_table$`From*`),]
Output_table$ID <- NULL

#adjust column names in trans tables to include the '*' necessary for Dinamica to recognise them as key columns
names(Dummy_BAU_trans_table)[names(Dummy_BAU_trans_table) == "From."] <- "From*"
names(Dummy_BAU_trans_table)[names(Dummy_BAU_trans_table) == "To."] <- "To*"

#save a copy of the dummy table for each time point in the sequence
lulcc.savescenariotranstables(Scenario_name = "BAU",
                              Time_steps = Time_steps,
                              trans_table = Output_table,
                              Base_folder = base_trans_table_folder)

### =========================================================================
### G- Trans tables for scenario's
### =========================================================================

#1. identify current areal coverage of each LULC class to be modelled
#using raster from 2018

#2. load percentage increases in each class according to scenario
#create a csv table that specifies this

#3. calculate cumulative percentage increase required to go from starting amount
#to desired end amount over prescribed number of time steps

#4. use existing multi-step transition rates tables to calculate the relative percentages
#of total transitions to a given LULC class individual transitions are responsible for

#5. divide value of cumulative % increase per time point according to relative % contributions of transitions that
#contribute to increases in that class.

#6. Some LULC classes we are not specifying increases in under scenarios but the cumulative increases in transitions
#should not exceed the amount of total land area available
#check total projected area of land under increases expressed in scenarios (use LULC raster)
#re-scale rates of transitions in other classes so that total land area is not exceeded.

#Save results using function above either do it in wide DF format and
#loop across columns or do it in long format and subset DF to time point
