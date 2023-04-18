#############################################################################
## Spatial_prob_perturb: Prepare spatial layers for perturbation of cellular
##transition probabilities in simulation steps
##
## Date: 18-11-2021
## Author: Ben Black
#############################################################################

#Vector packages for loading
# packs<-c("foreach", "data.table", "raster", "tidyverse", "testthat",
#          "sjmisc", "tictoc", "parallel", "terra", "pbapply", "rgdal", "rgeos",
#          "sf", "tiff", "bfsMaps", "rjstat", "future.apply", "future", "stringr",
#          "stringi" ,"readxl","rlist", "rstatix", "openxlsx", "pxR", "rvest", "landscapemetrics")
#
# new.packs<-packs[!(packs %in% installed.packages()[,"Package"])]
#
# if(length(new.packs)) install.packages(new.packs)
#
# # Load required packages
# invisible(lapply(packs, require, character.only = TRUE))

Ref_grid <- raster(Ref_grid_path)
Ref_crs <- crs(Ref_grid)

#need to reload the model tool vars because it has been updated
#during the process of preparing the calibration allocation parameters
Model_tool_vars <- readRDS("Tools/Model_tool_vars.rds")
list2env(Model_tool_vars, .GlobalEnv)

### =========================================================================
### A- Prepare building zone data
### =========================================================================

#list URLs to be downloaded
Data_URLs <- c("https://www.kgk-cgc.ch/download_file/1018/239", #building zones 2022
               "https://www.kgk-cgc.ch/download_file/1019/239") #undeveloped areas in building zones 2022

#To Do: rewrite this codes as a function that searches for multiple file extensions for spatial data
#create dir
BZ_dir <- "Data/Spat_prob_perturb_layers/Bulding_zones"
dir.create(BZ_dir, recursive = TRUE)

#download directly from website
tmpdir <- tempdir()
url <- "https://www.kgk-cgc.ch/download_file/1018/239.zip"
file <- basename(url)
download.file(url, file, mode = "wb")
zip::unzip(zipfile = file, exdir = tmpdir)
unlink(file)

gpkg <- list.files(tmpdir, pattern = ".gpkg", full.names = TRUE)

#load shapefile from geopackage
shp_file <- sf::st_read(gpkg)

#re-project to research CRS
shp_file <- sf::st_transform(shp_file, crs = Ref_crs)

#convert shapefile to raster
BZ_rast <- rasterize(shp_file, Ref_grid, field= shp_file$CH_CODE_HN)
#BZ_rast <- raster("Data/Spat_prob_perturb_layers/Bulding_zones/BZ_raster.gri")

#create a raster attribute table
#using these names to create a raster attribute table (rat)
BZ_rast <- ratify(BZ_rast)
BZ_rat <- levels(BZ_rast)[[1]]

#get the german zone names
BZ_IDs <- unique(shp_file$CH_BEZ_D)

#add name column to RAT
BZ_rat$Class_Names <- str_replace_all(BZ_IDs, " ", "_")

#Convert names to english
BZ_rat$Class_Names <- c("Residential zones","Mixed zones",
"Zones_for_public_uses", "Restricted_building_zones",
"Work_zones","Centre_zones",
"Other_Building_Zones", "Tourism_and_Recreation_Zones",
"Traffic_zones")


#add RAT to raster object
levels(BZ_rast) <- BZ_rat

BZ_reclass_mat <- BZ_rat
BZ_reclass_mat$Class_Names <- 1

#convert raster to binary values (0 or 1 and NA)
BZ_reclass <- reclassify(BZ_rast, rcl = BZ_reclass_mat)

#saving the raster in R's native .grd format which preserves the attribute table
writeRaster(BZ_rast, filename= "Data/Spat_prob_perturb_layers/Bulding_zones/BZ_raster_all_classes.grd", overwrite = TRUE)
writeRaster(BZ_reclass, filename = "Data/Spat_prob_perturb_layers/Bulding_zones/BZ_raster.grd", overwrite = TRUE)

#create a distance to building zones raster
BZ_distance <- distance(BZ_rast)

#load in land use raster to mask distance raster
LULC_years <- as.numeric(gsub(".*?([0-9]+).*", "\\1", list.files("Data/Historic_LULC", full.names = FALSE, pattern = ".grd")))
Final_lulc <- raster(grep(paste(max(LULC_years)), list.files("Data/Historic_LULC", full.names = TRUE, pattern = ".gri"), value = TRUE))

#convert all non-NA values to 1
Final_lulc[!is.na(Final_lulc)] <- 1

#mask distance raster
BZ_distance_masked <- mask(BZ_distance, Final_lulc)

#save
writeRaster(BZ_distance_masked, filename = "Data/Spat_prob_perturb_layers/Bulding_zones/BZ_distance.tif", overwrite = TRUE)

#remove downloaded shape files
unlink(list(file, tmpdir))

### =========================================================================
### B- Typology of municipalities
### =========================================================================

#load municipalities shapefile download in SA_var_prep.R
Muni_shp <- shapefile("Data/Preds/Raw/CH_geoms/SHAPEFILE_LV95_LN02/swissBOUNDARIES3D_1_3_TLM_HOHEITSGEBIET.shp")

#filter out non-swiss municipalities
Muni_shp <- Muni_shp[Muni_shp@data$ICC == "CH" & Muni_shp@data$OBJEKTART == "Gemeindegebiet", ]

#Import data of typology of municipalities from FSO web service using condition:
#1. Municipality designations as of 01/05/2022 (to match mutations)

#scrape content from html address
Muni_type_content <- read_html("https://www.agvchapp.bfs.admin.ch/de/typologies/results?SnapshotDate=01.05.2022&SelectedTypologies%5B0%5D=HR_GDETYP2012")
muni_typology <- as.data.frame(html_table(Muni_type_content, fill = TRUE)[[1]][-1,]) #remove duplicate row names

#remove columns 7 and 8 which specify the other typologies (3 and 9 categories)
#and rename remaining col
muni_typology <- muni_typology[,-c(7,8)]
colnames(muni_typology)[[7]] <- "muni_type_ID"
muni_typology$`BFS-Gde Nummer` <- as.numeric(muni_typology$`BFS-Gde Nummer`)

#create manual legend (data documentation only specifies categories in German/French)
Muni_typ_legend <- data.frame(ID = sort(unique(muni_typology$muni_type_ID)),
                              type = c("City-center_large_agglomeration",
"Urban_employment_municipality_large_agglomeration",
"Residential_urban_municipality_large_agglomeration",
"City-centre_medium_agglomeration",
"Urban_employment_municipality_medium_agglomeration",
"Residential_urban_municipality_medium_agglomeration",
"Urban_tourist_municipality_of_a_small_agglomeration",
"Industrial_urban_municipality_of_a_small_agglomeration",
"Tertiary_urban_municipality_of_a_small_agglomeration",
"High-density_industrial_peri-urban_municipality",
"High-density_tertiary_peri-urban_municipality",
"Mid-density_industrial_peri-urban_municipality",
"Medium-density_tertiary_peri-urban_municipality",
"Low-density_agricultural_peri-urban_municipality",
"Low-density_industrial_peri-urban_municipality",
"Low_density_tertiary_peri-urban_municipality",
"Tourist_town_of_a_rural_center",
"Industrial_municipality_of_a_rural_center",
"Tertiary_municipality_of_a_rural_center",
"Rural_agricultural_municipality_in_a_central_location",
"Rural_industrial_municipality_in_a_central_location",
"Tertiary_rural_municipality_in_a_central_location",
"Peripheral_rural_tourist_municipality",
"Peripheral_agricultural_rural_municipality",
"Peripheral_mixed_rural_municipality"))

#add municipality type to data
muni_typology$muni_type <- sapply(muni_typology$muni_type_ID, function(x){
  Muni_typ_legend[Muni_typ_legend$ID == x, "type"]
})

#add Muni_type_ID to shapefile
Muni_shp@data$muni_type_ID <- as.numeric(sapply(Muni_shp@data$BFS_NUMMER, function(Muni_num){
  type <- muni_typology[muni_typology$`BFS-Gde Nummer` == Muni_num, "muni_type_ID"]
  }, simplify = TRUE))

#rasterize
Muni_type_rast <- rasterize(Muni_shp, Ref_grid, field = "muni_type_ID")

#link raster attribute table
levels(Muni_type_rast) <- Muni_typ_legend

#save
save_dir <- "Data/Spat_prob_perturb_layers/Municipality_typology/"
dir.create(save_dir)
raster::writeRaster(Muni_type_rast, filename = paste0(save_dir, "Muni_type_raster.grd"), overwrite = TRUE)


### =========================================================================
### C- Mountain areas
### =========================================================================

#Import data of municipalities in mountainous areas from FSO web service using condition:
#1. Municipality designations as of 01/05/2022 (to match mutations)

#scrape content from html address
Mount_content <- read_html("https://www.agvchapp.bfs.admin.ch/de/typologies/results?SnapshotDate=01.05.2022&SelectedTypologies%5B0%5D=HR_MONT2019")
muni_mountains <- as.data.frame(html_table(Mount_content, fill = TRUE)[[1]][-1,]) #remove duplicate row names

#rename column of interest
colnames(muni_mountains)[[7]] <- "mountainous"
muni_mountains$`BFS-Gde Nummer` <- as.numeric(muni_mountains$`BFS-Gde Nummer`)

#create attribute table: 0:non-mountain, 1:Mountainous
Muni_mount_legend <- data.frame(ID = c(0,1),
                              type = c("Non-mountainous", "Mountainous"))

#add Muni_type_ID to shapefile
Muni_shp@data$mountainous <- as.numeric(sapply(Muni_shp@data$BFS_NUMMER, function(Muni_num){
  type <- muni_mountains[muni_typology$`BFS-Gde Nummer` == Muni_num, "mountainous"]
  }, simplify = TRUE))

#rasterize
Muni_mount_rast <- rasterize(Muni_shp, Ref_grid, field = "mountainous")

#link raster attribute table
levels(Muni_mount_rast) <- Muni_mount_legend

#save
save_dir <- "Data/Spat_prob_perturb_layers/Mountainous_municipalities/"
dir.create(save_dir)
raster::writeRaster(Muni_mount_rast, filename = paste0(save_dir, "Muni_mountainous_raster.grd"), overwrite = TRUE)

### =========================================================================
### D- Protected areas
### =========================================================================

#Raw data aggregated from FOEN sources by Pierre-Louis Rey (UNIL)
#read shp file


#set locations of existing PAs to 0 in prioritization map

#exclude large urban areas


### =========================================================================
### D- Agricultural areas
### =========================================================================

#path to Biodiversity promotion areas .gpkg file
BPA_path <- "Data/Spat_prob_perturb_layers/Agriculture_bio_areas/Agri_bio_areas.gpkg"

#get layer names
BPA_layers <- st_layers(BPA_path)

#read in correct layer
BPAs <- st_read(BPA_path,layer = BPA_layers$name[[1]], geometry_column = "wkb_geometry")

#remove entries with empty geometries
BPAs <- BPAs[!st_is_empty(BPAs),,drop=FALSE]

#re-project to research CRS
BPAs <- sf::st_transform(BPAs, crs = Ref_crs)

#convert to spat vector
BPAs <- vect(BPAs)

#add ID col
BPAs$ID <- seq(1:nrow(BPAs))

#check for invalid polygons (i.e. holes)
polys_invalid <- any(is.valid(BPAs, messages=FALSE, as.points=FALSE)==FALSE)

#if invalid polygons then makeValid
if(polys_invalid == TRUE){
BPAs <- makeValid(BPAs)
}

#rasterize using the most recent LULC layer as a mask
Mask_rast <- rast("E:/LULCC_CH/Data/Historic_LULC/LULC_2018_agg.grd")
BPA_raster <- terra::mask(Mask_rast, BPAs)

#change non-NA values to 1
BPA_raster <- ifel(!is.na(BPA_raster), 1, NA)
writeRaster(BPA_raster, "Data/Spat_prob_perturb_layers/Agriculture_bio_areas/BPA_raster.tif")

### =========================================================================
### Load testing_data
### =========================================================================

#TO DO: determine whether it is faster to perform the probability perturbation
#operations on dataframes or rasters:
#Pro's of dataframe: smaller size (not including NAs) however identifying cells
#based on XY values may be slower than raster operations.
#Con's of dataframe: requires converting every mechanism layer into dataframe

#Pro's of raster: possibly faster operations and can calculate focal values
#and euclidean distances etc.
#Con's of raster: requires converting prob predictions into a raster brick and
#then back to dataframe in order to be re-scaled.

#dataframe of predicted probabilities not including NA cells
#Prediction_probs <- readRDS("Data/Spat_prob_perturb_layers/EXP_pred_probs_rescaled.rds")

#dataframe of NA cells to be combined with predicted probabilities
#Trans_dataset_na <- readRDS("Data/Spat_prob_perturb_layers/EXP_trans_dataset_NA_values.rds")

#process for binding NA values with data
#Trans_dataset_na[setdiff(names(Prediction_probs), names(Trans_dataset_na))] <- NA
#Raster_prob_values <- rbind(Prediction_probs, Trans_dataset_na)

#Dataframe of combined predicted probability values and NA values
#Raster_prob_values <- readRDS("Data/Spat_prob_perturb_layers/EXP_raster_prob_values.rds")
#
# #sort by ID
# Raster_prob_values[order(Raster_prob_values$ID),]
#
# Simulation_time_step <- "2040"
# Scenario_ID <- "BAU"

### =========================================================================
### Intervention in allocation params
### =========================================================================

#load table of scenario interventions
Interventions <- openxlsx::read.xlsx(Scenario_specs_path, sheet = "Interventions")

#convert Time_step and Target_classes columns back to character vectors
Interventions$Time_step <- sapply(Interventions$Time_step, function(x) {
  x <- str_remove_all(x, " ")
  rep <- unlist(strsplit(x, ","))
  },simplify=FALSE)

Interventions$Target_classes <- sapply(Interventions$Target_classes, function(x) {
  x <- str_remove_all(x, " ")
  rep <- unlist(strsplit(x, ","))
  },simplify=FALSE)

#test to see if spatial zoning
if(any(Interventions$Intervention_type == "Param_adjust")){

  #subset to interventions involving parameter adjustment
  Param_ints <- Interventions[Interventions$Intervention_type == "Param_adjust",]

  #load the LULC aggregation scheme
  LULC_agg <- openxlsx::read.xlsx(LULC_aggregation_path)

  #swap the target classes for class numbers
  Param_ints$Target_classes <- sapply(Param_ints$Target_classes, function(x){
    class_nums <- unique(LULC_agg[LULC_agg$Class_abbreviation %in%x, "Aggregated_ID"])
  })

  #loop over interventions adjust param tables
  sapply(1:nrow(Param_ints), function(i){

    #get paths of param tables for relevant scenario and time points
    Param_table_paths <- list.files(paste0(Simulation_param_dir, "/", Param_ints[i, "Scenario_ID"]),
               pattern = paste0(Param_ints[[i, "Time_step"]], collapse = "|"),
               full.names = TRUE)

    #loop over paths adjusting tables
    sapply(Param_table_paths, function(tbl_path){

      #load table
      param_table <- read.csv(tbl_path)

      #adjust column names
      colnames(param_table) <- c("From*","To*"," Mean_Patch_Size","Patch_Size_Variance","Patch_Isometry", "Perc_expander", "Perc_patcher")

      #alter rows for Target_classes
      param_table[param_table$`To*` %in% Param_ints[[i, "Target_classes"]], "Perc_expander"] <- 1
      param_table[param_table$`To*` %in% Param_ints[[i, "Target_classes"]], "Perc_patcher"] <- 0

      #save table
      write_csv(param_table, file = tbl_path)
      }) #close loop over tables

    }) #close loop over intervention rows

} #close if statement


### =========================================================================
### Function to perform perturbations (raster version)
### =========================================================================

#if Perc_diff is too small then the effect will likely not be achieved
#set a threshold of minimum probability perturbation

lulcc.spatprobperturbation <- function(Perturbation_ID, Raster_prob_values, Time_step){}

  #vector names of columns of probability predictions (matching on Prob_)
  Pred_prob_columns <- grep("Prob_", names(Raster_prob_values), value = TRUE)

  #convert probability table to raster stack
  Prob_raster_stack <- stack(lapply(Pred_prob_columns, function(x) rasterFromXYZ(Raster_prob_values[,c("x", "y", x)])))
  names(Prob_raster_stack@layers) <- Pred_prob_columns

  #load table of scenario interventions
  Interventions <- openxlsx::read.xlsx(Scenario_specs_path, sheet = "Interventions")

  #convert Time_step and Target_classes columns back to character vectors
  Interventions$Time_step <- sapply(Interventions$Time_step, function(x) {
    rep <- unlist(strsplit(x, ","))
    },simplify=FALSE)

  Interventions$Target_classes <- sapply(Interventions$Target_classes, function(x) {
    x <- str_remove_all(x, " ")
    rep <- unlist(strsplit(x, ","))
    },simplify=FALSE)

  #subset interventions to scenario
  Scenario_interventions <- Interventions[Interventions$Scenario_ID == Scenario_ID,]

  #subset to interventions for current time point
  Time_step_rows <- sapply(Scenario_interventions$Time_step, function(x) any(grepl(Simulation_time_step, x)))

  Current_interventions <- Scenario_interventions[Time_step_rows, ]

  #loop over rows
  if(nrow(Current_interventions) !=0){}
  for(i in nrow(Current_interventions)){}

    i = 1

    Intervention_ID <- Current_interventions[i, "Intervention_ID"]
    Target_classes <- paste0("Prob_", Current_interventions[[i, "Target_classes"]])
    Intervention_data <- Current_interventions[i, "Intervention_data"]
    Prob_perturb_thresh <-Current_interventions[i, "Prob_perturb_threshold"]

    #--------------------------------------------------------------------------
    # Urban_densification intervention
    #--------------------------------------------------------------------------
    if(Intervention_ID == "Urban_densification"){

    #load building zone raster
    Intervention_rast <- raster(Intervention_data)

    #identify pixels inside of building zones
    Intersecting <- mask(Prob_raster_stack@layers[[Target_classes]], Intervention_rast == 1)

    #increase probability to one
    Intersecting[Intersecting > 0] <- 1

    #index which cells need to have value updated
    ix <- Intersecting == 1

    #replace values in target raster
    Prob_raster_stack@layers[[Target_classes]][ix] <- Intersecting[ix]

    }#close Urban_densification chunk


    #--------------------------------------------------------------------------
    #Urban_sprawl intervention
    #--------------------------------------------------------------------------
    if(Intervention_ID == "Urban_sprawl"){

    #load building zone raster
    Intervention_rast <- raster(Intervention_data)

    #identify pixels inside of building zones
    Intersecting <- mask(Prob_raster_stack@layers[[Target_classes]], Intervention_rast == 1)

    #identify pixels outside of building zones
    non_intersecting <- overlay(Prob_raster_stack@layers[[Target_classes]],Intervention_rast,fun = function(x, y) {
      x[y==1] <- NA
      return(x)
    })

    #calculate 90th percentile values of probability for pixels inside vs.outside
    #excluding those with a value of 0
    Intersect_percentile <- quantile(Intersecting@data@values[Intersecting@data@values >0], probs = 0.90, na.rm=TRUE)
    Nonintersect_percentile <- quantile(non_intersecting@data@values[non_intersecting@data@values >0], probs = 0.90, na.rm=TRUE)

    #get the means of the values above the 90th percentile
    Intersect_percentile_mean <- mean(Intersecting@data@values[Intersecting@data@values >= Intersect_percentile], na.rm = TRUE)
    Nonintersect_percentile_mean <- mean(non_intersecting@data@values[non_intersecting@data@values >= Nonintersect_percentile], na.rm = TRUE)

    #mean difference
    Mean_diff <- Intersect_percentile_mean - Nonintersect_percentile_mean

    #Average of means
    Average_mean <- mean(Intersect_percentile_mean, Nonintersect_percentile_mean)

    #calculate percentage difference
    Perc_diff <- (Mean_diff/Average_mean)*100

    #The intended effect of the intervention is to increase the probability of
    #urban development outside the building zone, however depending on the
    #valency of the Perc_diff values this needs to be implemented differently

    #If Perc_diff is >0 then increase the probability of instances above the
    #90th percentile for the outside pixels by the percentage difference
    #between the means
    if(Perc_diff >0){

      #check threshold
      if(abs(Perc_diff) < Prob_perturb_thresh){Perc_diff <- Prob_perturb_thresh}

      non_intersecting[non_intersecting > Nonintersect_percentile] <- non_intersecting[non_intersecting > Nonintersect_percentile] + (non_intersecting[non_intersecting > Nonintersect_percentile]/100)*Perc_diff

      #replace any values greater than 1 with 1
      non_intersecting[non_intersecting > 1] <- 1

      #index which cells need to have value updated
      ix <- non_intersecting > Nonintersect_percentile

      #replace values in target raster
      Prob_raster_stack@layers[[Target_classes]][ix] <- non_intersecting[ix]
      positive_test <- Prob_raster_stack@layers[[Target_classes]]

      #else if Perc_diff is <0 then decrease the probability of instances above the
      #90th percentile for the inside pixels by the percentage difference
      #between the means
      }else if(Perc_diff <0){

      #check threshold
      if(abs(Perc_diff) < Prob_perturb_thresh){Perc_diff <- -(Prob_perturb_thresh)}

      Intersecting[Intersecting > Intersect_percentile] <- Intersecting[Intersecting > Intersect_percentile] + (Intersecting[Intersecting > Intersect_percentile]/100)*Perc_diff

      #replace any values greater than 1 with 1
      Intersecting[Intersecting < 0] <- 0

      #index which cells need to have value updated
      ix <- Intersecting > Intersect_percentile

      #replace values in target raster
      Prob_raster_stack@layers[[Target_classes]][ix] <- Intersecting[ix]
      negative_test <- Prob_raster_stack@layers[[Target_classes]]
      } #close else if statement

    }#close Urban_sprawl chunk

    #--------------------------------------------------------------------------
    # Urban_migration intervention
    #--------------------------------------------------------------------------
    if(Intervention_ID == "Urban_migration"){

    #load municipality typology raster
    Intervention_rast <- raster(Intervention_data)

    #seperate raster legend and recode values for remote rural municaplities
    #for this intervention: 325, 326, 327, 335, 338
    Leg <- Intervention_rast@data@attributes[[1]]
    Leg[Leg$ID %in% c(325, 326, 327, 335, 338), "type"] <- 1
    Leg[Leg$type != 1, "type"] <- NA
    Leg$type <- as.numeric(Leg$type)

    #reclassify raster
    Intervention_rast <- reclassify(Intervention_rast, rcl = Leg)

    #identify pixels inside of remote rural municipalities
    Intersecting <- mask(Prob_raster_stack@layers[[Target_classes]], Intervention_rast == 1)

    #identify pixels outside of remote rural municipalities
    non_intersecting <- overlay(Prob_raster_stack@layers[[Target_classes]],Intervention_rast,fun = function(x, y) {
      x[y==1] <- NA
      return(x)
    })

    #Because the intended effect of the intervention is to decrease the
    #probability of urban development in the remote rural municipalities
    #calculate 90th percentile value of probability for pixels inside
    #and the 80th percentile value for pixels outside
    #excluding cells with a value of 0 in both cases
    Intersect_percentile <- quantile(Intersecting@data@values[Intersecting@data@values >0], probs = 0.90, na.rm=TRUE)
    Nonintersect_percentile <- quantile(non_intersecting@data@values[non_intersecting@data@values >0], probs = 0.80, na.rm=TRUE)

    #get the means of the values above the percentiles
    Intersect_percentile_mean <- mean(Intersecting@data@values[Intersecting@data@values >= Intersect_percentile], na.rm = TRUE)
    Nonintersect_percentile_mean <- mean(non_intersecting@data@values[non_intersecting@data@values >= Nonintersect_percentile], na.rm = TRUE)

    #mean difference
    Mean_diff <- Nonintersect_percentile_mean - Intersect_percentile_mean

    #Average of means
    Average_mean <- mean(Intersect_percentile_mean, Nonintersect_percentile_mean)

    #percentage difference
    Perc_diff <- (Mean_diff/Average_mean)*100

    #If Perc_diff is < 0 then decrease the probability of instances above the
    #90th percentile for the pixels in remote rural municipalities by the percentage difference
    #between the means
    if(Perc_diff <0){

      #check threshold
      if(abs(Perc_diff) < Prob_perturb_thresh){Perc_diff <- -(Prob_perturb_thresh)}

      Intersecting[Intersecting > Intersect_percentile] <-Intersecting[Intersecting > Intersect_percentile] + (Intersecting[Intersecting > Intersect_percentile]/100)*Perc_diff

      #replace any values greater than 1 with 1
      Intersecting[Intersecting > 1] <- 1

      #index which cells need to have value updated
      ix <- Intersecting > Intersect_percentile

      #replace values in target raster
      Prob_raster_stack@layers[[Target_classes]][ix] <- Intersecting[ix]

      #else if Perc_diff is >0 then increase the probability of instances above the
      #90th percentile for the outside pixels by the percentage difference
      #between the means
      }else if(Perc_diff >0){

      #check threshold
      if(abs(Perc_diff) < Prob_perturb_thresh){Perc_diff <- Prob_perturb_thresh}

      non_intersecting[non_intersecting > Nonintersect_percentile] <- non_intersecting[non_intersecting > Nonintersect_percentile] + (non_intersecting[non_intersecting > Nonintersect_percentile]/100)*Perc_diff

      #replace any values greater than 1 with 1
      non_intersecting[non_intersecting > 1] <- 1

      #index which cells need to have value updated
      ix <- non_intersecting > Nonintersect_percentile

      #replace values in target raster
      Prob_raster_stack@layers[[Target_classes]][ix] <- non_intersecting[ix]
      } #close else if statement

    }#close Urban_migration chunk

    #--------------------------------------------------------------------------
    #Mountain_development intervention
    #--------------------------------------------------------------------------
    if(Intervention_ID == "Mountain_development"){

    #load municipality typology raster
    Intervention_rast <- raster(Intervention_data)

    #seperate raster legend and recode values for remote rural municaplities
    Leg <- Intervention_rast@data@attributes[[1]]

    #For this intervention there are two different specs for scenarios
    #EI_NAT: 314
    #EI_SOC: 314,334
    if(Scenario_ID == "EI_NAT"){
    Leg[Leg$ID == 314, "type"] <- 1
    } else if(Scenario_ID == "EI_SOC"){
    Leg[Leg$ID %in% c(314, 334), "type"] <- 1
    }

    Leg[Leg$type != 1, "type"] <- NA
    Leg$type <- as.numeric(Leg$type)

    #reclassify raster
    Intervention_rast <- reclassify(Intervention_rast, rcl = Leg)

    #identify pixels inside of mountainous remote municipalities
    Intersecting <- mask(Prob_raster_stack@layers[[Target_classes]], Intervention_rast == 1)

    #identify pixels outside of mountainous remote municipalities
    non_intersecting <- overlay(Prob_raster_stack@layers[[Target_classes]],Intervention_rast,fun = function(x, y) {
      x[y==1] <- NA
      return(x)
    })

    #Because the intended effect of the intervention is to increase the
    #probability of urban development in the mountainous municipalities

    #calculate 90th percentile value of probability for pixels inside and outside
    #excluding cells with a value of 0 in both cases
    Intersect_percentile <- quantile(Intersecting@data@values[Intersecting@data@values >0], probs = 0.90, na.rm=TRUE)
    Nonintersect_percentile <- quantile(non_intersecting@data@values[non_intersecting@data@values >0], probs = 0.90, na.rm=TRUE)

    #get the means of the values above the percentiles
    Intersect_percentile_mean <- mean(Intersecting@data@values[Intersecting@data@values >= Intersect_percentile], na.rm = TRUE)
    Nonintersect_percentile_mean <- mean(non_intersecting@data@values[non_intersecting@data@values >= Nonintersect_percentile], na.rm = TRUE)

    #mean difference
    Mean_diff <- Nonintersect_percentile_mean - Intersect_percentile_mean

    #Average of means
    Average_mean <- mean(Intersect_percentile_mean, Nonintersect_percentile_mean)

    #percentage difference
    Perc_diff <- (Mean_diff/Average_mean)*100

    #If Perc_diff is > 0 then increase the probability of instances above the
    #90th percentile for the pixels in mountainous municipalities by the
    #percentage difference between the means (or the threshold value)
    if(Perc_diff >0){

      #check threshold
      if(abs(Perc_diff) < Prob_perturb_thresh){Perc_diff <- Prob_perturb_thresh}

      #increase the values
      Intersecting[Intersecting > Intersect_percentile] <-Intersecting[Intersecting > Intersect_percentile] + (Intersecting[Intersecting > Intersect_percentile]/100)*Perc_diff

      #replace any values greater than 1 with 1
      Intersecting[Intersecting > 1] <- 1

      #index which cells need to have value updated
      ix <- Intersecting > Intersect_percentile

      #replace values in target raster
      Prob_raster_stack@layers[[Target_classes]][ix] <- Intersecting[ix]


      #else if Perc_diff is >0 then increase the probability of instances above the
      #90th percentile for the outside pixels by the percentage difference
      #between the means
      }else if(Perc_diff <0){

      #check threshold
      if(abs(Perc_diff) < Prob_perturb_thresh){Perc_diff <- -c(Prob_perturb_thresh)}

      non_intersecting[non_intersecting > Nonintersect_percentile] <- non_intersecting[non_intersecting > Nonintersect_percentile] + (non_intersecting[non_intersecting > Nonintersect_percentile]/100)*Perc_diff

      #replace any values greater than 1 with 1
      non_intersecting[non_intersecting > 1] <- 1

      #index which cells need to have value updated
      ix <- non_intersecting > Nonintersect_percentile

      #replace values in target raster
      Prob_raster_stack@layers[[Target_classes]][ix] <- non_intersecting[ix]
      } #close else if statement

    }#close Mountain_development chunk

    #--------------------------------------------------------------------------
    # Rural_migration intervention
    #--------------------------------------------------------------------------
    if(Intervention_ID == "Rural_migration"){

    #load municipality typology raster
    Intervention_rast <- raster(Intervention_data)

    #seperate raster legend and recode values for remote rural municaplities
    Leg <- Intervention_rast@data@attributes[[1]]
    Leg[Leg$ID %in% c(325, 326, 327, 335, 338), "type"] <- 1
    Leg[Leg$type != 1, "type"] <- NA
    Leg$type <- as.numeric(Leg$type)

    #reclassify raster
    Intervention_rast <- reclassify(Intervention_rast, rcl = Leg)

    #identify pixels inside of remote rural municipalities
    Intersecting <- mask(Prob_raster_stack@layers[[Target_classes]], Intervention_rast == 1)

    #identify pixels outside of remote rural municipalities
    non_intersecting <- overlay(Prob_raster_stack@layers[[Target_classes]],Intervention_rast,fun = function(x, y) {
      x[y==1] <- NA
      return(x)
    })

    #Because the intended effect of the intervention is to increase the
    #probability of urban development in the remote rural municipalities

    #calculate 90th percentile value of probability for pixels inside and outside
    #excluding cells with a value of 0 in both cases
    Intersect_percentile <- quantile(Intersecting@data@values[Intersecting@data@values >0], probs = 0.90, na.rm=TRUE)
    Nonintersect_percentile <- quantile(non_intersecting@data@values[non_intersecting@data@values >0], probs = 0.90, na.rm=TRUE)

    #get the means of the values above the percentiles
    Intersect_percentile_mean <- mean(Intersecting@data@values[Intersecting@data@values >= Intersect_percentile], na.rm = TRUE)
    Nonintersect_percentile_mean <- mean(non_intersecting@data@values[non_intersecting@data@values >= Nonintersect_percentile], na.rm = TRUE)

    #mean difference
    Mean_diff <- Nonintersect_percentile_mean - Intersect_percentile_mean

    #Average of means
    Average_mean <- mean(Intersect_percentile_mean, Nonintersect_percentile_mean)

    #percentage difference
    Perc_diff <- (Mean_diff/Average_mean)*100

    #If Perc_diff is > 0 then increase the probability of instances above the
    #90th percentile for the pixels in remote rural municipalities by the
    #percentage difference between the means (or the threshold value)
    if(Perc_diff >0){

      #check threshold
      if(abs(Perc_diff) < Prob_perturb_thresh){Perc_diff <- Prob_perturb_thresh}

      #increase the values
      Intersecting[Intersecting > Intersect_percentile] <-Intersecting[Intersecting > Intersect_percentile] + (Intersecting[Intersecting > Intersect_percentile]/100)*Perc_diff

      #replace any values greater than 1 with 1
      Intersecting[Intersecting > 1] <- 1

      #index which cells need to have value updated
      ix <- Intersecting > Intersect_percentile

      #replace values in target raster
      Prob_raster_stack@layers[[Target_classes]][ix] <- Intersecting[ix]


      #else if Perc_diff is >0 then increase the probability of instances above the
      #90th percentile for the pixels outside the remote rural municipalities
      #by the percentage difference between the means
      }else if(Perc_diff <0){

      #check threshold
      if(abs(Perc_diff) < Prob_perturb_thresh){Perc_diff <- -c(Prob_perturb_thresh)}

      non_intersecting[non_intersecting > Nonintersect_percentile] <- non_intersecting[non_intersecting > Nonintersect_percentile] + (non_intersecting[non_intersecting > Nonintersect_percentile]/100)*Perc_diff

      #replace any values greater than 1 with 1
      non_intersecting[non_intersecting > 1] <- 1

      #index which cells need to have value updated
      ix <- non_intersecting > Nonintersect_percentile

      #replace values in target raster
      Prob_raster_stack@layers[[Target_classes]][ix] <- non_intersecting[ix]
      } #close else if statement

    }#close Rural_migration chunk

    #--------------------------------------------------------------------------
    # Agri_abandonment intervention
    #--------------------------------------------------------------------------

    #The predicted probability of cells to transition from agriculture to other
    #LULC classes already uses accessibility based predictors such as
    #distance to roads/slope however other variables e.g climaticor soil may be having
    #a larger effect hence we should apply a simple analysis based upon the
    #model used by Gellrich et al. 2007 that considers distance to roads,
    #slope and distance to building zones as a measure of 'marginality' and
    #then select the 90th percentile of pixels according to this value

    #load the static predictor layers and re-scale between 0-1

    #function for rescaling:
    rescale <- function(x, x.min, x.max, new.min = 0, new.max = 1) {
    if(is.null(x.min)) {x.min = min(x)}
    if(is.null(x.max)) {x.max = max(x)}
    new.min + (x - x.min) * ((new.max - new.min) / (x.max - x.min))
    }

    #distance to roads
    Dist2rds <- raster("Data/Preds/Prepared/Layers/Transport/Distance_to_roads_mean_100m.tif")
    Dist2rds <- calc(Dist2rds, function(x) rescale(x,x.min= minValue(Dist2rds),
                                       x.max = maxValue(Dist2rds)))

    #Slope
    Slope <- raster("Data/Preds/Prepared/Layers/Topographic/Slope_mean_100m.tif")
    Slope <- calc(Slope, function(x) rescale(x, x.min= minValue(Slope),
                                       x.max = maxValue(Slope)))

    # Distance to building zones
    #This layer needs to be inverted when re-scaling because
    #greater distance from building zones means lower land cost
    #which means less likely to abandon hence x.min and x.max values swapped
    Dist2BZ <- raster("Data/Spat_prob_perturb_layers/Bulding_zones/BZ_distance.tif")
    Dist2BZ <- calc(Dist2BZ, function(x) rescale(x, x.min= maxValue(Dist2BZ),
                                       x.max = minValue(Dist2BZ)))

    #stack dist2rds, slope and forest_dist layers and sum values as raster
    Marginality_rast <- calc(stack(Dist2rds, Slope), sum)

    #subset the marginality raster to only the pixels of the agricultural
    #land types (Int_AG, Alp_Past)
    Agri_rast <- rasterFromXYZ(Raster_prob_values[,c("x", "y", "Alp_Past")])
    Agri_rast[Agri_rast == 0] <- NA
    Agri_marginality <- mask(Marginality_rast, Agri_rast)

    #calculate the upper quartile value of marginality for the agricultural cells
    Marginality_percentile <- quantile(Agri_marginality@data@values, probs = 0.75, na.rm=TRUE)

    #indexes of all cells above/below the upper quartile
    marginal_index <- Agri_marginality > Marginality_percentile
    non_marginal_index <- Agri_marginality < Marginality_percentile

    #loop over target classes
    for(class in Target_classes){}
    class <- Target_classes[[1]]

    #calculate the average probability of transition to the target in the
    #marginal agricultural cells vs. non-marginal
    marginal_cells <- Prob_raster_stack@layers[[class]][marginal_index]
    marginal_percentile <- quantile(marginal_cells[marginal_cells >0],probs = 0.9, na.rm=TRUE)
    mean_marginal_percentile <- mean(marginal_cells[marginal_cells > marginal_percentile])

    non_marginal_cells <- Prob_raster_stack@layers[[class]][non_marginal_index]
    nonmarginal_percentile <- quantile(non_marginal_cells[non_marginal_cells >0], probs = 0.9, na.rm=TRUE)
    mean_nonmarginal_percentile <- mean(non_marginal_cells[non_marginal_cells > nonmarginal_percentile])

    #} close loop over target classes

    writeRaster(Agri_marginality, "E:/LULCC_CH/Data/Temp/Intervention_raster.tif", overwrite = TRUE)
    writeRaster(Agri_rast, "E:/LULCC_CH/Data/Temp/Target_raster.tif", overwrite = TRUE)
    writeRaster(Intersecting, "E:/LULCC_CH/Data/Temp/Intersecting_raster.tif", overwrite = TRUE)
    writeRaster(non_intersecting, "E:/LULCC_CH/Data/Temp/Non_Intersect_raster.tif", overwrite = TRUE)

    #--------------------------------------------------------------------------
    #Protection intervention
    #--------------------------------------------------------------------------


  #convert raster stack back to table

  #}close loop over interventions
  #}close if statement

#} close function



    # if(Perturbation_ID == "Land_fragment"){
    #
    # #identify existing settlements with surrounding buffer in order to create regions in which to
    # #calculate landscape fragmentation
    #
    # #calculate effective mesh size for natural and semi-natural land covers
    # #take mean and identify cells in the upper quartile?
    # #(lower mesh size value = greater fragmentation)
    # landscapemetrics::lsm_l_mesh(landscape= current_lulc_map, directions=)
    # }
