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
PA_SHP <- shapefile("Data/Spat_prob_perturb_layers/Protected_areas/SwissPA.shp")

#subset to only certain types of PAs

#rasterize and save

### =========================================================================
### D- Agricultural areas
### =========================================================================

#waiting for data from final two cantons...

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
Prediction_probs <- readRDS("Data/Spat_prob_perturb_layers/EXP_pred_probs_rescaled.rds")

#dataframe of NA cells to be combined with predicted probabilities
Trans_dataset_na <- readRDS("Data/Spat_prob_perturb_layers/EXP_trans_dataset_NA_values.rds")

#Dataframe of combined predicted probability values and NA values
Raster_prob_values <- readRDS("Data/Spat_prob_perturb_layers/EXP_raster_prob_values.rds")


#process for binding NA values with data
Trans_dataset_na[setdiff(names(Prediction_probs), names(Trans_dataset_na))] <- NA
Raster_prob_values <- rbind(Prediction_probs, Trans_dataset_na)

#sort by ID
Raster_prob_values[order(Raster_prob_values$ID),]

Simulation_time_step <- "2040"
Scenario_ID <- "EI_NAT"

### =========================================================================
### Function to perform perturbations (raster version)
### =========================================================================


lulcc.spatprobperturbation <- function(Perturbation_ID, Raster_prob_values, Time_step){}

  #vector names of columns of probability predictions (matching on Prob_)
  Pred_prob_columns <- grep("Prob_", names(Raster_prob_values), value = TRUE)

  #convert probability table to raster stack
  Prob_raster_stack <- stack(lapply(Pred_prob_columns, function(x) rasterFromXYZ(Raster_prob_values[,c("x", "y", x)])))
  names(Prob_raster_stack@layers) <- Pred_prob_columns

  #load table of scenario interventions
  Interventions <- openxlsx::read.xlsx(Scenario_specs_path, sheet = "Interventions")

  #convert Time_step back to character vector
  Interventions$Time_step <- sapply(Interventions$Time_step, function(x) {
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
    Target_classes <- paste0("Prob_", Current_interventions[i, "Target_classes"])
    Intervention_data <- Current_interventions[i, "Intervention_data"]

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
    non_intersecting <- mask(Prob_raster_stack@layers[[Target_classes]], is.na(Intervention_rast))

    #calculate 90th percentile values of probability for pixels inside vs.outside
    #excluding those with a value of 0
    Intersect_90p <- quantile(Intersecting@data@values[Intersecting@data@values >0], probs = 0.90, na.rm=TRUE)
    Nonintersect_90p <- quantile(non_intersecting@data@values[non_intersecting@data@values >0], probs = 0.90, na.rm=TRUE)

    #get the means of the values above the 90th percentile
    Intersect_90p_mean <- mean(Intersecting@data@values[Intersecting@data@values >= Intersect_90p], na.rm = TRUE)
    Nonintersect_90p_mean <- mean(non_intersecting@data@values[non_intersecting@data@values >= Nonintersect_90p], na.rm = TRUE)

    #mean difference
    Mean_diff <- Intersect_90p_mean- Nonintersect_90p_mean

    #Average of means
    Average_mean <- mean(Intersect_90p_mean, Nonintersect_90p_mean)

    #percentage difference
    Perc_diff <- (Mean_diff/Average_mean)*100

    #increase the probability of instances above the 90th percentile
    #for the outside pixels by percentage difference between the means
    non_intersecting[non_intersecting > Nonintersect_90p] <- non_intersecting[non_intersecting > Nonintersect_90p] + (non_intersecting[non_intersecting > Nonintersect_90p]/100)*Perc_diff

    #replace any values greater than 1 with 1
    non_intersecting[non_intersecting > 1] <- 1

    #index which cells need to have value updated
    ix <- non_intersecting > Nonintersect_90p

    #replace values in target raster
    Prob_raster_stack@layers[[Target_classes]][ix] <- non_intersecting[ix]

    }#close Urban_sprawl chunk

    #--------------------------------------------------------------------------
    # Urban_migration intervention
    #--------------------------------------------------------------------------
    if(Intervention_ID == "Urban_migration"){}

    #--------------------------------------------------------------------------
    #Mountain_development intervention
    #--------------------------------------------------------------------------
    if(Intervention_ID == "Mountain_development"){}

    #--------------------------------------------------------------------------
    #Rural_migration intervention
    #--------------------------------------------------------------------------
    if(Intervention_ID == "Rural_migration"){}


    # writeRaster(Intervention_rast, "E:/LULCC_CH/Data/Temp/Intervention_raster.tif")
    # writeRaster(Prob_raster_stack@layers[[Target_classes]], "E:/LULCC_CH/Data/Temp/Target_raster.tif")
    # writeRaster(Intersect_rast, "E:/LULCC_CH/Data/Temp/Intersect_raster.tif", overwrite = TRUE)
    # writeRaster(Intersecting, "E:/LULCC_CH/Data/Temp/Intersecting_raster.tif", overwrite = TRUE)
    # writeRaster(non_intersecting, "E:/LULCC_CH/Data/Temp/Non_Intersect_raster.tif", overwrite = TRUE)


  if(Perturbation_ID == "Land_fragment"){

    #identify existing settlements with surrounding buffer in order to create regions in which to
    #calculate landscape fragmentation

    #calculate effective mesh size for natural and semi-natural land covers
    #take mean and identify cells in the upper quartile?
    #(lower mesh size value = greater fragmentation)
    landscapemetrics::lsm_l_mesh(landscape= current_lulc_map, directions=)

}



  #convert raster stack back to table

  #}close loop over interventions
  #}close if statement

#} close function


