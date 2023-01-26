#############################################################################
## Spatial_prob_perturb: Prepare spatial layers for perturbation of cellular
##transition probabilities in simulation steps
##
## Date: 18-11-2021
## Author: Ben Black
#############################################################################

#Vector packages for loading
packs<-c("foreach", "data.table", "raster", "tidyverse", "testthat",
         "sjmisc", "tictoc", "parallel", "terra", "pbapply", "rgdal", "rgeos",
         "sf", "tiff", "bfsMaps", "rjstat", "future.apply", "future", "stringr",
         "stringi" ,"readxl","rlist", "rstatix", "openxlsx", "pxR", "rvest", "landscapemetrics")

new.packs<-packs[!(packs %in% installed.packages()[,"Package"])]

if(length(new.packs)) install.packages(new.packs)

# Load required packages
invisible(lapply(packs, require, character.only = TRUE))

Ref_grid <- raster("Data/Ref_grid.gri")
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
url <- "https://www.kgk-cgc.ch/download_file/237/239.zip"
file <- basename(url)
download.file(url, file)
unzip(file, exdir = tmpdir)
unlink(file)

sp_patterns <- paste0(".shp", ".gpkg", collapse = "" )

shapeFile <- list.files(tmpdir, pattern = ".shp", full.names = TRUE)

#load shapefile
shp_file <- sf::st_read(shapeFile)

#re-project to research CRS
shp_file <- sf::st_transform(shp_file, crs = Ref_crs)

#convert shapefile to raster
BZ_rast <- rasterize(shp_file, Ref_grid, field= shp_file$CH_CODE_HN)

#create a raster attribute table
#using these names to create a raster attribute table (rat)
BZ_rast <- ratify(BZ_rast)
BZ_rat <- levels(BZ_rast)[[1]]

#get the german zone names
BZ_IDs <- unique(shp_file$CH_BEZ_D)

#add name column to RAT
BZ_rat$Class_Names <- str_replace_all(BZ_IDs, " ", "_")

#add RAT to raster object
levels(BZ_rast) <- BZ_rat

#saving the raster in R's native .grd format which preserves the attribute table
writeRaster(BZ_rast, filename= "Data/Spat_prob_perturb_layers/Bulding_zones/BZ_raster.grd", overwrite = TRUE)
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

#remove columns 7 and 9 which specify the other typologies and rename remaining col
muni_typology <- muni_typology[,-c(7,9)]
colnames(muni_typology)[[7]] <- "muni_type_ID"
muni_typology$`BFS-Gde Nummer` <- as.numeric(muni_typology$`BFS-Gde Nummer`)

#create manual legend (data documentation only specifies categories in German/French)
Muni_typ_legend <- data.frame(ID = c(11,12,13,21,22,23,31,32,33),
                              type = c("Municipality_large_agglomeration",
                                    "Municipality_medium-sized_agglomeration",
                                    "Municipality_small_outside_agglomeration",
                                    "High_Density_Periurban",
                                    "Medium_Density_Periurban",
                                    "Low_density_periurban",
                                    "Rural_Center_Community",
                                    "Rural_central_community",
                                    "Rural_peripheral_community"))

#add type to data
muni_typology$muni_type <- sapply(muni_typology$muni_type_ID, function(x){
  Muni_typ_legend[Muni_typ_legend$ID == x, "type"]
})

#add Muni_type_ID to shapefile
Muni_shp@data$muni_type <- as.numeric(sapply(Muni_shp@data$BFS_NUMMER, function(Muni_num){
  type <- muni_typology[muni_typology$`BFS-Gde Nummer` == Muni_num, "muni_type_ID"]
  }, simplify = TRUE))

#rasterize
Muni_type_rast <- rasterize(Muni_shp, Ref_grid, field = "muni_type")

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

### =========================================================================
### write function to perform perturbations
### =========================================================================

#function should be looped over a list of Perturbation ID's according to scenario ID

lulcc.spatprobperturbation <- function(Perturbation_ID, Perturb_perc, Spatial_prob_table, time_step){

  if(Perturbation_ID == "Build_zone"){

  }

  if(Perturbation_ID == "Land_fragment"){

    #identify existing settlements with surrounding buffer in order to create regions in which to
    #calculate landscape fragmentation

    #calculate effective mesh size for natural and semi-natural land covers
    #take mean and identify cells in the upper quartile?
    #(lower mesh size value = greater fragmentation)
    landscapemetrics::lsm_l_mesh(landscape= current_lulc_map, directions=)

  }

}

list_lsm(level = "landscape",
         type=)
