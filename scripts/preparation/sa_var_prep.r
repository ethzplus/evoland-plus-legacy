#############################################################################
## Convert predictor (covariate) data layers into a uniform 100m resolution, crs and extent
## Layers produced are saved in a logical folder structure with the suffix of each file name corresponding to the covariate number
## detailed in the data table: C:/Users/bblack/switchdrive/Private/PhD/Modelling/Dry_run/Documentation/Covariate data for dry-run.xlsx
## Date: 01-08-2021
## Author: Ben Black
############################################################################

### =========================================================================
### A- Preparation
### =========================================================================
# Set your working directory
wpath <- "E:/LULCC_CH"
setwd(wpath)

# Install packages if they are not already installed
packs<-c("foreach", "doMC", "data.table", "raster", "tidyverse", "testthat", "sjmisc", "tictoc", "parallel", "terra", "pbapply", "rgdal", "rgeos", "sf", "tiff")

new.packs<-packs[!(packs %in% installed.packages()[,"Package"])]

if(length(new.packs)) install.packages(new.packs)

# Load required packages
invisible(lapply(packs, require, character.only = TRUE))

#Load in the grid to use use for re-projecting the CRS and extent of covariate data
Ref_grid <- raster("Data/Ref_grid.gri")

#create directories
dir.create("Data/Preds/Prepared/Stacks", recursive = TRUE)
dir.create("Data/Preds/Prepared/Layers", recursive = TRUE)

#modification to predictor table
#get names of sheets to loop over
sheets <- excel_sheets("Data/Preds/Predictor_table.xlsx")

#load all sheets as a list
# Pred_tables <- sapply(sheets, function(x) read.xlsx("Data/Preds/Predictor_table.xlsx", sheet = x))
#
# Pred_tables_update <- lapply(Pred_tables, function(x){
# x$File_name <- str_replace_all(str_remove_all(x$File_name, "Prepared/"), "Raw/", "Prepared/Layers/")
# return(x)
# })
#
# #save the updated tables
# openxlsx::write.xlsx(Pred_tables_update, file = "Data/Preds/Predictor_table.xlsx", overwrite = TRUE)


### =========================================================================
### B- Load in covariate layers that need to be re-projected and aggregated###
### =========================================================================





#create a list of raw data rasters then loop over them and change extent, res and crs
#before saving to the correct location in: Preds/Prepared/Layers/'pred name'/'layer'
test <- list.files("E:/LULCC_CH/Data/Preds/Raw", recursive = TRUE, full.names = TRUE)



#socio-economic covariates

#municipality level employment data from Gerecke et al. 2019
#read in the rasters for both sectors at each time point

Sect1_1985 <- raster("Covariates/Socio_economic/Employment/Municipality_employment/Original/SecTor1_1985.tif")
Sect1_1997 <- raster("Covariates/Socio_economic/Employment/Municipality_employment/Original/SECTor1_1997.tif")
Sect1_2009 <- raster("Covariates/Socio_economic/Employment/Municipality_employment/Original/Sector1_2009.tif")
Sect23_1985 <- raster("Covariates/Socio_economic/Employment/Municipality_employment/Original/Sector23_1985.tif")
Sect23_1997 <- raster("Covariates/Socio_economic/Employment/Municipality_employment/Original/Sector23_1997.tif")
Sect23_2009 <- raster("Covariates/Socio_economic/Employment/Municipality_employment/Original/Sector23_2009.tif")

        #change extent of layers
        Sect1_1985 <- setExtent(Sect1_1985, Ref_grid)
        Sect1_1997 <- setExtent(Sect1_1997, Ref_grid)
        Sect1_2009 <- setExtent(Sect1_2009, Ref_grid)
        Sect23_1985 <- setExtent(Sect23_1985, Ref_grid)
        Sect23_1997 <- setExtent(Sect23_1997, Ref_grid)
        Sect23_2009 <- setExtent(Sect23_2009, Ref_grid)

        #removing negative values and NAs
        values(Sect1_1985)[is.na(values(Sect1_1985))] <- 0
        values(Sect1_1997)[is.na(values(Sect1_1997))] <- 0
        values(Sect1_2009)[is.na(values(Sect1_2009))] <- 0
        values(Sect23_1985)[is.na(values(Sect23_1985))] <- 0
        values(Sect23_1997)[is.na(values(Sect23_1997))] <- 0
        values(Sect23_2009)[is.na(values(Sect23_2009))] <- 0
        Sect1_1985[Sect1_1985<0]<-0
        Sect1_1997[Sect1_1997<0]<-0
        Sect1_2009[Sect1_2009<0]<-0
        Sect23_1985[Sect23_1985<0]<-0
        Sect23_1997[Sect23_1997<0]<-0
        Sect23_2009[Sect23_2009<0]<-0

        #calculating change in employment numbers between periods
        Primary_period1 <- Sect1_1997-Sect1_1985
        Primary_period2 <- Sect1_2009-Sect1_1997
        S23_period1 <- Sect23_1997-Sect23_1985
        S23_period2 <- Sect23_2009-Sect23_1997

        #resampling
        Primary_period1_res <- resample(Primary_period1, Ref_grid)
        Primary_period2_res <- resample(Primary_period2, Ref_grid)
        S23_period1_res <- resample(S23_period1, Ref_grid)
        S23_period2_res <- resample(S23_period2, Ref_grid)

        #write rasters
        writeRaster(Sect1_1985, "Covariates/Socio_economic/Employment/Municipality_employment/Prepared/SecTor1_1985", overwrite = TRUE)
        writeRaster(Sect1_1997, "Covariates/Socio_economic/Employment/Municipality_employment/Prepared/SecTor1_1997", overwrite = TRUE)
        writeRaster(Sect1_2009, "Covariates/Socio_economic/Employment/Municipality_employment/Prepared/SecTor1_2009", overwrite = TRUE)
        writeRaster(Sect23_1985, "Covariates/Socio_economic/Employment/Municipality_employment/Prepared/SecTor23_1985", overwrite = TRUE)
        writeRaster(Sect23_1997, "Covariates/Socio_economic/Employment/Municipality_employment/Prepared/SecTor23_1997", overwrite = TRUE)
        writeRaster(Sect23_2009, "Covariates/Socio_economic/Employment/Municipality_employment/Prepared/SecTor23_2009", overwrite = TRUE)

        writeRaster(Primary_period1_res, "Covariates/Socio_economic/Employment/Municipality_employment/Prepared/Primary_employment_change_85_97_cov10.tif", overwrite = TRUE)
        writeRaster(Primary_period2_res, "Covariates/Socio_economic/Employment/Municipality_employment/Prepared/Primary_employment_change_97_09_cov11.tif", overwrite = TRUE)
        writeRaster(S23_period1_res, "Covariates/Socio_economic/Employment/Municipality_employment/Prepared/Sect23_employment_change_85_97_cov12.tif", overwrite = TRUE)
        writeRaster(S23_period2_res, "Covariates/Socio_economic/Employment/Municipality_employment/Prepared/Sect23_employment_change_97_09_cov13.tif", overwrite = TRUE)

#Soil variables: Descombes et al. 2020
    #Soil aeration
    soil_aeration_org <- raster("Covariates/Soil/Original/SPEEDMIND_SoilD.tif")
    plot(soil_aeration_org)
    #re-project and change resolution
    Soil_aeration <- projectRaster(soil_aeration_org, crs= crs(Ref_grid), res = res(Ref_grid))
    plot(Soil_aeration)
    Soil_aeration <- resample(Soil_aeration, Ref_grid)
    plot(Soil_aeration)
    extent(Soil_aeration)
    writeRaster(Soil_aeration, "Covariates/Soil/Prepared/Soil_aeration_cov5.tif", overwrite=TRUE)

    #Soil moisture
    soil_moisture_org <- raster("Covariates/Soil/Original/SPEEDMIND_SoilF.tif")
    #re-project and change resolution
    Soil_moisture <- projectRaster(soil_moisture_org, crs= crs(Ref_grid), res = res(Ref_grid))
    Soil_moisture <- resample(Soil_moisture, Ref_grid)
    extent(Soil_moisture)
    plot(Soil_moisture)
    res(Soil_moisture)
    writeRaster(Soil_moisture, "Covariates/Soil/Prepared/Soil_moisture_cov3.tif", overwrite=TRUE)

    #Soil humus
    soil_humus_org <- raster("Covariates/Soil/Original/SPEEDMIND_SoilH.tif")
    #re-project and change resolution
    Soil_humus <- projectRaster(soil_humus_org, crs= crs(Ref_grid), res = res(Ref_grid))
    Soil_humus <- resample(Soil_humus, Ref_grid)
    writeRaster(Soil_humus, "Covariates/Soil/Prepared/Soil_humus_cov6.tif", overwrite=TRUE)

    #Soil nutrients
    soil_nutrients_org <- raster("Covariates/Soil/Original/SPEEDMIND_SoilN.tif")
    #re-project and change resolution
    Soil_nutrients <- projectRaster(soil_nutrients_org, crs= crs(Ref_grid), res = res(Ref_grid))
    Soil_nutrients <-resample(Soil_nutrients, Ref_grid)
    plot(Soil_nutrients)
    writeRaster(Soil_nutrients, "Covariates/Soil/Prepared/Soil_nutrients_cov2.tif", overwrite=TRUE)

    #Soil PH
    soil_PH_org <- raster("Covariates/Soil/Original/SPEEDMIND_SoilR.tif")
    plot(soil_PH_org)
    #re-project and change resolution
    Soil_PH <- projectRaster(soil_PH_org, crs= crs(Ref_grid), res = res(Ref_grid))
    Soil_PH <- resample(Soil_PH, Ref_grid)
    plot(Soil_PH)
    writeRaster(Soil_PH, "Covariates/Soil/Prepared/Soil_PH_cov1.tif", overwrite=TRUE)

    #Soil moisture_variability
    soil_moisture_variability_org <- raster("Covariates/Soil/Original/SPEEDMIND_SoilW.tif")
    #re-project and change resolution
    Soil_moisture_variability <- projectRaster(soil_moisture_variability_org, crs= crs(Ref_grid), res = res(Ref_grid))
    Soil_moisture_variability <- resample(Soil_moisture_variability, Ref_grid)
    writeRaster(Soil_moisture_variability, "Covariates/Soil/Prepared/Soil_moisture_variability_cov4.tif", overwrite=TRUE)

#Topographic covariates:
    #Elevation (mean)
    Elevation_mean <- readRDS("Covariates/Topographic/dem/Original/ch_topo_alti3d2016_pixel_dem_mean2m.rds")
    Elevation_100m <- aggregate(Elevation_mean, fact=4, fun=mean)
    res(Elevation_100m)
    extent(Elevation_100m)
    crs(Elevation_100m)
    writeRaster(Elevation_100m, "Covariates/Topographic/dem/Prepared/Elevation_mean_100m_cov14.tif", overwrite=TRUE)

    #Aspect (mean)
    Aspect_mean <- readRDS("Covariates/Topographic/aspect/Original/ch_topo_alti3d2016_pixel_aspect_mean2m.rds")
    Aspect_100m <- aggregate(Aspect_mean, fact=4, fun=mean)
    res(Aspect_100m)
    extent(Aspect_100m)
    crs(Aspect_100m)
    writeRaster(Aspect_100m, "Covariates/Topographic/aspect/Prepared/Aspect_mean_100m_cov15.tif", overwrite=TRUE)

    #Hillshade (mean)
    Hillshade_mean <- readRDS("Covariates/Topographic/hillshade/Original/ch_topo_alti3d2016_pixel_hillshade_mean.rds")
    Hillshade_100m <- aggregate(Hillshade_mean, fact=4, fun=mean)
    res(Hillshade_100m)
    extent(Hillshade_100m)
    crs(Hillshade_100m)
    writeRaster(Hillshade_100m, "Covariates/Topographic/hillshade/Prepared/Hillshade_mean_100m_cov16.tif", overwrite=TRUE)

    #Slope
    Slope_mean <- readRDS("Covariates/Topographic/slope/Original/ch_topo_alti3d2016_pixel_slope_mean2m.rds")
    Slope_100m <- aggregate(Slope_mean, fact=4, fun=mean)
    res(Slope_100m)
    extent(Slope_100m)
    crs(Slope_100m)
    writeRaster(Slope_100m, "Covariates/Topographic/slope/Prepared/Slope_mean_100m_cov17.tif", overwrite=TRUE)

    #light
    light_mean <- raster("Covariates/Topographic/light/Original/SPEEDMIND_SoilL.tif")
    #re-project and change resolution
    light_100m <- projectRaster(light_mean, crs= crs(Ref_grid), res = res(Ref_grid))
    light_100m <- resample(light_100m, Ref_grid)
    writeRaster(light_100m, "Covariates/Topographic/light/Prepared/light_100m_cov18.tif", overwrite=TRUE)
    res(light_100m)
    extent(light_100m)
    crs(light_100m)

#Transport-related covariates
    #Noise pollution index (mean from 25m data)
    noise_25m <- readRDS("Covariates/Transport_related/Noise/Original/ch_transport_sonbase_pixel_noise.rds")
    noise_100m <- aggregate(noise_25m, fact=4, fun=mean)
    res(noise_100m)
    extent(noise_100m)
    crs(noise_100m)
    writeRaster(noise_100m, "Covariates/Transport_related/Noise/Prepared/noise_mean_100m_cov19.tif", overwrite=TRUE)

    #Distance to roads (mean distance to all classes from 25m data)
    Distance_to_roads_25m <- readRDS("Covariates/Transport_related/Distance_to_roads/Original/ch_transport_tlm3d_pixel_dist2road_all.rds")
    Distance_to_roads_100m <- aggregate(Distance_to_roads_25m, fact=4, fun=mean)
    res(Distance_to_roads_100m)
    extent(Distance_to_roads_100m)
    crs(Distance_to_roads_100m)
    writeRaster(Distance_to_roads_100m, "Covariates/Transport_related/Distance_to_roads/Prepared/Distance_to_roads_mean_100m_cov20.tif", overwrite=TRUE)
    plot(Distance_to_roads_25m)
    plot(Distance_to_roads_100m)

#Distance to hydrological features covariates
    #Distance to lakes (mean distance from 25m data to all lakes of various categories)
    Distance_to_lakes_25m <- readRDS("Covariates/Hydro/Distance_lakes/Original/ch_hydro_gwn07_pixel_dist2lake_all.rds")
    Distance_to_lakes_100m <- aggregate(Distance_to_lakes_25m, fact=4, fun=mean)
    res(Distance_to_lakes_100m)
    extent(Distance_to_lakes_100m)
    crs(Distance_to_lakes_100m)
    writeRaster(Distance_to_lakes_100m, "Covariates/Hydro/Distance_lakes/Prepared/Distance_to_lakes_mean_100m_cov21.tif", overwrite=TRUE)
    plot(Distance_to_lakes_25m)
    plot(Distance_to_lakes_100m)

    #Distance to rivers (mean distance from 25m data to all rivers of various categories)
    Distance_to_rivers_25m <- readRDS("Covariates/Hydro/Distance_rivers/Original/ch_hydro_gwn07_pixel_dist2riverstrahler_all.rds")
    Distance_to_rivers_100m <- aggregate(Distance_to_rivers_25m, fact=4, fun=mean)
    res(Distance_to_rivers_100m)
    extent(Distance_to_rivers_100m)
    crs(Distance_to_rivers_100m)
    writeRaster(Distance_to_rivers_100m, "Covariates/Hydro/Distance_rivers/Prepared/Distance_to_rivers_mean_100m_cov22.tif", overwrite=TRUE)
    plot(Distance_to_rivers_25m)
    plot(Distance_to_rivers_100m)

       #Continentality
    Continentality <- raster("Covariates/Climatic/Continentality/Original/SPEEDMIND_SoilK.tif")
    #re-project and change resolution
    Continentality_100m <- projectRaster(Continentality, crs= crs(Ref_grid), res = res(Ref_grid))
    Continentality_100m <- resample(Continentality_100m, Ref_grid)
    res(Continentality_100m)
    extent(Continentality_100m)
    crs(Continentality_100m)
    writeRaster(Continentality_100m, "Covariates/Climatic/Continentality/Prepared/Continentality_100m_cov23.tif", overwrite=TRUE)

#Climatic covariates

#periodic averages of each climatic variable for each period

        #first we need an efficient way to find and copy the files from the SwitchDrive 'CH-BMG' folder architecture

        #bio1 predictor
        bio1_dirs <- list.files("C:/Users/bblack/switchdrive/CH-BMG/data/predictors/ch/bioclim/chclim25/present/pixel", pattern = "*bio1.rds$", recursive = TRUE)

        #split files into periods
        bio1_1985 <- bio1_dirs[c(1:5)]
        bio1_1997 <- bio1_dirs[c(6:11)]
        bio1_2009 <- bio1_dirs[c(12:17)]

        #copy the files for each period to folders in my file structure
        rawPath <- "C:/Users/bblack/switchdrive/CH-BMG/data/predictors/ch/bioclim/chclim25/present/pixel"
        bio1_directory_1985 <- "Covariates/Climatic/Periodic_averages/Period1_1981_1985/Original/Avg_ann_temp/"
        bio1_directory_1997 <- "Covariates/Climatic/Periodic_averages/Period2_1993_1997/Original/Avg_ann_temp/"
        bio1_directory_2009 <- "Covariates/Climatic/Periodic_averages/Period3_2005_2009/Original/Avg_ann_temp/"

        file.copy(file.path(rawPath, bio1_1985), bio1_directory_1985 , overwrite = TRUE)
        file.copy(file.path(rawPath, bio1_1997), bio1_directory_1997 , overwrite = TRUE)
        file.copy(file.path(rawPath, bio1_2009), bio1_directory_2009 , overwrite = TRUE)


        #bio12 predictor
        bio12_dirs <- list.files("C:/Users/bblack/switchdrive/CH-BMG/data/predictors/ch/bioclim/chclim25/present/pixel", pattern = "*bio12.rds$", recursive = TRUE)
        #split files into periods
        bio12_1985 <- bio12_dirs[c(1:5)]
        bio12_1997 <- bio12_dirs[c(6:11)]
        bio12_2009 <- bio12_dirs[c(12:17)]

        #copy the files for each period to folders in my file structure
        bio12_directory_1985 <- "Covariates/Climatic/Periodic_averages/Period1_1981_1985/Original/Avg_precip/"
        bio12_directory_1997 <- "Covariates/Climatic/Periodic_averages/Period2_1993_1997/Original/Avg_precip/"
        bio12_directory_2009 <- "Covariates/Climatic/Periodic_averages/Period3_2005_2009/Original/Avg_precip/"

        file.copy(file.path(rawPath, bio12_1985), bio12_directory_1985 , overwrite = TRUE)
        file.copy(file.path(rawPath, bio12_1997), bio12_directory_1997 , overwrite = TRUE)
        file.copy(file.path(rawPath, bio12_2009), bio12_directory_2009 , overwrite = TRUE)


        #gdd0 predictor
        gdd0_dirs <- list.files("C:/Users/bblack/switchdrive/CH-BMG/data/predictors/ch/bioclim/chclim25/present/pixel", pattern = "*gdd0.rds$", recursive = TRUE)
        #split files into periods
        gdd0_1985 <- gdd0_dirs[c(1:5)]
        gdd0_1997 <- gdd0_dirs[c(6:11)]
        gdd0_2009 <- gdd0_dirs[c(12:17)]

        #copy the files for each period to folders in my file structure
        gdd0_directory_1985 <- "Covariates/Climatic/Periodic_averages/Period1_1981_1985/Original/Sum_gdays_0deg/"
        gdd0_directory_1997 <- "Covariates/Climatic/Periodic_averages/Period2_1993_1997/Original/Sum_gdays_0deg/"
        gdd0_directory_2009 <- "Covariates/Climatic/Periodic_averages/Period3_2005_2009/Original/Sum_gdays_0deg/"

        file.copy(file.path(rawPath, gdd0_1985), gdd0_directory_1985 , overwrite = TRUE)
        file.copy(file.path(rawPath, gdd0_1997), gdd0_directory_1997 , overwrite = TRUE)
        file.copy(file.path(rawPath, gdd0_2009), gdd0_directory_2009 , overwrite = TRUE)

        #gdd3 predictor
        gdd3_dirs <- list.files("C:/Users/bblack/switchdrive/CH-BMG/data/predictors/ch/bioclim/chclim25/present/pixel", pattern = "*gdd3.rds$", recursive = TRUE)
        #split files into periods
        gdd3_1985 <- gdd3_dirs[c(1:5)]
        gdd3_1997 <- gdd3_dirs[c(6:11)]
        gdd3_2009 <- gdd3_dirs[c(12:17)]

        #copy the files for each period to folders in my file structure
        gdd3_directory_1985 <- "Covariates/Climatic/Periodic_averages/Period1_1981_1985/Original/Sum_gdays_3deg/"
        gdd3_directory_1997 <- "Covariates/Climatic/Periodic_averages/Period2_1993_1997/Original/Sum_gdays_3deg/"
        gdd3_directory_2009 <- "Covariates/Climatic/Periodic_averages/Period3_2005_2009/Original/Sum_gdays_3deg/"

        file.copy(file.path(rawPath, gdd3_1985), gdd3_directory_1985 , overwrite = TRUE)
        file.copy(file.path(rawPath, gdd3_1997), gdd3_directory_1997 , overwrite = TRUE)
        file.copy(file.path(rawPath, gdd3_2009), gdd3_directory_2009 , overwrite = TRUE)


        #gdd5 predictor
        gdd5_dirs <- list.files("C:/Users/bblack/switchdrive/CH-BMG/data/predictors/ch/bioclim/chclim25/present/pixel", pattern = "*gdd5.rds$", recursive = TRUE)
        #split files into periods
        gdd5_1985 <- gdd5_dirs[c(1:5)]
        gdd5_1997 <- gdd5_dirs[c(6:11)]
        gdd5_2009 <- gdd5_dirs[c(12:17)]

        #copy the files for each period to folders in my file structure
        gdd5_directory_1985 <- "Covariates/Climatic/Periodic_averages/Period1_1981_1985/Original/Sum_gdays_5deg/"
        gdd5_directory_1997 <- "Covariates/Climatic/Periodic_averages/Period2_1993_1997/Original/Sum_gdays_5deg/"
        gdd5_directory_2009 <- "Covariates/Climatic/Periodic_averages/Period3_2005_2009/Original/Sum_gdays_5deg/"

        file.copy(file.path(rawPath, gdd5_1985), gdd5_directory_1985 , overwrite = TRUE)
        file.copy(file.path(rawPath, gdd5_1997), gdd5_directory_1997 , overwrite = TRUE)
        file.copy(file.path(rawPath, gdd5_2009), gdd5_directory_2009 , overwrite = TRUE)


        #Using a loop to calculate average values for each covariate in the years that make each up LULC flying period (i.e. lulc year and preceding 4 years)


        Covariates <- c("Avg_ann_temp", "Avg_precip", "Sum_gdays_0deg", "Sum_gdays_3deg" , "Sum_gdays_5deg")
        Covariates_names_1985 <- c("Avg_ann_temp_cov24", "Avg_precip_cov25", "Sum_gdays_0deg_cov26", "Sum_gdays_3deg_cov27" , "Sum_gdays_5deg_cov28")
        Covariates_names_1997 <- c("Avg_ann_temp_cov29", "Avg_precip_cov30", "Sum_gdays_0deg_cov31", "Sum_gdays_3deg_cov32" , "Sum_gdays_5deg_cov33")
        Covariates_names_2009 <- c("Avg_ann_temp_cov34", "Avg_precip_cov35", "Sum_gdays_0deg_cov36", "Sum_gdays_3deg_cov37" , "Sum_gdays_5deg_cov38")


        #initiate function for period1 (1981-1985) (passing covariates/folders argument)
        batch_rastMean_1985 <- function(Covariates){

        #loop through the different folders
        for (i in 1:length(Covariates)) {

            #get a list of the input rasters in each folder
            #pattern = "*.tif$" filters for main raster files only and skips any associated files (e.g. world files)
            grids <- list.files(paste0("Covariates/Climatic/Periodic_averages/Period1_1981_1985/Original/", Covariates[i], "/"), pattern = "*.rds$")
            x <- lapply(paste0("Covariates/Climatic/Periodic_averages/Period1_1981_1985/Original/", Covariates[i], "/", grids), readRDS)

            #create a raster stack from the input grids (in this example there are 12 tif files in each folder)
            s <-stack(x)

            #run the mean function on the raster stack - i.e. add (non-cumulatively) the rasters together
            r25 <- mean(s)

            #aggregate the raster from 25m to 100m
            r100 <- aggregate(r25, fact=4, fun=mean)

            #write the output raster to file
            r100 <- writeRaster(r100, filename = paste0("Covariates/Climatic/Periodic_averages/Period1_1981_1985/Prepared/", "Average_", Covariates_names_1985[i], ".tif"), overwrite=TRUE)

        }
    }

    #run the function
    batch_rastMean_1985(Covariates)

    #initiate function for period 2 (1993-1997) (passing covariates/folders argument)
    batch_rastMean_1997 <- function(Covariates){

        #loop through the different folders
        for (i in 1:length(Covariates)) {

            #get a list of the input rasters in each folder
            #pattern = "*.tif$" filters for main raster files only and skips any associated files (e.g. world files)
            grids <- list.files(paste0("Covariates/Climatic/Periodic_averages/Period2_1993_1997/Original/", Covariates[i], "/"), pattern = "*.rds$")
            x <- lapply(paste0("Covariates/Climatic/Periodic_averages/Period2_1993_1997/Original/", Covariates[i], "/", grids), readRDS)

            #create a raster stack from the input grids (in this example there are 12 tif files in each folder)
            s <-stack(x)

            #run the mean function on the raster stack - i.e. add (non-cumulatively) the rasters together
            r25 <- mean(s)

            #aggregate the raster from 25m to 100m
            r100 <- aggregate(r25, fact=4, fun=mean)

            #write the output raster to file
            r100 <- writeRaster(r100, filename = paste0("Covariates/Climatic/Periodic_averages/Period2_1993_1997/Prepared/", "Average_", Covariates_names_1997[i], ".tif"), overwrite=TRUE)

        }
    }


    #run the function
    batch_rastMean_1997(Covariates)

    #initiate function for period 3 (2005-2009) (passing covariates/folders argument)
    batch_rastMean_2009 <- function(Covariates){

        #loop through the different folders
        for (i in 1:length(Covariates)) {

            #get a list of the input rasters in each folder
            #pattern = "*.tif$" filters for main raster files only and skips any associated files (e.g. world files)
            grids <- list.files(paste0("Covariates/Climatic/Periodic_averages/Period3_2005_2009/Original/", Covariates[i], "/"), pattern = "*.rds$")
            x <- lapply(paste0("Covariates/Climatic/Periodic_averages/Period3_2005_2009/Original/", Covariates[i], "/", grids), readRDS)

            #create a raster stack from the input grids (in this example there are 12 tif files in each folder)
            s <-stack(x)

            #run the mean function on the raster stack - i.e. add (non-cumulatively) the rasters together
            r25 <- mean(s)

            #aggregate the raster from 25m to 100m
            r100 <- aggregate(r25, fact=4, fun=mean)

            #write the output raster to file
            r100 <- writeRaster(r100, filename = paste0("Covariates/Climatic/Periodic_averages/Period3_2005_2009/Prepared/", "Average_", Covariates_names_2009[i], ".tif"), overwrite=TRUE)

        }
    }

    #run the function
    batch_rastMean_2009(Covariates)

cat(paste0(' Preparation of Suitability and accessibility predictor layers complete \n'))




