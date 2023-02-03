#############################################################################
## Simulation_predictor_prep: Prepare predictor data lookup tables for simulation
## time steps
##
## Date: 18-11-2021
## Author: Ben Black
#############################################################################

#Vector packages for loading
packs<-c("foreach", "data.table", "raster", "tidyverse",
         "testthat", "sjmisc", "tictoc", "doParallel",
         "lulcc", "pbapply", "stringr", "readr", "openxlsx", "bfsMaps",
         "jsonlite", "httr", "xlsx")

new.packs<-packs[!(packs %in% installed.packages()[,"Package"])]

if(length(new.packs)) install.packages(new.packs)

# Load required packages
invisible(lapply(packs, require, character.only = TRUE))

#Predictor table file path
Pred_table_path <- "Tools/Predictor_table.xlsx"

#Load in the grid to use use for re-projecting the CRS and extent of predictor data
Ref_grid <- raster("Data/Ref_grid.gri")

#vector time steps for future predictions
#Simulation start time is 2020 and end time is 2060
#we have initially agreed to use 5 year time steps
Simulation_start <- 2020
Simulation_end <- 2060
Step_length <- 5

Time_steps <- seq(Simulation_start, Simulation_end, Step_length)

### =========================================================================
### A- Future economic scenarios
### =========================================================================

#base dir for saving
Prepared_FTE_dir <- "Data/Preds/Prepared/Layers/Socio_economic/Employment"

#TO DO: convert process to use zenodo downloading functions from inborutils::
#use Zenodo API service to get URLs for file downloads
ES_data_URLs <- fromJSON(rawToChar(GET("https://zenodo.org/api/records/4774914")[["content"]]))[["files"]][["links"]][["download"]]

#remove files for sensitivity analysis
ES_data_URLs <- ES_data_URLs[-c(5,6)]

#create dir
ES_dir <- "Data/Preds/Raw/Socio_economic/Employment/Employment_scenarios"
dir.create(ES_dir, recursive = TRUE)

#loop over URLS, downloading and unzipping
for(i in ES_data_URLs){

  file <- basename(i)

  #if file is zipped then create temp dir and extract
  if(grepl(".zip", i)== TRUE){
    tmpdir <- tempdir()
    download.file(i, paste0(tmpdir, "/", file))
    unzip(paste0(tmpdir, "/", file), exdir = ES_dir) #unzip folder, saving to new dir
    unlink(tmpdir) #remove temp dir
  }else{ #non-zipped just download
    download.file(i, paste0(ES_dir, "/", file), mode = "wb")
    }
}

#because the future projected economic values as expressed using codes for the
#various Spatial aggregations we need a look up table to parse them to
#class values for spatializing

#Load in the sheet of the metadata file which details the spatial aggregations (regions)
#use read.xlsx2 to not ommit blank columns
Metadat_regions <- xlsx::read.xlsx2("Data/Preds/Raw/Socio_economic/Employment/Employment_scenarios/Metadata.xlsx",
                        sheetName = "Region")
#remove empty row
Metadat_regions <- Metadat_regions[-c(1),]

#replace whitespaces with NA
Metadat_regions <- Metadat_regions %>% mutate_all(na_if,"")

n <- colSums(is.na(Metadat_regions)) == nrow(Metadat_regions) #identify empty columns
cs <- cumsum(n) + 1 #create division vector assign unique number to each group of columns
cs <- cs[!n] #remove empty cols from division vector

#split df according to grouped columns
Lookup_tables_regions <- lapply(unique(cs), function(x){
Dat <- Metadat_regions[, names(cs[cs== x])]
Dat <- Dat[complete.cases(Dat),]
})

#no easy programmatic way to name the different aggregation tables
#so we vector based on manual inspection
#1. Master table with all designations for each municipality
#2. Employment basins (labour market regions) disaggregated by canton
#3. MS (spatial mobility) region designation (106)
#4. Employment basins from 2018 (labour market regions) (101): FSO (2019)
#5. Cantons
#6. Large labour market area (see 4.) (16)
#7. Large Region
#8. Urban-rural typology
names(Lookup_tables_regions) <- c("Master",
                                  "LMR_by_Canton",
                                  "Spatial_mobility_regions",
                                  "LMR_regions",
                                  "Cantons",
                                  "Large_LMR",
                                  "Large_region",
                                  "Urban-rural_type")

#because we are interested in spatializing the projected employment data at the
#level of the labour market regions then only the corresponding lookup table is needed
LMR_lookup <- Lookup_tables_regions[["LMR_regions"]]

#clean names
names(LMR_lookup) <- c("ID", "Region_name")

#remove prefix of ID number
LMR_lookup$ID <- str_remove_all(LMR_lookup$ID, "BE")

#remove leading zeros
LMR_lookup$ID <- sub("^0+", "", LMR_lookup$ID)

#In addition we need a second look up table for aggregating the economic sector
#names in the future data to the primary,secondary and tertiary sectors
#in line with the historic data

#The economic sectors are grouped by their division numbers as detailed in this report:
#https://www.bfs.admin.ch/bfsstatic/dam/assets/347016/master
#Primary sector = divisions 1-3
#Secondary sector = divisions 4-43
#Tertiary sector = 45-98
Sector_divisions <- list(Sec1 = c(0,3),
                         Sec2 = c(4,43),
                         Sec3 = c(45,98))

#Load sheet of metadata linking economic division numbers to category names
Sector_lookup <- xlsx::read.xlsx2("Data/Preds/Raw/Socio_economic/Employment/Employment_scenarios/Metadata.xlsx",
                        sheetName = "Industry")

#remove empty row
Sector_lookup <- Sector_lookup[-1,]

#change column name to avoid accent
names(Sector_lookup)[2] <- "Category"

#Alter division column to single values
Sector_lookup$max_div <- sapply(Sector_lookup$Division, function(x){
  if(grepl(x, pattern = "-")== TRUE){max_value <- as.numeric(str_split(x, "-")[[1]][2])}else{
    max_value <- x
  }
})

#add column with aggregated sector name
Sector_lookup$Sector <- sapply(Sector_lookup$max_div, function(x){
  Sec_test <- sapply(Sector_divisions, function(y){
    between(x, y[1], y[2])
  })
  Sec_name <- names(which(Sec_test == TRUE))
})

#get file paths of future data under all scenarios
Econ_data_paths <- as.list(list.files("Data/Preds/Raw/Socio_economic/Employment/Employment_scenarios", full.names = TRUE))
names(Econ_data_paths) <- str_remove_all(list.files("Data/Preds/Raw/Socio_economic/Employment/Employment_scenarios", full.names = FALSE), ".xlsx")

#exclude the metadata file
Econ_data_paths$Metadata <- NULL

#The string at the start of the file name is an abbreviation of the
#scenario name and the regional variation

Scenario_name <- list(c = "Combined",
                      e = "Ecolo",
                      t = "Techno",
                      r = "Ref")

Scenario_vari <- list(n1 = "Central",
                      ru = "Urban",
                      rp = "Peri_urban")

#clean scenario names
names(Econ_data_paths) <- sapply(names(Econ_data_paths), function(x){
  name <- Scenario_name[substr(x, 1, 1)]
  vari <- Scenario_vari[substr(x, 2, 3)]
  paste0(name, "_", vari)
})

#Subset to which econ scenarios are required for our scenarios

#load scenario specifications table
Scenario_data_table <- openxlsx::read.xlsx("Tools/Scenario_specifications.xlsx", sheet = "Predictor_data")
Scenario_corr <- as.list()
names(Scenario_corr) <- Scenario_data_table[,"Scenario_ID"]

#load shapefile of labour market regions
LMR_shp <- sf::st_read("E:/LULCC_CH/Data/Preds/Raw/CH_geoms/2022_GEOM_TK/03_ANAL/Gesamtfläche_gf/K4_amre_20180101_gf/K4amre_20180101gf_ch2007Poly.shp")

#rasterize
LMR_shp$name <- as.factor(LMR_shp$name)
LMR_shp$rast_ID <- seq(1:nrow(LMR_shp))
LMR_rast <- rasterize(LMR_shp, Ref_grid, field = "rast_ID", fun='last', background=NA)

#upper loop over scenario list
for(i in 1:length(Scenario_corr)){

  #load future dataset
  Econ_dat <- readxl::read_excel(Econ_data_paths[[Scenario_corr[[i]]]], sheet = "Bassins d'emploi")

  #subset to relevant columns
  Econ_dat <- Econ_dat[,c("time", "EmpFTE", "sector", "regionBE")]

  #subset to years of time steps
  Econ_dat <- Econ_dat[Econ_dat$time %in% Time_steps,]

  #add column for economic sector
  Econ_dat$Agg_sec <- sapply(Econ_dat$sector, function(x){
     Sector_lookup[Sector_lookup$Category == x, "Sector"]
  })

  #add region name
  Econ_dat$Region_name <- sapply(Econ_dat$regionBE, function(x){
    LMR_lookup[LMR_lookup$ID == x, "Region_name"]
  })

  #Calculate the average annual difference
  Region_values <- as.data.frame(Econ_dat %>% group_by(Agg_sec, Region_name, time) %>% #grouping
                                   summarize(SumFTE = sum(EmpFTE)) %>% #sum over the economic activities
                                      mutate(Avg_chg_FTE = (SumFTE - lag(SumFTE))/Step_length)) %>% #calculate avg. annual change between time points
                                          select(-(SumFTE))

  #remove incomplete rows (2020 time point)
  Region_values <- Region_values[complete.cases(Region_values), ]

  #add ID for LMR region to match raster values
  Region_values$ID <- sapply(Region_values$Region_name, function(x){
    id <- LMR_shp[LMR_shp$name == x, "rast_ID"][[1]]
    })

  #split by sector
  Sectoral_values <- split(Region_values, Region_values$Agg_sec)

  #loop over sectors, pivoting data to wide and creating rasters
  Future_FTE_file_paths <- lapply(Sectoral_values, function(Sector_dat){

    #pivot to wide
    Sector_dat_wide <- as.data.frame(Sector_dat %>% pivot_wider(names_from = time,
                                                  values_from = Avg_chg_FTE,
                                                  names_sep = "_"))

    #vector column indices for timepoints
    Date_cols <- which(colnames(Sector_dat_wide) %in% paste(Time_steps[-1]))
    names(Date_cols) <- sapply(1:(length(Time_steps)-1), function(x) {
            paste0(Time_steps[x], "_", Time_steps[x+1])})

    #vector file names
    FTE_file_names <- paste0(Prepared_FTE_dir, "/", "Avg_chg_FTE_", names(Date_cols),
                             "_", unique(Sector_dat_wide[,"Agg_sec"]), "_",
                                    names(Scenario_corr)[i],".tif")

    #use subs to match raster values based on ID and repeat across all columns
    Sector_rasts <- subs(LMR_rast,
                  Sector_dat_wide,
                  by='ID',
                  which= Date_cols) #-1 used to exclude '2020'

    #save a seperate file for each layer
    writeRaster(Sector_rasts,
            filename = FTE_file_names,
            bylayer=TRUE,
            format="GTiff",
            overwrite = TRUE)

    return(FTE_file_names)
    }) #close loop over sectors
} #close loop over scenarios

### =========================================================================
### B- future population projections
### =========================================================================

#This process does not result in the creation of spatial layers of
#future population, rather it involves modelling the relationship between
#historic % of the cantonal population and the % of urban area per municipality
#These models are then used within the simulation step to predict % of cantonal
#population based on the current simulated % of urban area per municipality
#with population values then being distributed according the correct projection
#under the scenario being simulated.

### X.1- Prepare historic cantonal population data

#reload historic muni pop data produced in historic predictor prep
raw_mun_popdata <- readRDS("Data/Preds/Raw/Socio_economic/Population/raw_muni_pop_historic.rds")

#load kanton shapefile
Canton_shp <- shapefile("Data/Preds/Raw/CH_geoms/SHAPEFILE_LV95_LN02/swissBOUNDARIES3D_1_3_TLM_KANTONSGEBIET.shp")

#read in population data from html and convert to DF
px_data <- data.frame(read.px("https://dam-api.bfs.admin.ch/hub/api/dam/assets/23164063/master"))

# subset px_data to historic cantonal population data
raw_can_popdata <- px_data[px_data$Demografische.Komponente == "Bestand am 31. Dezember" &
                     px_data$Staatsangehörigkeit..Kategorie. == "Staatsangehörigkeit (Kategorie) - Total" &
                     px_data$Geschlecht == "Geschlecht - Total", c(4:6)]
names(raw_can_popdata) <- c("Name_Canton", "Year", "Population")
raw_can_popdata$Name_Canton <- as.character(raw_can_popdata$Name_Canton)

#remove municipalities records by inverse-matching on the numeric contained in their name
raw_can_popdata <- raw_can_popdata[grep(".*?([0-9]+).*", raw_can_popdata$Name_Canton, invert = TRUE),]

#all cantons
raw_can_popdata <- raw_can_popdata[grep(">>", raw_can_popdata$Name_Canton, invert = TRUE),]
raw_can_popdata <- raw_can_popdata[grep(paste0(unique(Canton_shp@data[["NAME"]]), collapse = "|"), raw_can_popdata$Name_Canton),]

#pivot to wide
raw_can_popdata <- raw_can_popdata %>% pivot_wider(names_from = "Year",
                                                values_from = "Population")

#match Canton names to the shape file
for(i in unique(Canton_shp@data[["NAME"]])){
raw_can_popdata$Name_Canton[grep(i, raw_can_popdata$Name_Canton)] <- i
}

#add cantons numbers
raw_can_popdata$KANTONSNUM <- sapply(raw_can_popdata$Name_Canton, function(x){
unique(Canton_shp@data[Canton_shp@data$NAME == x, "KANTONSNUM"])
})

#get the indices of columns that represent the years
date_cols <- na.omit(as.numeric(gsub(".*?([0-9]+).*", "\\1", colnames(raw_can_popdata))))

#check that the raw municipality pop values sum to the raw cantonal pop values
raw_canton_sums <- do.call(cbind, lapply(date_cols, function(x){
pop_sum <- raw_mun_popdata %>%
  group_by(KANTONSNUM) %>%
  summarise(across(c(paste(x)), sum))
return(pop_sum)
}))

### X.2- Calculate % cantonal population per municipality

pop_percentages <- do.call(cbind, sapply(date_cols, function(year){

#loop over canton numbers
muni_percs <- rbindlist(sapply(unique(raw_can_popdata$KANTONSNUM), function(canton_num){

#subset cantonal data by year
can_data <- raw_can_popdata[raw_can_popdata$KANTONSNUM == canton_num, c(paste(year), "KANTONSNUM")]

#subset the municipality data by kanton name and year
muni_data <- raw_mun_popdata[raw_mun_popdata$KANTONSNUM == can_data$KANTONSNUM, c(paste(year), "KANTONSNUM")]
muni_data$KANTONSNUM <- NULL

#loop over municipalities
muni_data[[paste0("Perc_", year)]]  <- as.numeric(sapply(muni_data[[paste(year)]], function(year_value){
perc_value <- year_value/can_data[,paste(year)]*100
},simplify = TRUE)) #close loop over municipalities

return(muni_data)
}, simplify = FALSE))#close loop over kantons

return(muni_percs)
}, simplify = FALSE)) #close loop over years

#add back in BFS and Cantons numbers
pop_percentages$BFS_NUM <- raw_mun_popdata$BFS_NUM
pop_percentages$KANTONSNUM <- raw_mun_popdata$KANTONSNUM

#check that muni pop percentages equate to 100%
pop_percentages_validation <- do.call(cbind, lapply(date_cols, function(x){
pop_sum <- pop_percentages %>%
  group_by(KANTONSNUM) %>%
  summarise(across(c(paste0("Perc_", x)), sum))
pop_sum$KANTONSNUM <- NULL
return(pop_sum)
}))

### X.3- Calculate % urban area per municipality

#Load the most recent LULC map
current_LULC <- raster("Data/Historic_LULC/LULC_2018_agg.gri")

#subset to just urban cell
Urban_rast <- current_LULC == 10

#Zonal stats to get urban area per kanton
Canton_urban_areas <- raster::extract(Urban_rast, Canton_shp, fun=sum, na.rm=TRUE, df=TRUE)

#append Canton ID
Canton_urban_areas$Canton_num <- Canton_shp$KANTONSNUM

#combine areas for cantons with multiple polygons
Canton_urban_areas <- Canton_urban_areas %>%
  group_by(Canton_num) %>%
  summarise(across(c(layer), sum))

#load the municipality shape file
Muni_shp <- shapefile("Data/Preds/Raw/CH_geoms/SHAPEFILE_LV95_LN02/swissBOUNDARIES3D_1_3_TLM_HOHEITSGEBIET.shp")

#filter out non-swiss municipalities
Muni_shp <- Muni_shp[Muni_shp@data$ICC == "CH" & Muni_shp@data$OBJEKTART == "Gemeindegebiet", ]

#Zonal stats to get number of Urban cells per Municipality polygon
#sum is used as a function because urban cells = 1 all others = 0
Muni_urban_areas <- raster::extract(Urban_rast, Muni_shp, fun=sum, na.rm=TRUE, df=TRUE)

#append Canton and Municipality IDs
Muni_urban_areas$Canton_num <- Muni_shp@data[["KANTONSNUM"]]
Muni_urban_areas$Muni_num <- Muni_shp$BFS_NUMMER
Muni_urban_areas$Perc_urban <- 0

#loop over kanton numbers and calculate municipality urban areas as a % of canton urban area
for(i in Canton_urban_areas$Canton_num){

#vector kanton urban area
Kan_urban_area <- as.numeric(Canton_urban_areas[Canton_urban_areas$Canton_num == i, "layer"])

#subset municipalities to this canton number
munis_indices <- which(Muni_urban_areas$Canton_num == i)

#loop over municipalities in the Canton and calculate their urban areas as a % of the Canton's total
for(muni in munis_indices){
Muni_urban_areas$Perc_urban[muni] <- (Muni_urban_areas[muni, "layer"]/Kan_urban_area)*100
  } #close inner loop
} #close outer loop

### X.4- Model relationship between cantonal % population and % urban area

#subset pop percentages to 2018
Muni_percs <- pop_percentages[, c("BFS_NUM", "KANTONSNUM", "Perc_2018")]
colnames(Muni_percs) <-  c("BFS_NUM", "KANTONSNUM", "Perc_pop")

#combine with % urban values
Muni_percs$Perc_urban <- sapply(Muni_percs$BFS_NUM, function(x){
sum(Muni_urban_areas[Muni_urban_areas$Muni_num == x, "Perc_urban"])
})

#loop over kantons and model relationship
Canton_models <- lapply(unique(Muni_percs$KANTONSNUM), function(canton_num){

#subset to data for this canton
kanton_data <- Muni_percs[Muni_percs$KANTONSNUM == canton_num,]

#produce GLM
Canton_model <- glm(data = kanton_data, formula = Perc_pop ~ Perc_urban, family = gaussian())

return(Canton_model)
})
names(Canton_models) <- unique(Muni_percs$KANTONSNUM)

#save models
saveRDS(Canton_models, "Data/Preds/Tools/Dynamic_pop_models.rds")

### X.5- Prepare Cantonal projections of population development

raw_data_path <- "Data/Preds/Raw/Socio_economic/Population/raw_pop_projections.xlsx"

#Download the required file
download.file(url = "https://dam-api.bfs.admin.ch/hub/api/dam/assets/12107013/master", destfile = raw_data_path, mode="wb")

#Vector existing sheet names
Sheet_names <- getSheetNames(raw_data_path)

#name the sheet names with the new names
names(Sheet_names) <- c("Ref", "High", "Low")

#Create a workbook to add the ckleaned data too
pop_proj_wb <- createWorkbook()

#The population projection table does not contain the canton numbers and because
#the Canton names are in German they cannot be matched with the shapefile
#to get the numbers (e.g Wallis - Valais etc.) instead load a different FSO
#table to get the numbers
Canton_lookup <- read.xlsx("https://www.atlas.bfs.admin.ch/core/projects/13.40/xshared/xlsx/134_131.xlsx", rows = c(4:31), cols = c(2,3))
Canton_lookup$Canton_num <- seq(1:nrow(Canton_lookup))

#loop over time steps adding sheets and adding the predictors to them
for (i in 1:length(Sheet_names)){

org_sheet_name <- Sheet_names[i]
new_sheet_name <- names(Sheet_names)[i]

#load correct sheet of raw data
tempdf <- read.xlsx(raw_data_path, sheet = org_sheet_name, startRow = 2)
colnames(tempdf)[1] = "Canton_name" #rename first column
tempdf <- tempdf[tempdf$Canton_name != "Schweiz",] # remove the total for switzerland
tempdf <- tempdf[complete.cases(tempdf),] #remove incomplete or empty rows
tempdf$Canton_num <- sapply(tempdf$Canton_name, function(x) {
Canton_lookup[grepl(x, Canton_lookup$X2), "Canton_num"]
})
#replace values of non-matching names
tempdf[setdiff(Canton_lookup$Canton_num, tempdf$Canton_num), "Canton_num"] <- Canton_lookup$Canton_num[setdiff(Canton_lookup$Canton_num, tempdf$Canton_num)]
rownames(tempdf) <- 1:nrow(tempdf) #correct row names

#create sheet in workbook, the try() is necessary in case sheets already exist
try(addWorksheet(pop_proj_wb, sheetName = new_sheet_name))

#write the data to the sheet
writeData(pop_proj_wb, sheet = new_sheet_name, x = tempdf)
}

#save workbook
openxlsx::saveWorkbook(pop_proj_wb, "Data/Preds/Tools/Population_projections.xlsx", overwrite = TRUE)

### =========================================================================
### C- future climatic data
### =========================================================================

#



##TEMPORARILY PERFORMING THIS PROCESS FOR BAU USING AGGREGATED CLIMATIC DATA
## ADAPT WHEN FUTURE DATA IS PRODUCED AT 5 YEAR INTERVALS

#upscale climatic data to 100m
#list the files of the raw climatic variables, pattern match on rcp45 because this is all that is need for BAU
Clim_var_paths <- list.files("E:/LULCC_CH/Data/Preds/Raw/Climatic/Future", recursive = TRUE, full.names = TRUE, pattern = "rcp45", ignore.case = TRUE)

Clim_var_names <- c("bio1", "bio12", "gdd0", "gdd3", "gdd5")
names(Clim_var_names) <- Predictor_table[Predictor_table$Predictor_category == "Climatic", "Covariate_ID"][2:6]

#subset to the required variables
Clim_var_paths <- grep(paste0("\\b(", paste(Clim_var_names, collapse="|"), ")\\b"), Clim_var_paths, value = TRUE)
names(Clim_var_paths) <- str_remove_all(str_remove_all(str_replace_all(Clim_var_paths, "Raw", "Prepared/Layers"), ".rData"), ".rds")
print(Clim_var_paths)

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

#list files of aggregated climatic data
Prep_clim_vars_paths <- list.files("Data/Preds/Prepared/Layers/Climatic/Future", recursive = TRUE, full.names = TRUE, pattern = ".tif")

#filter out tif.aux files and remove work dir
Prep_clim_vars_paths <- str_remove_all(grep(Prep_clim_vars_paths, pattern='.aux', invert=TRUE, value=TRUE), wpath)

### =========================================================================
### D- Prepare a table of info for future predictors to be added to predictor lookup table
### =========================================================================

#bind together vectors of file paths for future predictors that have been created
Future_pred_paths <- c(unlist(Future_FTE_file_paths) ,Prep_clim_vars_paths)

#load the predictor table sheet for the most recent calibration period
Predictor_table <- read.xlsx(Pred_table_path, sheetName = "2009_2018")

#filtering to static predictors
Static_preds <- Predictor_table[Predictor_table$Static_or_dynamic == "static",]
Static_preds$Scenario <- "All"

#create DF for capturing info
Dynamic_preds <- data.frame(matrix(ncol = length(colnames(Static_preds)), nrow=1))
colnames(Dynamic_preds) <- colnames(Static_preds)

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
### D- Add static/dynamic variable data to sheets  for future time points
### =========================================================================

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
pred_workbook <- openxlsx::loadWorkbook(file = Pred_table_path)

#loop over time steps adding sheets and adding the predictors to them
for(i in Time_steps){
#the try() is necessary in case sheets already exist
try(addWorksheet(pred_workbook, sheetName = i))
writeData(pred_workbook, sheet = paste(i), x = Combined_vars_for_time_steps[[paste(i)]])
}

#save workbook
openxlsx::saveWorkbook(pred_workbook, Pred_table_path, overwrite = TRUE)

### =========================================================================
### E- create predictor variable stack for each scenario/time point
### =========================================================================

#To do: implement a check in the loop to make sure there are no duplicate
#predictors being included in the stacks

#vector scenario names
Scenario_names <- c("BAU", "BIOPRO", "DIV", "SHAD", "FUTEI")

#upper loop over time steps
sapply(Time_steps, function(sim_year){

  #load corresponding sheet of predictor table
  pred_details <- openxlsx::read.xlsx(Pred_table_path, sheet = paste(sim_year))

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
