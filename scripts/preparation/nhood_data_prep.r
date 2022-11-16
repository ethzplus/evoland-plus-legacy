#############################################################################
## Nhood_data_prep: Neighbourhood effect predictor layer preparation
##
## when devising neighborhood effect predictors there are two considerations:
## 1.The size of the neighborhood (no. of cells: n)
##  2.The decay rate from the central value outwards
##  and to a lesser extent the choice of method for interpolating the decay rate values
##  This script will follow the process for automatic rule detection procedure (ARD)
##  devised by Roodposhti et al. (2020) to test various permutations of these values
##  for more details refer to: http://www.spatialproblems.com/wp-content/uploads/2019/09/ARD.html
##
## Date: 29-06-2021
## Author: Ben Black
#############################################################################

### =========================================================================
### A- Preparation
### =========================================================================
# Set working directory
wpath <- "E:/LULCC_CH"
setwd(wpath)

#navigate to the working directory in the files pane for easy viewing
rstudioapi::filesPaneNavigate(wpath)

# Install packages if they are not already installed
packs<-c("foreach", "doMC", "data.table", "raster", "tidyverse",
         "testthat","sjmisc", "tictoc", "parallel", "terra",
         "pbapply", "readr", "readxl", "openxlsx", "viridis")

new.packs<-packs[!(packs %in% installed.packages()[,"Package"])]

if(length(new.packs)) install.packages(new.packs)

# Load required packages
invisible(lapply(packs, require, character.only = TRUE))

# Source custom functions
invisible(sapply(list.files("Scripts/Functions",
                            pattern = ".R",
                            full.names = TRUE,
                            recursive=TRUE), source))

#create folders required
nhood_folder_names <- c("Data/Preds/Tools/Neighbourhood_details_for_dynamic_updating",
"Data/Preds/Tools/Neighbourhood_matrices",
"Data/Preds/Prepared/Layers/Neighbourhood/Neighbourhood_layers")

sapply(nhood_folder_names, function(x){dir.create(x, recursive = TRUE)})

### =========================================================================
### B- Generating the desired number of random matrices for testing as
### focal windows for neighbourhood effect
### =========================================================================

#ONLY REPEAT THIS SECTION OF CODE IF YOU WISH TO REPLACE THE EXISTING
#RANDOM MATRICES WHICH ARE CARRIED FORWARD IN THE TRANSITION MODELLING

#set the number of neighbourhood windows to be tested and the maximum sizes of moving windows

#Specify sizes of matrices to be used as focal windows
# (each value corresponds to row and col size)
#matrix_sizes <- c(11, 9, 7, 5, 3) #(11x11; 9x9; 7x7; 5x5; 3x3)

#Specify how many random decay rate  matrices should be created for each size
#nw   <- 5 # How many random matrices to create for each matrix size below

#Create matrices
#All_matrices <- lapply(matrix_sizes, function(matrix_dim) {
#matrix_list_single_size <- randomPythagorianMatrix(nw, matrix_dim, interpolation="smooth", search = "random")
#names(matrix_list_single_size) <- c(paste0("n", matrix_dim, "_", seq(1:nw)))
#return(matrix_list_single_size)})

#add top-level item names to list
#names(All_matrices) <- c(paste0("n", matrix_sizes, "_matrices"))

# writing it to a file
#saveRDS(All_matrices, "Data/Preds/Tools/Neighbourhood_matrices/ALL_matrices")


### =========================================================================
### C- Applying the sets of random matrices to create focal window layers for each active LULC type
### =========================================================================

#the active LULC types are:
#Settlement/urban/amenities(raster value= 10)
#Intensive agriculture (15)
#Alpine pastures	(16)
#Grassland or meadows	(17)
#Permanent crops	(18)

#Load back in the matrices
All_matrices <- unlist(readRDS("Data/Preds/Tools/Neighbourhood_matrices/ALL_matrices"), recursive = FALSE)

#adjust names
names(All_matrices) <- sapply(names(All_matrices), function(x) {split_name <- (str_split(x, "[.]"))[[1]][2]})

#Load rasters of LULC data for historic periods (adjust list as necessary)
LULC_years <- lapply(str_extract_all(str_replace_all(Data_periods,
           "_", " "), "\\d+"), function(x) x[[1]])
names(LULC_years) <- paste0("LULC_", LULC_years)

LULC_rasters <- lapply(LULC_years, function(x) {
  LULC_pattern <- glob2rx(paste0("*",x,"*gri*")) #generate regex
  raster(list.files("Data/Historic_LULC", full.names = TRUE, pattern = LULC_pattern)) #load raster
  })

#provide vector of active LULC class names
Active_class_names <- c('Urban', 'Int_AG', 'Alp_Past', 'Grassland', 'Perm_crops')

Nhood_folder_path <- "Data/Preds/Prepared/Layers/Neighbourhood/Neighbourhood_layers/"

#mapply function over the LULC rasters and Data period names
#saves rasters to file and return list of focal layer names
mapply(lulcc.generatenhoodrasters,
               LULC_raster = LULC_rasters,
               Data_period = Data_periods,
               MoreArgs = list(Neighbourhood_matrices = All_matrices,
                               Active_LULC_class_names = Active_class_names,
                               Nhood_folder_path = Nhood_folder_path))

### =========================================================================
### D- Manage file names/details for neighbourhood layers
### =========================================================================

#get names of all neighbourhood layers from files
new_nhood_names <- list.files(path = "Data/Preds/Prepared/Layers/Neighbourhood/Neighbourhood_layers", pattern = ".gri")

#split by period
new_names_by_period <- lapply(Data_periods, function(x) grep(x, new_nhood_names, value = TRUE))
names(new_names_by_period) <- Data_periods

#function to do numerical re-ordering
numerical.reorder <- function(period_names){
split_to_identifier <- sapply(strsplit(period_names, "nhood_n"), "[[", 2)
split_nsize <- as.numeric(sapply(strsplit(split_to_identifier, "_"), "[[", 1))
new_nhood_names_ordered <- period_names[order(split_nsize, decreasing = TRUE)]
}

#use grepl over each period names to extract names by LULC class and then re-order
new_names_period_LULC <- lapply(new_names_by_period, function(x){
LULC_class_names <- lapply(Active_class_names, function(LULC_class_name) {
  names_for_class <- grep(LULC_class_name, x, value = TRUE)
})

LULC_class_names_reordered <-lapply(LULC_class_names, numerical.reorder)

names_for_period <- Reduce(c,LULC_class_names_reordered)
})

#reduce nested list to a single vector of layer names
layer_names <- str_remove(Reduce(c, new_names_period_LULC), ".gri")

#regex strings of period and active class names and matrix_IDs
period_names_regex <- str_c(Data_periods, collapse = "|")
class_names_regex <- str_c(Active_class_names, collapse ="|")
matrix_id_regex <- str_c(names(All_matrices), collapse ="|")

#create data.frame to store details of neighbourhood layers to be used when layers are creating during simulations
Focal_details <- setNames(data.frame(matrix(ncol = 4, nrow= length(layer_names))), c("layer_name", "period", "active_lulc", "matrix_id"))
Focal_details$layer_name <- layer_names
Focal_details$period <- str_extract(Focal_details$layer_name, period_names_regex)
Focal_details$active_lulc <- str_extract(Focal_details$layer_name, class_names_regex)
Focal_details$matrix_id <- str_extract(Focal_details$layer_name, matrix_id_regex)

#save dataframe to use as a look up table in dynamic focal layer creation.
saveRDS(Focal_details, "Data/Preds/Prepared/Layers/Neighbourhood/Focal_layer_lookup")

### =========================================================================
### E- Updating covariate table with layer names- updated xl.
### =========================================================================

#work with the list of layer names nested by period: New_names_period_LULC
nested_layer_names <- lapply(new_names_period_LULC, function(x) str_remove(x, ".gri"))
names(nested_layer_names) <- Data_periods

#append folder path to give full file paths needed for covariate table
nested_filepaths <- lapply(new_names_period_LULC, function(x) paste0("Data/Preds/Prepared/Layers/Neighbourhood/Neighbourhood_layers/", x))
names(nested_filepaths) <- Data_periods

#load covariate table and replace the values in the columns
Covariate_tables <- lapply(Data_periods, function(x) data.table(read.xlsx("Data/Preds/Predictor_table.xlsx", sheet = x)))
names(Covariate_tables) <- Data_periods

#loop over the sets of file paths for each period and adjust sheets

Updated_covariate_tables <- mapply(
  function(file_paths, layer_names, Covariate_table){
Covariate_table[Predictor_category == "Neighbourhood", File_name:= file_paths] #replace file name in rows for neighbourhood predictors
Covariate_table[Predictor_category == "Neighbourhood", Layer_name:= layer_names] #replace layer name in rows for neighbourhood predictors
return(Covariate_table)}, layer_names = nested_layer_names,
                          file_paths = nested_filepaths,
                          Covariate_table = Covariate_tables,
                          SIMPLIFY = FALSE)

openxlsx::write.xlsx(Updated_covariate_tables, file = "Data/Preds/Predictor_table.xlsx", overwrite = TRUE)

### =========================================================================
### F- Example plotting of nhood matrices and decay rates
### =========================================================================

# extrafont::loadfonts(device = "win")
#
# matrices_plot <- plotPythagorianMatrix(All_matrices[[7]])
#
# matrices_decay <- rbindlist(lapply(All_matrices, function(x) plotPythagorianMatrixDecay(x, plot = TRUE)), idcol = "matrix_ID")
#
# matrices_decay$nsize <- sapply(matrices_decay$matrix_ID, function(y) {str_split(y, "_")[[1]][1]})
#
#  Decay_plot <- matrices_decay %>%
#   ggplot( aes(x=x, y=y, group= matrix_ID, color= nsize)) +
#     geom_line() +
#    labs(color = guide_legend(title = "Matrix size:"),
#         x = "Distance from central cell",
#         y = "Cell value")+
#    theme_classic()+
#    theme(
#      text=element_text(family="Times New Roman"),
#      legend.background = element_rect(size = 0.5, colour = "black"),
#      legend.position = c(.8, .5),
#      legend.title = element_text(size = 10),
#      )+
#    scale_colour_discrete(labels = c("11x11", "9x9", "7x7", "5x5", "3x3"))
#
# ggsave(plot = Decay_plot, filename = "E:/Dry_run/Results/Figures/publication_specific/nhood_decay_curves", device='tiff', dpi=300, width = 15, height = 9, units = "cm")

cat(paste0(' Preparation of Neighbourhood predictor layers complete \n'))