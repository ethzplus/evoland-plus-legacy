### =========================================================================
### lulcc.extractsplitrasterstack: Extracting values from raster stack to dataframes and splitting by Bioregion
### =========================================================================
#' returns a list of dataframes named for each Bioregion containing dataframes for each LULC class including all predictor
#' variables and x y coordinates for each cell
#'
#' @author Ben Black
#' @export

  lulcc.extractsplitrasterstack <- function(Rasterstack_for_splitting, Filename_data_period, Folder_path, Split_by){

  df_conversion <- raster::as.data.frame(Rasterstack_for_splitting) #Convert Rasterstack to dataframe, because the LULC and Region layers have attribute tables the function creates two columns for each: Pixel value and class name
  xy_coordinates <- coordinates(Rasterstack_for_splitting) #Get XY coordinates of cells
  df_with_xy <- na.omit(cbind(df_conversion, xy_coordinates)) #cbind XY coordinates to dataframe and remove NAs


  if(Split_by == "NONE"){
   non_split_save_path <- paste0(Folder_path, "/", Filename_data_period, "_full_data")
   saveRDS(df_with_xy, file = non_split_save_path)
  } else if (Split_by != "NONE"){

     DFs_by_bioregion <- split(df_with_xy, f = df_with_xy[[Split_by]]) #Split DF using the Bioregion class names column inherited from the raster attribute table

    #save if desired
    #create Folder path incase it doesn't exist
    #dir.create(Folder_path, recursive = TRUE)
    #Full_save_path <- paste0(Folder_path, "/", Filename_data_period, "_regional_data")
    #saveRDS(DFs_by_bioregion, file = Full_save_path) #save nested list of DFs using folder path argument and periodic naming inherited from input list names
  return(DFs_by_bioregion)
  }

  } #close function