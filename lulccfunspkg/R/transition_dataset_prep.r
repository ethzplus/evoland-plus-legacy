#############################################################################
## Transition_dataset_creation: Script for gathering the layers of LULC
## (dependent variable) and predictors for each historic period then separating
## into viable LULC transitions at the scale of Switzerland and Bioregions
## Date: 25-09-2021
## Author: Ben Black (Modified version)
#############################################################################

### =========================================================================
### A- Preparation
### =========================================================================
transition_dataset_prep <- function() {
  # Historic LULC data folder path
  LULC_folder <- "Data/Historic_LULC"

  # character string for data period inherited from LULCC_CH_master
  # otherwise uncomment here:
  # data_periods <- c("1985_1997", "1997_2009", "2009_2018")

  # Produce regionalized datasets?: inherited from LULCC_CH_master
  # regionalization <- TRUE

  # If regionalization = TRUE provide regional raster file path
  Region_raster_path <- "Data/Bioreg_CH"

  # create save dir
  save_dir <- "Data/Transition_datasets/Pre_predictor_filtering"
  Periodic_dirs <- paste0(save_dir, "/", data_periods)
  sapply(Periodic_dirs, function(x) dir.create(x, recursive = TRUE, showWarnings = FALSE))

  ### =========================================================================
  ### B- Load data and filter for each time period
  ### =========================================================================

  # The predictor data table is used to identify the file names of variables that
  # are to be included in the stack for each time period
  # Predictor table file path (received from output_env only uncomment here for testing)
  # pred_table_path <- "Tools/Predictor_table.xlsx"

  # load tables as list
  predictor_tables <- lapply(data_periods, function(x) data.table(openxlsx::read.xlsx(pred_table_path, sheet = x)))
  names(predictor_tables) <- data_periods

  # subsetting to only the necessary columns
  Preds_by_period <- lapply(predictor_tables, function(x) {
    Pred_subset <- x[, c("Prepared_data_path", "Covariate_ID")]
    Pred_subset$File_name <- Pred_subset$Prepared_data_path
    Pred_subset$Prepared_data_path <- NULL
    names(Pred_subset)[names(Pred_subset) == "Covariate_ID"] <- "Layer_name"
    return(Pred_subset)
  })

  # Appending the initial LULC classes (1st year of period) and the
  # outcome LULC (last year of period) as well regional designation
  # create a list of the file paths of the historic LULC rasters
  # pattern matching on the .gri extension first and then
  # excluding the accompanying .ovr files with grep
  LULC_raster_paths <- data.frame(matrix(ncol = 2, nrow = 4))
  colnames(LULC_raster_paths) <- c("File_name", "Layer_name")
  LULC_raster_paths["File_name"] <- as.data.frame(list.files(LULC_folder, pattern = ".gri", full.names = TRUE))
  LULC_raster_paths["Layer_name"] <- str_remove(str_extract(LULC_raster_paths$File_name, "(?<=/)[^/]*$"), ".gri") # extract everything that begins with / and runs to the end of the string.

  # Collect file path for regional raster
  Region_path <- data.frame(matrix(ncol = 2, nrow = 1))
  colnames(Region_path) <- c("File_name", "Layer_name")
  Region_path["File_name"] <- list.files(Region_raster_path, pattern = ".gri", full.names = TRUE)
  Region_path["Layer_name"] <- "Bioregion"

  # Create regexes for LULC periods
  LULC_period_regexes <- lapply(data_periods, function(x) {
    str_replace(x, pattern = "_", "|")
  })
  names(LULC_period_regexes) <- data_periods

  # Filter LULC files by period
  LULC_paths_by_period <- lapply(LULC_period_regexes, function(x) {
    dplyr::filter(LULC_raster_paths, grepl(x, File_name))
  })

  # Change layer names to Initial and Final for easier splitting later
  LULC_paths_by_period <- lapply(LULC_paths_by_period, function(x) {
    x$Layer_name[1] <- "Initial_class"
    x$Layer_name[2] <- "Final_class"
    return(x)
  })

  # Combine predictor paths with LULC (and region if needed)
  if (regionalization == TRUE) {
    Combined_paths_by_period <- lapply(
      (mapply(rbind, Preds_by_period, LULC_paths_by_period, SIMPLIFY = FALSE)),
      function(x) rbind(x, Region_path)
    )
  } else {
    Combined_paths_by_period <- mapply(rbind, Preds_by_period, LULC_paths_by_period, SIMPLIFY = FALSE)
  }

  # read in all rasters in the list to check compatibility before stacking
  Rasters_by_periods <- lapply(Combined_paths_by_period, function(x) {
    raster_list <- lapply(x$File_name, function(y) terra::rast(y))
    names(raster_list) <- x$Layer_name
    raster_list
  })

  ### =========================================================================
  ### C- Confirm Raster compatibility for stacking
  ### =========================================================================

  # Use a function to test rasters in the list against an 'exemplar'
  # which has the extent, crs and resolution that we want
  # in this case the Ref_grid file used for re-sampling some of the predictors.
  # Exemplar raster for comparison
  Exemplar_raster <- terra::rast(ref_grid_path)

  Raster_comparison_results <- lapply(Rasters_by_periods, function(raster_list) {
    lulcc.TestRasterCompatibility(raster_list, Exemplar_raster)
  })
  if ((is_empty(unlist(Raster_comparison_results), first.only = FALSE, all.na.empty = TRUE)) == FALSE) {
    stop("Differences in Raster characteristics means they are unsuitable for stacking
refer to object Raster_comparison_results to locate problems")
  }

  # Create SpatRaster stacks for each time period
  Rasterstacks_by_periods <- mapply(
    function(Raster_list, Period_name) {
      # Combine layers into a single SpatRaster
      raster_stack_for_period <- do.call(terra::c, Raster_list)
      names(raster_stack_for_period) <- names(Raster_list)
      Data_period <- str_remove(Period_name, "Period_")
      saveRDS(raster_stack_for_period, file = paste0("Data/Preds/Prepared/Stacks/Calibration/Pred_stack_", Data_period, ".rds"))
      raster_stack_for_period
    },
    Raster_list = Rasters_by_periods,
    Period_name = names(Rasters_by_periods),
    SIMPLIFY = FALSE
  )
  rm(Rasters_by_periods)

  ### =========================================================================
  ### C.1- Data extraction
  ### =========================================================================

  future::plan(multisession, workers = parallel::availableCores() - 2)

  future.apply::future_lapply(data_periods, function(period) {
    # Select data for period
    period_data <- Rasterstacks_by_periods[[paste(period)]]

    # Convert SpatRaster to data.frame including coordinates
    # Assumes factor layers already set, so class names appear
    df_conversion <- as.data.frame(period_data, xy = TRUE, na.rm = FALSE)

    # Remove NAs
    Trans_data <- na.omit(df_conversion)
    rm(df_conversion, period_data)

    ### =========================================================================
    ### D. Transition dataset creation
    ### =========================================================================
    ### =========================================================================
    ### D. Transition dataset creation
    ### =========================================================================

    # Datasets still contain instances of all class-class transitions which is not
    # useful for modelling because some are not viable. Thus we need to filter out
    # these transitions to give separate data sets for each initial LULC class to
    # final LULC class transition but each needs to contain all of the data points
    # of the other transitions from the same initial class to the other final classes.
    # Given the total number of class-class transitions this could be a
    # lengthy process so first we filter out the transitions that are not viable.
    # This should be a two step process:
    # 1. Filtering out entries based on Initial LULC classes that do not
    # undergo transitions (static class)
    # 2. Use a list of viable transitions to separate out transitions that do not
    # occur at a sufficient rate.
    # read in list of viable transition for period
    # created in script 'Transition_identification'
    viable_trans_list <- readRDS("Tools/Viable_transitions_lists.rds")[[paste(period)]]

    # loop over viable transitions and add column for each to the data with the values:
    # 1: If the row is positive for the given transition: If both Initial and Final classes match that of the transition)
    # 0 :if the row is negative for this transition: If the initial class matches but the final class does not);
    # NA :if the row is Not applicable: neither the initial or final class match that of the transition)
    cat(paste0("Creating transition datasets for period ", period))

    # Load predictor data table
    predictor_table <- openxlsx::read.xlsx(pred_table_path, sheet = period)

    # Create binarized transition datasets for each transition
    # remove rows with static initial class using key.
    Trans_data_subset <- Trans_data[Trans_data$Initial_class_lulc_name != "Static", ]

    # add a numeric ID for recombining later
    Trans_data_subset$Num_ID <- seq.int(nrow(Trans_data_subset))

    # seperate transition related columns
    Trans_rel_cols <- Trans_data_subset[, c("Num_ID", "Initial_class_lulc_name", "Final_class_lulc_name")]

    # create empty list for results
    Trans_DF <- data.frame(matrix(NA,
      nrow = nrow(Trans_rel_cols),
      ncol = nrow(viable_trans_list)
    ))
    colnames(Trans_DF) <- viable_trans_list[, "Trans_name"]

    # Assign 1/0 for each viable transition
    for (row_i in 1:nrow(viable_trans_list)) {
      f <- viable_trans_list[row_i, "Initial_class"]
      t <- viable_trans_list[row_i, "Final_class"]
      Trans_DF[which(Trans_rel_cols$Initial_class_lulc_name == f &
        Trans_rel_cols$Final_class_lulc_name == t), row_i] <- 1
      Trans_DF[which(Trans_rel_cols$Initial_class_lulc_name == f &
        Trans_rel_cols$Final_class_lulc_name != t), row_i] <- 0
    }

    # Combine each transition column with predictor/info cols
    Binarized_trans_datasets <- lapply(viable_trans_list[, "Trans_name"], function(trans_name) {
      Trans_data_combined <- na.omit(cbind(Trans_data_subset, Trans_DF[, trans_name]))
      names(Trans_data_combined)[ncol(Trans_data_combined)] <- trans_name
      Trans_data_combined
    })

    # Rename datasets using period to split LULC classes
    names(Binarized_trans_datasets) <- sapply(
      1:nrow(viable_trans_list),
      function(i) paste0(viable_trans_list[i, "Initial_class"], "/", viable_trans_list[i, "Final_class"])
    )

    # regionalization if needed
    if (regionalization == TRUE) {
      Binarized_trans_datasets_regionalized <- unlist(lapply(Binarized_trans_datasets, function(x) {
        split(x, f = x[["Bioregion_Class_Names"]], sep = "/")
      }), recursive = FALSE)

      # Reverse order of name components
      names(Binarized_trans_datasets_regionalized) <- sapply(
        strsplit(names(Binarized_trans_datasets_regionalized), "\\."),
        function(x) str_replace_all(paste(x[2], x[1], sep = "."), "/", ".")
      )
    }

    names(Binarized_trans_datasets) <- str_replace_all(names(Binarized_trans_datasets), "/", ".")
    rm(Trans_DF, Trans_data_subset, Trans_data, Trans_rel_cols)

    # Loop over transition datasets splitting each into:
    # the transition result column
    # non-transition columns,
    # weight vector,
    # measure of class imbalance
    # number of units in the dataset
    Trans_datasets_full <- lapply(Binarized_trans_datasets, function(x) {
      lulcc.splitforcovselection(x, predictor_table = predictor_table)
    })
    rm(Binarized_trans_datasets)

    if (regionalization == TRUE) {
      Trans_datasets_regionalized <- lapply(Binarized_trans_datasets_regionalized, function(x) {
        lulcc.splitforcovselection(x, predictor_table = predictor_table)
      })
      rm(Binarized_trans_datasets_regionalized)

      #  Remove regional datasets without sufficient transitions
      Trans_datasets_regionalized <- Trans_datasets_regionalized[sapply(Trans_datasets_regionalized, function(x) sum(x[["trans_result"]] == 1)) > 5]
    }

    # Save datasets
    sapply(names(Trans_datasets_full), function(dataset_name) {
      Full_save_path <- paste0(save_dir, "/", period, "/", dataset_name, "_full_ch.rds")
      saveRDS(Trans_datasets_full[[paste(dataset_name)]], Full_save_path)
    })

    if (regionalization == TRUE) {
      sapply(names(Trans_datasets_regionalized), function(dataset_name) {
        Full_save_path <- paste0(save_dir, "/", period, "/", dataset_name, "_regionalized.rds")
        saveRDS(Trans_datasets_regionalized[[paste(dataset_name)]], Full_save_path)
      })
    }

    rm(Trans_datasets_full, Trans_datasets_regionalized)
    cat(paste((paste0("Transition Datasets for:", period, "complete")), "", sep = "\n"))

    gc()
  }) # close loop over periods

  future::plan(sequential)

  cat(paste0("Preparation of transition datasets complete \n"))
}
