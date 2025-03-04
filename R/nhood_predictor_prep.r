#' Nhood_data_prep: Neighbourhood effect predictor layer preparation
#'
#' When devising neighborhood effect predictors there are two considerations:
#' 1.The size of the neighborhood (no. of cells: n)
#' 2.The decay rate from the central value outwards and to a lesser extent the choice of
#' method for interpolating the decay rate values.
#'
#' This script will follow the process for automatic rule detection procedure (ARD)
#' devised by Roodposhti et al. (2020) to test various permutations of these values; for
#' more details refer to:
#' http://www.spatialproblems.com/wp-content/uploads/2019/09/ARD.html
#'
#' @author Ben Black
#' @param config list of configuration parameters
#' @param redo_random_matrices logical indicating whether to re-generate the random matrices

nhood_predictor_prep <- function(config = get_config(), redo_random_matrices = FALSE) {
  ### =========================================================================
  ### A- Preparation
  ### =========================================================================
  # vector years of LULC data
  LULC_years <- gsub(
    ".*?([0-9]+).*",
    "\\1",
    list.files(config[["historic_lulc_basepath"]], full.names = FALSE, pattern = ".gri")
  )

  # create a list of the data/modelling periods
  LULC_change_periods <- c()
  for (i in 1:(length(LULC_years) - 1)) {
    LULC_change_periods[[i]] <- c(LULC_years[i], LULC_years[i + 1])
  }
  names(LULC_change_periods) <- sapply(
    LULC_change_periods,
    function(x) paste(x[1], x[2], sep = "_")
  )

  # character string for data period
  data_periods <- names(LULC_change_periods)

  # create folders required
  nhood_folder_names <- c(
    file.path(config[["preds_tools_dir"]], "neighbourhood_details_for_dynamic_updating"),
    file.path(config[["preds_tools_dir"]], "neighbourhood_matrices"),
    file.path(config[["prepped_lyr_path"]], "neighbourhood")
  )

  purrr::walk(nhood_folder_names, ensure_dir)

  ### =========================================================================
  ### B- Generating the desired number of random matrices for testing as
  ### focal windows for neighbourhood effect
  ### =========================================================================
  all_matrices_path <- file.path(
    config[["preds_tools_dir"]], "neighbourhood_matrices", "all_matrices.rds"
  )
  if (redo_random_matrices) {
    # FIXME this looks like you could simply set the seed to be reproducible?
    # ONLY REPEAT THIS SECTION OF CODE IF YOU WISH TO REPLACE THE EXISTING RANDOM
    # MATRICES WHICH ARE CARRIED FORWARD IN THE TRANSITION MODELLING set the number of
    # neighbourhood windows to be tested and the maximum sizes of moving windows Specify
    # sizes of matrices to be used as focal windows (each value corresponds to row and
    # col size)
    matrix_sizes <- c(11, 9, 7, 5, 3) # (11x11; 9x9; 7x7; 5x5; 3x3)

    # Specify how many random decay rate  matrices should be created for each size
    nw <- 5 # How many random matrices to create for each matrix size below

    # Create matrices
    All_matrices <- lapply(matrix_sizes, function(matrix_dim) {
      matrix_list_single_size <- random_pythagorean_matrix(
        n = nw, x = matrix_dim,
        interpolation = "smooth", search = "random"
      )
      names(matrix_list_single_size) <- c(paste0("n", matrix_dim, "_", seq(1:nw)))
      return(matrix_list_single_size)
    })

    # add top-level item names to list
    names(All_matrices) <- c(paste0("n", matrix_sizes, "_matrices"))

    # writing it to a file
    saveRDS(All_matrices, all_matrices_path)
  }

  ### =========================================================================
  # C- Applying the sets of random matrices to create focal window layers for each
  # active LULC type
  ### =========================================================================
  # Load back in the matrices
  All_matrices <- unlist(readRDS(all_matrices_path), recursive = FALSE)

  # adjust names
  names(All_matrices) <- sapply(names(All_matrices), function(x) {
    stringr::str_split(x, "[.]")[[1]][2]
  })

  # Load rasters of LULC data for historic periods (adjust list as necessary)
  # FIXME until here, LULC_years is a vector, from here on out it's a list
  LULC_years <- lapply(
    stringr::str_extract_all(stringr::str_replace_all(data_periods, "_", " "), "\\d+"),
    function(x) x[[1]]
  )
  names(LULC_years) <- paste0("LULC_", LULC_years)

  LULC_rasters <- lapply(LULC_years, function(x) {
    LULC_pattern <- glob2rx(paste0("*", x, "*grd")) # generate regex
    raster::raster(
      list.files(config[["historic_lulc_basepath"]],
        full.names = TRUE,
        pattern = LULC_pattern
      )
    )
  })

  # provide vector of active LULC class names
  # FIXME this should be predictor metadata, not hardcoded
  Active_class_names <- c("Urban", "Int_AG", "Alp_Past", "Grassland", "Perm_crops")

  Nhood_folder_path <- file.path(
    config[["prepped_lyr_path"]],
    "Neighbourhood"
  )

  # mapply function over the LULC rasters and Data period names
  # saves rasters to file and return list of focal layer names
  future.apply::future_mapply(
    lulcc_generatenhoodrasters,
    lulc_raster = LULC_rasters,
    data_period = data_periods,
    MoreArgs = list(
      neighbourhood_matrices = All_matrices,
      active_lulc_class_names = Active_class_names,
      nhood_folder_path = Nhood_folder_path
    ),
    future.seed = TRUE
  )

  ### =========================================================================
  ### D- Manage file names/details for neighbourhood layers
  ### =========================================================================

  # get names of all neighbourhood layers from files
  new_nhood_names <- list.files(
    path = Nhood_folder_path,
    pattern = ".grd",
    full.names = TRUE
  )

  # split by period
  new_names_by_period <- lapply(data_periods, function(x) grep(x, new_nhood_names, value = TRUE))
  names(new_names_by_period) <- data_periods

  # function to do numerical re-ordering
  numerical.reorder <- function(period_names) {
    split_to_identifier <- sapply(strsplit(period_names, "nhood_n"), "[[", 2)
    split_nsize <- as.numeric(sapply(strsplit(split_to_identifier, "_"), "[[", 1))
    period_names[order(split_nsize, decreasing = TRUE)]
  }

  # use grepl over each period names to extract names by LULC class and then re-order
  new_names_period_LULC <- lapply(new_names_by_period, function(x) {
    LULC_class_names <- lapply(Active_class_names, function(LULC_class_name) {
      grep(LULC_class_name, x, value = TRUE)
    })

    LULC_class_names_reordered <- lapply(LULC_class_names, numerical.reorder)

    return(Reduce(c, LULC_class_names_reordered))
  })

  # reduce nested list to a single vector of layer names
  layer_names <- Reduce(c, new_names_period_LULC)

  # regex strings of period and active class names and matrix_IDs
  period_names_regex <- stringr::str_c(data_periods, collapse = "|")
  class_names_regex <- stringr::str_c(Active_class_names, collapse = "|")
  matrix_id_regex <- stringr::str_c(names(All_matrices), collapse = "|")

  # create data.frame to store details of neighbourhood layers to be used when layers
  # are creating during simulations
  Focal_details <- setNames(
    data.frame(
      matrix(ncol = 4, nrow = length(layer_names))
    ),
    c("layer_name", "period", "active_lulc", "matrix_id")
  )
  Focal_details$Prepared_data_path <- layer_names
  Focal_details$layer_name <- stringr::str_remove_all(
    stringr::str_remove_all(layer_names, paste0(Nhood_folder_path, "/")), ".grd"
  )
  Focal_details$period <- stringr::str_extract(Focal_details$layer_name, period_names_regex)
  Focal_details$active_lulc <- stringr::str_extract(Focal_details$layer_name, class_names_regex)
  Focal_details$matrix_id <- stringr::str_extract(Focal_details$layer_name, matrix_id_regex)

  # save dataframe to use as a look up table in dynamic focal layer creation.
  saveRDS(
    Focal_details,
    file.path(
      config[["preds_tools_dir"]], "neighbourhood_details_for_dynamic_updating",
      "focal_layer_lookup.rds"
    )
  )

  ### =========================================================================
  ### E- Updating predictor table with layer names- updated xl.
  ### =========================================================================

  # Add additional columns to Focal details
  Focal_details$Covariate_ID <- paste0(
    Focal_details$active_lulc, "_nhood_", Focal_details$matrix_id
  )
  Focal_details$CA_category <- "Neighbourhood"
  Focal_details$Predictor_category <- "Neighbourhood"
  Focal_details$Variable_name <- mapply(
    function(LULC_class, MID) {
      width <- stringr::str_remove(stringr::str_split(MID, "_")[[1]][1], "n")
      version <- stringr::str_split(MID, "_")[[1]][2]
      paste0(
        LULC_class,
        " Neighbourhood effect matrix (size: ",
        width, "x",
        width, "cells; random central value and decay rate version:",
        version
      )
    },
    LULC_class = Focal_details$active_lulc,
    MID = Focal_details$matrix_id,
    SIMPLIFY = TRUE
  )
  Focal_details$Static_or_dynamic <- "Dynamic"
  Focal_details$Prepared <- "Y"
  Focal_details$Original_resolution <- "100m"
  Focal_details$Temporal_coverage <- sapply(
    Focal_details$period,
    function(x) stringr::str_split(x, "_")[[1]][1]
  )
  Focal_details$Data_citation <- NA
  Focal_details$URL <- NA
  Focal_details$Temporal_resolution <- Focal_details$period
  Focal_details$Raw_data_path <- NA
  Focal_details$Scenario_variant <- NA

  # split focal_details by period
  Periodic_focal_details <- split(Focal_details, Focal_details$period)

  # get names of sheets to loop over
  sheets <- readxl::excel_sheets(config[["pred_table_path"]])

  # load all sheets as a list
  Pred_tables <- lapply(
    sheets,
    function(x) openxlsx::read.xlsx(config[["pred_table_path"]], sheet = x)
  )
  names(Pred_tables) <- sheets

  # load predictor_table as workbook to add sheets
  Pred_table_update <- openxlsx::loadWorkbook(file = config[["pred_table_path"]])

  # loop over periods and add the rows of focal details table to the correct table of the pred table
  lapply(names(Periodic_focal_details), function(x) {
    # subset to Focal table to columns of pred table
    Period_table <- Periodic_focal_details[[x]]
    Period_table <- Period_table[, which(colnames(Period_table) %in% colnames(Pred_tables[[x]]))]

    # incase nhood rows already exist remove them
    Pred_table <- Pred_tables[[x]]
    Pred_table <- Pred_table[Pred_table$Predictor_category != "Neighbourhood", ]

    # create Unique_ID seq proceeding from last row value in pred table
    last_cov_num <- as.numeric(
      stringr::str_remove(
        tail(c(Pred_table$Unique_ID), n = 1), "cov_"
      )
    )
    Period_table$Unique_ID <- paste0(
      "cov_",
      seq(last_cov_num + 1, last_cov_num + nrow(Period_table))
    )
    # bind together
    combined_table <- rbind(Pred_table, Period_table)

    # add table to worksheet, try() is necessary in case sheets already exist
    if (length((tbl_name <- openxlsx::getTables(Pred_table_update, sheet = x))) == 1) {
      openxlsx::removeTable(Pred_table_update, sheet = x, table = tbl_name)
    } else if (length(tbl_name) > 1) {
      stop("Multiple tables found in sheet ", x)
    }
    openxlsx::writeData(Pred_table_update, sheet = x, x = combined_table)
  })

  # save workbook
  openxlsx::saveWorkbook(Pred_table_update, config[["pred_table_path"]], overwrite = TRUE)

  ### =========================================================================
  ### F- Example plotting of nhood matrices and decay rates
  ### =========================================================================

  message(" Preparation of Neighbourhood predictor layers complete \n")
}
