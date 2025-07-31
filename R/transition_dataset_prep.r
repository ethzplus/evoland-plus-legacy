#' transition_dataset_creation
#'
#' Script for gathering the layers of LULC (dependent variable) and predictors for each
#' historic period then separating into viable LULC transitions at the scale of
#' Switzerland and Bioregions
#'
#' @param config list of configuration parameters
#'
#' @author Ben Black
#' @export

transition_dataset_prep <- function(config = get_config()) {
  ### =========================================================================
  ### A- Preparation
  ### =========================================================================

  # create save dirs
  purrr::walk(
    file.path(
      config[["trans_pre_pred_filter_dir"]],
      config[["data_periods"]]
    ),
    ensure_dir
  )

  ### =========================================================================
  ### B- Load data and filter for each time period
  ### =========================================================================

  # The predictor data table is used to identify the file names of variables that
  # are to be included in the stack for each time period

  # load tables as list
  predictor_tables <- lapply(
    config[["data_periods"]],
    function(x) {
      data.table::data.table(
        openxlsx::read.xlsx(config[["pred_table_path"]], sheet = x)
      )
    }
  )
  names(predictor_tables) <- config[["data_periods"]]

  # subsetting to only the necessary columns
  preds_by_period <- lapply(predictor_tables, function(x) {
    pred_subset <- x[, c("Prepared_data_path", "Covariate_ID")]
    pred_subset$File_name <- file.path(
      config[["data_basepath"]], pred_subset$Prepared_data_path
    )
    pred_subset$Prepared_data_path <- NULL
    names(pred_subset)[names(pred_subset) == "Covariate_ID"] <- "Layer_name"
    return(pred_subset)
  })

  # Appending the initial LULC classes (1st year of period) and the outcome LULC (last
  # year of period) as well regional designation create a list of the file paths of the
  # historic LULC rasters pattern matching on the .grd$ extension first and then
  # excluding the accompanying .ovr files with grep
  lulc_raster_paths <- data.frame(matrix(ncol = 2, nrow = 4))
  colnames(lulc_raster_paths) <- c("File_name", "Layer_name")
  lulc_raster_paths["File_name"] <- as.data.frame(
    list.files(config[["historic_lulc_basepath"]], pattern = ".grd$", full.names = TRUE)
  )

  # extract everything that begins with / and runs to the end of the string.
  lulc_raster_paths["Layer_name"] <-
    lulc_raster_paths$File_name |>
    stringr::str_extract("(?<=/)[^/]*$") |>
    stringr::str_remove(".grd$")

  # Collect file path for regional raster
  region_path <- data.frame(matrix(ncol = 2, nrow = 1))
  colnames(region_path) <- c("File_name", "Layer_name")
  region_path["File_name"] <- list.files(
    config[["bioreg_dir"]],
    pattern = ".grd$", full.names = TRUE
  )
  region_path["Layer_name"] <- "Bioregion"

  # Create regexes for LULC periods
  lulc_period_regexes <- lapply(config[["data_periods"]], function(x) {
    stringr::str_replace(x, pattern = "_", "|")
  })
  names(lulc_period_regexes) <- config[["data_periods"]]

  # Filter LULC files by period
  lulc_paths_by_period <- lapply(lulc_period_regexes, function(x) {
    dplyr::filter(lulc_raster_paths, grepl(x, File_name))
  })

  # Change layer names to Initial and Final for easier splitting later
  lulc_paths_by_period <- lapply(lulc_paths_by_period, function(x) {
    x$Layer_name[1] <- "Initial_class"
    x$Layer_name[2] <- "Final_class"
    return(x)
  })

  # Combine predictor paths with LULC (and region if needed)
  if (config[["regionalization"]]) {
    combined_paths_by_period <- lapply(
      (mapply(rbind, preds_by_period, lulc_paths_by_period, SIMPLIFY = FALSE)),
      function(x) rbind(x, region_path)
    )
  } else {
    combined_paths_by_period <- mapply(
      rbind, preds_by_period, lulc_paths_by_period,
      SIMPLIFY = FALSE
    )
  }

  # read in all rasters in the list to check compatibility before stacking
  rasters_by_periods <- purrr::map(combined_paths_by_period, function(x) {
    raster_list <- purrr::map(x$File_name, function(raster_file_name) {
      r <- terra::rast(raster_file_name)
      if (!terra::global(r, "anynotNA")[[1]]) {
        warning("Raster ", raster_file_name, " is all NA, discarding")
        return(NULL) # discard all-NA rasters quietly
      }
      r
    })
    names(raster_list) <- x$Layer_name
    purrr::compact(raster_list) # drop NULLs
  })
  names(rasters_by_periods) <- config[["data_periods"]]

  ### =========================================================================
  ### C- Confirm Raster compatibility for stacking
  ### =========================================================================

  # Use a function to test rasters in the list against an 'exemplar'
  # which has the extent, crs and resolution that we want
  # in this case the Ref_grid file used for re-sampling some of the predictors.

  purrr::walk(
    rasters_by_periods,
    lulcc.TestRasterCompatibility,
    exemplar_raster = terra::rast(config[["ref_grid_path"]])
  )

  # Create SpatRaster stacks for each time period.
  ensure_dir(config[["prepped_pred_stacks"]])
  rasterstacks_by_periods <- mapply(
    function(raster_list, period_name) {
      # Combine layers into a single SpatRaster
      raster_stack_for_period <- terra::rast(raster_list)

      # Saving as rds doesn't work for terra objects, but these stacks aren't actually
      # used anywhere. It's actually a good idea though, because this avoids a memory
      # bottleneck down the line.
      # saveRDS(
      #   raster_stack_for_period,
      #   file = file.path(
      #     config[["prepped_pred_stacks"]],
      #     paste0("pred_stack_", period_name, ".rds")
      #   )
      # )
      raster_stack_for_period
    },
    raster_list = rasters_by_periods,
    period_name = names(rasters_by_periods),
    SIMPLIFY = FALSE
  )

  rm(rasters_by_periods)

  ### =========================================================================
  ### C.1- Data extraction
  ### =========================================================================

  message("Starting data extraction for each transition period")
  # futures crash with an error on a raster::cellFromXY() call
  # i could not trace the error, so sequential computation it is for now.
  # furrr::future_walk(
  purrr::walk(
    config[["data_periods"]],
    process_period_transitions,
    config = config,
    rasterstacks_by_periods = rasterstacks_by_periods
  )
  message("Preparation of transition datasets complete")
}


process_period_transitions <- function(period, rasterstacks_by_periods, config) {
  # Convert SpatRaster to data.frame including coordinates
  # Cannot take advantage of sparsity because the distances to roads, lakes, rivers is
  # computed for the entire domain; all others are about half that
  # TODO this is slow and a memory hog; can we not avoid this by directly constructing a
  # table like this?
  trans_data <-
    terra::as.data.frame(
      rasterstacks_by_periods[[paste(period)]],
      xy = TRUE, na.rm = FALSE
    ) |>
    tibble::as_tibble(rownames = "Num_ID") |>
    # this also drops NAs, not what Ben implemented, but possibly what he intended
    dplyr::filter(Initial_class != "Static")

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
  viable_trans_list <- readRDS(config[["viable_transitions_lists"]])[[paste(period)]]

  # loop over viable transitions and add column for each to the data with the values:
  # 1: If the row is positive for the given transition: If both Initial and Final
  # classes match that of the transition)
  # 0 :if the row is negative for this transition: If the initial class matches but
  # the final class does not);
  # NA :if the row is Not applicable: neither the initial or final class match that of
  # the transition)
  message("Creating transition datasets for period ", period)

  # Load predictor data table
  predictor_table <-
    openxlsx::read.xlsx(config[["pred_table_path"]], sheet = period) |>
    tibble::as_tibble() |>
    # FIXME? silently filtering here on the presumption that the warning above suffices
    dplyr::filter(Covariate_ID %in% names(trans_data))

  # seperate transition related columns
  trans_rel_cols <- trans_data[
    ,
    c("Num_ID", "Initial_class", "Final_class")
  ]

  # create empty list for results
  trans_df <- matrix(NA_integer_,
    nrow = nrow(trans_rel_cols),
    ncol = nrow(viable_trans_list)
  ) |>
    data.frame() |> # cannot go to tibble directly; it can hold matrix columns
    tibble::as_tibble()

  colnames(trans_df) <- viable_trans_list[, "Trans_name"]

  # Assign 1/0 for each viable transition
  for (row_i in seq_len(nrow(viable_trans_list))) {
    f <- viable_trans_list[row_i, "Initial_class"]
    t <- viable_trans_list[row_i, "Final_class"]
    trans_df[
      which(trans_rel_cols$Initial_class == f &
        trans_rel_cols$Final_class == t), row_i
    ] <- 1L
    trans_df[
      which(trans_rel_cols$Initial_class == f &
        trans_rel_cols$Final_class != t), row_i
    ] <- 0L
  }

  # Combine each transition column with predictor/info cols
  binarized_trans_datasets <- lapply(viable_trans_list[, "Trans_name"], function(trans_name) {
    trans_data_combined <- tidyr::drop_na(dplyr::bind_cols(
      # the order of these columns is relied upon in lulcc.splitforcovselection
      trans_dataset = trans_data,
      trans_df[, trans_name]
    ))

    # TODO check if this is necessary; the tibble being joined should already have this colname
    names(trans_data_combined)[ncol(trans_data_combined)] <- trans_name
    trans_data_combined
  })

  # Rename datasets using period to split LULC classes
  names(binarized_trans_datasets) <- sapply(
    seq_len(nrow(viable_trans_list)),
    function(i) {
      paste0(viable_trans_list[i, "Initial_class"], "/", viable_trans_list[i, "Final_class"])
    }
  )

  if (config[["regionalization"]]) {
    binarized_trans_datasets_regionalized <- unlist(
      lapply(binarized_trans_datasets, function(x) {
        split(x, f = x[["Bioregion"]], sep = "/")
      }),
      recursive = FALSE
    )

    # Reverse order of name components
    names(binarized_trans_datasets_regionalized) <- sapply(
      strsplit(names(binarized_trans_datasets_regionalized), "\\."),
      function(x) stringr::str_replace_all(paste(x[2], x[1], sep = "."), "/", ".")
    )
  }

  names(binarized_trans_datasets) <- stringr::str_replace_all(
    names(binarized_trans_datasets), "/", "."
  )
  rm(trans_df, trans_data, trans_rel_cols)

  # Loop over transition datasets splitting each into:
  # the transition result column
  # non-transition columns,
  # weight vector,
  # measure of class imbalance
  # number of units in the dataset
  trans_datasets_full <- lapply(binarized_trans_datasets, function(x) {
    lulcc.splitforcovselection(
      trans_dataset = x,
      covariate_ids = predictor_table$Covariate_ID
    )
  })
  rm(binarized_trans_datasets)

  if (config[["regionalization"]]) {
    trans_datasets_regionalized <- lapply(binarized_trans_datasets_regionalized, function(x) {
      lulcc.splitforcovselection(
        trans_dataset = x,
        covariate_ids = predictor_table$Covariate_ID
      )
    })
    rm(binarized_trans_datasets_regionalized)

    #  Remove regional datasets without sufficient transitions
    trans_datasets_regionalized <- trans_datasets_regionalized[
      sapply(trans_datasets_regionalized, function(x) sum(x[["trans_result"]] == 1)) > 5
    ]
  }

  # Save datasets
  sapply(names(trans_datasets_full), function(dataset_name) {
    full_save_path <- file.path(
      config[["trans_pre_pred_filter_dir"]], period, paste0(dataset_name, "_full_ch.rds")
    )
    saveRDS(trans_datasets_full[[paste(dataset_name)]], full_save_path)
  })

  if (config[["regionalization"]]) {
    sapply(names(trans_datasets_regionalized), function(dataset_name) {
      full_save_path <- file.path(
        config[["trans_pre_pred_filter_dir"]], period, paste0(dataset_name, "_regionalized.rds")
      )
      saveRDS(trans_datasets_regionalized[[paste(dataset_name)]], full_save_path)
    })
  }

  rm(trans_datasets_full, trans_datasets_regionalized)
  message("Transition Datasets for: ", period, " complete\n")

  gc()
}
