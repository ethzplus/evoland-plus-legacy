#' Prepare soil predictor data by downloading, aligning to a reference grid, and saving.
#' This function ensures soil predictor rasters are aligned to a specified reference grid.
#' @param config A list of configuration parameters, typically obtained from `get_config()`.
#' @param refresh_cache Logical; if TRUE, re-download raw data even if it exists and re-process existing prepared layers.
#' @return NULL; the function saves aligned rasters to disk.
#' @author Ben Black
#' @examples
#' #' # Prepare soil predictor data using default configuration
#' soil_pred_prep()
soil_pred_prep <- function(config = get_config(), refresh_cache = FALSE) {
  message("Preparing soil predictor data...")
  # if refresh_cache is TRUE then re-download the raw data even if it exists
  # if (refresh_cache) {
  #   reticulate::source_python(
  #     file = file.path(getwd(), "Python", "soilgrids_dl.py")
  #   )

  #   download_soilgrids(
  #     ref_grid = "data/spatial_reference_grid/ref_grid.tif",
  #     out_root = "data/predictors/raw/soil"
  #   )
  # }
  ensure_dir(config[["prepped_lyr_path"]])

  terra_temp <- "E:/terra_temp"
  ensure_dir(terra_temp)

  # Use large disk for temp files
  terra::terraOptions(tempdir = terra_temp)

  # Increase memory limit (in MB)
  terra::terraOptions(memfrac = 0.8) # Use up to 80% of available RAM

  # list all files in the raw soil predictor directory
  raw_soil_files <- list.files(
    file.path(config[["predictors_raw_dir"]], "soil"),
    full.names = TRUE,
    pattern = "\\.tif(f)?$",
    recursive = TRUE
  )

  # name using basename
  names(raw_soil_files) <- tools::file_path_sans_ext(basename(raw_soil_files))

  # loop over each passing to align_raster_to_ref function and save the output

  prepped_soil_paths <- c()
  for (f in raw_soil_files) {
    message(paste("Processing", basename(f)))

    # save the aligned raster to the prepped_lyr_path directory
    out_name <- basename(f)
    # prepend "soil_" to the output filename
    out_path <- file.path(
      config[["prepped_lyr_path"]],
      paste0("soil_", out_name)
    )
    prepped_soil_paths <- c(prepped_soil_paths, out_path)

    # if the file exists and refresh_cache is FALSE, skip processing
    # this allows to resume interrupted processing
    if (file.exists(out_path) & !refresh_cache) {
      message(paste("File", out_name, "already exists. Skipping..."))
      next
    } else {
      message(paste("Saving to", out_path))
    }

    start_time <- Sys.time()
    # align the raster to the reference grid using the terra function
    align_to_ref(
      x = f,
      ref = config[["ref_grid_path"]],
      filename = out_path,
      tempdir = terra_temp
    )
    end_time <- Sys.time()
    message(paste(
      "Processed",
      out_name,
      "in",
      round(difftime(end_time, start_time, units = "mins"), 2),
      "minutes"
    ))
  }

  # update predictor table to include new topo predictors
  pred_table <- read.csv(config[["pred_table_path"]])
  pred_table <- rbind(
    pred_table,
    data.frame(
      pred_name = names(raw_soil_files),
      # replace '_' with ' ' in clean_name
      clean_name = gsub("_", " ", names(raw_soil_files)),
      pred_category = rep("suitability", length(prepped_soil_paths)),
      static_or_dynamic = rep("static", length(prepped_soil_paths)),
      metadata = rep(NA, length(topo_measures)),
      scenario_variant = rep(NA, length(prepped_soil_paths)),
      period = rep("all", length(prepped_soil_paths)),
      path = prepped_soil_paths,
      stringsAsFactors = FALSE
    )
  )

  # remove duplicates if any
  pred_table <- pred_table[!duplicated(pred_table$pred_name), ]
  write.csv(
    pred_table,
    file = config[["pred_table_path"]],
    row.names = FALSE
  )

  message("Soil predictor data preparation complete.")
}
