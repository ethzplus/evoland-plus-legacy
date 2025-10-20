hydrological_pred_prep <- function(
  config = get_config(),
  refresh_cache = FALSE
) {
  message("Preparing hydrological predictor data...")

  # Load predictor YAML
  pred_yaml_file <- config[["pred_table_path"]]
  pred_table <- yaml::yaml.load_file(pred_yaml_file)

  # Subset to entries where grouping is 'hydrological'
  hydro_pred_entries <- Filter(
    function(x) !is.null(x$grouping) && x$grouping == "hydrological",
    pred_table
  )

  # Construct full vector paths
  hydro_vect_paths <- sapply(
    hydro_pred_entries,
    function(x) {
      file.path(
        config[["predictors_raw_dir"]],
        x$raw_data_dir,
        x$raw_filename
      )
    },
    USE.NAMES = TRUE
  )

  terra_temp <- "E:/terra_temp"
  ensure_dir(terra_temp)

  terra::terraOptions(
    memfrac = 0.5, # limit in-memory cache usage
    tempdir = terra_temp, # directory for temporary files
    progress = 1,
    todisk = TRUE
  )

  # Loop over hydrological vector files
  for (i in seq_along(hydro_vect_paths)) {
    vect_path <- hydro_vect_paths[i]
    pred_name <- names(hydro_vect_paths)[i]
    message(paste("Processing", pred_name))

    out_name <- paste0(pred_name, ".tif")
    out_path <- file.path(config[["prepped_lyr_path"]], out_name)

    # Skip if already exists and refresh_cache is FALSE
    if (file.exists(out_path) & !refresh_cache) {
      message(paste("File", out_name, "already exists. Skipping..."))
      next
    } else {
      message(paste("Saving to", out_path))
    }

    # start_time <- Sys.time()
    # distance_from_shapefile(
    #   shapefile = vect_path,
    #   ref = config[["ref_grid_path"]],
    #   out_path = out_path,
    #   tempdir = terra_temp,
    #   datatype = "FLT8S",
    #   compress = c("DEFLATE", "ZSTD"),
    #   deflate_level = 9,
    #   predictor = 2,
    #   rasterize_field = 1,
    #   rasterize_background = NA,
    #   rasterize_all_touched = TRUE
    # )
    # end_time <- Sys.time()
    # message(paste(
    #   "Processed",
    #   out_name,
    #   "in",
    #   round(difftime(end_time, start_time, units = "mins"), 2),
    #   "minutes"
    # ))

    # Update YAML entry for this predictor
    entry <- hydro_pred_entries[[pred_name]]

    # remove the data_dir from the output path
    rel_path <- out_path |>
      fs::path_rel(config[["data_basepath"]])

    update_predictor_yaml(
      yaml_file = pred_yaml_file,
      pred_name = pred_name,
      clean_name = pred_name,
      pred_category = "suitability",
      static_or_dynamic = "static",
      metadata = if (!is.null(entry$metadata)) entry$metadata else NULL,
      scenario_variant = if (!is.null(entry$scenario_variant)) {
        entry$scenario_variant
      } else {
        NULL
      },
      period = if (!is.null(entry$period)) entry$period else "all",
      path = rel_path,
      grouping = "hydrological",
      description = if (!is.null(entry$description)) {
        entry$description
      } else {
        paste("Hydrological predictor:", pred_name)
      },
      date = Sys.Date(),
      author = "Your Name",
      wfs_url = if (!is.null(entry$wfs_url)) entry$wfs_url else NULL,
      download_url = if (!is.null(entry$download_url)) {
        entry$download_url
      } else {
        NULL
      },
      raw_data_dir = if (!is.null(entry$raw_data_dir)) {
        entry$raw_data_dir
      } else {
        "hydrological"
      },
      raw_filename = if (!is.null(entry$raw_filename)) {
        entry$raw_filename
      } else {
        NULL
      }
    )
  }

  message("Hydrological predictor data preparation complete.")
}
