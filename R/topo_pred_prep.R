topo_pred_prep <- function(config = get_config()) {
  ensure_dir(config[["prepped_lyr_path"]])

  # Load clipped DEM raster
  DEM_raster <- terra::rast(file.path(
    config[["predictors_raw_dir"]],
    "topographical",
    "masked_dem.tif"
  ))

  # align DEM raster to the template raster using align_raster_to_ref function
  DEM_raster_aligned <- align_raster_to_ref(
    DEM_raster,
    config[["ref_grid_path"]]
  )

  # Save the aligned DEM raster
  write_raster(
    DEM_raster_aligned,
    filename = file.path(config[["prepped_lyr_path"]], "elevation.tif")
  )

  # Calculate slope, aspect, TPI, TRI, roughness
  topo_measures <- c("slope", "aspect", "TPI", "TRI", "roughness")
  names(topo_measures) <- file.path(
    config[["prepped_lyr_path"]],
    paste0(topo_measures, ".tif")
  )

  # Loop over measures, calculate and save
  for (i in seq_along(topo_measures)) {
    out_raster <- terra::terrain(
      DEM_raster_aligned,
      v = topo_measures[i],
      unit = "degrees"
    )
    out_path <- names(topo_measures)[i]
    write_raster(
      out_raster,
      filename = out_path
    )
  }

  # add DEM to topo_measures for predictor table update
  topo_measures <- c("elevation", topo_measures)
  topo_measures <- unique(topo_measures)
  names(topo_measures) <- file.path(
    config[["prepped_lyr_path"]],
    paste0(topo_measures, ".tif")
  )

  # update predictor table to include new topo predictors
  pred_table <- read.csv(config[["pred_table_path"]])
  pred_table <- rbind(
    pred_table,
    data.frame(
      pred_name = topo_measures,
      clean_name = topo_measures,
      pred_category = rep("suitability", length(topo_measures)),
      static_or_dynamic = rep("static", length(topo_measures)),
      metadata = rep(NA, length(topo_measures)),
      scenario_variant = rep(NA, length(topo_measures)),
      period = rep("all", length(topo_measures)),
      path = names(topo_measures),
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
}
