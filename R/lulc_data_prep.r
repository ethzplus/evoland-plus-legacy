#' LULC_data_prep
#'
#' Preparing land use land cover rasters for all historic periods in the the Swiss Areal
#' Statistiks and aggregating LULC rasters to new classification scheme
#' @author Ben Black
#' @export

lulc_data_prep <- function(config = get_config(), refresh_cache = FALSE) {
  if (refresh_cache || !fs::file_exists(config[["arealstat_zip_local"]])) {
    ensure_dir(dirname(config[["arealstat_zip_local"]]))
    curl::curl_download(
      url = config[["arealstat_zip_remote"]],
      destfile = config[["arealstat_zip_local"]],
      quiet = FALSE
    )
  }

  # read from file for security
  AS_table <- readr::read_csv2(config[["arealstat_zip_local"]])

  # splitting into a list of tables for separate periods
  # Keep only columns of coordinates and LULC classes under the 72 categories scheme
  AS_tables_seperate_periods <- list(
    noas04_1985 = AS_table[, c("E_COORD", "N_COORD", "AS85_72")],
    noas04_1997 = AS_table[, c("E_COORD", "N_COORD", "AS97_72")],
    noas04_2009 = AS_table[, c("E_COORD", "N_COORD", "AS09_72")],
    noas04_2018 = AS_table[, c("E_COORD", "N_COORD", "AS18_72")]
  )
  rm(AS_table)

  # TODO why are we resampling to the ref_grid if the area stats are the main
  # determinant of data availability?
  # instantiate small function for raster creation
  create.reproject.save.raster <- function(table_for_period, raster_name) {
    Raster_for_period <- terra::rast(table_for_period, type = "xyz")
    terra::crs(Raster_for_period) <- config[["reference_crs"]]
    Ref_grid <- terra::rast(config[["ref_grid_path"]])
    cropped_raster_for_period <- terra::crop(Raster_for_period, Ref_grid)
    reprojected_raster_for_period <- terra::project(
      cropped_raster_for_period,
      Ref_grid,
      method = "near"
    )
    terra::writeRaster(reprojected_raster_for_period,
      filename = file.path(
        config[["rasterized_lulc_dir"]],
        paste0(raster_name, ".tif")
      ),
      overwrite = TRUE
    )
  }

  # Loop function over tables
  ensure_dir(config[["rasterized_lulc_dir"]])
  mapply(
    create.reproject.save.raster,
    table_for_period = AS_tables_seperate_periods,
    raster_name = names(AS_tables_seperate_periods)
  )


  ### =========================================================================
  ### B- Preparing aggregated LULC Rasters for each period
  ### =========================================================================

  # Aggregated categories and numerical ID's for the purpose of the LULC modeling:
  # 10 Settlement/urban/amenities
  # 11 Static class
  # 12 Open Forest
  # 13 Closed forest
  # 14 Overgrown/shrubland/unproductive vegetation
  # 15 Intensive agriculture
  # 16 Alpine pastures
  # 17 Grassland or meadows
  # 18 Permanent crops
  # 19 Glacier
  # Aggregated categories and numerical ID's for the purpose of the LULC modeling:
  # 10 Settlement/urban/amenities
  # 11 Static class
  # 12 Open Forest
  # 13 Closed forest
  # 14 Overgrown/shrubland/unproductive vegetation
  # 15 Intensive agriculture
  # 16 Alpine pastures
  # 17 Grassland or meadows
  # 18 Permanent crops
  # 19 Glacier

  # Preparing rasters using a 2 col (initial value, new value) matrix
  # load aggregation scheme
  Aggregation_scheme <- readxl::read_excel(config[["LULC_aggregation_path"]])

  # subset to just the ID cols
  Agg_matrix <- as.matrix(Aggregation_scheme[, c("NOAS04_ID", "Aggregated_ID")])

  # load each tif just created, reclassify, and store in a list
  NOAS04_list <- list.files(config[["rasterized_lulc_dir"]],
    pattern = ".tif$", full.names = TRUE
  )
  NOAS04_list <- NOAS04_list[order(NOAS04_list)]

  period_rasts <- lapply(NOAS04_list, terra::rast)
  Reclassified_rasters <- lapply(period_rasts, function(x) terra::classify(x, rcl = Agg_matrix))
  names(Reclassified_rasters) <- c("lulc_1985_agg", "lulc_1997_agg", "lulc_2009_agg", "lulc_2018_agg")

  # add raster attribute table (rat)
  # TODO are we sure that the order is preserved when calling unique?
  LULC_IDs <- unique(Aggregation_scheme$Aggregated_ID)
  names(LULC_IDs) <- sapply(LULC_IDs, function(x) {
    unique(Aggregation_scheme[Aggregation_scheme$Aggregated_ID == x, "Class_abbreviation"])
  })
  LULC_IDs <- sort(LULC_IDs)

  LULC_rat <- data.frame(
    ID = LULC_IDs,
    Pixel_value = LULC_IDs,
    lulc_name = names(LULC_IDs)
  )

  Reclassified_rasters <- lapply(Reclassified_rasters, function(x) {
    levels(x) <- LULC_rat
    x
  })

  mapply(
    FUN = terra::writeRaster,
    x = Reclassified_rasters,
    filename = file.path(
      config[["historic_lulc_basepath"]],
      paste0(names(Reclassified_rasters), ".grd")
    ),
    overwrite = TRUE
  )
}
