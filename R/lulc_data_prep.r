#' LULC_data_prep
#'
#' Preparing land use land cover rasters for all historic periods in the the Swiss Areal
#' Statistiks and aggregating LULC rasters to new classification scheme
#' @author Ben Black
#' @export

lulc_data_prep <- function(config) {
  if (!file.exists(config[["arealstat_zip_local"]])) {
    message("Downloading Arealstatistik csv as zip")
    dir.create(dirname(config[["arealstat_zip_local"]]), recursive = TRUE)
    curl::curl_download(
      url = config[["arealstat_zip_remote"]],
      destfile = config[["arealstat_zip_local"]],
      quiet = FALSE
    )
  }

  # read from file for security
  AS_table <-
    unz(config[["arealstat_zip_local"]], "ag-b-00.03-37-area-all-csv.csv") |>
    read.csv2()

  # splitting into a list of tables for separate periods
  # Keep only columns of coordinates and LULC classes under the 72 categories scheme
  AS_tables_seperate_periods <- list(
    NOAS04_1985 = AS_table[, c("E_COORD", "N_COORD", "AS85_72")],
    NOAS04_1997 = AS_table[, c("E_COORD", "N_COORD", "AS97_72")],
    NOAS04_2009 = AS_table[, c("E_COORD", "N_COORD", "AS09_72")],
    NOAS04_2018 = AS_table[, c("E_COORD", "N_COORD", "AS18_72")]
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
    reprojected_raster_for_period <- terra::project(cropped_raster_for_period, Ref_grid, method = "near")
    terra::writeRaster(reprojected_raster_for_period,
      filename = paste0(config[["rasterized_lulc_dir"]], "/", raster_name, ".tif"),
      overwrite = TRUE
    )
  }

  # Loop function over tables
  dir.create(config[["rasterized_lulc_dir"]])
  NOAS04_periods_rasters <- mapply(create.reproject.save.raster,
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

  period_rasts <- lapply(NOAS04_list, function(x) terra::rast(x))
  Reclassified_rasters <- lapply(period_rasts, function(x) terra::classify(x, rcl = Agg_matrix))
  names(Reclassified_rasters) <- c("LULC_1985_agg", "LULC_1997_agg", "LULC_2009_agg", "LULC_2018_agg")

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
    x <- terra::as.factor(x)
    df <- data.frame(value = LULC_rat$Pixel_value, label = LULC_rat$lulc_name)
    levels(x)[[1]] <- df
    x
  })

  mapply(
    FUN = terra::writeRaster,
    x = Reclassified_rasters,
    filename = paste0(
      config[["historic_lulc_basepath"]], "/",
      names(Reclassified_rasters), ".grd"
    ),
    overwrite = TRUE
  )
}
