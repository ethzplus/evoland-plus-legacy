#' Region Preparation: region_prep
#'
#' Preparing Rasters of regions in Peru used for regionalized modeling of land use
#' and land cover transitions
#'
#' @author Ben Black
#' @export

region_prep <- function(config = get_config()) {
  ensure_dir(config[["reg_dir"]])

  # Load in the grid to use use for re-projecting the CRS and extent of predictor data
  ref_grid <- terra::rast(config[["ref_grid_path"]])

  # Load in shapefile of regions
  reg_vect <- terra::vect(
    file.path(config[["reg_dir"]], "regions.shp")
  )

  # get the values of the Workshop_r field
  regions_pretty <- reg_vect$Workshop_r
  regions_clean <- tolower(gsub(" ", "_", regions_pretty))
  region_vals <- seq(0, length(regions_clean) - 1)

  # combine todf and save to JSON
  todf <- data.frame(
    value = region_vals,
    label = regions_clean,
    pretty = regions_pretty
  )
  jsonlite::write_json(
    todf,
    file.path(config[["reg_dir"]], "regions.json"),
    pretty = TRUE
  )

  # rasterize regions to match reference grid
  reg_rast <-
    terra::rasterize(reg_vect, ref_grid, field = "Workshop_r") |>
    terra::mask(ref_grid) |> # masking needed because LULC data have finer border def
    terra::focal(
      w = matrix(1, 3, 3),
      fun = "modal",
      na.policy = "omit",
      fillvalue = NA
    )

  # save the raster
  outpath <- file.path(config[["reg_dir"]], "regions.tif")
  write_raster(
    reg_rast,
    filename = outpath
  )
  message("Completed conversion of regions shapefile to raster: ", outpath)
}
