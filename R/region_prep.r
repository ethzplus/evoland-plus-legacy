#' Bioregion Preparation: region_prep
#'
#' Preparing Rasters of Bioregions in Switzerland to sub-divide data for regional
#' modelling This script is adapted from the process of Gerecke et al. 2019
#'
#' @author Ben Black
#' @export

region_prep <- function(config = get_config()) {
  ensure_dir(config[["bioreg_dir"]])

  # Load in an exemplar Raster of the correct resolution, extent, and NAs
  lulc_mask <- terra::rast(
    file.path(config[["rasterized_lulc_dir"]], "NOAS04_2009.tif")
  )

  # Load in shapefile of Bioregions of Switzerland
  bioreg_vect <- terra::vect(
    file.path(
      "/vsizip//vsicurl", config[["bioreg_zip_remote"]],
      "BiogeographischeRegionen", "N2020_Revision_BiogeoRegion.shp"
    )
  )

  # TODO why are we applying the focal window twice?
  bioreg_rast <-
    terra::rasterize(bioreg_vect, lulc_mask, field = "RegionNumm") |>
    terra::mask(lulc_mask) |> # masking needed because LULC data have finer border def
    terra::focal(
      w = matrix(1, 3, 3), fun = "modal",
      na.policy = "omit", fillvalue = NA
    ) |>
    terra::focal(
      w = matrix(1, 3, 3), fun = "modal",
      na.policy = "omit", fillvalue = NA
    )


  # using these names to create a raster attribute table (rat)
  bioreg_rast <- terra::as.factor(bioreg_rast)
  rat_bioregions <- data.frame(
    Pixel_Values = c(1, 2, 3, 4, 5, 6),
    Class_Names = c(
      "Jura", "Plateau", "Northern_Prealps", "Southern_Prealps",
      "Western_Central_Alps", "Eastern_Central_Alps"
    )
  )
  df <- data.frame(value = rat_bioregions$Pixel_Values, label = rat_bioregions$Class_Names)
  levels(bioreg_rast)[[1]] <- df

  # FIXME? this is _mostly_ the same as what i got from https://doi.org/10.5281/zenodo.10171467
  # the difference is in some slight shifts around the boundaries, and fewer NAs
  # TODO the grd writer used to include the RAT, but it appears to have stopped that behaviour. do
  # we rely on the raster attributes downstream?
  # saving the raster in R's native .grd format which SHOULD preserve the attribute table
  terra::writeRaster(
    bioreg_rast,
    filename = file.path(config[["bioreg_dir"]], "bioreg_raster.tif"),
    overwrite = TRUE
  )
}
