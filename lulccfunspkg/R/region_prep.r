#' region_prep
#'
#' Preparing Rasters of Bioregions in Switzerland to sub-divide data for regional
#' modelling This script is adapted from the process of Gerecke et al. 2019
#'
#' @author Ben Black
#' @export

region_prep <- function() {
  #############################################################################

  ### =========================================================================
  ### A- Preparation
  ### =========================================================================

  # define projection systems
  Ref_grid <- terra::rast(Ref_grid_path)
  Ref_crs <- terra::crs(Ref_grid)

  ### =========================================================================
  ### B- Load data and Rasterize
  ### =========================================================================

  # Load in an exemplar Raster of the correct res/extent etc.
  LULC_2009 <- terra::rast("Data/historic_LULC/NOAS04_LULC/rasterized/NOAS04_2009.tif")

  # Download data
  Bioreg_dir <- "Data/Bioreg_CH"
  lulcc.downloadunzip(
    url = "https://data.geo.admin.ch/ch.bafu.biogeographische_regionen/data.zip",
    save_dir = Bioreg_dir
  )

  # Load in shapefile of Bioregions of Switzerland
  bioreg.shp <- terra::vect("Data/Bioreg_CH/BiogeographischeRegionen/N2020_Revision_BiogeoRegion.shp")

  # project to new CRS
  bioreg.shp <- terra::project(bioreg.shp, Ref_crs)

  # save shapefile in new CRS as an .rds file for easy use in R
  saveRDS(bioreg.shp, file = "Data/Bioreg_CH/Bioreg_shape.rds")

  # necessary to set the regionNumm column as numeric data type
  bioreg.shp$RegionNumm <- as.numeric(bioreg.shp$RegionNumm)

  bioreg_6.r <- terra::rasterize(bioreg.shp, LULC_2009, field = "RegionNumm")
  bioreg_6.r2 <- terra::mask(bioreg_6.r, LULC_2009)
  bioreg_6.r3 <- terra::focal(bioreg_6.r2,
    w = matrix(1, 3, 3), fun = "modal",
    na.policy = "omit", fillvalue = NA
  )
  bioreg_6.r3 <- terra::focal(bioreg_6.r3,
    w = matrix(1, 3, 3), fun = "modal",
    na.policy = "omit", fillvalue = NA
  )
  bioreg_6.r4 <- terra::mask(bioreg_6.r3, LULC_2009)

  # writing the raster as a .tif file because .grd files cannot be loaded into Dinamica
  terra::writeRaster(bioreg_6.r4, file = "Data/Bioreg_CH/Bioreg_raster.tif", overwrite = TRUE)

  # Create a table from the shape file columns to link region numbers with their descriptions
  Region_names <- data.frame(bioreg.shp$RegionNumm, bioreg.shp$DERegionNa)

  # remove duplicates
  Region_names %>% distinct(bioreg.shp.RegionNumm, .keep_all = TRUE)

  # using these names to create a raster attribute table (rat)
  bioreg_6.r4 <- terra::as.factor(bioreg_6.r4)
  rat_bioregions <- data.frame(
    Pixel_Values = c(1, 2, 3, 4, 5, 6),
    Class_Names = c(
      "Jura", "Plateau", "Northern_Prealps", "Southern_Prealps",
      "Western_Central_Alps", "Eastern_Central_Alps"
    )
  )
  df <- data.frame(value = rat_bioregions$Pixel_Values, label = rat_bioregions$Class_Names)
  levels(bioreg_6.r4)[[1]] <- df

  # saving the rat as .csv in case it needs to be loaded in with the tif later
  write.csv(rat_bioregions, file = "Data/Bioreg_CH/Bioregion_rat", row.names = FALSE)

  # saving the raster in R's native .grd format which preserves the attribute table
  terra::writeRaster(bioreg_6.r4, filename = "Data/Bioreg_CH/Bioreg_raster.grd", overwrite = TRUE)

  cat(paste0(" Preparation of Bio-regions raster complete \n"))
}
