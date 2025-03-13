#' Preparation of Predictor Data
#'
#' Not yet clear why this script was originally called `calibration_predictor_prep.r` -
#' 100m resolution, crs and extent prepared layers are saved seperately in a
#' Use predictor table to prepare predictor layers at a uniform
#' 100m resolution, crs and extent prepared layers are saved seperately in a
#' and then combined into raster stacks for easy loading
#' Date: 01-08-2021
#' Author: Ben Black
#'
#' @param config A list object
#'
#' @export

calibration_predictor_prep <- function(
    config = get_config(),
    refresh_cache = FALSE,
    ignore_excel = FALSE) {
  # Load in the grid to use use for re-projecting the CRS and extent of predictor data
  Ref_grid <- terra::rast(config[["ref_grid_path"]])

  # vector years of LULC data
  LULC_years <-
    list.files(config[["historic_lulc_basepath"]], full.names = FALSE, pattern = ".gri") |>
    gsub(pattern = ".*?([0-9]+).*", replacement = "\\1", x = _)

  # create a list of the data/modelling periods
  LULC_change_periods <- list()
  for (i in 1:(length(LULC_years) - 1)) {
    LULC_change_periods[[i]] <- c(LULC_years[i], LULC_years[i + 1])
  }
  names(LULC_change_periods) <- sapply(
    LULC_change_periods,
    function(x) paste(x[1], x[2], sep = "_")
  )

  if (refresh_cache) {
    # download basic map geometries for Switzerland
    lulcc.downloadunzip(
      # This was using the 2022 URL but that zip had encoding issues. Only one
      # "arbeitsmarktregion" changed afaik.
      # https://dam-api.bfs.admin.ch/hub/api/dam/assets/21245514/master
      url = "https://dam-api.bfs.admin.ch/hub/api/dam/assets/33807959/master",
      save_dir = config[["ch_geoms_path"]],
      filename = paste0(basename(config[["ch_geoms_path"]]), ".zip")
    )
  }

  ### =========================================================================
  ### B- Gather predictor information
  ### =========================================================================

  # The predictor table contains details of the suitability and accessibility
  # predictors in seperate sheets for the different time periods, some predictors
  # are the same across periods (static) whereas other are temporally dynamic
  # In terms of preparations some predictors are prepared by downloading the data
  # direct from raw and then processing with others being standardized
  # from existing raw data

  # create base directory for prepared predictor layers
  ensure_dir(config[["prepped_lyr_path"]])

  # get names of sheets to loop over
  sheets <- readxl::excel_sheets(config[["pred_table_path"]])

  # load all sheets as a list
  Pred_tables <- lapply(
    sheets,
    function(x) readxl::read_excel(config[["pred_table_path"]], sheet = x)
  )
  names(Pred_tables) <- sheets

  # combine tables for all periods
  # TODO is the fill = TRUE assumption safe?
  Pred_table_long <- data.table::rbindlist(Pred_tables, fill = TRUE)

  # create dirs for all predictor categories
  sapply(unique(Pred_table_long[["Predictor_category"]]), function(x) {
    ensure_dir(tolower(paste0(config[["prepped_lyr_path"]], "/", x)))
  })

  # seperate unprepared/prepared layers
  if (ignore_excel) {
    Pred_table_long$Prepared <- "N"
  } else {
    # TODO ideally, we wouldn't store state in the excel table at all.
    # just delete them or add a refresh argument to all prep functions
    Pred_table_long$Prepared <- ifelse(
      file.exists(Pred_table_long$Prepared_data_path),
      "Y",
      "N"
    )
  }
  Preds_to_prepare <- Pred_table_long[Pred_table_long$Prepared != "Y", ]

  ### =========================================================================
  ### C- Predictors from raw data
  ### =========================================================================

  # The static predictors with a raw data raw are those already prepared in ValPar.CH
  # at 25m resolution because they already use a grid of the same extent and CRS
  # processing is just aggregating to 100m. However the dynamic climatic predictors need
  # to be aggregated from several layers.

  # seperate the preds that have a Raw path
  Preds_raw <- Preds_to_prepare[!is.na(Preds_to_prepare$Raw_data_path), ]

  # First process the static predictors
  Preds_static <- Preds_raw[Preds_raw$Static_or_dynamic == "static", ]

  # reduce to unique predictors
  Preds_static_unique <- Preds_static[
    !duplicated(Preds_static$Covariate_ID),
    c("Covariate_ID", "Predictor_category", "URL", "Raw_data_path", "Prepared_data_path")
  ]

  # FIXME all the valpar datasets have artifacts around their borders
  # TODO replace all topographic predictors with DHM25 derived hectare mean
  # /dem/ch_topo_alti3d2016_pixel_dem_mean2m.rds
  # /aspect/ch_topo_alti3d2016_pixel_aspect_mean2m.rds
  # /slope/ch_topo_alti3d2016_pixel_slope_mean2m.rds
  # /hillshade/ch_topo_alti3d2016_pixel_hillshade_mean.rds

  # TODO replace noise from sonBASE, which appear to simply be the daytime noise
  # emmisions from road noise. proposal to replace this by taking the maximum over all
  # the noise maps in the sonBASE dataset
  # /Noise/ch_transport_sonbase_pixel_noise.rds

  # TODO replace distance to roads with own tlm3d based calculation
  # /Distance_to_roads/ch_transport_tlm3d_pixel_dist2road_all.rds

  # TODO replace with tlm3d-based calculations
  # /Distance_lakes/ch_hydro_gwn07_pixel_dist2lake_all.rds
  # /Distance_rivers/ch_hydro_gwn07_pixel_dist2riverstrahler_all.rds

  # loop over the preds loading the raw data, processing and saving, returning a prepared data_path
  for (i in seq_len(nrow(Preds_static_unique))) {
    # load data
    # TODO move to tifs or some other standard format
    Raw_dat <- readRDS(unlist(Preds_static_unique[i, "Raw_data_path"]))
    Raw_dat@srs <- raster::crs(Raw_dat@crs@projargs)

    # aggregate
    Agg_dat <- raster::aggregate(Raw_dat, fact = 4, fun = mean)

    # vector save path
    layer_path <- tolower(paste0(
      config[["prepped_lyr_path"]], "/",
      Preds_static_unique[i, "Predictor_category"], "/",
      Preds_static_unique[i, "Covariate_ID"], ".tif"
    ))

    # save
    raster::writeRaster(Agg_dat, layer_path, overwrite = TRUE)

    #  add the prepared path to the table
    Pred_table_long[
      Pred_table_long$Covariate_ID == Preds_static_unique[i, "Covariate_ID"],
      "Prepared_data_path"
    ] <- layer_path
    Pred_table_long[
      Pred_table_long$Covariate_ID == Preds_static_unique[i, "Covariate_ID"],
      "Prepared"
    ] <- "Y"

    # clean up
    rm(Raw_dat, Agg_dat, layer_path)
  }

  # TODO these are apparently only the climatic predictors from broennimann, CHclim25
  # https://zenodo.org/communities/chclim25 which we want to ditch for CHELSA
  # Process the dynamic predictors
  Preds_dynamic <- Preds_raw[Preds_raw$Static_or_dynamic == "dynamic", ]

  # Loop over the predictors, Calculating periodic averages for each
  # re-scaling the rasters, saving and updating predictor table
  for (i in seq_len(nrow(Preds_dynamic))) {
    message("Processing: ", Preds_dynamic[i, "Covariate_ID"])
    # read in rasters as stack
    temp_list <- lapply(
      list.files(Preds_dynamic[i, "Raw_data_path"], full.names = TRUE),
      function(x) terra::rast(readRDS(x))
    )
    raster_stack <- do.call(c, temp_list)

    # calculate mean on the stack
    raster_mean <- terra::app(raster_stack, mean)

    # aggregate
    Agg_dat <- terra::aggregate(raster_mean, fact = 4, fun = mean)

    # vector save path
    layer_path <- tolower(paste0(
      config[["prepped_lyr_path"]], "/",
      Preds_dynamic[i, "Predictor_category"], "/",
      Preds_dynamic[i, "Covariate_ID"], "_", Preds_dynamic[i, "period"], ".tif"
    ))

    # save
    terra::writeRaster(Agg_dat, layer_path, overwrite = TRUE)

    # add the prepared path to the table
    Pred_table_long[which(Pred_table_long$Covariate_ID == Preds_dynamic[i, "Covariate_ID"] &
      Pred_table_long$period == Preds_dynamic[i, "period"]), "Prepared_data_path"] <- layer_path
    Pred_table_long[which(Pred_table_long$Covariate_ID == Preds_dynamic[i, "Covariate_ID"] &
      Pred_table_long$period == Preds_dynamic[i, "period"]), "Prepared"] <- "Y"

    rm(raster_stack, raster_mean, Agg_dat, layer_path, temp_list)
  }

  ### =========================================================================
  ### D- Predictors from source
  ### =========================================================================
  # The predictors that are prepared directly from source require different processing
  # operations as such they are grouped into seperate chunks below

  ### -------------------------------------------------------------------------
  ### D.1- Socio_economic: Employment
  ### -------------------------------------------------------------------------

  # In order to have a common variable between the historic data and the future economic
  # scenarios, the variables we will derive will be:
  # 1. Average annual change in number of full time equivalent employees in the primary
  #    sector
  # 2. Average annual change in number of full time equivalent employees in the
  #    secondary and tertiary sectors combined

  # As the future scenarios of employment are expressed at the scale of labour market
  # regions rather than municipalities then the historic data needs to be aggregated to
  # this scale

  # The historic employment data from the Federal statistical office
  # comes from two raws:
  # 1. The business census (conducted from 1995-2008 for the timepoints: 1995, 2001,
  #    2005, 2008)
  # 2. The business structure statistics (STATENT, conducted annual from 2011)

  # The primary, secondary and tertiary sectors are themselves aggregations of all of
  # the individual economic industries each of which as an official classification under
  # the NOGA schematic. This schematic did change in 2008 with definitions/codes used
  # for some industries being changed. It is difficult to match the old/new NOGA schemes
  # exactly however changes are minor in the scope of the number of industries them
  # exactly

  Statent_dir <- file.path(
    config[["predictors_raw_dir"]],
    "socio_economic",
    "employment",
    "historic_employment",
    "statent"
  )

  statent_raw_tbl <-
    Preds_to_prepare |>
    dplyr::filter(stringr::str_detect(Covariate_ID, "Avg_chg_FTE")) |>
    dplyr::select(period, URL, Raw_data_path, Prepared_data_path) |>
    dplyr::mutate(
      # extracting urls and names from "year = url, year2 = url2" format strings
      URL =
        stringr::str_split(URL, ",") |>
          purrr::map(stringr::str_squish) |>
          purrr::map(function(x) {
            parts <-
              stringr::str_split_fixed(x, "=", n = 2)
            rlang::set_names(
              stringr::str_trim(parts[, 2]),
              stringr::str_trim(parts[, 1])
            )
          })
    ) |>
    tidyr::unnest_longer(URL, values_to = "URL", indices_to = "URL_id") |>
    dplyr::distinct()

  ### Statent data: only years 2011-2020
  Statent_urls <-
    statent_raw_tbl |>
    dplyr::filter(stringr::str_detect(URL_id, "^\\d+$")) |>
    dplyr::transmute(
      url = URL,
      filename = paste0(URL_id, ".zip")
    ) |>
    dplyr::distinct()

  if (refresh_cache) {
    # Download and unzip all datasets
    purrr::pwalk(
      Statent_urls,
      lulcc.downloadunzip,
      save_dir = Statent_dir
    )
  }

  # gather the relevant files
  Statent_paths <- grep(
    list.files(Statent_dir, recursive = TRUE, full.names = TRUE, pattern = "csv"),
    pattern = paste(c("GMDE", "NOLOC"), collapse = "|"),
    invert = TRUE,
    value = TRUE
  )

  # name using numerics in paths
  names(Statent_paths) <- stringr::str_extract(
    Statent_paths,
    pattern = "STATENT(\\d{4})",
    group = 1L
  )

  # Get the variable IDs for the number of Full Time Equivalents in each sector
  # webpage for variable list: https://www.bfs.admin.ch/bfs/de/home/dienstleistungen/geostat/geodaten-bundesstatistik/arbeitsstaetten-beschaeftigung/statistik-unternehmensstruktur-statent-ab-2011.assetdetail.23264982.html
  # download file from API URL
  Statent_metadata <-
    openxlsx::read.xlsx(
      "https://dam-api.bfs.admin.ch/hub/api/dam/assets/23264982/master",
      startRow = 9,
      cols = c(1, 3),
      colNames = FALSE
    ) |>
    rlang::set_names(c("ID", "Name"))

  # subset using German variable names
  Statent_var_names <- c(
    "Vollzeitäquivalente Sektor 1",
    "Vollzeitäquivalente Sektor 2",
    "Vollzeitäquivalente Sektor 3"
  )
  Statent_var_IDs <- Statent_metadata[
    which(Statent_metadata$Name %in% Statent_var_names), "ID"
  ]

  # Provide clean names
  names(Statent_var_IDs) <- c("Sec1", "Sec2", "Sec3")

  # add the column names corresponding to year and spatial information
  Statent_desc_vars <- c("E_KOORD", "N_KOORD", "RELI")

  # loop over each file loading it in and seperating on the required variables including coords
  Statent_data_by_year <- mapply(
    function(annual_data_path, year) {
      # named vector for select/rename with readr
      statent_var_ids_yr <- Statent_var_IDs
      names(statent_var_ids_yr) <- paste0(names(Statent_var_IDs), "_", year)

      readr::read_delim(
        file = annual_data_path,
        col_select = tidyselect::all_of(c(Statent_desc_vars, statent_var_ids_yr)),
        show_col_types = FALSE,
        progress = FALSE
      )
    },
    annual_data_path = Statent_paths,
    year = names(Statent_paths),
    SIMPLIFY = FALSE
  )

  # Merge based on Statent_desc_vars
  Statent_merged <- Reduce(
    function(x, y) dplyr::full_join(x, y, by = Statent_desc_vars),
    Statent_data_by_year
  )
  rm(Statent_data_by_year)

  # rasterize
  Statent_brick <- terra::rast(Statent_merged, crs = config[["reference_crs"]])
  # rensample to match extent
  # FIXME why is it nearest neighbour instead of bilinear? not categorical data!
  Statent_brick <- terra::resample(Statent_brick, Ref_grid, method = "near")

  ### Business census data
  Biz_census_urls <-
    statent_raw_tbl |>
    dplyr::filter(stringr::str_detect(URL_id, "^\\d+$", negate = TRUE)) |>
    dplyr::transmute(
      url = URL,
      filename = paste0(URL_id, ".zip")
    ) |>
    dplyr::distinct()

  # specify dir and download datasets
  Biz_census_dir <- file.path(
    config[["predictors_raw_dir"]], "socio_economic", "employment",
    "historic_employment", "business_census"
  )

  if (refresh_cache) {
    purrr::pwalk(
      Biz_census_urls,
      function(url, filename) {
        lulcc.downloadunzip(
          url = url,
          save_dir = file.path(Biz_census_dir, basename(filename)),
          filename = filename
        )
      }
    )
  }

  # list all csv files.
  Biz_census_paths <- list.files(
    path = Biz_census_dir,
    full.names = TRUE,
    recursive = TRUE,
    pattern = "csv",
    ignore.case = TRUE
  )

  # extract year based on abbreviations in file name e.g '05' -> 2005
  names(Biz_census_paths) <-
    Biz_census_paths |>
    basename() |>
    tolower() |>
    stringr::str_extract("bz(\\d{2})", group = 1) |>
    as.integer() |>
    (\(x) ifelse(x > 90, x + 1900, x + 2000))()


  if (FALSE) {
    # TODO this is used to find some variable names that are then just hardcoded. not
    # sure this needs to be included as code, or easier to have explanation as comment
    # each dataset uses different IDs for the variable of Full Time Equivalents
    # detailed in seperate meta data files
    Biz_census_meta_paths <- list.files(
      path = Biz_census_dir,
      full.names = TRUE,
      recursive = TRUE,
      pattern = "xls",
      ignore.case = TRUE
    )
    # Find the IDs used for the Full Time Equivalents variables in each metadata file
    unlist(sapply(Biz_census_meta_paths, function(x) {
      meta_df <- readxl::read_excel(x)
      meta_df <- meta_df[26:nrow(meta_df), c(1, 5)]
      return(meta_df[which(meta_df[[2]] %in% Statent_var_names), 1])
    }))
  }

  # The IDs contain a common string across the datasets i.e 'VZAS' followed by
  # 1,2 or 3 for the sector. Use to match columns across all datasets.
  BC_var_strings <- c("VZAS1", "VZAS2", "VZAS3")
  names(BC_var_strings) <- c("Sec1", "Sec2", "Sec3")

  # The spatial variables are named differently for the Business census datasets
  BC_desc_vars <- c("X", "Y")
  names(BC_desc_vars) <- BC_desc_vars

  # combine the variable ames vectors
  BC_vars <- c(BC_desc_vars, BC_var_strings)

  # loop over Business census datasets
  BC_data_by_year <- mapply(
    function(annual_data_path, year) {
      # load the file
      Annual_data <- readr::read_delim(annual_data_path)

      # subset to just the required variables
      Data_subset <- Annual_data[
        ,
        grepl(pattern = paste(c(BC_vars), collapse = "|"), names(Annual_data))
      ]

      # rename the sectoral columns appending year
      names(Data_subset) <- sapply(names(Data_subset), function(y) {
        new_name <- names(BC_vars)[
          which(BC_vars %in% stringr::str_match(pattern = paste(c(BC_vars), collapse = "|"), y))
        ]

        if (grepl(new_name, pattern = "Sec")) {
          paste0(new_name, "_", year)
        } else {
          new_name
        }
      })
      return(Data_subset)
    },
    annual_data_path = Biz_census_paths,
    year = names(Biz_census_paths),
    SIMPLIFY = FALSE
  )

  # merge
  BC_merged <- Reduce(
    function(x, y) dplyr::full_join(x, y, by = BC_desc_vars),
    BC_data_by_year
  )

  BC_brick <-
    terra::rast(BC_merged, crs = "epsg:21781") |>
    terra::project(config[["reference_crs"]]) |>
    terra::resample(Ref_grid)

  # Combine the two bricks together
  Data_stack <- c(Statent_brick, BC_brick)

  # intersect with labour market regions
  # load shapefile of labour market regions
  # TODO can we not find this in LV95?
  LMR_shp <-
    terra::vect(file.path(
      config[["ch_geoms_path"]],
      "2025_GEOM_TK", "03_ANAL", "Gesamtfläche_gf",
      "K4_amre20190101_gf", "K4amre_20190101gf_ch2007Poly.shp"
    )) |>
    terra::project(config[["reference_crs"]])

  # sum data in each labour market region
  FTE_lab_market <- terra::extract(
    Data_stack, LMR_shp,
    fun = sum, na.rm = TRUE
  )
  FTE_lab_market$name <- LMR_shp$name

  # split into sectors, vector sector numbers
  sector_nums <- c(1, 2, 3)

  # loop over sector numbers separating data and perform linear model based interpolation
  Sector_extrapolations <- lapply(
    sector_nums, function(x) {
      Sector_string <- paste0("Sec", x, "_")
      Sector_data <- FTE_lab_market[
        ,
        which(grepl(colnames(FTE_lab_market),
          pattern = paste(c(Sector_string, "ID", "name"), collapse = "|")
        ))
      ]
      # FIXME "NAs introduced by coercion" for ID and name cols
      years <- as.numeric(stringr::str_remove_all(
        colnames(Sector_data),
        pattern = Sector_string
      ))
      names(years) <- colnames(Sector_data)
      # sort drops the NAs
      years <- sort(years, decreasing = FALSE)

      # reorder columns on the basis of ascending years using the names of the years
      Sector_data <- Sector_data[, c("ID", "name", names(years))]

      # vector years to interpolate
      Interpolate_years <- c(1985, 1997, 2009)

      # add columns for interpolation years
      Sector_data[paste0(Sector_string, Interpolate_years)] <- NA

      # loop over rows (regions)
      for (i in seq_len(nrow(Sector_data))) {
        # create linear model
        mod <- lm(unlist(Sector_data[i, names(years)]) ~ years)

        # loop over years to interpolate
        Sector_data[
          i,
          paste0(Sector_string, Interpolate_years)
        ] <- sapply(
          Interpolate_years, function(y) round(coef(mod)[1] + coef(mod)[2] * y, 0)
        ) # close loop over interpolation years
      } # close loop over rows
      return(Sector_data)
    }
  ) # close loop over sectors
  names(Sector_extrapolations) <- paste0("Sec", sector_nums)

  # Divide the changes in municipal employment estimated for the AS flying periods by
  # the number of years to get an average annual rate of change for each period
  # because this can also be calculated for the future projected data

  # Outer loop over LULC_change_periods
  Period_sector_values <- data.table::rbindlist(lapply(
    LULC_change_periods, function(period_dates) {
      # calc period length
      Duration <- abs(diff(as.numeric(period_dates)))

      # Inner loop over sector_extrapolations
      data.table::rbindlist(mapply(
        function(Sector_data, Sector_name, period_dates) {
          # subset data
          dat <- Sector_data[, paste0(Sector_name, "_", period_dates)]
          Sector_data$Avg.diff <- (dat[, 1] - dat[, 2]) / Duration
          return(Sector_data[, c("ID", "name", "Avg.diff")])
        },
        Sector_data = Sector_extrapolations,
        Sector_name = names(Sector_extrapolations),
        MoreArgs = list(period_dates = period_dates),
        SIMPLIFY = FALSE
      ), idcol = "Sector", fill = TRUE)
    }
  ), idcol = "Period") # close outer loop

  # pivot to wide
  LMR_values <- tidyr::pivot_wider(
    data = Period_sector_values,
    id_cols = c("ID"),
    names_from = c("Period", "Sector"),
    values_from = "Avg.diff",
    names_sep = "_"
  )
  # rasterize
  LMR_shp$name <- as.factor(LMR_shp$name)
  LMR_rast <- terra::rasterize(LMR_shp, Ref_grid, field = "name")

  FTE_rasts <- LMR_rast
  # Mimic 'subs' functionality for multiple new layers
  # (no new comments; just replacing the approach with terra)
  valmat <- as.data.frame(FTE_rasts, cells = TRUE, na.rm = FALSE)
  colnames(valmat)[2] <- "ID"
  FTE_list <- list()

  for (k in 2:ncol(LMR_values)) {
    tmp <- valmat
    colname_k <- colnames(LMR_values)[k]
    matchdf <- data.frame(ID = LMR_values$ID, newval = LMR_values[[colname_k]])
    tmp <- merge(tmp, matchdf, by = "ID", all.x = TRUE)
    tmp <- tmp[order(tmp$cell), ]
    newlayer <- terra::rast(FTE_rasts)
    terra::values(newlayer) <- tmp$newval
    FTE_list[[k - 1]] <- newlayer
  }

  FTE_rasts <- do.call(c, FTE_list)

  ensure_dir(config[["prepped_fte_dir"]])

  # vector file names
  FTE_file_names <- file.path(
    config[["prepped_fte_dir"]],
    paste0("avg_chg_fte_", names(LMR_values)[2:length(LMR_values)], ".tif")
  )

  # save a seperate file for each layer
  terra::writeRaster(
    FTE_rasts,
    filename = FTE_file_names,
    overwrite = TRUE
  )

  # update the predictor table with the file paths
  Pred_table_long[
    grepl(Covariate_ID, pattern = "Avg_chg_FTE") & is.na(Scenario_variant),
    "Prepared_data_path"
  ] <- FTE_file_names
  Pred_table_long[
    grepl(Covariate_ID, pattern = "Avg_chg_FTE") & is.na(Scenario_variant),
    "Prepared"
  ] <- "Y"

  ### -------------------------------------------------------------------------
  ### D.2- Biophysical: Soil, continentality and light (Descombes et al. 2020)
  ### -------------------------------------------------------------------------

  # grab url from table
  # FIXME temporarily hardcoding URL from here instead of relying on arbitrarily sorted table
  # https://www.envidat.ch/dataset/spatial-modelling-of-ecological-indicator-values/resource/e0faab13-0d1b-492a-8539-5370d48b9e35
  Biophys_url <- paste0(
    "https://www.envidat.ch/",
    "dataset/4ab13d14-6f96-41fd-96b0-b3ea45278b3d/",
    "resource/e0faab13-0d1b-492a-8539-5370d48b9e35",
    "/download/predictors.zip"
  )

  # use function to download and unpack
  Biophys_dir <- file.path(config[["predictors_raw_dir"]], "biophysical")
  if (refresh_cache) {
    lulcc.downloadunzip(url = Biophys_url, save_dir = Biophys_dir)
  }

  # download metadata
  Biophys_meta <-
    openxlsx::read.xlsx(paste0(
      "https://www.envidat.ch/dataset/4ab13d14-6f96-41fd-96b0-b3ea45278b3d/resource",
      "/81c046c3-8d1d-45bc-a833-7d8240cebd12/download/predictors_description.xlsx"
    )) |> data.table::as.data.table()

  # clean required column names
  names(Biophys_meta)[1:3] <- c("Layer_name", "Abbrev", "Desc_name")

  # correct spelling mistake
  Biophys_meta$Desc_name[25] <- "Continentality"

  # get layer names using variable names
  Biophys_var_names <- unique(Preds_to_prepare[
    Data_citation == "Descombes et al. 2020", Variable_name
  ])
  Biophys_layer_names <- Biophys_meta[
    Biophys_meta$Desc_name %in% Biophys_var_names,
    Layer_name
  ]

  # Get variable descriptive names
  Biophys_desc_names <- Biophys_meta[
    Biophys_meta$Desc_name %in% Biophys_var_names,
    Desc_name
  ]

  # FIXME how on earth can we be sure these names are in the right order?
  # Match descriptive names with the pred table and return the covariate ID
  names(Biophys_layer_names) <- unique(sapply(Biophys_desc_names, function(x) {
    Preds_to_prepare[Variable_name == x, Covariate_ID]
  }))

  # get layer paths
  Biophys_paths <- lapply(Biophys_layer_names, function(x) {
    list.files(Biophys_dir, full.names = TRUE, pattern = x, recursive = TRUE)
  })

  # loop over paths and process layers
  for (i in seq_along(Biophys_paths)) {
    Var_path <- Biophys_paths[[i]]
    Var_name <- names(Biophys_paths)[i]
    Raw_dat <- terra::rast(Var_path)
    Prepped_dat <- terra::project(
      Raw_dat, terra::crs(Ref_grid),
      res = terra::res(Ref_grid)
    )
    Prepped_dat_resamp <- terra::resample(Prepped_dat, Ref_grid)
    layer_path <-
      file.path(
        config[["prepped_lyr_path"]],
        unique(Preds_to_prepare[
          Covariate_ID == Var_name, Predictor_category
        ]),
        paste0(Var_name, ".tif")
      ) |> tolower()
    terra::writeRaster(Prepped_dat_resamp, layer_path, overwrite = TRUE)
    Pred_table_long[Covariate_ID == Var_name, "Prepared_data_path"] <- layer_path
    Pred_table_long[Covariate_ID == Var_name, "Prepared"] <- "Y"
  }

  ### -------------------------------------------------------------------------
  ### D.3- Population
  ### -------------------------------------------------------------------------

  ### Prepare municipality population data incorporating mutations

  # use if statement to only perform prep if layers have not already been prepared
  if (any(stringr::str_detect(Preds_to_prepare$Covariate_ID, "Muni_pop"))) {
    # read in PX data from http and convert to DF
    px_data <- as.data.frame(pxR::read.px(
      "https://dam-api.bfs.admin.ch/hub/api/dam/assets/23164063/master"
    ))


    # subset to desired rows based on conditions:
    # Total population on 1st of January;
    # Total populations (Swiss and foreigners)
    # Total population (Men and Women)
    raw_mun_popdata <- px_data[
      px_data$Demografische.Komponente == "Bestand am 31. Dezember" &
        px_data$Staatsangehörigkeit..Kategorie. == "Staatsangehörigkeit (Kategorie) - Total" &
        px_data$Geschlecht == "Geschlecht - Total",
    ]

    # Identify municipalities records by matching on the numeric contained in their name
    raw_mun_popdata <- raw_mun_popdata[
      grepl(".*?([0-9]+).*", raw_mun_popdata$Kanton.......Bezirk........Gemeinde.........),
      c(4:6)
    ]
    names(raw_mun_popdata) <- c("Name_Municipality", "Year", "Population")
    raw_mun_popdata <- raw_mun_popdata |> tidyr::pivot_wider(
      names_from = "Year",
      values_from = "Population"
    )
    # Remove the periods in the name column
    raw_mun_popdata$Name_Municipality <- gsub(
      "[......]", "", as.character(raw_mun_popdata$Name_Municipality)
    )

    # Seperate BFS number from name
    raw_mun_popdata$BFS_NUM <- as.numeric(gsub(
      ".*?([0-9]+).*", "\\1", raw_mun_popdata$Name_Municipality
    ))

    # Remove BFS number from name
    raw_mun_popdata$Name_Municipality <- gsub(
      "[[:digit:]]", "", raw_mun_popdata$Name_Municipality
    )

    # subset to only municipalities existing in 2021
    raw_mun_popdata <- raw_mun_popdata[raw_mun_popdata$`2021` > 0, ]

    if (refresh_cache) {
      # fetch municipality shape file
      lulcc.downloadunzip(
        url = paste0(
          "https://data.geo.admin.ch/ch.swisstopo.swissboundaries3d/swissboundaries3d_2021-07/",
          "swissboundaries3d_2021-07_2056_5728.shp.zip"
        ),
        save_dir = config[["ch_geoms_path"]]
      )
    }

    Muni_shp <- raster::shapefile(file.path(
      config[["ch_geoms_path"]],
      # TODO check that 1_3 and 1_5 are interchangeable
      "swissBOUNDARIES3D_1_5_TLM_HOHEITSGEBIET.shp"
    ))

    # filter out non-swiss municipalities
    Muni_shp <- Muni_shp[
      Muni_shp@data$ICC == "CH" &
        Muni_shp@data$OBJEKTART == "Gemeindegebiet",
    ]

    # Import data of municipality mutations from FSO web service using conditions:
    # 1. Muations between 01/01/1981 and 01/05/2022
    # 2. Name change mutations only (other mutations captured by shapefile)

    # scrape content from html address
    content <- rvest::read_html(paste0(
      "https://www.agvchapp.bfs.admin.ch/de/mutated-communes/results?",
      "EntriesFrom=01.01.1981&EntriesTo=01.05.2022&NameChange=True"
    ))
    muni_mutations <- rvest::html_table(content, fill = TRUE)[[1]]

    # remove 1st row because it contains additional column names
    muni_mutations <- muni_mutations[-1, ]

    # rename columns
    colnames(muni_mutations) <- c(
      "Mutation_Number", "Pre_canton_ID",
      "Pre_District_num", "Pre_BFS_num",
      "Pre_muni_name", "Post_canton_ID",
      "Post_district_num", "Post_BFS_num",
      "Post_muni_name", "Change_date"
    )

    # identify which municipalities have mutations associated with them
    mutation_index <- match(raw_mun_popdata$BFS_NUM, muni_mutations$Pre_BFS_num)

    # change municpality BFS number in population table according to the mutation for
    # each row in the pop df if there is an NA in the mutation index do not replace the
    # BFS number If there is not an NA then replace with the new BFS number of the
    # mutation table.

    for (i in seq_len(nrow(raw_mun_popdata))) {
      if (!is.na(mutation_index[i])) {
        raw_mun_popdata$BFS_NUM[[i]] <- muni_mutations[[mutation_index[[i]], "Post_BFS_num"]]
      }
    }

    # If after introducing the mutations we have multiple rows with the same BFS numbers
    # then we need to combine their populations values as these indicate municipalities merging

    if (length(unique(raw_mun_popdata$BFS_NUM)) != nrow(raw_mun_popdata)) {
      # get the indices of columns that represent the years
      Time_points <- na.omit(as.numeric(gsub(".*?([0-9]+).*", "\\1", colnames(raw_mun_popdata))))

      # create a empty df for results
      Muni_pop_final <- as.data.frame(matrix(
        ncol = length(Time_points),
        nrow = length(unique(raw_mun_popdata$BFS_NUM))
      ))
      colnames(Muni_pop_final) <- Time_points

      # Add column for BFS number
      Muni_pop_final$BFS_NUM <- sort(unique(raw_mun_popdata$BFS_NUM))

      # loop over date cols and rows summing values where BFS number is non-unique
      for (j in Time_points) {
        for (i in seq_along(unique(raw_mun_popdata$BFS_NUM))) {
          Muni_pop_final[i, paste(j)] <- sum(
            raw_mun_popdata[raw_mun_popdata$BFS_NUM == Muni_pop_final[i, "BFS_NUM"], paste(j)]
          )
        }
      }
      # replace old data with revised data
      raw_mun_popdata <- Muni_pop_final
    } # close if statement

    # Add canton number
    raw_mun_popdata$KANTONSNUM <- sapply(raw_mun_popdata$BFS_NUM, function(x) {
      unique(Muni_shp@data[Muni_shp@data$BFS_NUMMER == x, "KANTONSNUM"])
    })

    # save a copy raw_mun_popdata for preparation of future population layers
    ensure_dir(config[["raw_pop_dir"]])
    saveRDS(
      raw_mun_popdata,
      file.path(config[["raw_pop_dir"]], "raw_muni_pop_historic.rds")
    )

    ### Create historic municipality population rasters

    # seperate pop data for LULC years
    # (minus 2018 as only one layer is required for each period)
    pop_in_LULC_years <- raw_mun_popdata[, c("BFS_NUM", LULC_years[1:3])]

    Var_name <- "Muni_pop"

    # link with spatial municipality data, rasterize and save
    Muni_save_paths <- sapply(LULC_years[1:3], function(i) {
      # file_path
      save_path <- paste0(
        config[["prepped_lyr_path"]], "/",
        unique(Preds_to_prepare[Covariate_ID == Var_name, Predictor_category]),
        "/population/",
        Var_name, "_", i, ".tif"
      ) |> tolower()

      # loop over the BFS numbers of the polygons and match to population values
      Muni_shp@data[paste0("Pop_", i)] <- as.numeric(sapply(
        Muni_shp@data$BFS_NUMMER, function(Muni_num) {
          as.numeric(
            pop_in_LULC_years[pop_in_LULC_years$BFS_NUM == Muni_num, paste(i)]
          )
        },
        simplify = TRUE
      ))

      # rasterize
      pop_rast <- terra::rasterize(terra::vect(Muni_shp), Ref_grid, field = paste0("Pop_", i))

      # save
      ensure_dir(dirname(save_path))
      terra::writeRaster(pop_rast, save_path, overwrite = TRUE)

      return(save_path)
    }) # close loop over LULC years

    # Add prepared path to predictor table
    Pred_table_long[
      Covariate_ID == Var_name,
      "Prepared_data_path"
    ] <- Muni_save_paths
    Pred_table_long[
      Covariate_ID == Var_name,
      "Prepared"
    ] <- "Y"
  } # close if statement

  ### =========================================================================
  ### X- Update predictor table for SA predictors
  ### =========================================================================

  # load predictor_table as workbook to add sheets
  Pred_table_update <- openxlsx::loadWorkbook(file = config[["pred_table_path"]])
  Pred_table_update <- openxlsx::createWorkbook(creator = "lulcc-ch")

  # split the table back into Dfs for each period and save
  Periodic_pred_tables <- split(Pred_table_long, Pred_table_long$period)

  # loop over period tables adding sheets and adding the predictors to them
  for (i in names(Periodic_pred_tables)) {
    # the try() is necessary in case sheets already exist
    try(openxlsx::addWorksheet(Pred_table_update, sheetName = paste(i)))
    openxlsx::writeDataTable(Pred_table_update, sheet = paste(i), x = Periodic_pred_tables[[i]])
  }

  # save workbook
  openxlsx::saveWorkbook(Pred_table_update, config[["pred_table_path"]], overwrite = TRUE)

  message(" Preparation of Suitability and accessibility predictor layers complete")

  ### =========================================================================
  ### X- Create Neighbourhood predictors
  ### =========================================================================

  # This process is lengthy so is presented in a seperate script,which includes
  # updating of the predictor table after layer creation

  # raw script
  nhood_predictor_prep()
}
