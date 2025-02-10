#' LULCC_CH setup: control table, config
#'
#' Creates / reads control table and config files
#'
#' @export

get_config <- function(
    scenario_names = c("BAU", "EI-NAT", "EI-CUL", "EI-SOC", "GR-EX"),
    step_length = 5L,
    simctrl_tbl_path = "tools/simulation_control.csv",
    data_periods = c("1985_1997", "1997_2009", "2009_2018"),
    regionalization = TRUE) {
  # FIXME the input simulation control table should remain untouched, i.e. not be
  # updatable in place
  # TODO does it make sense to generate this table programmatically?
  # prepare_simctrl_tbl() |> readr::write_csv(simctrl_tbl_path)

  # TODO path composition could happen here through nested lists, or e.g. json dicts?
  data_basepath <- "data-raw" # the name "data" is taken by R package convention
  historic_lulc_basepath <- file.path(data_basepath, "historic_lulc")
  bioreg_dir <- file.path(data_basepath, "bioregions")
  predictors_dir <- file.path(data_basepath, "preds")
  predictors_prepped_dir <- file.path(predictors_dir, "prepared")
  predictors_raw_dir <- file.path(predictors_dir, "raw")
  allocation_pars_dir <- file.path(data_basepath, "allocation_parameters")

  # TODO move all the config tables into a common xlsx and read individual sheets?
  config <- list(
    LULC_aggregation_path = "tools/lulc_class_aggregation.xlsx", # LULC class aggregation table
    model_specs_path = "tools/model_specs.xlsx", # model specifications table
    param_grid_path = "tools/param-grid.xlsx", # model hyper parameter grids
    pred_table_path = "tools/predictor_table.xlsx", # predictor table
    spat_ints_path = "tools/spatial_interventions.csv", # spatial interventions table
    EI_ints_path = "tools/ei_interventions.xlsx", # EI interventions table
    ref_grid_path = file.path(data_basepath, "ref_grid.grd"),
    calibration_param_dir = file.path(allocation_pars_dir, "calibration"),
    simulation_param_dir = file.path(allocation_pars_dir, "simulation"),
    trans_rate_table_dir = file.path(data_basepath, "transition_tables", "prepared_trans_tables"),
    simctrl_tbl_path = simctrl_tbl_path, # simulation control table
    step_length = step_length,
    scenario_names = scenario_names,
    # FIXME the next two used to be read from model_specs_path;
    # we should first move to a coherent singular config
    data_periods = data_periods,
    regionalization = regionalization,
    inclusion_thres = 0.5,
    arealstat_zip_remote =
      "https://dam-api.bfs.admin.ch/hub/api/dam/assets/32376216/appendix",
    historic_lulc_basepath = historic_lulc_basepath,
    arealstat_zip_local = file.path(historic_lulc_basepath, "ag-b-00.03-37-area-all-csv.zip"),
    rasterized_lulc_dir = file.path(historic_lulc_basepath, "rasterized"),
    bioreg_dir = bioreg_dir,
    bioreg_zip_remote = "https://data.geo.admin.ch/ch.bafu.biogeographische_regionen/data.zip",
    bioreg_zip_local = file.path(bioreg_dir, "biogeographische_regionen.zip"),
    predictors_raw_dir = predictors_raw_dir,
    ch_geoms_path = file.path(predictors_raw_dir, "ch_geoms"),
    prepped_lyr_path = file.path(predictors_prepped_dir, "layers"),
    reference_crs = "epsg:2056"
  )

  return(config)
}
