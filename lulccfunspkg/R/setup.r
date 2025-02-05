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

  # TODO move all the config tables into a common xlsx and read individual sheets?
  config <- list(
    LULC_aggregation_path = "tools/lulc_class_aggregation.xlsx", # LULC class aggregation table
    model_specs_path = "tools/model_specs.xlsx", # model specifications table
    param_grid_path = "tools/param-grid.xlsx", # model hyper parameter grids
    pred_table_path = "tools/predictor_table.xlsx", # predictor table
    spat_ints_path = "tools/spatial_interventions.csv", # spatial interventions table
    EI_ints_path = "tools/ei_interventions.xlsx", # EI interventions table
    ref_grid_path = "data/ref_grid.grd", # spatial grid to standardise spatial data
    calibration_param_dir = "data/allocation_parameters/calibration",
    simulation_param_dir = "data/allocation_parameters/simulation",
    trans_rate_table_dir = "data/transition_tables/prepared_trans_tables",
    simctrl_tbl_path = simctrl_tbl_path, # simulation control table
    step_length = step_length,
    scenario_names = scenario_names,
    # FIXME the next two used to be read from model_specs_path;
    # we should first move to a coherent singular config
    data_periods = data_periods,
    regionalization = regionalization,
    inclusion_thres = 0.5
  )

  return(config)
}
