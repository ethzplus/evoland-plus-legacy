#' LULCC_CH setup: control table, config
#'
#' Creates / reads control table and config files
#'
#' @export

setup <- function(
    scenario_names = c("BAU", "EI-NAT", "EI-CUL", "EI-SOC", "GR-EX"),
    scenario_start = 2020L,
    scenario_end = 2060L,
    step_length = 5L,
    reps = 1L,
    simctrl_tbl_path = "tools/simulation_control.csv") {
  prepare_simctrl_tbl(
    scenario_names = scenario_names,
    scenario_start = scenario_start,
    scenario_end = scenario_end,
    step_length = step_length,
    reps = reps
  ) |>
    readr::write_csv(simctrl_tbl_path)

  # Modelling set-up
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
    inclusion_thres = 0.5
  )

  # Import model specifications table
  model_specs <- readxl::read_excel(config$model_specs_path)

  # Vector data periods contained in model specifications table
  config$data_periods <- unique(model_specs$data_period_name)

  # attach string to env. indicating whether regionalized datasets should be produced
  if (any(grep(
    model_specs$model_scale,
    pattern = "regionalized",
    ignore.case = TRUE
  )) == TRUE) {
    config$regionalization <- TRUE
  } else {
    config$regionalization <- FALSE
  }

  return(config)
}
