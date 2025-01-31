#' LULCC_CH setup: control table, config
#'
#' Creates / reads control table and config files
#'
#' @export

setup <- function() {
  ### Simulation control table prep
  Sim_control_path <- "tools/simulation_control.csv"
  # TODO the code I found here was missing some columns vs. the csv that's been committed
  # to the repo.
  # simcontrolprep() |>
  #   readr::write_csv(Sim_control_path)

  # Modelling set-up
  Model_tool_vars <- list(
    LULC_aggregation_path = "tools/lulc_class_aggregation.xlsx", # LULC class aggregation table
    Model_specs_path = "tools/model_specs.xlsx", # model specifications table
    Param_grid_path = "tools/param-grid.xlsx", # model hyper parameter grids
    Pred_table_path = "tools/predictor_table.xlsx", # predictor table
    Spat_ints_path = "tools/spatial_interventions.csv", # spatial interventions table
    EI_ints_path = "tools/ei_interventions.xlsx", # EI interventions table
    Ref_grid_path = "data/ref_grid.grd", # spatial grid to standardise spatial data
    Calibration_param_dir = "data/allocation_parameters/calibration",
    Simulation_param_dir = "data/allocation_parameters/simulation",
    Trans_rate_table_dir = "data/transition_tables/prepared_trans_tables",
    Sim_control_path = Sim_control_path, # simulation control table
    Step_length = Step_length,
    Scenario_names = Scenario_names,
    Inclusion_thres = 0.5
  )

  # Import model specifications table
  model_specs <- readxl::read_excel(Model_tool_vars$Model_specs_path)

  # Vector data periods contained in model specifications table
  Model_tool_vars$Data_periods <- unique(model_specs$Data_period_name)

  # attach string to env. indicating whether regionalized datasets should be produced
  if (any(grep(
    model_specs$Model_scale,
    pattern = "regionalized",
    ignore.case = TRUE
  )) == TRUE) {
    Model_tool_vars$Regionalization <- TRUE
  } else {
    Model_tool_vars$Regionalization <- FALSE
  }
}
