#############################################################################
## Deterministic_trans_prep: Prepare spatial data for locations of deterministic
## trans to implement during simulations
##
## Date: 03-05-2023
## Author: Ben Black
#############################################################################

deterministic_trans_prep <- function(config = get_config()) {
  ### =========================================================================
  ### A- Preparation
  ### =========================================================================
  LULC_years <- gsub(
    pattern = ".*?([0-9]+).*",
    replacement = "\\1",
    x = list.files(
      config[["historic_lulc_basepath"]],
      full.names = FALSE, pattern = ".gri"
    )
  ) |> as.integer()

  data_periods <- config[["data_periods"]]
  step_length <- config[["step_length"]]

  # The model lookup table specifies which transitions are modelled and
  # should be used to subset the transition rates tables

  # Load Model lookup tables for each period and subset to just transition names
  Periodic_trans_names <- lapply(
    data_periods,
    function(Period) {
      full_table <- openxlsx::read.xlsx(
        config[["model_lookup_path"]],
        sheet = Period
      )
      return(unique(full_table[["Trans_name"]]))
    }
  )
  names(Periodic_trans_names) <- data_periods

  # load simulation control table
  Sim_control_table <- read.csv(config[["simctrl_tbl_path"]])

  # vector all time steps in calibration and simulation
  All_time_steps <- seq(
    from = min(LULC_years),
    to = max(Sim_control_table$scenario_end.real),
    by = step_length
  )

  # vector simulation time steps
  # (i.e. only the time steps until the end, omitting the start year)
  Sim_time_steps <- All_time_steps[
    dplyr::between(
      All_time_steps,
      min(Sim_control_table$scenario_start.real) + step_length,
      max(Sim_control_table$scenario_end.real)
    )
  ]

  # vector all simulation years (i.e. including initial year)
  Sim_years <- c(
    round(max(LULC_years) / step_length) * step_length,
    Sim_time_steps
  )

  ### =========================================================================
  ### B - calculate glacial change rates and wrangle indices of change locations
  ### =========================================================================

  # The files provided by Farinotti et al. contained the locations
  # of glacier (1) and absence of glacier (0) according to the index of cells in
  # our spatial grid under the different RCPs.

  # We need to use these indices of glacier locations to calculate
  # glacial coverage in each time step for each scenario as input
  # for the calculation of modified transition rates.

  # Wrangle glacial location data for each RCP
  Glacier_indices <- lapply(
    list.files(
      file.path(config[["glacial_change_path"]], "median_scenarios"),
      full.names = TRUE
    ),
    function(x) {
      # load
      Glacier_index <- read.table(file = x, skip = 10, header = TRUE)

      # adjust column names to reflect years
      colnames(Glacier_index)[2:ncol(Glacier_index)] <- seq(from = 2005, to = 2100, by = 5)

      # subset to simulation years
      return(Glacier_index[, c("ID_loc", Sim_years)])
    }
  )

  # extract RCP designation between other strings
  names(Glacier_indices) <- lapply(
    list.files(
      file.path(config[["glacial_change_path"]], "median_scenarios"),
      full.names = FALSE
    ),
    function(x) stringr::str_match(x, "series_\\s*(.*?)\\s*_median")[, 2]
  )

  # create directory for scenario specific indices
  Glacial_scenario_dir <- file.path(
    config[["glacial_change_path"]],
    "scenario_indices"
  )
  ensure_dir(Glacial_scenario_dir)

  # Loop over Glacial_indices saving each as rds. using name of RCP in file name
  lapply(
    names(Glacier_indices),
    function(x) {
      saveRDS(
        Glacier_indices[[x]],
        paste0(Glacial_scenario_dir, "/", x, "_glacial_change.rds")
      )
    }
  )

  # calculate glacial change area per time step and combine to single DF
  Glacial_change <- data.table::rbindlist(lapply(Glacier_indices, function(x) {
    # calculate col sums
    Area_per_year <- colSums(x[, 2:ncol(x)])

    # calculate change in area between each time point
    Areal_change <- data.frame(t(
      sapply(
        1:(length(Area_per_year) - 1),
        function(i) {
          return(Area_per_year[i] - Area_per_year[i + 1])
        }
      )
    ))

    colnames(Areal_change) <- names(Area_per_year)[2:length(Area_per_year)]
    return(Areal_change)
  }), idcol = "RCP")

  # save areal change across scenario's table
  # FIXME this table is not the one that was committed to the repo 
  # - I removed that xlsx with this commit
  # - the idcol above should possibly read "Scenario"
  # - the Glacier_indices names should possibly not be named according to RCP scenarios,
  #   but to BAU / EI_NAT, EI_CUL, EI_SOC, GR_EX
  xlsx::write.xlsx(Glacial_change, config[["glacial_area_change_xlsx"]], row.names = FALSE)
}
