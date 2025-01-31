#' Create a simulation control table
#'
#' From master lulcc: how to prepare a sim control table
#' User enter start and end dates for the scenarios either enter a single number
#' value or a vector of values the same length as the number of scenarios earliest
#' possible model start time is 1985 and end time is 2060, simulations begin from
#' 2020 and we have initially agreed to use 5 year time steps
#'
#' @param Scenario_names char
#' @param Scenario_start 1985 or later
#' @param Scenario_end 2060 per default
#' @param Step_length in years
#' @param reps Number of runs for each simulation
#'
#' @export

simcontrolprep <- function(
    Scenario_names = c("BAU", "EI-NAT", "EI-CUL", "EI-SOC", "GR-EX"),
    Scenario_start = 2020L,
    Scenario_end = 2060L,
    Step_length = 5L,
    reps = 1L) {
  Simulation_control_table <- data.frame(matrix(ncol = 11, nrow = 0))
  colnames(Simulation_control_table) <- c(
    "Simulation_num.",
    "Scenario_ID.string",
    "Simulation_ID.string",
    "Model_mode.string",
    "Scenario_start.real",
    "Scenario_end.real",
    "Step_length.real",
    "Parallel_TPC.string",
    "Spatial_interventions.string",
    "Deterministic_trans.string",
    "Completed.string"
    # FIXME this is lacking some columns, compare with tools/simulation_control.csv
    # might be because they get added later?
    # "Econ_scenario.string",
    # "Climate_scenario.string",
    # "Pop_scenario.string",
    # "EI_intervention_ID.string",
  )

  # expand vector of scenario names according to number of repetitions and add to table
  Scenario_IDs <- c(sapply(Scenario_names, function(x) rep(x, reps), simplify = TRUE))
  Simulation_control_table[1:length(Scenario_IDs), "Scenario_ID.string"] <- Scenario_IDs

  # fill other columns
  Simulation_control_table$Simulation_ID.string <- rep(paste0("v", seq(1, reps, 1)), length(Scenario_names))
  Simulation_control_table$Scenario_start.real <- if (length(unique(Scenario_start)) == 1) {
    Scenario_start
  } else {
    c(rep(Scenario_start, length(Scenario_names)))
  }
  Simulation_control_table$Scenario_end.real <- if (length(unique(Scenario_end)) == 1) {
    Scenario_end
  } else {
    c(rep(Scenario_end, length(Scenario_names)))
  }
  Simulation_control_table$Step_length.real <- Step_length
  Simulation_control_table$Model_mode.string <- "Simulation"
  Simulation_control_table$Simulation_num. <- seq(1, nrow(Simulation_control_table), 1)
  Simulation_control_table$Parallel_TPC.string <- "N"
  Simulation_control_table$Spatial_interventions.string <- "Y"
  Simulation_control_table$Deterministic_trans.string <- "Y"
  Simulation_control_table$Completed.string <- "N"

  return(Simulation_control_table)
}
