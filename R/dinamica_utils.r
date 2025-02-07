#' Dinamica Utility Functions
#'
#' Various functions to interact with Dinamica from R
#'
#' @name dinamica_utils
NULL

#' @describeIn dinamica_utils Check Dinamica is installed
#' @export

check_dinamica <- function() {
  # install Dinamica R package from source
  # install.packages("Model/dinamica_1.0.4.tar.gz", repos=NULL, type="source")

  # TODO: Check if Dinamica EGO is already installed
  # Diego.installed <- system(command = paste('*dinamica7* -v'))==0
  stop("This doesn't do anything yet")
}

#' @describeIn dinamica_utils Get Dinamica path
#' @export
get_dinamica_path <- function() {
  return("C:\\Program Files\\Dinamica EGO 7\\DinamicaConsole7.exe")
}

#' @describeIn dinamica_utils Run a dinamica simulation set
#' @export
run_dinamica_sims <- function() {
  Pre_check_result <- lulccfunspkg::lulcc.modelprechecks(
    Control_table_path,
    Param_dir = simulation_param_dir
  )

  # Run the Dinamica simulation model
  # Fail pre-check condition
  if (Pre_check_result == FALSE) {
    print("Some elements required for modelling are not present/incorrect,
        consult the pre-check results object")
  } else if (Pre_check_result == TRUE) {
    # save a temporary copy of the model.ego file to run
    print("Creating a copy of the Dinamica model using the current control table")
    Temp_model_path <- gsub(
      ".ego", paste0("_simulation_", Sys.Date(), ".ego"),
      "model/dinamica_models/lulcc_ch.ego"
    )
    writeLines(Model_text, Temp_model_path)

    # vector a path for saving the output text of this simulation
    # run which indicates any errors
    output_path <- paste0(Sim_log_dir, "/simulation_output_", Sys.Date(), ".txt")

    # set environment path for Dinamica log/debug files
    # create a temporary dir for storing the Dinamica output files
    # Logdir <- "Model/Dinamica_models/Model_log_files"
    # dir.create(Logdir)
    # Win_logdir <- paste0(getwd(), "/", Logdir)

    print("Starting to run model with Dinamica EGO")
    lulccfunspkg::check_dinamica()
    system2(
      command = lulccfunspkg::get_dinamica_path(),
      args = c("-disable-parallel-steps -log-level 7", Temp_model_path)
      # env = c(
      #   DINAMICA_EGO_7_LOG_PATH = Win_logdir
      # )
    )

    # because the simulations may fail without the system command returning an error
    # (if the error occurs in Dinamica) then check the simulation control table to see
    # if/how many simulations have failed
    Updated_control_tbl <- read.csv(simctrl_tbl_path)

    if (any(Updated_control_tbl$completed.string == "ERROR")) {
      message(
        length(which(Updated_control_tbl$completed.string == "ERROR")),
        "of", nrow(Updated_control_tbl),
        "simulations have failed to run till completion, check log for details of errors"
      )
    } else {
      # Send completion message
      print("All simulations completed sucessfully")

      # Delete the temporary model file
      # unlink(Temp_model_path)
    }
  } # close if statement running simulation
}
