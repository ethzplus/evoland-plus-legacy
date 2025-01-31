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
