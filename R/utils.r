#' Utility functions for evoland
#'
#' Sundry functions that are often used
#'
#' @name util
NULL

#' @describeIn util Ensure that a directory exists
ensure_dir <- function(dir) {
  dir.create(dir, showWarnings = FALSE, recursive = TRUE)
  invisible(dir)
}
