#if (!file.exists("/.dockerenv")) {
#  # Don't activate renv if running in a docker container - 
#  # The assumption is that we use the system library there
#  source("renv/activate.R")
#}
options(
  "menu.graphics" = FALSE,
  # see https://rlang.r-lib.org/reference/rlang_backtrace_on_error.html
  rlang_backtrace_on_error = "full",
  error = rlang::entrace
)
