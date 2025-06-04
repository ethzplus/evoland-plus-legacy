#' Dinamica Utility Functions
#'
#' Functions to interact with Dinamica from R
#'
#' @name dinamica_utils
NULL

#' @describeIn dinamica_utils Execute a Dinamica .ego file using `DinamicaConsole`
#' @param model_path Path to the .ego model file to run. Any submodels must be included
#' in a directory of the exact form `basename(modelpath)_ego_Submodels`, [see
#' wiki](https://csr.ufmg.br/dinamica/dokuwiki/doku.php?id=submodels)
#' @param disable_parallel Whether to disable parallel steps (default TRUE)
#' @param log_level Logging level (1-7, default NULL)
#' @param additional_args Additional arguments to pass to DinamicaConsole, see
#' `DinamicaConsole -help`
#' @param verbose Default FALSE, else prints stdout/stderr immediately
#'
#' @export

exec_dinamica <- function(model_path,
                          disable_parallel = TRUE,
                          log_level = NULL,
                          additional_args = NULL,
                          verbose = FALSE) {
  args <- character()
  if (disable_parallel) {
    args <- c(args, "-disable-parallel-steps")
  }
  if (!is.null(log_level)) {
    args <- c(args, paste0("-log-level ", log_level))
  }
  if (!is.null(additional_args)) {
    args <- c(args, additional_args)
  }
  args <- c(args, model_path)

  res <- processx::run(
    # If called directly, DinamicaConsole does not flush its buffer upon SIGTERM.
    # stdbuf -oL forces flushing the stdout buffer after every line.
    command = "stdbuf",
    args = c(
      "-oL",
      "DinamicaConsole", # assume that $PATH is complete
      args
    ),
    error_on_status = FALSE,
    echo = verbose,
    spinner = TRUE,
    env = c(
      "current",
      DINAMICA_HOME = fs::path_dir(model_path)
    )
  )

  # strip ansi escape sequences. some are unmatched, permanently altering output color.
  res[["stdout"]] <- cli::ansi_strip(res[["stdout"]])
  res[["stderr"]] <- cli::ansi_strip(res[["stderr"]])

  if (res[["status"]] != 0L) {
    # on error first write logfile, then abort
    logfile <- fs::path(fs::path_dir(model_path), "dinamica.log")
    writeLines(
      paste(
        Sys.time(),
        "stdout", res[["stdout"]],
        "stderr", res[["stderr"]],
        sep = "\n\n"
      ),
      logfile
    )
    cli::cli_abort(
      "Rerun with verbose = TRUE to see full logs or watch {.file {logfile}}",
      class = "dinamicaconsole_error",
      body = res[["stderr"]]
    )
  }

  res
}

#' @describeIn dinamica_utils Run a Dinamica EGO extrapolation simulation
#' @param run_modelprechecks bool, Validate that everything's in place for a model run.
#' Set to false for calibration runs.
#' @param work_dir Working dir, where to place ego files and control table
#' @param verbose bool
#' @param simctrl_tbl_template Path to the template simulation control table
#' @export
run_dinamica_extrapolation <- function(
    run_modelprechecks = TRUE,
    config = get_config(),
    work_dir = format(Sys.time(), "%Y-%m-%d_%Hh%Mm%Ss"),
    verbose = FALSE) {
  if (run_modelprechecks) {
    stopifnot(lulcc.modelprechecks())
  }

  # find raw ego files with decoded R/Python code chunks
  decoded_files <- fs::dir_ls(
    path = system.file("dinamica_model", package = "evoland"),
    regexp = "evoland.*\\.ego-decoded$",
    recurse = TRUE
  )

  purrr::walk(decoded_files, function(decoded_file) {
    # Determine relative path and new output path with .ego extension
    rel_path <- fs::path_rel(
      path = decoded_file,
      start = system.file("dinamica_model", package = "evoland")
    )
    out_path <- fs::path_ext_set(fs::path(work_dir, rel_path), "ego")
    fs::dir_create(fs::path_dir(out_path))
    process_dinamica_script(decoded_file, out_path)
  })

  # move simulation control csv into place
  fs::file_copy(
    config[["simtrl_tbl_path"]],
    fs::path(work_dir, "simulation_control.csv")
  )

  cli::cli_inform("Starting to run model with Dinamica EGO")
  exec_dinamica(model_path = fs::path(work_dir, "evoland.ego"), verbose = verbose)
}

#' @describeIn dinamica_utils Encode or decode raw R and Python code chunks in .ego
#' files and their submodels to/from base64
#' @param infile Input file path. Treated as input if passed AsIs using `base::I()`
#' @param outfile Output file path (optional)
#' @param mode Character, either "encode" or "decode"
#' @param check Default TRUE, simple check to ensure that you're handling what you're expecting

process_dinamica_script <- function(infile, outfile, mode = "encode", check = TRUE) {
  mode <- rlang::arg_match(mode, c("encode", "decode"))
  if (inherits(infile, "AsIs")) {
    file_text <- infile
  } else {
    # read the input file as a single string
    file_text <- readChar(infile, file.info(infile)$size)
  }

  # match the Calculate R or Python Expression blocks - guesswork involved
  pattern <- ':= Calculate(?:Python|R)Expression "(\\X*?)" (?:\\.no )?\\{\\{'
  # extracts both full match [,1] and capture group [,2]
  matches <- stringr::str_match_all(file_text, pattern)[[1]]

  if (check) {
    non_base64_chars_present <- stringr::str_detect(matches[, 2], "[^A-Za-z0-9+=\\n/]")
    if (mode == "encode" && any(!non_base64_chars_present)) {
      stop(
        "There are no non-base64 chars in one of the matched patterns, which seems ",
        "unlikely for an unencoded code chunk. Override this check with ",
        "check = FALSE if you're sure that this is an unencoded file."
      )
    } else if (mode == "decode" && any(non_base64_chars_present)) {
      stop(
        "There are non-base64 chars in one of the matched patterns, which seems ",
        "unlikely for an encoded code chunk. Override this check with ",
        "check = FALSE if you're sure that this is an unencoded file."
      )
    }
  }

  if (nrow(matches) > 0) {
    encoder_decoder <- ifelse(mode == "encode",
      \(code) base64enc::base64encode(charToRaw(code)),
      \(code) rawToChar(base64enc::base64decode(code))
    )
    # matches[,2] contains the captured R/python code OR base64-encoded code
    encoded_vec <- purrr::map_chr(matches[, 2], encoder_decoder)
    # replace each original code with its base64 encoded version
    for (i in seq_along(encoded_vec)) {
      file_text <- stringr::str_replace(
        string = file_text,
        pattern = stringr::fixed(matches[i, 2]),
        replacement = encoded_vec[i]
      )
    }
  }

  # Write to outfile if specified, otherwise return the substituted string
  if (!missing(outfile)) {
    writeChar(file_text, outfile, eos = NULL)
    invisible(outfile)
  } else {
    file_text
  }
}
