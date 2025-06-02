#' Dinamica Utility Functions
#'
#' Functions to interact with Dinamica from R
#'
#' @name dinamica_utils
NULL

#' @describeIn dinamica_utils Execute a Dinamica .ego file using `DinamicaConsole`
#' @param model_path Path to the .ego model file to run. Any submodels must be included
#' in a directory of the exact form `basename("model.ego")_Submodels`,
#' [see wiki](https://csr.ufmg.br/dinamica/dokuwiki/doku.php?id=submodels)
#' @param disable_parallel Whether to disable parallel steps (default TRUE)
#' @param log_level Logging level (1-7, default NULL)
#' @param additional_args Additional arguments to pass to DinamicaConsole, see
#' [this wiki page](https://dinamicaego.com/dokuwiki/doku.php?id=tutorial:dinamica_ego_script_language_and_console_launcher)  # nolint: line_length_linter.
#'
#' @export
exec_dinamica <- function(model_path,
                          disable_parallel = TRUE,
                          log_level = NULL,
                          additional_args = NULL) {
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

  invisible(processx::run(
    command = "DinamicaConsole",
    args = args,
    spinner = TRUE,
    env = c(
      "current",
      DINAMICA_HOME = fs::path_dir(model_path)
    )
  ))
}

#' @describeIn dinamica_utils Run a Dinamica EGO extrapolation simulation
#' @export
run_dinamica_extrapolation <- function(
    run_modelprechecks = TRUE,
    config = get_config(),
    work_dir = file.path(".", format(Sys.time(), "%Y-%m-%d %Hh%Mm%Ss"))) {
  if (run_modelprechecks) {
    stopifnot(lulcc.modelprechecks())
  }

  # find raw ego files with decoded R/Python code chunks
  decoded_files <- fs::dir_ls(
    path = system.file("dinamica_model", package = "evoland"),
    regexp = "\\.ego-decoded$",
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

  message("Starting to run model with Dinamica EGO")
  exec_dinamica(model_path = fs::path(work_dir, "evoland.ego"))

  # because the simulations may fail without the system command returning an error
  # (if the error occurs in Dinamica) then check the simulation control table to see
  # if/how many simulations have failed
  Updated_control_tbl <- read.csv(config[["simctrl_tbl_path"]])

  if (any(Updated_control_tbl$completed.string == "ERROR")) {
    stop(
      length(which(Updated_control_tbl$completed.string == "ERROR")),
      "of", nrow(Updated_control_tbl),
      "simulations have failed to run till completion, check log for details of errors"
    )
  } else {
    # Send completion message
    message("All simulations completed sucessfully")
  }
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
