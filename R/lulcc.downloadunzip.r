#' lulcc.downloadunzip
#'
#' Download folder from URL and unzip to target directory
#'
#' @param url character string url to download folder from
#'   produced by function lulcc.evalfeatureselection
#' @param save_path character string path of directory to save unzipped files in
#' @param filename defaults to the `basename(url)`
#'
#' @author Ben Black
#' @export

lulcc.downloadunzip <- function(url, save_dir, filename = basename(url)) {
  # create dir
  ensure_dir(save_dir)

  # if file is zipped then create temp dir and extract
  if (grepl("\\.zip", filename)) {
    tmpdir <- tempdir()
    download.file(url, paste0(tmpdir, "/", filename), mode = "wb")
    unzip(
      zipfile = paste0(tmpdir, "/", filename),
      exdir = save_dir,
      # system unzip on *nix; more robust against encoding issues
      unzip = getOption("unzip")
    )
    unlink(tmpdir) # remove temp dir
  } else { # non-zipped just download
    download.file(url, paste0(save_dir, "/", filename), mode = "wb")
  }
}
