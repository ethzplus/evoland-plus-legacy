#' Get predictor data from Zenodo
#'
#' This fetches predictor data from a very specifically structured Zenodo repo, namely
#' like <https://doi.org/10.5281/zenodo.8263509>
#'
#' @param DOI which identifier to use
#'
#' @export

fetch_zenodo_predictors <- function(doi = "10.5281/zenodo.8263509") {
  # connect to Zenodo API
  zenodo <- zen4r::ZenodoManager$new()

  # Get record info
  # TODO: won't work until record is made open access
  rec <- zenodo$getRecordByDOI(doi)
  files <- rec$listFiles(pretty = TRUE)
  files <- my_rec$listFiles(pretty = TRUE)

  # increase timeout limit for downloading file
  options(timeout = 6000)

  # create a temporary directory to store the zipped file
  tmpdir <- tempdir()

  # Download to tmpdir
  my_rec$downloadFiles(path = tmpdir)
  download.file(files$download, paste0(tmpdir, "/", files$filename), mode = "wb")

  # using function
  decompress_file(tmpdir, file = paste0(tmpdir, "\\", files$filename), .file_cache = FALSE)

  # using r utils::unzip
  unzip(
    paste0(tmpdir, "/", files$filename),
    exdir = str_remove(paste0(tmpdir, "/", files$filename), ".zip")
  )

  # TODO: update path when Manuel has finished Zenodo upload.
  # select just the raw data
  raw_data_path <- str_replace(paste0(tmpdir, "/", files$filename), ".zip", "/Data/Raw")

  # Move files into project structure
  file.copy(raw_data_path, "Data/Preds", recursive = TRUE)

  # remove the zipped folder in temp dir
  unlink(paste0(tmpdir, "/", files$filename))
}

decompress_file <- function(directory, file, .file_cache = FALSE) {
  stop("This code should be checked before being run")
  if (.file_cache == TRUE) {
    print("decompression skipped")
  } else {
    # Set working directory for decompression
    # simplifies unzip directory location behavior
    # FIXME get rid of sideeffect
    wd <- getwd()
    setwd(directory)

    # Run decompression
    decompression <-
      system2("unzip",
        args = c(
          "-o", # include override flag
          file
        ),
        stdout = TRUE
      )

    # Reset working directory
    setwd(wd)
    rm(wd)

    # Test for success criteria
    # change the search depending on
    # your implementation
    if (grepl("Warning message", tail(decompression, 1))) {
      print(decompression)
    }
  }
}
