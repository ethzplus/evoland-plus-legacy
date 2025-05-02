#' Get predictor data from Zenodo
#'
#' This fetches predictor data from a very specifically structured Zenodo repo, namely
#' like <https://doi.org/10.5281/zenodo.8263509>
#'
#' @param url The url of the zip to fetch
#' @param target_dir The dir in which to depose the data
#'
#' @export

fetch_zenodo_predictors <- function(
    url = "https://zenodo.org/records/8263509/files/LULCC_CH_dat.zip",
    target_dir = "data/preds/") {
  # WONTFIX currently, the raw data resides at the zenodo record, which will stay about
  # as persistent as a DOI based lookup. We need to make assumptions about the data
  # structure anyways, so let's stick with this.
  # WONTFIX no2 the zip file is corrupted on zenodo, so you need to locally fix it using
  # zip -FF infile.zip --out outfile.zip
  # FIXME in #18 by reuploading

  tmpfile <- tempfile()
  curl::curl_download(
    url = url,
    destfile = tmpfile
  )

  raw_files <-
    zip::zip_list(tmpfile) |>
    purrr::pluck("filename") |>
    # TODO are these files enough for reproducibility?
    purrr::keep(stringr::str_detect, pattern = "Data/Raw")

  zip::unzip(
    zipfile = tmpfile,
    files = raw_files,
    exdir = target_dir
  )

  # move files into place
  file.rename(
    from = file.path(target_dir, raw_files),
    to = file.path(
      target_dir,
      stringr::str_remove(raw_files, "LULCC_CH_dat/Data/Raw/")
    )
  )

  unlink(file.path(target_dir, "LULCC_CH_dat/Data/Raw/"))
  unlink(tmpfile)
}

get_employment_scenarios <- function(
    urls = c(
      "https://zenodo.org/api/records/4774914/files/combo.zip/content",
      "https://zenodo.org/api/records/4774914/files/Metadata.xlsx/content",
      "https://zenodo.org/api/records/4774914/files/référence.zip/content",
      "https://zenodo.org/api/records/4774914/files/sensibilité_combo.zip/content",
      "https://zenodo.org/api/records/4774914/files/ecolo.zip/content"
    ),
    target_dir) {
  # WONTFIX currently, the raw data resides at the zenodo record, which will stay about
  # as persistent as a DOI based lookup; not worth relying on the substandard zenodo
  # API. We need to make assumptions about the data structure anyways, so let's stick
  # with this.
  if (missing(target_dir)) {
    target_dir <- file.path(
      get_config()[["raw_employment_dir"]],
      "employment_scenarios"
    )
  }

  purrr::walk(
    urls,
    \(x) lulcc.downloadunzip(
      x,
      save_dir = target_dir,
      filename = stringr::str_remove(x, "/content") |> basename()
    )
  )
}
