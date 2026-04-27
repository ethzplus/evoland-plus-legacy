#' Get predictor data from Zenodo
#'
#' This fetches predictor data from a very specifically structured Zenodo repo, namely
#' like <https://doi.org/10.5281/zenodo.19481151>
#'
#' @param url The url of the zip to fetch
#' @param target_dir The dir in which to depose the data
#'
#' @export

fetch_zenodo_predictors <- function(
    url = "https://zenodo.org/records/19481151/files/evoland-ch-data.zip",
    target_dir = fs::path(Sys.getenv("EVOLAND_DATA_BASEPATH", unset = "data-raw"))) {
  # WONTFIX this used to be a DOI based lookup, but since a DOI is roughly as
  # persistent as a Zenodo reference, we're just pointing to Zenodo directly.
  # We need to make assumptions about the data structure anyways, so let's
  # stick with this.

  tmpfile <- tempfile()
  curl::curl_download(
    url = url,
    destfile = tmpfile
  )

  zip::unzip(
    zipfile = tmpfile,
    exdir = target_dir
  )

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
