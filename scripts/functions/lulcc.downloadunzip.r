### =========================================================================
### lulcc.downloadunzip: Download folder from URL and unzip to target directory
### =========================================================================
#'
#'
#' @param url character string url to download folder from
#'   produced by function lulcc.evalfeatureselection
#' @param save_path character string path of directory to save unzipped files in
#'
#' @author Ben Black
#' @export

lulcc.downloadunzip <- function(url, save_dir){

#create dir
dir.create(save_dir, recursive = TRUE)

#create temporary directory
tmpdir <- tempdir()

#download folder from url
file <- basename(url)
download.file(url, paste0(tmpdir, "/", file), mode = "wb")
unzip(paste0(tmpdir, "/", file), exdir = save_dir)
unlink(tmpdir)
}