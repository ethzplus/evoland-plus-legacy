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

  #get file base name
  file <- basename(i)

  #if file is zipped then create temp dir and extract
  if(grepl(".zip", i)== TRUE){
    tmpdir <- tempdir()
    download.file(i, paste0(tmpdir, "/", file))
    unzip(paste0(tmpdir, "/", file), exdir = save_dir) #unzip folder, saving to new dir
    unlink(tmpdir) #remove temp dir
  }else{ #non-zipped just download
    download.file(i, paste0(save_dir, "/", file), mode = "wb")
    }

}