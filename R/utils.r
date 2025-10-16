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

#' makes datatable-aware, so NSE works
.datatable.aware <- TRUE

#' @describeIn util Write a raster with memory and compression options
write_raster <- function(r, filename, ...) {
  terra::writeRaster(
    r,
    filename = filename,
    overwrite = TRUE,
    wopt = list(
      datatype = "INT2S",
      NAflag = -32768,
      gdal = c("COMPRESS=LZW", "ZLEVEL=9")
    ),
    ...
  )
}


#' @describeIn util Align a raster to a reference raster
#' Handles differences in CRS, extent, and resolution
#' Uses gdalwarp and gdal_translate for efficiency
#' @param in_files Character vector of input .tif file paths to be aligned and compressed
#' @param ref_path Character; path to reference .tif file
#' @param out_dir Character; output directory for aligned/compressed .tif files
#' @param resample Character; resampling method for gdalwarp ("near", "bilinear", "cubic", etc.)
#' @param dst_nodata Numeric or NA; nodata value to set in output; NA to auto-detect per source
#' @param keep_vrts Logical; if TRUE, keeps intermediate VRT files (default: FALSE)
#' @param compressor Character; compression method for GeoTIFF ("DEFLATE" or "ZSTD")
#' @param deflate_level Integer; DEFLATE compression level (1-9, default: 9)
#' @param zstd_level Integer; ZSTD compression level (1-22, default: 19)
#' @param blocksize Integer; tile block size for GeoTIFF (default: 512)
#' @param use_sparse Logical; if TRUE, enables SPARSE_OK for GeoTIFF (default: TRUE)
#' @param big_tiff Character; "YES" to allow BigTIFF output, "NO" otherwise (default: "YES")
#' @param warp_mem_mb Integer; memory limit in MB for gdalwarp (default: 2048)
#' @param gdal_cachemax Integer; GDAL_CACHEMAX setting in MB (default: 2048)
#' @return NULL; function writes aligned and compressed .tif files to out_dir
#' @author Manuel Kurmann
align_and_compress_tifs <- function(
  ref_path,
  in_files, # ← vector of input .tif paths
  out_dir,
  resample = "bilinear", # "near" for categorical, "bilinear"/"cubic" for continuous
  dst_nodata = NA, # numeric for explicit nodata; NA to auto-detect per source
  keep_vrts = FALSE, # keep intermediate VRTs?
  compressor = "DEFLATE", # "DEFLATE" or "ZSTD"
  deflate_level = 9,
  zstd_level = 19,
  blocksize = 512,
  use_sparse = TRUE,
  big_tiff = "YES", # "YES" or "NO"
  warp_mem_mb = 2048,
  gdal_cachemax = 2048 # in MB
) {
  # ---- Performance-oriented GDAL settings ----
  Sys.setenv(GDAL_CACHEMAX = as.character(gdal_cachemax))

  # ---- Read reference grid ----
  ref <- terra::rast(ref_path)
  ext <- terra::ext(ref)
  res_xy <- terra::res(ref)
  crs_desc <- try(terra::crs(ref, describe = TRUE), silent = TRUE)

  if (
    !inherits(crs_desc, "try-error") &&
      !is.null(crs_desc$code) &&
      !is.na(crs_desc$code)
  ) {
    target_srs <- paste0("EPSG:", crs_desc$code)
  } else {
    target_srs <- terra::crs(ref, proj = TRUE)
  }

  fmt_num <- function(x) sprintf("%.15f", x)
  xmin <- fmt_num(ext$xmin)
  ymin <- fmt_num(ext$ymin)
  xmax <- fmt_num(ext$xmax)
  ymax <- fmt_num(ext$ymax)
  xres <- fmt_num(res_xy[1])
  yres <- fmt_num(res_xy[2])

  if (!dir.exists(out_dir)) {
    dir.create(out_dir, recursive = TRUE)
  }

  # ---- Filter input TIFFs ----
  tifs <- normalizePath(in_files, mustWork = FALSE)
  tifs <- tifs[file.exists(tifs)]
  tifs <- setdiff(tifs, normalizePath(ref_path, mustWork = FALSE))
  if (length(tifs) == 0L) {
    stop("No valid input .tif files found in 'in_files'.")
  }

  # ---- Common warp args ----
  warp_args_common <- c(
    "-t_srs",
    target_srs,
    "-te_srs",
    target_srs,
    "-te",
    xmin,
    ymin,
    xmax,
    ymax,
    "-tr",
    xres,
    yres,
    "-r",
    resample,
    "-multi",
    "-wo",
    "NUM_THREADS=ALL_CPUS",
    "-wm",
    as.character(warp_mem_mb),
    "-overwrite"
  )

  # ---- Helper functions ----
  make_dstnodata <- function(src) {
    if (is.na(dst_nodata)) {
      info <- try(gdalUtilities::gdalinfo(src), silent = TRUE)
      if (!inherits(info, "try-error")) {
        line <- grep("NoData Value=", info, value = TRUE)
        if (length(line)) {
          val <- sub(".*NoData Value=([^\\r\\n]+).*", "\\1", line[1])
          return(c("-dstnodata", val))
        }
      }
      return(character(0))
    } else {
      return(c("-dstnodata", as.character(dst_nodata)))
    }
  }

  detect_predictor <- function(src) {
    info <- try(gdalUtilities::gdalinfo(src), silent = TRUE)
    if (inherits(info, "try-error")) {
      return("2")
    }
    type_lines <- grep("Type=", info, value = TRUE)
    if (any(grepl("Float(32|64)", type_lines))) "3" else "2"
  }

  make_gtiff_co <- function(predictor) {
    base <- c(
      "-co",
      "TILED=YES",
      "-co",
      paste0("BLOCKXSIZE=", blocksize),
      "-co",
      paste0("BLOCKYSIZE=", blocksize),
      "-co",
      paste0("PREDICTOR=", predictor),
      "-co",
      paste0("BIGTIFF=", big_tiff),
      "-co",
      "INTERLEAVE=BAND"
    )
    comp <- if (toupper(compressor) == "ZSTD") {
      c("-co", "COMPRESS=ZSTD", "-co", paste0("ZSTD_LEVEL=", zstd_level))
    } else {
      c("-co", "COMPRESS=DEFLATE", "-co", paste0("ZLEVEL=", deflate_level))
    }
    sparse <- if (use_sparse) c("-co", "SPARSE_OK=TRUE") else character(0)
    c(base, comp, sparse)
  }

  # ---- Process files ----
  for (i in seq_along(tifs)) {
    src <- tifs[i]
    bn <- tools::file_path_sans_ext(basename(src))
    vrt_out <- file.path(
      if (keep_vrts) out_dir else tempdir(),
      paste0(bn, "_aligned.vrt")
    )
    tif_out <- file.path(out_dir, paste0(bn, "_aligned.tif"))
    nd_args <- make_dstnodata(src)
    pred <- detect_predictor(src)
    co <- make_gtiff_co(pred)

    message(sprintf("[%d/%d] Processing: %s", i, length(tifs), basename(src)))

    # Step 1: Warp to reference grid (VRT)
    gdalUtilities::gdalwarp(
      srcfile = src,
      dstfile = vrt_out,
      of = "VRT",
      options = c(warp_args_common, nd_args)
    )

    # Step 2: Compress to GeoTIFF
    gdalUtilities::gdal_translate(
      srcfile = vrt_out,
      dstfile = tif_out,
      of = "GTiff",
      options = co
    )

    if (!keep_vrts && file.exists(vrt_out)) unlink(vrt_out)
  }

  message(
    "✅ Alignment and compression complete for ",
    length(tifs),
    " file(s)."
  )
}
