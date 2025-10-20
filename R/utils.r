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
#' @param out_path Character; output file path for aligned .tif
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
#' @return NULL; function writes aligned and compressed .tif files to out_path
#' @author Manuel Kurmann
align_to_ref_gdal <- function(
  ref_path,
  in_files, # ← vector of input .tif paths
  out_path,
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
      if (keep_vrts) dirname(out_path) else tempdir(),
      paste0(bn, "_aligned.vrt")
    )

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
      dstfile = out_path,
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


#' @describeIn util Align a raster to a reference raster using terra (optimized)
#' Align a raster to a reference raster using terra
#'
#' Ensures that a raster matches a reference raster's CRS, extent, and resolution.
#' Automatically reprojects and/or resamples if needed, and applies an NA mask
#' from the reference. Optimized for speed and memory efficiency.
#'
#' @param x SpatRaster or file path to input raster
#' @param ref SpatRaster or file path to reference raster
#' @param method Resampling method (default: "bilinear").
#'   Options: "near", "bilinear", "cubic", "cubicspline", "lanczos", "average", "mode", "max", "min", "med", "q1", "q3"
#' @param tolerance Numeric tolerance for comparing geometry (default: 1e-10)
#' @param return_original Logical; if TRUE and no correction needed, returns input unchanged (default: TRUE)
#' @param verbose Logical; print progress messages (default: TRUE)
#' @param datatype Output data type (optional, e.g., "FLT4S")
#' @param filename Optional output path; if supplied, writes only the masked raster to disk
#' @param tempdir Optional path to directory for temporary files (recommended for large rasters)
#' @param mem_safe Logical; if TRUE, forces disk-based processing for large rasters (default: TRUE)
#' @param wopt List of writing options for terra (datatype, gdal options, etc.).
#'   Default uses INT2S with DEFLATE compression. Set to NULL to disable.
#' @param ... Additional arguments passed to terra::project() or terra::resample()
#'
#' @return A SpatRaster aligned to `ref`. If `filename` is provided, returns a SpatRaster written to disk.
#' @author Ben Black
#'
#' @examples
#' # Set temporary directory to high-memory disk
#' # terraOptions(tempdir = "/path/to/large/disk/temp")
#' # result <- align_to_ref("input.tif", "reference.tif",
#' #                        filename = "aligned_masked.tif",
#' #                        tempdir = "/path/to/large/disk/temp")
#' #
#' # Custom compression settings:
#' # result <- align_to_ref("input.tif", "reference.tif",
#' #                        filename = "aligned.tif",
#' #                        wopt = list(datatype = "FLT4S",
#' #                                    gdal = c("COMPRESS=DEFLATE", "PREDICTOR=2")))
#'
#' @describeIn util Align a raster to a reference raster using terra (optimized)
#' Align a raster to a reference raster using terra
#'
#' Ensures that a raster matches a reference raster's CRS, extent, and resolution.
#' Automatically reprojects and/or resamples if needed, and applies an NA mask
#' from the reference. Optimized for speed and memory efficiency.
#'
#' @param x SpatRaster or file path to input raster
#' @param ref SpatRaster or file path to reference raster
#' @param method Resampling method (default: "bilinear").
#'   Options: "near", "bilinear", "cubic", "cubicspline", "lanczos", "average", "mode", "max", "min", "med", "q1", "q3"
#' @param tolerance Numeric tolerance for comparing geometry (default: 1e-10)
#' @param return_original Logical; if TRUE and no correction needed, returns input unchanged (default: TRUE)
#' @param verbose Logical; print progress messages (default: TRUE)
#' @param datatype Output data type (optional, e.g., "FLT4S")
#' @param filename Optional output path; if supplied, writes only the masked raster to disk
#' @param tempdir Optional path to directory for temporary files (recommended for large rasters)
#' @param mem_safe Logical; if TRUE, forces disk-based processing for large rasters (default: TRUE)
#' @param wopt List of writing options for terra (datatype, gdal options, etc.).
#'   Default uses INT2S with DEFLATE compression. Set to NULL to disable.
#' @param ... Additional arguments passed to terra::project() or terra::resample()
#'
#' @return A SpatRaster aligned to `ref`. If `filename` is provided, returns a SpatRaster written to disk.
#' @author Ben Black
#'
#' @examples
#' # Set temporary directory to high-memory disk
#' # terraOptions(tempdir = "/path/to/large/disk/temp")
#' # result <- align_to_ref("input.tif", "reference.tif",
#' #                        filename = "aligned_masked.tif",
#' #                        tempdir = "/path/to/large/disk/temp")
#' #
#' # Custom compression settings:
#' # result <- align_to_ref("input.tif", "reference.tif",
#' #                        filename = "aligned.tif",
#' #                        wopt = list(datatype = "FLT4S",
#' #                                    gdal = c("COMPRESS=DEFLATE", "PREDICTOR=2")))
#'
align_to_ref <- function(
  x,
  ref,
  method = "bilinear",
  tolerance = 1e-10,
  return_original = TRUE,
  verbose = TRUE,
  datatype = NULL,
  filename = NULL,
  tempdir = NULL,
  mem_safe = TRUE,
  wopt = list(
    datatype = "INT2S",
    NAflag = -32768,
    gdal = c(
      "COMPRESS=DEFLATE",
      "PREDICTOR=2",
      "ZLEVEL=6",
      "NUM_THREADS=ALL_CPUS"
    )
  ),
  ...
) {
  # --- Set temp directory if provided ---
  if (!is.null(tempdir)) {
    if (!dir.exists(tempdir)) {
      dir.create(tempdir, recursive = TRUE, showWarnings = FALSE)
    }
    terra::terraOptions(tempdir = tempdir)
    if (verbose) {
      message(sprintf("Temporary files will be written to: %s", tempdir))
    }
  }

  # --- Handle datatype and wopt ---
  # If datatype is explicitly provided, it overrides wopt$datatype
  if (!is.null(datatype) && !is.null(wopt)) {
    wopt$datatype <- datatype
  } else if (!is.null(datatype)) {
    wopt <- list(datatype = datatype)
  }

  # Extract datatype for intermediate operations
  final_datatype <- if (!is.null(wopt) && !is.null(wopt$datatype)) {
    wopt$datatype
  } else {
    datatype
  }

  # Capture extra args
  extra_args <- list(...)

  # --- Load rasters if paths provided ---
  if (is.character(x)) {
    x <- terra::rast(x)
  }
  if (is.character(ref)) {
    ref <- terra::rast(ref)
  }

  # --- Validate inputs ---
  if (!inherits(x, "SpatRaster") || !inherits(ref, "SpatRaster")) {
    stop("Both 'x' and 'ref' must be SpatRaster objects or file paths.")
  }

  # --- Extract raster properties ---
  crs_x <- terra::crs(x, proj = TRUE)
  crs_ref <- terra::crs(ref, proj = TRUE)
  res_x <- terra::res(x)
  res_ref <- terra::res(ref)
  ext_x <- terra::ext(x)
  ext_ref <- terra::ext(ref)

  # --- Compare geometry ---
  crs_match <- identical(crs_x, crs_ref)
  res_match <- all(abs(res_x - res_ref) < tolerance)
  ext_match <- all(
    abs(c(
      ext_x$xmin - ext_ref$xmin,
      ext_x$xmax - ext_ref$xmax,
      ext_x$ymin - ext_ref$ymin,
      ext_x$ymax - ext_ref$ymax
    )) <
      tolerance
  )

  needs_correction <- !crs_match || !res_match || !ext_match

  # --- Early return if no correction needed ---
  if (!needs_correction) {
    if (verbose) {
      message(
        "Raster already matches reference geometry — no correction needed."
      )
    }
    if (return_original) {
      # Still apply mask if needed
      if (!is.null(filename)) {
        mask_args <- list(
          x = x,
          mask = ref,
          filename = filename,
          overwrite = TRUE
        )
        if (!is.null(wopt)) {
          for (opt_name in names(wopt)) {
            mask_args[[opt_name]] <- wopt[[opt_name]]
          }
        }
        result <- do.call(terra::mask, mask_args)
        if (verbose) {
          message(sprintf("Masked raster written to: %s", filename))
        }
        return(result)
      }
      return(x)
    }
  }

  # --- Apply corrections ---
  corrections <- character()
  result <- x
  intermediate_files <- character()

  # CRS mismatch → reproject
  if (!crs_match) {
    if (verbose) {
      message("CRS mismatch detected — reprojecting...")
    }
    corrections <- c(corrections, "CRS")

    # Create args list
    args <- list(x = result, y = ref, method = method)
    if (!is.null(final_datatype)) {
      args$datatype <- final_datatype
    }

    # For large rasters or if mem_safe=TRUE, write to temp file
    if (mem_safe || terra::ncell(result) > 1e7) {
      temp_reproj <- tempfile(
        tmpdir = if (!is.null(tempdir)) tempdir else tempdir(),
        fileext = ".tif"
      )
      args$filename <- temp_reproj
      args$overwrite <- TRUE
      intermediate_files <- c(intermediate_files, temp_reproj)
    }

    if (length(extra_args) > 0) {
      for (arg_name in names(extra_args)) {
        args[[arg_name]] <- extra_args[[arg_name]]
      }
    }

    result <- do.call(terra::project, args)
  }

  # Extent/resolution mismatch → resample
  if (crs_match && (!res_match || !ext_match)) {
    if (verbose) {
      if (!res_match) {
        corrections <- c(corrections, "resolution")
      }
      if (!ext_match) {
        corrections <- c(corrections, "extent")
      }
      message("Resampling raster to match reference geometry...")
    }

    args <- list(x = result, y = ref, method = method)
    if (!is.null(final_datatype)) {
      args$datatype <- final_datatype
    }

    # For large rasters or if mem_safe=TRUE, write to temp file
    if (mem_safe || terra::ncell(result) > 1e7) {
      temp_resamp <- tempfile(
        tmpdir = if (!is.null(tempdir)) tempdir else tempdir(),
        fileext = ".tif"
      )
      args$filename <- temp_resamp
      args$overwrite <- TRUE
      intermediate_files <- c(intermediate_files, temp_resamp)
    }

    if (length(extra_args) > 0) {
      for (arg_name in names(extra_args)) {
        args[[arg_name]] <- extra_args[[arg_name]]
      }
    }

    result <- do.call(terra::resample, args)
  }

  if (verbose && length(corrections) > 0) {
    message(sprintf("Corrected: %s", paste(corrections, collapse = ", ")))
  }

  # --- Efficient masking using terra::mask ---
  if (verbose) {
    message("Applying mask from reference...")
  }

  # Use mask() instead of ifel() - much more efficient
  if (is.null(filename)) {
    result <- terra::mask(result, ref)
  } else {
    mask_args <- list(
      x = result,
      mask = ref,
      filename = filename,
      overwrite = TRUE
    )
    if (!is.null(wopt)) {
      for (opt_name in names(wopt)) {
        mask_args[[opt_name]] <- wopt[[opt_name]]
      }
    }
    result <- do.call(terra::mask, mask_args)
    if (verbose) message(sprintf("Masked raster written to: %s", filename))
  }

  # --- Clean up intermediate files ---
  if (length(intermediate_files) > 0) {
    for (f in intermediate_files) {
      if (file.exists(f)) {
        unlink(f)
        if (verbose) {
          message(sprintf("Cleaned up intermediate file: %s", basename(f)))
        }
      }
    }
  }

  if (verbose) {
    message("Processing complete.")
  }

  return(result)
}

#' @describeIn util Compute distance rasters from shapefiles using a reference raster (terra-optimized)
#' @title Create distance raster from shapefile using terra
#' @description
#' Efficiently compute a distance raster from a shapefile (vector layer)
#' using a reference raster as the template. Designed for extremely large rasters
#' (billions of cells) using disk-based temporary processing and high-compression outputs.
#'
#' @param shapefile Character path or SpatVector — input vector layer.
#' @param ref Character path or SpatRaster — reference raster defining CRS, extent, and resolution.
#' @param out_path Optional path for the output raster (.tif). If NULL, the raster remains in memory.
#' @param tempdir Optional directory for temporary files (recommended for large rasters).
#' @param datatype Output raster datatype (default: "FLT8S" — double precision).
#' @param compress Compression algorithm: "DEFLATE" (default) or "ZSTD".
#' @param deflate_level Compression level for DEFLATE (default: 9).
#' @param predictor Predictor for DEFLATE/ZSTD compression (default: 2).
#' @param blocksize Tile size in pixels (default: 512).
#' @param bigtiff Logical; write BigTIFF files for large rasters (default: TRUE).
#' @param mem_safe Logical; if TRUE, forces on-disk raster operations (default: TRUE).
#' @param verbose Logical; print progress messages (default: TRUE).
#' @param rasterize_field Optional field in the shapefile to rasterize.
#' @param rasterize_background Background value for rasterization (default: NA).
#' @param rasterize_all_touched Logical; rasterize all touched cells (default: FALSE).
#'
#' @return A `SpatRaster` containing Euclidean distances (in map units).
#' If `out_path` is supplied, the raster is written to disk.
#'
#' @examples
#' \dontrun{
#' distance_from_shapefile(
#'   shapefile = "hydro_units.shp",
#'   ref = "reference_raster.tif",
#'   out_path = "distances.tif",
#'   tempdir = "/scratch/terra_temp"
#' )
#' }
#'
#' @author Ben Black
#' @export
distance_from_shapefile <- function(
  shapefile,
  ref,
  out_path = NULL,
  tempdir = NULL,
  datatype = "FLT8S",
  compress = c("DEFLATE", "ZSTD"),
  deflate_level = 9,
  predictor = 2,
  blocksize = 256,
  bigtiff = TRUE,
  mem_safe = TRUE,
  verbose = TRUE,
  rasterize_field = 1,
  rasterize_background = NA,
  rasterize_all_touched = TRUE
) {
  # --- Validate inputs ---
  compress <- match.arg(compress)

  if (is.character(shapefile)) {
    if (!file.exists(shapefile)) {
      stop(sprintf("Shapefile not found: %s", shapefile))
    }
    shp <- terra::vect(shapefile)
  } else if (inherits(shapefile, "SpatVector")) {
    shp <- shapefile
  } else {
    stop("'shapefile' must be a file path or SpatVector.")
  }

  if (is.character(ref)) {
    if (!file.exists(ref)) {
      stop(sprintf("Reference raster not found: %s", ref))
    }
    ref_rast <- terra::rast(ref)
  } else if (inherits(ref, "SpatRaster")) {
    ref_rast <- ref
  } else {
    stop("'ref' must be a file path or SpatRaster.")
  }

  # --- Set temporary directory ---
  if (!is.null(tempdir)) {
    if (!dir.exists(tempdir)) {
      dir.create(tempdir, recursive = TRUE, showWarnings = FALSE)
    }
    terra::terraOptions(tempdir = tempdir)
    if (verbose) message(sprintf("Using temp directory: %s", tempdir))
  }

  # --- Prepare compression options ---
  gdal_opts <- if (toupper(compress) == "ZSTD") {
    c(
      "COMPRESS=ZSTD",
      "ZSTD_LEVEL=19",
      sprintf("PREDICTOR=%d", predictor),
      "TILED=YES",
      sprintf("BLOCKXSIZE=%d", blocksize),
      sprintf("BLOCKYSIZE=%d", blocksize),
      if (bigtiff) "BIGTIFF=YES" else NULL
    )
  } else {
    c(
      "COMPRESS=DEFLATE",
      sprintf("ZLEVEL=%d", deflate_level),
      sprintf("PREDICTOR=%d", predictor),
      "TILED=YES",
      sprintf("BLOCKXSIZE=%d", blocksize),
      sprintf("BLOCKYSIZE=%d", blocksize),
      if (bigtiff) "BIGTIFF=YES" else NULL
    )
  }

  wopt <- list(
    datatype = datatype,
    gdal = gdal_opts,
    overwrite = TRUE
  )

  # --- Reproject shapefile to match raster CRS ---
  if (!terra::same.crs(shp, ref_rast)) {
    if (verbose) {
      message("Reprojecting shapefile to match reference raster CRS...")
    }
    shp <- terra::project(shp, terra::crs(ref_rast))
  }

  # --- Rasterize shapefile to match reference geometry ---
  if (verbose) {
    message("Rasterizing shapefile...")
  }
  r_temp <- if (mem_safe) {
    tempfile(
      tmpdir = if (!is.null(tempdir)) tempdir else tempdir(),
      fileext = ".tif"
    )
  } else {
    ""
  }

  rasterized <- terra::rasterize(
    x = shp,
    y = ref_rast,
    field = rasterize_field,
    background = rasterize_background,
    touches = rasterize_all_touched
  )

  # --- Compute distance raster ---
  if (verbose) {
    message("Computing Euclidean distance raster...")
  }

  mask_rast <- terra::ifel(is.na(rasterized), 0, 1)

  distance_raster <- terra::distance(
    mask_rast,
    filename = if (!is.null(out_path)) {
      out_path
    } else {
      tempfile(fileext = ".tif", tmpdir = tempdir)
    },
    datatype = datatype
  )

  # --- Write result ---
  if (!is.null(out_path)) {
    if (verbose) {
      message(sprintf("Writing distance raster to: %s", out_path))
    }
    distance_raster <- terra::writeRaster(
      distance_raster,
      out_path,
      wopt = wopt
    )
  }

  # --- Clean up ---
  if (mem_safe && file.exists(r_temp)) {
    unlink(r_temp)
    if (verbose) message("Cleaned up temporary raster.")
  }

  if (verbose) {
    message("Distance computation complete.")
  }
  return(distance_raster)
}

#' @describeIn util Create metadata and citation files from a WFS endpoint
#' Create metadata + citation files from a WFS endpoint
#'
#' @param dataset_name Custom name you assign to this dataset (e.g. "Peru_Cuencas")
#' @param wfs_url URL of the WFS GetCapabilities document or service endpoint
#' @param download_url URL to static downloadable data (ZIP, GPKG, etc.)
#' @param output_dir Directory to save JSON and .bib files
#'
#' @return A list with paths to the created files
create_metadata_from_wfs <- function(
  dataset_name,
  wfs_url,
  download_url,
  output_dir = "."
) {
  message("Connecting to WFS service: ", wfs_url)

  # Connect to the WFS (explicitly calling from ows4R)
  wfs <- ows4R::WFSClient$new(
    url = wfs_url,
    serviceVersion = "2.0.0"
  )

  # List available feature types
  feature_types <- wfs$listFeatureTypes()

  # Extract metadata for each feature type
  meta_list <- lapply(feature_types, function(ft) {
    list(
      wfs_layer_name = ft$getName(),
      title = ft$getTitle(),
      abstract = ft$getAbstract(),
      default_crs = ft$getDefaultCRS(),
      bbox = ft$getBoundingBox(),
      output_formats = ft$getOutputFormats()
    )
  })

  # Combine dataset-level metadata
  metadata <- list(
    dataset_name = dataset_name,
    wfs_service_url = wfs_url,
    download_url = download_url,
    number_of_layers = length(meta_list),
    layers = meta_list,
    retrieved_on = Sys.time()
  )

  # Create output directory if missing
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  # --- Write JSON metadata file (explicit jsonlite call) ---
  json_path <- file.path(output_dir, paste0(dataset_name, "_metadata.json"))
  jsonlite::write_json(
    metadata,
    json_path,
    pretty = TRUE,
    auto_unbox = TRUE
  )

  # --- Create BibTeX citation (explicit bibtex call) ---
  bib_entry <- bibtex::bibentry(
    bibtype = "Misc",
    title = dataset_name,
    author = "Autoridad Nacional del Agua (ANA)",
    year = format(Sys.Date(), "%Y"),
    url = download_url,
    note = paste0("WFS metadata source: ", wfs_url)
  )

  bib_path <- file.path(output_dir, paste0(dataset_name, ".bib"))
  bibtex::write.bib(bib_entry, file = bib_path)

  # --- Output messages ---
  message("✅ Created metadata and citation files:")
  message(" - JSON: ", json_path)
  message(" - BibTeX: ", bib_path)

  invisible(list(json = json_path, bib = bib_path))
}


#' @describeIn util Update predictor YAML file with new entry
#' Add or update a predictor entry in a YAML file
#'
#' All fields are explicitly supplied as arguments; missing ones are set to NULL.
#'
#' @param yaml_file Path to the YAML file to update.
#' @param pred_name Name to use as the YAML key (required).
#' @param clean_name Clean name of the predictor (default NULL)
#' @param pred_category Predictor category (default NULL)
#' @param static_or_dynamic "Static" or "Dynamic" (default NULL)
#' @param metadata Metadata information (default NULL)
#' @param scenario_variant Scenario variant (default NULL)
#' @param period Period coverage (default NULL)
#' @param path File path or template path (default NULL)
#' @param grouping Grouping category (default NULL)
#' @param description Description of the predictor (default NULL)
#' @param date Date string (default NULL)
#' @param author Author name (default NULL)
#' @param wfs_url URL for WFS access (default NULL)
#' @param download_url URL for download (default NULL)
#' @param raw_data_dir Path to raw data directory (default NULL)
update_predictor_yaml <- function(
  yaml_file,
  pred_name,
  clean_name = NULL,
  pred_category = NULL,
  static_or_dynamic = NULL,
  metadata = NULL,
  scenario_variant = NULL,
  period = NULL,
  path = NULL,
  grouping = NULL,
  description = NULL,
  date = NULL,
  author = NULL,
  wfs_url = NULL,
  download_url = NULL,
  raw_data_dir = NULL,
  raw_filename = NULL
) {
  # Load existing YAML if it exists
  if (file.exists(yaml_file)) {
    yaml_content <- yaml::yaml.load_file(yaml_file)
    if (!is.list(yaml_content)) yaml_content <- list()
  } else {
    yaml_content <- list()
  }

  # Build entry
  entry <- list(
    clean_name = clean_name,
    pred_category = pred_category,
    static_or_dynamic = static_or_dynamic,
    metadata = metadata,
    scenario_variant = scenario_variant,
    period = period,
    path = path,
    grouping = grouping,
    description = description,
    date = date,
    author = author,
    wfs_url = wfs_url,
    download_url = download_url,
    raw_data_dir = raw_data_dir,
    raw_filename = raw_filename
  )

  # Add or update entry under pred_name key
  yaml_content[[pred_name]] <- entry

  # Write YAML back to file
  writeLines(yaml::as.yaml(yaml_content), con = yaml_file)

  message("YAML updated successfully: ", yaml_file)
}
