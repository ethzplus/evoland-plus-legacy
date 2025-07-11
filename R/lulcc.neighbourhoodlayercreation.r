#' lulcc.neighbourhoodlayercreation
#'
#' TODO text
#'
#' @author Ben Black (lulcc function), Majid Shadman Roodposhti (matrix functions)
# A - Instantiate small functions for neighbourhood creation ####

# function to set up a random pythagorean matrix generating the central values
# of each matrix x0,0 and their corresponding decay rates
# decay rates are positive, pseudorandom number from the uniform distribution
# in the range 0>??>1. For a randomised search, ??  may be a pseudorandom number
# from the uniform distribution, while for a grid search ??  may be any number
# from a user-defined set
# function to plot all generated random pythagorean matrices for checking,
# with values of each cell labelled in the center
#' @export
random_pythagorean_matrix <- function(n, x, interpolation = "smooth", search = "random") {
  # current usage in the codebase, use this as simplifying assumption
  stopifnot(rlang::is_scalar_integerish(x))

  choices <- c("random", "grid")
  search <- choices[pmatch(search, choices, duplicates.ok = FALSE)]
  if (search == "random") {
    seed <- runif(n, 5, 50)
    drops <- mapply(runif, 1, 1, seed)
  } else {
    seed <- rep(seq.int(5, n, 5), each = 5)
    drops <- rep(c(3, 6, 12, 24, 48), times = n / 5)
  }
  Map(get_pythagorean_matrix, x, seed, drops, interpolation)
}

# function that shapes the actual random pythagorean matrix using
# smooth or linear interpolation
get_pythagorean_matrix <- function(x, mid, drop, interpolation = "smooth") {
  if (x %% 2 == 0 | x < 0) stop("x must be an odd positive number")

  choices <- c("smooth", "linear")
  interpolation <- choices[pmatch(interpolation, choices, duplicates.ok = FALSE)]

  dists <- outer(
    abs(1:x - ceiling(x / 2)), abs(1:x - ceiling(x / 2)),
    function(x, y) sqrt(x^2 + y^2)
  )
  if (interpolation == "smooth") {
    mat <- (1 / drop)^dists * mid
  } else {
    mat <- matrix(approx(x = 0:x, y = 0.1^(0:x) * mid, xout = dists)$y, ncol = x, nrow = x)
  }
  return(mat)
}

#' @export
plot_pythagorean_matrix <- function(mat) {
  colors <- colorRampPalette(c(
    "deepskyblue4", "deepskyblue3", "darkslateblue", "deepskyblue1", "lightblue1", "gray88"
  ))(256)
  corrplot::corrplot(mat,
    is.corr = FALSE, method = "shade",
    col = colors, tl.pos = "n",
    number.cex = .7, addCoef.col = "black"
  )
  text(row(mat), col(mat), round(mat, 2), cex = 0.7)
}

# function to plot the outcomes of applying different decay rates on every generated
# central cell
#' @export
plot_pythagorean_matrix_decay <- function(mat, plot = TRUE, ...) {
  mid <- ceiling(ncol(mat) / 2)
  drop <- mat[mid, mid + 1] / mat[mid, mid]
  xs <- seq(0, mid, 0.1)
  if (isTRUE(all.equal(mat[mid + 1, mid + 1], drop^sqrt(2) * mat[mid, mid]))) {
    ys <- drop^(xs) * mat[mid, mid]
    if (plot) {
      plot(xs, ys,
        type = "l", las = 1, lwd = 2, xlab = "distance", ylab = "value",
        cex.axis = 0.7, mgp = c(2, 0.5, 0), tck = -0.01, ...
      )
    }
  } else {
    ys <- approx(0:mid, drop^(0:mid), xout = xs)$y * mat[mid, mid]
    if (plot) {
      plot(xs, ys,
        type = "l", las = 1, lwd = 2, xlab = "distance", ylab = "value",
        cex.axis = 0.7, mgp = c(2, 0.5, 0), tck = -0.01, ...
      )
    }
  }
  return(data.frame(x = xs, y = ys))
}


#' Generate Neighborhood Rasters for Land Use/Land Cover Classes
#'
#' This function creates neighborhood rasters for specified land use/land cover (LULC)
#' classes using given neighborhood matrices. For each active LULC class and
#' neighborhood matrix combination, it generates and saves a focal raster.
#'
#' @param lulc_raster A RasterLayer object containing land use/land cover data
#' @param neighbourhood_matrices List of matrices defining different neighborhood configurations
#' @param active_lulc_class_names Character vector of active LULC class names to process
#' @param data_period String indicating the time period of the data
#' @param nhood_folder_path String path to folder where neighborhood rasters will be saved
#'
#' @return List of lists containing generated focal rasters for each LULC class and
#' neighborhood combination
#'
#' @details
#' The function performs the following steps:
#' 1. Extracts pixel values for active LULC classes
#' 2. Creates binary rasters for each active LULC class
#' 3. Applies focal operations using provided neighborhood matrices
#' 4. Saves resulting rasters as .grd files
#'
#' @note Output rasters are saved with INT2U datatype
#' @importMethodsFrom raster ==
#' @export

lulcc_generatenhoodrasters <- function(
    lulc_raster,
    neighbourhood_matrices,
    active_lulc_class_names,
    data_period,
    nhood_folder_path) {
  # get pixel values of all active LULC classes
  active_class_values <- sapply(
    active_lulc_class_names,
    function(x) {
      lulc_raster@data@attributes[[1]][
        lulc_raster@data@attributes[[1]]$lulc_name == x, "Pixel_value"
      ]
    }
  )

  # subset LULC raster by all Active_class_values
  active_class_raster_subsets <- lapply(
    active_class_values,
    function(subset_value) {
      lulc_raster == subset_value
    }
  )

  # outer loop over the active class LULC rasters
  all_focal_rasters <- mapply(
    function(active_class_raster, active_class_raster_name) {
      # Inner loop: running the focal function using each matrix in the list
      matrices_for_lulc_class <- mapply(
        function(single_matrix, matrix_name) {
          focal_layer <- raster::focal(
            x = active_class_raster,
            w = single_matrix,
            na.rm = FALSE,
            pad = TRUE,
            padValue = 0,
            NAonly = FALSE
          ) # create focal layer using matrix
          focal_file_name <- paste(
            active_class_raster_name, "nhood", matrix_name, data_period,
            sep = "_"
          ) # create file path for saving this layer

          focal_full_path <- file.path(
            nhood_folder_path,
            paste0(focal_file_name, ".grd")
          ) # create full folder path
          raster::writeRaster(
            focal_layer,
            focal_full_path,
            datatype = "INT2U",
            overwrite = TRUE
          ) # save layer
          names(focal_layer) <- focal_file_name # rename focal layer
          return(focal_layer) # return layer for inspection
        },
        single_matrix = neighbourhood_matrices,
        matrix_name = names(neighbourhood_matrices),
        SIMPLIFY = FALSE
      ) # close inner loop
      return(matrices_for_lulc_class)
    }, # close outer loop
    active_class_raster = active_class_raster_subsets,
    active_class_raster_name = names(active_class_raster_subsets), SIMPLIFY = FALSE
  )

  return(all_focal_rasters)
}
