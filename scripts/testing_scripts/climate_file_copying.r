Switch_dir <- "E:/LULCC_CH_dat/Data/Intermediate/Present/ch/bioclim/chclim25/present/pixel/1981_2010"
dir.create(Switch_dir, recursive = TRUE, showWarnings = FALSE)

clim_layers <- list.files("X:/CH_ValPar.CH/03_workspaces/Climate_present/pixel/1981_2010",
  recursive = TRUE, full.names = TRUE,
  pattern = ".tif"
)

# vector dir names
clim_dirs <- list.dirs("X:/CH_ValPar.CH/03_workspaces/Climate_present/pixel/1981_2010",
  recursive = TRUE, full.names = FALSE
)
clim_dirs <- clim_dirs[2:length(clim_dirs)]

# create all clim dirs
sapply(clim_dirs, function(x) {
  dir.create(paste0(Switch_dir, "/", x), recursive = TRUE, showWarnings = FALSE)
})

# move clim_layers into correct dirs
sapply(clim_layers, function(x) {
  # extract the dir name
  Dir_folder <- sapply(clim_dirs, function(y) {
    stringr::str_extract(basename(x), stringr::fixed(y))
  })

  # remove NAs
  Bio_Dir <- Dir_folder[is.na(Dir_folder) == FALSE]

  # subset to last entry for cases of bio1, bio11
  Bio_Dir <- Bio_Dir[length(Bio_Dir)]

  # combine with save_dir
  save_dir <- paste0(Switch_dir, "/", Bio_Dir)

  # remove the '_100m.tif' from the file name
  new_file_name <- stringr::str_replace(basename(x), "_100m.tif", "")

  # load the file
  r <- raster::raster(x)

  # Reformat
  r <- round(r * 100)
  storage.mode(r[]) <- "integer"

  # name the layer
  names(r) <- new_file_name

  # save as RDS
  saveRDS(r, paste0(save_dir, "/", new_file_name, ".rds"))
})
