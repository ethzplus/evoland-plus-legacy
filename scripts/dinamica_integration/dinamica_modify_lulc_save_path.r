#############################################################################
## Dinamica_modify_lulc_save_path: Finalize save path for simulated LULC map
## according to time step
## Date: 10-10-2022
## Author: Ben Black
#############################################################################

#receive basic path from Dinamica (produced by script Dinamica_initialize)
LULC_base_path <- s1

#receive time step
Time_step <- v1

#add step length to current time step to get simulation year
Simulated_lulc_year <-  Time_step + v2

#paste together and include '.tif' as the file type
Final_lULC_path <- paste0(LULC_base_path, Simulated_lulc_year, ".tif")

#Output the path
outputString("simulated_lulc_save_path", Final_lULC_path)