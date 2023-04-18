#############################################################################
## lulcc.spatprobmanipulation: function to manipulate spatial transition
##probabilities to prepresent interventions/policies/management activities
## Date: 17-03-2023
## Author: Ben Black
#############################################################################
#'
#' @param Scenario_ID Chr, abbreviation of current scenario
#' @param Raster_prob_values Dataframe, predicted cellular transitions
#'                            probabilities with x,y and cell ID cols
#' @param Simulation_time_step Chr, current simultion time step
#'
#' @author Ben Black
#' @export
#'

### =========================================================================
### Function to perform perturbations (raster version)
### =========================================================================

lulcc.spatprobmanipulation <- function(Scenario_ID, Raster_prob_values, Simulation_time_step){

  #vector names of columns of probability predictions (matching on Prob_)
  Pred_prob_columns <- grep("Prob_", names(Raster_prob_values), value = TRUE)

  #convert probability table to raster stack
  Prob_raster_stack <- stack(lapply(Pred_prob_columns, function(x) rasterFromXYZ(Raster_prob_values[,c("x", "y", x)])))
  names(Prob_raster_stack@layers) <- Pred_prob_columns

  #load table of scenario interventions
  Interventions <- openxlsx::read.xlsx(Scenario_specs_path, sheet = "Interventions")

  #convert Time_step and Target_classes columns back to character vectors
  Interventions$Time_step <- sapply(Interventions$Time_step, function(x) {
    rep <- unlist(strsplit(x, ","))
    },simplify=FALSE)

  Interventions$Target_classes <- sapply(Interventions$Target_classes, function(x) {
    x <- str_remove_all(x, " ")
    rep <- unlist(strsplit(x, ","))
    },simplify=FALSE)

  #subset interventions to scenario
  Scenario_interventions <- Interventions[Interventions$Scenario_ID == Scenario_ID,]

  #subset to interventions for current time point
  Time_step_rows <- sapply(Scenario_interventions$Time_step, function(x) any(grepl(Simulation_time_step, x)))
  Current_interventions <- Scenario_interventions[Time_step_rows, ]

  #loop over rows
  if(nrow(Current_interventions) !=0){
  for(i in nrow(Current_interventions)){

    #vector intervention details for easy reference
    Intervention_ID <- Current_interventions[i, "Intervention_ID"]
    Target_classes <- paste0("Prob_", Current_interventions[[i, "Target_classes"]])
    Intervention_data <- Current_interventions[i, "Intervention_data"]

    #if Perc_diff is too small then the effect will likely not be achieved
    #to mitigate use a threshold of minimum probability perturbation
    Prob_perturb_thresh <-Current_interventions[i, "Prob_perturb_threshold"]

    #--------------------------------------------------------------------------
    # Urban_densification intervention
    #--------------------------------------------------------------------------
    if(Intervention_ID == "Urban_densification"){

    #load building zone raster
    Intervention_rast <- raster(Intervention_data)

    #identify pixels inside of building zones
    Intersecting <- mask(Prob_raster_stack@layers[[Target_classes]], Intervention_rast == 1)

    #increase probability to one
    Intersecting[Intersecting > 0] <- 1

    #index which cells need to have value updated
    ix <- Intersecting == 1

    #replace values in target raster
    Prob_raster_stack@layers[[Target_classes]][ix] <- Intersecting[ix]

    }#close Urban_densification chunk


    #--------------------------------------------------------------------------
    #Urban_sprawl intervention
    #--------------------------------------------------------------------------
    if(Intervention_ID == "Urban_sprawl"){

    #load building zone raster
    Intervention_rast <- raster(Intervention_data)

    #identify pixels inside of building zones
    Intersecting <- mask(Prob_raster_stack@layers[[Target_classes]], Intervention_rast == 1)

    #identify pixels outside of building zones
    non_intersecting <- overlay(Prob_raster_stack@layers[[Target_classes]],Intervention_rast,fun = function(x, y) {
      x[y==1] <- NA
      return(x)
    })

    #calculate 90th percentile values of probability for pixels inside vs.outside
    #excluding those with a value of 0
    Intersect_percentile <- quantile(Intersecting@data@values[Intersecting@data@values >0], probs = 0.90, na.rm=TRUE)
    Nonintersect_percentile <- quantile(non_intersecting@data@values[non_intersecting@data@values >0], probs = 0.90, na.rm=TRUE)

    #get the means of the values above the 90th percentile
    Intersect_percentile_mean <- mean(Intersecting@data@values[Intersecting@data@values >= Intersect_percentile], na.rm = TRUE)
    Nonintersect_percentile_mean <- mean(non_intersecting@data@values[non_intersecting@data@values >= Nonintersect_percentile], na.rm = TRUE)

    #mean difference
    Mean_diff <- Intersect_percentile_mean - Nonintersect_percentile_mean

    #Average of means
    Average_mean <- mean(Intersect_percentile_mean, Nonintersect_percentile_mean)

    #calculate percentage difference
    Perc_diff <- (Mean_diff/Average_mean)*100

    #The intended effect of the intervention is to increase the probability of
    #urban development outside the building zone, however depending on the
    #valency of the Perc_diff values this needs to be implemented differently

    #If Perc_diff is >0 then increase the probability of instances above the
    #90th percentile for the outside pixels by the percentage difference
    #between the means
    if(Perc_diff >0){

      #check threshold
      if(abs(Perc_diff) < Prob_perturb_thresh){Perc_diff <- Prob_perturb_thresh}

      non_intersecting[non_intersecting > Nonintersect_percentile] <- non_intersecting[non_intersecting > Nonintersect_percentile] + (non_intersecting[non_intersecting > Nonintersect_percentile]/100)*Perc_diff

      #replace any values greater than 1 with 1
      non_intersecting[non_intersecting > 1] <- 1

      #index which cells need to have value updated
      ix <- non_intersecting > Nonintersect_percentile

      #replace values in target raster
      Prob_raster_stack@layers[[Target_classes]][ix] <- non_intersecting[ix]
      positive_test <- Prob_raster_stack@layers[[Target_classes]]

      #else if Perc_diff is <0 then decrease the probability of instances above the
      #90th percentile for the inside pixels by the percentage difference
      #between the means
      }else if(Perc_diff <0){

      #check threshold
      if(abs(Perc_diff) < Prob_perturb_thresh){Perc_diff <- -(Prob_perturb_thresh)}

      Intersecting[Intersecting > Intersect_percentile] <- Intersecting[Intersecting > Intersect_percentile] + (Intersecting[Intersecting > Intersect_percentile]/100)*Perc_diff

      #replace any values greater than 1 with 1
      Intersecting[Intersecting < 0] <- 0

      #index which cells need to have value updated
      ix <- Intersecting > Intersect_percentile

      #replace values in target raster
      Prob_raster_stack@layers[[Target_classes]][ix] <- Intersecting[ix]
      negative_test <- Prob_raster_stack@layers[[Target_classes]]
      } #close else if statement

    }#close Urban_sprawl chunk

    #--------------------------------------------------------------------------
    # Urban_migration intervention
    #--------------------------------------------------------------------------
    if(Intervention_ID == "Urban_migration"){

    #load municipality typology raster
    Intervention_rast <- raster(Intervention_data)

    #seperate raster legend and recode values for remote rural municaplities
    #for this intervention: 325, 326, 327, 335, 338
    Leg <- Intervention_rast@data@attributes[[1]]
    Leg[Leg$ID %in% c(325, 326, 327, 335, 338), "type"] <- 1
    Leg[Leg$type != 1, "type"] <- NA
    Leg$type <- as.numeric(Leg$type)

    #reclassify raster
    Intervention_rast <- reclassify(Intervention_rast, rcl = Leg)

    #identify pixels inside of remote rural municipalities
    Intersecting <- mask(Prob_raster_stack@layers[[Target_classes]], Intervention_rast == 1)

    #identify pixels outside of remote rural municipalities
    non_intersecting <- overlay(Prob_raster_stack@layers[[Target_classes]],Intervention_rast,fun = function(x, y) {
      x[y==1] <- NA
      return(x)
    })

    #Because the intended effect of the intervention is to decrease the
    #probability of urban development in the remote rural municipalities
    #calculate 90th percentile value of probability for pixels inside
    #and the 80th percentile value for pixels outside
    #excluding cells with a value of 0 in both cases
    Intersect_percentile <- quantile(Intersecting@data@values[Intersecting@data@values >0], probs = 0.90, na.rm=TRUE)
    Nonintersect_percentile <- quantile(non_intersecting@data@values[non_intersecting@data@values >0], probs = 0.80, na.rm=TRUE)

    #get the means of the values above the percentiles
    Intersect_percentile_mean <- mean(Intersecting@data@values[Intersecting@data@values >= Intersect_percentile], na.rm = TRUE)
    Nonintersect_percentile_mean <- mean(non_intersecting@data@values[non_intersecting@data@values >= Nonintersect_percentile], na.rm = TRUE)

    #mean difference
    Mean_diff <- Nonintersect_percentile_mean - Intersect_percentile_mean

    #Average of means
    Average_mean <- mean(Intersect_percentile_mean, Nonintersect_percentile_mean)

    #percentage difference
    Perc_diff <- (Mean_diff/Average_mean)*100

    #If Perc_diff is < 0 then decrease the probability of instances above the
    #90th percentile for the pixels in remote rural municipalities by the percentage difference
    #between the means
    if(Perc_diff <0){

      #check threshold
      if(abs(Perc_diff) < Prob_perturb_thresh){Perc_diff <- -(Prob_perturb_thresh)}

      Intersecting[Intersecting > Intersect_percentile] <-Intersecting[Intersecting > Intersect_percentile] + (Intersecting[Intersecting > Intersect_percentile]/100)*Perc_diff

      #replace any values greater than 1 with 1
      Intersecting[Intersecting > 1] <- 1

      #index which cells need to have value updated
      ix <- Intersecting > Intersect_percentile

      #replace values in target raster
      Prob_raster_stack@layers[[Target_classes]][ix] <- Intersecting[ix]

      #else if Perc_diff is >0 then increase the probability of instances above the
      #90th percentile for the outside pixels by the percentage difference
      #between the means
      }else if(Perc_diff >0){

      #check threshold
      if(abs(Perc_diff) < Prob_perturb_thresh){Perc_diff <- Prob_perturb_thresh}

      non_intersecting[non_intersecting > Nonintersect_percentile] <- non_intersecting[non_intersecting > Nonintersect_percentile] + (non_intersecting[non_intersecting > Nonintersect_percentile]/100)*Perc_diff

      #replace any values greater than 1 with 1
      non_intersecting[non_intersecting > 1] <- 1

      #index which cells need to have value updated
      ix <- non_intersecting > Nonintersect_percentile

      #replace values in target raster
      Prob_raster_stack@layers[[Target_classes]][ix] <- non_intersecting[ix]
      } #close else if statement

    }#close Urban_migration chunk

    #--------------------------------------------------------------------------
    #Mountain_development intervention
    #--------------------------------------------------------------------------
    if(Intervention_ID == "Mountain_development"){

    #load municipality typology raster
    Intervention_rast <- raster(Intervention_data)

    #seperate raster legend and recode values for remote rural municaplities
    Leg <- Intervention_rast@data@attributes[[1]]

    #For this intervention there are two different specs for scenarios
    #EI_NAT: 314
    #EI_SOC: 314,334
    if(Scenario_ID == "EI_NAT"){
    Leg[Leg$ID == 314, "type"] <- 1
    } else if(Scenario_ID == "EI_SOC"){
    Leg[Leg$ID %in% c(314, 334), "type"] <- 1
    }

    Leg[Leg$type != 1, "type"] <- NA
    Leg$type <- as.numeric(Leg$type)

    #reclassify raster
    Intervention_rast <- reclassify(Intervention_rast, rcl = Leg)

    #identify pixels inside of mountainous remote municipalities
    Intersecting <- mask(Prob_raster_stack@layers[[Target_classes]], Intervention_rast == 1)

    #identify pixels outside of mountainous remote municipalities
    non_intersecting <- overlay(Prob_raster_stack@layers[[Target_classes]],Intervention_rast,fun = function(x, y) {
      x[y==1] <- NA
      return(x)
    })

    #Because the intended effect of the intervention is to increase the
    #probability of urban development in the mountainous municipalities

    #calculate 90th percentile value of probability for pixels inside and outside
    #excluding cells with a value of 0 in both cases
    Intersect_percentile <- quantile(Intersecting@data@values[Intersecting@data@values >0], probs = 0.90, na.rm=TRUE)
    Nonintersect_percentile <- quantile(non_intersecting@data@values[non_intersecting@data@values >0], probs = 0.90, na.rm=TRUE)

    #get the means of the values above the percentiles
    Intersect_percentile_mean <- mean(Intersecting@data@values[Intersecting@data@values >= Intersect_percentile], na.rm = TRUE)
    Nonintersect_percentile_mean <- mean(non_intersecting@data@values[non_intersecting@data@values >= Nonintersect_percentile], na.rm = TRUE)

    #mean difference
    Mean_diff <- Nonintersect_percentile_mean - Intersect_percentile_mean

    #Average of means
    Average_mean <- mean(Intersect_percentile_mean, Nonintersect_percentile_mean)

    #percentage difference
    Perc_diff <- (Mean_diff/Average_mean)*100

    #If Perc_diff is > 0 then increase the probability of instances above the
    #90th percentile for the pixels in mountainous municipalities by the
    #percentage difference between the means (or the threshold value)
    if(Perc_diff >0){

      #check threshold
      if(abs(Perc_diff) < Prob_perturb_thresh){Perc_diff <- Prob_perturb_thresh}

      #increase the values
      Intersecting[Intersecting > Intersect_percentile] <-Intersecting[Intersecting > Intersect_percentile] + (Intersecting[Intersecting > Intersect_percentile]/100)*Perc_diff

      #replace any values greater than 1 with 1
      Intersecting[Intersecting > 1] <- 1

      #index which cells need to have value updated
      ix <- Intersecting > Intersect_percentile

      #replace values in target raster
      Prob_raster_stack@layers[[Target_classes]][ix] <- Intersecting[ix]


      #else if Perc_diff is >0 then increase the probability of instances above the
      #90th percentile for the outside pixels by the percentage difference
      #between the means
      }else if(Perc_diff <0){

      #check threshold
      if(abs(Perc_diff) < Prob_perturb_thresh){Perc_diff <- -c(Prob_perturb_thresh)}

      non_intersecting[non_intersecting > Nonintersect_percentile] <- non_intersecting[non_intersecting > Nonintersect_percentile] + (non_intersecting[non_intersecting > Nonintersect_percentile]/100)*Perc_diff

      #replace any values greater than 1 with 1
      non_intersecting[non_intersecting > 1] <- 1

      #index which cells need to have value updated
      ix <- non_intersecting > Nonintersect_percentile

      #replace values in target raster
      Prob_raster_stack@layers[[Target_classes]][ix] <- non_intersecting[ix]
      } #close else if statement

    }#close Mountain_development chunk

    #--------------------------------------------------------------------------
    # Rural_migration intervention
    #--------------------------------------------------------------------------
    if(Intervention_ID == "Rural_migration"){

    #load municipality typology raster
    Intervention_rast <- raster(Intervention_data)

    #seperate raster legend and recode values for remote rural municaplities
    Leg <- Intervention_rast@data@attributes[[1]]
    Leg[Leg$ID %in% c(325, 326, 327, 335, 338), "type"] <- 1
    Leg[Leg$type != 1, "type"] <- NA
    Leg$type <- as.numeric(Leg$type)

    #reclassify raster
    Intervention_rast <- reclassify(Intervention_rast, rcl = Leg)

    #identify pixels inside of remote rural municipalities
    Intersecting <- mask(Prob_raster_stack@layers[[Target_classes]], Intervention_rast == 1)

    #identify pixels outside of remote rural municipalities
    non_intersecting <- overlay(Prob_raster_stack@layers[[Target_classes]],Intervention_rast,fun = function(x, y) {
      x[y==1] <- NA
      return(x)
    })

    #Because the intended effect of the intervention is to increase the
    #probability of urban development in the remote rural municipalities

    #calculate 90th percentile value of probability for pixels inside and outside
    #excluding cells with a value of 0 in both cases
    Intersect_percentile <- quantile(Intersecting@data@values[Intersecting@data@values >0], probs = 0.90, na.rm=TRUE)
    Nonintersect_percentile <- quantile(non_intersecting@data@values[non_intersecting@data@values >0], probs = 0.90, na.rm=TRUE)

    #get the means of the values above the percentiles
    Intersect_percentile_mean <- mean(Intersecting@data@values[Intersecting@data@values >= Intersect_percentile], na.rm = TRUE)
    Nonintersect_percentile_mean <- mean(non_intersecting@data@values[non_intersecting@data@values >= Nonintersect_percentile], na.rm = TRUE)

    #mean difference
    Mean_diff <- Nonintersect_percentile_mean - Intersect_percentile_mean

    #Average of means
    Average_mean <- mean(Intersect_percentile_mean, Nonintersect_percentile_mean)

    #percentage difference
    Perc_diff <- (Mean_diff/Average_mean)*100

    #If Perc_diff is > 0 then increase the probability of instances above the
    #90th percentile for the pixels in remote rural municipalities by the
    #percentage difference between the means (or the threshold value)
    if(Perc_diff >0){

      #check threshold
      if(abs(Perc_diff) < Prob_perturb_thresh){Perc_diff <- Prob_perturb_thresh}

      #increase the values
      Intersecting[Intersecting > Intersect_percentile] <-Intersecting[Intersecting > Intersect_percentile] + (Intersecting[Intersecting > Intersect_percentile]/100)*Perc_diff

      #replace any values greater than 1 with 1
      Intersecting[Intersecting > 1] <- 1

      #index which cells need to have value updated
      ix <- Intersecting > Intersect_percentile

      #replace values in target raster
      Prob_raster_stack@layers[[Target_classes]][ix] <- Intersecting[ix]


      #else if Perc_diff is >0 then increase the probability of instances above the
      #90th percentile for the pixels outside the remote rural municipalities
      #by the percentage difference between the means
      }else if(Perc_diff <0){

      #check threshold
      if(abs(Perc_diff) < Prob_perturb_thresh){Perc_diff <- -c(Prob_perturb_thresh)}

      non_intersecting[non_intersecting > Nonintersect_percentile] <- non_intersecting[non_intersecting > Nonintersect_percentile] + (non_intersecting[non_intersecting > Nonintersect_percentile]/100)*Perc_diff

      #replace any values greater than 1 with 1
      non_intersecting[non_intersecting > 1] <- 1

      #index which cells need to have value updated
      ix <- non_intersecting > Nonintersect_percentile

      #replace values in target raster
      Prob_raster_stack@layers[[Target_classes]][ix] <- non_intersecting[ix]
      } #close else if statement

    }#close Rural_migration chunk

    #--------------------------------------------------------------------------
    # Agri_abandonment intervention
    #--------------------------------------------------------------------------

    #The predicted probability of cells to transition from agriculture to other
    #LULC classes already uses accessibility based predictors such as
    #distance to roads/slope however other variables e.g climaticor soil may be having
    #a larger effect hence we should apply a simple analysis based upon the
    #model used by Gellrich et al. 2007 that considers distance to roads,
    #slope and distance to building zones as a measure of 'marginality' and
    #then select the 90th percentile of pixels according to this value

    #load the static predictor layers and re-scale between 0-1

    #function for rescaling:
    # rescale <- function(x, x.min, x.max, new.min = 0, new.max = 1) {
    # if(is.null(x.min)) {x.min = min(x)}
    # if(is.null(x.max)) {x.max = max(x)}
    # new.min + (x - x.min) * ((new.max - new.min) / (x.max - x.min))
    # }
    #
    # #distance to roads
    # Dist2rds <- raster("Data/Preds/Prepared/Layers/Transport/Distance_to_roads_mean_100m.tif")
    # Dist2rds <- calc(Dist2rds, function(x) rescale(x,x.min= minValue(Dist2rds),
    #                                    x.max = maxValue(Dist2rds)))
    #
    # #Slope
    # Slope <- raster("Data/Preds/Prepared/Layers/Topographic/Slope_mean_100m.tif")
    # Slope <- calc(Slope, function(x) rescale(x, x.min= minValue(Slope),
    #                                    x.max = maxValue(Slope)))
    #
    # # Distance to building zones
    # #This layer needs to be inverted when re-scaling because
    # #greater distance from building zones means lower land cost
    # #which means less likely to abandon hence x.min and x.max values swapped
    # Dist2BZ <- raster("Data/Spat_prob_perturb_layers/Bulding_zones/BZ_distance.tif")
    # Dist2BZ <- calc(Dist2BZ, function(x) rescale(x, x.min= maxValue(Dist2BZ),
    #                                    x.max = minValue(Dist2BZ)))
    #
    # #stack dist2rds, slope and forest_dist layers and sum values as raster
    # Marginality_rast <- calc(stack(Dist2rds, Slope), sum)
    #
    # #subset the marginality raster to only the pixels of the agricultural
    # #land types (Int_AG, Alp_Past)
    # Agri_rast <- rasterFromXYZ(Raster_prob_values[,c("x", "y", "Alp_Past")])
    # Agri_rast[Agri_rast == 0] <- NA
    # Agri_marginality <- mask(Marginality_rast, Agri_rast)
    #
    # #calculate the upper quartile value of marginality for the agricultural cells
    # Marginality_percentile <- quantile(Agri_marginality@data@values, probs = 0.75, na.rm=TRUE)
    #
    # #indexes of all cells above/below the upper quartile
    # marginal_index <- Agri_marginality > Marginality_percentile
    # non_marginal_index <- Agri_marginality < Marginality_percentile
    #
    # #loop over target classes
    # for(class in Target_classes){}
    # class <- Target_classes[[1]]
    #
    # #calculate the average probability of transition to the target in the
    # #marginal agricultural cells vs. non-marginal
    # marginal_cells <- Prob_raster_stack@layers[[class]][marginal_index]
    # marginal_percentile <- quantile(marginal_cells[marginal_cells >0],probs = 0.9, na.rm=TRUE)
    # mean_marginal_percentile <- mean(marginal_cells[marginal_cells > marginal_percentile])
    #
    # non_marginal_cells <- Prob_raster_stack@layers[[class]][non_marginal_index]
    # nonmarginal_percentile <- quantile(non_marginal_cells[non_marginal_cells >0], probs = 0.9, na.rm=TRUE)
    # mean_nonmarginal_percentile <- mean(non_marginal_cells[non_marginal_cells > nonmarginal_percentile])
    #
    # #} close loop over target classes
    #
    # writeRaster(Agri_marginality, "E:/LULCC_CH/Data/Temp/Intervention_raster.tif", overwrite = TRUE)
    # writeRaster(Agri_rast, "E:/LULCC_CH/Data/Temp/Target_raster.tif", overwrite = TRUE)
    # writeRaster(Intersecting, "E:/LULCC_CH/Data/Temp/Intersecting_raster.tif", overwrite = TRUE)
    # writeRaster(non_intersecting, "E:/LULCC_CH/Data/Temp/Non_Intersect_raster.tif", overwrite = TRUE)

    #--------------------------------------------------------------------------
    #Protection intervention
    #--------------------------------------------------------------------------

  } #close loop over interventions

    #convert raster stack back to dataframe
  Prob_df <- raster::as.data.frame(Prob_raster_stack)

  #replace the original predictions with the manipulated values
  Raster_prob_values[,names(Prob_df)] <- Prob_df

  #subset to only the prediction and spatial info cols
  Raster_prob_values <- Raster_prob_values[,c("ID", "x", "y", Pred_prob_columns)]
  } #close if statement
return(Raster_prob_values)
} # close function
