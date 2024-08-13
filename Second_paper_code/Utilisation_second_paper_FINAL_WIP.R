#### Park Utilisation measures, second paper ####
# R Script for subsequent analysis following publication of paper
# "Measuring spatial inequality of urban park accessibility and utilisation: A case study of public housing developments in Auckland, New Zealand"
# 2024
# Jessie Colbert, Katarzyna Sila-Nowicka, I-Ting Chuang, The University of Auckland

#### WIP - the metrics that use the actual park visits network analysis distance are still TBC in the Python script ####
#### Note that this only affects the calculation of the DAT, which currently uses the Euclidean distance as a stand-in, and the network distance version of the NPVR. The Euclidean distance versions can be output using the script below, and the network distance code will be added ASAP ####

# load packages
library(sp)
library(sf)
library(rgdal)
library(dplyr)
library(ggplot2)
library(plotly)
library(lubridate)

library(foreach)
library(doParallel)

#### Load in the data and prepare ####
# load in the distance results and join

# list of input datasets to iterate over
input_datasets = list("18_03_27_03", "20_28_04", "25_26_03", "28_4_05_05", "2703004", "04_03_20")

# study site boundaries shapefile
innercity_LCDB_no_airports_shp = st_read("Input/Study_site_boundaries.shp")

# centroid points for the parks within the Auckland Regional Council boundaries that meet the criteria for inclusion
# see first paper for inclusion criteria for parks
park_points = st_read("Input/ParkExtent_Parks_Reg_10mill_sqm_Akld_RC_clip_points.shp")

# create a table version (no geometry)
park_points.df = st_drop_geometry(park_points)
# add column for joining
park_points.df$polygon_ID = park_points.df$ORIG_FID + 1

# rename for use in script
parks_Akld_RC_points = park_points

# park polygons for the points
parks_Akld_RC_poly = st_read("Input/ParkExtent_Parks_Reg_10mill_sqm_Akld_RC_clip.shp")

# set CRS to 2193
park_points = st_transform(park_points, crs = 2193)
parks_Akld_RC_points = st_transform(parks_Akld_RC_points, crs = 2193)
parks_Akld_RC_poly = st_transform(parks_Akld_RC_poly, crs = 2193)

# convert to sp object for analysis
parks_Akld_RC_points = as(parks_Akld_RC_points, "Spatial")
parks_Akld_RC_poly = as(parks_Akld_RC_poly, "Spatial")

# optional if need to load in the grid cells to re-join
# inner city clip version (not LCDB and no airports though)
grid_cells_shp = st_read("Input/grid_300m_hexagon_innercity_clip.shp")



#### Combine the three distance calculations ####

# run in parallel
# Get the number of available cores
cores = detectCores()

# Create a parallel backend using the available cores (minus one to avoid overloading the computer)
cl = makeCluster(cores[1] -1)
registerDoParallel(cl)

foreach(i = 1:length(input_datasets),
        .packages = c("sp", "sf", "dplyr")) %dopar% {
  
  # near, polygons (shapefile)
  near_polygons_shp = st_read(paste0("Output/Output_", input_datasets[[i]], "/homeloc_", input_datasets[[i]], "_nearest_park_polygon.shp"))
  
  # near, centroids (shapefile)
  network_centroids_shp = st_read(paste0("Output/Output_", input_datasets[[i]], "/homeloc_", input_datasets[[i]], "_nearest_park_network_routes.shp"))
  
  # convert all to df and merge together based on the user hashed_id
  near_polygons.df = st_drop_geometry(near_polygons_shp)
  
  network_centroids.df = st_drop_geometry(network_centroids_shp)
  
  # set up columns
  network_centroids.df$homeloc_ID = gsub("^([0-9]+) - Location [0-9]+$", "\\1", network_centroids.df$Name)
  # Add park information for facility ID
  network_centroids.df_parks = merge(network_centroids.df, park_points.df[, c(6,13)], by.x = "FacilityID", by.y = "polygon_ID", all.x = TRUE)
  network_centroids.df_parks$polygon_ID = network_centroids.df_parks$FacilityID
  
  # merge together into one df
  # the two near distances are already joined into one df
  length(near_polygons.df$ID)
  length(network_centroids.df_parks$FacilityID)
  
  print(paste0("The # rows in near polygons is ", length(near_polygons.df$ID), ", the # rows in network centroids is ", length(network_centroids.df_parks$FacilityID), " for ", input_datasets[[i]]))
  
  # join on the network results
  merge_distances_df = merge(near_polygons.df, network_centroids.df_parks[,c(11,12,14,15,16)], by.x = "ID", by.y = "homeloc_ID", all.x = TRUE, all.y = TRUE)
  
  # join to the home location geometry for the users
  # load in the home location geometry
  homes_shp = st_read(paste0("Input/median_centroids_first_cluster_table_", input_datasets[[i]], "_filtered_no_duplicates.shp"))
  
  # clip to the LCDB built-up area, no airports in the inner city (study site)
  clipped_points = st_intersection(homes_shp, innercity_LCDB_no_airports_shp)
  
  print(paste0("The # rows in homeloc clipped to study site is ", length(clipped_points$ID), " for ", input_datasets[[i]]))
  
  # join the merged table based on hashed_id, keeping only those ids that have a match in the clipped points
  clipped_points_merge = merge(clipped_points[,c(1:8,14:19)], merge_distances_df[,c(7,9:18)], by.x = "hashd_d", by.y = "hashd_d", all.x = TRUE)
  
  # for the remaining point locations, aggregate to the grid cells
  # join the points to the grid cells -> assign grid cell ID to each user ID

  overlay_points = st_join(clipped_points_merge, grid_cells_shp)
  
  print(paste0("The # rows in the points dataset joined to the grid cells is ", length(overlay_points$hashd_d), " for ", input_datasets[[i]]))
  
  # export this result for later work
  st_write(overlay_points, paste0("Output/Output_", input_datasets[[i]], "/homeloc_", input_datasets[[i]], "_three_distances_LCDB_grid_id.shp"))
  
  
  # average (mean, median) distance of users per grid cell, for the distances
  summary_points_by_grid_id = st_drop_geometry(overlay_points) %>%
    group_by(id) %>%
    summarise(
      Mean_near_pt = mean(as.numeric(NEARFIDpt), na.rm = TRUE),
      Median_near_pt = median(as.numeric(NEARFIDpt), na.rm = TRUE),
      Mean_near_py = mean(as.numeric(NEARDISTpy), na.rm = TRUE),
      Median_near_py = median(as.numeric(NEARDISTpy), na.rm = TRUE),
      Mean_network = mean(as.numeric(Total_Leng), na.rm = TRUE),
      Median_network = median(as.numeric(Total_Leng), na.rm = TRUE)
    )
  
  # save the result to a table which when combined with the grid cell geometries can be mapped
  write.csv(summary_points_by_grid_id, paste0("Output/Output_", input_datasets[[i]], "/homeloc_", input_datasets[[i]], "_three_distances_LCDB_grid_id_averages.csv"))
  
}

stopCluster(cl)


#### Utilisation metrics ####

# run the script iterating over each input dataset

# run in parallel
cl = makeCluster(cores[1] -1)
registerDoParallel(cl)

foreach(i = 1:length(input_datasets),
        .packages = c("sp", "sf", "dplyr")) %dopar% {

  #### Load in the data ####
  
  # Load in the three distances per median centroid (hashed_id), the geometry is the home location point
  homeloc_dist_shp = st_read(paste0("Output/Output_", input_datasets[[i]], "/homeloc_", input_datasets[[i]], "_three_distances_LCDB_grid_id.shp"))
  
  # Load in the result (so far) of the actual park visits
  # NOTE this will eventually also contain the network analysis for actual park visits, Euclidean distance only for now
  actual_park_visits_lines = st_read(paste0("Output/Output_", input_datasets[[i]], "/homeloc_", input_datasets[[i]], "_join_parks_intersect_user_traj_not_null_merge_lines.shp"))
  
  # use the park point centroid version
  actual_park_visits_park_centroid = st_read(paste0("Output/Output_", input_datasets[[i]], "/park_points_join_homeloc_", input_datasets[[i]], "_parks_intersect_user_traj_not_null_N.shp"))
  
  # convert to NZGD CRS for later calculations
  actual_park_visits_park_centroid = st_transform(actual_park_visits_park_centroid, crs = 2193)
  
  actual_park_visits_park_centroid_dist = merge(actual_park_visits_park_centroid, st_drop_geometry(actual_park_visits_lines)[,c(2,3)], by = "Line_ID", all.x = TRUE, all.y = TRUE)
  
  actual_park_visits_park_centroid_dist$Near_dist = actual_park_visits_park_centroid_dist$LENGTH
  
  # clip to those users that have their home location within the study site
  # subset to those rows where the hashed_id is within the unique list of values in the LCDB output shapefile
  clipped_hashed_id = unique(homeloc_dist_shp$hashd_d)
  
  actual_park_visits_park_centroid_dist_LCDB = actual_park_visits_park_centroid_dist[actual_park_visits_park_centroid_dist$hashd_d %in% clipped_hashed_id,]
  # this is the park centroids for the park visited by a user within the REGC, for those users who have a home location in the study site, with the euclidean distance from the home location point to the park centroid
  
  # load in the buffers
  buffer_1km = st_read(paste0("Output/Output_", input_datasets[[i]], "/homeloc_", input_datasets[[i]], "_1km_buffer.shp"))
  
  buffer_2km = st_read(paste0("Output/Output_", input_datasets[[i]], "/homeloc_", input_datasets[[i]], "_2km_buffer.shp"))
  
  buffer_5km = st_read(paste0("Output/Output_", input_datasets[[i]], "/homeloc_", input_datasets[[i]], "_5km_buffer.shp"))
  
  # subset to those home locations within the study site
  buffer_1km_LCDB = buffer_1km[buffer_1km$hashd_d %in% clipped_hashed_id,]
  buffer_2km_LCDB = buffer_2km[buffer_2km$hashd_d %in% clipped_hashed_id,]
  buffer_5km_LCDB = buffer_5km[buffer_5km$hashd_d %in% clipped_hashed_id,]
  
  
  ## ANALYSIS ##
  
  # minimum distance per unique hashed_id to park actually visited
  # euclidean distance (LENGTH)
  actual_min_park_visited = st_drop_geometry(actual_park_visits_park_centroid_dist_LCDB) %>%
    group_by(hashd_d) %>%
    filter(LENGTH == min(LENGTH, na.rm = TRUE))
  
  actual_min_park_visited.df = as.data.frame(actual_min_park_visited)
  
  # remove duplicate rows
  actual_min_park_visited.df_nodup = actual_min_park_visited.df %>% distinct(hashd_d, .keep_all = TRUE)
  
  # join the theoretical and actual park visits based on the hashed_id
  merged_table = merge(st_drop_geometry(homeloc_dist_shp), actual_min_park_visited.df_nodup,
                       by.x = "hashd_d", by.y = "hashd_d", all.x = TRUE, all.y = TRUE)
  
  # some users with a home location within the study site may not have visited any park, so some rows will have NA values for the actual park visit distance
  
  
  #### DAT ####
  # FOR EUCLIDEAN AS A PROXY - this ideally should use the network distance for the actual visited closest park - TBA
  # Use theoretical closest park (network distance for home locations)
  # Use euclidean distance actual visited closest park
  
  # calculate differences between minimum distance to park actually visited and minimum distance to any park
  # actual - theoretical
  merged_table$DAT = merged_table$LENGTH - merged_table$Ttl_Lng
  hist(merged_table$DAT)
  summary(merged_table$DAT)
  
  # average to grid cells (mean, median)
  # average (mean, median) distance of users per grid cell, for the distances
  summary_DAT_by_grid_id = merged_table %>%
    group_by(id_1) %>%
    summarise(
      Mean_DAT = mean(DAT, na.rm = TRUE),
      Median_DAT = median(DAT, na.rm = TRUE)
    )
  
  colnames(summary_DAT_by_grid_id)[1] = "grid_id"
  
  # save the result to a table which when combined with the grid cell geometries can be mapped
  # send this to Sila
  write.csv(summary_DAT_by_grid_id, paste0("Output/Output_", input_datasets[[i]], "/homeloc_", input_datasets[[i]], "_DAT_averages_grid_id.csv"))
  
  
  #### UPO ####
  # Number of parks visited divided by the number of parks available (within 1km, 2km, 5km)  - calculate it per person and then average per grid.
  # for the centroid of the park, or the edge of the polygon for the park
  
  # STEP ONE - Calculate the numerator, number of parks visited within distance threshold buffer #
  # for unique hashed_id in list of all unique users in the study site, extract out the respective buffer from the shapefile and all the park visit points with the same user ID
  # then run selection (within/point in polygon) so only those points in the buffer polygon are included
  # then calculate the number of unique polygon IDs and add the output number to the new dataframe to use in further calculations
  
  # new dataframe to fill with results
  users_list_innercity_index_to_fill = data.frame(
    hashed_id = clipped_hashed_id,
    number_parks_visited_1km = rep(0, length(clipped_hashed_id)),
    number_parks_visited_2km = rep(0, length(clipped_hashed_id)),
    number_parks_visited_5km = rep(0, length(clipped_hashed_id))
  )
  
  users_innercity_1km_buffer = as(buffer_1km_LCDB, "Spatial")
  users_innercity_2km_buffer = as(buffer_2km_LCDB, "Spatial")
  users_innercity_5km_buffer = as(buffer_5km_LCDB, "Spatial")
  
  # assign CRS
  #proj4string(users_innercity_1km_buffer) = CRS("+init=epsg:2193")
  #proj4string(users_innercity_2km_buffer) = CRS("+init=epsg:2193")
  #proj4string(users_innercity_5km_buffer) = CRS("+init=epsg:2193")
  
  # loop through the hashed_ids and run step one calculations
  for (j in 1:length(clipped_hashed_id)) {
    
    # buffer subset for user
    user_id_1km_buffer = users_innercity_1km_buffer[users_innercity_1km_buffer$hashd_d %in% clipped_hashed_id[j],]
    user_id_2km_buffer = users_innercity_2km_buffer[users_innercity_2km_buffer$hashd_d %in% clipped_hashed_id[j],]
    user_id_5km_buffer = users_innercity_5km_buffer[users_innercity_5km_buffer$hashd_d %in% clipped_hashed_id[j],]
    
    # extract subset of user visited park points for the user id
    user_id_parks_visited_points = as(actual_park_visits_park_centroid_dist_LCDB, "Spatial")
    user_id_parks_visited_points = user_id_parks_visited_points[user_id_parks_visited_points$hashd_d %in% clipped_hashed_id[j],]
    
    if (!length(user_id_parks_visited_points$hashd_d) %in% 0) {
      
      # select points that are within the buffers (point in polygon)
      # create output df where the points with a joined polygon buffer are included in the count
      user_id_parks_visited_points_1km = user_id_parks_visited_points
      user_id_park_visit_points_in_poly_1km = over(user_id_parks_visited_points, user_id_1km_buffer)
      user_id_parks_visited_points_1km@data = cbind(user_id_parks_visited_points_1km@data, user_id_park_visit_points_in_poly_1km)
      
      user_id_parks_visited_points_2km = user_id_parks_visited_points
      user_id_park_visit_points_in_poly_2km = over(user_id_parks_visited_points, user_id_2km_buffer)
      user_id_parks_visited_points_2km@data = cbind(user_id_parks_visited_points_2km@data, user_id_park_visit_points_in_poly_2km)
      
      user_id_parks_visited_points_5km = user_id_parks_visited_points
      user_id_park_visit_points_in_poly_5km = over(user_id_parks_visited_points, user_id_5km_buffer)
      user_id_parks_visited_points_5km@data = cbind(user_id_parks_visited_points_5km@data, user_id_park_visit_points_in_poly_5km)
      
      # find which rows in the output df have a join to the buffer polygon
      # extract rows that have the polygon buffer information joined (there should be a column with Y/N etc identifying)
      user_id_parks_visited_points_1km_joined = user_id_parks_visited_points_1km[user_id_parks_visited_points_1km$BUFF_DIST %in% 1000,]
      user_id_parks_visited_points_2km_joined = user_id_parks_visited_points_2km[user_id_parks_visited_points_2km$BUFF_DIST %in% 2000,]
      user_id_parks_visited_points_5km_joined = user_id_parks_visited_points_5km[user_id_parks_visited_points_5km$BUFF_DIST %in% 5000,]
      
      # count the number of unique polygon ids
      # check what the name of the polygon id column is
      n_parks_visited_1km = length(unique(user_id_parks_visited_points_1km_joined$polygon_id))
      n_parks_visited_2km = length(unique(user_id_parks_visited_points_2km_joined$polygon_id))
      n_parks_visited_5km = length(unique(user_id_parks_visited_points_5km_joined$polygon_id))
      
      # add to the dataframe under the respective column
      users_list_innercity_index_to_fill$number_parks_visited_1km[j] = n_parks_visited_1km
      users_list_innercity_index_to_fill$number_parks_visited_2km[j] = n_parks_visited_2km
      users_list_innercity_index_to_fill$number_parks_visited_5km[j] = n_parks_visited_5km
      
    } else {
      
      # add to the dataframe under the respective column
      users_list_innercity_index_to_fill$number_parks_visited_1km[j] = 0
      users_list_innercity_index_to_fill$number_parks_visited_2km[j] = 0
      users_list_innercity_index_to_fill$number_parks_visited_5km[j] = 0
      
    }
    
    print(paste0("User ", j, " (User ID: ", clipped_hashed_id[j], ")"))
    
  }
  
  # export part one as a back-up
  write.csv(users_list_innercity_index_to_fill, paste0("Output/Output_", input_datasets[[i]], "/homeloc_", input_datasets[[i]], "_UPO_by_hashed_id_LCDB_partone.csv"), row.names = FALSE)
  
  # STEP TWO - Calculate the denominator, the number of parks available within the distance threshold #
  
  # dataframe to fill, for part two
  users_list_innercity_index_to_fill_pt2 = users_list_innercity_index_to_fill
  users_list_innercity_index_to_fill_pt2$number_parks_available_1km = rep(0, length(clipped_hashed_id))
  users_list_innercity_index_to_fill_pt2$number_parks_available_2km = rep(0, length(clipped_hashed_id))
  users_list_innercity_index_to_fill_pt2$number_parks_available_5km = rep(0, length(clipped_hashed_id))
  
  users_list_innercity_index_to_fill_pt2$number_parks_available_1km_centroids = rep(0, length(clipped_hashed_id))
  users_list_innercity_index_to_fill_pt2$number_parks_available_2km_centroids = rep(0, length(clipped_hashed_id))
  users_list_innercity_index_to_fill_pt2$number_parks_available_5km_centroids = rep(0, length(clipped_hashed_id))
  
  # loop through the hashed_ids and run step two calculations
  for (j in 1:length(clipped_hashed_id)) {
 
    # buffer subset for user
    user_id_1km_buffer = users_innercity_1km_buffer[users_innercity_1km_buffer$hashd_d %in% clipped_hashed_id[j],]
    user_id_2km_buffer = users_innercity_2km_buffer[users_innercity_2km_buffer$hashd_d %in% clipped_hashed_id[j],]
    user_id_5km_buffer = users_innercity_5km_buffer[users_innercity_5km_buffer$hashd_d %in% clipped_hashed_id[j],]
    
    # select parks (polygon/centroid) that are within the buffers (point in polygon, or polygon intersect polygon)
    # create output dataframe where the points/polygons with a joined polygon buffer are included in the count
    
    ## park polygons ##
    user_id_parks_available_polygons_1km = parks_Akld_RC_poly
    user_id_parks_available_poly_in_poly_1km = over(user_id_parks_available_polygons_1km, user_id_1km_buffer)
    user_id_parks_available_polygons_1km@data = cbind(user_id_parks_available_polygons_1km@data, user_id_parks_available_poly_in_poly_1km)
    
    user_id_parks_available_polygons_2km = parks_Akld_RC_poly
    user_id_parks_available_poly_in_poly_2km = over(user_id_parks_available_polygons_2km, user_id_2km_buffer)
    user_id_parks_available_polygons_2km@data = cbind(user_id_parks_available_polygons_2km@data, user_id_parks_available_poly_in_poly_2km)
    
    user_id_parks_available_polygons_5km = parks_Akld_RC_poly
    user_id_parks_available_poly_in_poly_5km = over(user_id_parks_available_polygons_5km, user_id_5km_buffer)
    user_id_parks_available_polygons_5km@data = cbind(user_id_parks_available_polygons_5km@data, user_id_parks_available_poly_in_poly_5km)
    
    # find which rows in the output dataframe have a join to the buffer polygon
    # extract rows that have the polygon buffer info joined (there should be a col with Y/N etc identifying)
    user_id_parks_available_polygons_1km_joined = user_id_parks_available_polygons_1km[user_id_parks_available_polygons_1km$BUFF_DIST %in% 1000,]
    user_id_parks_available_polygons_2km_joined = user_id_parks_available_polygons_2km[user_id_parks_available_polygons_2km$BUFF_DIST %in% 2000,]
    user_id_parks_available_polygons_5km_joined = user_id_parks_available_polygons_5km[user_id_parks_available_polygons_5km$BUFF_DIST %in% 5000,]
    
    # count the number of unique polygon ids
    # check what the name of the polygon id column is
    n_parks_available_polygons_1km = length(unique(user_id_parks_available_polygons_1km_joined$SAPID))
    n_parks_available_polygons_2km = length(unique(user_id_parks_available_polygons_2km_joined$SAPID))
    n_parks_available_polygons_5km = length(unique(user_id_parks_available_polygons_5km_joined$SAPID))
    
    # add to the dataframe under the respective column
    users_list_innercity_index_to_fill_pt2$number_parks_available_1km[j] = n_parks_available_polygons_1km
    users_list_innercity_index_to_fill_pt2$number_parks_available_2km[j] = n_parks_available_polygons_2km
    users_list_innercity_index_to_fill_pt2$number_parks_available_5km[j] = n_parks_available_polygons_5km
    
    
    ## centroids of parks ##
    user_id_parks_available_centroids_1km = parks_Akld_RC_points
    user_id_parks_available_point_in_poly_1km = over(user_id_parks_available_centroids_1km, user_id_1km_buffer)
    user_id_parks_available_centroids_1km@data = cbind(user_id_parks_available_centroids_1km@data, user_id_parks_available_point_in_poly_1km)
    
    user_id_parks_available_centroids_2km = parks_Akld_RC_points
    user_id_parks_available_point_in_poly_2km = over(user_id_parks_available_centroids_2km, user_id_2km_buffer)
    user_id_parks_available_centroids_2km@data = cbind(user_id_parks_available_centroids_2km@data, user_id_parks_available_point_in_poly_2km)
    
    user_id_parks_available_centroids_5km = parks_Akld_RC_points
    user_id_parks_available_point_in_poly_5km = over(user_id_parks_available_centroids_5km, user_id_5km_buffer)
    user_id_parks_available_centroids_5km@data = cbind(user_id_parks_available_centroids_5km@data, user_id_parks_available_point_in_poly_5km)
    
    # which rows in the output df have a join to the buffer polygon?
    # extract rows that have the polygon buffer info joined (there should be a col with Y/N etc identifying)
    user_id_parks_available_centroids_1km_joined = user_id_parks_available_centroids_1km[user_id_parks_available_centroids_1km$BUFF_DIST %in% 1000,]
    user_id_parks_available_centroids_2km_joined = user_id_parks_available_centroids_2km[user_id_parks_available_centroids_2km$BUFF_DIST %in% 2000,]
    user_id_parks_available_centroids_5km_joined = user_id_parks_available_centroids_5km[user_id_parks_available_centroids_5km$BUFF_DIST %in% 5000,]
    
    # count the number of unique polygon ids
    # check what the name of the polygon id column is
    n_parks_available_centroids_1km = length(unique(user_id_parks_available_centroids_1km_joined$SAPID))
    n_parks_available_centroids_2km = length(unique(user_id_parks_available_centroids_2km_joined$SAPID))
    n_parks_available_centroids_5km = length(unique(user_id_parks_available_centroids_5km_joined$SAPID))
    
    # add to the df under the respective column
    users_list_innercity_index_to_fill_pt2$number_parks_available_1km_centroids[j] = n_parks_available_centroids_1km
    users_list_innercity_index_to_fill_pt2$number_parks_available_2km_centroids[j] = n_parks_available_centroids_2km
    users_list_innercity_index_to_fill_pt2$number_parks_available_5km_centroids[j] = n_parks_available_centroids_5km
    
    print(paste0("User ", j, " (User ID: ", clipped_hashed_id[j], ")"))
    
  }
  
  # export part two as a back-up
  write.csv(users_list_innercity_index_to_fill_pt2, paste0("Output/Output_", input_datasets[[i]], "/homeloc_", input_datasets[[i]], "_UPO_by_hashed_id_LCDB_parttwo.csv"), row.names = FALSE)
  
  # step three
  # calculate the final UPO by 1km, 2km, 5km by either park centroid or park edge (polygon)
  
  users_list_innercity_index_to_fill_pt3 = users_list_innercity_index_to_fill_pt2
  
  # by polygon
  users_list_innercity_index_to_fill_pt3$index_1km_polygon = users_list_innercity_index_to_fill_pt3$number_parks_visited_1km/users_list_innercity_index_to_fill_pt3$number_parks_available_1km
  
  users_list_innercity_index_to_fill_pt3$index_2km_polygon = users_list_innercity_index_to_fill_pt3$number_parks_visited_2km/users_list_innercity_index_to_fill_pt3$number_parks_available_2km
  
  users_list_innercity_index_to_fill_pt3$index_5km_polygon = users_list_innercity_index_to_fill_pt3$number_parks_visited_5km/users_list_innercity_index_to_fill_pt3$number_parks_available_5km
  
  # by centroid
  users_list_innercity_index_to_fill_pt3$index_1km_centroid = users_list_innercity_index_to_fill_pt3$number_parks_visited_1km/users_list_innercity_index_to_fill_pt3$number_parks_available_1km_centroids
  
  users_list_innercity_index_to_fill_pt3$index_2km_centroid = users_list_innercity_index_to_fill_pt3$number_parks_visited_2km/users_list_innercity_index_to_fill_pt3$number_parks_available_2km_centroids
  
  users_list_innercity_index_to_fill_pt3$index_5km_centroid = users_list_innercity_index_to_fill_pt3$number_parks_visited_5km/users_list_innercity_index_to_fill_pt3$number_parks_available_5km_centroids
  
  # export to ArcGIS
  # take average of this proportion per user per grid cell
  # visualise the output using maps
  write.csv(users_list_innercity_index_to_fill_pt3, paste0("Output/Output_", input_datasets[[i]], "/homeloc_", input_datasets[[i]], "_UPO_by_hashed_id_LCDB.csv"), row.names = FALSE)
  
  # join the grid id to the df, based on the hashed_id and the home location of that user
  users_list_innercity_index_to_fill_pt3_grid_id = merge(users_list_innercity_index_to_fill_pt3, merged_table[,c(1,24)],
                                                         by.x = "hashed_id", by.y = "hashd_d", all.x = TRUE)
  
  # average to grid cells (mean, median)
  # average (mean, median) distance of users per grid cell
  summary_UPO_by_grid_id = users_list_innercity_index_to_fill_pt3_grid_id %>%
    group_by(id_1) %>%
    summarise(
      Mean_UPO_1km_centroid = mean(index_1km_centroid, na.rm = TRUE),
      Median_UPO_1km_centroid = median(index_1km_centroid, na.rm = TRUE),
      Mean_UPO_2km_centroid = mean(index_2km_centroid, na.rm = TRUE),
      Median_UPO_2km_centroid = median(index_2km_centroid, na.rm = TRUE),
      Mean_UPO_5km_centroid = mean(index_5km_centroid, na.rm = TRUE),
      Median_UPO_5km_centroid = median(index_5km_centroid, na.rm = TRUE),
      
      Mean_UPO_1km_polygon = mean(index_1km_polygon, na.rm = TRUE),
      Median_UPO_1km_polygon = median(index_1km_polygon, na.rm = TRUE),
      Mean_UPO_2km_polygon = mean(index_2km_polygon, na.rm = TRUE),
      Median_UPO_2km_polygon = median(index_2km_polygon, na.rm = TRUE),
      Mean_UPO_5km_polygon = mean(index_5km_polygon, na.rm = TRUE),
      Median_UPO_5km_polygon = median(index_5km_polygon, na.rm = TRUE)
    )
  
  colnames(summary_UPO_by_grid_id)[1] = "grid_id"
  
  # save the result to a table which when combined with the grid cell geometries can be mapped
  # send this to Sila
  write.csv(summary_UPO_by_grid_id, paste0("Output/Output_", input_datasets[[i]], "/homeloc_", input_datasets[[i]], "_UPO_averages_grid_id.csv"))
  
  
  #### LPV ####
  # Number of unique parks visited (1km, 2km, 5km versions) divided by the number of unique parks visited any distance from home location
  # if 0 in the denominator the LPV will be 0
  
  # the same code from the UPO is reused, but modified to have a consistent denominator for each distance threshold (the count of unique parks visited by the user, anywhere in the Auckland Regional Council boundaries)
  
  ## STEP ONE ##
  # step one of the calculation is the same
  users_list_innercity_index_to_fill_cons_denom = users_list_innercity_index_to_fill
  
  ## STEP TWO ##
  # step two (denominator) is consistent for all, so only need to add one column
  users_list_innercity_index_to_fill_cons_denom$number_unique_parks_visited_all = rep(0, length(clipped_hashed_id))
  
  # loop through the hashed_ids and run step one calculations
  for (j in 1:length(clipped_hashed_id)) {
    
    # extract subset of user visited park points for the user id
    user_id_parks_visited_points = as(actual_park_visits_park_centroid_dist_LCDB, "Spatial")
    user_id_parks_visited_points = user_id_parks_visited_points[user_id_parks_visited_points$hashd_d %in% clipped_hashed_id[j],]
    
    if (!length(clipped_hashed_id) %in% 0) {
      
      # count the number of unique polygon ids
      # check what the name of the polygon id column is
      n_parks_visited_all = length(unique(user_id_parks_visited_points$polygon_id))
      
      # add to the dataframe under the respective column
      users_list_innercity_index_to_fill_cons_denom$number_unique_parks_visited_all[j] = n_parks_visited_all
      
    } else {
      
      # add to the dataframe under the respective column
      users_list_innercity_index_to_fill_cons_denom$number_unique_parks_visited_all[j] = 0
      
    }
    
    print(paste0("User ", j, " (User ID: ", clipped_hashed_id[j], ")"))
    
  }
  
  ## STEP THREE ##
  # divide all the columns from part one by the same denominator column
  users_list_innercity_index_to_fill_cons_denom$index_1km_cons_denom = users_list_innercity_index_to_fill_cons_denom$number_parks_visited_1km
  users_list_innercity_index_to_fill_cons_denom$index_2km_cons_denom = users_list_innercity_index_to_fill_cons_denom$number_parks_visited_2km
  users_list_innercity_index_to_fill_cons_denom$index_5km_cons_denom = users_list_innercity_index_to_fill_cons_denom$number_parks_visited_5km
  
  users_list_innercity_index_to_fill_cons_denom[,c(6:8)] = users_list_innercity_index_to_fill_cons_denom[,c(6:8)]/users_list_innercity_index_to_fill_cons_denom$number_unique_parks_visited_all
  
  # export to csv for visualisation
  write.csv(users_list_innercity_index_to_fill_cons_denom, paste0("Output/Output_", input_datasets[[i]], "/homeloc_", input_datasets[[i]], "_LPV_by_hashed_id_LCDB.csv"), row.names = FALSE)
  
  # join the grid id to the df, based on the hashed_id and the home location of that user
  users_list_innercity_index_to_fill_cons_denom_grid_id = merge(users_list_innercity_index_to_fill_cons_denom, merged_table[,c(1,24)],
                                                         by.x = "hashed_id", by.y = "hashd_d", all.x = TRUE)
  
  # average to grid cells (mean, median)
  # average (mean, median) distance of users per grid cell
  summary_LPV_by_grid_id = users_list_innercity_index_to_fill_cons_denom_grid_id %>%
    group_by(id_1) %>%
    summarise(
      Mean_LPV_1km = mean(index_1km_cons_denom, na.rm = TRUE),
      Median_LPV_1km = median(index_1km_cons_denom, na.rm = TRUE),
      Mean_LPV_2km = mean(index_2km_cons_denom, na.rm = TRUE),
      Median_LPV_2km = median(index_2km_cons_denom, na.rm = TRUE),
      Mean_LPV_5km = mean(index_5km_cons_denom, na.rm = TRUE),
      Median_LPV_5km = median(index_5km_cons_denom, na.rm = TRUE)
    )
  
  colnames(summary_LPV_by_grid_id)[1] = "grid_id"
  
  # save the result to a table which when combined with the grid cell geometries can be mapped
  # send this to Sila
  write.csv(summary_LPV_by_grid_id, paste0("Output/Output_", input_datasets[[i]], "/homeloc_", input_datasets[[i]], "_LPV_averages_grid_id.csv"))
  
  
  #### NPVR ####
  #### Nearest Park Visitation Rate ####
  # Proportion of users that have a home location within a specific grid cell that visited their closest theoretical park
  # Number of users who live (have home location within) in a particular grid cell that visited their closest theoretical park divided by the total number of users who live (have home location within) within the grid cell
  
  # calculate for both Euclidean distance and network distance
  #### NOTE - currently only calculated for the Euclidean distance ####
  #### Network distance version TBA ####
  
  ## STEP ONE ##
  # Using dataset from DAT with list of parks visited by a user, with the hashed_id of the user who visited the park and the distance between the park centroid and the home location of the user (using either Euclidean distance or network distance)
  # calculate how many users visited the park closest to them (based on the park ID)
  
  # merged_table again
  # this step is already done
  # calculate differences between minimum distance to park actually visited and minimum distance to any park
  # actual - theoretical
  #merged_table$DAT = merged_table$LENGTH - merged_table$Ttl_Lng
  
  ## STEP TWO ##
  # Create list of unique hashed_ids and add column for Y/N for visited their nearest park (using the two different metrics calculated, Euclidean distance and network distance)
  # Separately point locations (home locations) for all hashed_ids in the dataset
  # join the list of visits to point locations
  
  # Network distance (closest facility), park_ID is for the original data, polygon_id is for the actual visits (new dataset)
  # Euclidean distance, NEAR_FID is the original data, polygon_id is for the actual visits (new dataset)
  
  # EUCLIDEAN DISTANCE ONLY FOR NOW
  # theoretical closest park ID
  # actual closest park ID (Euclidean)
  
  # Identify the IDs of those users who visited the closest theoretical park to them
  NPVR_calc = merged_table
  
  NPVR_calc_visited_park = NPVR_calc[NPVR_calc$plyg_ID == NPVR_calc$polygon_id,]
  
  # Add a column to track if a user is included
  NPVR_calc_visited_park$included = rep(1, length(NPVR_calc_visited_park$hashd_d))
  
  # full list of all hashed_ids in the study site (regardless of whether they visited any park)
  users_list_innercity = data.frame(
    hashed_id = clipped_hashed_id
  )
  
  # add information on whether a user visited closest theoretical park or not
  users_list_innercity_join = merge(users_list_innercity, NPVR_calc_visited_park[,c(1,23,56,62)], all.x = TRUE,
                               by.x = "hashed_id", by.y = "hashd_d")
  
  
  # if user did not visit closest theoretical park (NA), Euclidean distance, assign value of 0 else 1
  users_list_innercity_join$visited_park_euclidean = ifelse(users_list_innercity_join$included %in% c(NA), 0, 1)
  
    # join the grid id to the df, based on the hashed_id and the home location of that user
  users_list_innercity_join_grid_id = merge(users_list_innercity_join, merged_table[,c(1,24)],
                                                                by.x = "hashed_id", by.y = "hashd_d", all.x = TRUE)
  
  # calculate the NPVR
  # by grid cell, calculate the count of unique users that visited their closest theoretical park, and the total number of users in the grid cell
  # divide one by the other to get the proportion
  NPVR_by_grid_id = users_list_innercity_join_grid_id %>%
    group_by(id_1) %>%
    summarise(
      total_rows = n(),
      count_visit_theoretical_park = sum(visited_park_euclidean == 1, na.rm = TRUE)
    )
  
  NPVR_by_grid_id$NPVR_euclidean = NPVR_by_grid_id$count_visit_theoretical_park/NPVR_by_grid_id$total_rows
  
  colnames(NPVR_by_grid_id)[1] = "grid_id"
  
  # save the result to a table which when combined with the grid cell geometries can be mapped
  write.csv(NPVR_by_grid_id, paste0("Output/Output_", input_datasets[[i]], "/homeloc_", input_datasets[[i]], "_NPVR_averages_grid_id.csv"))

  
}


# end of script

stopCluster(cl)
