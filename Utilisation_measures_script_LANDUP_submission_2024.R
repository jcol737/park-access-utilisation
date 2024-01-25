#### Park Utilisation measures ####
# R Script to accompany paper submission titled 
# "Measuring spatial inequality of urban park accessibility and utilisation: A case study of public housing developments in Auckland, New Zealand"
# 2024

# load packages
library(sp)
library(sf)
library(rgdal)
library(dplyr)
library(ggplot2)
library(plotly)
library(lubridate)

#### Load in the data ####
# list of unique user hashed_id keys
user_hashed_id_key = read.csv("all_user_hashed_ids_desc_row.csv", header = TRUE)

# list of parks visited within the Auckland Regional Council boundaries
# parks visited at least once by at least one user (at least one user point in their trajectory intersects with park polygon)
# dataset lists which user visited the park, and for their home location the Euclidean distance and network distance from the park visited to their home location
new_calc = readOGR("parks_visited_join_home_locations_clip_Akld_RC_euc_dist_centroids_network_analysis.shp")
# contains the new metrics (new three metrics -> UPO, LPV and NPVR) for all park visits, for all of the RC not just the inner city study site

# study site is referred to as "inner city" throughout the script
# dataset of all users with a home location within the study site, clipped to the non built up areas and excluding airports
# theoretical closest park using network distance (facility) and Euclidean distance, for park centroid only
innercity_facility = readOGR("all_users_innercity_clip_closest_facility_LCDB.shp")
innercity_parkcentroid = readOGR("all_users_innercity_clip_closest_park_centroid_LCDB.shp")


#### Data Preparation ####
# join datasets based on user ID
# first extract out the key columns, and filter the rows to the minimum park, one point (the closest visited park)
new_calc.df = new_calc@data
innercity_facility.df = innercity_facility@data
innercity_parkcentroid.df = innercity_parkcentroid@data

new_calc_filtered.df = new_calc.df[,c(1:15, 24, 29:30)]

# minimum distance per unique hashed_id (user) to park actually visited
# euclidean distance (Length_met)
new_calc_filtered_selected_euc.df = new_calc_filtered.df %>%
  select(hashed_id, polygon_id, user_ID, Length_met)

new_calc_filtered_euc.df = new_calc_filtered_selected_euc.df %>%
  group_by(user_ID) %>%
  summarise(
    MinLength_metByUser = min(Length_met, na.rm = T)
  ) %>%
  arrange(user_ID)

# repeat for network distance (Total_Leng)
new_calc_filtered_selected_network.df = new_calc_filtered.df %>%
  select(hashed_id, polygon_id, user_ID, Total_Leng)

new_calc_filtered_network.df = new_calc_filtered_selected_network.df %>%
  group_by(user_ID) %>%
  summarise(
    MinTotal_LengByUser = min(Total_Leng, na.rm = T)
  ) %>%
  arrange(user_ID)


#### Difference between minimum actual visited park distance and theoretical closest park distance, DAT ####
# compare minimum distances for parks actually visited to those minimum distance calculated previously (theoretical closest distance)
# filter input dataset to essential columns required for the calculation
innercity_facility_filtered.df = innercity_facility.df[,c(1:8,18)]
innercity_parkcentroid_filtered.df = innercity_parkcentroid.df

# add column to innercity_facility_filtered.df for park ID
# extract from the Name column
innercity_facility_filtered.df$park_ID = gsub("Location [0-9]+ - Location ", "", innercity_facility.df$Name)

# join
innercity_facility_filtered_join.df = merge(innercity_facility_filtered.df, new_calc_filtered_network.df)
innercity_parkcentroid_filtered_join.df = merge(innercity_parkcentroid_filtered.df, new_calc_filtered_euc.df)

# calculate differences between minimum distance to park actually visited and minimum distance to any park
# network distance, park centroid
innercity_facility_filtered_join.df$diff = innercity_facility_filtered_join.df$MinTotal_LengByUser - innercity_facility_filtered_join.df$Total_Leng
#hist(innercity_facility_filtered_join.df$diff)
#summary(innercity_facility_filtered_join.df$diff)

# Euclidean distance, park centroid
innercity_parkcentroid_filtered_join.df$diff = innercity_parkcentroid_filtered_join.df$MinLength_metByUser - innercity_parkcentroid_filtered_join.df$NEAR_DIST
#hist(innercity_parkcentroid_filtered_join.df$diff)
#summary(innercity_parkcentroid_filtered_join.df$diff)


#### Used Park Opportunities ####
# Number of parks visited divided by the number of parks available (within 1km, 2km, 5km)  - calculate it per person and then average per grid.
# for the centroid of the park, or the edge of the polygon for the park

# 1km, 2km and 5km buffer circles calculated in ArcGIS Pro for each user home location
# load in the shapefiles
users_innercity_1km_buffer = readOGR("all_users_innercity_clip_LCDB_nocalc_1km_buffer.shp")
users_innercity_2km_buffer = readOGR("all_users_innercity_clip_LCDB_nocalc_2km_buffer.shp")
users_innercity_5km_buffer = readOGR("all_users_innercity_clip_LCDB_nocalc_5km_buffer.shp")

# park points visited by a user, for specific user ID
park_visits_joined_home_locations_Akld_RC = readOGR("parks_visited_join_home_locations_clip_Akld_RC.shp")

# STEP ONE - Calculate the numerator, number of parks visited within distance threshold buffer #
# for unique hashed_id in list of all unique users in the study site, extract out the respective buffer from the shapefile and all the park visit points with the same user ID
# then run selection (within/point in polygon) so only those points in the buffer polygon are included
# then calculate the number of unique polygon IDs and add the output number to the new dataframe to use in further calculations

# create list of unique user IDs within the study site
users_list_innercity = innercity_facility@data[,c(1,2)]
# join hashed_ids to the list
users_list_innercity = merge(users_list_innercity, user_hashed_id_key, all.x = TRUE, by.x = "user_ID", by.y = "user_id")

# new dataframe to fill with results
users_list_innercity_index_to_fill = users_list_innercity
users_list_innercity_index_to_fill$number_parks_visited_1km = rep(0, length(users_list_innercity$user_ID))
users_list_innercity_index_to_fill$number_parks_visited_2km = rep(0, length(users_list_innercity$user_ID))
users_list_innercity_index_to_fill$number_parks_visited_5km = rep(0, length(users_list_innercity$user_ID))

# loop through the hashed_ids and run step one calculations
for (i in 1:length(users_list_innercity$user_ID)) {
  
  # buffer subset for user
  user_id_1km_buffer = users_innercity_1km_buffer[users_innercity_1km_buffer$user_ID %in% users_list_innercity$user_ID[i],]
  user_id_2km_buffer = users_innercity_2km_buffer[users_innercity_2km_buffer$user_ID %in% users_list_innercity$user_ID[i],]
  user_id_5km_buffer = users_innercity_5km_buffer[users_innercity_5km_buffer$user_ID %in% users_list_innercity$user_ID[i],]
  
  # extract subset of user visited park points for the user id
  user_id_parks_visited_points = park_visits_joined_home_locations_Akld_RC[park_visits_joined_home_locations_Akld_RC$user_ID %in% 
                                                                             users_list_innercity$user_ID[i],]
  
  if (!length(user_id_parks_visited_points$hashed_id) %in% 0) {
    
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
    users_list_innercity_index_to_fill$number_parks_visited_1km[i] = n_parks_visited_1km
    users_list_innercity_index_to_fill$number_parks_visited_2km[i] = n_parks_visited_2km
    users_list_innercity_index_to_fill$number_parks_visited_5km[i] = n_parks_visited_5km
    
  } else {
    
    # add to the dataframe under the respective column
    users_list_innercity_index_to_fill$number_parks_visited_1km[i] = 0
    users_list_innercity_index_to_fill$number_parks_visited_2km[i] = 0
    users_list_innercity_index_to_fill$number_parks_visited_5km[i] = 0
    
  }
  
}


# STEP TWO - Calculate the denominator, the number of parks available within the distance threshold #
# load in the parks polygons and centroids
parks_Akld_RC_poly = readOGR("ParkExtent_Parks_Reg_10mill_sqm_Akld_RC_clip.shp")
parks_Akld_RC_points = readOGR("ParkExtent_Parks_Reg_10mill_sqm_Akld_RC_clip_points.shp")

# convert to 2193 CRS
parks_Akld_RC_poly = spTransform(parks_Akld_RC_poly, users_innercity_1km_buffer@proj4string)
parks_Akld_RC_points = spTransform(parks_Akld_RC_points, users_innercity_1km_buffer@proj4string)

# dataframe to fill, for part two
users_list_innercity_index_to_fill_pt2 = users_list_innercity_index_to_fill
users_list_innercity_index_to_fill_pt2$number_parks_available_1km = rep(0, length(users_list_innercity$user_ID))
users_list_innercity_index_to_fill_pt2$number_parks_available_2km = rep(0, length(users_list_innercity$user_ID))
users_list_innercity_index_to_fill_pt2$number_parks_available_5km = rep(0, length(users_list_innercity$user_ID))

users_list_innercity_index_to_fill_pt2$number_parks_available_1km_centroids = rep(0, length(users_list_innercity$user_ID))
users_list_innercity_index_to_fill_pt2$number_parks_available_2km_centroids = rep(0, length(users_list_innercity$user_ID))
users_list_innercity_index_to_fill_pt2$number_parks_available_5km_centroids = rep(0, length(users_list_innercity$user_ID))

# loop through the hashed_ids and run step two calculations
for (i in 1:length(users_list_innercity$user_ID)) {
  
  # buffer subset for user
  user_id_1km_buffer = users_innercity_1km_buffer[users_innercity_1km_buffer$user_ID %in% users_list_innercity$user_ID[i],]
  user_id_2km_buffer = users_innercity_2km_buffer[users_innercity_2km_buffer$user_ID %in% users_list_innercity$user_ID[i],]
  user_id_5km_buffer = users_innercity_5km_buffer[users_innercity_5km_buffer$user_ID %in% users_list_innercity$user_ID[i],]
  
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
  users_list_innercity_index_to_fill_pt2$number_parks_available_1km[i] = n_parks_available_polygons_1km
  users_list_innercity_index_to_fill_pt2$number_parks_available_2km[i] = n_parks_available_polygons_2km
  users_list_innercity_index_to_fill_pt2$number_parks_available_5km[i] = n_parks_available_polygons_5km
  
  
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
  users_list_innercity_index_to_fill_pt2$number_parks_available_1km_centroids[i] = n_parks_available_centroids_1km
  users_list_innercity_index_to_fill_pt2$number_parks_available_2km_centroids[i] = n_parks_available_centroids_2km
  users_list_innercity_index_to_fill_pt2$number_parks_available_5km_centroids[i] = n_parks_available_centroids_5km
  
  print(paste0("User ", i, " (User ID: ", users_list_innercity$user_ID[i], ")"))
  
}

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
write.csv(users_list_innercity_index_to_fill_pt3, "UPO_output.csv", row.names = FALSE)


#### Locality of Park Visits ####
# Number of unique parks visited (1km, 2km, 5km versions) divided by the number of unique parks visited any distance from home location
# if 0 in the denominator the LPV will be 0

# the same code from the UPO is reused, but modified to have a consistent denominator for each distance threshold (the count of unique parks visited by the user, anywhere in the Auckland Regional Council boundaries)

## STEP ONE ##
# step one of the calculation is the same
users_list_innercity_index_to_fill_cons_denom = users_list_innercity_index_to_fill

## STEP TWO ##
# step two (denominator) is consistent for all, so only need to add one column
users_list_innercity_index_to_fill_cons_denom$number_unique_parks_visited_all = rep(0, length(users_list_innercity$user_ID))

# loop through the hashed_ids and run step one calculations
for (i in 1:length(users_list_innercity$user_ID)) {
  
  # extract subset of user visited park points for the user id
  user_id_parks_visited_points = park_visits_joined_home_locations_Akld_RC[park_visits_joined_home_locations_Akld_RC$user_ID %in% 
                                                                             users_list_innercity$user_ID[i],]
  
  if (!length(user_id_parks_visited_points$hashed_id) %in% 0) {
    
    # count the number of unique polygon ids
    # check what the name of the polygon id column is
    n_parks_visited_all = length(unique(user_id_parks_visited_points$polygon_id))
    
    # add to the dataframe under the respective column
    users_list_innercity_index_to_fill_cons_denom$number_unique_parks_visited_all[i] = n_parks_visited_all
    
  } else {
    
    # add to the dataframe under the respective column
    users_list_innercity_index_to_fill_cons_denom$number_unique_parks_visited_all[i] = 0
    
  }
  
  print(paste0("User ", i, " (User ID: ", users_list_innercity$user_ID[i], ")"))
  
}

## STEP THREE ##
# divide all the columns from part one by the same denominator column
users_list_innercity_index_to_fill_cons_denom$index_1km_cons_denom = users_list_innercity_index_to_fill_cons_denom$number_parks_visited_1km
users_list_innercity_index_to_fill_cons_denom$index_2km_cons_denom = users_list_innercity_index_to_fill_cons_denom$number_parks_visited_2km
users_list_innercity_index_to_fill_cons_denom$index_5km_cons_denom = users_list_innercity_index_to_fill_cons_denom$number_parks_visited_5km

users_list_innercity_index_to_fill_cons_denom[,c(8:10)] = users_list_innercity_index_to_fill_cons_denom[,c(8:10)]/users_list_innercity_index_to_fill_cons_denom$number_unique_parks_visited_all

# export to csv for visualisation
write.csv(users_list_innercity_index_to_fill_cons_denom, "LPV_output.csv", row.names = FALSE)



#### Nearest Park Visitation Rate ####
# Proportion of users that have a home location within a specific grid cell that visited their closest theoretical park
# Number of users who live (have home location within) in a particular grid cell that visited their closest theoretical park divided by the total number of users who live (have home location within) within the grid cell

# calculate for both Euclidean distance and network distance

## STEP ONE ##
# Using dataset from DAT with list of parks visited by a user, with the hashed_id of the user who visited the park and the distance between the park centroid and the home location of the user (using either Euclidean distance or network distance)
# calculate how many users visited the park closest to them (based on the park ID)
# need to re-extract the minimum distance park, park ID

# Euclidean distance
new_calc_filtered_euc_v2.df = new_calc_filtered_selected_euc.df %>%
  group_by(user_ID) %>%
  slice_min(n = 1, Length_met) %>%
  distinct()

# Network distance
new_calc_filtered_network_v2.df = new_calc_filtered_selected_network.df %>%
  group_by(user_ID) %>%
  slice_min(n = 1, Total_Leng) %>%
  distinct()

# join again, and then compare the IDs of the polygons (parks)
# comparing between the park ID of the minimum distance park actual and theoretical
innercity_facility_filtered_join_v2.df = merge(innercity_facility_filtered.df, as.data.frame(new_calc_filtered_network_v2.df), by.x = "user_ID", by.y = "user_ID")
innercity_parkcentroid_filtered_join_v2.df = merge(innercity_parkcentroid_filtered.df, as.data.frame(new_calc_filtered_euc_v2.df), by.x = "user_ID", by.y = "user_ID")

# facility, park_ID is for the original data, polygon_id is for the actual visits (new dataset)
# euclidean, NEAR_FID is the original data (may need +1 as index started with 0), polygon_id is for the actual visits (new dataset)
# network (facility)
#length(innercity_facility_filtered_join_v2.df[innercity_facility_filtered_join_v2.df$park_ID == innercity_facility_filtered_join_v2.df$polygon_id,]$user_ID)

# prep
innercity_parkcentroid_filtered_join_v2.df$NEAR_FID_num = as.numeric(innercity_parkcentroid_filtered_join_v2.df$NEAR_FID) + 1
innercity_parkcentroid_filtered_join_v2.df$polygon_id_num = as.numeric(innercity_parkcentroid_filtered_join_v2.df$polygon_id)
length(innercity_parkcentroid_filtered_join_v2.df[innercity_parkcentroid_filtered_join_v2.df$NEAR_FID_num == innercity_parkcentroid_filtered_join_v2.df$polygon_id_num,]$user_ID)

# Euclidean distance
#length(innercity_parkcentroid_filtered_join_v2.df[innercity_parkcentroid_filtered_join_v2.df$NEAR_FID_num == innercity_parkcentroid_filtered_join_v2.df$polygon_id_num,]$user_ID)


## STEP TWO ##
# Create list of unique hashed_ids and add column for Y/N for visited their nearest park (using the two different metrics calculated, Euclidean distance and network distance)
# Separately point locations (home locations) for all hashed_ids in the dataset
# join the list of visits to point locations

# Network distance (closest facility), park_ID is for the original data, polygon_id is for the actual visits (new dataset)
# Euclidean distance, NEAR_FID is the original data, polygon_id is for the actual visits (new dataset)

# Identify the IDs of those users who visited the closest theoretical park to them
visited_park_innercity_facility = innercity_facility_filtered_join_v2.df[innercity_facility_filtered_join_v2.df$park_ID == innercity_facility_filtered_join_v2.df$polygon_id,]
visited_park_innercity_euclidean = innercity_parkcentroid_filtered_join_v2.df[innercity_parkcentroid_filtered_join_v2.df$NEAR_FID_num == innercity_parkcentroid_filtered_join_v2.df$polygon_id_num,]

# join these to a list of hashed_ids for total dataset
users_list_innercity = innercity_facility@data[,c(1,2)]
# join hashed_ids to the list
users_list_innercity = merge(users_list_innercity, user_hashed_id_key, all.x = TRUE, by.x = "user_ID", by.y = "user_id")

# add the information on whether user visited closest theoretical park or not
users_list_innercity_join_euclidean_visited = merge(users_list_innercity, visited_park_innercity_euclidean[,c(1,2,14)], all.x = TRUE, 
                                                    #all.y = TRUE, 
                                                    by.x = "user_ID", by.y = "user_ID")
# if user did not visit closest theoretical park (NA), Euclidean distance, assign value of 0 else 1
users_list_innercity_join_euclidean_visited$visited_park_euclidean = ifelse(users_list_innercity_join_euclidean_visited$polygon_id_num %in% c(NA), 0, 1)

users_list_innercity_join_euclidean_and_network_visited = merge(users_list_innercity_join_euclidean_visited, visited_park_innercity_facility[,c(1,2,12)],
                                                                all.x = TRUE, 
                                                                #all.y = TRUE, 
                                                                by.x = "user_ID", by.y = "user_ID")
# if user did not visit closest theoretical park (NA), network distance, assign value of 0 else 1
users_list_innercity_join_euclidean_and_network_visited$visited_park_network = ifelse(users_list_innercity_join_euclidean_and_network_visited$polygon_id %in% c(NA), 0, 1)

# clean up the file for export
users_list_innercity_join_euclidean_and_network_visited_tidy = users_list_innercity_join_euclidean_and_network_visited[,c(1,3,6,9)]
# list of each user hashed_id who have a home location in the inner city, and columns for whether they visited their closest theoretical park, either using Euclidean distance or network distance
write.csv(users_list_innercity_join_euclidean_and_network_visited_tidy, "hashed_ids_innercity_visited_nearest_park_euc_network.csv", row.names = FALSE)

# this output file is loaded into ArcGIS Pro, and the proportion of users within each grid cell that visited their closest theoretical park is calculated
# per grid cell sum of count of unique users (hashed_ids) that visited closest theoretical park divided by the total number of unique users (hashed_ids) in each grid cell, based on the home location of the user


