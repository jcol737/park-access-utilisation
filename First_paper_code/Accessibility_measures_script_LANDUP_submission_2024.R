#### Accessibility measures ####
# R Script to accompany paper submission titled 
# "Measuring spatial inequality of urban park accessibility and utilisation: A case study of public housing developments in Auckland, New Zealand"
# 2024

# load packages
library(dplyr)
library(accessibility)
library(sf)
library(sp)
library(ggplot2)
library(plotly)
library(tmap)
library(rgdal)
library(stats)
library(RColorBrewer)
library(ggridges)
library(hrbrthemes)
library(viridis)

#### Load data ####
# SA2 2018 OD matrix (centroid to centroid)
# calculated in ArcMap using Stats NZ Statistical Area 2 2018 polygon centroids, distance walking between each polygon centroid to all other polygon centroids in the dataset
# clipped to those SA2s within the Auckland Regional Council boundaries
OD_matrix_SA2_raw = st_read("OD_matrix_walking_SA2_2018_raw_centroids_Akld_RC.shp")

# format the OD matrix
input_matrix = as.data.frame(OD_matrix_SA2_raw)[,c(7,8,5)]
colnames(input_matrix) = c("from_id", "to_id", "travel_distance")

# Count and area of parks intersecting each SA2 2018 polygon
SA2_polygons_pop_parks = st_read("SA2_2018_polygons_Akld_RC.shp")

land_use_data = as.data.frame(SA2_polygons_pop_parks)[,c(1,27,36:39)]
colnames(land_use_data) = c("id", "population", "parks_sum_area", "parks_mean_area", "parks_sum_area_shape", "parks_count")

spatial_data_raw <- merge(SA2_polygons_pop_parks, land_use_data, by.x = "SA22018_V1", by.y = "id")


#### Accessibility measures ####
# parks summed area
negative_exp_summed_area <- gravity(
  input_matrix,
  land_use_data,
  opportunity = "parks_sum_area",
  travel_cost = "travel_distance",
  decay_function = decay_exponential(decay_value = 0.2)
)

# parks count
negative_exp_count <- gravity(
  input_matrix,
  land_use_data,
  opportunity = "parks_count",
  travel_cost = "travel_distance",
  decay_function = decay_exponential(decay_value = 0.2)
)

# join all the output onto the land use df and export
accessibility_calcs_by_SA2 = land_use_data

accessibility_calcs_by_SA2$negative_exp_summed_area = negative_exp_summed_area$parks_sum_area
accessibility_calcs_by_SA2$negative_exp_count = negative_exp_count$parks_count

write.csv(accessibility_calcs_by_SA2, "accessibility_calcs_by_SA2.csv", row.names = FALSE)



