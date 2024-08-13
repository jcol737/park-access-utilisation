#### Finding the most likely home locations of users ####
# R Script to accompany paper submission titled 
# "Measuring spatial inequality of urban park accessibility and utilisation: A case study of public housing developments in Auckland, New Zealand"
# 2024

# set wd as required
#setwd("DIRECTORY/LINK/HERE")

# import libraries
library(foreign)
library(rgdal)
library(sf)
library(sp)
library(dplyr)
library(devtools)
library(lubridate)
library(tidyverse)
library(here)
library(ggplot2)
#library(dbscan)
library(fpc)
library(factoextra)

require('RPostgreSQL')
library('RPostgres')
library('DBI')

# connect to dataset in PostgreSQL
db = 'DATABASE_NAME'
host_db = 'localhost'
db_port = 5432
db_user = 'postgres'
db_password = 'PASSWORD'

con = dbConnect(RPostgres::Postgres(),
                dbname = db,
                host = host_db,
                port = db_port,
                user = db_user,
                password = db_password)


#### All users ####
# input dataset is of point trajectories of users within the Auckland Regional Council boundaries
# for the first week of March 2020
# the dataset is structured as follows:
# columns:
# gid [row id]
# nzl [country of collection, New Zealand]
# hashed_id [Unique device ID]
# latitude [latitude coordinate of the point in WGS 1984 projection]
# longitude [longitude coordinate of the point in WGS 1984 projection]
# unix_timestamp [date and timestamp of the point collection in unix format]
# geom [raw geometry column (string)]
# geom_final [geometry column when loaded into PostgreSQL as a spatial dataset]

# run the code for clustering and saving the median centroid coordinates of the first cluster for ALL users
# those users which meet our criteria (number = 22,214 users)
# set the input parameters of the function outside the loop
eps = 50
MinPts = 100
run_num = 1

all_users = c(1:22214)

start <- Sys.time()
print(paste("Starting at", start))

for (i in seq_along(all_users)){
  # import the distinct points for the unique user from the DB
  user_traj <- dbGetQuery(con, paste0("SELECT *
  FROM march20firstweek_nzgd_times_gridid
WHERE hashed_id IN (
  SELECT hashed_id
    FROM march20firstweek_nzgd_gridsubset_final_desc_count
  LIMIT 1 OFFSET ", i - 1, " )")) #i
  
  # extract out the geometry
  user_traj_geometry_cols = st_as_sfc(user_traj$geom_final)
  
  # convert time
  user_traj$datetime_NZ = as.POSIXct(as.numeric(as.character(user_traj$time)), origin="1970-01-01", tz="Pacific/Auckland")
  
  # set seed for DBSCAN
  set.seed(123)
  
  # pull out coordinates for NZGD projection
  user_traj_geometry = user_traj
  user_traj_geometry$lat_nzgd = rep(0, length(user_traj_geometry$lat))
  user_traj_geometry$long_nzgd = rep(0, length(user_traj_geometry$long))
  
  for (j in seq_along(user_traj_geometry$lat)){
    user_traj_geometry$lat_nzgd[j] = user_traj_geometry_cols[[j]][1]
    user_traj_geometry$long_nzgd[j] = user_traj_geometry_cols[[j]][2]
  }
  
  # just pull out the coordinate columns
  user_traj_cols = user_traj_geometry[,c(12,13)]
  
  # DBSCAN
  user_traj_db <- fpc::dbscan(user_traj_cols, eps = eps, MinPts = MinPts)
  
  # save visualisation of results into rds file
  plot_DBSCAN_result = fviz_cluster(user_traj_db, user_traj_cols, geom = "point")
  saveRDS(plot_DBSCAN_result, paste0("./DBSCAN_all_users/", "user", i, "_traj_db_join_nonoise_run", run_num, "_plot.rds"))
  
  # save output and centroids
  user_traj_db_join = user_traj_geometry
  user_traj_db_join$cluster_num = user_traj_db$cluster
  user_traj_db_join$isseed = user_traj_db$isseed
  
  # exclude noise (cluster_num != 0)
  user_traj_db_join_nonoise = user_traj_db_join[user_traj_db_join$cluster_num != 0, ]
  
  # export to visualise in QGIS
  write.csv(user_traj_db_join_nonoise, paste0("./DBSCAN_all_users/", "user", i, "_traj_db_join_nonoise_run", run_num, ".csv"), row.names = FALSE)
  
  # find centroid of points in each cluster
  # average of the coordinates in each cluster
  # columns 4 and 5 are coordinates
  user_traj_db_join_nonoise_centroids = data.frame(Cluster_ID = unique(user_traj_db_join_nonoise$cluster_num),
                                                   Avg_lat_median = rep(0, length(unique(user_traj_db_join_nonoise$cluster_num))),
                                                   Avg_long_median = rep(0, length(unique(user_traj_db_join_nonoise$cluster_num))),
                                                   Avg_lat_mean = rep(0, length(unique(user_traj_db_join_nonoise$cluster_num))),
                                                   Avg_long_mean = rep(0, length(unique(user_traj_db_join_nonoise$cluster_num))))
  
  user_cluster_IDs = unique(user_traj_db_join_nonoise$cluster_num)
  for (j in seq_along(user_cluster_IDs)){
    # select subset of coordinates for the cluster
    subset_coords = user_traj_db_join_nonoise[user_traj_db_join_nonoise$cluster_num == user_cluster_IDs[j],] #j
    # calculate the average lat and long of these coordinates
    avg_lat_median = median(subset_coords$lat_nzgd)
    avg_long_median = median(subset_coords$long_nzgd)
    avg_lat_mean = mean(subset_coords$lat_nzgd)
    avg_long_mean = mean(subset_coords$long_nzgd)
    # add output to the df
    user_traj_db_join_nonoise_centroids$Avg_lat_median[j] =  avg_lat_median
    user_traj_db_join_nonoise_centroids$Avg_long_median[j] =  avg_long_median
    user_traj_db_join_nonoise_centroids$Avg_lat_mean[j] =  avg_lat_mean
    user_traj_db_join_nonoise_centroids$Avg_long_mean[j] =  avg_long_mean
    
  }
  
  # export to visualise in QGIS
  write.csv(user_traj_db_join_nonoise_centroids, paste0("./DBSCAN_all_users/", "user", i, "_traj_db_join_nonoise_centroids_run", run_num, ".csv"), row.names = FALSE)
  
}

## Extract median centroid coordinates for first cluster for all users ##
# import the csv files into a list
# then create a df which has the correct # of rows
# then pull out the median centroid coordinates and add to the df

require(data.table)
files = list.files(path = './DBSCAN_all_users/', pattern = 'csv$')

# file names subset
files_centroids = grep('user[0-9]+_traj_db_join_nonoise_centroids_run[0-9]+.csv', files)
files_centroids_names = files[files_centroids]

# add full path
files_wPath = paste0(getwd(), "./DBSCAN_all_users/", files_centroids_names)
files_wPath = gsub("\\/", "\\//", files_wPath)

# read in the files
centroid_dataset_list = lapply(files_wPath, function (x) read.csv(x, header = TRUE, stringsAsFactors = FALSE))

# need to name these to distinguish
file_names = gsub(".csv$", "", files_centroids_names)
names(centroid_dataset_list) = file_names

input_datasets_names = file_names
input_datasets = centroid_dataset_list
df_length = length(files_centroids_names)

# empty dataframe to fill with median centroid coordinates for the first cluster
# length based on the length of the input files (number of csv files)
median_centroids_first_cluster_df = data.frame(ID = 1:df_length,
                                               user_ID = 1:df_length,
                                               Avg_lat_median = 1:df_length,
                                               Avg_long_median = 1:df_length,
                                               Avg_lat_mean = 1:df_length,
                                               Avg_long_mean = 1:df_length,
                                               file_name = rep("NA", df_length))

for (i in seq_along(input_datasets)){
  # choose the user dataset
  user_dataset = input_datasets[[i]]
  
  # pull out the coordinates, and file name, and add to df
  median_centroids_first_cluster_df[i,3:6] = user_dataset[1,2:5]
  median_centroids_first_cluster_df[i,7] = input_datasets_names[i]
  
}

# extract user number
median_centroids_first_cluster_df$user_ID = gsub("user([0-9]+)_traj.+", "\\1", median_centroids_first_cluster_df$file_name)

# export results
write.csv(median_centroids_first_cluster_df, "median_centroids_first_cluster_all_users.csv", row.names = FALSE)

# time to finish
print(paste("Ended at", Sys.time()))
print(paste("Ended at", Sys.time(), "took", round(difftime(Sys.time(), start, units = "secs")), "seconds"))


