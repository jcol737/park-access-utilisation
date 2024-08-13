####Finding the most likely home locations of users ####
# R Script for subsequent analysis following publication of paper
# "Measuring spatial inequality of urban park accessibility and utilisation: A case study of public housing developments in Auckland, New Zealand"
# 2024
# Jessie Colbert, Katarzyna Sila-Nowicka, I-Ting Chuang, The University of Auckland

## Note: the inclusion criteria for user trajectories has been updated to users having a minimum of 12 points in at least two days anywhere in the dataset ##

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

library(foreach)

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



#### Full users, minpts 50, parallel processing ####
# input datasets are a set of point trajectories for users within the Auckland Regional Council boundaires
# six datasets are run, for different time periods
# the datasets all have the same structure as follows:
# columns:
# gid [row id]
# nzl [country of collection, New Zealand]
# hashed_id [Unique device ID]
# latitude [latitude coordinate of the point in WGS 1984 projection]
# longitude [longitude coordinate of the point in WGS 1984 projection]
# unix_timestamp [date and timestamp of the point collection in unix format]
# geom [raw geometry column (string)]
# geom_final [geometry column when loaded into PostgreSQL as a spatial dataset]

# prepare input lists for iterating the code over the input datasets
# the raw user trajectory tables in PGAdmin
input_tables = list(
  "auckland_traj_18_03_27_03_updated_nzgd",
  "auckland_traj_20_28_04_updated_nzgd",
  "auckland_traj_25_26_03_updated_nzgd",
  "auckland_traj_28_4_05_05_updated_nzgd",
  "auckland_traj_2703004_updated_nzgd",
  "auckland_traj_04_03_20_updated_nzgd"
)

# the tables listing the IDs of the users that meet the inclusion criteria
input_tables_users = list(
  "user_traj_18_03_27_03_hashed_id_12pt_inclusion",
  "user_traj_20_28_04_hashed_id_12pt_inclusion",
  "user_traj_25_26_03_hashed_id_12pt_inclusion",
  "user_traj_28_4_05_05_hashed_id_12pt_inclusion",
  "user_traj_2703004_hashed_id_12pt_inclusion",
  "user_traj_04_03_20_hashed_id_12pt_inclusion"
)

# run the code for clustering and saving the medican centroid coordinates for the first cluster for all users who meet the criteria

# set the input parameters of the function outside the loop
eps = 50
MinPts = 50
run_num = 1

library(doParallel)
num_cores = 6
cl = makePSOCKcluster(6)
registerDoParallel(cl)


fetchData = function(ignore) {
  con
}

clusterEvalQ(cl, {
  library(DBI)
  library(RPostgreSQL)
  con = dbConnect(RPostgres::Postgres(),
                  dbname = 'Second_park_use_paper',
                  host = 'localhost',
                  port = 5432,
                  user = 'postgres',
                  password = 'Tourism_SH')
  NULL
})


# run the for loop in parallel to iterate over the input datasets, loading in the required packages
foreach(i = 1:length(input_tables),
        .noexport="con",
        .verbose=TRUE,
        .packages = c("DBI", "RPostgreSQL", "lubridate", "sf", "dplyr", "factoextra")) %dopar% {
          
          # list to store the query result
          query_result = list()
          
          # load the csv with the user hashed_ids
          input_user_IDs = read.csv(paste0(input_tables_users[i], ".csv"))
          
          # input unique rows
          unique_row_count = length(unique(input_user_IDs$hashed_id))
          
          # vector of the number of users that meet the criteria, to iterate over and extract the points for these users
          user_num = 1:unique_row_count
          
          # extract out the query result from the table i
          for (j in seq_along(user_num)){
            
            fetchData(i)
            
            query = paste0(
              "SELECT * FROM ", input_tables[[i]],
              " WHERE hashed_id = '", input_user_IDs$hashed_id[j], "'"
            )
            
            # run query and store the result in a data frame
            traj = DBI::dbGetQuery(con, query)
            
            # subset the points to those with a timestamp between midnight and 6am
            traj = traj[hour(traj$timestamp) >= 0 & hour(traj$timestamp) < 6,]
            
            # append the results to the list
            query_result[[j]] = traj
            
            ## clustering ##
            # extract out the geometry columns for the user
            # working with WKB format
            geom = traj$geom_final
            
            # add a handle in case there are no points that are within the time limits for the specific user
            if (length(geom) %in% 0) {
              
              # skip to export an empty csv with just the header
              user_traj_db_join = data.frame(
                gid = integer(0),
                nzl = character(0),
                hashed_id = character(0),
                unix_timestamp = numeric(0),
                geom = character(0),
                geom_final = character(0),
                grid_id = integer(0),
                timestamp = numeric(0),
                day_of_the_week_num = numeric(0),
                geometry = numeric(0),
                long_nzgd = numeric(0),
                lat_nzgd = numeric(0),
                cluster_num = numeric(0)
              )
              
              
              # exclude noise (cluster_num != 0)
              user_traj_db_join_nonoise = user_traj_db_join[user_traj_db_join$cluster_num != 0, ]
              
              # export to visualise in QGIS
              write.csv(user_traj_db_join_nonoise, paste0("Results/", "table_", gsub("auckland_traj_|_updated_nzgd", "",input_tables[[i]]), "_user_", j, "_traj_db_join_nonoise_run_", run_num, ".csv"), row.names = FALSE)
              
              # skip to export an empty csv with just the header
              user_traj_db_join_nonoise_centroids = data.frame(Cluster_ID = unique(user_traj_db_join_nonoise$cluster_num),
                                                               Avg_lat_median = rep(0, length(unique(user_traj_db_join_nonoise$cluster_num))),
                                                               Avg_long_median = rep(0, length(unique(user_traj_db_join_nonoise$cluster_num))),
                                                               Avg_lat_mean = rep(0, length(unique(user_traj_db_join_nonoise$cluster_num))),
                                                               Avg_long_mean = rep(0, length(unique(user_traj_db_join_nonoise$cluster_num))),
                                                               user_number = rep(0, length(unique(user_traj_db_join_nonoise$cluster_num))),
                                                               hashed_id = rep(0, length(unique(user_traj_db_join_nonoise$cluster_num)))
              )
              
              # export to visualise in QGIS
              write.csv(user_traj_db_join_nonoise_centroids, paste0("Results/", "table_", gsub("auckland_traj_|_updated_nzgd", "",input_tables[[i]]), "_user_", j, "_traj_db_join_nonoise_centroids_run_", run_num, ".csv"), row.names = FALSE)
              
            } else {
              
              # use the WGS lat/long columns and convert to NZGD projection
              traj_geometry = st_as_sf(traj, coords = c("longitude", "latitude")) # WGS
              st_crs(traj_geometry) = 4326
              traj_geometry_nzgd = st_transform(traj_geometry, crs = 2193)
              
              # coordinate columns only
              traj_geometry_nzgd_cols = st_coordinates(traj_geometry_nzgd)
              
              # add coordinate columns to the df
              traj_geometry_nzgd$long_nzgd = st_coordinates(traj_geometry_nzgd)[,1]
              traj_geometry_nzgd$lat_nzgd = st_coordinates(traj_geometry_nzgd)[,2]
              
              
              ## DBSCAN ##
              # set seed for DBSCAN
              set.seed(123)
              
              user_traj_db = fpc::dbscan(traj_geometry_nzgd_cols, eps = eps, MinPts = MinPts)
              
              # save visualisation of results into rds file
              plot_DBSCAN_result = fviz_cluster(user_traj_db, traj_geometry_nzgd_cols, geom = "point")
              
              # save output and centroids
              user_traj_db_join = traj_geometry_nzgd
              user_traj_db_join$cluster_num = user_traj_db$cluster
              user_traj_db_join$isseed = user_traj_db$isseed
              
              # exclude noise (cluster_num != 0)
              user_traj_db_join_nonoise = user_traj_db_join[user_traj_db_join$cluster_num != 0, ]
              
              # export to visualise in QGIS
              write.csv(user_traj_db_join_nonoise, paste0("Results/", "table_", gsub("auckland_traj_|_updated_nzgd", "",input_tables[[i]]), "_user_", j, "_traj_db_join_nonoise_run_", run_num, ".csv"), row.names = FALSE)
              
              # find centroid of points in each cluster
              # average of the coordinates in each cluster
              # columns 4 and 5 are coordinates
              user_traj_db_join_nonoise_centroids = data.frame(Cluster_ID = unique(user_traj_db_join_nonoise$cluster_num),
                                                               Avg_lat_median = rep(0, length(unique(user_traj_db_join_nonoise$cluster_num))),
                                                               Avg_long_median = rep(0, length(unique(user_traj_db_join_nonoise$cluster_num))),
                                                               Avg_lat_mean = rep(0, length(unique(user_traj_db_join_nonoise$cluster_num))),
                                                               Avg_long_mean = rep(0, length(unique(user_traj_db_join_nonoise$cluster_num))),
                                                               user_number = rep(0, length(unique(user_traj_db_join_nonoise$cluster_num))),
                                                               hashed_id = rep(0, length(unique(user_traj_db_join_nonoise$cluster_num)))
              )
              
              user_cluster_IDs = unique(user_traj_db_join_nonoise$cluster_num)
              for (k in seq_along(user_cluster_IDs)){
                # select subset of coordinates for the cluster
                subset_coords = user_traj_db_join_nonoise[user_traj_db_join_nonoise$cluster_num == user_cluster_IDs[k],] #k
                # calculate the average lat and long of these coordinates
                avg_lat_median = median(subset_coords$lat_nzgd)
                avg_long_median = median(subset_coords$long_nzgd)
                avg_lat_mean = mean(subset_coords$lat_nzgd)
                avg_long_mean = mean(subset_coords$long_nzgd)
                # add output to the df
                user_traj_db_join_nonoise_centroids$Avg_lat_median[k] =  avg_lat_median
                user_traj_db_join_nonoise_centroids$Avg_long_median[k] =  avg_long_median
                user_traj_db_join_nonoise_centroids$Avg_lat_mean[k] =  avg_lat_mean
                user_traj_db_join_nonoise_centroids$Avg_long_mean[k] =  avg_long_mean
                
              }
              
              # add hashed id for the user for identification
              if (length(user_traj_db_join_nonoise_centroids$Cluster_ID) != 0) {
                user_traj_db_join_nonoise_centroids$user_number = j
                user_traj_db_join_nonoise_centroids$hashed_id = unique(user_traj_db_join_nonoise$hashed_id)
              }
              
              # export to visualise in QGIS
              write.csv(user_traj_db_join_nonoise_centroids, paste0("Results/", "table_", gsub("auckland_traj_|_updated_nzgd", "",input_tables[[i]]), "_user_", j, "_traj_db_join_nonoise_centroids_run_", run_num, ".csv"), row.names = FALSE)
              
              
            }
            
          }
          
          # for the selected table i, read in all the csv files for the users clusters
          
          ## Extract median centroid coordinates for first cluster for all users ##
          # import the csv files into a list
          # then create a df which has the correct number of rows
          # then pull out the median centroid coordinates and add to the df
          
          require(data.table)
          files = list.files(path = "Results/", pattern = 'csv$')
          
          # file names subset
          files_centroids = grep('._traj_db_join_nonoise_centroids_run_[0-9]+.csv', files)
          files_centroids_names = files[files_centroids]
          
          # further subset to only those within the currently selected table
          files_centroids_table = grep(gsub("auckland_traj_|_updated_nzgd", "",input_tables[[i]]), files_centroids_names)
          files_centroids_table_names = files_centroids_names[files_centroids_table]
          
          # add full path
          files_wPath = paste0("Results/", files_centroids_table_names)
          files_wPath = gsub("\\/", "\\//", files_wPath)
          
          # read in the files
          centroid_dataset_list = lapply(files_wPath, function (x) read.csv(x, header = TRUE, stringsAsFactors = FALSE))
          
          # need to name these to distinguish
          file_names = gsub(".csv$", "", files_centroids_table_names)
          names(centroid_dataset_list) = file_names
          
          input_datasets_names = file_names
          input_datasets = centroid_dataset_list
          df_length = length(files_centroids_table_names)
          
          # empty dataframe to fill with median centroid coordinates for the first cluster
          # length based on the length of the input files (number of csv files)
          median_centroids_first_cluster_df = data.frame(ID = 1:df_length,
                                                         user_ID = 1:df_length,
                                                         Avg_lat_median = 1:df_length,
                                                         Avg_long_median = 1:df_length,
                                                         Avg_lat_mean = 1:df_length,
                                                         Avg_long_mean = 1:df_length,
                                                         file_name = rep("NA", df_length),
                                                         hashed_id_output = rep("NA", df_length),
                                                         hashed_id = rep("NA", df_length))
          
          for (j in seq_along(input_datasets)){
            # choose the user dataset
            user_dataset = input_datasets[[j]]
            
            # pull out the coordinates, and file name, and add to df
            median_centroids_first_cluster_df[j,3:6] = user_dataset[1,2:5]
            median_centroids_first_cluster_df[j,7] = input_datasets_names[j]
            
            # add hashed_id from the output table
            median_centroids_first_cluster_df[j,8] = user_dataset[1,7]
            
          }
          
          # extract user number
          median_centroids_first_cluster_df$user_ID = gsub("table_[0-9|^0-9_]+_user_([0-9]+)_traj.+", "\\1", median_centroids_first_cluster_df$file_name)
          
          # add hashed id of the user from query_result for the NA users
          # run iteratively
          for (k in 1:nrow(median_centroids_first_cluster_df)) {
            ID = as.numeric(median_centroids_first_cluster_df[k,2]) #user_ID
            
            # extract the associated hashed_id from the query_result for the table
            hashed_id_extract = unique(query_result[[ID]]$hashed_id)
            
            if (length(hashed_id_extract) != 0) {
              
              median_centroids_first_cluster_df[k,9] = hashed_id_extract # hashed_id_output
              
            }
            
          }
          
          # check the hashed_ids are the same
          median_centroids_first_cluster_df$same_hashed_id = ifelse(median_centroids_first_cluster_df$hashed_id_output == median_centroids_first_cluster_df$hashed_id, TRUE, FALSE)
          
          table_length = length(median_centroids_first_cluster_df$ID)
          print(paste0("Table length is ", table_length))
          
          NA_length = length(median_centroids_first_cluster_df[median_centroids_first_cluster_df$same_hashed_id %in% NA,]$ID)
          print(paste0("NA length is ", NA_length))
          
          number_of_NAs = length(median_centroids_first_cluster_df[median_centroids_first_cluster_df$Avg_lat_mean %in% NA,]$ID)
          
          false_length = length(median_centroids_first_cluster_df[median_centroids_first_cluster_df$same_hashed_id %in% FALSE,]$ID)
          print(paste0("FALSE length is ", false_length, " rows"))
          
          print(ifelse(NA_length == number_of_NAs, "CORRECT", paste0("CHECK OUTPUT FOR TABLE ", input_tables[[i]])))
          
          # export results
          write.csv(median_centroids_first_cluster_df, paste0("Results/", "median_centroids_first_cluster_table_", gsub("auckland_traj_|_updated_nzgd", "",input_tables[[i]]), ".csv"), row.names = FALSE)
          
          
        }

stopCluster(cl)

# end of for loop


# the output needs to be prepared before being loaded into the Python script for calculating distances
# additional preparation code
input_datasets_duplicates = list("18_03_27_03", "20_28_04", "25_26_03", "28_4_05_05", "2703004", "04_03_20")


for (i in 1:length(input_datasets_duplicates)) {
  input_file = paste0("Results/median_centroids_first_cluster_table_", input_datasets_duplicates[[i]], ".csv")
  
  data = read.csv(input_file, header = TRUE)
  
  # filter to just those users that have a point (not NA value for lat and long)
  data_filtered = data[complete.cases(data[c("Avg_lat_median", "Avg_long_median")]), ]
  
  # REMOVE DUPLICATE ROWS, keep only the first entry per hashed_id
  data_filtered_no_duplicates = data_filtered %>% distinct(hashed_id, .keep_all = TRUE)
  
  # create an sf object with lat/long coordinates, crs of 2193
  data.sf = st_as_sf(data_filtered_no_duplicates, coords = c("Avg_long_median", "Avg_lat_median"), crs = 2193)
  
  output_file = paste0("Results/median_centroids_first_cluster_table_", input_datasets_duplicates[[i]], "_filtered_no_duplicates.shp")
  
  # write to shapefile
  st_write(data.sf, output_file)
  
}

# end of script



