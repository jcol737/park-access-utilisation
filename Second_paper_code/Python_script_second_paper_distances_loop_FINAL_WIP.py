# Python Script for second paper
# Distance calculations required for input into utilisation metrics R script
# automate running code over all input csv files
# Jessie Colbert, The University of Auckland, 2024

import arcpy
from arcpy import env
import arcpy.management
import arcpy.na
import pandas as pd
import os

# Check out Network Analyst license if available. Fail if the Network Analyst license is not available.
if arcpy.CheckExtension("network") == "Available":
    arcpy.CheckOutExtension("network")
else:
    raise arcpy.ExecuteError("Network Analyst Extension license is not available.")

# Set environment settings
## USER update as required ##
output_dir = "Output"

input_dir = "Input"

# The Network Analysis layer's data will be saved to the workspace specified here
env.workspace = os.path.join(output_dir, "Output.gdb")
env.overwriteOutput = True

# Set local variables

# input shapefiles
# park shapefile for calculations
parks_shp = "Input/ParkExtent_Parks_Reg_10mill_sqm_Akld_RC_clip.shp"

park_points_shp = "Input/ParkExtent_Parks_Reg_10mill_sqm_Akld_RC_clip_points.shp"

# Auckland RC boundaries
Akld_RC_shp = "Input/Akld_RC_boundaries.shp"

# Inner city boundaries
inner_city_shp = "Input/StatsNZ_UR2021_Akld_Clip_Major_urban.shp"

## Iterate over input datasets ##
# input datasets
input_datasets = ["18_03_27_03", "20_28_04", "25_26_03", "28_4_05_05", "2703004", "04_03_20"]

for dataset in input_datasets:
    # Load in the csv file of home location points
    median_centroids_shp = os.path.join(input_dir, f"median_centroids_first_cluster_table_{dataset}_filtered_no_duplicates.shp")

    # Create a new GDB for the output
    gdb_path = os.path.join(output_dir, f"Output_{dataset}.gdb")
    arcpy.management.CreateFileGDB(output_dir, f"Output_{dataset}.gdb")

    # Process the point locations for analysis
    # clip to just those centroids within the Auckland RC
    outputShp = os.path.join(gdb_path, f"median_centroids_{dataset}_Akld_RC_clip")
    arcpy.Clip_analysis(median_centroids_shp, Akld_RC_shp, outputShp)

    # further clip to inner city boundaries
    inputShp = outputShp
    outputShp = os.path.join(gdb_path, f"median_centroids_{dataset}_Akld_RC_clip_innercity")
    arcpy.Clip_analysis(inputShp, inner_city_shp, outputShp)

    # save a copy of the clipped attribute table to a csv for later working
    clipped_shp = outputShp
    # get all the field names from the clipped shapefile
    fields_to_export = [field.name for field in arcpy.ListFields(clipped_shp)]
    # create a csv file and write the header
    csv_file = os.path.join(output_dir, f"median_centroids_{dataset}_Akld_RC_clip_innercity.csv")
    with open(csv_file, 'w') as csvfile:
        # Write the header row
        header = ','.join(fields_to_export) + '\n'
        csvfile.write(header)

        # use a SearchCursor to iterate through rows and write data to the csv
        with arcpy.da.SearchCursor(clipped_shp, fields_to_export) as cursor:
            for row in cursor:
                # Convert each value to a string and join them with commas
                row_str = ','.join(map(str, row)) + '\n'
                csvfile.write(row_str)
        
    print(f"Attribute table exported to {csv_file}")

    # Analysis (distance)
    # near analysis, distance from home location centroids to closest park CENTROIDS

    # Create a folder for the output shapefiles for this dataset
    # Check if the folder exists otherwise create it
    if not os.path.exists(os.path.join(output_dir, f"Output_{dataset}")):
        os.makedirs(os.path.join(output_dir, f"Output_{dataset}"))
    
    # create a copy of the clipped shapefile for the output
    # Save to the folder for this dataset
    clipped_shp_for_near = os.path.join(output_dir, f"Output_{dataset}", f"homeloc_{dataset}_nearest_park_centroid.shp") # use the file name of the output that will be generating
    arcpy.CopyFeatures_management(clipped_shp, clipped_shp_for_near)
    data1 = clipped_shp_for_near
    # input dataset 2, park centroids (the feature layers or classes containing the near feature candidates)
    data2 = park_points_shp
    # find the nearest park centroid for each home location centroid
    arcpy.Near_analysis(data1, data2)
    # results are automatically added to the data1 shapefile, so no need to export to a new shapefile

    # join the attribute information to the near output from the park points shapefile, based on the ID
    arcpy.JoinField_management(data1, "NEAR_FID", data2, "FID", ["SiteName"])

    # rename the field names that have been joined
    arcpy.AddField_management(data1, "NEAR_FID", "INTEGER")
    arcpy.CalculateField_management(data1, "NEARFIDpt", "!NEAR_FID!", "PYTHON")

    arcpy.AddField_management(data1, "NEAR_DIST", "DOUBLE")
    arcpy.CalculateField_management(data1, "NEARDISTpt", "!NEAR_DIST!", "PYTHON")

    arcpy.AddField_management(data1, "SiteName", "STRING")
    arcpy.CalculateField_management(data1, "SITENAMEpt", "!SiteName!", "PYTHON")

    # Delete once added all the new fields
    arcpy.DeleteField_management(data1, "NEAR_FID")
    arcpy.DeleteField_management(data1, "NEAR_DIST")
    arcpy.DeleteField_management(data1, "SiteName")

    # print success message
    print(f"Near analysis for {dataset} complete")

    # near analysis, distance from home location centroids to closest park POLYGON
    clipped_shp_for_near_poly = os.path.join(output_dir, f"Output_{dataset}", f"homeloc_{dataset}_nearest_park_polygon.shp") # use the file name of the output that will be generating
    arcpy.CopyFeatures_management(data1, clipped_shp_for_near_poly)
    # set input features
    data1 = clipped_shp_for_near_poly
    # set near features
    data2 = parks_shp
    # find the nearest park polygon for each home location centroid
    arcpy.Near_analysis(data1, data2)

    # join the attribute information to the near output from the park points shapefile, based on the ID
    arcpy.JoinField_management(data1, "NEAR_FID", data2, "FID", ["SiteName"])

    # rename the fields
    arcpy.AddField_management(data1, "NEAR_FID", "INTEGER")
    arcpy.CalculateField_management(data1, "NEARFIDpy", "!NEAR_FID!", "PYTHON")

    arcpy.AddField_management(data1, "NEAR_DIST", "DOUBLE")
    arcpy.CalculateField_management(data1, "NEARDISTpy", "!NEAR_DIST!", "PYTHON")

    arcpy.AddField_management(data1, "SiteName", "STRING")
    arcpy.CalculateField_management(data1, "SITENAMEpy", "!SiteName!", "PYTHON")

    # delete the original columns
    arcpy.DeleteField_management(data1, "NEAR_FID")
    arcpy.DeleteField_management(data1, "NEAR_DIST")
    arcpy.DeleteField_management(data1, "SiteName")

    # print success message
    print(f"Near analysis for {dataset} complete")

    # third distance, network analysis, closest facility analysis.
    # quicker to run if the input facilities and incidents are saved in the same gdb
    # add to the output gdb for the input dataset
    # park points
    arcpy.CopyFeatures_management(park_points_shp, f"{gdb_path}\\park_points")
    # home location points
    arcpy.CopyFeatures_management(clipped_shp, f"{gdb_path}\\homeloc_{dataset}")

    # input facilities
    input_fac = f"{gdb_path}\\park_points"
    # input incidents
    input_inc = f"{gdb_path}\\homeloc_{dataset}"
    # output routes
    output_routes = os.path.join(output_dir, f"Output_{dataset}", f"homeloc_{dataset}_nearest_park_network_routes.shp")

    # Run the network analysis
    result_object = arcpy.na.MakeClosestFacilityAnalysisLayer(
        network_data_source=r"Input\nzogps_240215_nal1.gdb\nzogps_240215_nal\nzogps_240215_nal",
        layer_name="Closest Facility",
        travel_mode="Walking Time", # "" or "Driving Time"
        travel_direction="TO_FACILITIES",
        cutoff=None,
        number_of_facilities_to_find=1,
        time_of_day=None,
        time_zone="LOCAL_TIME_AT_LOCATIONS",
        time_of_day_usage="START_TIME",
        line_shape="ALONG_NETWORK",
        accumulate_attributes=None,
        generate_directions_on_solve="NO_DIRECTIONS",
        ignore_invalid_locations="SKIP"
    )

    # Get the layer object from the result object. The closest facility layer can now be referenced using the layer object
    layer_object = result_object.getOutput(0)

    # Get the names of all the sublayers within the closest facility layer
    sublayer_names = arcpy.na.GetNAClassNames(layer_object)

    # Stores the layer names that we will use later
    facilities_layer_name = sublayer_names["Facilities"]
    incidents_layer_name = sublayer_names["Incidents"]

    # Load the park points and home location points as facilities and incidents
    arcpy.na.AddLocations(layer_object, facilities_layer_name, input_fac, "", "") # may need to add a search tolerance

    field_mappings = arcpy.na.NAClassFieldMappings(layer_object,
                                                    incidents_layer_name)
    field_mappings["Name"].mappedFieldName = "ID" # name to use from the input features
    arcpy.na.AddLocations(layer_object, incidents_layer_name, input_inc,
                          field_mappings, "")

    # Solve the closest facility layer
    arcpy.na.Solve(layer_object)

    # Save the solved closest facility layer as a layer file on disk
    routes_layer = layer_object.listLayers("Routes")[0]

    # Save to a shapefile instead
    arcpy.management.CopyFeatures(routes_layer, output_routes)

    # print success message
    print(f"Network analysis for {dataset} complete")


    # Analysis (buffers)
    # home location points in the study site
    # Create 1km buffer circle around home location points in the study site
    inputShp = clipped_shp
    outputShp = os.path.join(output_dir, f"Output_{dataset}", f"homeloc_{dataset}_1km_buffer.shp")
    buffer_distance = "1000 Meters"
    
    # Create the buffer
    arcpy.Buffer_analysis(inputShp, outputShp, buffer_distance)
    
    # 2km
    outputShp = os.path.join(output_dir, f"Output_{dataset}", f"homeloc_{dataset}_2km_buffer.shp")
    buffer_distance = "2000 Meters"
    
    # output to shapefile
    arcpy.Buffer_analysis(inputShp, outputShp, buffer_distance)
    
    # 5km
    outputShp = os.path.join(output_dir, f"Output_{dataset}", f"homeloc_{dataset}_5km_buffer.shp")
    buffer_distance = "5000 Meters"
    
    # output to shapefile
    arcpy.Buffer_analysis(inputShp, outputShp, buffer_distance)

    # Print success message
    print(f"Buffer analysis for {dataset} complete")


    # Analysis (park intersect user trajectories, actual park visits)
    # Output from PGAdmin
    # Load csv
    intersect_csv = os.path.join(input_dir, f"parks_intersect_user_traj_{dataset}.csv")
    # Load the csv into a pandas dataframe
    df = pd.read_csv(intersect_csv)

    # Use lat lon to create a WGS shapefile (XYpoint)
    arcpy.management.XYTableToPoint(
        in_table=intersect_csv,
        out_feature_class=os.path.join(gdb_path, f"parks_intersect_user_traj_{dataset}_XYTableToPoint"),
        x_field="lon",
        y_field="lat",
        z_field=None,
        coordinate_system='GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]];-400 -400  1000000000;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision'
    )

    # Export to shapefile
    arcpy.conversion.ExportFeatures(
        in_features=os.path.join(gdb_path, f"parks_intersect_user_traj_{dataset}_XYTableToPoint"),
        out_features=os.path.join(gdb_path, f"parks_intersect_user_traj_{dataset}_points_WGS"),
        where_clause="",
        use_field_alias_as_name="NOT_USE_ALIAS",
        sort_field=None
    )

    # Convert to NZGD and export
    arcpy.management.Project(
        in_dataset = os.path.join(gdb_path, f"parks_intersect_user_traj_{dataset}_points_WGS"),
        out_dataset = os.path.join(gdb_path, f"parks_intersect_user_traj_{dataset}_points_NZGD"),
        out_coor_system='PROJCS["NZGD_2000_New_Zealand_Transverse_Mercator",GEOGCS["GCS_NZGD_2000",DATUM["D_NZGD_2000",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM ["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],PARAMETER["False_Easting",1600000.0],PARAMETER["False_Northing",10000000.0],PARAMETER ["Central_Meridian",173.0],PARAMETER["Scale_Factor",0.9996],PARAMETER["Latitude_Of_Origin",0.0],UNIT["Meter",1.0]]',
        transform_method="NZGD_2000_To_WGS_1984_1",
        in_coor_system='GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]',
        preserve_shape="NO_PRESERVE_SHAPE",
        max_deviation=None,
        vertical="NO_VERTICAL"
    )

    # Export the attribute table as a dbf table
    arcpy.conversion.ExportTable(
        in_table = os.path.join(gdb_path, f"parks_intersect_user_traj_{dataset}_points_NZGD"),
        out_table = os.path.join(gdb_path, f"parks_intersect_user_traj_{dataset}_points_NZGD_table"),
        where_clause="",
        use_field_alias_as_name="NOT_USE_ALIAS",
        sort_field=None
    )

    # Join the park points to the home location points
    # Home location points file (already inner city only)

    original_setting = arcpy.env.qualifiedFieldNames

    try:
        arcpy.env.qualifiedFieldNames = False

        homeloc_join_table = arcpy.management.AddJoin(
            in_layer_or_view = os.path.join(gdb_path, f"homeloc_{dataset}"),
            in_field="hashd_d",
            join_table = os.path.join(gdb_path, f"parks_intersect_user_traj_{dataset}_points_NZGD_table"),
            join_field="hashed_id",
            join_type="KEEP_ALL",
            index_join_fields="NO_INDEX_JOIN_FIELDS"
        )

        fields_to_display = [field.name for field in arcpy.ListFields(homeloc_join_table)]
        print(fields_to_display)

        homeloc_join_table_select = arcpy.management.SelectLayerByAttribute(
            homeloc_join_table,
            selection_type="NEW_SELECTION",
            where_clause="hashed_id IS NOT NULL",
            invert_where_clause=None
        )

        result = arcpy.management.CopyFeatures(
            homeloc_join_table_select, os.path.join(gdb_path, f"homeloc_{dataset}_join_parks_intersect_user_traj_not_null")
        )

        fields_to_display = [field.name for field in arcpy.ListFields(result)]
        print(fields_to_display)


    finally:
        arcpy.env.qualifiedFieldNames = original_setting
    # end of try/finally block

    # Join the resulting rows to the park points table
    # Use the alias name for the result join field
    park_join_table = arcpy.management.AddJoin(
        in_layer_or_view = os.path.join(gdb_path, f"park_points"),
        in_field="OBJECTID",
        join_table = os.path.join(gdb_path, f"homeloc_{dataset}_join_parks_intersect_user_traj_not_null"),
        join_field="polygon_id",
        join_type="KEEP_ALL",
        index_join_fields="NO_INDEX_JOIN_FIELDS"
    )

    # Select desired features from the joined table
    park_join_table_select = arcpy.management.SelectLayerByAttribute(
        park_join_table,
        selection_type="NEW_SELECTION",
        where_clause="polygon_id IS NOT NULL",
        invert_where_clause=None
    )

    # version without the qualified field names
    original_setting = arcpy.env.qualifiedFieldNames

    try:
        arcpy.env.qualifiedFieldNames = False

        arcpy.management.CopyFeatures(
            park_join_table_select, os.path.join(output_dir, f"Output_{dataset}", f"park_points_join_homeloc_{dataset}_parks_intersect_user_traj_not_null_N.shp")    
        )
    finally:
        arcpy.env.qualifiedFieldNames = original_setting
    # end of try/finally block
    
    # Analysis
    # Near analysis
    # Workaround as line length
    # Add field to both input tables as the "Line_ID"
    arcpy.management.AddField(
        in_table = os.path.join(output_dir, f"Output_{dataset}", f"park_points_join_homeloc_{dataset}_parks_intersect_user_traj_not_null_N.shp"),
        field_name = "Line_ID",
        field_type = "LONG"
    )

    arcpy.management.AddField(
        in_table = os.path.join(gdb_path, f"homeloc_{dataset}_join_parks_intersect_user_traj_not_null"),
        field_name = "Line_ID",
        field_type = "LONG"
    )

    # Calculate field
    arcpy.management.CalculateField(
        in_table = os.path.join(output_dir, f"Output_{dataset}", f"park_points_join_homeloc_{dataset}_parks_intersect_user_traj_not_null_N.shp"),
        field="Line_ID",
        expression = "!OBJECTID_2!",
        expression_type="PYTHON3",
        code_block="",
        field_type="TEXT",
        enforce_domains="NO_ENFORCE_DOMAINS"
    )

    arcpy.management.CalculateField(
        in_table = os.path.join(gdb_path, f"homeloc_{dataset}_join_parks_intersect_user_traj_not_null"),
        field="Line_ID",
        expression="!OBJECTID_1!",
        expression_type="PYTHON3",
        code_block="",
        field_type="TEXT",
        enforce_domains="NO_ENFORCE_DOMAINS"
    )

    # Merge the two point datasets
    arcpy.management.Merge(
        inputs = f"{os.path.join(output_dir, f'Output_{dataset}', f'park_points_join_homeloc_{dataset}_parks_intersect_user_traj_not_null_N.shp')};{os.path.join(gdb_path, f'homeloc_{dataset}_join_parks_intersect_user_traj_not_null')}",
        output = os.path.join(output_dir, f"Output_{dataset}", f"homeloc_{dataset}_join_parks_intersect_user_traj_not_null_merge.shp"),
        add_source="NO_SOURCE_INFO"
    )

    # Create lines between the paired points, based on line ID
    arcpy.management.PointsToLine(
        Input_Features = os.path.join(output_dir, f"Output_{dataset}", f"homeloc_{dataset}_join_parks_intersect_user_traj_not_null_merge.shp"),
        Output_Feature_Class = os.path.join(output_dir, f"Output_{dataset}", f"homeloc_{dataset}_join_parks_intersect_user_traj_not_null_merge_lines.shp"),
        Line_Field="Line_ID",
        Sort_Field=None,
        Close_Line="NO_CLOSE",
        Line_Construction_Method="TWO_POINT",
        Attribute_Source="NONE",
        Transfer_Fields=None
    )

    # Calculate the length of the lines
    # Add field
    arcpy.management.AddField(
        in_table = os.path.join(output_dir, f"Output_{dataset}", f"homeloc_{dataset}_join_parks_intersect_user_traj_not_null_merge_lines.shp"),
        field_name = "LENGTH",
        field_type = "DOUBLE"
    )

    # Calculate Geometry
    arcpy.management.CalculateGeometryAttributes(
        in_features = os.path.join(output_dir, f"Output_{dataset}", f"homeloc_{dataset}_join_parks_intersect_user_traj_not_null_merge_lines.shp"),
        geometry_property="LENGTH LENGTH",
        length_unit="METERS",
        area_unit="",
        coordinate_system='PROJCS["NZGD_2000_New_Zealand_Transverse_Mercator",GEOGCS["GCS_NZGD_2000",DATUM["D_NZGD_2000",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],PARAMETER["False_Easting",1600000.0],PARAMETER["False_Northing",10000000.0],PARAMETER["Central_Meridian",173.0],PARAMETER["Scale_Factor",0.9996],PARAMETER["Latitude_Of_Origin",0.0],UNIT["Meter",1.0]]',
        coordinate_format="SAME_AS_INPUT"
    )

    # Print success message
    print(f"Line distance calculation for dataset {dataset} completed successfully")


    #### Code below to the end of the script is a WIP ####
    # This code should work, but is not currently outputting any routes
    # Most of the utilisation metrics can be calculated without this result (see note in r utilisation script)

    # Network analysis
    # Paired points
    # Using the paired points, calculate the network distance between the points
    output_routes = os.path.join(output_dir, f"Output_{dataset}", f"homeloc_{dataset}_actual_park_visit_network_routes.shp")
    
    # Use Route analysis
    # Create layer
    result_object = arcpy.na.MakeRouteAnalysisLayer(
        network_data_source=r"Input\nzogps_240215_nal1.gdb\nzogps_240215_nal\nzogps_240215_nal",
        layer_name="Route",
        travel_mode="Walking Time", # "" or "Driving Time"
    )

    # Get the layer object from the result object. The route layer can now be referenced using the layer object
    layer_object = result_object.getOutput(0)

    # Get the names of all the sublayers within the route layer
    sublayer_names = arcpy.na.GetNAClassNames(layer_object)

    # Stores the layer names that we will use later
    stops_layer_name = sublayer_names["Stops"]

    # Add the paired points as stops
    field_mappings = arcpy.na.NAClassFieldMappings(layer_object, stops_layer_name)

    field_mappings["Name"].mappedFieldName = "Line_ID"

    out_stops = os.path.join(output_dir, f"Output_{dataset}", f"homeloc_{dataset}_join_parks_intersect_user_traj_not_null_merge.shp")

    arcpy.na.AddLocations(layer_object, stops_layer_name, out_stops, field_mappings, "")

    # Solve, ignoring any invalid locations
    arcpy.na.Solve(layer_object, "SKIP")

    # Save the output to a shapefile
    routes_layer = layer_object.listLayers("Routes")[0]
    arcpy.management.CopyFeatures(routes_layer, output_routes)
    
    # Check that all the points were added as stops
    # Get the number of features in the input dataset
    input_point_count = arcpy.management.GetCount(
        in_rows=os.path.join(output_dir, f"Output_{dataset}", f"homeloc_{dataset}_join_parks_intersect_user_traj_not_null_merge.shp")
    )

    print(input_point_count)

    # Get the number of stops added
    stops_added = int(arcpy.GetCount_management("Route\\Stops").getOutput(0))

    print(stops_added)

    # If the number of stops added is not equal to the number of features in the input dataset, print a warning
    if input_point_count != stops_added:
        print(f"Warning: Not all stops were added to the network analysis layer for dataset {dataset}")

    # Add code here to handle the case where not all stops were added
    # TBA

    # Print success message
    print(f"Network analysis for dataset {dataset} completed successfully")

# End of loop

# End of script