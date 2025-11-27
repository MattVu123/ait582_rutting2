# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Data Processing Code

# %% [markdown]
# This juypter notebook processes the data from dbfs

# %% [markdown]
# ## This section imports the raw datasets and pre-processed them for analysis

# %%
# Import libraries
from pyspark.sql import SparkSession, functions as F
from functools import reduce
import os
import pandas as pd
from statsmodels.stats.outliers_influence import variance_inflation_factor
import glob
import shutil

# Initialize Spark session
spark = SparkSession.builder.appName("SouthernRuttingPreprocessing").getOrCreate()

# Define base path
base_path = "/Volumes/workspace/mlrutting-3/mlrutting-3/"

# %%
# Step 1: Load CSV datasets
humidity_df   = spark.read.option("header", True).option("inferSchema", True).option("nullValue", "NULL").csv(base_path + "humidity.csv")
precip_df     = spark.read.option("header", True).option("inferSchema", True).option("nullValue", "NULL").csv(base_path + "precipitation.csv")
rutting_df    = spark.read.option("header", True).option("inferSchema", True).option("nullValue", "NULL").csv(base_path + "rutting.csv")
solar_df      = spark.read.option("header", True).option("inferSchema", True).option("nullValue", "NULL").csv(base_path + "solar.csv")
temp_df       = spark.read.option("header", True).option("inferSchema", True).option("nullValue", "NULL").csv(base_path + "temp.csv")
# traffic_df    = spark.read.option("header", True).option("inferSchema", True).option("nullValue", "NULL").csv(base_path + "mm_ct.csv")
traffic_df = spark.read.option("header", True).option("inferSchema", True).option("nullValue", "NULL").csv(base_path + "traffic_annual.csv")
wind_df       = spark.read.option("header", True).option("inferSchema", True).option("nullValue", "NULL").csv(base_path + "wind.csv")
grid_df       = spark.read.option("header", True).option("inferSchema", True).option("nullValue", "NULL").csv(base_path + "merra_grid_section.csv")

print("All CSVs loaded successfully")

# %%
# Step 2: Merge all climate-related datasets
climate_dfs = [humidity_df, precip_df, solar_df, temp_df, wind_df]

merged_climate = reduce(
    lambda left, right: left.join(right, on=["MERRA_ID", "YEAR", "MONTH"], how="outer"),
    climate_dfs
)

print("Climate datasets merged")

# Step 3: Add grid metadata (STATE, SHRP_ID, ELEVATION, LATITUDE, LONGITUDE)
merged_climate = merged_climate.join(
    grid_df,
    on="MERRA_ID",
    how="left"
)

print("Added STATE, SHRP_ID, ELEVATION, LATITUDE, and LONGITUDE to climate records")
print (merged_climate.take(5))

# %%
# Step 4: Merge with rutting data (target variable) with climate (features)
rutting_df = rutting_df.withColumn("SURVEY_DATE", F.to_date(F.col("SURVEY_DATE"), "MM/dd/yyyy")) \
                       .withColumn("YEAR", F.year("SURVEY_DATE")) \
                       .withColumn("MONTH", F.month("SURVEY_DATE")) \
                       .withColumn("MAX_MEAN_DEPTH_1_8", F.col("MAX_MEAN_DEPTH_1_8").cast("double")) \
                       .withColumn("SHRP_ID", F.trim(F.col("SHRP_ID").cast("string"))
                       )

# Midwest
midwest_states = [
    "Illinois", "Indiana", "Iowa", "Kansas", "Michigan", "Minnesota",
    "Missouri", "Nebraska", "North Dakota", "Ohio", "South Dakota", "Wisconsin"
]

# Northeast
northeast_states = [
    "Maine", "New Hampshire", "Vermont", "Massachusetts", "Rhode Island",
    "Connecticut", "New York", "New Jersey", "Pennsylvania"
]

# South
southern_states = [
    "Alabama", "Arkansas", "Delaware", "Florida", "Georgia", "Kentucky",
    "Louisiana", "Maryland", "Mississippi", "North Carolina", "Oklahoma",
    "South Carolina", "Tennessee", "Texas", "Virginia", "West Virginia"
]

# List of southern states
states = southern_states + northeast_states + midwest_states # + western_states
print(len(states))

# Filter rutting_df for southern states
rutting_df = rutting_df.filter(F.col("STATE_CODE_EXP").isin(states))

print(rutting_df.count())

# trim off white space and make string
merged_climate = merged_climate.withColumn(
    "SHRP_ID",
    F.trim(F.col("SHRP_ID").cast("string"))
)
# Ensure SHRP_ID is a string and pad with leading zeros to length 4
merged_climate = merged_climate.withColumn(
    "SHRP_ID",
    F.lpad(F.col("SHRP_ID").cast("string"), 4, "0")
)

# Merge with rutting + climate
rutting_climate = rutting_df.join(
    merged_climate,
    on=["STATE_CODE", "SHRP_ID", "YEAR", "MONTH"],
    how="left"
)

# view
print("Rutting + climate merged successfully")

# Show shape and schema
row_count = rutting_climate.count()
print(f"\nTotal rows: {row_count:,}")
print("Schema:")
rutting_climate.printSchema()
print("-" * 60)


# %%
# Step 5: Merge with rutting + climate + traffic
# Rename the traffic column so it doesn't conflict
traffic_df = traffic_df.withColumnRenamed("STATE_CODE_EXP", "STATE_CODE_EXP_traffic")
# trim off white space and make string
traffic_df = traffic_df.withColumn(
    "SHRP_ID",
    F.trim(F.col("SHRP_ID").cast("string"))
)
# Ensure SHRP_ID is a string and pad with leading zeros to length 4
traffic_df = traffic_df.withColumn(
    "SHRP_ID",
    F.lpad(F.col("SHRP_ID").cast("string"), 4, "0")
)
# join
rutting_climate_traffic = rutting_climate.join(
    traffic_df,
    on=["STATE_CODE", "SHRP_ID", "CONSTRUCTION_NO", "YEAR"],
    how="left"
)


# Show shape and schema
row_count = rutting_climate_traffic.count()
print(f"\nTotal rows: {row_count:,}")
print("Schema:")
rutting_climate_traffic.printSchema()
print("-" * 60)


# %%
# select only relevant cols
keep_cols = [
    "STATE_CODE",
    "SHRP_ID",
    "CONSTRUCTION_NO",
    "YEAR",
    "MONTH",
    "STATE_CODE_EXP",
    "MAX_MEAN_DEPTH_1_8",
    "REL_HUM_AVG_AVG",
    "PRECIPITATION",
    "EVAPORATION",
    "PRECIP_DAYS",
    "CLOUD_COVER_AVG",
    "SHORTWAVE_SURFACE_AVG",
    "TEMP_AVG",
    "FREEZE_INDEX",
    "FREEZE_THAW",
    "WIND_VELOCITY_AVG",
    "AADTT_VEH_CLASS_4_TREND",
    "AADTT_VEH_CLASS_5_TREND",
    "AADTT_VEH_CLASS_6_TREND",
    "AADTT_VEH_CLASS_7_TREND",
    "AADTT_VEH_CLASS_8_TREND",
    "AADTT_VEH_CLASS_9_TREND",
    "AADTT_VEH_CLASS_10_TREND",
    "AADTT_VEH_CLASS_11_TREND",
    "AADTT_VEH_CLASS_12_TREND",
    "AADTT_VEH_CLASS_13_TREND"
]

rutting_climate_traffic = rutting_climate_traffic.select(keep_cols)

# %%
from pyspark.sql import functions as F

# List of climate variables
climate_vars = [
    "REL_HUM_AVG_AVG", "PRECIPITATION", "EVAPORATION", "PRECIP_DAYS",
    "CLOUD_COVER_AVG", "SHORTWAVE_SURFACE_AVG", "TEMP_AVG",
    "FREEZE_INDEX", "FREEZE_THAW", "WIND_VELOCITY_AVG"
]

# List of traffic variables
traffic_vars = [f"AADTT_VEH_CLASS_{i}_TREND" for i in range(4, 14)]

# Count rows with at least 1 missing climate variable
missing_climate_count = rutting_climate_traffic.filter(
    sum(F.col(c).isNull().cast("int") for c in climate_vars) >= 1
).count()

# Count rows with at least 1 missing traffic variable
missing_traffic_count = rutting_climate_traffic.filter(
    sum(F.col(c).isNull().cast("int") for c in traffic_vars) >= 1
).count()

print(f"Number of records with missing climate variables: {missing_climate_count:,}")
print(f"Number of records with missing traffic volume variables: {missing_traffic_count:,}")


# %%
# remove nulls
rutting_climate_traffic = rutting_climate_traffic.na.drop()

# %%
# Show shape and schema
row_count = rutting_climate_traffic.count()
print(f"\nTotal rows: {row_count:,}")
print("Schema:")
rutting_climate_traffic.printSchema()
print("-" * 60)

# %%
# Select distinct state codes and state names, order by STATE_CODE
distinct_states = rutting_df.select("STATE_CODE", "STATE_CODE_EXP").distinct().orderBy("STATE_CODE")

# Show all states
distinct_states.show(50, truncate=False)

# Print the number of rows
row_count = distinct_states.count()
print(f"Total number of distinct states: {row_count}")

# %%
# Paths
# save data to Volumes in DBFS
spark_folder = os.path.join(base_path, "rutting_climate_traffic_processed")
final_csv = os.path.join(base_path, "rutting_climate_traffic.csv")

# Step 1: Write the Spark DataFrame to a folder (keep Spark output)
# Coalesce to 1 partition so we can extract a single CSV later
rutting_climate_traffic.coalesce(1).write.mode("overwrite").option("header", True).csv(spark_folder)

# Step 2: Find the part-0000*.csv file Spark created
part_file = glob.glob(os.path.join(spark_folder, "part-*.csv"))[0]

# Step 3: Copy it to the final CSV (does NOT touch the Spark folder)
shutil.copy(part_file, final_csv)

print(f"Saved Spark output folder: {spark_folder}")
print(f"Saved single CSV file: {final_csv}")


# %%
# move to processed data folder in git
git_final_csv = os.path.join("/Workspace/Shared/ait614_rutting2/data/processed", "rutting_climate_traffic.csv")
shutil.copy(final_csv, git_final_csv)


# %%
# review
rutting_climate_traffic.show(5)

print("Data processing is done!")
