# Databricks notebook source
# MAGIC %md ## Importing functions

# COMMAND ----------

### Loading in packages
import pandas as pd
import boto3
import sys
import csv
import io
import os
import awswrangler as wr


# COMMAND ----------

# MAGIC %md ## Reading in Dataset

# COMMAND ----------

### Pulling in csv data from Amazon S3 bucket 

circuits = wr.s3.read_csv('s3://columbia-gr5069-main/raw/circuits.csv')
constructor_results = wr.s3.read_csv('s3://columbia-gr5069-main/raw/constructor_results.csv')
constructor_standings = wr.s3.read_csv('s3://columbia-gr5069-main/raw/constructor_standings.csv')
constructors = wr.s3.read_csv('s3://columbia-gr5069-main/raw/constructors.csv')
driver_standings = wr.s3.read_csv('s3://columbia-gr5069-main/raw/driver_standings.csv')
drivers = wr.s3.read_csv('s3://columbia-gr5069-main/raw/drivers.csv')
lap_times = wr.s3.read_csv('s3://columbia-gr5069-main/raw/lap_times.csv')
pit_stops = wr.s3.read_csv('s3://columbia-gr5069-main/raw/pit_stops.csv')
qualifying = wr.s3.read_csv('s3://columbia-gr5069-main/raw/qualifying.csv')
races = wr.s3.read_csv('s3://columbia-gr5069-main/raw/races.csv')
results = wr.s3.read_csv('s3://columbia-gr5069-main/raw/results.csv')
seasons = wr.s3.read_csv('s3://columbia-gr5069-main/raw/seasons.csv')
sprint_results = wr.s3.read_csv('s3://columbia-gr5069-main/raw/sprint_results.csv')
status = wr.s3.read_csv('s3://columbia-gr5069-main/raw/status.csv')

# COMMAND ----------

# MAGIC %md ## Explore

# COMMAND ----------

### Looking at the data held in in the dataframes loaded in

pit_stops.info()

drivers.info()

# COMMAND ----------

### Looking at drivers df
drivers.info()


# COMMAND ----------

### Looking at results df
results.info()

### Looking at the columns that will help me connect driver with race and rank
results[['raceId', 'driverId', 'rank']]

# COMMAND ----------

### Looking at races df
races.info()

# COMMAND ----------

# MAGIC %md ## Transform

# COMMAND ----------

# MAGIC %md ### 1. What was the average time each driver spent at the pit stop for each race?

# COMMAND ----------

### Dropping rows with NA values 
pit_stops_cleaned = pit_stops.dropna()

### Grouping cleaned df by driverId and getting mean
average_driver_race = pit_stops_cleaned.groupby(['driverId','raceId'])[['milliseconds']].mean()

### Rounding milliseconds up
average_driver_race.milliseconds = average_driver_race.milliseconds.round()

average_driver_race

# COMMAND ----------

### Pulling out Driver names from driver df
driver_info = drivers[['driverId', 'forename', 'surname']]

### Pulling out race results from results df
race_results = results[['raceId', 'driverId', 'rank']]

### Pulling out race name from races df
race_name = races[['raceId', 'name']]

### Merging driver_info with race_results
driver_race_results = race_results.merge(driver_info, how = "left", on = 'driverId')

### Merging average driver time df with merged race_results
merged_avg = pd.merge(average_driver_race, driver_race_results, how = "left", on = ['driverId', 'raceId'])
merged_avg.head()

# COMMAND ----------

# MAGIC %md ### 2. Rank the average time spent at the pit stop in order of who won each race

# COMMAND ----------

### Merging race name into df
merged_avg_race = pd.merge(merged_avg, race_name, how = 'left', on = 'raceId')

### Ranking average time spent at pit stop based on who won race
merged_avg_race['pit_time_rank'] = merged_avg_race.groupby(['name','rank'])['milliseconds'].rank(method = 'max')

### Changing name of name colum to race name
merged_avg_race.rename(columns = {'name':'race_name'}, inplace = True)

# COMMAND ----------

# MAGIC %md ## 3. Insert the missing code for drivers based on the 'drivers' dataset

# COMMAND ----------

### Pulling codes from drivers df
driver_code = drivers[['driverId', 'code']]

### seeing if there were any NA columns
driver_code.dropna()

### Insert driver codes based on driver df
code_merged_df = pd.merge(merged_avg_race, driver_code, how = 'left', on = 'driverId')
code_merged_df.info()

# COMMAND ----------

# MAGIC %md ## 4. Who is the youngest and oldest driver for each race? Create a new column named "Age"

# COMMAND ----------

### Pulling driverId and dob from Drivers df
driver_dob = drivers[['driverId', 'dob']]

### Pulling raceId and race dates from race df
race_date = races[['raceId', 'date']]

### Merging dobs to aggregated df
dob_merged_df = pd.merge(code_merged_df, driver_dob, how = 'left', on = 'driverId')

### Merging race date to aggregated df
race_dob_merged = pd.merge(dob_merged_df, race_date, how = 'left', on = 'raceId')

### renaming date to race_date
race_dob_merged.rename(columns = {'date':'race_date'}, inplace = True)

### Converting to dob and race date to DateTime
race_dob_merged['dob'] = pd.to_datetime(race_dob_merged['dob'])
race_dob_merged['race_date'] = pd.to_datetime(race_dob_merged['race_date'])

### Calcualting age
race_dob_merged['age_at_race'] = race_dob_merged['race_date'] - race_dob_merged['dob']
race_dob_merged = race_dob_merged.astype({'age_at_race':'int'})

race_dob_merged.info()

# COMMAND ----------

### Ranking youngest and oldest driver for each race
### Ranking average time spent at pit stop based on who won race
race_dob_merged['age_rank'] = race_dob_merged.groupby(['raceId'])['age_at_race'].rank(method = 'max')

youngest_oldest_race = race_dob_merged.groupby(['raceId'])['age_rank'].agg(['min', 'max'])

youngest_oldest_race

