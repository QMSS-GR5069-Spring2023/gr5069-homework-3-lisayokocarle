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
drivers.head()


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
average_df = pit_stops_cleaned.groupby('driverId')[['milliseconds']].mean()

average_df


# COMMAND ----------

### Extracting Drivers info from Drivers df
driver_info = drivers[['driverId', 'forename', 'surname']]

### Merging extracted driver info with average pit stop df
average_df.merge(driver_info, on = 'driverId', how = 'left')

### Checking the newly created df
average_df

### Rounding milliseconds up
average_df.milliseconds = average_df.milliseconds.round()

### Checking once more
average_df.info()
average_df

# COMMAND ----------

# MAGIC %md ### 2. Rank the average time spent at the pit stop in order of who won each race

# COMMAND ----------

### Pulling out 
