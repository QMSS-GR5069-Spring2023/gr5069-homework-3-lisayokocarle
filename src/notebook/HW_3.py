# Databricks notebook source
# MAGIC %md ## Importing functions

# COMMAND ----------

### Installing latest version of Pandas and awswrangler
dbutils.library.installPyPI("pandas")
dbutils.library.installPyPI("awswrangler")

# COMMAND ----------

### Loading in packages
import pandas as pd
import numpy as np
import boto3
import sys
import csv
import io
import os
import datetime as dt
import awswrangler as wr
import matplotlib.pyplot as plt


!pip install s3fs 



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

# MAGIC %md # Explore

# COMMAND ----------

sprint_results.head()

# COMMAND ----------

### Looking at the data held in in the dataframes loaded in
pit_stops.info()

drivers.info()

# COMMAND ----------

### Looking at drivers df
drivers.info()


# COMMAND ----------

### Looking at status df
status.head()

# COMMAND ----------

### Looking at Driver df
driver_standings

# COMMAND ----------

### Looking at constructor df
constructors.head()

# COMMAND ----------

### Looking at seasons df
seasons.head()

# COMMAND ----------

### Looking at results df
display(results)

# COMMAND ----------

### Looking at the columns that will help me connect driver with race and rank
results[['raceId', 'driverId', 'rank']]

# COMMAND ----------

### Looking at results df
results

# COMMAND ----------

### Looking at races df
races

# COMMAND ----------

# MAGIC %md # Transform

# COMMAND ----------

# MAGIC %md ### 1. What was the average time each driver spent at the pit stop for each race?

# COMMAND ----------

### Dropping rows with NA values 
pit_stops_cleaned = pit_stops.dropna()

### Grouping cleaned df by driverId and raceId to get mean milliseconds of each driver for each race
average_driver_race = pd.DataFrame(pit_stops_cleaned.groupby(['driverId','raceId'])[['milliseconds']].mean())

### Creating new column where milliseconds is converted to seconds
average_driver_race[['average_seconds']] = round((average_driver_race[['milliseconds']] * .001), 2)

average_driver_race.head()

# COMMAND ----------

### Pulling out Driver names from driver df
driver_info = drivers[['driverId', 'forename', 'surname']]

### Pulling out race results from results df
race_results = results[['raceId', 'driverId', 'position']]

### Pulling out race name from races df
race_name = races[['raceId', 'name']]

### Merging driver_info with race_results
driver_race_results = race_results.merge(driver_info, how = "left", on = 'driverId')

### Merging average driver time df with merged race_results
average_driver_names = average_driver_race.merge(driver_race_results, how = "left", on = ['driverId', 'raceId'])


# COMMAND ----------

average_driver_names.head()

# COMMAND ----------

# MAGIC %md ### 2. Rank the average time spent at the pit stop in order of who won each race

# COMMAND ----------

### Dropping '\\N' values from average driver df 
average_driver_names['position'] = average_driver_names['position'].replace('\\N', np.nan)
average_driver_names = average_driver_names.dropna()


# COMMAND ----------

### Converting rank column into integers
average_driver_names['position'] = average_driver_names['position'].astype('int')

# COMMAND ----------

### Filtering average driver time df for winners
race_winners = average_driver_names[average_driver_names['position'] == 1]

# COMMAND ----------

race_winners

# COMMAND ----------

### Merging race name into df
merged_avg_race = race_winners.merge(race_name, how = 'left', on = 'raceId')

### Ranking average time spent at pit stop based on who won race
merged_avg_race['pit_time_rank'] = merged_avg_race['average_seconds'].rank(method = 'min')

### Changing name of name colum to race name
merged_avg_race.rename(columns = {'name':'race_name'}, inplace = True)

merged_avg_race

# COMMAND ----------

# MAGIC %md ### 3. Insert the missing code for drivers based on the 'drivers' dataset

# COMMAND ----------

### Pulling codes from drivers df
driver_code = drivers[['driverId', 'code']]

### seeing if there were any NA columns
driver_code.dropna()

average_driver_names = average_driver_names.merge(race_name, how = 'left', on = 'raceId')

### Insert driver codes based on driver df
code_merged_df = average_driver_names.merge(driver_code, how = 'left', on = 'driverId')
code_merged_df.head()

# COMMAND ----------

# MAGIC %md ### 4. Who is the youngest and oldest driver for each race? Create a new column named "Age"

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
race_dob_merged = race_dob_merged.rename(columns = {'date':'race_date'})

### Converting to dob and race date to DateTime
race_dob_merged['dob'] = pd.to_datetime(race_dob_merged['dob'])
race_dob_merged['race_date'] = pd.to_datetime(race_dob_merged['race_date'])

### Calcualting age
race_dob_merged['age_at_race'] = race_dob_merged['race_date'] - race_dob_merged['dob']
race_dob_merged['age_at_race'] = round(race_dob_merged['age_at_race'] / np.timedelta64(1,'Y'), 2)

### Converting age to years
race_dob_merged.head()

# COMMAND ----------

### Ranking youngest and oldest driver for each race
race_dob_merged['age_rank'] = race_dob_merged.groupby(['raceId'])['age_at_race'].rank(method = 'max')


### Creating df of youngest_racers
youngest_racers = race_dob_merged[race_dob_merged['age_rank']== 1]
youngest_racers.head()

# COMMAND ----------

display(youngest_racers)

# COMMAND ----------

### Creating df of oldest_racers
oldest_idx= race_dob_merged.groupby('raceId')['age_rank'].transform(max) == race_dob_merged['age_rank']

oldest_racers = race_dob_merged[oldest_idx]

# COMMAND ----------

### Combining the df of youngest and oldest racer in each race

oldest_youngestdf = [youngest_racers, oldest_racers]

oldest_youngestdf = pd.concat(oldest_youngestdf)

# COMMAND ----------

### Table of youngest and oldest driver of each race

oldest_youngestdf = oldest_youngestdf[['raceId', 'forename', 'surname', 'name', 'code', 'age_at_race']]

display(oldest_youngestdf)

# COMMAND ----------

# MAGIC %md ### 5. For a given race, which driver has the most wins and losses?

# COMMAND ----------

### Converting results df, rank column to integer
results['rank'] = pd.to_numeric(results['rank'], errors = 'coerce')

### Checking to see how rank column came out
results['rank'].unique()
results.info()

wins = results[results['rank'] == 1]

# COMMAND ----------

### Pulling df of count of wins for each driver
wins = results[results['rank'] == 1]
wins_df = wins.groupby('driverId').count()
wins_df_cleaned = wins_df[['rank']]

### Renaming column name, since we are just seeing count of times
wins_df_cleaned = wins_df_cleaned.rename(columns = {'rank':'count_of_wins'})

wins_df_cleaned

# COMMAND ----------

### Pulling df of count of losses for each driver
losses = results[results['rank'] != 1]
losses_df = losses.groupby('driverId').count()
losses_df_cleaned = losses_df[['rank']]

### Renaming column name, since we are just seeing count of times
losses_df_cleaned = losses_df_cleaned.rename(columns = {'rank':'count_of_losses'})

losses_df_cleaned

# COMMAND ----------

### Creating new df merging driver_win df with driver_loss df
driver_wins_losses_merge = wins_df_cleaned.merge(losses_df_cleaned, how = 'left', on= 'driverId')

driver_wins_losses_merge

# COMMAND ----------

### Merging wins and losses counts with main df
exploratory_df = race_dob_merged.merge(driver_wins_losses_merge, how = 'left', on = 'driverId')

exploratory_df

# COMMAND ----------

### Creating new variable in main df by which drivers had most wins for each race
exploratory_df['wins_rank'] = exploratory_df.groupby(['raceId'])['count_of_wins'].rank(method = 'max')

### Creating new variable in main df by which drivers has most losses for each race
exploratory_df['loss_rank'] = exploratory_df.groupby(['raceId'])['count_of_losses'].rank(method = 'max')

exploratory_df

# COMMAND ----------

### Creating df of drivers with most wins by race
most_wins_df = exploratory_df[exploratory_df['wins_rank']== 1]
most_wins_df


# COMMAND ----------

### Creating df of drivers with most losses by race
most_losses_df = exploratory_df[exploratory_df['loss_rank']== 1]
most_losses_df


# COMMAND ----------

# MAGIC %md ### 6). Who had the fastest pit stop times in each country? Who had the slowest?

# COMMAND ----------

### Merging exploratory_df with driver nationality
drivers_nationality = drivers[['driverId', 'nationality']]
country_df = exploratory_df.merge(drivers_nationality, how = 'left', on = 'driverId')
country_df

# COMMAND ----------

### Creating ranking of countries with fastest pits stops
country_df['fastest_country'] = country_df.groupby('nationality')['average_seconds'].rank()

country_df

# COMMAND ----------

###Sorting values based on ranking of countries with fastest pit times
sorted_countries_fastest = country_df[country_df['fastest_country'] == 1].sort_values('average_seconds')

sorted_countries_fastest

# COMMAND ----------

### Sorting data based on countries with slowest pit stops
sorted_countries_slowest = country_df.groupby('nationality')['fastest_country', 'nationality'].tail()

sorted_countries_slowest

# COMMAND ----------


