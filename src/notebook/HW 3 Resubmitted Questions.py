# Databricks notebook source
# MAGIC %md ### Importing Packages

# COMMAND ----------

### Installing s3fs
!pip install s3fs

# COMMAND ----------

### Importing packages
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
import datetime as dt

# COMMAND ----------

### Importing datasets
circuits = pd.read_csv('s3://columbia-gr5069-main/raw/circuits.csv')
constructor_results = pd.read_csv('s3://columbia-gr5069-main/raw/constructor_results.csv')
constructor_standings = pd.read_csv('s3://columbia-gr5069-main/raw/constructor_standings.csv')
constructors = pd.read_csv('s3://columbia-gr5069-main/raw/constructors.csv')
driver_standings = pd.read_csv('s3://columbia-gr5069-main/raw/driver_standings.csv')
drivers = pd.read_csv('s3://columbia-gr5069-main/raw/drivers.csv')
lap_times = pd.read_csv('s3://columbia-gr5069-main/raw/lap_times.csv')
pit_stops = pd.read_csv('s3://columbia-gr5069-main/raw/pit_stops.csv')
qualifying = pd.read_csv('s3://columbia-gr5069-main/raw/qualifying.csv')
races = pd.read_csv('s3://columbia-gr5069-main/raw/races.csv')
results = pd.read_csv('s3://columbia-gr5069-main/raw/results.csv')
seasons = pd.read_csv('s3://columbia-gr5069-main/raw/seasons.csv')
sprint_results = pd.read_csv('s3://columbia-gr5069-main/raw/sprint_results.csv')
status = pd.read_csv('s3://columbia-gr5069-main/raw/status.csv')

# COMMAND ----------

# MAGIC %md ### Exploring data

# COMMAND ----------

drivers

# COMMAND ----------

races

# COMMAND ----------

results

# COMMAND ----------

pit_stops_cleaned

# COMMAND ----------

pit_stops_cleaned['raceId'].nunique()

# COMMAND ----------

races['raceId'].nunique()

# COMMAND ----------

results.info()

# COMMAND ----------

results['raceId'].nunique()

# COMMAND ----------

races.info()

# COMMAND ----------

results[results['position']== "1"]['raceId'].nunique()

# COMMAND ----------

# MAGIC %md ### 2. Rank the average time spent at the pit stop in order of who won each race

# COMMAND ----------

### Dropping rows with NA values 
pit_stops_cleaned = pit_stops.dropna()

### Grouping cleaned df by driverId and raceId to get mean milliseconds of each driver for each race
average_driver_race = pd.DataFrame(pit_stops_cleaned.groupby(['driverId'])[['milliseconds']].mean())

### Creating new column where milliseconds is converted to seconds
average_driver_race[['average_seconds']] = round((average_driver_race[['milliseconds']] * .001), 2)

average_driver_race.head()

# COMMAND ----------

average_driver_ungrouped = average_driver_race.reset_index()

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
average_driver_names = driver_race_results.merge(average_driver_ungrouped, how = "left", on = ['driverId'])


# COMMAND ----------

average_driver_names['raceId'].nunique()

# COMMAND ----------

### Dropping '\\N' values from average driver df 
average_driver_names['position'] = average_driver_names['position'].replace('\\N', np.nan)
average_driver_names = average_driver_names[average_driver_names['position'].notnull()]

# COMMAND ----------

average_driver_names

# COMMAND ----------

### Converting rank column into integers
average_driver_names['position'] = average_driver_names['position'].astype('int')

# COMMAND ----------

### Filtering average driver time df for winners
race_winners = average_driver_names[average_driver_names['position'] == 1]

# COMMAND ----------

### Merging race name into df
merged_avg_race = race_winners.merge(race_name, how = 'left', on = 'raceId')

### Ranking average time spent at pit stop based on who won race
merged_avg_race['pit_time_rank'] = merged_avg_race['average_seconds'].rank(method = 'min')

### Changing name of name colum to race name
merged_avg_race.rename(columns = {'name':'race_name'}, inplace = True)

###Dropping all null pit time rank values
merged_avg_race = merged_avg_race[merged_avg_race['pit_time_rank'].notnull()]


# COMMAND ----------

# MAGIC %md As you can see from the table below, it is hard to actually rank the average time spent at the pit stop in order of who won each race, because the pit stop data is missing data for a lot of races. I tried to preserve all of the races, but there was a lot of missing pit stop time data. So, in the end we only have a ranking of 439 rows, and of those rows there are a lot of ties. I also chose to average the pit stop times exclusively by driver, because if I were to average by race and driver there would be even more null rows of data to merge with the race result data. 

# COMMAND ----------

display(merged_avg_race)

# COMMAND ----------

# MAGIC %md ### 4. Who is the youngest and oldest driver for each race? Create a new column called "Age"

# COMMAND ----------

### Pulling driverId, dob, forename, surname from Drivers df
driver_dob = drivers[['driverId', 'dob', 'forename', 'surname']]

### Pulling raceId, race name, and race dates from race df
race_date = races[['raceId', 'date', 'name']]

### Merging dobs to results
dob_merged_df = pd.merge(results, driver_dob, how = 'left', on = 'driverId')

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

### Creating df of oldest_racers
oldest_idx= race_dob_merged.groupby('raceId')['age_rank'].transform(max) == race_dob_merged['age_rank']

oldest_racers = race_dob_merged[oldest_idx]

# COMMAND ----------

oldest_racers

# COMMAND ----------

### Combining the df of youngest and oldest racer in each race

oldest_youngestdf = [youngest_racers, oldest_racers]

oldest_youngestdf = pd.concat(oldest_youngestdf)

# COMMAND ----------

### Table of youngest and oldest driver of each race
oldest_youngestdf = oldest_youngestdf[['raceId', 'driverId', 'forename', 'surname', 'name', 'age_at_race']]

display(oldest_youngestdf)

# COMMAND ----------

oldest_youngestdf['raceId'].nunique()
