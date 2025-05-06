&emsp;This is my submission for the Codecademy Data Engineer Career Path data management project. \*__*Click on the bike_rental.ipynb file to view the FULL PROJECT.*__\*

&emsp;The 'data' folder contains the original data files used for the project. The 'data-dictionaries' folder contains files that give more detail on what data is included and how the data is measured.

&emsp;The 'database_screenshots' folder contains screenshots of the Postbird app, showing how the database's tables and views look on my computer.

&emsp;The 'bike_rental_database_schema.png' file shows the database schema.

&emsp;To view __only__ the __INTRODUCTION__ and __SUMMARY__, continue reading below.

&emsp;To view __only__ the __SQL STATEMENTS__, click on the 'SQL statements.md' file.



## Introduction

&emsp;This project represents my submission for the Data Management Project from Codecademy's Data Engineer Career Path. In this Career Path, students undergo approximately 90 hours of study in order to learn how to create robust and resilient data pipelines that connect data sources to analytics tools.

&emsp;The goal of this project is to clean and wrangle data, create a database schema, create a PostgreSQL database, and create a few views based on the data in the database.

&emsp;Data for this project was provided by Lyft and NOAA. Lyft provided 12 csv files with 2016 Citi Bike ridership data from New York City, separated by month. NOAA provided 1 csv file with 2016 weather data from a Newark weather station.

&emsp;The Citi Bike data includes 247,584 rows and the following 15 columns:
- `Trip Duration`
- `Start Time`
- `Stop Time`
- `Start Station ID`
- `Start Station Name`
- `Start Station Latitude`
- `Start Station Longitude`
- `End Station ID`
- `End Station Name`
- `End Station Latitude`
- `End Station Longitude`
- `Bike ID`
- `User Type`
- `Birth Year`
- `Gender`

&emsp;The NOAA data includes 366 rows and the following 16 columns:
- `STATION`
- `NAME`
- `DATE`
- `AWND`
- `PGTM`
- `PRCP`
- `SNOW`
- `SNWD`
- `TAVG`
- `TMAX`
- `TMIN`
- `TSUN`
- `WDF2`
- `WDF5`
- `WSF2`
- `WSF5`

&emsp;There are also data dictionary files that help to show what each column means and how each column is measured.



## Summary

&emsp;To start, I imported all the Citi Bike files and saved them as 1 DataFrame. Then, I inspected the data with the `.head()`, `.describe()`, and `.info()` methods.

&emsp;After inspecting the data, I decided to change the `Start Time` and `Stop Time` columns to `datetime` values. Then, I replaced the null values in the `Birth Year` column with zeroes, so that I could change the column to an `int` type (Years do not have decimal values). Finally, I decided to replace the null values in the `User Type` column with the string `'Unknown'`. This removed all null values from the data. As a side note, there are also zeros representing null values in the `Gender` column, but for this scenario, I would prefer to allow Data Analysts, Data Scientists, etc. to decide if they want to discard rows of data because of null values.

&emsp;Next, I decided to change the column names, to make them easier to write code with (e.g. I can type `df.column` instead of `df['column']`). The new column names are: `trip_duration`,`start_datetime`, `end_datetime`, `start_station_id`, `start_station_name`, `start_station_latitude`, `start_station_longitude`, `end_station_id`, `end_station_name`, `end_station_latitude`, `end_station_longitude`, `bike_id`, `user_type`, `user_birth_year`, and `user_gender`. I also decided to separate the `start_datetime` and `end_datetime` columns into `start_date`, `start_time`, `end_date`, and `end_time` to make it easier to analyze the data based on date and/or time. In addition, I added an `id` column to give each row an ID and a `weather_station_id` column to associate each row with the NOAA data.

&emsp;With the Citi Bike data cleaned and wrangled, it was time to start working on the NOAA weather data. I began my importing the data and inspecting it with the `.head()`, `.describe()`, and `.info()` methods. The two columns `PGTM` and `TSUN` held no data, so I dropped them from the DataFrame. Then, I looked in the data dictionary and decided to rename each column to something more readable and easier to write code with. The new column names are: `id`, `station_name`, `date`, `avg_wind_speed`, `precipitation`, `snow`, `snow_depth`, `temp_avg`, `temp_high`, `temp_low`, `two_min_wind_dir`, `five_sec_wind_dir`, `two_min_wind_speed`, and `five_sec_wind_speed`.

&emsp;I noticed that there were two null values in the `five_sec_wind_dir` column and two null values in the `five_sec_wind_speed` column. I decided to replace the null values with values from the corresponding `two_min_wind_dir` and `two_min_wind_speed` columns, simply because it makes sense to me that a five-second measurement can be taken from a two-minute measurement. I am not a meteorologist, though. So, I would ask a more informed individual what to do with this, in a real-world scenario.

&emsp;In addition, I noticed that the `five_sec_wind_dir` column was a `float` type because of the null values. With the null values gone, I decided to change it to an `int` to match the `two_min_wind_dir` column. Furthermore, I decided to make the `date` column a `datetime` type.

&emsp;The last thing I wanted to do before creating the actual database was to separate the bike station data into its own DataFrame, so I could easily make a separate table out of it. I started by creating a DataFrame with only the `start_station_id`, `start_station_name`, `start_station_latitude`, and `start_station_longitude` columns from the original DataFrame. Then I renamed the columns to `id`, `station_name`, `station_latitude`, and `station_longitude`. I did the same thing with the four `end_station` columns, and two new DataFrames together. Then, I removed the duplicates from the final DataFrame, and I was ready to start creating the database.

&emsp;Before writing any SQL, I went to [sqldbm.com](sqldbm.com) to draw out the database schema. What I came up with is shown in the included 'bike_rental_database_schema.png' file. I basically created a schema with three different tables and column names to match the column names of the three associated DataFrames. Next, I had to write the actual SQL statements.

&emsp;First, the `trips` table was created with the following SQL statement:
~~~~sql
CREATE TABLE trips (
  id int PRIMARY KEY,
  trip_duration int,
  start_time time,
  end_time time,
  bike_id int,
  user_type varchar(20),
  user_birth_year int,
  user_gender int
);
~~~~

&emsp;Then, the `bike_stations` table was created:
~~~~sql
CREATE TABLE bike_stations (
  id int PRIMARY KEY,
  station_name varchar(50),
  station_latitude real,
  station_longitude real
);
~~~~

&emsp;After that, the `start_station_id` and `end_station_id` columns were added to the `trips` table with association to the `id` column from the `bike_stations` table:
~~~~sql
ALTER TABLE trips
ADD start_station_id int
REFERENCES bike_stations(id);

ALTER TABLE trips
ADD end_station_id int
REFERENCES bike_stations(id);
~~~~

&emsp;Thereafter, the `weather_stations` table was created:
~~~~sql
CREATE TABLE weather_stations (
  id varchar(20),
  date date, 
  station_name text,
  avg_wind_speed real,
  precipitation real,
  snow real,
  snow_depth real,
  temp_avg int,
  temp_high int,
  temp_low int,
  two_min_wind_dir int,
  five_sec_wind_dir int,
  two_min_wind_speed real,
  five_sec_wind_speed real,
  PRIMARY KEY (id, date)
);
~~~~

&emsp;Finally, the `weather_station_id`, `start_date`, and `end_date` columns were added to the `trips` table, with foreign key associations to columns from the `weather_stations` table:
~~~~sql
ALTER TABLE trips
ADD weather_station_id varchar(20);

ALTER TABLE trips
ADD start_date date;

ALTER TABLE trips
ADD end_date date;

ALTER TABLE trips
ADD CONSTRAINT trips_weather_station_id_fkey
FOREIGN KEY (weather_station_id, start_date)
REFERENCES weather_stations(id, date);
~~~~

&emsp;Once the tables were created in the database, I used SQLAlchemy to insert all of the data from the three DataFrames into the three tables. In the end, my final task was to create a few views that might be helpful to Data Analysts or Scientists who work on this data.

&emsp;One view that I created shows the total Citi Bike rides per month and the total rides broken down by user type. It was created with the following SQL statement:
~~~~sql
CREATE VIEW monthly_rides AS
WITH months AS 
(SELECT
CAST('2016-01-01' AS date) AS first_day,
CAST('2016-01-31' AS date) AS last_day
 UNION
 SELECT
CAST('2016-02-01' AS date) AS first_day,
CAST('2016-02-28' AS date) AS last_day
 UNION
 SELECT
CAST('2016-03-01' AS date) AS first_day,
CAST('2016-03-31' AS date) AS last_day
 UNION
 SELECT
CAST('2016-04-01' AS date) AS first_day,
CAST('2016-04-30' AS date) AS last_day
 UNION
 SELECT
CAST('2016-05-01' AS date) AS first_day,
CAST('2016-05-31' AS date) AS last_day
 UNION
 SELECT
CAST('2016-06-01' AS date) AS first_day,
CAST('2016-06-30' AS date) AS last_day
 UNION
 SELECT
CAST('2016-07-01' AS date) AS first_day,
CAST('2016-07-31' AS date) AS last_day
 UNION
 SELECT
CAST('2016-08-01' AS date) AS first_day,
CAST('2016-08-31' AS date) AS last_day
 UNION
 SELECT
CAST('2016-09-01' AS date) AS first_day,
CAST('2016-09-30' AS date) AS last_day
 UNION
 SELECT
CAST('2016-10-01' AS date) AS first_day,
CAST('2016-10-31' AS date) AS last_day
 UNION
 SELECT
CAST('2016-11-01' AS date) AS first_day,
CAST('2016-11-30' AS date) AS last_day
 UNION
 SELECT
CAST('2016-12-01' AS date) AS first_day,
CAST('2016-12-31' AS date) AS last_day
),
cross_join AS
(SELECT *
FROM trips
CROSS JOIN months
),
status AS
(SELECT id,
 first_day AS month,
 CASE
 WHEN 
 (start_date >= first_day)
 AND (end_date <= last_day)
 AND (user_type = 'Subscriber')
 THEN 1
 ELSE 0
 END AS is_subscriber,
 CASE
 WHEN 
 (start_date >= first_day)
 AND (end_date <= last_day)
 AND (user_type = 'Customer')
 THEN 1
 ELSE 0
 END AS is_customer,
 CASE
 WHEN 
 (start_date >= first_day)
 AND (end_date <= last_day)
 AND (user_type = 'Unknown')
 THEN 1
 ELSE 0
 END AS is_unknown_user
 FROM cross_join
),
status_aggregate AS
(SELECT month,
 SUM(is_subscriber) AS total_subscriber_rides,
 SUM(is_customer) AS total_customer_rides,
 SUM(is_unknown_user) AS total_unknown_user_rides
 FROM status
 GROUP BY month
)
SELECT
month,
total_subscriber_rides,
total_customer_rides,
total_unknown_user_rides,
total_subscriber_rides + total_customer_rides + total_unknown_user_rides AS total_rides
FROM status_aggregate;
~~~~

&emsp;The other view that I created shows the data from each trip, along with the corresponding bike station and weather data. It was created with the following SQL statement:
~~~~sql
CREATE VIEW trips_and_weather AS
WITH table_1 AS (
SELECT 
trips.id AS trip_id,
start_date,
start_time,
end_date,
end_time,
trip_duration,
bike_id,
start_station_id AS bike_start_station_id,
bike_stations.station_name AS bike_start_station_name,
station_latitude AS bike_start_station_latitude,
station_longitude AS bike_start_station_longitude,
end_station_id AS bike_end_station_id,
user_type,
user_birth_year,
user_gender,
weather_station_id,
weather_stations.station_name AS weather_station_name,
avg_wind_speed,
precipitation,
snow,
snow_depth,
temp_avg,
temp_high,
temp_low,
two_min_wind_dir,
five_sec_wind_dir,
two_min_wind_speed,
five_sec_wind_speed
FROM trips
JOIN
weather_stations
ON trips.weather_station_id = weather_stations.id
AND trips.start_date = weather_stations.date
JOIN
bike_stations
ON trips.start_station_id = bike_stations.id
)
SELECT
trip_id,
start_date,
start_time,
end_date,
end_time,
trip_duration,
bike_id,
bike_start_station_id,
bike_start_station_name,
bike_start_station_latitude,
bike_start_station_longitude,
bike_end_station_id,
bike_stations.station_name AS bike_end_station_name,
station_latitude AS bike_end_station_latitude,
station_longitude AS bike_end_station_longitude,
user_type,
user_birth_year,
user_gender,
weather_station_id,
weather_station_name,
avg_wind_speed,
precipitation,
snow,
snow_depth,
temp_avg,
temp_high,
temp_low,
two_min_wind_dir,
five_sec_wind_dir,
two_min_wind_speed,
five_sec_wind_speed
FROM table_1
JOIN
bike_stations
ON table_1.bike_end_station_id = bike_stations.id;
~~~~

&emsp;In conclusion, I cleaned and wrangled Citi Bike data from Lyft and weather data from NOAA. Then, I drew up a database schema, created a PostgreSQL database based on the schema, and used SQLAlchemy to insert the data into the database's tables. Lastly, I created views based on what someone might want to see from the data. This project was a great way to get hands on with pandas, PostgreSQL, and SQLAlchemy. I will be able to use the hands-on experience for any data role that I take on in the future.