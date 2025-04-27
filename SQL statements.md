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

CREATE TABLE bike_stations (
  id int PRIMARY KEY,
  station_name varchar(50),
  station_latitude real,
  station_longitude real
);

ALTER TABLE trips
ADD start_station_id int
REFERENCES bike_stations(id);

ALTER TABLE trips
ADD end_station_id int
REFERENCES bike_stations(id);

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