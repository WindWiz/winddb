CREATE TABLE winddb_stations (
  stationid varchar(255) NOT NULL,
  friendlyname varchar(255) NOT NULL,
  pollrate int(10) unsigned NOT NULL,
  position_lat float default NULL,
  position_lon float default NULL,
  description text default NULL,
  UNIQUE KEY stationid (stationid)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE VIEW
	winddb_samples 
AS
	SELECT 
		a.stationid,
		COUNT(a.id) as num_samples,
		MIN(a.sample_time) as first_sample,
		MAX(a.sample_time) as last_sample,
		MAX(a.wind_speed) as windspeed_max,
		MIN(a.wind_speed) as windspeed_min,
		AVG(a.wind_speed) as windspeed_avg,
		AVG(a.airtemp) as airtemp_avg,
		AVG(a.wind_dir) as winddir_avg,					/* FIXME */
		STDDEV(a.wind_dir) as winddir_stability,
		NULL as humidity,
		NULL as air_pressure
	FROM 
		windnode as a
	LEFT JOIN
		(winddb_stations as b) ON (b.stationid = a.stationid)
	WHERE
		b.stationid IS NOT NULL
	GROUP BY 
		FLOOR((UNIX_TIMESTAMP()-a.receive_time) / (60*60)), 
		a.stationid

	UNION ALL 

	SELECT
		a.station,
		4,
		UNIX_TIMESTAMP(a.create_stamp) - (a.sample_interval*60) as first_sample,
		UNIX_TIMESTAMP(a.create_stamp) as last_sample,
		a.wind_max/3.6,
		a.wind_min/3.6,
		a.wind_avg/3.6,
		a.temp_avg,
		a.wind_dir,
		a.wind_stability,
		a.humidity,
		a.air_pressure
	FROM
		awsx as a
	LEFT JOIN
		(winddb_stations as b) ON (b.stationid = a.station)
	WHERE 
		b.stationid IS NOT NULL;
		

