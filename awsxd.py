#!/usr/bin/env python
# Copyright (c) 2010-2012 Magnus Olsson (magnus@minimum.se)
# See LICENSE for details

import MySQLdb as mysql
from source import source

class awsxd(source):
	def __init__(self, config):
		source.__init__(self, "awsxd", config)
		dbhost = config['host'] if 'host' in config else "localhost"
		dbuser = config['user'] if 'user' in config else ""
		dbpass = config['pass'] if 'pass' in config else ""
		dbname = config['db'] if 'db' in config else "awsxd"
		
		self.db = mysql.connect(host=dbhost,
					user=dbuser,
					passwd=dbpass,
					db=dbname,
					use_unicode=True,
					charset='utf8')
							
	def get_latest_update(self, station):
		cursor = self.db.cursor(mysql.cursors.DictCursor)

		query = """SELECT 
		UNIX_TIMESTAMP(create_stamp) as last_update
		FROM awsx 
		WHERE station = %s
		ORDER BY last_update DESC
		LIMIT 1"""
		
		if not cursor.execute(query, (station, )):
			return None
			
		row = cursor.fetchone()
		cursor.close()
		
		if row is None:
			return None			
			
		return row['last_update']
								  
	def get_samples(self, station, t):
		cursor = self.db.cursor(mysql.cursors.DictCursor)

		query = """SELECT 
		4 as num_samples, 
		UNIX_TIMESTAMP(create_stamp) as last_sample, 
		(UNIX_TIMESTAMP(create_stamp) - sample_interval*60) as first_sample,
		wind_max as windspeed_max,
		wind_min as windspeed_min,
		wind_avg as windspeed_avg,
		temp_avg as airtemp_avg,
		wind_dir as winddir_avg,
		wind_stability as winddir_stability,
		humidity,
		air_pressure as airpressure
		FROM awsx 
		WHERE station = %s
		HAVING first_sample >= %d
		ORDER BY last_sample DESC""" % ("%s", t)

		if not cursor.execute(query, (station, )):
			return None

		# Collect the samples (fixup MySQL datatypes)
		samples = []
		for row in cursor:
			row["windspeed_max"] = float(row["windspeed_max"])
			row["windspeed_min"] = float(row["windspeed_min"])
			row["windspeed_avg"] = float(row["windspeed_avg"])
			row["winddir_avg"] = float(row["winddir_avg"])
			row["winddir_stability"] = float(row["winddir_stability"])
			row["airtemp_avg"] = float(row["airtemp_avg"]) 		
			samples.append(row)

		cursor.close()
		
		if not samples:
			return None

		return samples

