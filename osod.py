#!/usr/bin/env python
# Copyright (c) 2010-2012 Magnus Olsson (magnus@minimum.se)
# See LICENSE for details

import sqlite3
from source import source
import datetime

class osod(source):
	def __init__(self, config):
		source.__init__(self, "osod", config)
		dbfile = self.config['db'] if 'db' in config else "osod.db"
		self.periodtime = self.config['periodtime'] if 'periodtime' in config else 5
		self.db = sqlite3.connect(dbfile)
		self.db.row_factory = sqlite3.Row
		
	def get_latest_update(self, station):
		cursor = self.db.cursor()
		query = """SELECT 
		MAX(sample_tstamp) as last_sample
		FROM osod
		WHERE instance = ? 
		GROUP BY sample_tstamp/(pollrate*%d)
		ORDER BY last_sample DESC
		LIMIT 1,1""" % (self.periodtime)
		
		if not cursor.execute(query, (station, )):
			return None

		row = cursor.fetchone()
		cursor.close()

		if row is None:
			return None

		return row['last_sample']
				
	def get_samples(self, station, t):
		cursor = self.db.cursor()
		print datetime.datetime.utcfromtimestamp(t)
		query = """SELECT 
		COUNT(*) as num_samples, 
		MAX(sample_tstamp) as last_sample, 
		(MIN(sample_tstamp) - pollrate) as first_sample,
		MAX(windspeed_max) as windspeed_max, 
		MIN(windspeed_min) as windspeed_min,
		AVG(windspeed_avg) as windspeed_avg, 
		AVG(airtemp_avg) as airtemp_avg, 
		ROUND(AVG(wind_dir),0) as winddir_avg, 
		0 as winddir_stability, 
		AVG(humidity) as humidity, 
		AVG(airpressure) as airpressure 
		FROM osod 
		WHERE instance = ? 
		GROUP BY sample_tstamp/(pollrate*%d)
		HAVING first_sample >= ?
		ORDER BY last_sample DESC
		LIMIT 1,999999""" % (self.periodtime)
		
		if not cursor.execute(query, (station, t)):
			return None

		samples = []
		for row in cursor:
			sample = {}
			for key in row.keys():
				sample[key] = row[key]
			samples.append(sample)
				 
		cursor.close()				 
				 
		if not samples:
			return None
			
		return samples
