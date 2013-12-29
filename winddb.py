#!/usr/bin/env python
# Copyright (c) 2010-2012 Magnus Olsson (magnus@minimum.se)
# See LICENSE for details

"""winddb - WindWiz webdata generator
  
usage: winddb [options]

options:
-f <dbfile>     WindDB Database file (defaults to wind.db)
-x <configfile> WindDB configuration file (defaults to winddb.conf)
-s <station>    Station to produce output for (omit to output for all)
-o <dir>        Output directory (defaults to '.')
-a <age>        Maximum sample age to process, in minutes (defaults to 180)
-j              Do not generate JSON
-p              Do not generate JSONP
-i <indent>     Number of JSON indentations spaces (defaults to 0)
-c <callback>   JSONP callback function (defaults to 'callback')
"""

import getopt
import sys
import os.path
import simplejson as json
import codecs
import sqlite3
import time

# plugins
import awsxd
import osod

def usage(*args):
	sys.stdout = sys.stderr
	print __doc__
	for msg in args:
		print msg
	sys.exit(1)

def create_database_table(db):
        query = """CREATE TABLE IF NOT EXISTS stations (
		id varchar(255),
		friendlyname varchar(255),
		pollrate int,
		position_lat float,
		position_lon float,
		description text,
		handler VARCHAR(255),
		PRIMARY KEY (id))"""

        cursor = db.cursor()
        success = cursor.execute(query)
        cursor.close()
        db.commit()

        return success

class station(object):
	def __init__(self, fields, source):
		self.fields = fields
		self.source = source
		self.fields['lastupdate'] = self.get_latest_update()

	def __getattr__(self, name):
		if name in self.fields:
			return self.fields[name]
		else:
			raise AttributeError

	def get_samples(self, t):
		return self.source.get_samples(self.id, t)
		
	def get_latest_update(self):
		return self.source.get_latest_update(self.id)
	
def write_station(station, outputdir, do_json, do_jsonp, t, callback, indent):
	samples = station.get_samples(t)
	
	if samples is None:
		print "warning: No samples for '%s'" % station.id
		return

	stationdir = os.path.join(outputdir, station.id)
	try:
		os.makedirs(stationdir)
	except OSError: 
		pass

	latest = json.dumps(samples[0], indent=indent)
	history = json.dumps(samples, indent=indent)
	
	if (do_json):
		out = open(os.path.join(stationdir, "latest.json"), 'w')
		out.write(latest)
		out.close()
		out = open(os.path.join(stationdir, "history.json"), 'w')
		out.write(history)
		out.close()
		
	if (do_jsonp):
		out = open(os.path.join(stationdir, "latest.jsonp"), 'w')
		out.write("%s(%s);" % (callback, latest))
		out.close()
		out = open(os.path.join(stationdir, "history.jsonp"), 'w')
		out.write("%s(%s);" % (callback, history))
		out.close()

def write_index(stations, outputdir, do_json, do_jsonp, callback, indent):
	try:
		os.makedirs(outputdir)
	except OSError: 
		pass

	index = json.dumps(map(lambda x: x.fields, stations))

	if (do_json):
		out = open(os.path.join(outputdir, "index.json"), 'w')
		out.write(index)
		out.close()

	if (do_jsonp):
		out = open(os.path.join(outputdir, "index.jsonp"), 'w')
		out.write("%s(%s);" % (callback, index))
		out.close()

def enum_sources(configfile):
	infile = open(configfile, 'r')
	config = json.load(infile)
	infile.close()

	# TODO: Enumerate sources from subclasses of source.source
	# for x in source.source.__subclasses__(): ... 
	a = awsxd.awsxd(config['awsxd'] if 'awsxd' in config else {})
	o = osod.osod(config['osod'] if 'osod' in config else {})
	
	return { a.name: a, 
			 o.name: o }

def get_stations(configfile, db, t, idfilter = None):
	cursor = db.cursor()
	if idfilter is None:
		ret = cursor.execute("SELECT * FROM stations ORDER BY id")
	else:
		ret = cursor.execute("SELECT * FROM stations WHERE id LIKE ? ORDER BY id",
						   	 (idfilter, ))

	if (not ret):
		raise Exception("Failed to fetch station list")

	sources = enum_sources(configfile)
	stations = []
	for row in cursor:
		handler = row['handler']
		if (handler not in sources):
			print "warning: %s doesnt have a valid handler (%s)" % (row['id'],
																	handler)
			continue

		fields = {'id': row['id'],
				  'friendlyname': row['friendlyname'],
				  'pollrate': row['pollrate'],
				  'pos_lat': row['position_lat'],
				  'pos_lon': row['position_lon'],
				  'description': row['description']}

		s = station(fields, sources[handler])
		# Filter away inactive stations (no samples)
		if (s.lastupdate is None):
			print "warning: no samples for station %s" % s.id
			continue
			
		# Filter away inactive stations (old samples)			
		if (s.lastupdate < t):
			print "warning: skipping %s (latest sample %d < %d)" % (s.id, s.lastupdate, t)
			continue

		stations.append(s)

	return stations

if __name__ == "__main__":
	scriptdir = os.path.dirname(os.path.realpath(__file__))
	outputdir = '.'
	do_json = True
	do_jsonp = True
	maxage = 180
	station_name = None
	callback = 'callback'
	dbfile = os.path.join(scriptdir, 'wind.db')
	indent = 0
	configfile = os.path.join(scriptdir, 'winddb.conf')

	try:
		opts, args = getopt.getopt(sys.argv[1:], 'f:s:o:pjc:a:i:x:')
	except getopt.error, msg:
		usage(msg)

	for o, a in opts:
		if o == '-x': configfile = a
		if o == '-f': dbfile = a
		if o == '-s': station_name = a
		if o == '-o': outputdir = a
		if o == '-p': do_jsonp = False
		if o == '-j': do_json = False
		if o == '-c': callback = a
		if o == '-a': maxage = int(a)
		if o == '-i': indent = int(a)

	if not os.path.isdir(outputdir):
		print("'%s' is not a directory, aborting" % outputdir)
		sys.exit(1)
		
	if not os.access(outputdir, os.W_OK):
		print("'%s' is not a writable directory, aborting" % outputdir)
		sys.exit(1)

	outputdir = os.path.abspath(outputdir)
	db = sqlite3.connect(dbfile)
	db.row_factory = sqlite3.Row	# "dict-cursor"-ish support
	create_database_table(db)

	t = time.time() - (maxage * 60)
	stations = get_stations(configfile, db, t, station_name)
	
	write_index(stations, outputdir, do_json, do_jsonp, callback, indent)	
	for station in stations:
		write_station(station, outputdir, do_json, do_jsonp, t, callback, indent)

	db.close()
