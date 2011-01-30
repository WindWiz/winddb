#! /usr/bin/env python
# Copyright (c) 2010-2011 Magnus Olsson (magnus@minimum.se)
# See LICENSE for details

"""winddb - WindWiz webdata generator
  
usage: winddb [options]

options:
-f <cfgfile>	Database configuration (defaults to 'winddb.conf')
-s <station>	Station to produce output for (omit to output for all)
-d <dir>		Output directory (defaults to '.')
-l <samples>	Number of samples to output (defaults to 30)
-x				Do not generate XML 
-j				Do not generate JSON
"""

import ConfigParser
import MySQLdb
import getopt
import sys
import os.path
import simplejson as json

global db
global outputdir
global do_xml
global do_json
global samplelimit

xml_sample_prolog = """<?xml version='1.0' encoding='UTF-8' standalone='yes' ?>
<!DOCTYPE history [
	<!ELEMENT history (sampleperiod+)>
	<!ELEMENT sampleperiod EMPTY>

	<!ATTLIST history
		stationid CDATA "Dummy"
	>

	<!ATTLIST sampleperiod
		num_samples CDATA "0"
		first_sample CDATA "0"
		last_sample CDATA "0"
		windspeed_max CDATA "0.0"
		windspeed_min CDATA "0.0"
		windspeed_avg CDATA "0.0"
		airtemp_avg	CDATA "0.0"
		winddir_avg	CDATA "0.0"
		winddir_stability CDATA	"0.0"
		humidity CDATA "0.0"
		air_pressure CDATA "0.0"
	>
]>
"""

xml_index_prolog = """<?xml version='1.0' encoding='UTF-8' standalone='yes' ?>
<!DOCTYPE stations [
	<!ELEMENT stations (station*)>
	<!ELEMENT station EMPTY>

	<!ATTLIST station
		id CDATA "0"
		friendlyname CDATA "None"
		lastupdate CDATA "0"
	>
]>
"""

xml_info_prolog = """<?xml version='1.0' encoding='UTF-8' standalone='yes' ?>
<!DOCTYPE info [
	<!ELEMENT info EMPTY>

	<!ATTLIST info
		stationid CDATA "0"
		friendlyname CDATA "None"
		pollrate CDATA "0"
		pos_lat CDATA "0.0"
		pos_lon	CDATA "0.0"
	>
]>
"""

xml_sample = '  <sampleperiod num_samples="%d" first_sample="%d" \
last_sample="%d" windspeed_max="%f" windspeed_min="%f" windspeed_avg="%f" \
airtemp_avg="%f" winddir_avg="%f" winddir_stability="%f" humidity="%s" \
air_pressure="%s"/>\n'

xml_station = '  <station id="%s" lastupdate="%d" friendlyname="%s"/>\n'

xml_info = '<info id="%s" friendlyname="%s" pollrate="%d" pos_lat="%s" \
pos_lon="%s"/>\n'

def usage(*args):
	sys.stdout = sys.stderr
	print __doc__
	for msg in args:
		print msg
	sys.exit(2)

def get_stations():
	cur = db.cursor()
	stationList = []
	if (not cur.execute("SELECT stationid FROM winddb_stations")):
		print "Failed to fetch stationlist"
		return stationList
	
	while (1):
		row = cur.fetchone()
		if row == None:
			break
		stationList.append(row[0])

	cur.close()
	return stationList

def output_latest_json(filepath, latest):
	out = open(filepath, 'w')
	out.write(json.dumps(latest, indent=2))
	out.close()

def output_history_json(filepath, history):
	out = open(filepath, 'w')
	out.write(json.dumps(history, indent=2))
	out.close()

def output_info_json(filepath, info):
	out = open(filepath, 'w')
	out.write(json.dumps(info, indent=2))
	out.close()

def output_latest_xml(filepath, period):
	out = open(filepath, 'w')
	out.write(xml_sample_prolog)
	out.write('<history stationid="%s">\n' % period["stationid"])
	out.write(xml_sample % (period['num_samples'], period['first_sample'], 
		period['last_sample'], period['windspeed_max'], 
		period['windspeed_min'], period['windspeed_avg'], 
		period['airtemp_avg'], period['winddir_avg'], 
		period['winddir_stability'], 
		period['humidity'] if period['humidity'] is not None else "NULL", 
		period['air_pressure'] if period['air_pressure'] is not None else "NULL"))
	out.write('</history>')
	out.close()

def output_history_xml(filepath, history):
	out = open(filepath, 'w')
	out.write(xml_sample_prolog)
	out.write('<history stationid="%s">\n' % history[0]["stationid"])
	for period in history:
		out.write(xml_sample % (period['num_samples'], period['first_sample'], 
							period['last_sample'], period['windspeed_max'], 
							period['windspeed_min'], period['windspeed_avg'], 
							period['airtemp_avg'], period['winddir_avg'], 
							period['winddir_stability'], 
							period['humidity'] if period['humidity'] is not None else "NULL", 
							period['air_pressure'] if period['air_pressure'] is not None else "NULL"))
	out.write('</history>')
	out.close()
	
def output_info_xml(filepath, info):
	out = open(filepath, 'w')
	out.write(xml_info_prolog)
	out.write(xml_info % (info['stationid'], info["friendlyname"], 
		info["pollrate"], 
		info["pos_lat"] if info["pos_lat"] is not None else "NULL",
		info["pos_lon"] if info["pos_lon"] is not None else "NULL"))
	out.close()
	
def build_station(stationid):
	cur = db.cursor(MySQLdb.cursors.DictCursor)
	samples = []

	q = """SELECT stationid, num_samples, first_sample, last_sample, 
		ROUND(windspeed_max, 2) as windspeed_max, 
		ROUND(windspeed_min, 2) as windspeed_min, 
		ROUND(windspeed_avg, 2) as windspeed_avg, 
		ROUND(winddir_avg, 0) as winddir_avg, 
		ROUND(winddir_stability, 1) as winddir_stability, 
		ROUND(airtemp_avg, 2) as airtemp_avg, 
		humidity, 
		air_pressure 
		FROM winddb_samples WHERE stationid = '%s' 
		ORDER BY last_sample DESC
		LIMIT %d""" % (stationid, samplelimit)

	if (not cur.execute(q)):
		print "Failed to fetch data for station '%s'" % stationid
		return
	
	# Collect the history
	history = []
	while (1):
		row = cur.fetchone()
		if row == None:
			break
		row["windspeed_max"] = float(row["windspeed_max"])
		row["windspeed_min"] = float(row["windspeed_min"])
		row["windspeed_avg"] = float(row["windspeed_avg"])
		row["winddir_avg"] = float(row["winddir_avg"])
		row["winddir_stability"] = float(row["winddir_stability"])
		row["airtemp_avg"] = float(row["airtemp_avg"])	
		history.append(row)

	stationdir = os.path.join(outputdir, stationid)
	try:
		os.makedirs(stationdir)
	except OSError: pass

	# Collect the station info
	q = """SELECT stationid, friendlyname, pollrate, position_lat as pos_lat, position_lon as pos_lon
		   FROM winddb_stations 
		   WHERE stationid='%s' 
		   LIMIT 1"""
	if (not cur.execute(q % stationid)):
		print "Failed to fetch station info for station '%s'" % stationid
		return

	info = cur.fetchone()
	if info == None:
		print "No such station '%s' (fetch station info)" % stationid
		return
			
	cur.close()		
	
	if (do_json): 
		output_latest_json(os.path.join(stationdir, "latest.json"), history[0])
		output_history_json(os.path.join(stationdir, "history.json"), history)
		output_info_json(os.path.join(stationdir, "info.json"), info)
		
	if (do_xml): 
		output_latest_xml(os.path.join(stationdir, "latest.xml"), history[0])
		output_history_xml(os.path.join(stationdir, "history.xml"), history)
		output_info_xml(os.path.join(stationdir, "info.xml"), info)

def output_index_json(filepath, stations):
	out = open(filepath, 'w')
	out.write(json.dumps(stations, indent=2))
	out.close()

def output_index_xml(filepath, stations):
	out = open(filepath, 'w')
	out.write(xml_index_prolog)
	out.write('<stations>\n')
	for station in stations:
		out.write(xml_station % (station['id'], station['lastupdate'], 
					station['friendlyname']))
	out.write('</stations>')
	out.close()	

def build_index():
	cur = db.cursor(MySQLdb.cursors.DictCursor)

	q = """SELECT a.stationid as id, MAX(b.last_sample) as lastupdate, a.friendlyname 
		FROM winddb_stations a
		LEFT JOIN (winddb_samples b) ON (a.stationid = b.stationid)
		GROUP BY b.stationid
		HAVING lastupdate IS NOT NULL
		ORDER BY a.stationid ASC"""

	if (not cur.execute(q)):
		print "Failed to fetch stationindex"
		return

	stations = []
	while (1):
		row = cur.fetchone()
		if row == None:
			break
		stations.append(row)

	cur.close()

	if (do_json):
		output_index_json(os.path.join(outputdir, "index.json"), stations)
	
	if (do_xml):
		output_index_xml(os.path.join(outputdir, "index.xml"), stations)

				
if __name__ == "__main__":
	config = ConfigParser.RawConfigParser({'dbhost': 'localhost',
										   'dbpass': '',
										   'dbuser': 'winddb',
										   'dbname': 'winddb'})

	cfgfile = "winddb.conf"
	outputdir = "."	
	do_xml = True
	do_json = True
	station = False
	samplelimit = 30

	try:
		opts, args = getopt.getopt(sys.argv[1:], 'f:s:d:xjcl:')
	except getopt.error, msg:
		usage(msg)

	for o, a in opts:
		if o == '-f': cfgfile = a
		if o == '-s': station = a
		if o == '-d': outputdir = a
		if o == '-x': do_xml = False
		if o == '-j': do_json = False
		if o == '-l': samplelimit = int(a)

	if not os.path.isdir(outputdir):
		print "'%s' is not a directory, aborting" % outputdir
		sys.exit(1)
		if not os.access(outputdir, os.W_OK):
			print "'%s' is not a writable directory, aborting" % outputdir
			sys.exit(1)

	outputdir = os.path.abspath(outputdir)

	config.read(cfgfile)

	db = MySQLdb.connect(host=config.get('mysql', 'dbhost'), 
						 user=config.get('mysql', 'dbuser'),
						 passwd=config.get('mysql', 'dbpass'), 
						 db=config.get('mysql', 'dbname'))
		
	if (station == False):
		stationlist = get_stations()
	else:
		stationlist = [station]

	print "Station(s) to process: %s" % stationlist
	for stationid in stationlist:
		build_station(stationid)
	
	build_index()