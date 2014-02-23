#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2010-2014 Magnus Olsson (magnus@minimum.se)
# See LICENSE for details

"""winddb - WindWiz webdata generator

usage: winddb [options]

options:
-s <station>    Only process the specified station (may be specified multiple times).
-g              Do not generate station index.
-d              Enter daemon mode.
-f <dbfile>     WindDB Database file (defaults to wind.db)
-x <configfile> WindDB configuration file (defaults to winddb.conf)
-o <dir>        Output directory (defaults to '.')
-a <age>        Maximum sample age to process, in minutes (defaults to 180)
-i <indent>     Number of JSON indentations spaces (defaults to 0)
-c <callback>   JSONP callback function (defaults to 'callback')

In daemon mode, winddb will listen for source change-events on the locally bound
TCP socket port 10000.
"""

import getopt
import sys
import os.path
import simplejson as json
import codecs
import sqlite3
import time
import socket
import logging

# Plugins
import source
import awsxd
import osod
import vivad

# Daemon mode TCP port
TCP_LISTEN_PORT = 10000

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

def get_sources(config):
    sources = {}
    for source_class in source.Source.__subclasses__():
        source_name = source_class.__name__
        if source_name in config:
            source_cfg = config[source_name]
        else:
            source_cfg = {}
        sources[source_name] = source_class(source_cfg)

    return sources

class Station(object):
    def __init__(self, fields, source):
        self.fields = fields
        self.source = source
        self.caps = source.get_capabilities(self.id)
        self.fields['caps'] = self.caps

    def __getattr__(self, name):
        if name in self.fields:
            return self.fields[name]
        else:
            raise AttributeError

    def get_capabilities(self):
        return self.caps

    def get_samples(self, t, x = None):
        if x is None:
            x = self.caps

        return self.source.get_samples(self.id, t, x)

    def get_latest_tstamp(self, x = None):
        if x is None:
            x = self.caps

        return self.source.get_latest_tstamp(self.id, x)

def write_json(path, json_obj, cfg):
    dirname = os.path.dirname(path)

    try:
        os.makedirs(dirname)
    except OSError:
        pass

    data = json.dumps(json_obj, indent=cfg['indent'])
    out = open(path + '.jsonp', 'w')
    out.write("%s(%s);" % (cfg['callback'], data))
    out.close()

    out = open(path + '.json', 'w')
    out.write(data)
    out.close()

def write_one_station(station, cfg):
    logger = logging.getLogger('winddb')
    t = time.time() - (cfg['maxage'] * 60)
    samples = station.get_samples(t)

    if samples is None:
        print('error: failed to get samples for "%s"' % station.id)
        return

    if len(samples) == 0:
        logger.warning('no samples for "%s"' % station.id)
        return

    stationdir = os.path.join(cfg['outputdir'], station.id)
    for sample_type in samples.keys():
        all_samples = {'samples': samples[sample_type], 'sample_type': sample_type}
        latest = {'samples': samples[sample_type][0], 'sample_type': sample_type}

        path = os.path.join(stationdir, sample_type[0], sample_type[1], "latest")
        write_json(path, latest, cfg)

        path = os.path.join(stationdir, sample_type[0], sample_type[1], "samples")
        write_json(path, all_samples, cfg)

def write_multiple_stations(stations, cfg):
    for station in stations:
        write_one_station(stations[station], cfg)

def write_index(stations, cfg):
    path = os.path.join(cfg['outputdir'], "index")
    write_json(path, map(lambda x: stations[x].fields, stations), cfg)

def get_stations(configfile, db, station_filter):
    cursor = db.cursor()
    ret = cursor.execute("SELECT * FROM stations ORDER BY id")

    if (not ret):
        raise Exception("Failed to fetch station list")

    infile = open(configfile, 'r')
    config = json.load(infile)
    infile.close()

    sources = get_sources(config)
    stations = {}
    for row in cursor:
        station_id = row['id']

        if station_filter and station_id not in station_filter:
                continue

        handler = row['handler']
        if (handler not in sources):
            print "warning: %s doesnt have a valid handler (%s)" % (row['id'],
                                                                    handler)
            continue

        source = sources[handler]
        fields = {
            'id': row['id'],
            'friendlyname': row['friendlyname'],
            'pollrate': row['pollrate'],
            'pos_lat': row['position_lat'],
            'pos_lon': row['position_lon'],
            'description': row['description'],
            'caps': source.get_capabilities(row['id'])
        }

        stations[station_id] = Station(fields, sources[handler])

    return stations

if __name__ == "__main__":
    scriptdir = os.path.dirname(os.path.realpath(__file__))

    # Command line defaults
    station_filter = []
    generate_index = True
    daemon = False
    dbfile = os.path.join(scriptdir, 'wind.db')
    configfile = os.path.join(scriptdir, 'winddb.conf')

    output_cfg = {
        'callback': 'callback',
        'indent': 2,
        'outputdir': '.',
        'maxage': 180
    }

    try:
        opts, args = getopt.getopt(sys.argv[1:], 's:gdf:x:o:a:i:c:')
    except getopt.error, msg:
        usage(msg)

    for o, a in opts:
        if o == '-s': station_filter.append(a)
        if o == '-g': generate_index = False
        if o == '-d': daemon = True
        if o == '-f': dbfile = a
        if o == '-x': configfile = a
        if o == '-o': output_cfg['outputdir'] = a
        if o == '-a': output_cfg['maxage'] = int(a)
        if o == '-i': output_cfg['indent'] = int(a)
        if o == '-c': output_cfg['callback'] = a

    logging.basicConfig()
    logger = logging.getLogger('winddb')
    logger.setLevel(logging.DEBUG)

    if not os.path.isdir(output_cfg['outputdir']):
        print("error: '%s' is not a directory" % output_cfg['outputdir'])
        sys.exit(1)

    if not os.access(output_cfg['outputdir'], os.W_OK):
        print("error: '%s' is not writable" % output_cfg['outputdir'])
        sys.exit(1)

    output_cfg['outputdir'] = os.path.abspath(output_cfg['outputdir'])
    db = sqlite3.connect(dbfile)
    db.row_factory = sqlite3.Row
    create_database_table(db)

    stations = get_stations(configfile, db, station_filter)

    if generate_index:
        write_index(stations, output_cfg)

    write_multiple_stations(stations, output_cfg)

    if daemon:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('127.0.0.1', TCP_LISTEN_PORT))
        server.listen(10)

        logger.info('Awaiting connections...')
        while True:
            client, addr = server.accept()
            file = client.makefile()
            station_id = file.readline().rstrip()
            file.close()
            client.close()

            if station_id in stations:
                write_one_station(stations[station_id], output_cfg)

    db.close()
