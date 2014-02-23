#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2014 Magnus Olsson (magnus@minimum.se)
# See LICENSE for details

import sqlite3
import logging
import datetime
import time
from source import *

class vivad(Source):
    __row2cap = {
        'AVG_WIND': WINDSPEED_AVG,
        'GUST_WIND': WINDSPEED_MAX,
        'WIND_DIRECTION': WINDDIR_AVG,
        'VISIBILITY': VISIBILITY_AVG,
        'AIR_TEMP': AIRTEMP_AVG,
        'AIR_HUMIDITY': HUMIDITY_AVG,
        'AIR_PRESSURE': AIRPRESSURE_AVG
    }
    __cap2row = {
        WINDSPEED_AVG: 'AVG_WIND',
        WINDSPEED_MAX: 'GUST_WIND',
        WINDDIR_AVG: 'WIND_DIRECTION',
        VISIBILITY_AVG: 'VISIBILITY',
        AIRTEMP_AVG: 'AIR_TEMP',
        HUMIDITY_AVG: 'AIR_HUMIDITY',
        AIRPRESSURE_AVG: 'AIR_PRESSURE',
    }

    def __init__(self, config):
        Source.__init__(self, "vivad", config)
        dbfile = self.config['db'] if 'db' in config else "vivad.db"
        self.db = sqlite3.connect(dbfile)
        self.db.row_factory = sqlite3.Row
        self.caps = {}

    def __get_capabilities(self, station):
        if station in self.caps:
            return self.caps[station]
        else:
            logger = logging.getLogger('winddb.vivad')
            cursor = self.db.cursor()
            query = """SELECT sample_type
            FROM vivad_samples
            WHERE station_name = ?
            GROUP BY sample_type"""

            if not cursor.execute(query, (station, )):
                logger.debug('unable to determine caps for %s' % station)
                return []

            caps = []
            for row in cursor:
                if row['sample_type'] in self.__row2cap:
                    caps.append(self.__row2cap[row['sample_type']])

            self.caps[station] = caps
            logger.debug('%s has capabilities %s' % (station, caps))
            return caps

    def __caps2types(self, caps):
        cols = []
        for cap in caps:
            cols.append(self.__cap2row[cap])

        return cols

    def get_capabilities(self, station):
        return self.__get_capabilities(station)

    def get_samples(self, station, t, x):
        logger = logging.getLogger('winddb.vivad')
        cursor = self.db.cursor()

        types = self.__caps2types(set(x) &
                                  set(self.__get_capabilities(station)))

        query = """SELECT sample_type, sample_value, sample_tstamp
        FROM vivad_samples
        WHERE (station_name = ?) AND (sample_tstamp >= ?) AND
              (sample_type IN (%s))
        """

        # Because we cannot directly bind an array to the IN statement in our
        # query, we expand the array and insert each string separately instead.
        query = query % (", ".join(['?'] * len(types)))

        if not cursor.execute(query, (station, t) + tuple(types)):
            logger.debug('get_samples(%s) failed (t=%d)' % (station, t))
            return None

        samples = {}
        row_count = 0
        for row in cursor:
            row_count += 1
            sample_type = self.__row2cap[row['sample_type']]
            sample_value = float(row['sample_value'])
            sample = {'tstamp': row['sample_tstamp'], 'svalue': sample_value}

            if sample_type not in samples:
                samples[sample_type] = [sample]
            else:
                samples[sample_type].append(sample)

        logger.debug('get_samples(%s) = %d samples (%d types)' %
                     (station, row_count, len(samples)))
        return samples
