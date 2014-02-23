#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2010-2014 Magnus Olsson (magnus@minimum.se)
# See LICENSE for details

import sqlite3
import logging
import datetime
from source import *

class osod(Source):
    capabilities = [
        AIRTEMP_AVG,
        AIRPRESSURE_AVG,
        HUMIDITY_AVG,
        WINDSPEED_MAX,
        WINDSPEED_AVG,
        WINDSPEED_MIN,
        WINDDIR_AVG
    ]

    __cap2row = {
        AIRTEMP_AVG: 'airtemp_avg',
        AIRPRESSURE_AVG: 'airpressure',
        HUMIDITY_AVG: 'humidity',
        WINDSPEED_MAX: 'windspeed_max',
        WINDSPEED_AVG: 'windspeed_avg',
        WINDSPEED_MIN: 'windspeed_min',
        WINDDIR_AVG: 'wind_dir'
    }

    def __init__(self, config):
        Source.__init__(self, "osod", config)
        dbfile = self.config['db'] if 'db' in config else 'osod.db'
        self.db = sqlite3.connect(dbfile)
        self.db.row_factory = sqlite3.Row

    def get_capabilities(self, station):
        return self.capabilities

    def get_samples(self, station, t, x):
        logger = logging.getLogger('winddb.osod')
        cursor = self.db.cursor()
        query = """SELECT
        sample_tstamp as tstamp,
        airtemp_avg,
        airpressure,
        humidity,
        windspeed_max,
        windspeed_avg,
        windspeed_min,
        wind_dir
        FROM osod
        WHERE instance = ? AND tstamp >= ?
        ORDER BY tstamp DESC"""

        if not cursor.execute(query, (station, t)):
            logger.debug('get_samples(%s) failed (t=%d)' % (station, t))
            return None

        # Collect the samples (fixup MySQL datatypes)
        fields = set(x) & set(self.capabilities)

        samples = {}
        for sample_type in fields:
            samples[sample_type] = []

        row_count = 0
        for row in cursor:
            row_count += 1
            tstamp = row['tstamp']
            for sample_type in fields:
                sample_value = float(row[self.__cap2row[sample_type]])
                samples[sample_type].append({'tstamp': tstamp, 'svalue': sample_value})

        cursor.close()

        if not row_count:
            logger.debug('get_samples(%s) = 0 samples' % (station))
            return {}
        else:
            logger.debug('get_samples(%s) = %d samples' % (station, len(samples)))
            return samples
