#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2010-2014 Magnus Olsson (magnus@minimum.se)
# See LICENSE for details

import MySQLdb as mysql
import logging
from source import *

class awsxd(Source):
    capabilities = [
        WINDSPEED_MAX,
        WINDSPEED_AVG,
        WINDSPEED_MIN,
        WINDDIR_AVG,
        WINDDIR_STDDEV,
        AIRTEMP_AVG,
        HUMIDITY_AVG,
        AIRPRESSURE_AVG,
    ]

    __cap2row = {
        WINDSPEED_MAX: 'wind_max',
        WINDSPEED_AVG: 'wind_avg',
        WINDSPEED_MIN: 'wind_min',
        WINDDIR_AVG: 'wind_dir',
        WINDDIR_STDDEV: 'wind_stability',
        AIRTEMP_AVG: 'temp_avg',
        HUMIDITY_AVG: 'humidity',
        AIRPRESSURE_AVG: 'air_pressure'
    }

    def __init__(self, config):
        Source.__init__(self, "awsxd", config)
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

    def get_capabilities(self, station):
        return self.capabilities

    def get_samples(self, station, t, x):
        logger = logging.getLogger('winddb.awsxd')
        cursor = self.db.cursor(mysql.cursors.DictCursor)

        query = """SELECT
        UNIX_TIMESTAMP(create_stamp) as tstamp,
        wind_max,
        wind_min,
        wind_avg,
        temp_avg,
        wind_dir,
        wind_stability,
        humidity,
        air_pressure
        FROM awsx
        WHERE station = %s
        HAVING tstamp >= %d
        ORDER BY tstamp DESC""" % ('%s', t)

        if not cursor.execute(query, (station, )):
            logger.debug('get_samples() failed (station=%s, t=%d)' % (station, t))
            return None

        if not cursor.rowcount:
            logger.debug('get_samples(%s) = 0 samples' % station)
            cursor.close()
            return {}

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
        logger.debug('get_samples(%s) = %d samples' % (station, row_count))
        return samples

