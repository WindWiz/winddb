#!/usr/bin/env python
# Copyright (c) 2010-2014 Magnus Olsson (magnus@minimum.se)
# See LICENSE for details

import simplejson as json

WIND_SPEED  = 'wspeed'
WIND_DIR    = 'wdir'
HUMIDITY    = 'humidity'
AIRTEMP     = 'airtemp'
AIRPRESSURE = 'airpressure'
VISIBILITY  = 'visibility'

MIN         = 'min'
MAX         = 'max'
AVG         = 'avg'
STDDEV      = 'stddev'

WINDSPEED_MAX    = (WIND_SPEED,  MAX)
WINDSPEED_AVG    = (WIND_SPEED , AVG)
WINDSPEED_MIN    = (WIND_SPEED,  MIN)
WINDDIR_AVG      = (WIND_DIR,    AVG)
WINDDIR_STDDEV   = (WIND_DIR,    STDDEV)
AIRTEMP_AVG      = (AIRTEMP,     AVG)
HUMIDITY_AVG     = (HUMIDITY,    AVG)
AIRPRESSURE_AVG  = (AIRPRESSURE, AVG)
VISIBILITY_AVG   = (VISIBILITY,  AVG)

class Source(object):
    def __init__(self, name, config):
        self.name = name
        self.config = config

    def get_capabilities(self, station):
        raise NotImplementedError

    def get_samples(self, station, t, x):
        raise NotImplementedError
