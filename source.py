#!/usr/bin/env python
# Copyright (c) 2010-2012 Magnus Olsson (magnus@minimum.se)
# See LICENSE for details

class source(object):
	def __init__(self, name, config):
		self.name = name
		self.config = config

	""" get_samples()
	
    List of n latest sample periods. Each list entry shall be a dict with keys:
       * num_samples: number of samples in this period, int
	   * last_sample: UTC UNIX timestamp of last sample during period, int
	   * first_sample: UTC UNIX timestamp of first sample during period, int
	   * windspeed_max: Max windspeed during period (m/s), float
	   * windspeed_min: Min windspeed during period (m/s), float
	   * windspeed_avg: Avg windspeed during period (m/s), float
	   * airtemp_avg: Avg air temperature during period (celcius), float
	   * winddir_avg: Avg wind direction during period (degrees), int
	   * winddir_stability: Wind direction stddev during period, int
	   * humidity: Relative humidity 0-100 percent, int
	   * airpressure: Airpressure (hPa), int
	- List ordered by last_sample, descending order
	- If no samples are found, or an error occured None shall be returned.
	"""
	def get_samples(self, station, n):
		raise NotImplementedError

	""" get_latest_update()
	
	Return UTC UNIX timestamp of latest sample in latest sample period. If
	there are no samples, it shall return None.

	If not implemented by source, this default implementation will be used
	which fetches the latest sample period using get_samples().
	"""
	def get_latest_update(self, station):
		samples = self.get_samples(station, 1)
		if samples is None:
			return None
		if len(samples) == 0:
			return None
		latest = samples[0]
		return latest['last_sample']

