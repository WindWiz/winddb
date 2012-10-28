#!/usr/bin/env python
# Copyright (c) 2010-2012 Magnus Olsson (magnus@minimum.se)
# See LICENSE for details

class source(object):
	def __init__(self, name, config):
		self.name = name
		self.config = config

	""" get_samples(station, t)
	
    List samples captured for given station at time >= t (UNIX timestamp). It 
    shall return a list of dicts with the same keys as the API reference lists 
    for JSON members (see README). 
    The list shall be ordered by last_sample, descending order. If no samples 
    are  found, or an error occured, None shall be returned.	
	"""
	def get_samples(self, station, t):
		raise NotImplementedError

	""" get_latest_update()
	
	Return UTC UNIX timestamp of last sample in latest sample period. If
	there are no samples, it shall return None.
	"""
	def get_latest_update(self, station):
		raise NotImplementedError
