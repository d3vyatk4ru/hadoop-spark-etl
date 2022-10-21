#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr3_price_mapper.py"""

import sys


for line in sys.stdin:
	
	row = line.strip().split(';')
	key = int(row[0])
	value = row[1]
	
	print '{key}\ttable_price\t{value}'.format(key=key, value=value)

