#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr3_product_mapper.py"""

import sys

for line in sys.stdin:
	
	row = line.strip().split('\t')
	key = int(row[0])
	value = row[1]
	
	print '{key}\ttable_product\t{value}'.format(key=key, value=value)

