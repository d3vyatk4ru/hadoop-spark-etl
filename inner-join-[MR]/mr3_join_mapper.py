#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr3_join_mapper.py"""

import sys

for line in sys.stdin:
	
	key, table, value = line.strip().split('\t')
	
	print '{key}\t{value}\t{table}'.format(key=key, value=value, table=table)

# Ваш код
