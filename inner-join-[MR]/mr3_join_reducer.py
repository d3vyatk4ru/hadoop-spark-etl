#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr3_join_reducer.py"""

import sys

prev_key = None
table = None
data = []

for line in sys.stdin:

        row  = line.strip().split('\t')

        if prev_key == None:
                prev_key = row[0]
                table = row[2]
                data.append(row[:2])

        elif prev_key == row[0] and table == row[2]:
                prev_key = row[0]
                table = row[2]
                data.append(row[:2])

        elif prev_key == row[0] and table != row[2]:
                for key, val in data:
                        if key == row[0]:
                                if row[2] == 'table_product':
                                    print'{key}\t{val1}\t{val2}'.format(key=key, val1=val, val2=row[1])
                                else:
                                    print '{key}\t{val1}\t{val2}'.format(key=key, val1=row[1], val2=val)
        elif prev_key != row[0]:
                data = []
                data.append(row[:2])
                prev_key = row[0]
                table = row[2]
