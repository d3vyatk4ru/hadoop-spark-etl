#!/usr/bin/bash

echo "Cleans last output"
hdfs dfs -mkdir -p /user/$USER/data/ouputs/
hdfs dfs -rm -r -f /user/$USER/data/ouputs/*

mapred streaming -D mapred.reduce.tasks=1 \
-input /user/$USER/data/data/shop_price.csv \
-output /user/$USER/data/output/price \
-mapper mr3_price_mapper.py \
-file /home/$USER/mr3_price_mapper.py 

mapred streaming -D mapred.reduce.tasks=1 \
-input /user/$USER/data/data/shop_product.csv \
-output /user/$USER/data/output/product \
-mapper mr3_product_mapper.py \
-file /home/$USER/mr3_product_mapper.py

mapred streaming -D mapred.reduce.tasks=1 \
-input /user/$USER/data/output/* \
-output /user/$USER/data/output/join/ \
-mapper mr3_join_mapper.py \
-reducer mr3_join_reducer.py \
-file /home/$USER/mr3_join_mapper.py \
-file /home/$USER/mr3_join_reducer.py 

hdfs dfs -cat /user/$USER/data/output1/mr1_wcbooks/part-00000 | head