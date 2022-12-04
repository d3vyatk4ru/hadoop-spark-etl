
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as sf


def main(argv):

    price_rosstat_path = argv[1]
    product_for_stat = argv[2]
    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()

    #######################################################

    df_product_for_stat = spark.read.csv(product_for_stat)

    df_product_for_stat = df_product_for_stat.select(
        sf.col("_c0").cast(IntegerType()).alias("product_id"),
    )

    #######################################################

    df_rs_price = spark.read.csv(price_rosstat_path, sep=';')

    df_rs_price = df_rs_price.select(
        sf.col("_c0").cast(IntegerType()).alias("rs_city_id"),
        sf.col("_c1").cast(IntegerType()).alias("product_id"),
        sf.col("_c2").cast(StringType()).alias("price"), 
    )

    df_rs_price = df_rs_price.withColumn(
        "price", 
        sf.regexp_replace("price", ',', '.').cast("float"),
    ) 

    #######################################################

    price_stat = df_rs_price.alias("l") \
    .join(df_product_for_stat, df_product_for_stat.product_id == df_rs_price.product_id, "inner") \
    .select(sf.col("l.rs_city_id"), sf.col("l.product_id"), sf.col("l.price")) \
    .groupBy("product_id") \
    .agg(
        sf.min(sf.col("price")).cast(DecimalType(11,2)).alias("min_price"),
        sf.max(sf.col("price")).cast(DecimalType(11,2)).alias("max_price"),
        sf.avg(sf.col("price")).cast(DecimalType(11,2)).alias("avg_price"),
    ).orderBy(["product_id"], ascending=True)

    price_stat.repartition(1) \
    .coalesce(1)\
    .write\
    .mode("overwrite") \
    .option("header", "true") \
    .option("sep", ";") \
    .csv(output_path) \

    
if __name__ == '__main__':
    sys.exit(main(sys.argv))