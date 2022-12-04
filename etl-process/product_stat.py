
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as sf
from pyspark.sql.window import Window

def main(argv):

    city_rosstat_path = argv[1]
    price_rosstat_path = argv[2]
    product_rosstat_path = argv[3]
    dem_ok_path = argv[4]
    output_path = argv[5]

    spark = SparkSession.builder.getOrCreate()

    #######################################################

    df_rs_product = spark.read.csv(product_rosstat_path, sep=';')

    df_rs_product = df_rs_product.select(
        sf.col("_c0").cast(StringType()).alias("product"),
        sf.col("_c1").cast(IntegerType()).alias("product_id"),
    )

    #######################################################

    df_rs_city = spark.read.csv(city_rosstat_path, sep=";")

    df_rs_city = df_rs_city.select(
        sf.col("_c0").cast(StringType()).alias("city"),
        sf.col("_c1").cast(IntegerType()).alias("city_id"),
    )

    #######################################################

    df_rs_price = spark.read.csv(price_rosstat_path, sep=';')

    df_rs_price = df_rs_price.select(
        sf.col("_c0").cast(IntegerType()).alias("rs_city_id"),
        sf.col("_c1").cast(IntegerType()).alias("product_id"),
        sf.col("_c2").cast(StringType()).alias("price"), 
    )

    df_rs_price = df_rs_price.withColumn(
        'price', 
        sf.regexp_replace('price', ',', '.').cast('float'),
    ) 

    df_rs_price = df_rs_price.na.drop()

    #######################################################

    dem_ok = spark.read.option("header", "true").csv(dem_ok_path, sep=';')

    dem_ok = dem_ok.select(
        sf.col("city"),
        sf.col("user_cnt").cast(IntegerType()),
        sf.col("age_avg").cast(ShortType()),
        sf.col("men_cnt").cast(IntegerType()),
        sf.col("women_cnt").cast(IntegerType()),
        sf.col("men_share").cast(FloatType()),
        sf.col("women_share").cast(FloatType()),
    )

    #######################################################

    product_stat_part_of_task = dem_ok \
        .where(
            (sf.col("age_avg") == dem_ok.select(sf.max(sf.col("age_avg"))).collect()[0][0]) |
            (sf.col("age_avg") == dem_ok.select(sf.min(sf.col("age_avg"))).collect()[0][0]) |
            (sf.col("men_share") == dem_ok.select(sf.max(sf.col("men_share"))).collect()[0][0]) |
            (sf.col("women_share") == dem_ok.select(sf.max(sf.col("women_share"))).collect()[0][0])
        ) \
        .select(sf.col("city")).alias("tmp") \
        .join(df_rs_city, df_rs_city.city == sf.col("tmp.city"), "inner") \
        .select(sf.col("tmp.city"), sf.col("city_id")) \
        .join(df_rs_price, df_rs_price.rs_city_id == sf.col("city_id"), "inner") \
        .select(sf.col("city"), sf.col("product_id"), sf.col("price")).alias("tmp") \
        .join(df_rs_product, df_rs_product.product_id == sf.col("tmp.product_id"), "inner") \
        .select(sf.col("city"), sf.col("product"), sf.col("price")) 

    min_product_cost = product_stat_part_of_task \
            .withColumn('cheap', sf.min('price') \
                    .over(Window.partitionBy('city'))) \
            .where((sf.col("price") == sf.col("cheap"))) \
            .select(
                sf.col("city").alias("city_min"),
                sf.col("product").alias("product_min"),
                sf.col("cheap"),
            )

    max_product_cost = product_stat_part_of_task \
            .withColumn('expensive', sf.max('price') \
                    .over(Window.partitionBy('city'))) \
            .where((sf.col("price") == sf.col("expensive"))) \
        .select(sf.col("city").alias("city_max"), sf.col("product").alias("product_max"), sf.col("expensive"))

    product_stat = max_product_cost \
        .join(min_product_cost, min_product_cost.city_min == sf.col("city_max"), "inner") \
        .select(
            sf.col("city_min").alias("city_name"),
            sf.col("product_min").alias("cheapest_product_name"),
            sf.col("product_max").alias("most_expensive_product_name"),
            (sf.col("expensive") - sf.col("cheap")).alias("price_difference")
        )

    product_stat \
    .coalesce(1) \
    .repartition(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("sep", ";") \
    .csv(output_path) 


if __name__ == '__main__':
    sys.exit(main(sys.argv))
