import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as sf


def main(argv):

    current_dt = argv[1]
    price_rosstat_path = argv[2]
    price_stat_path = argv[3]
    city_rosstat_path = argv[4]
    city_ok_rosstat_path = argv[5]
    demography_ok_path = argv[6]
    output_path = argv[7]

    spark = SparkSession.builder.getOrCreate()

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

    #######################################################

    price_stat = spark.read.option("header", "true").csv(price_stat_path, sep=';')

    price_stat = price_stat.select(
        sf.col("product_id").cast(IntegerType()),
        sf.col("min_price").cast(DecimalType(11,2)),
        sf.col("max_price").cast(DecimalType(11,2)),
        sf.col("avg_price").cast(DecimalType(11,2)), 
    )

    #######################################################

    df_ok_rosstat_city = spark.read.csv(city_ok_rosstat_path, sep='\t')

    df_ok_rosstat_city = df_ok_rosstat_city.select(
        sf.col("_c0").cast(LongType()).alias("ok_city_id"),
        sf.col("_c1").cast(IntegerType()).alias("rs_city_id"),
    )

    #######################################################

    df_ok_demography = spark.read.csv(demography_ok_path, sep='\t')

    df_ok_demography = df_ok_demography.select(
        sf.col("_c0").cast(IntegerType()).alias("user_id"),
        sf.col("_c1").cast(LongType()).alias("create_date"),
        sf.col("_c2").cast(LongType()).alias("birth_date"),
        sf.col("_c3").cast(ShortType()).alias("gender"),
        sf.col("_c4").cast(LongType()).alias("country_id"),
        sf.col("_c5").cast(LongType()).alias("id_location"),
        sf.col("_c6").cast(IntegerType()).alias("login_region"),
    )

    #######################################################

    df_rs_city = spark.read.csv(city_rosstat_path, sep=";")

    df_rs_city = df_rs_city.select(
        sf.col("_c0").cast(StringType()).alias("city"),
        sf.col("_c1").cast(IntegerType()).alias("city_id"),
    )

    #######################################################

    ok_dem  = df_rs_price \
        .join(
            price_stat,
            [price_stat.product_id == df_rs_price.product_id, price_stat.avg_price < df_rs_price.price],
            "inner",
        ) \
        .select(sf.col('rs_city_id')).alias("tmp") \
        .distinct() \
        .join(df_rs_city, df_rs_city.city_id == sf.col('tmp.rs_city_id'), "inner") \
        .select(sf.col("city"), sf.col("rs_city_id")) \
        .join(df_ok_rosstat_city, df_ok_rosstat_city.rs_city_id == sf.col('tmp.rs_city_id'), "inner") \
        .select(sf.col('city'), sf.col('ok_city_id')) \
        .join(df_ok_demography, df_ok_demography.id_location == sf.col('ok_city_id'), 'inner') \
        .select(
            sf.col('city'),
            sf.col('birth_date'),
            sf.col('gender'),
        ) \
        .groupBy(sf.col('city')) \
        .agg(
            sf.count(sf.col('gender')).alias('user_cnt'),
            sf.avg(
                sf.datediff(sf.lit(current_dt), sf.from_unixtime(sf.col('birth_date') * 24 * 60 * 60)) / 365.25
            ).cast(IntegerType()).alias('age_avg'),
            sf.count(sf.when(sf.col('gender') == 1, True)).alias('men_cnt'),
            sf.count(sf.when(sf.col('gender') == 2, True)).alias('women_cnt'),
        ) \
        .select(
            sf.col('city'),
            sf.col('user_cnt'),
            sf.col('age_avg'),
            sf.col('men_cnt'),
            sf.col('women_cnt'),
            (sf.col('men_cnt') / sf.col('user_cnt')).cast(DecimalType(11,2)).alias('men_share'),
            (sf.col('women_cnt') / sf.col('user_cnt')).cast(DecimalType(11,2)).alias('women_share'),
        ) \
        .orderBy(sf.col('user_cnt').desc())

    ok_dem.repartition(1) \
    .coalesce(1)\
    .write\
    .mode("overwrite") \
    .option("header", "true") \
    .option("sep", ";") \
    .csv(output_path) \


if __name__ == '__main__':
    sys.exit(main(sys.argv))
