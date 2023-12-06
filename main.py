import findspark
findspark.init(r'C:\Spark\spark-3.0.0-bin-hadoop2.7')

from pyspark.sql import SparkSession


spark = SparkSession.builder.master('spark://192.168.0.45:7077').getOrCreate()
#spark.sparkContext.setLogLevel("DEBUG")

from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row
from pyspark.sql.functions import col, explode


def load_files(spark, entities):
    return [spark.read.json(rf'D:\DataSparkProj\{entity}\*') for entity in entities]

# liczba prac naukowych w zaleznosci od kraju
def works_per_country(spark):
    works, sources = load_files(spark, ['Works', 'Sources'])
    works.select(col('primary_location.source.id')).alias("w") \
         .join(sources.alias('s'), col('w.id') == col('s.id'), how='inner') \
         .groupby('country_code') \
         .count() \
         .show()

# liczba prac wydanych w wybranym kraju na przestrzeni kilku lat
def works_per_year(spark, country_code):
    sources = load_files(spark, ['Sources'])[0]
    works_count = sources \
        .select(col('works_count')) \
        .filter(col('country_code') == country_code) \
        .groupBy() \
        .sum() \
        .first()[0]
    print(f"There are {works_count:,} works for country code: {country_code}.")
    sources.select("counts_by_year").show()
    sources.select(explode("counts_by_year")).show()
    exploded_df = sources.select(explode("counts_by_year") \
                         .alias("year_stat")) \
                         .filter(col('country_code') == country_code)
    works_per_year_df = exploded_df.select(
        col("year_stat.year").cast("int").alias("year"),
        col("year_stat.works_count").cast("int").alias("works_count"),
        col("year_stat.cited_by_count").cast("int").alias("cited_by_count")
    ) \
        .filter((col("year") > 2012) & (col("year") < 2024)) \
        .groupBy("year") \
        .sum("works_count").withColumnRenamed("sum(works_count)",
                                              "works_count") \
        .orderBy("year")
    works_per_year_df.show()
    # import seaborn as sns
    # sns.set_theme()
    # g = sns.lineplot(works_per_year)
    # g.set_ylim(bottom=0)
    # g.set_xlim(2012, 2022)
    # g.set_ylabel("number of works")
    # 'from_publication_date:2012-08-24'

#works_per_year(spark, 'US')

spark.stop()

