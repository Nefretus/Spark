from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row
from pyspark.sql.functions import col, explode

spark = SparkSession.builder.master('spark://192.168.193.141:7077').getOrCreate()
#spark.sparkContext.setLogLevel("DEBUG")

def load_files(spark, entities):
    return [spark.read.json(rf'hdfs://localhost:9000/data/DataSparkProj/{entity}/*') for entity in entities]

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
    exploded_df = sources.select(explode("counts_by_year") \
                         .alias("year_stat")) \
                         .filter(col('country_code') == country_code)
    exploded_df.show()
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

# liczba cytowan poszczegolnych prac wybranej osoby
def citations_for_works_of_given_author(spark, orcid):
    works = load_files(spark, ['Works'])[0]
    works.createOrReplaceTempView("works")
    spark.sql("SELECT W.display_name, W.cited_by_count "
              "FROM works W "
              "WHERE W.is_paratext = FALSE "
             f"AND '{orcid}' IN (SELECT col.orcid FROM (SELECT EXPLODE(authorships.author) FROM works W1 WHERE W1.id = W.id) WHERE col.orcid IS NOT NULL) "
              "ORDER BY W.cited_by_count DESC").show()

works_per_year(spark, 'US')
#citations_for_works_of_given_author(spark, r'https://orcid.org/0000-0003-3000-5390')

spark.stop()

