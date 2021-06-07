###### TEDx-Load-Aggregate-Model
######

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join, length

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job




##### FROM FILES
tedx_dataset_path = "s3://tedx-data-project/tedx_dataset.csv"

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session


    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#### READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiline", "true") \
    .csv(tedx_dataset_path)
    
tedx_dataset.printSchema()
tedx_dataset = tedx_dataset.filter(length(col('idx')) == 32) 


#### FILTER ITEMS WITH NULL POSTING KEY
count_items = tedx_dataset.count()
count_items_null = tedx_dataset.filter("idx is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")



## READ TAGS DATASET
tags_dataset_path = "s3://tedx-data-project/tags_dataset.csv"
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)


# CREATE TEDX_AGGREGATE_MODEL, ADD TAGS TO TEDX_AGGREGATE_MODEL
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags"))
tags_dataset_agg.printSchema()
tedx_dataset_agg = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \

tedx_dataset_agg.printSchema()



## READ WATCH_NEXT DATASET
watch_next_dataset_path = "s3://tedx-data-project/watch_next_dataset.csv"
watch_next_dataset = spark.read.option("header","true").csv(watch_next_dataset_path).dropDuplicates() \
    .filter("url != 'https://www.ted.com/session/new?context=ted.www%2Fwatch-later'")


# MODIFY TEDX_AGGREGATE_MODEL, ADD WHATCH_NEXT TO TEDX_AGGREGATE_MODEL
watch_next_dataset_agg = watch_next_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("url").alias("watch_next_urls"))
watch_next_dataset_agg.printSchema()
tedx_dataset_agg = tedx_dataset_agg.join(watch_next_dataset_agg, tedx_dataset_agg._id == watch_next_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
    .select(col("*")) \

tedx_dataset_agg.printSchema()



## READ MAIN_SPEAKER_DESCRIPTION DATASET
main_speakers_dataset_path = "s3://tedx-data-project/main_speaker_description_dataset.csv"
main_speakers_dataset = spark.read.option("header","true").csv(main_speakers_dataset_path).dropDuplicates()


# CREATE MAIN_SPEAKERS_AGGREGATE_MODEL, ADD RELATED TALKS TO MAIN_SPEAKERS_AGGREGATE_MODEL
urls_dataset_agg = tedx_dataset.groupBy(col("main_speaker").alias("main_speaker_ref")).agg(collect_list("url").alias("related_urls"))
main_speakers_dataset_agg = urls_dataset_agg \
    .join(main_speakers_dataset, urls_dataset_agg.main_speaker_ref == main_speakers_dataset.main_speaker, "left") \
    .select(col("main_speaker").alias("main_speaker_name"), col("main_speaker_description"), col("related_urls")) 
    
    
main_speakers_dataset_agg.printSchema()




mongo_uri = "mongodb://mycluster-shard-00-00.2afuw.mongodb.net:27017,mycluster-shard-00-01.2afuw.mongodb.net:27017,mycluster-shard-00-02.2afuw.mongodb.net:27017"

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx_2021",
    "collection": "tedx_data",
    "username": "admin123",
    "password": "admin123",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_agg, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx_2021",
    "collection": "main_speakers_data",
    "username": "admin123",
    "password": "admin123",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(main_speakers_dataset_agg, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)