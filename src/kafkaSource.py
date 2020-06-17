import os
from kafka import KafkaConsumer
from json import loads
import pandas as pd

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

from src.paths import *

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 pyspark-shell'


class LocalKafkaSource:
    def __init__(self, topic_1, topic_2=None):
        self.topic_1 = topic_1
        self.consumer_1 = KafkaConsumer(topic_1,
                                        bootstrap_servers=['localhost:9092'],
                                        auto_offset_reset='earliest',
                                        value_deserializer=lambda x: loads(x.decode('utf-8')),
                                        consumer_timeout_ms=1000)
        self.topic_2 = topic_2

        if self.topic_2 is not None:
            self.consumer_2 = KafkaConsumer(topic_2,
                                            bootstrap_servers=['localhost:9092'],
                                            auto_offset_reset='earliest',
                                            value_deserializer=lambda x: loads(x.decode('utf-8')),
                                            consumer_timeout_ms=1000)

    def get_tables(self):
        json_records = [msg.value for msg in self.consumer_1]
        self.consumer_1.close()

        df_table_1 = pd.DataFrame.from_records(json_records)

        if self.topic_2 is not None:
            json_records = [msg.value for msg in self.consumer_2]
            self.consumer_2.close()

            df_table_2 = pd.DataFrame.from_records(json_records)

            return df_table_1, df_table_2

        return df_table_1

    def find_by(self, column, value, limit=None):
        df_table_1 = self.get_tables()

        if limit is not None:
            df_table_1 = df_table_1.head(round(len(df_table_1) * limit))

        df_table_1 = df_table_1[df_table_1[column] == value]

        return df_table_1

    def join(self, left_column, right_column):
        df_table_1, df_table_2 = self.get_tables()
        df_table_merged = pd.merge(df_table_1, df_table_2, how='left', left_on=left_column, right_on=right_column)

        return df_table_merged

    def max(self, column, group_by):
        df_table_1 = self.get_tables()
        df_table_1 = df_table_1.groupby(group_by)[column].max()

        return df_table_1

    def min(self, column, group_by):
        df_table_1 = self.get_tables()
        df_table_1 = df_table_1.groupby(group_by)[column].min()

        return df_table_1

    def avg(self, column, group_by):
        df_table_1 = self.get_tables()
        df_table_1 = df_table_1.groupby(group_by)[column].mean()

        return df_table_1

    def sum(self, column, group_by):
        df_table_1 = self.get_tables()
        df_table_1 = df_table_1.groupby(group_by)[column].sum()

        return df_table_1


class KafkaSource:
    def __init__(self, topic_1, schema_1, topic_2=None, schema_2=None):
        self.spark = SparkSession \
                        .builder \
                        .appName("KafkaSourceApp") \
                        .getOrCreate()

        self.topic_1 = topic_1
        self.json_schema_1 = self.spark.read.json(PATH_SCHEMA + schema_1).schema

        self.topic_2 = topic_2
        if self.topic_2 is not None:
            self.json_schema_2 = self.spark.read.json(PATH_SCHEMA + schema_2).schema

    # Creating a Kafka Source for Batch Queries
    def get_spark_dfs(self):
        df_1 = self.spark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", self.topic_1) \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .withColumn("value", from_json(col("value"), self.json_schema_1)) \
            .select("value.*")

        if self.topic_2 is not None:
            df_2 = self.spark \
                .read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", self.topic_2) \
                .option("startingOffsets", "earliest") \
                .load() \
                .selectExpr("CAST(value AS STRING)") \
                .withColumn("value", from_json(col("value"), self.json_schema_2)) \
                .select("value.*")

            return df_1, df_2

        return df_1

    def find_by(self, column, value, limit=None):
        df_table_1 = self.get_spark_dfs()

        if limit is None:
            df_table_1 = df_table_1.where(col(column) == value)
        else:
            df_table_1 = df_table_1\
                            .limit(round(df_table_1.count() * limit))\
                            .where(col(column) == value)

        return df_table_1

    def join(self, left_column, right_column):
        df_table_1, df_table_2 = self.get_spark_dfs()
        df_table_merged = df_table_1.join(df_table_2, df_table_1[left_column] == df_table_2[right_column], how="left")

        return df_table_merged

    def max(self, column, group_by):
        df_table_1 = self.get_spark_dfs()
        df_table_1 = df_table_1.groupby(group_by).max(column)

        return df_table_1

    def min(self, column, group_by):
        df_table_1 = self.get_spark_dfs()
        df_table_1 = df_table_1.groupby(group_by).min(column)

        return df_table_1

    def avg(self, column, group_by):
        df_table_1 = self.get_spark_dfs()
        df_table_1 = df_table_1.groupby(group_by).avg(column)

        return df_table_1

    def sum(self, column, group_by):
        df_table_1 = self.get_spark_dfs()
        df_table_1 = df_table_1.groupby(group_by).sum(column)

        return df_table_1



