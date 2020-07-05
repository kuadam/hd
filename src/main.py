import pyodbc
from cassandra.cluster import Cluster
from pymongo import MongoClient

from src.cassandraInsert import cassandra_insert
from src.mongoInsert import mongoInsert
from src.sqlServerInsert import sqlServer_insert

from src.mongoSource import *
from src.sqlServerSource import *
from src.kafkaSource import *

import sys
from src.measure import *
from src.userInterface import *
from src.paths import *


def get_sources(params):
    source_name = params.source
    local_src = None
    pd_src = None
    if source_name == "cassandra":
        cluster = Cluster()
        session = cluster.connect()
        session.row_factory = pandas_factory
        session.default_fetch_size = None
        local_src = LocalCassandraSource(session, params.database)
        pd_src = CassandraSource(session, params.database, params.join_version)
    elif source_name == "mongoDB":
        client = MongoClient()
        db = client['hd']
        local_src = LocalMongoSource(db)
        pd_src = MongoSource(db)
    elif source_name == "sqlServer":
        cnxn = pyodbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database={};Trusted_Connection=yes;'.format(params.database))
        local_src = LocalSqlServerSource(cnxn)
        pd_src = SqlServerSource(cnxn)
    elif source_name == "kafka":
        topic_1 = params.table
        topic_2 = None
        schema_1 = params.json_schema
        schema_2 = None
        if params.operation == "join":
            topic_1 = params.table[0]
            topic_2 = params.table[1]
            schema_1 = params.json_schema[0]
            schema_2 = params.json_schema[0]
        local_src = LocalKafkaSource(topic_1, topic_2)
        pd_src = KafkaSource(topic_1, schema_1, topic_2, schema_2)
    else:
        print("database " + source_name + " not found :(")
    return local_src, pd_src


def measure(params: Params, local_src, pd_src):
    # compare operations
    if params.operation == "find":
        if len(params.value.split(",")) == 1:
            find_by_compare(local_src, pd_src, params.table, params.column, params.value)
        else:
            find_in_compare(local_src, pd_src, params.table, params.column, params.value)
    elif params.operation == "join":
        join_compare(local_src, pd_src, params.table[0], params.table[1], params.column[0], params.column[1])
    elif params.operation == "max":
        max_compare(local_src, pd_src, params.table, params.aggregated, params.column)
    elif params.operation == "min":
        min_compare(local_src, pd_src, params.table, params.aggregated, params.column)
    elif params.operation == "avg":
        avg_compare(local_src, pd_src, params.table, params.aggregated, params.column)
    elif params.operation == "sum":
        sum_compare(local_src, pd_src, params.table, params.aggregated, params.column)


def show_ui(args):
    input_data = InputData(args)
    input_data.parse_arguments()
    input_data.get_missing_info()
    input_data.params.print()

    return input_data.params


def main():
    # CREATE DATABASE AND INSERT DATA
    # cassandra_insert(PATH_REC, PATH_DEV)
    # mongoInsert(PATH_REC, PATH_DEV)
    # sqlServer_insert(PATH_REC, PATH_DEV)
    # run KafkaProducer

    # UI
    # params = show_ui(sys.argv[1:]) #main arguments

    # EXAMPLES
    # example = "-t record -c energia -o max -v 5005 -db hd_keyspace -s cassandra"
    # example = "-s kafka -t kafka-source-records -j record_schema.json -c deviceid -o find -v 5004,5005"
    # example = "-s kafka -t kafka-source-records -j record_schema.json -a v_wiatr -c deviceid -o sum"
    # example = "-s kafka -t kafka-source-records;kafka-source-devices -j record_schema.json;device_schema.json -o join -c deviceid;deviceId"
    # example = "-t record -c energia -o max -v 5005 -db hd_keyspace"
    # example = "-s sqlServer -db hd -t device -o find -c deviceid -v 5005,5004"
    # example = "-s mongoDB -db hd -t record -o find -c deviceid -v 5005 -cnt 61296 -l 0.5"
    example = "-s cassandra -db hd_keyspace -t device_sorted;record_sorted -o join -c deviceid;deviceid -jv 1"

    example = example.split()
    params = show_ui(example)

    # CONNECT
    local_src, pd_src = get_sources(params)

    # MEASURE AND COMPARE
    if local_src is not None and pd_src is not None:
        measure(params, local_src, pd_src)


if __name__ == "__main__":
    main()
