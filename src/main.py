import pyodbc
from cassandra.cluster import Cluster
from pymongo import MongoClient
from src.cassandraInsert import cassandra_insert
from src.kafkaSource import *

from src.measure import *
from src.mongoInsert import mongoInsert
from src.mongoSource import *

from src.sqlServerInsert import sqlServer_insert
from src.sqlServerSource import *

import sys
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
        local_src = LocalCassandraSource(session)
        pd_src = CassandraSource(session)
    elif source_name == "mongoDB":
        client = MongoClient()
        db = client['hd']
        local_src = LocalMongoSource(params.database)
        pd_src = MongoSource(db)
    elif source_name == "sqlServer":
        cnxn = pyodbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=hd;Trusted_Connection=yes;')
        local_src = LocalSqlServerSource(cnxn)
        pd_src = SqlServerSource(cnxn)
    elif source_name == "kafka":
        print('nyny')
        # parameters for constructors shown below
        topic_1 = params.table
        topic_2 = None
        schema_1= params.json_schema
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
        find_by_compare(local_src, pd_src, params.table, params.column, params.value)
    elif params.operation == "join":
        join_compare(local_src, pd_src, params.table[0], params.table[1], params.column[0], params.column[1])
    elif params.operation == "max":
        max_compare(local_src, pd_src, params.table, params.column, params.value)
    elif params.operation == "min":
        min_compare(local_src, pd_src, params.table, params.column, params.value)
    elif params.operation == "avg":
        avg_compare(local_src, pd_src, params.table, params.column, params.value)
    elif params.operation == "sum":
        sum_compare(local_src, pd_src, params.table, params.column, params.value)


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
    # example = "-t record -c energia -o max -v 5005 -db hd_keyspace -s cassandra"
    example = "-s kafka -t record -c energia -o find -v 5005 "
    # example = ""
    example = example.split()
    params = show_ui(example)

    # CONNECT
    # local_src, pd_src = get_sources(params.source, params.database)

    # MEASURE AND COMPARE
    # if local_src is not None and pd_src is not None:
    #    measure(params, local_src, pd_src)

    # Examples for Kafka class instances initialization

    #local_src = LocalKafkaSource('kafka-source-records')
    # local_src = LocalKafkaSource('kafka-source-devices')
    # local_src = LocalKafkaSource('kafka-source-records', 'kafka-source-devices')

    # pd_src = KafkaSource('kafka-source-records', 'record_schema.json')
    # pd_src = KafkaSource('kafka-source-devices', 'device_schema.json')
    # pd_src = KafkaSource('kafka-source-records', 'record_schema.json', 'kafka-source-devices', 'device_schema.json')

    # Kafka TESTS

    #print(local_src.find_by('deviceid', 5005, 0.5, 61296))
    # print(local_src.find_by('deviceId', 5024, 0.16, 61296))
    # print(local_src.join('deviceid', 'deviceId'))
    # print(local_src.max('v_wiatr', 'deviceid'))
    # print(local_src.min('v_wiatr', 'deviceid'))
    # print(local_src.avg('v_wiatr', 'deviceid'))
    # print(local_src.sum('v_wiatr', 'deviceid'))

    # print(pd_src.find_by('deviceid', 5005, 0.5, 61296).count())
    # pd_src.find_by('deviceId', 5024, 0.16, 61296).show()
    # pd_src.join('deviceid', 'deviceId').show()
    # pd_src.max('v_wiatr', 'deviceid').show()
    # pd_src.min('v_wiatr', 'deviceid').show()
    # pd_src.avg('v_wiatr', 'deviceid').show()
    # pd_src.sum('v_wiatr', 'deviceid').show()


if __name__ == "__main__":
    main()
