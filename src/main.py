import pyodbc
from cassandra.cluster import Cluster
from pymongo import MongoClient
from src.cassandraInsert import cassandra_insert

from src.measure import *
from src.mongoInsert import mongoInsert
from src.mongoSource import *

from src.sqlServerInsert import sqlServer_insert
from src.sqlServerSource import *

import sys
from src.userInterface import *

PATH_REC = "../res/bialogard_archh_1/"
PATH_DEV = "../res/"


def get_sources(source_name, db):
    local_src = None
    pd_src = None
    if source_name == "cassandra":
        cluster = Cluster()
        session = cluster.connect()
        session.row_factory = pandas_factory
        session.default_fetch_size = None
        local_src = LocalCassandraSource(session, db)
        pd_src = CassandraSource(session, db)
    elif source_name == "mongoDB":
        client = MongoClient()
        db = client['hd']
        local_src = LocalMongoSource(db)
        pd_src = MongoSource(db)
    elif source_name == "sqlServer":
        cnxn = pyodbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=hd;Trusted_Connection=yes;')
        local_src = LocalSqlServerSource(cnxn)
        pd_src = SqlServerSource(cnxn)
    elif source_name == "kafka":
        print("Not implemented")
        # TODO
        pass
    else:
        print("database " + db + " not found :(")
    return local_src, pd_src


def measure(params: Params, local_src, pd_src):
    # compare operations
    if params.operation == "find":
        find_by_compare(local_src, pd_src, params.table, params.column, params.value)
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

    # UI
    # params = show_ui(sys.argv[1:]) #main arguments

    # example = "-t record -c energia -o max -v 5005 -db hd_keyspace -s cassandra"
    # example = "-t record -c energia -o max -v 5005 -db hd_keyspace"
    example = "-s cassandra -db hd_keyspace -t tab1 -o avg -c deviceid -a energia"
    
    example = example.split()
    params = show_ui(example)

    # CONNECT
    local_src, pd_src = get_sources(params.source, params.database)

    # MEASURE AND COMPARE
    if local_src is not None and pd_src is not None:
        measure(params, local_src, pd_src)



if __name__ == "__main__":
    main()
