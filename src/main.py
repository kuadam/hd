import pyodbc
from cassandra.cluster import Cluster
from pymongo import MongoClient
from src.cassandraInsert import cassandra_insert

from src.measure import *
from src.mongoInsert import mongoInsert
from src.mongoSource import *

from src.sqlServerInsert import sqlServer_insert
from src.sqlServerSource import *


from src.userInterface import *

PATH_REC = "../res/bialogard_archh_1/"
PATH_DEV = "../res/"


def measure():
    # choose database
    # db = "mongo"
    # db = "cassandra"
    db = "sqlServer"
    if db == "cassandra":
        cluster = Cluster()
        session = cluster.connect()
        session.row_factory = pandas_factory
        session.default_fetch_size = None
        local_src = LocalCassandraSource(session)
        pd_src = CassandraSource(session)
    elif db == "mongo":
        client = MongoClient()
        db = client['hd']
        local_src = LocalMongoSource(db)
        pd_src = MongoSource(db)
    elif db == "sqlServer":
        cnxn = pyodbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=hd;Trusted_Connection=yes;')
        local_src = LocalSqlServerSource(cnxn)
        pd_src = SqlServerSource(cnxn)
    else:
        print("database " + db + " not found :(")
        return

    # compare operations
    find_by_compare(local_src, pd_src, "record", "deviceId", "5004")
    # join_compare(local_src, pd_src, "record", "device", "deviceId", "deviceId")
    max_compare(local_src, pd_src, "record", "energia", "deviceid")
    min_compare(local_src, pd_src, "record", "energia", "deviceid")
    avg_compare(local_src, pd_src, "record", "energia", "deviceid")
    sum_compare(local_src, pd_src, "record", "energia", "deviceid")


    print("\n\nDone")

def uitests():
    test()


def main():
    # CREATE DATABASE AND INSERT DATA
    # cassandra_insert(PATH_REC, PATH_DEV)
    # mongoInsert(PATH_REC, PATH_DEV)
    # sqlServer_insert(PATH_REC, PATH_DEV)
    # MEASURE AND COMPARE
    # measure()
    uitests()

if __name__ == "__main__":
    main()
