from cassandra.cluster import Cluster
from pymongo import MongoClient
from src.cassandraInsert import cassandra_insert

from src.measure import *
from src.mongoInsert import mongoInsert
from src.mongoSource import *

PATH_REC = "../res/bialogard_archh_1/"
PATH_DEV = "../res/"


def measure():
    # choose database
    #db = ""
    db = "mongo"
    #db = "cassandra"
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
    else:
        print("database " + db + " not found :(")
        return

    # compare operations
    #find_by_compare(local_src, pd_src, "record", "deviceid", "5004")
    #join_compare(local_src, pd_src, "record", "device", "deviceid", "deviceid")
    #join_compare_cross(local_src, pd_src, "record", "device")
    #max_compare(local_src, pd_src, "record", "Energia", "deviceid")
    #min_compare(local_src, pd_src, "record", "Energia", "deviceid")
    #avg_compare(local_src, pd_src, "record", "Energia", "deviceid")
    #sum_compare(local_src, pd_src, "record", "Energia", "deviceid")

    print("\n\nDone")


def main():
    # CREATE DATABASE AND INSERT DATA
    #cassandra_insert(PATH_REC, PATH_DEV)
     #mongoInsert(PATH_REC, PATH_DEV)

    # MEASURE AND COMPARE
    measure()


if __name__ == "__main__":
    main()
