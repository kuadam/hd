from cassandra.cluster import Cluster
from pymongo import MongoClient
from src.cassandraInsert import *
from src.measure import *
from src.mongoInsert import *
from src.mongoSource import *

'''CREATE DB AND INSERT DATA'''
import time

PATH_REC = "../res/bialogard_archh_1/"
PATH_DEV = "../res/"


# def create_and_insert():
#     t1 = time.time()
#     cluster = Cluster()
#     session = cluster.connect()
#     create(session)
#     t2 = time.time()
#     insert(PATH_REC, PATH_DEV, session)
#     t3 = time.time()
#
#     print(f'Creating: {round(t2 - t1, 6)}s')
#     print(f'Inserting: {round(t3 - t2, 6)}s')
#     print(f'Total: {round(t3 - t1, 6)}s')


def mongoInsert():
    client = MongoClient()
    db = client['hd']
    insert(PATH_REC, PATH_DEV, db)


'''MEASURE'''


def measure():
    # choose database
    db = "mongo"
    if db == "cassandra":
        # cluster = Cluster()
        # session = cluster.connect()
        # session.row_factory = pandas_factory
        # session.default_fetch_size = None
        # local_src = LocalCassandraSource(session)
        # pd_src = CassandraSource(session)
        records = "record"
    elif db == "mongo":
        client = MongoClient()
        db = client['hd']

        # MongoDB
        local_src = LocalMongoSource(db)
        pd_src = MongoSource(db)

    # compare operations
    find_by_compare(local_src, pd_src, "record", "deviceId", "5004")
    join_compare(local_src, pd_src, "record", "device", "deviceId", "deviceId")
    max_compare(local_src, pd_src, "record", "Energia", "deviceId")
    min_compare(local_src, pd_src, "record", "Energia", "deviceId")
    avg_compare(local_src, pd_src, "record", "Energia", "deviceId")
    sum_compare(local_src, pd_src, "record", "Energia", "deviceId")


def main():
    # create_and_insert()
    measure()


if __name__ == "__main__":
    main()
