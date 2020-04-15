from pymongo import MongoClient
import pandas as pd
import time

client = MongoClient()
db = client['hd']
records = db['record']
device = db['device']


def show_params(elapsed_time, rows):
    print(f'Rows: {rows}')
    print(f'Elapsed time: {round(elapsed_time, 6)}s')
    print(f'Rows per second: {round(rows / elapsed_time, 2)}')


def find_by_compare(column, value):
    print("\n=================================")
    print(f"Find by '{column}' ({value})")
    start_time = time.time()
    df = find_by_pd(column, value)
    end_time = time.time()

    print("\nPUSH-DOWN")
    show_params(end_time - start_time, df.shape[0])

    start_time = time.time()
    df = find_by(column, value)
    end_time = time.time()

    print("\nNORMAL")
    show_params(end_time - start_time, df.shape[0])
    print("=================================")


def find_by(column, value):
    data_frame = pd.DataFrame(list(records.find()))
    data_frame = data_frame[data_frame[column] == value]
    return data_frame


def find_by_pd(column, value):
    return pd.DataFrame(list(records.find({column: value})))


def join_compare(left_column, right_column):
    print("\n=================================")
    print(f"Join ({left_column}, {right_column})")
    start_time = time.time()
    df = join_pd(left_column, right_column)
    end_time = time.time()

    print("\nPUSH-DOWN")
    show_params(end_time - start_time, df.shape[0])

    start_time = time.time()
    df = join(left_column, right_column)
    end_time = time.time()

    print("\nNORMAL")
    show_params(end_time - start_time, df.shape[0])
    print("=================================")


def join_pd(left_column, right_column):
    pipeline = [
        {"$lookup": {'from': 'device',
                     'localField': left_column,
                     'foreignField': right_column,
                     'as': 'rec'}}]
    return pd.DataFrame(list(records.aggregate(pipeline)))


def join(left_column, right_column):
    data_records = pd.DataFrame(list(records.find()))
    data_devices = pd.DataFrame(list(device.find()))
    data_merge = pd.merge(data_records, data_devices, how='left', left_on=left_column, right_on=right_column)
    return data_merge


find_by_compare("deviceId", "5004")
find_by_compare("Nr odczytu", 1)
join_compare("deviceId", "deviceId")
