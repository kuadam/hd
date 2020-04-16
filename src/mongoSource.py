import pandas as pd
import time



class LocalMongoSource:
    def __init__(self, session):
        self.session=session

    def find_by(self, records, column, value):
        data_frame = pd.DataFrame(list(records.find()))
        data_frame = data_frame[data_frame[column] == value]
        return data_frame


    def join(self,records,device, left_column, right_column):
        data_records = pd.DataFrame(list(records.find()))
        data_devices = pd.DataFrame(list(device.find()))
        data_merge = pd.merge(data_records, data_devices, how='left', left_on=left_column, right_on=right_column)
        return data_merge



class MongoSource:
    def __init__(self, session):
        self.session=session

    def find_by(self,records, column, value):
        return pd.DataFrame(list(records.find({column: value})))

    def join(self,records, left_column, right_column):
        pipeline = [
            {"$lookup": {'from': 'device',
                         'localField': left_column,
                         'foreignField': right_column,
                         'as': 'rec'}}]
        return pd.DataFrame(list(records.aggregate(pipeline)))
