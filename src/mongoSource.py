import pandas as pd
import time


class LocalMongoSource:
    def __init__(self, db):
        self.db = db

    def find_by(self, table_name, column, value):
        table = self.db[table_name]
        data_frame = pd.DataFrame(list(table.find()))
        data_frame = data_frame[data_frame[column] == value]
        return data_frame

    def join_cross(self, left_table_name, right_table_name):
        data_records = pd.DataFrame(list(self.db[left_table_name].find()))
        data_devices = pd.DataFrame(list(self.db[right_table_name].find()))
        data_records['key'] = 0
        data_devices['key'] = 0
        data_merge = pd.merge(data_records, data_devices, how='left', on='key')
        data_merge.drop('key', 1, inplace=True)
        return data_merge

    def join(self, left_table_name, right_table_name, left_column, right_column):
        data_records = pd.DataFrame(list(self.db[left_table_name].find()))
        data_devices = pd.DataFrame(list(self.db[right_table_name].find()))
        data_merge = pd.merge(data_records, data_devices, how='left', left_on=left_column, right_on=right_column)
        return data_merge

    def max(self, table_name, column, group_by):
        table = self.db[table_name]
        data_frame = pd.DataFrame(list(table.find()))
        data_frame = data_frame.groupby(group_by)[column].max()
        return data_frame

    def min(self, table_name, column, group_by):
        table = self.db[table_name]
        data_frame = pd.DataFrame(list(table.find()))
        data_frame = data_frame.groupby(group_by)[column].min()
        return data_frame

    def avg(self, table_name, column, group_by):
        table = self.db[table_name]
        data_frame = pd.DataFrame(list(table.find()))
        data_frame = data_frame.groupby(group_by)[column].mean()
        return data_frame

    def sum(self, table_name, column, group_by):
        table = self.db[table_name]
        data_frame = pd.DataFrame(list(table.find()))
        data_frame = data_frame.groupby(group_by)[column].sum()
        return data_frame


class MongoSource:
    def __init__(self, db):
        self.db = db

    def find_by(self, table_name, column, value):
        table = self.db[table_name]
        return pd.DataFrame(list(table.find({column: value})))

    def join_cross(self, left_table_name, right_table_name):
        left_table = self.db[left_table_name]
        pipeline = [
            {"$lookup": {'from': right_table_name,
                         'localField': "cJoinField",
                         'foreignField': "crossJField",
                         'as': 'rec'}
             },
            {"$unwind": {
                'path': "$rec"
            }
            }

        ]
        return pd.DataFrame(list(left_table.aggregate(pipeline)))

    def join(self, left_table_name, right_table_name, left_column, right_column):
        left_table = self.db[left_table_name]
        pipeline = [
            {"$lookup": {'from': right_table_name,
                         'localField': left_column,
                         'foreignField': right_column,
                         'as': 'rec'}}]
        return pd.DataFrame(list(left_table.aggregate(pipeline)))

    def max(self, table_name, column, group_by):
        table = self.db[table_name]
        pipeline = [{"$group": {"_id": f"${group_by}", "max": {"$max": f"${column}"}}}]
        return pd.DataFrame(list(table.aggregate(pipeline)))

    def min(self, table_name, column, group_by):
        table = self.db[table_name]
        pipeline = [{"$group": {"_id": f"${group_by}", "min": {"$min": f"${column}"}}}]
        return pd.DataFrame(list(table.aggregate(pipeline)))

    def avg(self, table_name, column, group_by):
        table = self.db[table_name]
        pipeline = [{"$group": {"_id": f"${group_by}", "avg": {"$avg": f"${column}"}}}]
        return pd.DataFrame(list(table.aggregate(pipeline)))

    def sum(self, table_name, column, group_by):
        table = self.db[table_name]
        pipeline = [{"$group": {"_id": f"${group_by}", "sum": {"$sum": f"${column}"}}}]
        return pd.DataFrame(list(table.aggregate(pipeline)))
