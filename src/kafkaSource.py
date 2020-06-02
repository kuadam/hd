from kafka import KafkaConsumer
from json import loads
from iteration_utilities import split
import pandas as pd
from collections import defaultdict


class LocalKafkaSource:
    def __init__(self, topic):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            value_deserializer=lambda x: loads(x.decode('utf-8')),
            consumer_timeout_ms=1000)

    def get_dict(self):
        json_records = [msg.value for msg in self.consumer]
        self.consumer.close()

        tables_dict = defaultdict(list)
        for item in list(split(json_records, lambda x: x == 'EOF')):
            tables_dict[item[0]].extend(item[1:])

        return tables_dict

    @staticmethod
    def get_tables(tables_dict, table_1, table_2=None):
        df_table_1 = pd.DataFrame.from_records(tables_dict[table_1])

        if table_2 is not None:
            df_table_2 = pd.DataFrame.from_records(tables_dict[table_2])

            return df_table_1, df_table_2

        return df_table_1

    def find_by(self, table_name, column, value):
        data_frame = self.get_tables(self.get_dict(), table_name)
        data_frame = data_frame[data_frame[column] == value]

        return data_frame

    def join_cross(self, left_table_name, right_table_name):
        data_records, data_devices = self.get_tables(self.get_dict(), left_table_name, right_table_name)
        data_records['key'] = 0
        data_devices['key'] = 0
        data_merge = pd.merge(data_records, data_devices, how='left', on='key')
        data_merge.drop('key', 1, inplace=True)

        return data_merge

    def join(self, left_table_name, right_table_name, left_column, right_column):
        data_records, data_devices = self.get_tables(self.get_dict(), left_table_name, right_table_name)
        data_merge = pd.merge(data_records, data_devices, how='left', left_on=left_column, right_on=right_column)

        return data_merge

    def max(self, table_name, column, group_by):
        data_frame = self.get_tables(self.get_dict(), table_name)
        data_frame = data_frame.groupby(group_by)[column].max()

        return data_frame

    def min(self, table_name, column, group_by):
        data_frame = self.get_tables(self.get_dict(), table_name)
        data_frame = data_frame.groupby(group_by)[column].min()

        return data_frame

    def avg(self, table_name, column, group_by):
        data_frame = self.get_tables(self.get_dict(), table_name)
        data_frame = data_frame.groupby(group_by)[column].mean()

        return data_frame

    def sum(self, table_name, column, group_by):
        data_frame = self.get_tables(self.get_dict(), table_name)
        data_frame = data_frame.groupby(group_by)[column].sum()

        return data_frame
