

import pandas as pd
KEYSPACE = "hd_keyspace"
TABLE1 = "record"
TABLE2 = "device"

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)



class LocalCassandraSource:
    def __init__(self, session):
        self.session=session

    def find_by(self, table_name, column_name, value):
        data_frame=self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE,table_name))._current_rows
        data_frame = data_frame[data_frame[column_name.lower()] == int(value)]
        return data_frame

    def join(self, left_column, right_column):
        data_records=self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE,TABLE1))._current_rows
        data_devices=self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE,TABLE2))._current_rows
        data_merge = pd.merge(data_records, data_devices, how='left', left_on=left_column.lower(), right_on=right_column.lower())
        return data_merge

    def max(self, table_name, column_name, group_by):
        data_frame=self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE,table_name))._current_rows
        data_frame = data_frame.groupby(group_by)[column_name].max()
        return data_frame

    def min(self, table_name, column_name, group_by):
        data_frame=self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE,table_name))._current_rows
        data_frame = data_frame.groupby(group_by)[column_name].min()
        return data_frame

    def avg(self, table_name, column_name, group_by):
        data_frame=self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE,table_name))._current_rows
        data_frame = data_frame.groupby(group_by)[column_name].mean()
        return data_frame


    def sum(self, table_name, column_name, group_by):
        data_frame=self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE,table_name))._current_rows
        data_frame = data_frame.groupby(group_by)[column_name].sum()
        return data_frame


class CassandraSource:
    def __init__(self, session):
        self.session=session

    def find_by(self,table_name, column_name, value):
        sql_query = "SELECT * FROM {}.{} WHERE {}={} ALLOW FILTERING;".format(KEYSPACE,table_name, column_name,str(value))
        rslt = self.session.execute(sql_query)
        return rslt._current_rows

    def join(self,left_column, right_column):
        return -1 #not supported

    def max(self, table_name, column_name, group_by):
        sql_query = "SELECT {}, MAX({}) FROM {}.{} GROUP BY {};".format(group_by, column_name, KEYSPACE,table_name, group_by)
        rslt = self.session.execute(sql_query)
        return rslt._current_rows


    def min(self, table_name, column_name, group_by):
        sql_query = "SELECT {}, MIN({}) FROM {}.{} GROUP BY {};".format(group_by, column_name, KEYSPACE, table_name, group_by)
        rslt = self.session.execute(sql_query)
        return rslt._current_rows


    def avg(self, table_name, column_name, group_by):
        pass
        sql_query = "SELECT {}, AVG({}) FROM {}.{} GROUP BY {};".format(group_by, column_name, KEYSPACE,table_name, group_by)
        rslt = self.session.execute(sql_query)
        return rslt._current_rows


    def sum(self, table_name, column_name, group_by):
        pass
        sql_query = "SELECT {}, SUM({}) FROM {}.{} GROUP BY {};".format(group_by, column_name, KEYSPACE,table_name, group_by)
        rslt = self.session.execute(sql_query)
        return rslt._current_rows

