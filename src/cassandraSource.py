

import pandas as pd
KEYSPACE = "hd_keyspace"
TABLE1 = "record"
TABLE2 = "device"

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)



class LocalCassandraSource:
    def __init__(self, session):
        self.session=session

    def find_by(self, table, column, value):
        data_frame=self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE,table))._current_rows
        data_frame = data_frame[data_frame[column.lower()] == int(value)]
        return data_frame

    def join(self, left_column, right_column):
        data_records=self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE,TABLE1))._current_rows
        data_devices=self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE,TABLE2))._current_rows
        data_merge = pd.merge(data_records, data_devices, how='left', left_on=left_column.lower(), right_on=right_column.lower())
        return data_merge

    def max(self,table, column):
        sql_query = "SELECT * FROM {}.{};".format(KEYSPACE,table)
        rslt = self.session.execute(sql_query)
        return rslt._current_rows[column].max()

class CassandraSource:
    def __init__(self, session):
        self.session=session

    def find_by(self,table, column, value):
        sql_query = "SELECT * FROM {}.{} WHERE {}={} ALLOW FILTERING;".format(KEYSPACE,table, column,str(value))
        rslt = self.session.execute(sql_query)
        return rslt._current_rows

    def join(self,left_column, right_column):
        return -1 #not supported


    def max(self,table, column):
        sql_query = "SELECT MAX({}) FROM {}.{};".format(column, KEYSPACE,table)
        rslt = self.session.execute(sql_query)
        return rslt._current_rows
