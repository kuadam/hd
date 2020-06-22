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
        data_frame = self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE, table_name))._current_rows
        data_frame = data_frame[data_frame[column_name.lower()] == int(value)]
        return data_frame

    def join(self, left_table_name, right_table_name, left_column, right_column):
        data_records = self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE, left_table_name))._current_rows
        data_devices = self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE, right_table_name))._current_rows
        data_merge = pd.merge(data_records, data_devices, how='left', left_on=left_column.lower(),
                              right_on=right_column)
        return data_merge

    def max(self, table_name, column_name, group_by):
        data_frame = self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE, table_name))._current_rows
        data_frame = data_frame.groupby(group_by)[column_name].max()
        return data_frame

    def min(self, table_name, column_name, group_by):
        data_frame = self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE, table_name))._current_rows
        data_frame = data_frame.groupby(group_by)[column_name].min()
        return data_frame

    def avg(self, table_name, column_name, group_by):
        data_frame = self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE, table_name))._current_rows
        data_frame = data_frame.groupby(group_by)[column_name].mean()
        return data_frame

    def sum(self, table_name, column_name, group_by):
        data_frame = self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE, table_name))._current_rows
        data_frame = data_frame.groupby(group_by)[column_name].sum()
        return data_frame


class CassandraSource:
    def __init__(self, session):
        self.session = session

    def find_by(self, table_name, column_name, value):
        sql_query = "SELECT * FROM {}.{} WHERE {}={} ALLOW FILTERING;".format(KEYSPACE, table_name, column_name, str(value))
        rslt = self.session.execute(sql_query)
        return rslt._current_rows

    def join_both_sorted(self, left_table_name, right_table_name, left_column, right_column):
        data_devices = self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE,right_table_name))._current_rows
        records_single = self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE,left_table_name))._current_rows
        i = 0
        j = 0
        pom = 0
        columns_dev = data_devices.columns.tolist()
        columns_rec = records_single.columns.tolist()
        columns_rec.remove(left_column)
        #rslt = pd.DataFrame(columns=columns_dev+columns_rec)
        while i < records_single.shape[0] and j < data_devices.shape[0]:
            while i != records_single.shape[0] and records_single.at[i, left_column] < data_devices.at[j, right_column]:
                i += 1
            while j != data_devices.shape[0] and records_single.at[i, left_column] > data_devices.at[j, right_column]:
                j += 1
            while i != records_single.shape[0] and j != data_devices.shape[0] and records_single.at[i, left_column] == data_devices.at[j, right_column]:
                #rslt = rslt.append(pd.Series(data_devices[columns_dev].loc[j].tolist() + records_single[columns_rec].loc[i].tolist(), index=rslt.columns), ignore_index=True)
                pom += 1
                if j+1 == data_devices.shape[0]:
                    i += 1
                elif records_single.at[i, left_column] == data_devices.at[j+1, right_column] and j+1 != data_devices.shape[0]:
                    j += 1
                elif records_single.at[i, left_column] != data_devices.at[j+1, right_column] and j+1 != data_devices.shape[0]:
                    i += 1
        return pom

    def join_left_sorted(self, left_table_name, right_table_name, left_column, right_column):
        data_devices = self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE,right_table_name))._current_rows
        data_devices.sort_values(by=[right_column], inplace=True, ignore_index=True)
        records_single = self.session.execute("SELECT * FROM {}.{};".format(KEYSPACE,left_table_name))._current_rows
        i = 0
        j = 0
        pom = 0
        columns_dev = data_devices.columns.tolist()
        columns_rec = records_single.columns.tolist()
        columns_rec.remove(left_column)
        #rslt = pd.DataFrame(columns=columns_dev+columns_rec)
        while i < records_single.shape[0] and j < data_devices.shape[0]:
            while i != records_single.shape[0] and records_single.at[i, left_column] < data_devices.at[j, right_column]:
                i += 1
            while j != data_devices.shape[0] and records_single.at[i, left_column] > data_devices.at[j, right_column]:
                j += 1
            while i != records_single.shape[0] and j != data_devices.shape[0] and records_single.at[i, left_column] == data_devices.at[j, right_column]:
                #rslt = rslt.append(pd.Series(data_devices[columns_dev].loc[j].tolist() + records_single[columns_rec].loc[i].tolist(), index=rslt.columns), ignore_index=True)
                pom += 1
                if j+1 == data_devices.shape[0]:
                    i += 1
                elif records_single.at[i, left_column] == data_devices.at[j+1, right_column] and j+1 != data_devices.shape[0]:
                    j += 1
                elif records_single.at[i, left_column] != data_devices.at[j+1, right_column] and j+1 != data_devices.shape[0]:
                    i += 1
        return pom

    def max(self, table_name, column_name, group_by):
        sql_query = "SELECT {}, MAX({}) FROM {}.{} GROUP BY {};".format(group_by, column_name, KEYSPACE, table_name, group_by)
        rslt = self.session.execute(sql_query)
        return rslt._current_rows

    def min(self, table_name, column_name, group_by):
        sql_query = "SELECT {}, MIN({}) FROM {}.{} GROUP BY {};".format(group_by, column_name, KEYSPACE, table_name, group_by)
        rslt = self.session.execute(sql_query)
        return rslt._current_rows

    def avg(self, table_name, column_name, group_by):
        sql_query = "SELECT {}, AVG({}) FROM {}.{} GROUP BY {};".format(group_by, column_name, KEYSPACE, table_name, group_by)
        rslt = self.session.execute(sql_query)
        return rslt._current_rows

    def sum(self, table_name, column_name, group_by):
        sql_query = "SELECT {}, SUM({}) FROM {}.{} GROUP BY {};".format(group_by, column_name, KEYSPACE, table_name, group_by)
        rslt = self.session.execute(sql_query)
        return rslt._current_rows