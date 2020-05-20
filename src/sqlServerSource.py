import pandas as pd

TABLE1 = "record"
TABLE2 = "device"


class LocalSqlServerSource:
    def __init__(self, cnxn):
        self.cnxn = cnxn

    def find_by(self, table_name, column, value):
        query = f'SELECT * FROM {table_name}'
        data_frame = pd.read_sql(query, self.cnxn)
        data_frame = data_frame[data_frame[column] == int(value)]
        return data_frame

    def join(self, left_table_name, right_table_name, left_column, right_column):
        query = f'SELECT * FROM {left_table_name}'
        data_records = pd.read_sql(query, self.cnxn)

        query = f'SELECT * FROM {right_table_name}'
        data_devices = pd.read_sql(query, self.cnxn)

        data_merge = pd.merge(data_records, data_devices, how='left', left_on=left_column, right_on=right_column)
        return data_merge

    def max(self, table_name, column, group_by):
        query = f'SELECT * FROM {table_name}'
        data_frame = pd.read_sql(query, self.cnxn)
        data_frame = data_frame.groupby(group_by)[column].max()
        return data_frame

    def min(self, table_name, column, group_by):
        query = f'SELECT * FROM {table_name}'
        data_frame = pd.read_sql(query, self.cnxn)
        data_frame = data_frame.groupby(group_by)[column].min()
        return data_frame

    def avg(self, table_name, column, group_by):
        query = f'SELECT * FROM {table_name}'
        data_frame = pd.read_sql(query, self.cnxn)
        data_frame = data_frame.groupby(group_by)[column].mean()
        return data_frame

    def sum(self, table_name, column, group_by):
        query = f'SELECT * FROM {table_name}'
        data_frame = pd.read_sql(query, self.cnxn)
        data_frame = data_frame.groupby(group_by)[column].sum()
        return data_frame


class SqlServerSource:
    def __init__(self, cnxn):
        self.cnxn = cnxn
        self.cursor = cnxn.cursor()

    def find_by(self, table_name, column, value):
        query = "SELECT * FROM {} WHERE {}={}".format(table_name, column, str(value))
        return pd.DataFrame(pd.read_sql(query, self.cnxn))

    def join(self, left_table_name, right_table_name, left_column, right_column):
        query = f'SELECT * FROM {left_table_name} AS l JOIN {right_table_name} AS r ON l.{left_column}=r.{right_column}'
        return pd.DataFrame(pd.read_sql(query, self.cnxn))

    def max(self, table_name, column, group_by):
        query = f'SELECT MAX({column}) FROM {table_name} GROUP BY {group_by}'
        return pd.DataFrame(pd.read_sql(query, self.cnxn))

    def min(self, table_name, column, group_by):
        query = f'SELECT MIN({column}) FROM {table_name} GROUP BY {group_by}'
        return pd.DataFrame(pd.read_sql(query, self.cnxn))

    def avg(self, table_name, column, group_by):
        query = f'SELECT AVG({column}) FROM {table_name} GROUP BY {group_by}'
        return pd.DataFrame(pd.read_sql(query, self.cnxn))

    def sum(self, table_name, column, group_by):
        query = f'SELECT SUM({column}) FROM {table_name} GROUP BY {group_by}'
        return pd.DataFrame(pd.read_sql(query, self.cnxn))
