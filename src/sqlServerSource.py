import pandas as pd

TABLE1 = "record"
TABLE2 = "device"


class LocalSqlServerSource:
    def __init__(self, cnxn):
        self.cnxn = cnxn

    def find_by(self, table_name, column, value):
        query = 'SELECT * FROM {}'.format(table_name)
        data_frame = pd.read_sql(query, self.cnxn)
        data_frame = data_frame[data_frame[column] == int(value)]
        return data_frame

    def find_in(self, table_name, column, values):
        query = 'SELECT * FROM {}'.format(table_name)
        data_frame = pd.read_sql(query, self.cnxn)
        data_frame = data_frame[data_frame[column].isin(values.split(","))]
        return data_frame

    def join(self, left_table_name, right_table_name, left_column, right_column):
        query = 'SELECT * FROM {}'.format(left_table_name)
        data_records = pd.read_sql(query, self.cnxn)

        query = 'SELECT * FROM {right_table_name}'.format(right_table_name)
        data_devices = pd.read_sql(query, self.cnxn)

        data_merge = pd.merge(data_records, data_devices, how='left', left_on=left_column, right_on=right_column)
        return data_merge

    def max(self, table_name, column, group_by):
        query = 'SELECT * FROM {}'.format(table_name)
        data_frame = pd.read_sql(query, self.cnxn)
        data_frame = data_frame.groupby(group_by)[column].max()
        return data_frame

    def min(self, table_name, column, group_by):
        query = 'SELECT * FROM {}'.format(table_name)
        data_frame = pd.read_sql(query, self.cnxn)
        data_frame = data_frame.groupby(group_by)[column].min()
        return data_frame

    def avg(self, table_name, column, group_by):
        query = 'SELECT * FROM {}'.format(table_name)
        data_frame = pd.read_sql(query, self.cnxn)
        data_frame = data_frame.groupby(group_by)[column].mean()
        return data_frame

    def sum(self, table_name, column, group_by):
        query = 'SELECT * FROM {}'.format(table_name)
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

    def find_in(self, table_name, column, values):
        query = "SELECT * FROM {} WHERE {} in ({})".format(table_name, column, values)
        return pd.DataFrame(pd.read_sql(query, self.cnxn))

    def join(self, left_table_name, right_table_name, left_column, right_column):
        query = 'SELECT * FROM {} AS l JOIN {} AS r ON l.{}=r.{}'\
            .format(left_table_name, right_table_name, left_column, right_column)
        return pd.DataFrame(pd.read_sql(query, self.cnxn))

    def max(self, table_name, column, group_by):
        query = 'SELECT MAX({}) FROM {} GROUP BY {}'.format(column, table_name, group_by)
        return pd.DataFrame(pd.read_sql(query, self.cnxn))

    def min(self, table_name, column, group_by):
        query = 'SELECT MIN({}) FROM {} GROUP BY {}'.format(column, table_name, group_by)
        return pd.DataFrame(pd.read_sql(query, self.cnxn))

    def avg(self, table_name, column, group_by):
        query = 'SELECT AVG({}) FROM {} GROUP BY {}'.format(column, table_name, group_by)
        return pd.DataFrame(pd.read_sql(query, self.cnxn))

    def sum(self, table_name, column, group_by):
        query = 'SELECT SUM({}) FROM {} GROUP BY {}'.format(column, table_name, group_by)
        return pd.DataFrame(pd.read_sql(query, self.cnxn))
