import pandas as pd
import time

from src.cassandraSource import *


def show_params(elapsed_time, rows, push_down=True):
    if push_down:
        print("\nPUSH-DOWN")
    else:
        print("\nNORMAL")
    if rows == -1:
        print("Not supported")
        return
    print('Rows: {}'.format(rows))
    print('Elapsed time: {}s'.format(round(elapsed_time, 6)))
    print('Rows per second: {}'.format(round(rows / elapsed_time, 2)))


'''FIND BY'''


def find_by_measure(source, table_name, column, value, push_down=True):
    start_time = time.time()
    df = source.find_by(table_name, column, value)
    end_time = time.time()
    if isinstance(df, int):
        show_params(end_time - start_time, df, push_down)
        return
    show_params(end_time - start_time, df.shape[0], push_down)


def find_by_compare(local_source, push_down_source, table_name, column, value):
    print("\n=================================")
    print("Find by '{}' ({})\n".format(column, value))
    find_by_measure(push_down_source, table_name, column, value, True)
    find_by_measure(local_source, table_name, column, value,  False)
    print("=================================")



'''FIND IN'''


def find_in_measure(source, table_name, column, value, push_down=True):
    start_time = time.time()
    df = source.find_in(table_name, column, value)
    end_time = time.time()
    if isinstance(df, int):
        show_params(end_time - start_time, df, push_down)
        return
    show_params(end_time - start_time, df.shape[0], push_down)


def find_in_compare(local_source, push_down_source, table_name, column, value):
    print("\n=================================")
    print("Find '{}' in ({})\n".format(column, value))
    find_in_measure(push_down_source, table_name, column, value, True)
    find_in_measure(local_source, table_name, column, value,  False)
    print("=================================")

'''JOIN'''


def join_measure_cross(source, left_table_name, right_table_name, push_down=True):
    start_time = time.time()
    df = source.join_cross(left_table_name, right_table_name)
    end_time = time.time()
    if isinstance(df, int):
        show_params(end_time - start_time, df, push_down)
        return
    show_params(end_time - start_time, df.shape[0], push_down)


def join_measure(source, left_table_name, right_table_name, left_column, right_column, push_down=True):
    start_time = time.time()
    df = source.join(left_table_name, right_table_name, left_column, right_column)
    end_time = time.time()
    if isinstance(df, int):
        show_params(end_time - start_time, df, push_down)
        return
    show_params(end_time - start_time, df.shape[0], push_down)


def join_compare_cross(local_source, push_down_source, left_table_name, right_table_name):
    print("\n=================================")
    print("Crossjoin\n")
    join_measure_cross(push_down_source, left_table_name, right_table_name, True)
    join_measure_cross(local_source, left_table_name, right_table_name, False)
    print("=================================")


def join_compare(local_source, push_down_source, left_table_name, right_table_name, left_column, right_column):
    print("\n=================================")
    print("Join ({}, {})\n".format(left_column, right_column))
    join_measure(push_down_source, left_table_name, right_table_name, left_column, right_column, True)
    join_measure(local_source, left_table_name, right_table_name, left_column, right_column, False)
    print("=================================")


'''MAX'''


def max_measure(source, table_name, column, group_by, push_down=True):
    start_time = time.time()
    df = source.max(table_name, column, group_by)
    end_time = time.time()
    if isinstance(df, int):
        show_params(end_time - start_time, df, push_down)
        return
    show_params(end_time - start_time,  df.shape[0], push_down)


def max_compare(local_source, push_down_source, table_name, column, group_by):
    print("\n=================================")
    print("Max ({}, {})\n".format(table_name, column))
    max_measure(push_down_source, table_name, column, group_by, True)
    max_measure(local_source, table_name, column, group_by, False)
    print("=================================")


'''MIN'''


def min_measure(source, table_name, column, group_by, push_down=True):
    start_time = time.time()
    df = source.min(table_name, column, group_by)
    end_time = time.time()
    if isinstance(df, int):
        show_params(end_time - start_time, df, push_down)
        return
    show_params(end_time - start_time, df.shape[0], push_down)


def min_compare(local_source, push_down_source, table_name, column, group_by):
    print("\n=================================")
    print("Min ({}, {})\n".format(table_name, column))
    min_measure(push_down_source, table_name, column, group_by, True)
    min_measure(local_source, table_name, column, group_by, False)
    print("=================================")


'''AVG'''


def avg_measure(source, table_name, column, group_by, push_down=True):
    start_time = time.time()
    df = source.avg(table_name, column, group_by)
    end_time = time.time()
    if isinstance(df, int):
        show_params(end_time - start_time, df, push_down)
        return
    show_params(end_time - start_time, df.shape[0], push_down)


def avg_compare(local_source, push_down_source, table_name, column, group_by):
    print("\n=================================")
    print("Avg ({}, {})\n".format(table_name, column))
    avg_measure(push_down_source, table_name, column, group_by, True)
    avg_measure(local_source, table_name, column, group_by, False)
    print("=================================")


'''SUM'''


def sum_measure(source, table_name, column, group_by, push_down=True):
    start_time = time.time()
    df = source.sum(table_name, column, group_by)
    end_time = time.time()
    if isinstance(df, int):
        show_params(end_time - start_time, df, push_down)
        return
    show_params(end_time - start_time, df.shape[0], push_down)


def sum_compare(local_source, push_down_source, table_name, column, group_by):
    print("\n=================================")
    print("Sum ({}, {})\n".format(table_name, column))
    sum_measure(push_down_source, table_name, column, group_by, True)
    sum_measure(local_source, table_name, column, group_by, False)
    print("=================================")
