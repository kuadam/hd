import pandas as pd
import time

from src.cassandraSource import *

def show_params(elapsed_time, rows, push_down=True):
    if push_down:
        print("\nPUSH-DOWN")
    else:
        print("\nNORMAL")
    if rows==-1:
        print("Not supported")
        return
    print(f'Rows: {rows}')
    print(f'Elapsed time: {round(elapsed_time, 6)}s')
    print(f'Rows per second: {round(rows / elapsed_time, 2)}')



'''FIND BY'''


def find_by_measure(source,table, column, value, push_down=True):
    start_time = time.time()
    df = source.find_by(table,column, value)
    end_time = time.time()
    if isinstance(df, int):
        show_params(end_time - start_time, df, push_down)
        return
    show_params(end_time - start_time, df.shape[0], push_down)


def find_by_compare(local_source, push_down_source,table, column, value):
    print("\n=================================")
    print(f"Find by '{column}' ({value})\n")
    find_by_measure(push_down_source,table, column, value, True)
    find_by_measure(local_source,table, column, value, False)
    print("=================================")


'''JOIN'''


def join_measure(source, left_column, right_column, push_down=True):
    start_time = time.time()
    df = source.join(left_column, right_column)
    end_time = time.time()
    if isinstance(df, int):
        show_params(end_time - start_time, df, push_down)
        return
    show_params(end_time - start_time, df.shape[0], push_down)



def join_compare(local_source, push_down_source, left_column, right_column):
    print("\n=================================")
    print(f"Join ({left_column}, {right_column})\n")
    join_measure(push_down_source, left_column, right_column, True)
    join_measure(local_source, left_column, right_column, False)
    print("=================================")



'''MAX'''


def max_measure(source, table, column, push_down=True):
    start_time = time.time()
    df = source.max(table, column)
    end_time = time.time()
    if isinstance(df, int):
        show_params(end_time - start_time, df, push_down)
        return
    show_params(end_time - start_time, 1, push_down)


def max_compare(local_source, push_down_source, table, column):
    print("\n=================================")
    print(f"Max ({table}, {column})\n")
    max_measure(push_down_source, table, column, True)
    max_measure(local_source, table, column, False)
    print("=================================")
