import pandas as pd
import pyodbc
from os import listdir
from os.path import isfile, join
import csv
import time

DATABASE = "hd"
TABLE1 = "record"
TABLE2 = "device"


def sqlServer_insert(path_rec, path_dev):
    t1 = time.time()
    cnxn = pyodbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=' + DATABASE + ';Trusted_Connection=yes;')
    cursor = cnxn.cursor()
    create(cnxn, cursor)
    t2 = time.time()
    insert(path_rec, path_dev, cursor, cnxn)
    t3 = time.time()

    print('Creating: {}s'.format(round(t2 - t1, 6)))
    print('Inserting: {}s'.format(round(t3 - t2, 6)))
    print('Total: {}s'.format(round(t3 - t1, 6)))


def create(cnxn, cursor):
    print("Creating table...")
    cursor.execute("""CREATE TABLE %s(
	                deviceId int,
                    nr_odczytu    int,
                    data_czas     text,
                    energia       float,
                    t_zewn        float,
                    v_wiatr       float,
                    wilg          float,
                    zachm         float,
                    dlug_dnia     float,
                    typ_dnia      float,
                    pora_roku     float,
                    )""" % TABLE1)
    cnxn.commit()

    cursor.execute("""
            CREATE TABLE %s (
                    deviceId int,
                    code text,
                    name text,
                    type text,
                    street  text,
                    location  text
            )
            """ % TABLE2)
    cnxn.commit()


def insert(path_rec, path_dec, cursor, cnxn):
    # RECORDS
    print("Inserting records...")

    insert_query = """
        INSERT INTO %s (deviceid,nr_odczytu,data_czas,energia,t_zewn,v_wiatr,wilg,zachm,dlug_dnia,typ_dnia,pora_roku)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """ % TABLE1
    files = [f for f in listdir(path_rec) if isfile(join(path_rec, f))]
    f_len = len(files)
    count = 0
    print("Reading ", len(files), "files...")
    for f in files:
        print('\r\tProgress: [%d%%]' % (100 * count / f_len), end="")
        filename = path_rec + "/" + f

        records = pd.read_csv(filename, sep=";", encoding="ISO-8859-1", skiprows=[2],
                              usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], skip_blank_lines=True, decimal=',')
        records.insert(loc=0, column='deviceid', value=filename[-8:-4])
        records.rename(columns=lambda x: x.replace('.', ''), inplace=True)
        records.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)

        for row in records.itertuples(index=True, name='Pandas'):  # try batch to optimize
            cursor.execute(insert_query,
                int(getattr(row, "deviceid")),
                int(getattr(row, "Nr_odczytu")),
                getattr(row, "Data_czas"),
                float(getattr(row, "Energia")),
                float(getattr(row, "T_zewn")),
                float(getattr(row, "V_wiatr")),
                float(getattr(row, "Wilg")),
                float(getattr(row, "Zachm")),
                float(getattr(row, "Dlug_dnia")),
                float(getattr(row, "Typ_dnia")),
                float(getattr(row, "Pora_roku"))
            )
            cnxn.commit()
        count += 1
    print('\r\tProgress: [%d%%]' % (100 * count / f_len), end="")
    print()

    # DEVICES
    print("Inserting devices...")
    filename = path_dec + "urzadzenia_rozliczeniowe_opis.csv"
    insert_query = """
        INSERT INTO %s (deviceid,code,name,type,street,location)
        VALUES (?, ?, ?, ?, ?, ?)
        """ % TABLE2
    with open(filename, newline='\n', encoding="ISO-8859-1") as csvfile:
        reader = csv.reader(csvfile, delimiter=";")
        for row in reader:
            cursor.execute(insert_query, int(row[0]), row[1], row[2], row[4], row[9], row[14])
            cnxn.commit()
