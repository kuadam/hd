import pandas as pd
from os import listdir
from os.path import isfile, join
import csv
import time
from cassandra.cluster import Cluster

KEYSPACE = "hd_keyspace"
TABLE1 = "record"
TABLE2 = "device"


def cassandra_insert(path_rec, path_dev):
    t1 = time.time()
    cluster = Cluster()
    session = cluster.connect()
    create(session)
    t2 = time.time()
    insert(path_rec, path_dev, session)
    t3 = time.time()

    print(f'Creating: {round(t2 - t1, 6)}s')
    print(f'Inserting: {round(t3 - t2, 6)}s')
    print(f'Total: {round(t3 - t1, 6)}s')


def create(session):
    print("Creating keyspace...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % KEYSPACE)
    session.set_keyspace(KEYSPACE)

    print("Creating table", TABLE1, "...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS %s (
            deviceid      bigint,
            nr_odczytu    bigint,
            data_czas     text,
            energia       text,
            t_zewn        text,
            v_wiatr       text,
            wilg          text,
            zachm         text,
            dlug_dnia     text,
            typ_dnia      text,
            pora_roku     text,
            PRIMARY KEY (deviceid,nr_odczytu)
        )
        """ % TABLE1)

    print("Creating table", TABLE2, "...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS %s (
                deviceid bigint,
                code text,
                name text,
                type text,
                street  text,
                location  text,
            PRIMARY KEY (deviceid)
        )
        """ % TABLE2)


def insert(path_rec, path_dec, session):
    # RECORDS
    print("Inserting records...")

    prepared = session.prepare("""
        INSERT INTO %s (deviceid,nr_odczytu,data_czas,energia,t_zewn,v_wiatr,wilg,zachm,dlug_dnia,typ_dnia,pora_roku)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """ % TABLE1)
    files = [f for f in listdir(path_rec) if isfile(join(path_rec, f))]
    f_len = len(files)
    count = 0
    print("Reading ", len(files), "files...")
    for f in files:
        print('\r\tProgress: [%d%%]' % (100 * count / f_len), end="")
        filename = path_rec + "/" + f


        records = pd.read_csv(filename, sep=";", encoding="ISO-8859-1", skiprows=[2],
                              usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], skip_blank_lines=True)
        records.insert(loc=0, column='deviceid', value=filename[-8:-4])
        records.rename(columns=lambda x: x.replace('.', ''), inplace=True)
        records.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)

        for row in records.itertuples(index=True, name='Pandas'):  # try batch to optimize
            session.execute(prepared, (
                int(getattr(row, "deviceid")),
                int(getattr(row, "Nr_odczytu")),
                getattr(row, "Data_czas"),
                getattr(row, "Energia"),
                getattr(row, "T_zewn"),
                getattr(row, "V_wiatr"),
                getattr(row, "Wilg"),
                getattr(row, "Zachm"),
                getattr(row, "Dlug_dnia"),
                getattr(row, "Typ_dnia"),
                getattr(row, "Pora_roku")
            ))
        count += 1
    print('\r\tProgress: [%d%%]' % (100 * count / f_len), end="")
    print()

    # DEVICES
    print("Inserting devices...")
    filename = path_dec + "urzadzenia_rozliczeniowe_opis.csv"
    prepared = session.prepare("""
        INSERT INTO %s (deviceid,code,name,type,street,location)
        VALUES (?, ?, ?, ?, ?, ?)
        """ % TABLE2)
    with open(filename, newline='\n', encoding="ISO-8859-1") as csvfile:
        reader = csv.reader(csvfile, delimiter=";")
        for row in reader:
            session.execute(prepared, (int(row[0]), row[1], row[2], row[4], row[9], row[14]))
