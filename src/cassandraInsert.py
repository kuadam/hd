import pandas as pd
from os import listdir
from os.path import isfile, join
import csv
import time
from cassandra.cluster import Cluster

KEYSPACE = "hd_keyspace"
TABLE1s = "record_sorted"
TABLE1ns = "record_notsorted"
TABLE1s150 = "record_sorted150"
TABLE2s = "device_sorted"
TABLE2ns = "device_notsorted"


def cassandra_insert(path_rec, path_dev):
    t1 = time.time()
    cluster = Cluster()
    session = cluster.connect()
    create(session)
    t2 = time.time()
    insert(path_rec, path_dev, session)
    t3 = time.time()

    print('Creating: {}s'.format(round(t2 - t1, 6)))
    print('Inserting: {}s'.format(round(t3 - t2, 6)))
    print('Total: {}s'.format(round(t3 - t1, 6)))


def create(session):
    print("Creating keyspace...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % KEYSPACE)
    session.set_keyspace(KEYSPACE)

    print("Creating table", TABLE1s, "...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS %s (
            partition     bigint,
            deviceid      bigint,
            nr_odczytu    bigint,
            data_czas     text,
            energia       float,
            t_zewn        float,
            v_wiatr       float,
            wilg          float,
            zachm         float,
            dlug_dnia     float,
            typ_dnia      text,
            pora_roku     text,
            PRIMARY KEY (partition, deviceid, nr_odczytu)
        )
        """ % TABLE1s)

    print("Creating table", TABLE1s150, "...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS %s (
            partition     bigint,
            deviceid      bigint,
            nr_odczytu    bigint,
            data_czas     text,
            energia       float,
            t_zewn        float,
            v_wiatr       float,
            wilg          float,
            zachm         float,
            dlug_dnia     float,
            typ_dnia      text,
            pora_roku     text,
            PRIMARY KEY (partition, deviceid, nr_odczytu)
        )
        """ % TABLE1s150)

    print("Creating table", TABLE2s, "...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS %s (
                partition bigint,
                deviceid bigint,
                code text,
                name text,
                type text,
                street  text,
                location  text,
            PRIMARY KEY (partition, deviceid)
        )
        """ % TABLE2s)

    print("Creating table", TABLE1ns, "...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS %s (
            deviceid      bigint,
            nr_odczytu    bigint,
            data_czas     text,
            energia       float,
            t_zewn        float,
            v_wiatr       float,
            wilg          float,
            zachm         float,
            dlug_dnia     float,
            typ_dnia      text,
            pora_roku     text,
            PRIMARY KEY (deviceid, nr_odczytu)
        )
        """ % TABLE1ns)

    print("Creating table", TABLE2ns, "...")
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
        """ % TABLE2ns)


def insert(path_rec, path_dec, session):
    # RECORDS
    print("Inserting records...")

    prepared1 = session.prepare("""
        INSERT INTO %s (partition,deviceid,nr_odczytu,data_czas,energia,t_zewn,v_wiatr,wilg,zachm,dlug_dnia,typ_dnia,pora_roku)
        VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """ % TABLE1s)
    prepared150 = session.prepare("""
        INSERT INTO %s (partition,deviceid,nr_odczytu,data_czas,energia,t_zewn,v_wiatr,wilg,zachm,dlug_dnia,typ_dnia,pora_roku)
        VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """ % TABLE1s150)
    prepared2 = session.prepare("""
         INSERT INTO %s (deviceid,nr_odczytu,data_czas,energia,t_zewn,v_wiatr,wilg,zachm,dlug_dnia,typ_dnia,pora_roku)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         """ % TABLE1ns)

    files = [f for f in listdir(path_rec) if isfile(join(path_rec, f))]
    f_len = len(files)
    count = 0
    partition = 111
    ct = 0
    print("Reading ", len(files), "files...")
    for f in files:
        print('\r\tProgress: [%d%%]' % (100 * count / f_len), end="")
        filename = path_rec + "/" + f

        records = pd.read_csv(filename, sep=";", encoding="ISO-8859-1", skiprows=[2],
                              usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], skip_blank_lines=True)
        records.insert(loc=0, column='deviceid', value=filename[-8:-4])
        records.rename(columns=lambda x: x.replace('.', ''), inplace=True)
        records.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
        records.replace({',': '.'}, regex=True, inplace=True)

        for row in records.itertuples(index=True, name='Pandas'):  # try batch to optimize
            session.execute(prepared1, (
                partition,
                int(getattr(row, "deviceid")),
                int(getattr(row, "Nr_odczytu")),
                getattr(row, "Data_czas"),
                float(getattr(row, "Energia")),
                float(getattr(row, "T_zewn")),
                float(getattr(row, "V_wiatr")),
                float(getattr(row, "Wilg")),
                float(getattr(row, "Zachm")),
                float(getattr(row, "Dlug_dnia")),
                getattr(row, "Typ_dnia"),
                getattr(row, "Pora_roku")
            ))
            session.execute(prepared2, (
                int(getattr(row, "deviceid")),
                int(getattr(row, "Nr_odczytu")),
                getattr(row, "Data_czas"),
                float(getattr(row, "Energia")),
                float(getattr(row, "T_zewn")),
                float(getattr(row, "V_wiatr")),
                float(getattr(row, "Wilg")),
                float(getattr(row, "Zachm")),
                float(getattr(row, "Dlug_dnia")),
                getattr(row, "Typ_dnia"),
                getattr(row, "Pora_roku")
            ))
            if ct < 150:
                session.execute(prepared150, (
                    partition,
                    int(getattr(row, "deviceid")),
                    int(getattr(row, "Nr_odczytu")),
                    getattr(row, "Data_czas"),
                    float(getattr(row, "Energia")),
                    float(getattr(row, "T_zewn")),
                    float(getattr(row, "V_wiatr")),
                    float(getattr(row, "Wilg")),
                    float(getattr(row, "Zachm")),
                    float(getattr(row, "Dlug_dnia")),
                    getattr(row, "Typ_dnia"),
                    getattr(row, "Pora_roku")
                ))
            ct += 1
        count += 1
    print('\r\tProgress: [%d%%]' % (100 * count / f_len), end="")
    print()

    # DEVICES
    print("Inserting devices...")
    filename = path_dec + "urzadzenia_rozliczeniowe_opis.csv"
    prepared1 = session.prepare("""
        INSERT INTO %s (partition,deviceid,code,name,type,street,location)
        VALUES (?,?, ?, ?, ?, ?, ?)
        """ % TABLE2s)
    prepared2 = session.prepare("""
        INSERT INTO %s (deviceid,code,name,type,street,location)
        VALUES (?, ?, ?, ?, ?, ?)
        """ % TABLE2ns)
    with open(filename, newline='\n', encoding="ISO-8859-1") as csvfile:
        reader = csv.reader(csvfile, delimiter=";")
        for row in reader:
            session.execute(prepared1, (partition,int(row[0]), row[1], row[2], row[4], row[9], row[14]))
            session.execute(prepared2, (int(row[0]), row[1], row[2], row[4], row[9], row[14]))

