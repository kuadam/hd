import csv
from pymongo import MongoClient
import pandas as pd
from os import listdir
from os.path import isfile, join


def mongoInsert(PATH_REC, PATH_DEV):
    client = MongoClient()
    db = client['hd']
    insert(PATH_REC, PATH_DEV, db)


def get_time(row):
    return row["Data_czas"].split()[1]


def get_date(row):
    l_date = row["Data_czas"].split()[0]
    return l_date.replace(".", "-")


def insert(destdir, resdir, db):
    files = [f for f in listdir(destdir) if isfile(join(destdir, f))]
    for f in files:
        file = destdir + "/" + f

        df = pd.read_csv(file, sep=";", encoding="ISO-8859-1", skiprows=[2], usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                         skip_blank_lines=True, decimal=',')

        df.insert(loc=0, column='deviceid', value=file[-8:-4])
        df.rename(columns=lambda x: x.replace('.', ''), inplace=True, )
        df.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
        df['czas'] = df.apply(lambda x: get_time(x), axis=1)
        df['Data_czas'] = df.apply(lambda x: get_date(x), axis=1)
        df.rename(inplace=True, columns={"Data_czas": "data"})
        records = db['record']

        data = df.to_dict("records")
        records.insert_many(data)
        print(f)

    devices = db['device']
    items = []
    with open(resdir + 'urzadzenia_rozliczeniowe_opis.csv', newline='\n') as csvfile:
        reader = csv.reader(csvfile, delimiter=";")
        for row in reader:
            item = {"deviceid": row[0],
                    "code": row[1],
                    "name": row[2],
                    "type": row[4],
                    "street": row[9],
                    "location": row[14],
                    }
            items.append(item)

    devices.insert_many(items)
