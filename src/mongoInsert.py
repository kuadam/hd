import csv
from pymongo import MongoClient
import pandas as pd
from os import listdir
from os.path import isfile, join

client = MongoClient()
db = client['hd']
destdir = "bialogard_archh_1"
files = [f for f in listdir(destdir) if isfile(join(destdir, f))]
for f in files:
    file = destdir + "/" + f

    df = pd.read_csv(file, sep=";", encoding="ISO-8859-1", skiprows=[2], usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                     skip_blank_lines=True)

    df.insert(loc=0, column='deviceId', value=file[-8:-4])
    df.rename(columns=lambda x: x.replace('.', ''), inplace=True)

    records = db['record']

    data = df.to_dict("records")
    records.insert_many(data)
    print(f)

devices = db['device']
items = []
with open('res/urzadzenia_rozliczeniowe_opis.csv', newline='\n') as csvfile:
    reader = csv.reader(csvfile, delimiter=";")
    for row in reader:
        item = {"deviceId": row[0],
                "code": row[1],
                "name": row[2],
                "a": row[3],
                "type": row[4],
                "r": row[5],
                "val1": row[6],
                "val2": row[7],
                "param": row[8],
                "street": row[9],
                "cityType": row[10],
                "c": row[11],
                "d": row[12],
                "city": row[13],
                "location": row[14],
                "date": row[15],
                "e": row[16],
                "f": row[17],
                }
        items.append(item)

devices.insert_many(items)
