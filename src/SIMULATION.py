import time
from pymongo import MongoClient
import datetime
import pandas as pd
import redis
import sys
# old_stdout = sys.stdout

# log_file = open("message.log","w")
# sys.stdout = log_file

r = redis.Redis(host='localhost', port=6379, db=0)


  
try:
    conn = MongoClient()
    print("Connected successfully!!!")
except:  
    print("Could not connect to MongoDB")
  
# database
db = conn.database
  
# Created or Switched to collection names: my_gfg_collection
collection = db.stock_collection_2

df = pd.read_csv("path of csv file")


def trade():
    for index, row in df.iterrows():
        print(row['open'], row['high'])

        try:
            timestamp = row['date']
            openx = row['open']
            high = row['high']
            low = row['low']
            close = row['close']
            volume = row['volume']
            candledata = {'close': close,
                        'high': high,
                        '_id': timestamp,
                        'low': low,
                        'open': openx,
                        'volume': volume}

            rec_id1 = collection.insert_one(candledata)
            collection_count = collection.count()
        
            print("Data inserted with record ids",rec_id1)
            print(collection_count)
            r.set('MONGOCOUNT', collection_count )
            print(timestamp)
            print(close)
            # print(datetime.datetime.now())

        except:
            print("exception")

        time.sleep(2)  

while True:
    trade()

# sys.stdout = old_stdout

