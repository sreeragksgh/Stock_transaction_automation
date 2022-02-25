import time
from pymongo import MongoClient
import datetime
import pandas as pd
import redis
import sys
from smartapi import SmartConnect 
import pandas as pd
import numpy as np
from datetime import datetime
import json
import http.client

#create object of call
obj=SmartConnect(api_key="QmCUZ6KE", access_token ="37dc2449-4f94-4d40-8878-e0441dbdbca9")


#login api call
print(obj)
data = obj.generateSession("usertoken","password")

print(data)
refreshToken= data['data']['refreshToken']

#fetch the feedtoken
feedToken=obj.getfeedToken()
print("FEEDTOKEN",feedToken)

#fetch User Profile
userProfile= obj.getProfile(refreshToken)
print(userProfile)

r = redis.Redis(host='localhost', port=6379, db=0)

def getdf():
    obj=SmartConnect(api_key="QmCUZ6KE", access_token ="37dc2449-4f94-4d40-8878-e0441dbdbca9")


    #login api call
    print(obj)
    data = obj.generateSession("usertoken","password")

    print(data)
    refreshToken= data['data']['refreshToken']

    #fetch the feedtoken
    feedToken=obj.getfeedToken()
    print("FEEDTOKEN",feedToken)

    #fetch User Profile
    userProfile= obj.getProfile(refreshToken)
    print(userProfile)
    INTERVALLST = ["ONE_MINUTE"]
    STRIPNAME = "TATAPOWER"
    TOKEN = "3426"
    for INTERVAL in INTERVALLST:
    # INTERVAL = "ONE_DAY"
        conn = http.client.HTTPSConnection("apiconnect.angelbroking.com")
        # payload = '''{\r\n     \"exchange\": \"NSE\",\r\n    
        #  \"symboltoken\": \"3426\",\r\n     \"interval\": \"ONE_DAY\",\r\n  
        #     \"fromdate\": \"2015-01-01 09:00\",\r\n     \"todate\": \"2021-05-31 09:59\"\r\n}'''
        payload = '''{\r\n     \"exchange\": \"NSE\",\r\n    
        \"symboltoken\": \"'''+TOKEN+'''\",\r\n     \"interval\": \"'''+INTERVAL+'''\",\r\n  
            \"fromdate\": \"2022-02-25 09:00\",\r\n     \"todate\": \"2022-02-25 15:59\"\r\n}'''
        headers = {
            'X-PrivateKey': 'QmCUZ6KE',
            'Accept': 'application/json',
            'X-SourceID': 'WEB',
            'X-ClientLocalIP': 'CLIENT_LOCAL_IP',
            'X-ClientPublicIP': 'CLIENT_PUBLIC_IP',
            'X-MACAddress': 'MAC_ADDRESS',
            'X-UserType': 'USER',
            'Authorization': data['data']['jwtToken'],
            'Accept': 'application/json',
            'X-SourceID': 'WEB',
            'Content-Type': 'application/json'
        }


        conn.request("POST", "/rest/secure/angelbroking/historical/v1/getCandleData", payload, headers)
        res = conn.getresponse()
        data = res.read()
        print(data.decode("utf-8"))


        df1= pd.read_json(data)
        df2=df1['data']
        date = []
        open = []
        high = []
        low = []
        close = []
        volume = []
        i=0
        df = df2
        for i in range(len(df)):
                    date.append(df[i][0])
                    open.append(df[i][1]) 
                    high.append(df[i][2])
                    low.append(df[i][3])
                    close.append(df[i][4])
                    volume.append(df[i][5])    
        dfx = pd.DataFrame(list(zip(date, open, high, low, close, volume)),columns=['date','open', 'high', 'low', 'close','volume'], index=None)
        return(dfx)


  
try:
    conn = MongoClient()
    print("Connected successfully!!!")
except:  
    print("Could not connect to MongoDB")
  
# database
db = conn.database
  
# Created or Switched to collection names: my_gfg_collection
collection = db.stock_collection_2

# df = pd.read_csv("path of csv file")

df = getdf()
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

        # time.sleep(.1)  

while True:
    trade()

# sys.stdout = old_stdout

