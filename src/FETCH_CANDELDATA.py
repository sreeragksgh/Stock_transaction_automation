from smartapi import SmartConnect 
import http.client
import json
import redis
import time
from pymongo import MongoClient
import datetime
import logging
  
#Create and configure logger
logging.basicConfig(filename="FETCH_CANDLE_LOG.log",format='%(asctime)s %(message)s',filemode='a+')
  
#Creating an object
logger=logging.getLogger()
  
#Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

try:
    conn = MongoClient()
    print("Connected successfully!!!")
except:  
    print("Could not connect to MongoDB")
  
# database
db = conn.database
r = redis.StrictRedis('localhost', 6379,  db=0 , charset="utf-8", decode_responses=True)

  
# Created or Switched to collection names: my_gfg_collection
collection = db.stock_collection_2

obj=SmartConnect(api_key="QmCUZ6KE", access_token ="37dc2449-4f94-4d40-8878-e0441dbdbca9")
data = obj.generateSession("usertoken","password")
print(data)
refreshToken= data['data']['refreshToken']
feedToken=obj.getfeedToken()
print("FEEDTOKEN",feedToken)
userProfile= obj.getProfile(refreshToken)
conn = http.client.HTTPSConnection("apiconnect.angelbroking.com")
payload = '''{\r\n     \"exchange\": \"NSE\",\r\n    
 \"symboltoken\": \"3426\",\r\n     \"interval\": \"ONE_MINUTE\",\r\n  
    \"fromdate\": \"2021-05-31 09:00\",\r\n     \"todate\": \"2021-05-31 15:59\"\r\n}'''

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
# r = redis.Redis(host='localhost', port=6379, db=0)
conn = http.client.HTTPSConnection("apiconnect.angelbroking.com")
def trade():

    try:
        conn.request("POST", "/rest/secure/angelbroking/historical/v1/getCandleData", payload, headers)
        res = conn.getresponse()
        data = res.read()
        data = json.loads(data)
        data
        timestamp = data['data'][-1][0]
        openx = data['data'][-1][1]
        high = data['data'][-1][2]
        low = data['data'][-1][3]
        close = data['data'][-1][4]
        volume = data['data'][-1][5]
        candledata = {'close': close,
                    'high': high,
                    '_id': timestamp,
                    'low': low,
                    'open': openx,
                    'volume': volume}

        rec_id1 = collection.insert_one(candledata)
        collection_count = collection.count()
        r.set('MONGOCOUNT', collection_count )    
        logger.debug("Data inserted with record ids {} ".format(rec_id1))
        logger.debug("Data timestamp =  {}".format(timestamp))
        # print("current time = ", datetime.datetime.now())

    except Exception as e:
                logger.debug(" Exception occoured {}".format(e))

    time.sleep(1)  

while True:
    trade()
