from smartapi import SmartConnect 
import http.client
import time
from pymongo import MongoClient
import datetime
import pandas as pd
import redis
from pandas.io.json import json_normalize
import logging
import json
  
#Create and configure logger
logging.basicConfig(filename="MA_STRATEGY_BUY_SELL_LOG.log",format='%(asctime)s %(message)s',filemode='a+')
  
#Creating an object
logger=logging.getLogger()
  
#Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())
obj=SmartConnect(api_key="QmCUZ6KE", access_token ="37dc2449-4f94-4d40-8878-e0441dbdbca9")
data = obj.generateSession("usertoken","password")
print(data)
refreshToken= data['data']['refreshToken']
feedToken=obj.getfeedToken()
print("FEEDTOKEN",feedToken)
userProfile= obj.getProfile(refreshToken)
payload = '''{\n    
    \"exchange\": \"NSE\",\n    
    \"tradingsymbol\": \"TATAPOWER-EQ\",\n     
    \"symboltoken\":\"3426\"\n
}'''

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


try:
    conndb = MongoClient()
    r = redis.StrictRedis('localhost', 6379,  db=0 , charset="utf-8", decode_responses=True)
    logger.debug(" Connected successfully!!! ")
except:  
    logger.debug(" Could not connect to MongoDB ")
  
# database
db = conndb.database
  
# Created or Switched to collection names: my_gfg_collection
collection = db.stock_collection_2

collection_count =  int(r.get('MONGOCOUNT'))


risk = .5
reward = 1

def getltp():
    conn = http.client.HTTPSConnection("apiconnect.angelbroking.com")
    conn.request("POST", "/rest/secure/angelbroking/order/v1/getLtpData", payload, headers)
    res = conn.getresponse()
    data = res.read()
    data = json.loads(data)
    ltp = data['data']['ltp']
    return(ltp)



def setredis(ltp,tradetype,target,sl):
    # print("TYPE === ",tradetype)
    r.set('TRADEACTIVE', "YES")
    r.set('STRIKEPRICE', ltp)
    r.set('ACTIVETRADETYPE', tradetype)
    r.set('TARGET', target)
    r.set('SL', sl)
    r.set('PNLFLAG',0)
    logger.debug("Redis values set successfully")


def buyatcmp():
    ltp = getltp()
    logger.debug("BUY CALLING")
    logger.debug("LTP = {}".format(ltp))
    sl = ltp - (risk *ltp) / 100
    target = ltp + (reward *ltp) / 100
    logger.debug("SL = {} TGT = {} ".format(sl,target))
    setredis(ltp , "BUY" , target , sl)


def sellatcmp():
    ltp = getltp()
    logger.debug("SELL CALLING")
    logger.debug("LTP = {}".format(ltp))
    target = ltp - (reward *ltp) / 100
    sl = ltp + (risk *ltp) / 100
    logger.debug("SL = {} TGT = {} ".format(sl,target))
    setredis(ltp , "SELL" , target , sl)

def buypercentagecalc(price_purchased, price_sold):
    gain_loss = ((price_sold-price_purchased)/price_purchased) *100
    logger.debug('BUY TRADE OUTCOME IS {} %'.format(gain_loss))
    
def sellpercentagecalc(price_purchased, price_sold):
    gain_loss = ((price_purchased-price_sold)/price_purchased) *100
    logger.debug('SELL TRADE OUTCOME IS {} %'.format(gain_loss))



def trade():
        global collection_count
        if(collection_count == int(r.get('MONGOCOUNT'))):
            logger.debug(" collection not changed ")
                    
        elif(collection_count < int(r.get('MONGOCOUNT'))):
            # cursor = collection.find().limit(1).sort([( '$natural', -1 )] ).limit(30)
            cursor = collection.find()
            df1 = json_normalize(cursor)
            logger.debug(" collection_count is {} ".format(collection_count))
            try:
                logger.debug(" MA check ")
                collection_count =  int(r.get('MONGOCOUNT'))
                df = df1 
                df['5ma'] = df['close'].rolling(5).sum()
                df['5ma'] = df['5ma']/5
                df['20ma'] = df['close'].rolling(20).sum()
                df['20ma'] = df['20ma']/20
                df['ma'] = (df['5ma']-df['20ma']) * 100
                ma_lst = df['ma'].tail(2).to_list()

                second_last_ma = ma_lst[0]
                last_ma = ma_lst[1]

                if( second_last_ma < 0 and last_ma >= 0 ) :
                    if (str(r.get('TRADEACTIVE')) == 'YES' ):
                        sellatcmp()
                        logger.debug(" EXITING PREV TRADE @ {} ".format(df.tail(1)['close']))
                        r.set('TRADEACTIVE', 'NO')
                        time.sleep(3)
                  
                    buyatcmp()
                    strikeprice  = float(r.get('STRIKEPRICE'))
                    buypercentagecalc( strikeprice , getltp)
                    logger.debug(" Profit Loss calculated ")
                    logger.debug(" Trade taken, positive crossover @ {} ".format(last_ma))
                    traded_ma = last_ma 
                    r.set('TRADEACTIVE','YES')
                    logger.debug(" buy taken @ {} ".format(df.tail(1)['close']))
                    logger.debug(" redis value is {} ".format(r.get('TRADEACTIVE')))

                if (second_last_ma > 0 and last_ma <= 0) :
                    if (str(r.get('TRADEACTIVE')) == 'YES' ):
                        logger.debug(" EXITING PREV TRADE @ {} ".format(df.tail(1)['close']))
                        buyatcmp()
                        r.set('TRADEACTIVE', 'NO')
                        time.sleep(3)
                    
                    sellatcmp()
                    strikeprice  = float(r.get('STRIKEPRICE'))
                    sellpercentagecalc( strikeprice , getltp)
                    logger.debug(" Profit Loss calculated ")
                    logger.debug(" Trade taken, negative crossover @ {} ".format(last_ma))
                    traded_ma = last_ma 
                    r.set('TRADEACTIVE', 'YES')
                    logger.debug(" SELL taken @ {} ".format(df.tail(1)['close']))
                    logger.debug(" redis value is {} ".format(r.get('TRADEACTIVE')))

            except Exception as e:
                logger.debug(" Exception occoured {}".format(e))

        else:
            pass

        time.sleep(1)  

while True:
    trade()
