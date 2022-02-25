from requests.api import get
from smartapi import SmartConnect 
import http.client
import json
import redis
import time
import logging
  
#Create and configure logger
logging.basicConfig(filename="TRADE_LOG.log",format='%(asctime)s %(message)s',filemode='a+')
  
#Creating an object
logger=logging.getLogger()
  
#Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

obj=SmartConnect(api_key="QmCUZ6KE", access_token ="37dc2449-4f94-4d40-8878-e0441dbdbca9")
data = obj.generateSession("usertoken","password")
logger.debug(" data is @ {} ".format(data))
refreshToken= data['data']['refreshToken']
feedToken=obj.getfeedToken()
logger.debug(" generated FEEDTOKEN is {} ".format(feedToken))
userProfile= obj.getProfile(refreshToken)
conn = http.client.HTTPSConnection("apiconnect.angelbroking.com")
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
# r = redis.Redis(host='localhost', port=6379, db=0)
conn = http.client.HTTPSConnection("apiconnect.angelbroking.com")

r = redis.StrictRedis('localhost', 6379,  db=0 , charset="utf-8", decode_responses=True)

r.get('number1')
conn.request("POST", "/rest/secure/angelbroking/order/v1/getLtpData", payload, headers)

res = conn.getresponse()
data = res.read()
data = json.loads(data)
ltp = data['data']['ltp']

def buypercentagecalc(price_purchased, price_sold):
    gain_loss = ((price_sold-price_purchased)/price_purchased) *100
    logger.debug('BUY TRADE OUTCOME IS {} %'.format(gain_loss))

def sellpercentagecalc(price_purchased, price_sold):
    gain_loss = ((price_purchased-price_sold)/price_purchased) *100
    logger.debug('SELL TRADE OUTCOME IS {} %'.format(gain_loss))

#increase target to be implemented

# def trailingsl(tradetype, ltp, sl):
#     purchase_price = 100
#     orgsl = sl
#     if(tradetype == "BUY"):
#         tempsl = sl + ((trailing_sl*sl)/100)
#         if(tempsl > ltp):
#             sl = tempsl
#     elif(tradetype == "SELL"):
#         tempsl = sl - ((trailing_sl*sl)/100)
#         if(tempsl < ltp):
#             sl = tempsl
#     return(sl)
# def trailingsl(tradetype, ltp, sl):

    # trailingsl(tradetype,sl,purchase price,ltp)
    # temp value=1
    # trailing price = purchase price+((temp value*purchase price)/100
    # if(ltp>trailing price)
    #   sl=sl+((temp value*sl)/100)
    #   purchase price = trailing price
    # trailing price = purchase price-((temp value*purchase price)/100
    # if(ltp>trailing price)
    #   sl=sl-((temp value*sl)/100)
    #   purchase price = trailing price
trailing_sl = 1
def trailingsl(tradetype, ltp, sl):
    purchase_price = r.get('STRIKEPRICE')
    if(tradetype == "BUY"):
        trailing_price = purchase_price+((trailing_sl*purchase_price)/100)
        if(ltp > trailing_price):
            sl = sl+((trailing_sl*sl)/100)
            r.set('STRIKEPRICE', trailing_price)

    if(tradetype == "SELL"):
        trailing_price = purchase_price-((trailing_sl*purchase_price)/100)
        if(ltp<trailing_price):
            sl=sl-((trailing_sl*sl)/100)
            r.set('STRIKEPRICE', trailing_price)
    return(sl)
        


def trade():
    
    if(str(r.get('TRADEACTIVE'))=="YES"):
        # global flag
        logger.debug(" TRADE IS ACTIVE ")
        conn.request("POST", "/rest/secure/angelbroking/order/v1/getLtpData", payload, headers)
        res = conn.getresponse()
        data = res.read()
        data = json.loads(data)
        ltp = float(data['data']['ltp'])
        sl = float(r.get('SL'))
        target = float(r.get('TARGET'))
        if(str(r.get('PNLFLAG')) == '0'):
            strikeprice  = float(r.get('STRIKEPRICE'))
            r.set('PNLFLAG',1)
            logger.debug("Flag Value Changed")
     
        if(r.get('ACTIVETRADETYPE')=='SELL'):
            if(ltp < target):
                sellpercentagecalc(strikeprice , ltp)
                logger.debug(" TARGET HIT @ {} ".format(target))
                r.set("TRADEACTIVE","NO")
            elif(ltp > sl):
                sellpercentagecalc(strikeprice , ltp)
                logger.debug(" SL HIT @ {} ".format(sl))
                r.set("TRADEACTIVE","NO")
            else:
                logger.debug(" SELL trade live waiting with ltp {} ".format(ltp))

        if(r.get('ACTIVETRADETYPE')=='BUY'):
            if(ltp > target):
                buypercentagecalc( strikeprice , ltp)
                logger.debug(" TARGET HIT @ {} ".format(target))
                r.set("TRADEACTIVE","NO")
            elif(ltp < sl):
                buypercentagecalc( strikeprice , ltp)                
                logger.debug(" SL HIT @ {} ".format(sl))
                r.set("TRADEACTIVE","NO")
            else:
                logger.debug(" BUY trade live waiting with ltp {} ".format(ltp))
               

                
    else:
        logger.debug(" TRADE INACTIVE {} ".format(str(r.get('TRADEACTIVE'))))
        


    time.sleep(1)  

while True:
    trade()
