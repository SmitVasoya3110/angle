from SmartApi import SmartConnect
import threading
import pyotp,time
from config import  *


obj=SmartConnect(api_key=apikey)
data = obj.generateSession(username,pwd,pyotp.TOTP(token).now())
print(data)
AUTH_TOKEN = data['data']['jwtToken']
refreshToken= data['data']['refreshToken']
FEED_TOKEN=obj.getfeedToken()
res = obj.getProfile(refreshToken)
print(res['data']['exchanges'])



#------- Websocket code

from SmartWebsocketv2 import SmartWebSocketV2
correlation_id = "dft_test1"
action = 1
mode = 1

token_list = [{"exchangeType": 5, "tokens": ["242738"]} , {"exchangeType": 1, "tokens": ["26009"]}]

sws = SmartWebSocketV2(AUTH_TOKEN, apikey, username, FEED_TOKEN)


def on_data(wsapp, message):
    print("Ticks: {}".format(message))


def on_open(wsapp):
    print("on open")
    sws.subscribe(correlation_id, mode, token_list)


def on_error(wsapp, error):
    print(error)


def on_close(wsapp):
    print("Close")


# Assign the callbacks.
sws.on_open = on_open
sws.on_data = on_data
sws.on_error = on_error
sws.on_close = on_close


threading.Thread(target = sws.connect).start()

print('Control Released')
time.sleep(10)
 
token_list2 = [{"exchangeType": 5, "tokens": ["244999","246083"]}]
sws.subscribe(correlation_id, mode, token_list2)
print(f'\n ------ subscribe {token_list2} ------- \n')
time.sleep(10)
sws.unsubscribe(correlation_id, mode, token_list2)
print(f'\n ---------usubscribe {token_list2}------ \n')



time.sleep(100)
sws.close_connection()
print(f'Closed')















































