from SmartApi import SmartConnect
from SmartWebsocketv2 import SmartWebSocketV2
import pyotp
from config import *
import time

def test_websocket():
    print("Initializing SmartConnect...")
    obj = SmartConnect(api_key=apikey)
    
    print("Generating session...")
    data = obj.generateSession(username, pwd, pyotp.TOTP(token).now())
    print("Session response:", data)
    
    auth_token = data['data']['jwtToken']
    feed_token = obj.getfeedToken()
    print("Feed token:", feed_token)
    
    print("Creating WebSocket instance...")
    sws = SmartWebSocketV2(auth_token, apikey, username, feed_token)
    
    def on_data(wsapp, message):
        print("Received data:", message)
    
    def on_open(wsapp):
        print("WebSocket opened")
        token_list = [{"exchangeType": 1, "tokens": [26009]}]  # Example token
        print("Subscribing to:", token_list)
        sws.subscribe("test_feed", 1, token_list)
    
    def on_error(wsapp, error):
        print("Error:", error)
    
    def on_close(wsapp):
        print("WebSocket closed")
    
    sws.on_data = on_data
    sws.on_open = on_open
    sws.on_error = on_error
    sws.on_close = on_close
    
    print("Connecting to WebSocket...")
    sws.connect()

if __name__ == "__main__":
    test_websocket()
