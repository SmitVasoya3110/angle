from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, validator
from typing import List, Dict
import multiprocessing
import threading
import time
from SmartApi import SmartConnect
from SmartWebsocketv2 import SmartWebSocketV2
import pyotp
from config import *

app = FastAPI()

@app.on_event("shutdown")
async def shutdown_event():
    print("Shutting down WebSocket connection...")
    if ws_manager and hasattr(ws_manager, 'stop_event') and ws_manager.stop_event:
        ws_manager.stop_event.set()
        if ws_manager.current_process:
            print("Waiting for WebSocket process to terminate...")
            ws_manager.current_process.join(timeout=5)
            if ws_manager.current_process.is_alive():
                print("Force terminating WebSocket process...")
                ws_manager.current_process.terminate()
                ws_manager.current_process.join(timeout=2)
                if ws_manager.current_process.is_alive():
                    print("Killing WebSocket process...")
                    ws_manager.current_process.kill()
    print("WebSocket cleanup completed.")

class TokenList(BaseModel):
    exchangeType: int
    tokens: List[int]

    @validator('exchangeType')
    def validate_exchange_type(cls, v):
        valid_types = {1, 2, 3, 4, 5, 7, 13}  # NSE_CM, NSE_FO, BSE_CM, BSE_FO, MCX_FO, NCX_FO, CDE_FO
        if v not in valid_types:
            raise ValueError(f'exchangeType must be one of {valid_types}')
        return v

class WebSocketManager:
    def __init__(self):
        self.current_process = None
        self.token_lists: List[Dict] = []
        self.stop_event = None
        self.initialize_auth()

    def initialize_auth(self):
        try:
            obj = SmartConnect(api_key=apikey, timeout=30)  # Increased timeout to 30 seconds
            data = obj.generateSession(username, pwd, pyotp.TOTP(token).now())
            self.auth_token = data['data']['jwtToken']
            self.feed_token = obj.getfeedToken()
            print(self.feed_token)
        except requests.exceptions.ConnectionError as e:
            print(f"Connection Error: {e}\nPlease check your internet connection and try again.")
            raise
        except Exception as e:
            print(f"Error during authentication: {e}")
            raise

    def websocket_process(self, token_lists, stop_event):
        try:
            print(f"Initializing WebSocket with token lists: {token_lists}")
            print(f"Auth token: {self.auth_token[:10]}...")
            print(f"Feed token: {self.feed_token}")
            
            sws = SmartWebSocketV2(self.auth_token, apikey, username, self.feed_token)
            correlation_id = "fastapi_feed"
            mode = 1

            def on_data(wsapp, message):
                print(f"Received data: {message}")

            def on_open(wsapp):
                print("WebSocket opened successfully")
                try:
                    print(f"Subscribing with correlation_id: {correlation_id}, mode: {mode}, token_lists: {token_lists}")
                    sws.subscribe(correlation_id, mode, token_lists)
                    print("Subscription request sent successfully")
                except Exception as sub_error:
                    print(f"Error during subscription: {sub_error}")
                    raise

            def on_error(wsapp, error):
                print(f"WebSocket error occurred: {error}")
                if not stop_event.is_set():
                    print("Attempting to reconnect...")

            def on_close(wsapp):
                print("WebSocket connection closed")

            sws.on_data = on_data
            sws.on_open = on_open
            sws.on_error = on_error
            sws.on_close = on_close

            print("Starting WebSocket connection...")
            ws_thread = threading.Thread(target=sws.connect)
            ws_thread.start()

            print("WebSocket thread started, waiting for events...")
            while not stop_event.is_set():
                time.sleep(1)

            print("Stop event received, closing connection...")
            sws.close_connection()
            ws_thread.join()
            print("WebSocket process completed")
            
        except Exception as e:
            print(f"Critical error in websocket_process: {str(e)}")
            raise

    def start_new_websocket(self, token_lists):
        print("Starting new WebSocket connection...")
        try:
            # Stop existing process if running
            if self.stop_event and self.current_process:
                print("Stopping existing WebSocket process...")
                self.stop_event.set()
                
                # Give the process a chance to clean up
                print("Waiting for process to terminate...")
                self.current_process.join(timeout=5)
                
                if self.current_process.is_alive():
                    print("Process did not terminate gracefully, terminating forcefully...")
                    self.current_process.terminate()
                    self.current_process.join(timeout=2)
                    
                    if self.current_process.is_alive():
                        print("Process still alive, killing it...")
                        self.current_process.kill()
                        self.current_process.join(timeout=1)
                
                self.current_process = None
                self.stop_event = None

            # Create new process
            print("Creating new WebSocket process...")
            self.stop_event = multiprocessing.Event()
            self.current_process = multiprocessing.Process(
                target=self.websocket_process,
                args=(token_lists, self.stop_event)
            )
            
            print("Starting new process...")
            self.current_process.start()
            
            # Wait for process to start
            start_time = time.time()
            while time.time() - start_time < 10:  # Wait up to 10 seconds
                if self.current_process.is_alive():
                    print("New WebSocket process started successfully")
                    return
                time.sleep(0.1)
            
            raise Exception("WebSocket process failed to start within timeout")
            
        except Exception as e:
            print(f"Error in start_new_websocket: {e}")
            if self.current_process and self.current_process.is_alive():
                self.current_process.terminate()
            self.current_process = None
            self.stop_event = None
            raise

ws_manager = WebSocketManager()

@app.post("/watchlist/add")
async def add_to_watchlist(tokens: List[TokenList]):
    try:
        # Convert tokens to dict and validate
        token_dicts = [token.dict() for token in tokens]
        
        # Update token list and start new WebSocket
        ws_manager.token_lists = token_dicts
        ws_manager.start_new_websocket(token_dicts)
        
        return {
            "message": "Watchlist updated successfully",
            "current_tokens": token_dicts
        }
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        print(f"Error in add_to_watchlist: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/watchlist")
async def get_watchlist():
    return {"tokens": ws_manager.token_lists}

if __name__ == "__main__":
    import uvicorn
    import signal
    
    def signal_handler(sig, frame):
        print("\nReceived shutdown signal, cleaning up...")
        # This will trigger FastAPI's shutdown event
        raise KeyboardInterrupt
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except KeyboardInterrupt:
        print("Server shutdown requested")
    finally:
        print("Cleanup complete")
