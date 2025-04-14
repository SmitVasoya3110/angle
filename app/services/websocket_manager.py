import multiprocessing
import threading
import time
import pyotp
from typing import List, Dict
from SmartApi import SmartConnect
from SmartWebsocketv2 import SmartWebSocketV2
from ..core.config import settings
from ..core.logger import get_logger, log_exceptions
import traceback

logger = get_logger(__name__)

class WebSocketManager:
    def __init__(self):
        self.current_process = None
        self.token_lists: List[Dict] = []
        self.stop_event = None
        self.initialize_auth()

    @log_exceptions(logger)
    def initialize_auth(self):
        """Initialize authentication with SmartAPI"""
        logger.info("Initializing SmartAPI authentication")
        try:
            obj = SmartConnect(api_key=settings.apikey, timeout=30)
            data = obj.generateSession(settings.username, settings.pwd, 
                                    pyotp.TOTP(settings.token).now())
            self.auth_token = data['data']['jwtToken']
            self.feed_token = obj.getfeedToken()
            logger.info(f"Authentication successful. Feed token received")
        except Exception as e:
            logger.error(f"Error during authentication: {str(e)}\nTraceback:\n{traceback.format_exc()}")
            raise

    @log_exceptions(logger)
    def websocket_process(self, token_lists, stop_event):
        """Handle WebSocket connection and data streaming"""
        logger.info(f"Starting WebSocket process with token lists: {token_lists}")
        
        sws = SmartWebSocketV2(self.auth_token, settings.apikey, 
                             settings.username, self.feed_token)
        correlation_id = "fastapi_feed"
        mode = 1

        def on_data(wsapp, message):
            if not stop_event.is_set():  # Only process data if not stopping
                logger.info(f"Received WebSocket data: {message}")

        def on_open(wsapp):
            if not stop_event.is_set():  # Only subscribe if not stopping
                logger.info("WebSocket connection opened")
                try:
                    sws.subscribe(correlation_id, mode, token_lists)
                    logger.info("Successfully subscribed to token feed")
                except Exception as sub_error:
                    logger.error(f"Subscription error: {str(sub_error)}\nTraceback:\n{traceback.format_exc()}")
                    raise

        def on_error(wsapp, error):
            if not stop_event.is_set():  # Only attempt reconnect if not stopping
                logger.error(f"WebSocket error: {str(error)}")
                logger.info("Attempting to reconnect...")

        def on_close(wsapp):
            logger.info("WebSocket connection closed")

        sws.on_data = on_data
        sws.on_open = on_open
        sws.on_error = on_error
        sws.on_close = on_close

        logger.info("Starting WebSocket connection thread")
        ws_thread = threading.Thread(target=sws.connect)
        ws_thread.start()

        while not stop_event.is_set():
            time.sleep(1)

        # Unsubscribe before closing
        try:
            logger.info("Unsubscribing from token feed")
            sws.unsubscribe(correlation_id, mode, token_lists)
        except Exception as e:
            logger.error(f"Error unsubscribing: {str(e)}")

        logger.info("Stop event received, closing connection")
        sws.close_connection()
        ws_thread.join(timeout=5)  # Add timeout to thread join
        if ws_thread.is_alive():
            logger.warning("WebSocket thread did not close cleanly")
        logger.info("WebSocket process completed")

    @log_exceptions(logger)
    def start_new_websocket(self, token_lists):
        """Start a new WebSocket connection with updated tokens"""
        logger.info("Starting new WebSocket connection")
        
        # Stop existing process if running
        if self.stop_event and self.current_process:
            logger.info("Stopping existing WebSocket process")
            self.stop_event.set()
            
            # Wait for process to stop gracefully
            self.current_process.join(timeout=5)
            
            if self.current_process.is_alive():
                logger.warning("Process did not terminate gracefully, forcing termination")
                self.current_process.terminate()
                self.current_process.join(timeout=2)
                
                if self.current_process.is_alive():
                    logger.warning("Process still alive, killing it")
                    self.current_process.kill()
                    self.current_process.join(timeout=1)
            
            self.current_process = None
            self.stop_event = None

        # Create new process
        logger.info("Creating new WebSocket process")
        self.stop_event = multiprocessing.Event()
        self.current_process = multiprocessing.Process(
            target=self.websocket_process,
            args=(token_lists, self.stop_event)
        )
        
        logger.info("Starting new process")
        self.current_process.start()
        
        # Wait for process to start
        start_time = time.time()
        while time.time() - start_time < 10:  # Wait up to 10 seconds
            if self.current_process.is_alive():
                logger.info("New WebSocket process started successfully")
                return True
            time.sleep(0.1)
        
        raise Exception("WebSocket process failed to start within timeout")

    @log_exceptions(logger)
    def update_tokens(self, new_tokens: List[Dict]) -> bool:
        """Update tokens and restart WebSocket if needed"""
        if new_tokens != self.token_lists:
            logger.info(f"Updating tokens: {new_tokens}")
            # First update token list
            self.token_lists = new_tokens
            # Then start new connection
            return self.start_new_websocket(new_tokens)
        logger.info("Tokens unchanged, no update needed")
        return False

    @log_exceptions(logger)
    def cleanup(self):
        """Clean up WebSocket connections"""
        logger.info("Starting WebSocket cleanup")
        if self.stop_event:
            self.stop_event.set()
            if self.current_process:
                self.current_process.join(timeout=5)
                if self.current_process.is_alive():
                    logger.warning("Process did not terminate gracefully, forcing termination")
                    self.current_process.terminate()
                    self.current_process.join(timeout=2)
                    if self.current_process.is_alive():
                        logger.warning("Process still alive, killing it")
                        self.current_process.kill()
        logger.info("WebSocket cleanup completed")

websocket_manager = WebSocketManager()
