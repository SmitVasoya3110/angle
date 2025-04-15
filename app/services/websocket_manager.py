import threading
import time
import pyotp
from typing import List, Dict, Optional
from dataclasses import dataclass
from SmartApi import SmartConnect
from SmartWebsocketv2 import SmartWebSocketV2
from ..core.config import settings
from ..core.logger import get_logger, log_exceptions
from ..core.redis_client import redis_manager
import json
import traceback
from datetime import datetime
import asyncio

logger = get_logger(__name__)

@dataclass
class WebSocketConnection:
    """Class to hold WebSocket connection details"""
    id: str
    websocket: SmartWebSocketV2
    thread: threading.Thread
    stop_event: threading.Event
    token_lists: List[Dict]
    created_at: datetime
    is_active: bool = True

class WebSocketManager:
    def __init__(self):
        self.connections: Dict[str, WebSocketConnection] = {}
        self.token_lists: List[Dict] = []
        self.auth_data = self.initialize_auth()

    @log_exceptions(logger)
    def initialize_auth(self) -> Dict:
        """Initialize authentication with SmartAPI"""
        logger.info("Initializing SmartAPI authentication")
        try:
            obj = SmartConnect(api_key=settings.apikey, timeout=30)
            data = obj.generateSession(settings.username, settings.pwd, 
                                    pyotp.TOTP(settings.token).now())
            return {
                'auth_token': data['data']['jwtToken'],
                'feed_token': obj.getfeedToken(),
                'api_key': settings.apikey,
                'username': settings.username
            }
        except Exception as e:
            logger.error(f"Error during authentication: {str(e)}\nTraceback:\n{traceback.format_exc()}")
            raise

    def _run_websocket(self, connection_id: str, token_lists: List[Dict], stop_event: threading.Event):
        """Run WebSocket connection in a thread"""
        try:
            sws = SmartWebSocketV2(
                self.auth_data['auth_token'],
                self.auth_data['api_key'],
                self.auth_data['username'],
                self.auth_data['feed_token']
            )

            correlation_id = f"feed_{connection_id}"
            mode = 2

            def on_data(wsapp, message):
                if not stop_event.is_set():
                    logger.info(f"[{connection_id}] Received data: {message}")
                    # Publish to Redis asynchronously using a background thread
                    try:
                        if isinstance(message, str):
                            message = json.loads(message)
                        threading.Thread(
                            target=redis_manager.publish_market_data,
                            args=(message,),
                            daemon=True
                        ).start()
                    except Exception as e:
                        logger.error(f"[{connection_id}] Failed to publish data: {str(e)}")

            def on_open(wsapp):
                if not stop_event.is_set():
                    logger.info(f"[{connection_id}] Connection opened")
                    try:
                        sws.subscribe(correlation_id, mode, token_lists)
                        logger.info(f"[{connection_id}] Subscribed to token feed")
                    except Exception as sub_error:
                        logger.error(f"[{connection_id}] Subscription error: {str(sub_error)}")
                        raise

            def on_error(wsapp, error):
                if not stop_event.is_set():
                    logger.error(f"[{connection_id}] Error: {str(error)}")

            def on_close(wsapp):
                logger.info(f"[{connection_id}] Connection closed")
                if connection_id in self.connections:
                    self.connections[connection_id].is_active = False

            sws.on_data = on_data
            sws.on_open = on_open
            sws.on_error = on_error
            sws.on_close = on_close

            # Store connection in registry
            if connection_id in self.connections:
                self.connections[connection_id].websocket = sws

            sws.connect()

            while not stop_event.is_set():
                time.sleep(0.1)

            # Cleanup
            try:
                logger.info(f"[{connection_id}] Unsubscribing from token feed")
                sws.unsubscribe(correlation_id, mode, token_lists)
            except Exception as e:
                logger.error(f"[{connection_id}] Error unsubscribing: {str(e)}")

            logger.info(f"[{connection_id}] Closing connection")
            sws.close_connection()

        except Exception as e:
            logger.error(f"[{connection_id}] Thread error: {str(e)}")
        finally:
            if connection_id in self.connections:
                self.connections[connection_id].is_active = False

    @log_exceptions(logger)
    def start_new_websocket(self, token_lists: List[Dict]) -> bool:
        """Start a new WebSocket connection with updated tokens"""
        connection_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.info(f"Starting new WebSocket connection: {connection_id}")

        # Create stop event and thread
        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._run_websocket,
            args=(connection_id, token_lists, stop_event)
        )
        thread.daemon = True

        # Create connection object
        connection = WebSocketConnection(
            id=connection_id,
            websocket=None,  # Will be set in _run_websocket
            thread=thread,
            stop_event=stop_event,
            token_lists=token_lists,
            created_at=datetime.now()
        )

        # Store in registry
        self.connections[connection_id] = connection

        # Start thread
        thread.start()
        return True

    @log_exceptions(logger)
    def update_tokens(self, new_tokens: List[Dict]) -> bool:
        """Update tokens and restart WebSocket if needed"""
        if new_tokens != self.token_lists:
            logger.info(f"Updating tokens: {new_tokens}")
            self.token_lists = new_tokens
            return self.start_new_websocket(new_tokens)
        logger.info("Tokens unchanged, no update needed")
        return self.start_new_websocket(new_tokens)
        # return False

    def _cleanup_connection(self, connection_id: str):
        """Clean up a specific connection"""
        if connection_id in self.connections:
            conn = self.connections[connection_id]
            logger.info(f"Cleaning up connection {connection_id}")
            conn.stop_event.set()
            if conn.thread.is_alive():
                conn.thread.join(timeout=5)
            del self.connections[connection_id]

    @log_exceptions(logger)
    def cleanup(self):
        """Clean up all WebSocket connections"""
        logger.info("Starting WebSocket cleanup")
        
        # Make a copy of keys as we'll be modifying the dict
        connection_ids = list(self.connections.keys())
        
        for connection_id in connection_ids:
            self._cleanup_connection(connection_id)

        logger.info("WebSocket cleanup completed")

    def get_active_connections(self) -> List[Dict]:
        """Get list of active connections"""
        return [
            {
                'id': conn.id,
                'created_at': conn.created_at.isoformat(),
                'is_active': conn.is_active,
                'token_count': len(conn.token_lists)
            }
            for conn in self.connections.values()
        ]

websocket_manager = WebSocketManager()
