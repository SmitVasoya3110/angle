from app.core.redis_client import redis_manager
from app.core.logger import get_logger
import json
import signal
import sys

logger = get_logger(__name__)

def handle_market_data(message):
    """Process market data messages from Redis"""
    try:
        if message['type'] != 'message':
            return
            
        data = json.loads(message['data'])
        token = data.get('token')
        exchange_type = data.get('exchange_type')
        last_traded_price = data.get('last_traded_price')
        
        logger.info(f"Exchange: {exchange_type}, Token: {token}, LTP: {last_traded_price}")
        
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")

def main():
    """Main subscriber function"""
    try:
        pubsub = redis_manager.redis_client.pubsub()
        
        # Subscribe to both exchange types
        channels = ['market_data:exchange:1', 'market_data:exchange:2']
        pubsub.subscribe(*channels)
        
        logger.info(f"Subscribed to channels: {channels}")
        logger.info("Waiting for messages... Press Ctrl+C to exit")
        
        # Handle graceful shutdown
        def signal_handler(sig, frame):
            logger.info("\nShutting down subscriber...")
            pubsub.unsubscribe()
            pubsub.close()
            sys.exit(0)
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start listening for messages
        for message in pubsub.listen():
            handle_market_data(message)
            
    except Exception as e:
        logger.error(f"Subscriber error: {str(e)}")
        raise
    finally:
        pubsub.unsubscribe()
        pubsub.close()

if __name__ == "__main__":
    main()
