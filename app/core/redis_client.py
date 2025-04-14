import redis
import json
from typing import Optional, Dict, List
from .config import settings
from .logger import get_logger, log_exceptions
import traceback

logger = get_logger(__name__)

class RedisManager:
    def __init__(self):
        try:
            self.redis_client = redis.from_url(settings.REDIS_URL)
            logger.info("Redis connection established")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}\nTraceback:\n{traceback.format_exc()}")
            raise
        
    @log_exceptions(logger)
    def get_tokens(self) -> Optional[List[Dict]]:
        """Get tokens from Redis"""
        tokens_json = self.redis_client.get('smartapi:tokens')
        if tokens_json:
            try:
                return json.loads(tokens_json)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode tokens JSON: {tokens_json}\nError: {str(e)}")
                raise
        logger.info("No tokens found in Redis")
        return None

redis_manager = RedisManager()
