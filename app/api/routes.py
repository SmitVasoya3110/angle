from fastapi import APIRouter, HTTPException, Request
from typing import Dict, List
from ..core.redis_client import redis_manager
from ..services.websocket_manager import websocket_manager
from ..core.logger import get_logger, log_exceptions
import json
import traceback

logger = get_logger(__name__)
router = APIRouter()

@router.post("/token-update")
async def token_update():
    """Endpoint to receive token updates"""
    try:
        # Get the raw request body
        # body = await request.json()
        # logger.info(f"Received token update notification: {body}")
        
        # Get new tokens from Redis
        new_tokens = redis_manager.get_tokens()
        if not new_tokens:
            logger.warning("No tokens found in Redis")
            return {"message": "No tokens found in Redis"}
            
        # Update WebSocket if tokens have changed
        updated = websocket_manager.update_tokens(new_tokens)
        return {
            "message": "WebSocket updated with new tokens" if updated else "No token updates needed",
            "tokens": websocket_manager.token_lists,
            "updated": updated
        }
            
    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON in request body: {str(e)}"
        logger.error(f"{error_msg}\nTraceback:\n{traceback.format_exc()}")
        raise HTTPException(status_code=400, detail=error_msg)
    except Exception as e:
        error_msg = f"Error processing token update: {str(e)}"
        logger.error(f"{error_msg}\nTraceback:\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=error_msg)

@router.get("/watchlist")
async def get_watchlist():
    """Get current watchlist tokens"""
    logger.info("Getting current watchlist")
    return {"tokens": websocket_manager.token_lists}
