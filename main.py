from fastapi import FastAPI
from app.api.routes import router
from app.services.websocket_manager import websocket_manager
from app.core.logger import get_logger
import traceback

logger = get_logger(__name__)
app = FastAPI()

# Include API routes
app.include_router(router)

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up WebSocket connections on shutdown"""
    logger.info("Initiating application shutdown")
    print("Shutting down WebSocket connection...")
    websocket_manager.cleanup()
    print("WebSocket cleanup completed.")
    logger.info("Application shutdown completed")

if __name__ == "__main__":
    import uvicorn
    import signal
    
    def signal_handler(sig, frame):
        logger.info("Received shutdown signal")
        print("\nReceived shutdown signal, cleaning up...")
        raise KeyboardInterrupt
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        logger.info("Starting application server")
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except KeyboardInterrupt:
        logger.info("Server shutdown requested")
        print("Server shutdown requested")
    except Exception as e:
        logger.error(f"Server error: {str(e)}\nTraceback:\n{traceback.format_exc()}")
        raise
    finally:
        logger.info("Cleanup complete")
        print("Cleanup complete")
