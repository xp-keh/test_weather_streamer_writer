from typing import Set
from fastapi import WebSocket

class WebSocketManager:
    """Manages WebSocket connections and broadcasts messages to clients."""
    
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        """Accept a WebSocket connection and add it to the active connections."""
        await websocket.accept()
        self.active_connections.add(websocket)

    async def disconnect(self, websocket: WebSocket):
        """Remove a WebSocket connection when the client disconnects."""
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        """Send a message to all active WebSocket clients."""
        for connection in self.active_connections:
            await connection.send_text(message)