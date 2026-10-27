import os
import sys
import time
import socket
from typing import Dict, List, Optional
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import RedirectResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from ytmusicapi import YTMusic
import yt_dlp

app = FastAPI(title="SpotiWave Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DIR_PATH = os.path.dirname(os.path.abspath(__file__))
ytm = YTMusic()

# Memory cache for audio stream URLs to make playback instantaneous
stream_cache: Dict[str, Dict] = {}

# Active WebSocket connections for Spotify Connect style syncing
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.current_state = {
            "track": None,
            "isPlaying": False,
            "position": 0,
            "activeDevice": "pc"
        }

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        # Send current player state on connect
        await websocket.send_json({"type": "INIT_STATE", "data": self.current_state})

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict, sender: WebSocket = None):
        for connection in self.active_connections:
            if connection != sender:
                try:
                    await connection.send_json(message)
                except Exception:
                    pass

manager = ConnectionManager()

def get_best_thumbnail(thumbnails: list) -> str:
    if not thumbnails:
        return "https://images.unsplash.com/photo-1614613535308-eb5fbd3d2c17?w=500&h=500&fit=crop"
    return thumbnails[-1].get("url", "")

@app.get("/api/search")
async def search_tracks(q: str = Query(..., min_length=1)):
    try:
        results = ytm.search(q, filter="songs")
        tracks = []
        for item in results[:20]:
            video_id = item.get("videoId")
            if not video_id:
                continue
            
            artists = ", ".join([a.get("name", "") for a in item.get("artists", [])]) or "Artiste Inconnu"
            album = item.get("album", {}).get("name", "") if item.get("album") else ""
            cover = get_best_thumbnail(item.get("thumbnails", []))

            tracks.append({
                "id": video_id,
                "title": item.get("title", "Titre Inconnu"),
                "artist": artists,
                "album": album,
                "duration": item.get("duration", "0:00"),
                "cover": cover
            })
        return JSONResponse(content={"success": True, "tracks": tracks})
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})

@app.get("/api/stream/{video_id}")
async def get_stream_url(video_id: str, redirect: bool = False):
    now = time.time()
    # Check cache (valid 3 hours)
    if video_id in stream_cache and (now - stream_cache[video_id]["cached_at"]) < 10800:
        url = stream_cache[video_id]["url"]
        if redirect:
            return RedirectResponse(url=url, status_code=307)
        return JSONResponse(content={"url": url})

    try:
        ydl_opts = {
            "format": "bestaudio/best",
            "quiet": True,
            "no_warnings": True,
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
            url = info.get("url")
            if not url:
                return JSONResponse(status_code=404, content={"error": "Audio stream not found"})

            stream_cache[video_id] = {
                "url": url,
                "cached_at": now
            }
            if redirect:
                return RedirectResponse(url=url, status_code=307)
            return JSONResponse(content={"url": url})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")
            payload = data.get("data", {})

            if msg_type == "UPDATE_STATE":
                manager.current_state.update(payload)
                await manager.broadcast({"type": "SYNC_STATE", "data": manager.current_state}, sender=websocket)
            elif msg_type == "CONTROL_COMMAND":
                # Remote control command (play/pause/next/prev/seek)
                await manager.broadcast({"type": "REMOTE_COMMAND", "data": payload}, sender=websocket)
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)

# Static files & PWA routes
@app.get("/")
async def get_index():
    return FileResponse(os.path.join(DIR_PATH, "index.html"))

@app.get("/manifest.json")
async def get_manifest():
    return FileResponse(os.path.join(DIR_PATH, "manifest.json"), media_type="application/manifest+json")

@app.get("/sw.js")
async def get_sw():
    return FileResponse(os.path.join(DIR_PATH, "sw.js"), media_type="application/javascript")

@app.get("/icon.svg")
async def get_icon():
    return FileResponse(os.path.join(DIR_PATH, "icon.svg"), media_type="image/svg+xml")

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

if __name__ == "__main__":
    import uvicorn
    local_ip = get_local_ip()
    print("\n" + "="*60)
    print("  SPOTIWAVE CLOUD / LOCAL BACKEND READY")
    print("="*60)
    print(f" [PC]     : http://localhost:8000")
    print(f" [iPhone] : http://{local_ip}:8000")
    print("="*60 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
