import os
import sys
import time
import socket
import re
import urllib.request
import json
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

# Memory caches
stream_cache: Dict[str, Dict] = {}
feed_cache: Dict[str, Dict] = {}

class ConnectionManager:
    def __init__(self):
        # websocket -> dict info { id, name, type }
        self.connections: Dict[WebSocket, dict] = {}
        self.active_speaker_id: Optional[str] = None
        self.sync_mode: str = "connect"  # "connect" (1 speaker) or "jam" (all play)
        self.current_state = {
            "track": None,
            "isPlaying": False,
            "position": 0,
            "timestamp": int(time.time() * 1000),
            "activeSpeakerId": None,
            "activeSpeakerName": "Aucun",
            "syncMode": "connect"
        }

    def get_devices_list(self) -> List[dict]:
        devs = []
        for ws, info in list(self.connections.items()):
            if info.get("id"):
                devs.append({
                    "id": info["id"],
                    "name": info.get("name", "Appareil"),
                    "type": info.get("type", "pc"),
                    "isSpeaker": (info["id"] == self.active_speaker_id)
                })
        return devs

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.connections[websocket] = {"id": "", "name": "Appareil", "type": "pc"}
        await websocket.send_json({
            "type": "INIT_STATE",
            "data": {
                **self.current_state,
                "devices": self.get_devices_list()
            }
        })

    def disconnect(self, websocket: WebSocket):
        info = self.connections.pop(websocket, None)
        if info and info.get("id") == self.active_speaker_id:
            # Reassign speaker to next available
            remaining = [d for d in self.connections.values() if d.get("id")]
            if remaining:
                self.active_speaker_id = remaining[0]["id"]
                self.current_state["activeSpeakerId"] = self.active_speaker_id
                self.current_state["activeSpeakerName"] = remaining[0]["name"]
            else:
                self.active_speaker_id = None
                self.current_state["activeSpeakerId"] = None
                self.current_state["activeSpeakerName"] = "Aucun"

    async def broadcast(self, message: dict, sender: WebSocket = None):
        for ws in list(self.connections.keys()):
            if ws != sender:
                try:
                    await ws.send_json(message)
                except Exception:
                    pass

    async def broadcast_all(self, message: dict):
        for ws in list(self.connections.keys()):
            try:
                await ws.send_json(message)
            except Exception:
                pass

    async def send_to_device(self, device_id: str, message: dict) -> bool:
        for ws, info in list(self.connections.items()):
            if info.get("id") == device_id:
                try:
                    await ws.send_json(message)
                    return True
                except Exception:
                    pass
        return False

manager = ConnectionManager()

def get_best_thumbnail(thumbnails: list) -> str:
    if not thumbnails:
        return "https://images.unsplash.com/photo-1614613535308-eb5fbd3d2c17?w=500&h=500&fit=crop"
    return thumbnails[-1].get("url", "")

def format_track(item: dict) -> Optional[dict]:
    video_id = item.get("videoId")
    if not video_id:
        return None
    artists = ", ".join([a.get("name", "") for a in item.get("artists", [])]) if item.get("artists") else "Artiste Inconnu"
    album = item.get("album", {}).get("name", "") if item.get("album") else ""
    cover = get_best_thumbnail(item.get("thumbnails", []))
    return {
        "id": video_id,
        "title": item.get("title", "Titre Inconnu"),
        "artist": artists,
        "album": album,
        "duration": item.get("duration", "3:30"),
        "cover": cover
    }

@app.get("/api/feed")
async def get_live_feed():
    now = time.time()
    if "home_feed" in feed_cache and (now - feed_cache["home_feed"]["time"]) < 3600:
        return JSONResponse(content={"success": True, "data": feed_cache["home_feed"]["data"]})

    try:
        charts_fr = ytm.get_charts(country='FR')
        videos_fr = charts_fr.get('videos', [])
        
        trending_tracks = []
        top50_tracks = []

        if len(videos_fr) > 0:
            p_trend = ytm.get_playlist(videos_fr[0]['playlistId'])
            for t in p_trend.get('tracks', [])[:15]:
                formatted = format_track(t)
                if formatted:
                    trending_tracks.append(formatted)

        if len(videos_fr) > 1:
            p_top = ytm.get_playlist(videos_fr[1]['playlistId'])
            for t in p_top.get('tracks', [])[:20]:
                formatted = format_track(t)
                if formatted:
                    top50_tracks.append(formatted)

        # Rap FR & Global hits search
        rap_results = ytm.search("Top Rap Français", filter="songs")[:15]
        rap_tracks = [format_track(t) for t in rap_results if format_track(t)]

        electro_results = ytm.search("Top Hits Pop Electro", filter="songs")[:15]
        electro_tracks = [format_track(t) for t in electro_results if format_track(t)]

        feed_data = {
            "trending": trending_tracks,
            "top50": top50_tracks,
            "rap": rap_tracks,
            "electro": electro_tracks
        }

        feed_cache["home_feed"] = {"time": now, "data": feed_data}
        return JSONResponse(content={"success": True, "data": feed_data})
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})

@app.get("/api/spotify/import")
async def import_spotify(url: str = Query(..., min_length=1)):
    m = re.search(r'(playlist|album|track)[/:]([a-zA-Z0-9]+)', url)
    if not m:
        return JSONResponse(status_code=400, content={'success': False, 'error': 'Lien Spotify invalide (ex: https://open.spotify.com/playlist/...)'})
    
    item_type = m.group(1)
    item_id = m.group(2)
    
    embed_url = f'https://open.spotify.com/embed/{item_type}/{item_id}'
    req = urllib.request.Request(embed_url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
    try:
        html = urllib.request.urlopen(req, timeout=10).read().decode('utf-8')
        m_json = re.search(r'id="__NEXT_DATA__"[^>]*>(.*?)</script>', html)
        if not m_json:
            return JSONResponse(status_code=404, content={'success': False, 'error': 'Impossible de lire les données publiques de cette playlist Spotify.'})
        
        data = json.loads(m_json.group(1))
        page_props = data.get('props', {}).get('pageProps', {})
        entity = page_props.get('state', {}).get('data', {}).get('entity', {})
        
        name = entity.get('name') or page_props.get('title') or 'Playlist Spotify'
        cover = "https://images.unsplash.com/photo-1614613535308-eb5fbd3d2c17?w=500&h=500&fit=crop"
        visual = entity.get('visualIdentity', {}) or {}
        if visual.get('image') and len(visual['image']) > 0:
            cover = visual['image'][-1].get('url', cover)

        tracks = []
        track_list = entity.get('trackList', [])
        for t in track_list:
            raw_dur = t.get('duration')
            formatted_dur = "3:30"
            if raw_dur:
                try:
                    num_ms = int(raw_dur)
                    if num_ms > 1000:
                        total_secs = num_ms // 1000
                    else:
                        total_secs = num_ms
                    formatted_dur = f"{total_secs // 60}:{total_secs % 60:02d}"
                except Exception:
                    formatted_dur = "3:30"

            tracks.append({
                'title': t.get('title', 'Titre'),
                'artist': t.get('subtitle', 'Artiste'),
                'duration': formatted_dur,
                'cover': cover
            })
            
        return JSONResponse(content={
            'success': True,
            'type': item_type,
            'title': name,
            'cover': cover,
            'count': len(tracks),
            'tracks': tracks
        })
    except Exception as e:
        return JSONResponse(status_code=500, content={'success': False, 'error': str(e)})

@app.get("/api/resolve")
async def resolve_track_query(q: str = Query(..., min_length=1)):
    try:
        # First attempt: search exact query in songs
        results = ytm.search(q, filter="songs")
        if not results:
            # Fallback attempt: search in general
            results = ytm.search(q)
        if results and len(results) > 0:
            for item in results:
                track = format_track(item)
                if track:
                    return JSONResponse(content={"success": True, "track": track})
        return JSONResponse(status_code=404, content={"success": False, "error": "Titre introuvable"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})

def extract_audio_url(video_id: str) -> Optional[str]:
    # 1. First try public high-speed Invidious API (bypasses datacenter bot blocks)
    invidious_endpoints = [
        f"https://yt.chocolatemoo53.com/api/v1/videos/{video_id}",
        f"https://invidious.jing.rocks/api/v1/videos/{video_id}"
    ]
    for ep in invidious_endpoints:
        try:
            req = urllib.request.Request(ep, headers={"User-Agent": "Mozilla/5.0"})
            res = urllib.request.urlopen(req, timeout=4)
            data = json.loads(res.read())
            audios = [f for f in data.get("adaptiveFormats", []) if "audio" in f.get("type", "")]
            if audios:
                audios.sort(key=lambda x: int(x.get("bitrate", 0)), reverse=True)
                return audios[0]["url"]
        except Exception:
            pass

    # 2. Second fallback: yt-dlp with mobile clients
    try:
        ydl_opts = {
            "format": "bestaudio/best",
            "quiet": True,
            "no_warnings": True,
            "extractor_args": {
                "youtube": {
                    "player_client": ["android", "ios", "mweb"]
                }
            }
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
            return info.get("url")
    except Exception:
        pass

    return None

@app.get("/api/stream/{video_id}")
async def get_stream_url(video_id: str, redirect: bool = False):
    now = time.time()
    if video_id in stream_cache and (now - stream_cache[video_id]["cached_at"]) < 10800:
        url = stream_cache[video_id]["url"]
        if redirect:
            return RedirectResponse(url=url, status_code=307)
        return JSONResponse(content={"url": url})

    url = extract_audio_url(video_id)
    if url:
        stream_cache[video_id] = {
            "url": url,
            "cached_at": now
        }
        if redirect:
            return RedirectResponse(url=url, status_code=307)
        return JSONResponse(content={"url": url})

    return JSONResponse(status_code=404, content={"error": "Audio stream not found"})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")
            payload = data.get("data", {})

            if msg_type == "REGISTER_DEVICE":
                dev_id = payload.get("id")
                dev_name = payload.get("name", "Appareil")
                dev_type = payload.get("type", "pc")
                manager.connections[websocket] = {
                    "id": dev_id,
                    "name": dev_name,
                    "type": dev_type
                }
                # If no active speaker yet, assign to first registered device
                if not manager.active_speaker_id:
                    manager.active_speaker_id = dev_id
                    manager.current_state["activeSpeakerId"] = dev_id
                    manager.current_state["activeSpeakerName"] = dev_name

                await manager.broadcast_all({
                    "type": "DEVICES_UPDATE",
                    "data": {
                        "devices": manager.get_devices_list(),
                        "activeSpeakerId": manager.active_speaker_id,
                        "activeSpeakerName": manager.current_state.get("activeSpeakerName", "Aucun"),
                        "syncMode": manager.sync_mode
                    }
                })

            elif msg_type == "UPDATE_STATE":
                manager.current_state.update(payload)
                manager.current_state["timestamp"] = int(time.time() * 1000)
                await manager.broadcast({
                    "type": "SYNC_STATE",
                    "data": manager.current_state
                }, sender=websocket)

            elif msg_type == "CONTROL_COMMAND":
                action = payload.get("action")
                # If in jam mode, broadcast to everyone
                if manager.sync_mode == "jam":
                    await manager.broadcast({
                        "type": "REMOTE_COMMAND",
                        "data": payload
                    }, sender=websocket)
                else:
                    # In connect mode: forward command to the active speaker
                    if manager.active_speaker_id:
                        sent = await manager.send_to_device(manager.active_speaker_id, {
                            "type": "REMOTE_COMMAND",
                            "data": payload
                        })
                        if not sent:
                            await manager.broadcast({"type": "REMOTE_COMMAND", "data": payload}, sender=websocket)
                    else:
                        await manager.broadcast({"type": "REMOTE_COMMAND", "data": payload}, sender=websocket)

            elif msg_type == "SET_ACTIVE_SPEAKER":
                new_speaker_id = payload.get("deviceId")
                manager.active_speaker_id = new_speaker_id
                manager.current_state["activeSpeakerId"] = new_speaker_id
                for ws, info in manager.connections.items():
                    if info.get("id") == new_speaker_id:
                        manager.current_state["activeSpeakerName"] = info.get("name", "Appareil")
                        break
                await manager.broadcast_all({
                    "type": "DEVICES_UPDATE",
                    "data": {
                        "devices": manager.get_devices_list(),
                        "activeSpeakerId": manager.active_speaker_id,
                        "activeSpeakerName": manager.current_state.get("activeSpeakerName", "Aucun"),
                        "syncMode": manager.sync_mode
                    }
                })

            elif msg_type == "SET_SYNC_MODE":
                new_mode = payload.get("syncMode", "connect")
                manager.sync_mode = new_mode
                manager.current_state["syncMode"] = new_mode
                await manager.broadcast_all({
                    "type": "DEVICES_UPDATE",
                    "data": {
                        "devices": manager.get_devices_list(),
                        "activeSpeakerId": manager.active_speaker_id,
                        "activeSpeakerName": manager.current_state.get("activeSpeakerName", "Aucun"),
                        "syncMode": manager.sync_mode
                    }
                })

    except WebSocketDisconnect:
        manager.disconnect(websocket)
        await manager.broadcast_all({
            "type": "DEVICES_UPDATE",
            "data": {
                "devices": manager.get_devices_list(),
                "activeSpeakerId": manager.active_speaker_id,
                "activeSpeakerName": manager.current_state.get("activeSpeakerName", "Aucun"),
                "syncMode": manager.sync_mode
            }
        })
    except Exception:
        manager.disconnect(websocket)

@app.get("/")
async def get_index():
    response = FileResponse(os.path.join(DIR_PATH, "index.html"))
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response

@app.get("/manifest.json")
async def get_manifest():
    return FileResponse(os.path.join(DIR_PATH, "manifest.json"), media_type="application/manifest+json")

@app.get("/sw.js")
async def get_sw():
    response = FileResponse(os.path.join(DIR_PATH, "sw.js"), media_type="application/javascript")
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response

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
    port = int(os.environ.get("PORT", 8000))
    print(f"SpotiWave running on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
