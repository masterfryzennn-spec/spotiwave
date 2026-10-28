import os
import sys
import time
import socket
import re
import asyncio
import urllib.request
import urllib.parse
import json
from typing import Dict, List, Optional
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import RedirectResponse, JSONResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from ytmusicapi import YTMusic
import yt_dlp
import aiohttp

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
artist_cache: Dict[str, Dict] = {}
album_cache: Dict[str, Dict] = {}
lyrics_cache: Dict[str, Dict] = {}

class Room:
    def __init__(self, room_id: str):
        self.room_id = room_id
        self.connections: Dict[WebSocket, dict] = {}
        self.active_speaker_id: Optional[str] = None
        self.sync_mode: str = "connect"  # "connect" or "jam"
        self.current_state = {
            "track": None,
            "isPlaying": False,
            "position": 0,
            "timestamp": int(time.time() * 1000),
            "activeSpeakerId": None,
            "activeSpeakerName": "Aucun",
            "syncMode": "connect",
            "roomId": room_id
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


class ConnectionManager:
    def __init__(self):
        self.rooms: Dict[str, Room] = {}
        self.ws_to_room: Dict[WebSocket, Room] = {}

    def get_room(self, room_id: str) -> Room:
        if not room_id:
            room_id = "default"
        if room_id not in self.rooms:
            self.rooms[room_id] = Room(room_id)
        return self.rooms[room_id]

    async def connect(self, websocket: WebSocket, room_id: str):
        await websocket.accept()
        room = self.get_room(room_id)
        room.connections[websocket] = {"id": "", "name": "Appareil", "type": "pc"}
        self.ws_to_room[websocket] = room
        await websocket.send_json({
            "type": "INIT_STATE",
            "data": {
                **room.current_state,
                "devices": room.get_devices_list(),
                "roomId": room.room_id
            }
        })

    def disconnect(self, websocket: WebSocket):
        room = self.ws_to_room.pop(websocket, None)
        if room:
            info = room.connections.pop(websocket, None)
            if info and info.get("id") == room.active_speaker_id:
                remaining = [d for d in room.connections.values() if d.get("id")]
                if remaining:
                    room.active_speaker_id = remaining[0]["id"]
                    room.current_state["activeSpeakerId"] = room.active_speaker_id
                    room.current_state["activeSpeakerName"] = remaining[0]["name"]
                else:
                    room.active_speaker_id = None
                    room.current_state["activeSpeakerId"] = None
                    room.current_state["activeSpeakerName"] = "Aucun"
            if not room.connections:
                self.rooms.pop(room.room_id, None)

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

@app.get("/api/ping")
async def ping():
    return JSONResponse(content={"status": "ok", "time": time.time()})

@app.on_event("startup")
async def startup_prewarm():
    # 1. Pre-fetch live feed on server start so the very first user gets 0ms response
    try:
        asyncio.create_task(get_live_feed())
    except Exception:
        pass

    # 2. Render Keep-Alive Heartbeat: pings external URL every 10 min to prevent 15-min idle spin-down
    render_url = os.environ.get("RENDER_EXTERNAL_URL") or "https://spotiwave.onrender.com"
    if render_url:
        async def keep_alive_loop():
            # Wait 5 minutes before beginning routine pings
            await asyncio.sleep(300)
            while True:
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                            f"{render_url.rstrip('/')}/api/ping",
                            timeout=aiohttp.ClientTimeout(total=10)
                        ) as resp:
                            if resp.status == 200:
                                pass
                except Exception:
                    pass
                # Render shuts down after 15 minutes of inactivity; pinging every 10 minutes keeps it permanently active
                await asyncio.sleep(600)

        asyncio.create_task(keep_alive_loop())

@app.get("/api/feed")
async def get_live_feed():
    now = time.time()
    if "home_feed" in feed_cache and (now - feed_cache["home_feed"]["time"]) < 21600:
        return JSONResponse(content={"success": True, "data": feed_cache["home_feed"]["data"]})

    try:
        def fetch_charts():
            try:
                charts_fr = ytm.get_charts(country='FR')
                videos_fr = charts_fr.get('videos', [])
                trending_tracks = []
                top50_tracks = []
                if len(videos_fr) > 0:
                    p_trend = ytm.get_playlist(videos_fr[0]['playlistId'])
                    trending_tracks = [format_track(t) for t in p_trend.get('tracks', [])[:15] if format_track(t)]
                if len(videos_fr) > 1:
                    p_top = ytm.get_playlist(videos_fr[1]['playlistId'])
                    top50_tracks = [format_track(t) for t in p_top.get('tracks', [])[:20] if format_track(t)]
                return trending_tracks, top50_tracks
            except Exception:
                return [], []

        def fetch_rap():
            try:
                res = ytm.search("Top Rap Français", filter="songs")[:15]
                return [format_track(t) for t in res if format_track(t)]
            except Exception:
                return []

        def fetch_electro():
            try:
                res = ytm.search("Top Hits Pop Electro", filter="songs")[:15]
                return [format_track(t) for t in res if format_track(t)]
            except Exception:
                return []

        (charts_res, rap_tracks, electro_tracks) = await asyncio.gather(
            asyncio.to_thread(fetch_charts),
            asyncio.to_thread(fetch_rap),
            asyncio.to_thread(fetch_electro)
        )

        feed_data = {
            "trending": charts_res[0],
            "top50": charts_res[1],
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

        track_list = entity.get('trackList', [])
        
        # Extract individual track IDs from Spotify URIs
        track_ids = []
        for t in track_list:
            uri = t.get('uri', '')
            if 'track:' in uri:
                track_ids.append(uri.split('track:')[-1])
            else:
                track_ids.append(None)

        def fetch_single_thumb(tid):
            if not tid:
                return tid, None
            try:
                target_url = f"https://open.spotify.com/track/{tid}"
                u = f"https://open.spotify.com/oembed?url={urllib.parse.quote(target_url, safe='')}"
                r = urllib.request.Request(u, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
                res = urllib.request.urlopen(r, timeout=3.0)
                d = json.loads(res.read())
                return tid, d.get('thumbnail_url')
            except Exception:
                return tid, None

        thumb_map = {}
        valid_tids = [tid for tid in track_ids if tid]
        if valid_tids:
            try:
                from concurrent.futures import ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=min(20, len(valid_tids))) as executor:
                    for tid, thumb in executor.map(fetch_single_thumb, valid_tids):
                        if thumb:
                            thumb_map[tid] = thumb
            except Exception:
                pass

        tracks = []
        for i, t in enumerate(track_list):
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

            tid = track_ids[i] if i < len(track_ids) else None
            # Use individual album cover if available; otherwise fallback to playlist cover
            t_cover = thumb_map.get(tid) or cover

            tracks.append({
                'title': t.get('title', 'Titre'),
                'artist': t.get('subtitle', 'Artiste'),
                'duration': formatted_dur,
                'cover': t_cover
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

def clean_music_query(query: str) -> str:
    q = re.sub(r'\(feat\..*?\)', '', query, flags=re.I)
    q = re.sub(r'\[feat\..*?\]', '', q, flags=re.I)
    q = re.sub(r'\(with.*?\)', '', q, flags=re.I)
    q = re.sub(r'[-–]\s*Remaster.*', '', q, flags=re.I)
    q = re.sub(r'\(Remaster.*?\)', '', q, flags=re.I)
    q = re.sub(r'\[Remaster.*?\]', '', q, flags=re.I)
    q = re.sub(r'\(Bonus Track.*?\)', '', q, flags=re.I)
    q = re.sub(r'\(Radio Edit.*?\)', '', q, flags=re.I)
    q = re.sub(r'\(Original Mix.*?\)', '', q, flags=re.I)
    q = re.sub(r'\s+', ' ', q)
    return q.strip()

@app.get("/api/resolve")
async def resolve_track_query(q: str = Query(..., min_length=1), exclude: Optional[str] = None):
    try:
        clean_q = clean_music_query(q)
        excluded = [x.strip() for x in exclude.split(",")] if exclude else []

        # 1. Search cleaned query in songs
        results = ytm.search(clean_q, filter="songs") or []
        
        # 2. Also append general audio/topic results for non-blocked alternatives
        try:
            audio_results = ytm.search(f"{clean_q} audio") or []
            existing_ids = set([r.get("videoId") for r in results if r.get("videoId")])
            for a in audio_results:
                vid = a.get("videoId")
                if vid and vid not in existing_ids:
                    results.append(a)
                    existing_ids.add(vid)
        except Exception:
            pass

        # 3. Third attempt: search raw query in songs if still empty
        if not results and clean_q != q:
            results = ytm.search(q, filter="songs") or []

        valid_items = []
        for item in results:
            vid = item.get("videoId")
            if vid and vid not in excluded:
                t = format_track(item)
                if t:
                    valid_items.append(t)

        if valid_items:
            best_track = valid_items[0]
            # Include alternative candidate IDs for automatic error-150 failover
            best_track["alternativeIds"] = [x["id"] for x in valid_items[1:6] if x.get("id")]
            return JSONResponse(content={"success": True, "track": best_track})

        return JSONResponse(status_code=404, content={"success": False, "error": "Titre introuvable"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})

@app.get("/api/search")
async def search_tracks(q: str = Query(..., min_length=1)):
    try:
        clean_q = clean_music_query(q)
        results = ytm.search(clean_q, filter="songs") or []
        if not results and clean_q != q:
            results = ytm.search(q, filter="songs") or []

        if len(results) < 5:
            more = ytm.search(clean_q) or []
            existing_ids = set([r.get("videoId") for r in results if r.get("videoId")])
            for m in more:
                if m.get("videoId") and m.get("videoId") not in existing_ids:
                    results.append(m)
                    existing_ids.add(m.get("videoId"))

        tracks = []
        for item in results[:25]:
            t = format_track(item)
            if t:
                tracks.append(t)

        artists = []
        try:
            art_raw = ytm.search(clean_q, filter="artists")[:4] or []
            for a in art_raw:
                th = a.get("thumbnails", [])
                c = th[-1].get("url", "") if th else "https://images.unsplash.com/photo-1511671782779-c97d3d27a1d4?w=300&h=300&fit=crop"
                b_id = a.get("browseId")
                if b_id:
                    artists.append({
                        "name": a.get("artist") or a.get("name"),
                        "browseId": b_id,
                        "subscribers": a.get("subscribers", ""),
                        "cover": c
                    })
        except Exception:
            pass

        return JSONResponse(content={"success": True, "count": len(tracks), "tracks": tracks, "artists": artists})
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})

@app.get("/api/artist")
async def get_artist_profile(q: Optional[str] = None, browseId: Optional[str] = None):
    now = time.time()
    cache_key = browseId or (q.strip().lower() if q else "")
    if cache_key and cache_key in artist_cache and (now - artist_cache[cache_key]["time"]) < 3600:
        return JSONResponse(content={"success": True, "artist": artist_cache[cache_key]["data"]})

    try:
        target_browse_id = browseId
        if not target_browse_id and q:
            art_search = ytm.search(q.strip(), filter="artists") or []
            if art_search and art_search[0].get("browseId"):
                target_browse_id = art_search[0].get("browseId")

        if not target_browse_id:
            return JSONResponse(status_code=404, content={"success": False, "error": "Artiste introuvable"})

        data = ytm.get_artist(target_browse_id)
        name = data.get("name") or q or "Artiste"
        subscribers = data.get("subscribers") or ""
        thumbs = data.get("thumbnails", [])
        cover = thumbs[-1].get("url", "") if thumbs else "https://images.unsplash.com/photo-1511671782779-c97d3d27a1d4?w=800&h=800&fit=crop"

        tracks = []
        songs_data = data.get("songs", {})
        pl_id = songs_data.get("browseId")
        if pl_id:
            try:
                pl = ytm.get_playlist(pl_id, limit=35)
                for t in pl.get("tracks", []):
                    vid = t.get("videoId")
                    if vid:
                        art = ", ".join([a.get("name", "") for a in t.get("artists", [])]) if t.get("artists") else name
                        th = t.get("thumbnails", [])
                        c = th[-1].get("url", "") if th else cover
                        tracks.append({
                            "id": vid,
                            "title": t.get("title", "Titre"),
                            "artist": art,
                            "duration": t.get("duration", "3:30"),
                            "cover": c
                        })
            except Exception:
                pass

        if len(tracks) < 5:
            fallback_songs = ytm.search(name, filter="songs")[:25] or []
            existing_ids = set([x["id"] for x in tracks])
            for t in fallback_songs:
                vid = t.get("videoId")
                if vid and vid not in existing_ids:
                    art = ", ".join([a.get("name", "") for a in t.get("artists", [])]) if t.get("artists") else name
                    th = t.get("thumbnails", [])
                    c = th[-1].get("url", "") if th else cover
                    tracks.append({
                        "id": vid,
                        "title": t.get("title", "Titre"),
                        "artist": art,
                        "duration": t.get("duration", "3:30"),
                        "cover": c
                    })
                    existing_ids.add(vid)

        albums = []
        for alb in data.get("albums", {}).get("results", [])[:12]:
            th = alb.get("thumbnails", [])
            c = th[-1].get("url", "") if th else cover
            albums.append({
                "title": alb.get("title"),
                "year": alb.get("year", ""),
                "cover": c,
                "browseId": alb.get("browseId"),
                "type": "Album"
            })

        singles = []
        for s in data.get("singles", {}).get("results", [])[:12]:
            th = s.get("thumbnails", [])
            c = th[-1].get("url", "") if th else cover
            singles.append({
                "title": s.get("title"),
                "year": s.get("year", ""),
                "cover": c,
                "browseId": s.get("browseId"),
                "type": "Single"
            })

        artist_obj = {
            "name": name,
            "browseId": target_browse_id,
            "subscribers": subscribers,
            "cover": cover,
            "tracks": tracks,
            "albums": albums,
            "singles": singles
        }

        if cache_key:
            artist_cache[cache_key] = {"data": artist_obj, "time": now}
        if target_browse_id:
            artist_cache[target_browse_id] = {"data": artist_obj, "time": now}

        return JSONResponse(content={"success": True, "artist": artist_obj})
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})

@app.get("/api/album")
async def get_album_details(browseId: str = Query(...)):
    now = time.time()
    if browseId in album_cache and (now - album_cache[browseId]["time"]) < 3600:
        return JSONResponse(content={"success": True, "album": album_cache[browseId]["data"]})

    try:
        data = ytm.get_album(browseId)
        title = data.get("title", "Album")
        artists = ", ".join([a.get("name", "") for a in data.get("artists", [])]) if data.get("artists") else "Artiste"
        thumbs = data.get("thumbnails", [])
        cover = thumbs[-1].get("url", "") if thumbs else "https://images.unsplash.com/photo-1614613535308-eb5fbd3d2c17?w=500&h=500&fit=crop"
        year = data.get("year", "")

        tracks = []
        for t in data.get("tracks", []):
            vid = t.get("videoId")
            if vid:
                art = ", ".join([a.get("name", "") for a in t.get("artists", [])]) if t.get("artists") else artists
                tracks.append({
                    "id": vid,
                    "title": t.get("title", "Titre"),
                    "artist": art,
                    "duration": t.get("duration", "3:30"),
                    "cover": cover
                })

        album_obj = {
            "title": title,
            "artist": artists,
            "year": year,
            "cover": cover,
            "browseId": browseId,
            "tracks": tracks
        }
        album_cache[browseId] = {"data": album_obj, "time": now}
        return JSONResponse(content={"success": True, "album": album_obj})
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})

@app.get("/api/lyrics")
async def get_track_lyrics(title: str = Query(...), artist: str = Query(""), videoId: Optional[str] = None):
    now = time.time()
    cache_key = f"{title.strip().lower()}_{artist.strip().lower()}"
    if cache_key in lyrics_cache and (now - lyrics_cache[cache_key]["time"]) < 86400:
        return JSONResponse(content=lyrics_cache[cache_key]["data"])

    clean_t = re.sub(r'\(feat\..*?\)', '', title, flags=re.I)
    clean_t = re.sub(r'\[feat\..*?\]', '', clean_t, flags=re.I)
    clean_t = re.sub(r'\(with.*?\)', '', clean_t, flags=re.I)
    clean_t = re.sub(r'[-–]\s*Remaster.*', '', clean_t, flags=re.I)
    clean_t = re.sub(r'\(Remaster.*?\)', '', clean_t, flags=re.I)
    clean_t = re.sub(r'\s+', ' ', clean_t).strip()
    clean_a = artist.split(',')[0].strip()

    # 1. First attempt: LRCLIB for line-by-line synchronized lyrics
    try:
        q_params = urllib.parse.urlencode({'track_name': clean_t, 'artist_name': clean_a})
        lrclib_url = f'https://lrclib.net/api/get?{q_params}'
        req = urllib.request.Request(lrclib_url, headers={'User-Agent': 'SpotiWave (https://github.com)'})
        with urllib.request.urlopen(req, timeout=3.5) as resp:
            data = json.loads(resp.read().decode())
            if data.get('syncedLyrics'):
                lines = []
                for line in data['syncedLyrics'].splitlines():
                    m = re.match(r'\[(\d+):(\d+(?:\.\d+)?)\](.*)', line)
                    if m:
                        mins = int(m.group(1))
                        secs = float(m.group(2))
                        lines.append({'time': round(mins * 60 + secs, 2), 'text': m.group(3).strip()})
                if lines:
                    payload = {'success': True, 'synced': True, 'lines': lines}
                    lyrics_cache[cache_key] = {'data': payload, 'time': now}
                    return JSONResponse(content=payload)
            elif data.get('plainLyrics'):
                lines = [{'time': -1, 'text': l.strip()} for l in data['plainLyrics'].splitlines() if l.strip()]
                if lines:
                    payload = {'success': True, 'synced': False, 'lines': lines}
                    lyrics_cache[cache_key] = {'data': payload, 'time': now}
                    return JSONResponse(content=payload)
    except Exception:
        pass

    # 2. Fallback: YouTube Music lyrics
    if videoId:
        try:
            watch = ytm.get_watch_playlist(videoId=videoId)
            lyr_id = watch.get('lyrics')
            if lyr_id:
                lyr_data = ytm.get_lyrics(lyr_id)
                raw = lyr_data.get('lyrics', '')
                lines = [{'time': -1, 'text': l.strip()} for l in raw.splitlines() if l.strip()]
                if lines:
                    payload = {'success': True, 'synced': False, 'lines': lines}
                    lyrics_cache[cache_key] = {'data': payload, 'time': now}
                    return JSONResponse(content=payload)
        except Exception:
            pass

    return JSONResponse(content={'success': False, 'error': 'Paroles non disponibles pour ce titre'})

def extract_audio_info(video_id: str) -> Optional[Dict]:
    # 1. yt-dlp extracting format 140 (AAC 128kbps m4a) or fallback to 18 (mp4) / best audio
    try:
        ydl_opts = {
            "format": "140/bestaudio[ext=m4a]/bestaudio/18/best",
            "quiet": True,
            "no_warnings": True,
            "socket_timeout": 6,
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
            url = info.get("url")
            if url:
                return {
                    "url": url,
                    "headers": dict(info.get("http_headers", {})),
                    "filesize": info.get("filesize") or info.get("filesize_approx"),
                    "ext": info.get("ext", "m4a"),
                    "cached_at": time.time()
                }
    except Exception as e:
        print(f"yt-dlp extract error for {video_id}: {e}")

    # 2. Fallback: Invidious API
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
                return {
                    "url": audios[0]["url"],
                    "headers": {"User-Agent": "Mozilla/5.0"},
                    "filesize": None,
                    "ext": "m4a",
                    "cached_at": time.time()
                }
        except Exception:
            pass

    return None

@app.get("/api/stream/{video_id}")
async def stream_audio_track(video_id: str, request: Request, redirect: bool = False, info: bool = False):
    now = time.time()
    stream_data = stream_cache.get(video_id)
    if not stream_data or (now - stream_data.get("cached_at", 0)) > 10800:
        extracted = await asyncio.to_thread(extract_audio_info, video_id)
        if extracted:
            stream_cache[video_id] = extracted
            stream_data = extracted
        else:
            return JSONResponse(status_code=404, content={"error": "Flux audio introuvable"})

    if info:
        return JSONResponse(content={"url": stream_data["url"], "filesize": stream_data.get("filesize")})

    if redirect:
        return RedirectResponse(url=stream_data["url"], status_code=307)

    # Proxy audio stream chunks with full byte-range support for iOS Safari & Android
    stream_url = stream_data["url"]
    upstream_headers = dict(stream_data.get("headers", {}))
    
    # Forward client Range header if requested
    client_range = request.headers.get("range")
    if client_range:
        upstream_headers["Range"] = client_range

    session = aiohttp.ClientSession(headers=upstream_headers)
    try:
        upstream_resp = await session.get(stream_url)
        
        # If token/URL expired (403/410), refresh cache once
        if upstream_resp.status in (403, 410):
            stream_cache.pop(video_id, None)
            stream_data = await asyncio.to_thread(extract_audio_info, video_id)
            if stream_data:
                stream_cache[video_id] = stream_data
                await upstream_resp.release()
                upstream_headers = dict(stream_data.get("headers", {}))
                if client_range:
                    upstream_headers["Range"] = client_range
                upstream_resp = await session.get(stream_data["url"])
            else:
                await upstream_resp.release()
                await session.close()
                return JSONResponse(status_code=404, content={"error": "Flux audio expiré"})

        status_code = upstream_resp.status
        content_type = upstream_resp.headers.get("Content-Type", "audio/mp4")
        if "audio" not in content_type and "video" not in content_type:
            content_type = "audio/mp4"

        response_headers = {
            "Accept-Ranges": "bytes",
            "Content-Type": content_type,
            "Cache-Control": "public, max-age=3600"
        }
        if "Content-Range" in upstream_resp.headers:
            response_headers["Content-Range"] = upstream_resp.headers["Content-Range"]
        if "Content-Length" in upstream_resp.headers:
            response_headers["Content-Length"] = upstream_resp.headers["Content-Length"]

        async def stream_generator():
            try:
                async for chunk in upstream_resp.content.iter_chunked(64 * 1024):
                    yield chunk
            finally:
                upstream_resp.close()
                await session.close()

        return StreamingResponse(stream_generator(), status_code=status_code, headers=response_headers)
    except Exception as e:
        await session.close()
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, room: str = Query("default")):
    await manager.connect(websocket, room)
    current_room = manager.ws_to_room.get(websocket)
    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")
            payload = data.get("data", {})
            if not current_room:
                continue

            if msg_type == "REGISTER_DEVICE":
                dev_id = payload.get("id")
                dev_name = payload.get("name", "Appareil")
                dev_type = payload.get("type", "pc")
                current_room.connections[websocket] = {
                    "id": dev_id,
                    "name": dev_name,
                    "type": dev_type
                }
                if not current_room.active_speaker_id:
                    current_room.active_speaker_id = dev_id
                    current_room.current_state["activeSpeakerId"] = dev_id
                    current_room.current_state["activeSpeakerName"] = dev_name

                await current_room.broadcast_all({
                    "type": "DEVICES_UPDATE",
                    "data": {
                        "devices": current_room.get_devices_list(),
                        "activeSpeakerId": current_room.active_speaker_id,
                        "activeSpeakerName": current_room.current_state.get("activeSpeakerName", "Aucun"),
                        "syncMode": current_room.sync_mode,
                        "roomId": current_room.room_id
                    }
                })

            elif msg_type == "UPDATE_STATE":
                current_room.current_state.update(payload)
                current_room.current_state["timestamp"] = int(time.time() * 1000)
                await current_room.broadcast({
                    "type": "SYNC_STATE",
                    "data": current_room.current_state
                }, sender=websocket)

            elif msg_type == "CONTROL_COMMAND":
                action = payload.get("action")
                if current_room.sync_mode == "jam":
                    await current_room.broadcast({
                        "type": "REMOTE_COMMAND",
                        "data": payload
                    }, sender=websocket)
                else:
                    if current_room.active_speaker_id:
                        sent = await current_room.send_to_device(current_room.active_speaker_id, {
                            "type": "REMOTE_COMMAND",
                            "data": payload
                        })
                        if not sent:
                            await current_room.broadcast({"type": "REMOTE_COMMAND", "data": payload}, sender=websocket)
                    else:
                        await current_room.broadcast({"type": "REMOTE_COMMAND", "data": payload}, sender=websocket)

            elif msg_type == "SET_ACTIVE_SPEAKER":
                new_speaker_id = payload.get("deviceId")
                current_room.active_speaker_id = new_speaker_id
                current_room.current_state["activeSpeakerId"] = new_speaker_id
                for ws, info in current_room.connections.items():
                    if info.get("id") == new_speaker_id:
                        current_room.current_state["activeSpeakerName"] = info.get("name", "Appareil")
                        break
                await current_room.broadcast_all({
                    "type": "DEVICES_UPDATE",
                    "data": {
                        "devices": current_room.get_devices_list(),
                        "activeSpeakerId": current_room.active_speaker_id,
                        "activeSpeakerName": current_room.current_state["activeSpeakerName"],
                        "syncMode": current_room.sync_mode,
                        "roomId": current_room.room_id
                    }
                })

            elif msg_type == "SET_SYNC_MODE":
                new_mode = payload.get("syncMode", "connect")
                current_room.sync_mode = new_mode
                current_room.current_state["syncMode"] = new_mode
                await current_room.broadcast_all({
                    "type": "DEVICES_UPDATE",
                    "data": {
                        "devices": current_room.get_devices_list(),
                        "activeSpeakerId": current_room.active_speaker_id,
                        "activeSpeakerName": current_room.current_state["activeSpeakerName"],
                        "syncMode": current_room.sync_mode,
                        "roomId": current_room.room_id
                    }
                })

    except WebSocketDisconnect:
        manager.disconnect(websocket)
        if current_room and current_room.connections:
            await current_room.broadcast_all({
                "type": "DEVICES_UPDATE",
                "data": {
                    "devices": current_room.get_devices_list(),
                    "activeSpeakerId": current_room.active_speaker_id,
                    "activeSpeakerName": current_room.current_state.get("activeSpeakerName", "Aucun"),
                    "syncMode": current_room.sync_mode,
                    "roomId": current_room.room_id
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
