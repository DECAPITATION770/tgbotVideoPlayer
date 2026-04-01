from __future__ import annotations

import asyncio
import json
import logging
import os
import pickle
import secrets
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse, RedirectResponse
from telethon import TelegramClient, events
from telethon.tl.types import Document

load_dotenv()

_MAIN_DIR = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("player")

API_ID          = int(os.environ["API_ID"])
API_HASH        = os.environ["API_HASH"]
BOT_TOKEN       = os.environ["BOT_TOKEN"]
PUBLIC          = os.environ["PUBLIC_BASE_URL"].rstrip("/")
PORT            = int(os.environ.get("PORT", "8000"))
ALLOWED_USER_ID = os.environ.get("ALLOWED_USER_ID", "").strip() or None
_sqlite_env     = os.environ.get("SQLITE_PATH", "").strip()
SQLITE_PATH     = os.path.abspath(
    _sqlite_env if _sqlite_env else os.path.join(_MAIN_DIR, "tgvideoplayer.db")
)

_session_dir_env = os.environ.get("SESSION_DIR", "").strip()
SESSION_DIR = os.path.abspath(_session_dir_env if _session_dir_env else _MAIN_DIR)
os.makedirs(SESSION_DIR, exist_ok=True)

CHUNK_SIZE = 512 * 1024       # 512 KB
THUMB_DIR  = "/tmp/tg_thumbs"
os.makedirs(THUMB_DIR, exist_ok=True)

_db_conn: sqlite3.Connection | None = None
_db_lock = threading.Lock()


def _sqlite_connect(path: str) -> sqlite3.Connection:
    d = os.path.dirname(os.path.abspath(path))
    if d:
        os.makedirs(d, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _sqlite_init_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS media_tokens (
            token TEXT PRIMARY KEY NOT NULL,
            payload BLOB NOT NULL,
            created_at REAL NOT NULL
        )
        """
    )
    conn.commit()


def _sqlite_save_ref(conn: sqlite3.Connection, token: str, ref: Any) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO media_tokens (token, payload, created_at) VALUES (?, ?, ?)",
        (token, pickle.dumps(ref, protocol=pickle.HIGHEST_PROTOCOL), time.time()),
    )
    conn.commit()


def _sqlite_load_ref(conn: sqlite3.Connection, token: str) -> Any | None:
    row = conn.execute(
        "SELECT payload FROM media_tokens WHERE token = ?", (token,)
    ).fetchone()
    if not row:
        return None
    return pickle.loads(row[0])


# ──────────────────────────────────────────────
# Хранилище
# ──────────────────────────────────────────────

@dataclass
class MediaRef:
    kind:     str   # "video" | "photo" | "animation" | "video_note"
    media:    Any   # объект Telethon (Document / Photo / etc.)
    thumb:    Any = None
    width:    int = 1280
    height:   int = 720
    duration: int | None = None
    created_at: float = field(default_factory=time.time)

    @property
    def has_thumb(self) -> bool:
        return self.thumb is not None


def _db() -> sqlite3.Connection:
    assert _db_conn is not None
    return _db_conn


def _new_token(ref: MediaRef) -> str:
    t = secrets.token_urlsafe(16)
    with _db_lock:
        _sqlite_save_ref(_db(), t, ref)
    return t


def _get(token: str) -> MediaRef | None:
    with _db_lock:
        r = _sqlite_load_ref(_db(), token)
    return r if isinstance(r, MediaRef) else None




def _tgp_player_html(media_rel: str, token: str) -> str:
    """
    Разметка как в TgPlayer (превью + play). Добавлены og:url / og:image на /t/{token},
    чтобы у ссылки в Telegram было превью-картинка (thumb из Telegram).
    """
    src_json = json.dumps(media_rel)
    page_url = f"{PUBLIC}/p/{token}"
    og_image = f"{PUBLIC}/t/{token}"
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>TgPlayer</title>
  <meta property="og:type" content="website" />
  <meta property="og:url" content="{page_url}" />
  <meta property="og:title" content="" />
  <meta property="og:description" content="" />
  <meta property="og:image" content="{og_image}" />
  <meta property="og:image:secure_url" content="{og_image}" />
  <meta name="twitter:card" content="summary_large_image" />
  <meta name="twitter:image" content="{og_image}" />
  <style>
    html, body {{
      margin: 0;
      padding: 0;
      width: 100%;
      height: 100%;
      background: black;
      overflow: hidden;
    }}
    body {{
      margin: 0;
      padding: 0;
    }}
    .root {{
      position: relative;
      width: 100%;
      height: 100%;
      display: flex;
      align-items: center;
      justify-content: center;
      background: black;
      cursor: pointer;
    }}
    .preview-media {{
      max-width: 100%;
      max-height: 100%;
      object-fit: cover;
      background: black;
      display: block;
    }}
    .play-overlay {{
      position: absolute;
      width: 96px;
      height: 96px;
      border-radius: 50%;
      background: rgba(0, 0, 0, 0.6);
      display: flex;
      align-items: center;
      justify-content: center;
    }}
    .play-overlay::before {{
      content: "";
      display: block;
      width: 0;
      height: 0;
      border-top: 18px solid transparent;
      border-bottom: 18px solid transparent;
      border-left: 28px solid white;
      margin-left: 6px;
    }}
  </style>
</head>
<body>
  <div class="root" id="root">
    <video
      id="preview"
      class="preview-media"
      src="{media_rel}"
      playsinline
      preload="metadata"
    ></video>
    <div class="play-overlay" id="play"></div>
  </div>
  <script>
    (function() {{
      const root = document.getElementById('root');
      const play = document.getElementById('play');
      const src = {src_json};

      if (!root || !play) return;

      function createPlayer() {{
        const el = document.createElement("video");
        el.src = src;
        el.controls = true;
        el.autoplay = true;
        el.playsInline = true;
        el.id = "player";
        el.className = "preview-media";
        return el;
      }}

      function goFullscreen(target) {{
        const docEl = document.documentElement;
        if (docEl.requestFullscreen) {{
          docEl.requestFullscreen().catch(() => {{}});
        }}
        if (screen.orientation && screen.orientation.lock) {{
          screen.orientation.lock('landscape').catch(() => {{}});
        }}
        if (target && target.play) {{
          target.play().catch(() => {{}});
        }}
      }}

      function startPlayback() {{
        const current = document.getElementById("player");
        if (!current) {{
          const player = createPlayer();
          root.innerHTML = "";
          root.appendChild(player);
          goFullscreen(player);
        }} else {{
          goFullscreen(current);
        }}
      }}

      root.addEventListener("click", startPlayback, {{ once: true }});
      play.addEventListener("click", function (ev) {{
        ev.stopPropagation();
        startPlayback();
      }}, {{ once: true }});

      window.addEventListener("load", function () {{
        setTimeout(() => {{
          // только визуальный превью, без звука
        }}, 0);
      }});
    }})();
  </script>
</body>
</html>"""


# ──────────────────────────────────────────────
# FastAPI
# ──────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _db_conn
    _db_conn = _sqlite_connect(SQLITE_PATH)
    _sqlite_init_schema(_db_conn)
    log.info("SQLite: %s", SQLITE_PATH)
    asyncio.create_task(_run_bot())
    log.info("Запуск сервера...")
    yield
    if _tg:
        await _tg.disconnect()
    if _db_conn is not None:
        _db_conn.close()
        _db_conn = None

app = FastAPI(lifespan=lifespan)
_tg: TelegramClient | None = None


def _client() -> TelegramClient:
    assert _tg is not None
    return _tg


@app.get("/p/{token}", response_class=HTMLResponse)
def page(token: str):
    ref = _get(token)
    if not ref:
        raise HTTPException(404)
    if ref.kind == "photo":
        return RedirectResponse(url=f"{PUBLIC}/i/{token}", status_code=302)
    media_rel = f"/s/{token}"
    return HTMLResponse(_tgp_player_html(media_rel, token))


def _parse_range(header: str) -> tuple[int, int | None]:
    if not header.startswith("bytes="):
        return 0, None
    parts = header[6:].split("-")
    try:
        start = int(parts[0]) if parts[0] else 0
        end   = int(parts[1]) if len(parts) > 1 and parts[1] else None
        limit = (end - start + 1) if end is not None else None
        return start, limit
    except ValueError:
        return 0, None


async def _stream(ref: MediaRef, request: Request) -> StreamingResponse:
    rng    = request.headers.get("range", "")
    offset, limit = _parse_range(rng)
    aligned = (offset // 4096) * 4096
    skip    = offset - aligned
    client  = _client()

    file_size: int | None = None
    mime = "video/mp4"
    if isinstance(ref.media, Document):
        file_size = ref.media.size
        mime = ref.media.mime_type or mime

    async def body():
        skipped = 0
        sent    = 0
        tl = limit + skip if limit is not None else None
        async for chunk in client.iter_download(
            ref.media,
            offset=aligned,
            request_size=CHUNK_SIZE,
            limit=tl,
        ):
            if skipped < skip:
                to_skip = min(skip - skipped, len(chunk))
                chunk   = chunk[to_skip:]
                skipped += to_skip
                if not chunk:
                    continue
            if limit is not None:
                chunk = chunk[: limit - sent]
            sent += len(chunk)
            yield chunk
            if limit is not None and sent >= limit:
                break

    status  = 206 if rng else 200
    headers: dict[str, str] = {"accept-ranges": "bytes", "content-type": mime}
    if file_size is not None:
        end_byte = (offset + limit - 1) if limit else (file_size - 1)
        if rng:
            headers["content-range"]  = f"bytes {offset}-{end_byte}/{file_size}"
            headers["content-length"] = str(end_byte - offset + 1)
        else:
            headers["content-length"] = str(file_size)

    return StreamingResponse(body(), status_code=status, headers=headers)


@app.api_route("/s/{token}", methods=["GET", "HEAD"])
async def stream_video(token: str, request: Request):
    ref = _get(token)
    if not ref or ref.kind == "photo":
        raise HTTPException(404 if not ref else 400)
    if request.method == "HEAD":
        mime = "video/mp4"
        if isinstance(ref.media, Document):
            mime = ref.media.mime_type or mime
        hdrs: dict[str, str] = {"accept-ranges": "bytes", "content-type": mime}
        if isinstance(ref.media, Document):
            hdrs["content-length"] = str(ref.media.size)
        return Response(status_code=200, headers=hdrs)
    return await _stream(ref, request)


@app.api_route("/i/{token}", methods=["GET", "HEAD"])
async def stream_image(token: str, request: Request):
    ref = _get(token)
    if not ref or ref.kind != "photo":
        raise HTTPException(404 if not ref else 400)
    if request.method == "HEAD":
        return Response(status_code=200, headers={"content-type": "image/jpeg"})
    client = _client()

    async def body():
        async for chunk in client.iter_download(ref.media, request_size=CHUNK_SIZE):
            yield chunk

    return StreamingResponse(body(), media_type="image/jpeg")

CACHE_HEADERS = {"cache-control": "public, max-age=31536000, immutable"}


@app.get("/t/{token}")
async def stream_thumb(token: str):
    path = f"{THUMB_DIR}/{token}.jpg"
    if os.path.exists(path):
        return FileResponse(path, media_type="image/jpeg", headers=CACHE_HEADERS)
    ref = _get(token)
    if not ref or not ref.has_thumb:
        raise HTTPException(404)
    buf = await _client().download_media(ref.media, file=bytes, thumb=ref.thumb)
    if not buf:
        raise HTTPException(404)
    return Response(content=buf, media_type="image/jpeg", headers=CACHE_HEADERS)


# ──────────────────────────────────────────────
# Telethon бот
# ──────────────────────────────────────────────
def _allowed(sender_id: int) -> bool:
    return not ALLOWED_USER_ID or str(sender_id) == ALLOWED_USER_ID


def _success_message(kind: str, url: str) -> str:
    """Как во вставленном боте (Bot API): жирный заголовок + ссылка «Открыть» (HTML)."""
    titles = {
        "photo": "Фото готово",
        "video": "Видео готово",
        "video_note": "Видео готово",
        "animation": "GIF готов",
    }
    title = titles.get(kind, "Готово")
    url_escaped = url.replace("&", "&amp;")
    return f'<b>{title}</b>\n\n<a href="{url_escaped}">Открыть</a>\n<code>{url_escaped}</code>'


def _pick_thumb(sizes: list) -> Any:
    photo_sizes = [s for s in sizes if type(s).__name__ == "PhotoSize"]
    if photo_sizes:
        return max(photo_sizes, key=lambda s: getattr(s, "w", 0) * getattr(s, "h", 0))
    for s in reversed(sizes):
        t = type(s).__name__
        if t in ("PhotoCachedSize", "PhotoStrippedSize") and getattr(s, "bytes", None):
            return s
    return None


def _ref_from_msg(msg: Any) -> MediaRef | None:
    if msg.photo:
        ph    = msg.photo
        thumb = _pick_thumb(ph.sizes)
        best = max(ph.sizes, key=lambda s: getattr(s, "w", 0) * getattr(s, "h", 0), default=None)
        w = getattr(best, "w", 1280) if best else 1280
        h = getattr(best, "h", 720)  if best else 720
        return MediaRef(kind="photo", media=ph, thumb=thumb, width=w, height=h)

    if msg.document:
        doc   = msg.document
        mime  = doc.mime_type or ""
        attrs = {type(a).__name__: a for a in (doc.attributes or [])}
        duration = None

        if "DocumentAttributeVideo" in attrs:
            vattr    = attrs["DocumentAttributeVideo"]
            kind     = "video_note" if getattr(vattr, "round_message", False) else "video"
            width    = getattr(vattr, "w", 1280)
            height   = getattr(vattr, "h", 720)
            duration = int(getattr(vattr, "duration", 0)) or None
        elif "DocumentAttributeAnimated" in attrs or mime == "image/gif":
            kind   = "animation"
            width  = 480
            height = 480
        elif mime.startswith("video/"):
            kind   = "video"
            width  = 1280
            height = 720
        else:
            return None

        thumb = _pick_thumb(doc.thumbs) if doc.thumbs else None
        return MediaRef(kind=kind, media=doc, thumb=thumb, width=width, height=height, duration=duration)

    return None


async def _run_bot():
    global _tg
    client = TelegramClient(os.path.join(SESSION_DIR, "bot_session"), API_ID, API_HASH)
    await client.start(bot_token=BOT_TOKEN)
    _tg = client
    log.info("Telethon бот запущен. URL: %s", PUBLIC)

    @client.on(events.NewMessage(pattern="/start"))
    async def on_start(event):
        if not _allowed(event.sender_id):
            return
        await event.reply("Пришлите видео или фото — получите ссылку на плеер.")

    @client.on(events.NewMessage(func=lambda e: e.media is not None))
    async def on_media(event):
        if not _allowed(event.sender_id):
            return
        ref = _ref_from_msg(event.message)
        if not ref:
            await event.reply("Нужно фото или видео.")
            return

        t = _new_token(ref)
        link = f"{PUBLIC}/p/{t}"

        await event.reply(
            _success_message(ref.kind, link),
            parse_mode="html",
            link_preview=True,
        )

    @client.on(events.NewMessage(func=lambda e: e.media is None and not e.text.startswith("/")))
    async def on_other(event):
        if not _allowed(event.sender_id):
            return
        await event.reply("Нужно фото или видео.")

    await client.run_until_disconnected()


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info")