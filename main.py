"""
Telegram-бот + веб-плеер на Telethon (MTProto).
Стримим файлы любого размера напрямую из Telegram без скачивания на диск.

Переменные окружения (или .env):
  API_ID            — из my.telegram.org
  API_HASH          — из my.telegram.org
  BOT_TOKEN         — токен бота от @BotFather
  PUBLIC_BASE_URL   — HTTPS URL без слэша, например https://xxx.ngrok-free.app
  PORT              — порт (по умолчанию 8000)
  ALLOWED_USER_ID   — опционально: только этот numeric user id

Зависимости:
  pip install telethon fastapi uvicorn python-dotenv pillow blurhash-python
  brew install ffmpeg  # macOS
  apt install ffmpeg   # Linux

Запуск: python main.py
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import secrets
import time
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from telethon import TelegramClient, events, Button
from telethon.tl.types import Document, Photo

load_dotenv()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("player")

API_ID          = int(os.environ["API_ID"])
API_HASH        = os.environ["API_HASH"]
BOT_TOKEN       = os.environ["BOT_TOKEN"]
PUBLIC          = os.environ["PUBLIC_BASE_URL"].rstrip("/")
PORT            = int(os.environ.get("PORT", "8000"))
ALLOWED_USER_ID = os.environ.get("ALLOWED_USER_ID", "").strip() or None

CHUNK_SIZE = 512 * 1024       # 512 KB
THUMB_DIR  = "/tmp/tg_thumbs"
os.makedirs(THUMB_DIR, exist_ok=True)

# ──────────────────────────────────────────────
# Хранилище
# ──────────────────────────────────────────────

@dataclass
class MediaRef:
    kind:           str        # "video" | "photo" | "animation" | "video_note"
    media:          Any        # объект Telethon (Document / Photo / etc.)
    thumb:          Any  = None
    width:          int  = 1280
    height:         int  = 720
    duration:       int | None = None
    dominant_color: str  = "#0a0a0a"
    blurhash_str:   str | None = None
    # Реальные размеры превью (вычисляются после генерации thumb)
    thumb_width:    int  = 1200
    thumb_height:   int  = 630
    created_at:     float = field(default_factory=time.time)

    @property
    def has_thumb(self) -> bool:
        return self.thumb is not None


_store: dict[str, MediaRef] = {}


def _new_token(ref: MediaRef) -> str:
    t = secrets.token_urlsafe(16)
    _store[t] = ref
    return t


def _get(token: str) -> MediaRef | None:
    return _store.get(token)


# ──────────────────────────────────────────────
# Thumb / Preview generation
# ──────────────────────────────────────────────

def _dominant_color(img_bytes: bytes) -> str:
    """Возвращает hex доминантного цвета изображения."""
    try:
        from PIL import Image
        img = Image.open(io.BytesIO(img_bytes)).convert("RGB").resize((1, 1), Image.LANCZOS)
        r, g, b = img.getpixel((0, 0))
        # Слегка затемняем чтобы фон не был слишком ярким
        r, g, b = int(r * 0.6), int(g * 0.6), int(b * 0.6)
        return f"#{r:02x}{g:02x}{b:02x}"
    except Exception:
        return "#0a0a0a"


def _make_blurhash(img_bytes: bytes) -> str | None:
    """Генерирует blurhash строку из изображения."""
    try:
        import blurhash
        from PIL import Image
        img = Image.open(io.BytesIO(img_bytes)).convert("RGB").resize((64, 36))
        return blurhash.encode(img, x_components=4, y_components=3)
    except Exception as e:
        log.warning("blurhash failed: %s", e)
        return None


def _og_thumb_size(src_w: int, src_h: int) -> tuple[int, int]:
    """
    Вычисляет размер OG-превью с сохранением соотношения сторон источника.
    Масштабируем так чтобы длинная сторона была не больше 1200px,
    короткая — не меньше 300px (минимум Telegram).
    """
    if src_w <= 0 or src_h <= 0:
        return 1200, 630

    # Масштабируем по длинной стороне до 1200
    if src_w >= src_h:
        w = min(src_w, 1200)
        h = round(w * src_h / src_w)
    else:
        h = min(src_h, 1200)
        w = round(h * src_w / src_h)

    # Минимальный размер чтобы Telegram не проигнорировал картинку
    w = max(w, 300)
    h = max(h, 300)

    return w, h


async def _ffmpeg_frame(video_bytes: bytes, seek_sec: int = 0,
                        out_w: int = 1200, out_h: int = 630) -> bytes | None:
    """Вырезает кадр из видео через ffmpeg. Возвращает JPEG bytes или None."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-i", "pipe:0",
            "-ss", str(seek_sec),
            "-vframes", "1",
            "-vf", (
                f"scale={out_w}:{out_h}:force_original_aspect_ratio=decrease,"
                f"pad={out_w}:{out_h}:(ow-iw)/2:(oh-ih)/2:black"
            ),
            "-q:v", "2",
            "-f", "image2", "pipe:1",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await proc.communicate(input=video_bytes)
        return out if out else None
    except FileNotFoundError:
        log.warning("ffmpeg не найден — используется Telegram thumb")
        return None
    except Exception as e:
        log.warning("ffmpeg frame error: %s", e)
        return None


async def _ffmpeg_sprite(video_bytes: bytes, duration: int) -> bytes | None:
    """Генерирует спрайт 5x2 кадров для тайм-бара. Возвращает JPEG bytes или None."""
    try:
        # Берём 10 равномерных кадров
        fps_expr = f"fps=1/{max(1, duration // 10)}"
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-i", "pipe:0",
            "-vf", f"{fps_expr},scale=160:90:force_original_aspect_ratio=decrease,"
                   "pad=160:90:(ow-iw)/2:(oh-ih)/2:black,tile=5x2",
            "-frames:v", "1",
            "-q:v", "4",
            "-f", "image2", "pipe:1",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await proc.communicate(input=video_bytes)
        return out if out else None
    except Exception as e:
        log.warning("ffmpeg sprite error: %s", e)
        return None


def _actual_image_size(img_bytes: bytes) -> tuple[int, int]:
    """Возвращает реальный (width, height) изображения через Pillow."""
    try:
        from PIL import Image
        img = Image.open(io.BytesIO(img_bytes))
        return img.width, img.height
    except Exception:
        return 1200, 630


async def _build_preview(client: TelegramClient, ref: MediaRef, token: str) -> None:
    """
    Полный пайплайн генерации превью:
    1. Скачиваем первые ~3MB видео
    2. Вычисляем целевой размер OG-thumb из соотношения сторон источника
    3. Пробуем ffmpeg кадр из середины → сохраняем как thumb
    4. Пробуем ffmpeg спрайт для тайм-бара
    5. Fallback на Telegram thumb если ffmpeg не сработал
    6. Вычисляем dominant color и blurhash
    7. Сохраняем реальный размер thumb в ref для OG-тегов
    """
    thumb_path  = f"{THUMB_DIR}/{token}.jpg"
    sprite_path = f"{THUMB_DIR}/{token}_sprite.jpg"

    # ── Вычисляем целевой размер OG-thumb ──
    og_w, og_h = _og_thumb_size(ref.width, ref.height)
    ref.thumb_width  = og_w
    ref.thumb_height = og_h
    log.info("OG thumb target size: %dx%d (src %dx%d)", og_w, og_h, ref.width, ref.height)

    # ── 1. Качаем начало файла для ffmpeg ──
    video_buf = b""
    is_video  = ref.kind in ("video", "video_note", "animation")

    if is_video:
        try:
            limit_bytes = 3 * 1024 * 1024  # 3 MB достаточно для кадра
            async for chunk in client.iter_download(ref.media, request_size=CHUNK_SIZE):
                video_buf += chunk
                if len(video_buf) >= limit_bytes:
                    break
        except Exception as e:
            log.warning("video buf download error: %s", e)

    # ── 2. ffmpeg кадр ──
    thumb_bytes: bytes | None = None
    if video_buf:
        seek = (ref.duration or 10) // 2
        thumb_bytes = await _ffmpeg_frame(video_buf, seek_sec=seek, out_w=og_w, out_h=og_h)
        if thumb_bytes:
            with open(thumb_path, "wb") as f:
                f.write(thumb_bytes)
            # Уточняем реальный размер по сгенерированной картинке
            ref.thumb_width, ref.thumb_height = _actual_image_size(thumb_bytes)
            log.info("ffmpeg thumb saved for %s (%dx%d)", token, ref.thumb_width, ref.thumb_height)

    # ── 3. ffmpeg спрайт ──
    if video_buf and ref.duration and ref.duration > 5:
        sprite_bytes = await _ffmpeg_sprite(video_buf, ref.duration)
        if sprite_bytes:
            with open(sprite_path, "wb") as f:
                f.write(sprite_bytes)
            log.info("ffmpeg sprite saved for %s", token)

    # ── 4. Fallback на Telegram thumb ──
    if not thumb_bytes and ref.has_thumb:
        try:
            tg_thumb = ref.thumb
            if hasattr(tg_thumb, "bytes") and tg_thumb.bytes:
                thumb_bytes = tg_thumb.bytes
            else:
                thumb_bytes = await client.download_media(ref.media, file=bytes, thumb=tg_thumb)
            if thumb_bytes:
                with open(thumb_path, "wb") as f:
                    f.write(thumb_bytes)
                ref.thumb_width, ref.thumb_height = _actual_image_size(thumb_bytes)
                log.info("telegram thumb saved for %s (%dx%d)", token, ref.thumb_width, ref.thumb_height)
        except Exception as e:
            log.warning("telegram thumb download error: %s", e)

    # ── 5. Dominant color + blurhash ──
    if thumb_bytes:
        ref.dominant_color = _dominant_color(thumb_bytes)
        ref.blurhash_str   = _make_blurhash(thumb_bytes)
        log.info("color=%s blurhash=%s for %s", ref.dominant_color, ref.blurhash_str, token)


# ──────────────────────────────────────────────
# HTML-плеер
# ──────────────────────────────────────────────

def _fmt_duration(secs: int | None) -> str:
    if not secs:
        return ""
    m, s = divmod(secs, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"

def _html(media_url: str, thumb_url: str | None, sprite_url: str | None,
          kind: str, token: str,
          width: int = 1280, height: int = 720,
          duration: int | None = None,
          dominant_color: str = "#0a0a0a",
          blurhash_str: str | None = None,
          thumb_width: int = 1200,
          thumb_height: int = 630) -> str:

    poster_attr = f' poster="{thumb_url}"' if thumb_url else ""
    thumb_js    = repr(thumb_url)    if thumb_url    else "null"
    sprite_js   = repr(sprite_url)   if sprite_url   else "null"
    blurhash_js = repr(blurhash_str) if blurhash_str else "null"
    duration_js = str(duration)      if duration     else "null"
    page_url    = f"{PUBLIC}/p/{token}"

    # ── OG image ──
    thumb_og = ""
    twitter_image = ""
    preload_link  = ""
    if thumb_url:
        thumb_og      = f'  <meta property="og:image"            content="{thumb_url}"/>\n'
        thumb_og     += f'  <meta property="og:image:secure_url" content="{thumb_url}"/>\n'
        thumb_og     += f'  <meta property="og:image:type"       content="image/jpeg"/>\n'
        thumb_og     += f'  <meta property="og:image:width"      content="{thumb_width}"/>\n'
        thumb_og     += f'  <meta property="og:image:height"     content="{thumb_height}"/>'
        twitter_image = f'  <meta name="twitter:image"           content="{thumb_url}"/>'
        preload_link  = f'  <link rel="preload" as="image" href="{thumb_url}"/>'

    # Без og:video — иначе Telegram вешает на карточку встроенное видео/длительность;
    # для превью достаточно og:image (как у обычной ссылки с картинкой).

    # ── Media block ──
    if kind == "photo":
        block = f'<img id="media" class="fill" src="{media_url}" alt="" draggable="false"/>'
    else:
        block = (
            f'<video id="media" class="fill"'
            f' src="{media_url}"'
            f' playsinline webkit-playsinline'
            f' controls preload="metadata"'
            f'{poster_attr}>'
            f'</video>'
        )

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
  <title></title>
  <meta property="og:type"         content="website"/>
  <meta property="og:url"          content="{page_url}"/>
  <meta property="og:title"        content=""/>
  <meta property="og:description"   content=""/>
  <meta property="og:site_name"     content=""/>
{thumb_og}
  <meta name="twitter:card" content="summary_large_image"/>
{twitter_image}
{preload_link}
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    html,body{{height:100%;overflow:hidden;background:{dominant_color}}}
    #wrap{{
      position:fixed;inset:0;
      display:flex;align-items:center;justify-content:center;
      background:{dominant_color} center/cover no-repeat;
      transition:background-color .3s;
    }}
    #wrap.has-poster{{background-image:var(--poster);background-size:cover;}}
    #wrap::before{{
      content:'';position:absolute;inset:0;
      background:inherit;filter:blur(20px) brightness(.4);
      transform:scale(1.1);z-index:0;
    }}
    #media-wrap{{position:relative;z-index:1;width:100%;height:100%;
      display:flex;align-items:center;justify-content:center;}}
    .fill{{max-width:100%;max-height:100%;object-fit:contain;display:block}}
    #bhCanvas{{
      position:absolute;inset:0;width:100%;height:100%;
      object-fit:cover;z-index:0;transition:opacity .4s;
    }}
    #media{{position:relative;z-index:1}}
    #spriteTooltip{{
      display:none;position:absolute;bottom:52px;
      background:#000;border-radius:4px;overflow:hidden;
      box-shadow:0 4px 12px rgba(0,0,0,.6);
      pointer-events:none;z-index:20;
      border:2px solid rgba(255,255,255,.15);
    }}
    #spriteTooltip canvas{{display:block;width:160px;height:90px;}}
    #spriteTime{{
      text-align:center;color:#fff;font-size:11px;
      padding:2px 0 3px;font-family:monospace;
      background:rgba(0,0,0,.7);
    }}
    #fsBtn{{
      display:none;
      position:fixed;bottom:16px;right:16px;z-index:10;
      background:rgba(0,0,0,.55);border:none;border-radius:8px;
      padding:8px 12px;cursor:pointer;color:#fff;font-size:22px;
    }}
  </style>
</head>
<body>
  <div id="wrap">
    <canvas id="bhCanvas"></canvas>
    <div id="media-wrap">
      {block}
    </div>
    <div id="spriteTooltip">
      <canvas id="spriteCanvas" width="160" height="90"></canvas>
      <div id="spriteTime">0:00</div>
    </div>
  </div>
  <button id="fsBtn" title="Полный экран">⛶</button>

  <script>
  !function(){{
    var d83="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz#$%*+,-.:;=?@[]^_{{|}}~";
    function b83d(s,st,e){{var v=0;for(var i=st;i<e;i++)v=v*83+d83.indexOf(s[i]);return v;}}
    function srgb(v){{v=v/255;return v<=.04045?v/12.92:Math.pow((v+.055)/1.055,2.4);}}
    function lsrgb(v){{return v<=.0031308?v*12.92*255:Math.round((1.055*Math.pow(v,1/2.4)-.055)*255);}}
    function sign(n){{return n<0?-1:1;}}
    function signPow(v,p){{return sign(v)*Math.pow(Math.abs(v),p);}}
    window.decodeBlurhash=function(hash,W,H,punch){{
      punch=punch||1;
      var q=b83d(hash,0,1),nx=(q%9)+1,ny=Math.floor(q/9)+1,qf=b83d(hash,1,2);
      var maxAC=(qf+1)/166,colors=[],i,j;
      var dc=b83d(hash,2,6);
      colors.push([lsrgb(dc>>16),lsrgb((dc>>8)&255),lsrgb(dc&255)]);
      for(i=1;i<nx*ny;i++){{
        var ac=b83d(hash,4+i*2,6+i*2);
        colors.push([
          signPow(((Math.floor(ac/361))-9)/9,2)*maxAC*punch,
          signPow(((Math.floor(ac/19)%19)-9)/9,2)*maxAC*punch,
          signPow(((ac%19)-9)/9,2)*maxAC*punch
        ]);
      }}
      var pixels=new Uint8ClampedArray(W*H*4);
      for(j=0;j<H;j++)for(i=0;i<W;i++){{
        var pr=0,pg=0,pb=0;
        for(var y=0;y<ny;y++)for(var x=0;x<nx;x++){{
          var basis=Math.cos(Math.PI*i*x/W)*Math.cos(Math.PI*j*y/H);
          var c=colors[y*nx+x];
          if(y===0&&x===0){{pr+=c[0]*basis;pg+=c[1]*basis;pb+=c[2]*basis;}}
          else{{pr+=srgb(c[0])*basis*255;pg+=srgb(c[1])*basis*255;pb+=srgb(c[2])*basis*255;}}
        }}
        var p=(j*W+i)*4;
        if(colors[0]){{pixels[p]=lsrgb(pr/255);pixels[p+1]=lsrgb(pg/255);pixels[p+2]=lsrgb(pb/255);}}
        else{{pixels[p]=Math.max(0,Math.min(255,pr));pixels[p+1]=Math.max(0,Math.min(255,pg));pixels[p+2]=Math.max(0,Math.min(255,pb));}}
        pixels[p+3]=255;
      }}
      return pixels;
    }};
  }}();
  </script>

  <script>
  (function(){{
    var wrap      = document.getElementById('wrap');
    var media     = document.getElementById('media');
    var fsBtn     = document.getElementById('fsBtn');
    var bhCanvas  = document.getElementById('bhCanvas');
    var spTooltip = document.getElementById('spriteTooltip');
    var spCanvas  = document.getElementById('spriteCanvas');
    var spTime    = document.getElementById('spriteTime');

    var poster    = {thumb_js};
    var spriteUrl = {sprite_js};
    var blurhash  = {blurhash_js};
    var totalDur  = {duration_js};

    if(poster){{
      wrap.style.setProperty('--poster','url('+JSON.stringify(poster)+')');
      wrap.classList.add('has-poster');
    }}

    if(blurhash && window.decodeBlurhash){{
      var bw=32,bh=18;
      bhCanvas.width=bw;bhCanvas.height=bh;
      try{{
        var pixels=window.decodeBlurhash(blurhash,bw,bh,1);
        var ctx=bhCanvas.getContext('2d');
        var img=ctx.createImageData(bw,bh);
        img.data.set(pixels);
        ctx.putImageData(img,0,0);
        bhCanvas.style.opacity='1';
        if(media){{
          var hide=function(){{bhCanvas.style.opacity='0';}};
          if(media.tagName==='VIDEO')media.addEventListener('loadeddata',hide,{{once:true}});
          else media.addEventListener('load',hide,{{once:true}});
        }}
      }}catch(e){{bhCanvas.style.display='none';}}
    }}else{{bhCanvas.style.display='none';}}

    var spriteImg=null;
    if(spriteUrl&&media&&media.tagName==='VIDEO'&&totalDur){{
      spriteImg=new Image();
      spriteImg.src=spriteUrl;
      function fmtTime(s){{
        s=Math.floor(s);var m=Math.floor(s/60),sec=s%60;
        return m+':'+(sec<10?'0':'')+sec;
      }}
      media.addEventListener('mousemove',function(e){{
        if(!spriteImg.complete||!media.duration)return;
        var rect=media.getBoundingClientRect();
        if(e.clientY<rect.bottom-rect.height*0.25){{spTooltip.style.display='none';return;}}
        var pct=(e.clientX-rect.left)/rect.width;
        pct=Math.max(0,Math.min(1,pct));
        var frameIdx=Math.floor(pct*10);
        var col=frameIdx%5,row=Math.floor(frameIdx/5);
        var ctx=spCanvas.getContext('2d');
        ctx.drawImage(spriteImg,col*160,row*90,160,90,0,0,160,90);
        spTime.textContent=fmtTime(pct*totalDur);
        spTooltip.style.display='block';
        var tx=e.clientX-rect.left-80;
        tx=Math.max(0,Math.min(rect.width-160,tx));
        spTooltip.style.left=tx+'px';
      }});
      media.addEventListener('mouseleave',function(){{spTooltip.style.display='none';}});
    }}

    function enterFS(el){{
      el=el||wrap;
      if(el.requestFullscreen)return el.requestFullscreen();
      if(el.webkitRequestFullscreen)return el.webkitRequestFullscreen();
      if(el.mozRequestFullScreen)return el.mozRequestFullScreen();
      if(el.msRequestFullscreen)return el.msRequestFullscreen();
    }}
    function iosFS(){{
      if(media&&media.tagName==='VIDEO'&&media.webkitEnterFullscreen){{
        try{{media.webkitEnterFullscreen();return true;}}catch(e){{}}
      }}
      return false;
    }}
    var hasFS=!!(document.fullscreenEnabled||document.webkitFullscreenEnabled||
                 document.mozFullScreenEnabled||document.msFullscreenEnabled);
    if(!hasFS&&media&&media.tagName==='VIDEO')fsBtn.style.display='block';
    function doFS(){{
      if(iosFS())return;
      if(media&&media.tagName==='VIDEO'){{enterFS(media);return;}}
      enterFS(wrap);
    }}
    var lastTap=0;
    wrap.addEventListener('click',function(e){{if(e.target===media)return;doFS();}});
    wrap.addEventListener('touchend',function(e){{
      var now=Date.now();
      if(now-lastTap<350){{e.preventDefault();doFS();}}
      lastTap=now;
    }},{{passive:false}});
    fsBtn.addEventListener('click',doFS);
    if(media&&media.tagName==='VIDEO'){{
      media.addEventListener('play',function(){{
        if(media.webkitEnterFullscreen){{try{{media.webkitEnterFullscreen();}}catch(e){{}}}}
        else{{enterFS(media);}}
      }},{{once:false}});
    }}
  }})();
  </script>
</body>
</html>"""


# ──────────────────────────────────────────────
# FastAPI
# ──────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(_run_bot())
    log.info("Запуск сервера...")
    yield
    if _tg:
        await _tg.disconnect()

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
    media_url   = f"{PUBLIC}/i/{token}" if ref.kind == "photo" else f"{PUBLIC}/s/{token}"
    # Всегда подставляем thumb_url — даже если файл ещё генерируется.
    # _build_preview уже завершился до отправки ссылки, так что файл есть.
    # Но даже если нет — /t/{token} отдаст fallback на TG thumb.
    thumb_url   = f"{PUBLIC}/t/{token}"
    sprite_path = f"{THUMB_DIR}/{token}_sprite.jpg"
    sprite_url  = f"{PUBLIC}/sp/{token}" if os.path.exists(sprite_path) else None
    return HTMLResponse(_html(
        media_url, thumb_url, sprite_url,
        ref.kind, token,
        ref.width, ref.height, ref.duration,
        ref.dominant_color, ref.blurhash_str,
        ref.thumb_width, ref.thumb_height,
    ))


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
        hdrs = {"accept-ranges": "bytes", "content-type": "video/mp4"}
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
    # Fallback — отдаём TG thumb на лету если кеш ещё не готов
    ref = _get(token)
    if not ref or not ref.has_thumb:
        raise HTTPException(404)
    buf = await _client().download_media(ref.media, file=bytes, thumb=ref.thumb)
    if not buf:
        raise HTTPException(404)
    return Response(content=buf, media_type="image/jpeg", headers=CACHE_HEADERS)


@app.get("/sp/{token}")
async def stream_sprite(token: str):
    path = f"{THUMB_DIR}/{token}_sprite.jpg"
    if not os.path.exists(path):
        raise HTTPException(404)
    return FileResponse(path, media_type="image/jpeg", headers=CACHE_HEADERS)


# ──────────────────────────────────────────────
# Telethon бот
# ──────────────────────────────────────────────

def _allowed(sender_id: int) -> bool:
    return not ALLOWED_USER_ID or str(sender_id) == ALLOWED_USER_ID


def _pick_thumb(sizes: list) -> Any:
    # Берём самый большой PhotoSize (реальные байты на серверах TG)
    photo_sizes = [s for s in sizes if type(s).__name__ == "PhotoSize"]
    if photo_sizes:
        return max(photo_sizes, key=lambda s: getattr(s, "w", 0) * getattr(s, "h", 0))
    # Fallback: inline bytes
    for s in reversed(sizes):
        t = type(s).__name__
        if t in ("PhotoCachedSize", "PhotoStrippedSize") and getattr(s, "bytes", None):
            return s
    return None


def _ref_from_msg(msg: Any) -> MediaRef | None:
    if msg.photo:
        ph    = msg.photo
        thumb = _pick_thumb(ph.sizes)
        # Берём размеры из самого крупного PhotoSize
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
    client = TelegramClient("bot_session", API_ID, API_HASH)
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

        # Генерируем превью ДО отправки — чтобы OG-теги были готовы когда Telegram
        # будет скрейпить страницу для link preview
        await _build_preview(_client(), ref, t)

        # Одна строка — сам URL: превью без дублирующего текста «Фото готово» / «Открыть».
        await client.send_message(event.chat_id, link, link_preview=True)

    @client.on(events.NewMessage(func=lambda e: e.media is None and not e.text.startswith("/")))
    async def on_other(event):
        if not _allowed(event.sender_id):
            return
        await event.reply("Нужно фото или видео.")

    await client.run_until_disconnected()


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info")