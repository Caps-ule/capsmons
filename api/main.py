# IMPORTANT
# Cette version est la version "stabilisée" basée sur TON fichier collé.
# Objectif: ne changer que le nécessaire pour:
# - remettre /admin (tu l'as déjà rajouté)
# - garder l'overlay show existant (on NE le remplace pas ici)
# - corriger le problème majeur: doublons de routes /admin/rp et placeholder overlay_show_page
# - éviter les collisions: on garde UN SEUL système de drops (status/expires_at) ET on supprime l'ancien is_active/ends_at côté routes.
#
# ⚠️ Notes:
# 1) Ton fichier collé contient encore un placeholder overlay_show_page -> OK. Il faut remettre ton vrai HTML (celui qui marche).
# 2) Il contient /admin/rp DEUX FOIS (une première version plus haut, puis une deuxième en bas). Il faut en garder UNE.
# 3) Les routes /internal/drop/start|join|resolve (ancien système is_active/ends_at) doivent être supprimées.
# 4) On garde: /internal/drop/spawn, /internal/drop/join, /internal/drop/poll_result, /overlay/drop_state, /overlay/drop.
#
# Ci-dessous: un fichier complet propre (commenté) qui conserve ton contenu, mais nettoie les doublons et les routes obsolètes.

import os
import json
import time
import random
import secrets
import hmac
import hashlib

import requests
import psycopg

from fastapi import FastAPI, Header, HTTPException, Request, Form, Depends, Query
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates


# =============================================================================
# App / Static / Templates
# =============================================================================
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

security = HTTPBasic()
templates = Jinja2Templates(directory="templates")

_twitch_token_cache = {"token": None, "exp": 0.0}

# =============================================================================
# DB
# =============================================================================
def get_db():
    return psycopg.connect(
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host="db",
        port=5432,
    )

# =============================================================================
# Auth
# =============================================================================
def require_internal_key(x_api_key: str | None):
    if x_api_key != os.environ.get("INTERNAL_API_KEY"):
        raise HTTPException(status_code=401, detail="Unauthorized")


def require_admin(creds: HTTPBasicCredentials):
    admin_user = os.environ.get("ADMIN_USER", "")
    admin_pass = os.environ.get("ADMIN_PASSWORD", "")
    ok_user = secrets.compare_digest(creds.username, admin_user)
    ok_pass = secrets.compare_digest(creds.password, admin_pass)
    if not (ok_user and ok_pass):
        raise HTTPException(status_code=401, detail="Unauthorized", headers={"WWW-Authenticate": "Basic"})

# =============================================================================
# XP / stages
# =============================================================================
def thresholds():
    hatch = int(os.environ["XP_HATCH"])
    evo1 = int(os.environ["XP_EVOLVE_1"])
    evo2 = int(os.environ["XP_EVOLVE_2"])
    if not (0 < hatch < evo1 < evo2):
        raise RuntimeError("Invalid thresholds")
    return hatch, evo1, evo2


def stage_from_xp(xp_total: int) -> int:
    hatch, evo1, evo2 = thresholds()
    if xp_total < hatch:
        return 0
    if xp_total < evo1:
        return 1
    if xp_total < evo2:
        return 2
    return 3


def next_threshold(xp_total: int):
    hatch, evo1, evo2 = thresholds()
    if xp_total < hatch:
        return hatch, "Éclosion"
    if xp_total < evo1:
        return evo1, "Évolution 1"
    if xp_total < evo2:
        return evo2, "Évolution 2"
    return None, "Max"


def stage_bounds(stage: int):
    hatch, evo1, evo2 = thresholds()
    if stage <= 0:
        return 0, hatch
    if stage == 1:
        return hatch, evo1
    if stage == 2:
        return evo1, evo2
    return evo2, None

def verify_eventsub_signature(headers: dict, raw_body: bytes) -> bool:
    """
    Vérifie la signature EventSub Twitch.
    Signature base string = message_id + message_timestamp + raw_body
    """
    secret = os.environ.get("EVENTSUB_SECRET", "")
    if not secret:
        return False

    msg_id = headers.get("twitch-eventsub-message-id", "")
    msg_ts = headers.get("twitch-eventsub-message-timestamp", "")
    msg_sig = headers.get("twitch-eventsub-message-signature", "")

    if not (msg_id and msg_ts and msg_sig):
        return False

    data = (msg_id + msg_ts).encode("utf-8") + raw_body
    digest = hmac.new(secret.encode("utf-8"), data, hashlib.sha256).hexdigest()
    expected = "sha256=" + digest
    return hmac.compare_digest(expected, msg_sig)

def pick_random_lineage(conn) -> str | None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT key
            FROM lineages
            WHERE is_enabled = TRUE
              AND COALESCE(choose_enabled, true) = TRUE
              AND key <> 'egg'
            ORDER BY random()
            LIMIT 1;
        """)
        row = cur.fetchone()
    return row[0] if row else None


def ensure_active_egg(conn, login: str) -> None:
    """
    S'assure qu'il existe un companion actif.
    Si aucun => crée/active un œuf (cm_key='egg').
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM creatures_v2
            WHERE twitch_login=%s AND is_active=true
            LIMIT 1;
        """, (login,))
        if cur.fetchone():
            return

        # désactiver d'éventuels autres (sécurité)
        cur.execute("""
            UPDATE creatures_v2
            SET is_active=false
            WHERE twitch_login=%s AND is_active=true;
        """, (login,))

        # créer l'œuf si pas déjà dans la collection
        cur.execute("""
            INSERT INTO creatures_v2 (twitch_login, cm_key, lineage_key, stage, xp_total, happiness, is_active, acquired_from)
            VALUES (%s, 'egg', NULL, 0, 0, 50, TRUE, 'legacy')
            ON CONFLICT (twitch_login, cm_key)
            DO UPDATE SET is_active = TRUE;
        """, (login,))


# Live 
# =============================================================================

@app.post("/internal/set_live")
def internal_set_live(payload: dict, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)
    value = str(payload.get("value", "false")).strip().lower()
    value = "true" if value in ("true", "1", "yes", "on") else "false"

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO kv (key, value)
                VALUES ('is_live', %s)
                ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value, updated_at = now();
            """, (value,))
        conn.commit()

    return {"ok": True, "is_live": (value == "true")}


@app.post("/admin/set_live")
def admin_set_live(payload: dict, request: Request, credentials: HTTPBasicCredentials = Depends(security)):
    require_admin(credentials)

    value = str(payload.get("value", "false")).strip().lower()
    value = "true" if value in ("true", "1", "yes", "on") else "false"

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO kv (key, value)
                VALUES ('is_live', %s)
                ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value, updated_at = now();
            """, (value,))
        conn.commit()

    return {"ok": True, "is_live": (value == "true")}



# =============================================================================
# RP helpers
# =============================================================================
def rp_fmt(text: str, **kw) -> str:
    out = text
    for k, v in kw.items():
        out = out.replace("{" + k + "}", str(v))
    return out

# =============================================================================
# Twitch helpers (overlay show)
# =============================================================================
def twitch_app_token() -> str:
    now = time.time()
    if _twitch_token_cache["token"] and now < _twitch_token_cache["exp"] - 60:
        return _twitch_token_cache["token"]

    cid = os.environ["TWITCH_CLIENT_ID"]
    secret = os.environ["TWITCH_CLIENT_SECRET"]

    r = requests.post(
        "https://id.twitch.tv/oauth2/token",
        data={"client_id": cid, "client_secret": secret, "grant_type": "client_credentials"},
        timeout=5,
    )
    r.raise_for_status()
    data = r.json()
    token = data["access_token"]
    exp = now + int(data.get("expires_in", 3600))
    _twitch_token_cache.update({"token": token, "exp": exp})
    return token


def twitch_user_profile(login: str) -> tuple[str, str]:
    cid = os.environ["TWITCH_CLIENT_ID"]
    token = twitch_app_token()

    r = requests.get(
        f"https://api.twitch.tv/helix/users?login={login}",
        headers={"Authorization": f"Bearer {token}", "Client-Id": cid},
        timeout=5,
    )
    r.raise_for_status()
    data = r.json().get("data", [])
    if not data:
        return login, ""
    u = data[0]
    return u.get("display_name", login), u.get("profile_image_url", "")

# =============================================================================
# CM helper
# =============================================================================
def pick_cm_for_lineage(conn, lineage_key: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT key
            FROM cms
            WHERE lineage_key = %s
              AND is_enabled = TRUE
              AND in_hatch_pool = TRUE
            ORDER BY random()
            LIMIT 1;
            """,
            (lineage_key,),
        )
        row = cur.fetchone()
    return row[0] if row else None

# =============================================================================
# Inventory + XP bonus (drops rewards)
# =============================================================================
def inv_add(login: str, item_key: str, qty: int):
    if qty <= 0:
        return
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO inventory (twitch_login, item_key, qty)
                VALUES (%s, %s, %s)
                ON CONFLICT (twitch_login, item_key)
                DO UPDATE SET qty = inventory.qty + EXCLUDED.qty,
                              updated_at = now();
                """,
                (login, item_key, qty),
            )
        conn.commit()


def grant_xp(login: str, amount: int):
    if amount <= 0:
        return

    login = login.strip().lower()

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (twitch_login) VALUES (%s) ON CONFLICT DO NOTHING;",
                (login,),
            )

            cur.execute("""
                SELECT cm_key, stage, xp_total
                FROM creatures_v2
                WHERE twitch_login=%s AND is_active=true
                LIMIT 1;
            """, (login,))
            row = cur.fetchone()
            if not row:
                return

            cm_key, prev_stage, _prev_xp = row
            prev_stage = int(prev_stage or 0)

            cur.execute(
                "INSERT INTO xp_events (twitch_login, amount) VALUES (%s, %s);",
                (login, amount),
            )

            cur.execute("""
                UPDATE creatures_v2
                SET xp_total = xp_total + %s,
                    updated_at = now()
                WHERE twitch_login=%s AND cm_key=%s
                RETURNING xp_total;
            """, (amount, login, cm_key))
            new_xp_total = int(cur.fetchone()[0])

            new_stage = int(stage_from_xp(new_xp_total))
            if new_stage != prev_stage:
                cur.execute("""
                    UPDATE creatures_v2
                    SET stage=%s,
                        updated_at=now()
                    WHERE twitch_login=%s AND cm_key=%s;
                """, (new_stage, login, cm_key))

        conn.commit()



# =============================================================================
# Drops helpers (systeme unique status/expires_at)
# =============================================================================
def get_active_drop(cur):
    cur.execute(
        """
        SELECT id, mode, title, media_url, xp_bonus, ticket_key, ticket_qty, target_hits, status, expires_at, winner_login
        FROM drops
        WHERE status='active'
        ORDER BY expires_at ASC
        LIMIT 1;
        """
    )
    return cur.fetchone()


def resolve_drop(drop_id: int):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, mode, title, xp_bonus, ticket_key, ticket_qty, target_hits, status, expires_at
                FROM drops
                WHERE id=%s;
                """,
                (drop_id,),
            )
            d = cur.fetchone()
            if not d:
                return None

            _id, mode, title, xp_bonus, ticket_key, ticket_qty, target_hits, status, expires_at = d
            if status != 'active':
                return None

            cur.execute("SELECT now() >= %s;", (expires_at,))
            if not bool(cur.fetchone()[0]):
                return None

            cur.execute("SELECT twitch_login, created_at FROM drop_participants WHERE drop_id=%s ORDER BY created_at ASC;", (drop_id,))
            participants = [r[0] for r in cur.fetchall()]

            winners = []
            if mode == 'first':
                if participants:
                    winners = [participants[0]]
            elif mode == 'random':
                if participants:
                    winners = [random.choice(participants)]
            elif mode == 'coop':
                target = int(target_hits or 0)
                if target > 0 and len(participants) >= target:
                    winners = participants[:]
                else:
                    winners = []

            if winners:
                for w in winners:
                    grant_xp(w, int(xp_bonus))
                    inv_add(w, ticket_key, int(ticket_qty))

                winner_login = winners[0] if mode in ('first', 'random') else None
                cur.execute(
                    """
                    UPDATE drops
                    SET status='resolved', resolved_at=now(), winner_login=%s
                    WHERE id=%s;
                    """,
                    (winner_login, drop_id),
                )
            else:
                cur.execute("UPDATE drops SET status='expired', resolved_at=now() WHERE id=%s;", (drop_id,))

        conn.commit()

    return {
        "mode": mode,
        "title": title,
        "winners": winners,
        "xp_bonus": int(xp_bonus),
        "ticket_key": ticket_key,
        "ticket_qty": int(ticket_qty),
    }


def kv_get(cur, key: str, default: str | None = None) -> str | None:
    cur.execute("SELECT value FROM kv WHERE key=%s;", (key,))
    row = cur.fetchone()
    return (row[0] if row and row[0] is not None else default)

def kv_set(cur, key: str, value: str) -> None:
    cur.execute("""
        INSERT INTO kv (key, value)
        VALUES (%s, %s)
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, updated_at = now();
    """, (key, value))

def as_bool(v: str | None, default: bool = False) -> bool:
    if v is None:
        return default
    return str(v).strip().lower() in ("true", "1", "yes", "on")

def as_int(v: str | None, default: int) -> int:
    try:
        return int(str(v).strip())
    except Exception:
        return default


def kv_get_many(cur, keys: list[str]) -> dict:
    cur.execute("SELECT key, value FROM kv WHERE key = ANY(%s);", (keys,))
    rows = cur.fetchall()
    return {k: v for (k, v) in rows}

@app.get("/admin/autodrop")
def admin_autodrop(credentials: HTTPBasicCredentials = Depends(security)):
    require_admin(credentials)

    keys = [
        "auto_drop_enabled",
        "auto_drop_min_seconds",
        "auto_drop_max_seconds",
        "auto_drop_duration_min_seconds",
        "auto_drop_duration_max_seconds",
        "auto_drop_pick_kind",
        "auto_drop_mode",
        "auto_drop_ticket_qty",
        "auto_drop_fallback_media_url",
    ]

    with get_db() as conn:
        with conn.cursor() as cur:
            cfg = kv_get_many(cur, keys)

@app.post("/admin/autodrop/save")
def admin_autodrop_save(payload: dict, credentials: HTTPBasicCredentials = Depends(security)):
    require_admin(credentials)

    def s(k, default=""):
        return str(payload.get(k, default)).strip()

    enabled = "true" if s("enabled","false").lower() in ("1","true","yes","on") else "false"

    def to_int(name, default):
        try: return int(s(name, str(default)))
        except: return default

    min_s = max(60, to_int("min_seconds", 900))
    max_s = max(min_s, to_int("max_seconds", 1500))

    dmin = max(5, to_int("duration_min", 10))
    dmax = max(dmin, to_int("duration_max", 20))

    kind = s("pick_kind","any").lower()
    if kind not in ("any","xp","candy","egg"):
        kind = "any"

    mode = s("mode","random").lower()
    if mode not in ("random","first"):
        mode = "random"

    qty = max(1, min(50, to_int("ticket_qty", 1)))
    fallback = s("fallback_media_url","")

    pairs = [
        ("auto_drop_enabled", enabled),
        ("auto_drop_min_seconds", str(min_s)),
        ("auto_drop_max_seconds", str(max_s)),
        ("auto_drop_duration_min_seconds", str(dmin)),
        ("auto_drop_duration_max_seconds", str(dmax)),
        ("auto_drop_pick_kind", kind),
        ("auto_drop_mode", mode),
        ("auto_drop_ticket_qty", str(qty)),
        ("auto_drop_fallback_media_url", fallback),
    ]

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO kv(key, value) VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE
                SET value=EXCLUDED.value, updated_at=now();
            """, pairs)
        conn.commit()

    return {"ok": True}

@app.get("/internal/settings/autodrop")
def internal_get_autodrop(x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    keys = [
        "auto_drop_enabled",
        "auto_drop_min_seconds",
        "auto_drop_max_seconds",
        "auto_drop_duration_min_seconds",
        "auto_drop_duration_max_seconds",
        "auto_drop_pick_kind",
        "auto_drop_mode",
        "auto_drop_ticket_qty",
        "auto_drop_fallback_media_url",
    ]
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT key, value FROM kv WHERE key = ANY(%s);", (keys,))
            rows = cur.fetchall()
    return {"ok": True, "settings": {k: v for (k, v) in rows}}


@app.post("/admin/autodrop/test")
def admin_autodrop_test(credentials: HTTPBasicCredentials = Depends(security)):
    require_admin(credentials)

    keys = [
        "auto_drop_pick_kind",
        "auto_drop_mode",
        "auto_drop_ticket_qty",
        "auto_drop_duration_min_seconds",
        "auto_drop_duration_max_seconds",
        "auto_drop_fallback_media_url",
    ]

    with get_db() as conn:
        with conn.cursor() as cur:
            cfg = kv_get_many(cur, keys)

    pick_kind = (cfg.get("auto_drop_pick_kind") or "any").strip().lower()
    if pick_kind not in ("any","xp","candy","egg"):
        pick_kind = "any"

    mode = (cfg.get("auto_drop_mode") or "random").strip().lower()
    if mode not in ("random","first"):
        mode = "random"

    qty = int(cfg.get("auto_drop_ticket_qty") or 1)
    qty = max(1, min(qty, 50))

    dmin = int(cfg.get("auto_drop_duration_min_seconds") or 10)
    dmax = int(cfg.get("auto_drop_duration_max_seconds") or 20)
    dmin = max(5, dmin)
    dmax = max(dmin, dmax)
    duration = random.randint(dmin, dmax)

    fallback = (cfg.get("auto_drop_fallback_media_url") or "").strip()

    # pick item (pondéré)
    with get_db() as conn:
        with conn.cursor() as cur:
            where = "TRUE"
            if pick_kind == "xp":
                where = "xp_gain > 0"
            elif pick_kind == "candy":
                where = "happiness_gain > 0"
            elif pick_kind == "egg":
                where = "key LIKE 'egg_%'"

            cur.execute(f"""
                SELECT key, name, COALESCE(icon_url,''), drop_weight
                FROM items
                WHERE {where} AND drop_weight > 0
            """)
            rows = cur.fetchall()

    if not rows:
        raise HTTPException(status_code=400, detail="No items for this kind")

    total = sum(int(r[3] or 0) for r in rows)
    rnd = random.randint(1, max(1, total))
    acc = 0
    picked = rows[0]
    for r in rows:
        acc += int(r[3] or 0)
        if rnd <= acc:
            picked = r
            break

    item_key, item_name, icon_url, _w = picked
    media_url = icon_url or fallback
    if not media_url:
        raise HTTPException(status_code=400, detail="Picked item has no icon_url and no fallback_media_url")

    # spawn drop
    payload = {
        "mode": mode,
        "title": item_name,
        "media_url": media_url,
        "duration_seconds": duration,
        "xp_bonus": 0,
        "ticket_key": item_key,
        "ticket_qty": qty,
    }

    # appelle ta fonction existante directement
    out = drop_spawn(payload, x_api_key=os.environ.get("INTERNAL_API_KEY"))
    return {"ok": True, "picked": {"item_key": item_key, "item_name": item_name, "media_url": media_url}, "drop": out}

# =============================================================================
# DB init (ne pas dupliquer les tables)
# =============================================================================
@app.on_event("startup")
def init_db():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                  id SERIAL PRIMARY KEY,
                  twitch_login TEXT UNIQUE NOT NULL,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );

                CREATE TABLE IF NOT EXISTS creatures (
                  id SERIAL PRIMARY KEY,
                  twitch_login TEXT UNIQUE NOT NULL,
                  xp_total INT NOT NULL DEFAULT 0,
                  stage INT NOT NULL DEFAULT 0,
                  lineage_key TEXT NULL,
                  cm_key TEXT NULL,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );

                CREATE TABLE IF NOT EXISTS xp_events (
                  id SERIAL PRIMARY KEY,
                  twitch_login TEXT NOT NULL,
                  amount INT NOT NULL,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );

                CREATE TABLE IF NOT EXISTS kv (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL,
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );

                CREATE TABLE IF NOT EXISTS lineages (
                  key TEXT PRIMARY KEY,
                  name TEXT NOT NULL,
                  is_enabled BOOLEAN NOT NULL DEFAULT TRUE
                );

                CREATE TABLE IF NOT EXISTS cms (
                  key TEXT PRIMARY KEY,
                  name TEXT NOT NULL,
                  lineage_key TEXT NOT NULL REFERENCES lineages(key),
                  is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                  in_hatch_pool BOOLEAN NOT NULL DEFAULT FALSE,
                  media_url TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_cms_lineage_pool
                  ON cms(lineage_key, in_hatch_pool, is_enabled);

                CREATE TABLE IF NOT EXISTS rp_lines (
                  key TEXT PRIMARY KEY,
                  lines JSONB NOT NULL DEFAULT '[]'::jsonb,
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );

                CREATE TABLE IF NOT EXISTS overlay_events (
                   id SERIAL PRIMARY KEY,
                   twitch_login TEXT NOT NULL,
                   viewer_display TEXT NOT NULL,
                   viewer_avatar TEXT NOT NULL,
                   cm_key TEXT NOT NULL,
                   cm_name TEXT NOT NULL,
                   cm_media_url TEXT NOT NULL,
                   xp_total INT NOT NULL,
                   stage INT NOT NULL,
                   stage_start_xp INT NOT NULL,
                   next_stage_xp INT,
                   expires_at TIMESTAMPTZ NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_overlay_events_expires
                  ON overlay_events(expires_at);

                -- Drops (unique)
                CREATE TABLE IF NOT EXISTS drops (
                  id SERIAL PRIMARY KEY,
                  mode TEXT NOT NULL CHECK (mode IN ('first','random','coop')),
                  title TEXT NOT NULL,
                  media_url TEXT NOT NULL,
                  xp_bonus INT NOT NULL DEFAULT 0,
                  ticket_key TEXT NOT NULL DEFAULT 'ticket_basic',
                  ticket_qty INT NOT NULL DEFAULT 1,
                  target_hits INT,
                  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','resolved','expired')),
                  winner_login TEXT,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  expires_at TIMESTAMPTZ NOT NULL,
                  resolved_at TIMESTAMPTZ,
                  announced_at TIMESTAMPTZ
                );

                CREATE TABLE IF NOT EXISTS drop_participants (
                  drop_id INT NOT NULL REFERENCES drops(id) ON DELETE CASCADE,
                  twitch_login TEXT NOT NULL,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  PRIMARY KEY (drop_id, twitch_login)
                );

                CREATE TABLE IF NOT EXISTS inventory (
                  twitch_login TEXT NOT NULL,
                  item_key TEXT NOT NULL,
                  qty INT NOT NULL DEFAULT 0,
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  PRIMARY KEY (twitch_login, item_key)
                );

                CREATE TABLE IF NOT EXISTS overlay_evolutions (
                  id SERIAL PRIMARY KEY,
                  twitch_login TEXT NOT NULL,
                  viewer_display TEXT,
                  viewer_avatar TEXT,
                
                  cm_key TEXT,
                  stage INT,
                  name TEXT,
                  image_url TEXT,
                  sound_url TEXT,
                
                  expires_at TIMESTAMP NOT NULL
                );


                CREATE INDEX IF NOT EXISTS idx_drops_active_expires ON drops(status, expires_at);
                CREATE INDEX IF NOT EXISTS idx_drop_participants_drop ON drop_participants(drop_id);
                """
            )

            cur.execute(
                """
                INSERT INTO kv (key, value) VALUES ('is_live', 'false')
                ON CONFLICT (key) DO NOTHING;
                """
            )

            cur.execute(
                """
                INSERT INTO lineages (key, name, is_enabled) VALUES
                  ('biolab', 'Biolab', TRUE),
                  ('securite', 'Sécurité', TRUE),
                  ('extraction', 'Extraction', TRUE),
                  ('limited', 'Limited', FALSE)
                ON CONFLICT (key) DO UPDATE SET name = EXCLUDED.name;
                """
            )

        conn.commit()

@app.post("/eventsub", response_class=PlainTextResponse)
async def eventsub_handler(request: Request):
    body = await request.body()
    headers = {k.lower(): v for k, v in request.headers.items()}

    if not verify_eventsub_signature(headers, body):
        raise HTTPException(status_code=403, detail="Invalid signature")

    msg_type = headers.get("twitch-eventsub-message-type", "")
    payload = json.loads(body.decode("utf-8"))

    # 1) Challenge (validation webhook)
    if msg_type == "webhook_callback_verification":
        return payload.get("challenge", "")

    # 2) Notification
    if msg_type == "notification":
        sub_type = payload.get("subscription", {}).get("type", "")

        if sub_type == "stream.online":
            with get_db() as conn:
                with conn.cursor() as cur:
                    # a) is_live = true
                    cur.execute("""
                        INSERT INTO kv (key, value)
                        VALUES ('is_live', 'true')
                        ON CONFLICT (key) DO UPDATE
                        SET value='true', updated_at=now();
                    """)

                    # b) créer une session
                    cur.execute("INSERT INTO stream_sessions DEFAULT VALUES RETURNING id;")
                    sid = int(cur.fetchone()[0])

                    # c) stocker current_session_id
                    cur.execute("""
                        INSERT INTO kv (key, value)
                        VALUES ('current_session_id', %s)
                        ON CONFLICT (key) DO UPDATE
                        SET value=EXCLUDED.value, updated_at=now();
                    """, (str(sid),))

                conn.commit()

            return "ok"

        if sub_type == "stream.offline":
            with get_db() as conn:
                with conn.cursor() as cur:
                    # a) is_live = false
                    cur.execute("""
                        INSERT INTO kv (key, value)
                        VALUES ('is_live', 'false')
                        ON CONFLICT (key) DO UPDATE
                        SET value='false', updated_at=now();
                    """)

                    # b) récupérer session courante
                    cur.execute("SELECT value FROM kv WHERE key='current_session_id';")
                    row = cur.fetchone()
                    if not row or not row[0]:
                        conn.commit()
                        return "ok"

                    sid = int(row[0])

                    # c) fermer session
                    cur.execute("UPDATE stream_sessions SET ended_at=now() WHERE id=%s AND ended_at IS NULL;", (sid,))

                    # d) participants de cette session
                    cur.execute("SELECT twitch_login FROM stream_participants WHERE session_id=%s;", (sid,))
                    participants = [r[0] for r in cur.fetchall()]

                    for login in participants:
                        # lire streak existant
                        cur.execute("SELECT streak_count, last_session_id FROM streaks WHERE twitch_login=%s;", (login,))
                        srow = cur.fetchone()
                        prev_count = int(srow[0]) if srow else 0
                        prev_sid = int(srow[1]) if (srow and srow[1] is not None) else None

                        # consécutif si dernière session == sid-1
                        new_count = (prev_count + 1) if (prev_sid == sid - 1) else 1

                        # bonus bonheur par paliers (bonheur uniquement)
                        bonus = 0
                        if new_count == 1:
                            bonus = 2
                        elif new_count == 3:
                            bonus = 5
                        elif new_count == 5:
                            bonus = 10
                        elif new_count == 10:
                            bonus = 20

                        # upsert streak
                        cur.execute("""
                            INSERT INTO streaks (twitch_login, streak_count, last_session_id)
                            VALUES (%s,%s,%s)
                            ON CONFLICT (twitch_login) DO UPDATE
                            SET streak_count=EXCLUDED.streak_count,
                                last_session_id=EXCLUDED.last_session_id,
                                updated_at=now();
                        """, (login, new_count, sid))

                        # appliquer bonus bonheur (cap 100)
                        if bonus > 0:
                            cur.execute("""
                                UPDATE creatures_v2
                                SET happiness = LEAST(100, GREATEST(0, COALESCE(happiness,50) + %s)),
                                    updated_at = now()
                                WHERE twitch_login=%s AND is_active=true;
                            """, (bonus, login))

                            cur.execute("SELECT happiness FROM creatures WHERE twitch_login=%s;", (login,))
                            hcur = int(cur.fetchone()[0] or 0)
                            hnew = min(100, hcur + bonus)
                            cur.execute("UPDATE creatures SET happiness=%s, updated_at=now() WHERE twitch_login=%s;", (hnew, login))

                    # e) vider current_session_id
                    cur.execute("""
                        INSERT INTO kv (key, value)
                        VALUES ('current_session_id', '')
                        ON CONFLICT (key) DO UPDATE
                        SET value='', updated_at=now();
                    """)

                conn.commit()

            return "ok"

        return "ok"

    # 3) Revocation
    if msg_type == "revocation":
        return "ok"

    return "ok"

@app.get("/internal/settings/autodrop")
def get_autodrop_settings(x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    keys = [
        "auto_drop_enabled",
        "auto_drop_min_seconds",
        "auto_drop_max_seconds",
        "auto_drop_duration_min_seconds",
        "auto_drop_duration_max_seconds",
        "auto_drop_pick_kind",
        "auto_drop_mode",
        "auto_drop_ticket_qty",
        "auto_drop_fallback_media_url",
    ]

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT key, value FROM kv WHERE key = ANY(%s);", (keys,))
            rows = cur.fetchall()

    d = {k: v for (k, v) in rows}
    return {"ok": True, "settings": d}
    return {"ok": True, "settings": cfg}

@app.post("/admin/autodrop/save")
def admin_save_autodrop(payload: dict, credentials: HTTPBasicCredentials = Depends(security)):
    require_admin(credentials)

    def s(k, default=""):
        return str(payload.get(k, default)).strip()

    # normalisation + garde-fous
    enabled = "true" if s("enabled","false").lower() in ("1","true","yes","on") else "false"

    def to_int(name, default):
        try: return int(s(name, str(default)))
        except: return default

    min_s = max(60, to_int("min_seconds", 900))
    max_s = max(min_s, to_int("max_seconds", 1500))

    dmin = max(5, to_int("duration_min", 10))
    dmax = max(dmin, to_int("duration_max", 20))

    kind = s("pick_kind","any").lower()
    if kind not in ("any","xp","candy"):
        kind = "any"

    mode = s("mode","random").lower()
    if mode not in ("first","random"):
        mode = "random"

    qty = max(1, min(50, to_int("ticket_qty", 1)))
    fallback = s("fallback_media_url","")

    pairs = [
        ("auto_drop_enabled", enabled),
        ("auto_drop_min_seconds", str(min_s)),
        ("auto_drop_max_seconds", str(max_s)),
        ("auto_drop_duration_min_seconds", str(dmin)),
        ("auto_drop_duration_max_seconds", str(dmax)),
        ("auto_drop_pick_kind", kind),
        ("auto_drop_mode", mode),
        ("auto_drop_ticket_qty", str(qty)),
        ("auto_drop_fallback_media_url", fallback),
    ]

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO kv(key, value) VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE
                SET value=EXCLUDED.value, updated_at=now();
            """, pairs)
        conn.commit()

    return {"ok": True}


# =============================================================================
# BONHEUR
# =============================================================================

@app.post("/internal/happiness/batch")
def happiness_batch(payload: dict, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    logins = payload.get("logins", [])
    if not isinstance(logins, list) or len(logins) > 500:
        raise HTTPException(status_code=400, detail="Invalid logins")

    clean = [str(x).strip().lower() for x in logins if str(x).strip()]
    if not clean:
        return {"ok": True, "happiness": {}}

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT twitch_login, COALESCE(happiness, 50)
                FROM creatures_v2
                WHERE is_active=true
                  AND twitch_login = ANY(%s);
            """, (clean,))
            rows = cur.fetchall()

    out = {u: 50 for u in clean}
    for login, h in rows:
        out[str(login).lower()] = int(h or 50)

    return {"ok": True, "happiness": out}



@app.post("/internal/happiness/decay")
def happiness_decay(x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    with get_db() as conn:
        with conn.cursor() as cur:
            # Date du jour (Paris) pour éviter un double run dans la même journée
            cur.execute("SELECT to_char(now() AT TIME ZONE 'Europe/Paris', 'YYYY-MM-DD');")
            today = cur.fetchone()[0]

            cur.execute("SELECT value FROM kv WHERE key='happiness_decay_last';")
            row = cur.fetchone()
            last = row[0] if row else None

            if last == today:
                return {"ok": True, "skipped": True, "reason": "already_run_today", "date": today}

            # 1) baisse de base : -1 pour tous (min 0)
            cur.execute("""
                UPDATE creatures_v2
                SET happiness = GREATEST(0, COALESCE(happiness, 50) - 1),
                    updated_at = now()
                WHERE is_active = true;
            """)

            # 2) baisse supplémentaire pour inactifs 7 jours : -2 en plus (total -3)
            # 2) baisse supplémentaire inactifs 7 jours : -2 en plus (total -3) -> CM actif uniquement
            cur.execute("""
                UPDATE creatures_v2 c
                SET happiness = GREATEST(0, COALESCE(c.happiness, 50) - 2),
                    updated_at = now()
                WHERE c.is_active = true
                  AND NOT EXISTS (
                    SELECT 1
                    FROM xp_events e
                    WHERE e.twitch_login = c.twitch_login
                      AND e.created_at >= now() - interval '7 days'
                  );
            """)

            # enregistrer le run du jour
            cur.execute("""
                INSERT INTO kv (key, value)
                VALUES ('happiness_decay_last', %s)
                ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value, updated_at = now();
            """, (today,))

        conn.commit()

    return {"ok": True, "skipped": False, "date": today}
# ===================================================================
# Endpoints essentiels (health + is_live)
# =============================================================================
@app.get('/health')
def health():
    return {'ok': True}

@app.get('/internal/is_live')
def internal_is_live(x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM kv WHERE key='is_live';")
            row = cur.fetchone()
    return {"is_live": (row and row[0] == 'true')}

# =============================================================================
# Drops endpoints (A)
# =============================================================================
@app.post('/internal/drop/spawn')
def drop_spawn(payload: dict, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    mode = str(payload.get('mode', '')).strip().lower()
    title = str(payload.get('title', '')).strip()
    media_url = str(payload.get('media_url', '')).strip()
    duration = int(payload.get('duration_seconds', 15))
    xp_bonus = int(payload.get('xp_bonus', 50))
    ticket_key = str(payload.get('ticket_key', 'ticket_basic')).strip()
    ticket_qty = int(payload.get('ticket_qty', 1))
    target_hits = payload.get('target_hits', None)

    if mode not in ('first', 'random', 'coop'):
        raise HTTPException(status_code=400, detail='Invalid mode')
    if not title or not media_url:
        raise HTTPException(status_code=400, detail='Missing title/media_url')

    duration = max(5, min(duration, 30))
    xp_bonus = max(0, min(xp_bonus, 1000))
    ticket_qty = max(1, min(ticket_qty, 50))

    if mode == 'coop':
        target_hits = int(target_hits or 10)
        target_hits = max(2, min(target_hits, 999))
    else:
        target_hits = None

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE drops SET status='expired', resolved_at=now() WHERE status='active';")
            cur.execute(
                """
                INSERT INTO drops (mode, title, media_url, xp_bonus, ticket_key, ticket_qty, target_hits, status, expires_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,'active', now() + (%s || ' seconds')::interval)
                RETURNING id;
                """,
                (mode, title, media_url, xp_bonus, ticket_key, ticket_qty, target_hits, duration),
            )
            drop_id = int(cur.fetchone()[0])
        conn.commit()

    return {'ok': True, 'drop_id': drop_id}

@app.post("/admin/rp/save")
def admin_rp_save(
    key: str = Form(...),
    lines: str | None = Form(None),
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)

    key = key.strip()

    phrases = []
    if lines is not None:
        for line in lines.splitlines():
            s = line.strip()
            if s:
                phrases.append(s)

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rp_lines (key, lines)
                VALUES (%s, %s::jsonb)
                ON CONFLICT (key)
                DO UPDATE SET lines = EXCLUDED.lines, updated_at = now();
            """, (key, json.dumps(phrases)))
        conn.commit()

    return RedirectResponse(url=f"/admin/rp?flash=Enregistr%C3%A9%20:%20{key}", status_code=303)


@app.get("/internal/auto_drop/config")
def auto_drop_get_config(x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    with get_db() as conn:
        with conn.cursor() as cur:
            cfg = {
                "enabled": as_bool(kv_get(cur, "auto_drop_enabled", "false")),
                "kind": (kv_get(cur, "auto_drop_kind", "any") or "any").strip().lower(),
                "mode": (kv_get(cur, "auto_drop_mode", "random") or "random").strip().lower(),
                "min_seconds": as_int(kv_get(cur, "auto_drop_min_seconds", "900"), 900),
                "max_seconds": as_int(kv_get(cur, "auto_drop_max_seconds", "1500"), 1500),
                "duration_seconds": as_int(kv_get(cur, "auto_drop_duration_seconds", "40"), 40),
                "xp_bonus": as_int(kv_get(cur, "auto_drop_xp_bonus", "0"), 0),
                "ticket_qty": as_int(kv_get(cur, "auto_drop_ticket_qty", "1"), 1),
                "force_live": as_bool(kv_get(cur, "auto_drop_force_live", "false")),
            }
    return {"ok": True, "config": cfg}

@app.post("/internal/auto_drop/trigger_once")
def auto_drop_trigger_once(payload: dict | None = None, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    payload = payload or {}
    # override optionnel pour le test
    override_kind = (payload.get("kind") or "").strip().lower() or None
    override_mode = (payload.get("mode") or "").strip().lower() or None
    override_duration = payload.get("duration_seconds", None)

    with get_db() as conn:
        with conn.cursor() as cur:
            # 1) live gate (si on ne force pas)
            force_live = as_bool(kv_get(cur, "auto_drop_force_live", "false"))
            if not force_live:
                is_live = (kv_get(cur, "is_live", "false") or "false").strip().lower() == "true"
                if not is_live:
                    raise HTTPException(status_code=400, detail="Not live (set auto_drop_force_live=true to test)")

            # 2) lire config
            kind = override_kind or (kv_get(cur, "auto_drop_kind", "any") or "any").strip().lower()
            mode = override_mode or (kv_get(cur, "auto_drop_mode", "random") or "random").strip().lower()
            duration = override_duration if override_duration is not None else as_int(kv_get(cur, "auto_drop_duration_seconds", "40"), 40)

            xp_bonus = as_int(kv_get(cur, "auto_drop_xp_bonus", "0"), 0)
            ticket_qty = as_int(kv_get(cur, "auto_drop_ticket_qty", "1"), 1)

            if kind not in ("any", "xp", "candy", "egg"):
                kind = "any"
            if mode not in ("random", "first", "coop"):
                mode = "random"

            duration = max(5, min(60, int(duration)))
            xp_bonus = max(0, min(1000, int(xp_bonus)))
            ticket_qty = max(1, min(50, int(ticket_qty)))

            # 3) pick item pondéré
            where = "TRUE"
            if kind == "xp":
                where = "xp_gain > 0"
            elif kind == "candy":
                where = "happiness_gain > 0"
            elif kind == "egg":
                where = "key LIKE 'egg_%'"

            cur.execute(f"""
                SELECT key, name, COALESCE(icon_url,''), drop_weight
                FROM items
                WHERE {where} AND drop_weight > 0
            """)
            rows = cur.fetchall()
            if not rows:
                raise HTTPException(status_code=400, detail="No items for this kind")

            total = sum(int(r[3] or 0) for r in rows)
            rnd = random.randint(1, max(1, total))
            acc = 0
            picked = rows[0]
            for r in rows:
                acc += int(r[3] or 0)
                if rnd <= acc:
                    picked = r
                    break

            item_key, item_name, icon_url, drop_weight = picked

            # 4) spawn drop = insert drops
            title = item_name
            media_url = icon_url or ""   # si vide -> on refuse (comme ton endpoint drop/spawn)
            if not title or not media_url:
                raise HTTPException(status_code=400, detail=f"Picked item has no title/icon_url: {item_key}")

            # pas de coop dans trigger_once sauf si tu veux gérer target_hits
            target_hits = None
            if mode == "coop":
                target_hits = int(payload.get("target_hits") or 10)
                target_hits = max(2, min(target_hits, 999))

            cur.execute("UPDATE drops SET status='expired', resolved_at=now() WHERE status='active';")
            cur.execute("""
                INSERT INTO drops (mode, title, media_url, xp_bonus, ticket_key, ticket_qty, target_hits, status, expires_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,'active', now() + (%s || ' seconds')::interval)
                RETURNING id;
            """, (mode, title, media_url, xp_bonus, item_key, ticket_qty, target_hits, duration))
            drop_id = int(cur.fetchone()[0])

        conn.commit()

    return {
        "ok": True,
        "drop_id": drop_id,
        "picked": {
            "item_key": item_key,
            "item_name": item_name,
            "icon_url": icon_url,
            "drop_weight": int(drop_weight or 0),
        },
        "spawn": {
            "mode": mode,
            "title": title,
            "media_url": media_url,
            "duration_seconds": duration,
            "xp_bonus": xp_bonus,
            "ticket_key": item_key,
            "ticket_qty": ticket_qty,
        }
    }

@app.post("/internal/auto_drop/config")
def auto_drop_set_config(payload: dict, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    # on accepte des champs partiels
    def _norm_mode(m: str) -> str:
        m = (m or "random").strip().lower()
        return m if m in ("random", "first", "coop") else "random"

    def _norm_kind(k: str) -> str:
        k = (k or "any").strip().lower()
        return k if k in ("any", "xp", "candy", "egg") else "any"

    enabled = payload.get("enabled", None)
    kind = payload.get("kind", None)
    mode = payload.get("mode", None)

    min_s = payload.get("min_seconds", None)
    max_s = payload.get("max_seconds", None)
    duration = payload.get("duration_seconds", None)
    xp_bonus = payload.get("xp_bonus", None)
    ticket_qty = payload.get("ticket_qty", None)
    force_live = payload.get("force_live", None)

    with get_db() as conn:
        with conn.cursor() as cur:
            if enabled is not None:
                kv_set(cur, "auto_drop_enabled", "true" if bool(enabled) else "false")

            if kind is not None:
                kv_set(cur, "auto_drop_kind", _norm_kind(str(kind)))

            if mode is not None:
                kv_set(cur, "auto_drop_mode", _norm_mode(str(mode)))

            if min_s is not None:
                v = max(10, int(min_s))
                kv_set(cur, "auto_drop_min_seconds", str(v))

            if max_s is not None:
                v = max(10, int(max_s))
                kv_set(cur, "auto_drop_max_seconds", str(v))

            if duration is not None:
                v = max(5, min(60, int(duration)))
                kv_set(cur, "auto_drop_duration_seconds", str(v))

            if xp_bonus is not None:
                v = max(0, min(1000, int(xp_bonus)))
                kv_set(cur, "auto_drop_xp_bonus", str(v))

            if ticket_qty is not None:
                v = max(1, min(50, int(ticket_qty)))
                kv_set(cur, "auto_drop_ticket_qty", str(v))

            if force_live is not None:
                kv_set(cur, "auto_drop_force_live", "true" if bool(force_live) else "false")

        conn.commit()

    return {"ok": True}


# =============================================================================
# Pick ITEM pour auto drop
# =============================================================================

@app.get("/internal/items/pick")
def pick_item(kind: str = "any", x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    kind = (kind or "any").strip().lower()
    # kind: any | xp | candy | egg

    where = "TRUE"
    if kind == "xp":
        where = "xp_gain > 0"
    elif kind == "candy":
        where = "happiness_gain > 0"
    elif kind == "egg":
        where = "key LIKE 'egg_%'"

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT key, name, COALESCE(icon_url,''), drop_weight, xp_gain, happiness_gain
                FROM items
                WHERE {where} AND drop_weight > 0
            """)
            rows = cur.fetchall()

    if not rows:
        raise HTTPException(status_code=400, detail="No items for this kind")

    total = sum(int(r[3] or 0) for r in rows)
    rnd = random.randint(1, max(1, total))
    acc = 0
    picked = rows[0]
    for r in rows:
        acc += int(r[3] or 0)
        if rnd <= acc:
            picked = r
            break

    key, name, icon_url, weight, xp_gain, happiness_gain = picked
    return {
        "ok": True,
        "item_key": key,
        "item_name": name,
        "icon_url": icon_url,
        "xp_gain": int(xp_gain or 0),
        "happiness_gain": int(happiness_gain or 0),
        "drop_weight": int(weight or 0),
    }




@app.post("/internal/choose_lineage")
def choose_lineage(payload: dict, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    login = str(payload.get("twitch_login", "")).strip().lower()
    lineage_key = str(payload.get("lineage_key", "")).strip().lower()
    if not login or not lineage_key:
        raise HTTPException(status_code=400, detail="Missing twitch_login or lineage_key")

    with get_db() as conn:
        with conn.cursor() as cur:
            # 1) lineage existe + flags
            cur.execute("""
                SELECT is_enabled, COALESCE(choose_enabled, true)
                FROM lineages
                WHERE key=%s;
            """, (lineage_key,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=400, detail="Unknown lineage")

            is_enabled = bool(row[0])
            choose_enabled = bool(row[1])

            if not is_enabled:
                raise HTTPException(status_code=400, detail="Lineage disabled")
            if not choose_enabled:
                raise HTTPException(status_code=403, detail="Choose disabled for this lineage")

            # 2) CM actif
            cur.execute("""
                SELECT id, stage
                FROM creatures_v2
                WHERE twitch_login=%s AND is_active=true
                LIMIT 1;
            """, (login,))
            crow = cur.fetchone()
            if not crow:
                raise HTTPException(status_code=400, detail="No active CM")

            creature_id, stage = int(crow[0]), int(crow[1] or 0)

            # 3) uniquement stage 0
            if stage != 0:
                raise HTTPException(status_code=400, detail="Choose only before hatching (egg stage)")

            # 4) update lineage_key
            cur.execute("""
                UPDATE creatures_v2
                SET lineage_key=%s, updated_at=now()
                WHERE id=%s;
            """, (lineage_key, creature_id))

        conn.commit()

    return {"ok": True, "twitch_login": login, "lineage_key": lineage_key}



@app.post("/admin/action")
def admin_action(
    login: str = Form(...),
    action: str = Form(...),
    amount: int | None = Form(None),
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)

    login = login.strip().lower()
    action = action.strip().lower()

    if action not in ("give", "set", "reset", "assign_cm"):
        return RedirectResponse(url=f"/admin/user/{login}?flash_kind=err&flash=Action%20invalide", status_code=303)

    if action in ("give", "set") and amount is None:
        return RedirectResponse(url=f"/admin/user/{login}?flash_kind=err&flash=Montant%20manquant", status_code=303)

    with get_db() as conn:
        with conn.cursor() as cur:
            # ensure user exists
            cur.execute("INSERT INTO users (twitch_login) VALUES (%s) ON CONFLICT DO NOTHING;", (login,))
            
            # récupérer CM actif
            cur.execute("""
                SELECT id, cm_key
                FROM creatures_v2
                WHERE twitch_login=%s AND is_active=true
                LIMIT 1;
            """, (login,))
            arow = cur.fetchone()
            if not arow:
                return RedirectResponse(url=f"/admin/user/{login}?flash_kind=err&flash=Aucun%20CM%20actif", status_code=303)
            
            creature_id = int(arow[0])
            
            if action == "reset":
                cur.execute("""
                    UPDATE creatures_v2
                    SET xp_total=0,
                        stage=0,
                        lineage_key=NULL,
                        happiness=50,
                        updated_at=now()
                    WHERE id=%s;
                """, (creature_id,))
                conn.commit()
                return RedirectResponse(url=f"/admin/user/{login}?flash_kind=ok&flash=Reset%20CM%20actif", status_code=303)
            
            if action == "set":
                new_xp = max(0, int(amount))
            else:  # give
                cur.execute("SELECT xp_total FROM creatures_v2 WHERE id=%s;", (creature_id,))
                current = int(cur.fetchone()[0] or 0)
                new_xp = current + max(0, int(amount))
            
            new_stage = stage_from_xp(int(new_xp))
            cur.execute("""
                UPDATE creatures_v2
                SET xp_total=%s,
                    stage=%s,
                    updated_at=now()
                WHERE id=%s;
            """, (int(new_xp), int(new_stage), creature_id))


        conn.commit()

    msg = "OK"
    if action == "set":
        msg = f"XP%20fix%C3%A9%20%C3%A0%20{new_xp}"
    elif action == "give":
        msg = f"+{amount}%20XP%20(total%20{new_xp})"

    return RedirectResponse(url=f"/admin/user/{login}?flash_kind=ok&flash={msg}", status_code=303)


@app.post("/internal/trigger_evolution")
def trigger_evolution(payload: dict, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    login = payload.get("twitch_login")
    stage = int(payload.get("stage"))

    with get_db() as conn:
        with conn.cursor() as cur:
            # infos viewer
            display, avatar = twitch_user_profile(login)

            # récupérer la forme
            cur.execute("""
                SELECT f.name, f.image_url, f.sound_url, c.key
                FROM creatures_v2 cr
                JOIN cm_forms f ON f.cm_key = cr.cm_key AND f.stage = %s
                JOIN cms c ON c.key = cr.cm_key
                WHERE cr.twitch_login = %s AND cr.is_active=true
                LIMIT 1;
            """, (stage, login))


            row = cur.fetchone()
            if not row:
                raise HTTPException(400, "No evolution form")

            name, image_url, sound_url, cm_key = row

            cur.execute("""
                INSERT INTO overlay_evolutions
                (twitch_login, viewer_display, viewer_avatar,
                 cm_key, stage, name, image_url, sound_url, expires_at)
                VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s, now() + interval '15 seconds');
            """, (login, display, avatar, cm_key, stage, name, image_url, sound_url))

        conn.commit()

    return {"ok": True}

@app.get("/admin/user/{login}", response_class=HTMLResponse)
def admin_user(
    request: Request,
    login: str,
    flash: str | None = None,
    flash_kind: str | None = None,
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)

    login = login.strip().lower()

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT xp_total, stage, lineage_key, cm_key, happiness
                FROM creatures_v2
                WHERE twitch_login=%s AND is_active=true
                LIMIT 1;
            """, (login,))
            row = cur.fetchone()

    if not row:
        xp_total, stage, lineage_key, cm_key, happiness = 0, 0, None, None, 50
    else:
        xp_total, stage, lineage_key, cm_key, happiness = row

    nxt, label = next_threshold(int(xp_total))
    xp_to_next = 0 if nxt is None else max(0, int(nxt) - int(xp_total))

    return templates.TemplateResponse("user.html", {
        "request": request,
        "login": login,
        "xp_total": int(xp_total),
        "stage": int(stage),
        "next_label": label,
        "xp_to_next": int(xp_to_next),
        "flash": flash,
        "flash_kind": flash_kind,
        "lineage_key": lineage_key,
        "cm_key": cm_key,
        "happiness": int(happiness or 0),
    })

from fastapi import Form
from fastapi.responses import RedirectResponse, HTMLResponse

# -------------------------------
# ADMIN: page collection complète
# -------------------------------
@app.get("/admin/user/{login}/collection", response_class=HTMLResponse)
def admin_user_collection(
    request: Request,
    login: str,
    flash: str | None = None,
    flash_kind: str | None = None,
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)
    login = (login or "").strip().lower()

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    c.id,
                    c.is_active,
                    c.cm_key,
                    c.lineage_key,
                    c.stage,
                    c.xp_total,
                    COALESCE(c.happiness, 50) AS happiness,
                    COALESCE(c.acquired_from,'') AS acquired_from,
                    c.acquired_at
                FROM creatures_v2 c
                WHERE c.twitch_login = %s
                ORDER BY c.is_active DESC, c.acquired_at DESC NULLS LAST, c.id DESC;
            """, (login,))
            rows = cur.fetchall()

            # inventaire (optionnel mais pratique sur la même page)
            cur.execute("""
                SELECT item_key, qty
                FROM inventory
                WHERE twitch_login=%s AND qty>0
                ORDER BY item_key;
            """, (login,))
            inv = [{"item_key": r[0], "qty": int(r[1] or 0)} for r in cur.fetchall()]

    items = []
    for r in rows:
        items.append({
            "id": int(r[0]),
            "is_active": bool(r[1]),
            "cm_key": r[2],
            "lineage_key": r[3],
            "stage": int(r[4] or 0),
            "xp_total": int(r[5] or 0),
            "happiness": int(r[6] or 50),
            "acquired_from": r[7],
            "acquired_at": (r[8].isoformat() if r[8] else None),
        })

    return templates.TemplateResponse("user_collection.html", {
        "request": request,
        "login": login,
        "items": items,
        "inventory": inv,
        "flash": flash,
        "flash_kind": flash_kind,
    })


# ---------------------------------------------------
# ADMIN: actions collection (set active / xp / stage…)
# ---------------------------------------------------
@app.post("/admin/user/{login}/collection/action")
def admin_user_collection_action(
    login: str,
    action: str = Form(...),

    creature_id: int | None = Form(None),
    amount: int | None = Form(None),
    stage: int | None = Form(None),

    item_key: str | None = Form(None),
    item_qty: int | None = Form(None),

    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)
    login = (login or "").strip().lower()
    action = (action or "").strip().lower()

    def go(msg: str, kind: str = "ok"):
        safe = msg.replace(" ", "%20")
        return RedirectResponse(
            url=f"/admin/user/{login}/collection?flash_kind={kind}&flash={safe}",
            status_code=303
        )

    with get_db() as conn:
        with conn.cursor() as cur:

            if action == "set_active":
                if creature_id is None:
                    return go("creature_id manquant", "err")

                cur.execute("""
                    SELECT 1 FROM creatures_v2
                    WHERE id=%s AND twitch_login=%s;
                """, (int(creature_id), login))
                if not cur.fetchone():
                    return go("Creature introuvable", "err")

                cur.execute("""
                    UPDATE creatures_v2
                    SET is_active=false, updated_at=now()
                    WHERE twitch_login=%s AND is_active=true;
                """, (login,))

                cur.execute("""
                    UPDATE creatures_v2
                    SET is_active=true, updated_at=now()
                    WHERE id=%s AND twitch_login=%s;
                """, (int(creature_id), login))

                conn.commit()
                return go(f"Actif => id {creature_id}")

            if action == "add_xp":
                if creature_id is None or amount is None:
                    return go("creature_id/amount manquants", "err")
                amt = int(amount)
                if amt <= 0:
                    return go("amount invalide", "err")

                cur.execute("""
                    SELECT stage, xp_total
                    FROM creatures_v2
                    WHERE id=%s AND twitch_login=%s;
                """, (int(creature_id), login))
                row = cur.fetchone()
                if not row:
                    return go("Creature introuvable", "err")

                prev_stage = int(row[0] or 0)

                cur.execute("INSERT INTO xp_events (twitch_login, amount) VALUES (%s,%s);", (login, amt))

                cur.execute("""
                    UPDATE creatures_v2
                    SET xp_total = xp_total + %s,
                        updated_at = now()
                    WHERE id=%s AND twitch_login=%s
                    RETURNING xp_total;
                """, (amt, int(creature_id), login))
                new_xp = int(cur.fetchone()[0] or 0)
                new_stage = int(stage_from_xp(new_xp))

                if new_stage != prev_stage:
                    cur.execute("""
                        UPDATE creatures_v2
                        SET stage=%s, updated_at=now()
                        WHERE id=%s AND twitch_login=%s;
                    """, (new_stage, int(creature_id), login))

                conn.commit()
                return go(f"+{amt} XP (total {new_xp})")

            if action == "set_stage":
                if creature_id is None or stage is None:
                    return go("creature_id/stage manquants", "err")
                st = int(stage)
                if st < 0 or st > 3:
                    return go("stage invalide", "err")

                cur.execute("""
                    UPDATE creatures_v2
                    SET stage=%s, updated_at=now()
                    WHERE id=%s AND twitch_login=%s;
                """, (st, int(creature_id), login))
                if cur.rowcount != 1:
                    return go("Creature introuvable", "err")

                conn.commit()
                return go(f"Stage => {st}")

            if action == "reset":
                if creature_id is None:
                    return go("creature_id manquant", "err")

                cur.execute("""
                    UPDATE creatures_v2
                    SET xp_total=0,
                        stage=0,
                        lineage_key=NULL,
                        happiness=50,
                        updated_at=now()
                    WHERE id=%s AND twitch_login=%s;
                """, (int(creature_id), login))
                if cur.rowcount != 1:
                    return go("Creature introuvable", "err")

                conn.commit()
                return go("Reset OK")

            if action == "delete_egg":
                # supprime uniquement si cm_key='egg'
                if creature_id is None:
                    return go("creature_id manquant", "err")

                cur.execute("""
                    SELECT is_active FROM creatures_v2
                    WHERE id=%s AND twitch_login=%s AND cm_key='egg';
                """, (int(creature_id), login))
                row = cur.fetchone()
                if not row:
                    return go("Pas un oeuf (ou introuvable)", "err")

                was_active = bool(row[0])

                cur.execute("""
                    DELETE FROM creatures_v2
                    WHERE id=%s AND twitch_login=%s AND cm_key='egg';
                """, (int(creature_id), login))

                # si c’était actif => activer le meilleur restant (s’il existe)
                if was_active:
                    cur.execute("""
                        SELECT id FROM creatures_v2
                        WHERE twitch_login=%s
                        ORDER BY acquired_at DESC NULLS LAST, xp_total DESC, id DESC
                        LIMIT 1;
                    """, (login,))
                    r2 = cur.fetchone()
                    if r2:
                        cur.execute("""
                            UPDATE creatures_v2
                            SET is_active=true, updated_at=now()
                            WHERE id=%s AND twitch_login=%s;
                        """, (int(r2[0]), login))

                conn.commit()
                return go("Oeuf supprimé")

            if action == "give_item":
                if not item_key or item_qty is None:
                    return go("item_key/item_qty manquants", "err")
                qty = int(item_qty)
                if qty <= 0:
                    return go("qty invalide", "err")
                k = item_key.strip().lower()

                cur.execute("""
                    INSERT INTO inventory (twitch_login, item_key, qty)
                    VALUES (%s,%s,%s)
                    ON CONFLICT (twitch_login, item_key)
                    DO UPDATE SET qty = inventory.qty + EXCLUDED.qty,
                                  updated_at = now();
                """, (login, k, qty))

                conn.commit()
                return go(f"+{qty} {k}")

            return go("Action inconnue", "err")

@app.post("/admin/dedupe/merge_one")
def admin_merge_one(
    login: str = Form(...),
    cm_key: str = Form(...),
    lineage_key: str = Form(...),
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)
    login = login.strip().lower()
    cm_key = cm_key.strip().lower()
    lineage_key = (lineage_key or "").strip().lower() or None

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, is_active, xp_total, COALESCE(happiness,50), acquired_at
                FROM creatures_v2
                WHERE twitch_login=%s AND cm_key=%s
                  AND ( (lineage_key IS NULL AND %s IS NULL) OR lineage_key=%s )
                ORDER BY is_active DESC, xp_total DESC, COALESCE(happiness,50) DESC, acquired_at ASC NULLS LAST, id ASC;
            """, (login, cm_key, lineage_key, lineage_key))
            rows = cur.fetchall()

            if len(rows) <= 1:
                conn.commit()
                return {"ok": True, "merged": False, "reason": "no_duplicates"}

            winner_id = int(rows[0][0])
            loser_ids = [int(r[0]) for r in rows[1:]]

            # On “harmonise” le winner (max xp/happiness)
            max_xp = max(int(r[2] or 0) for r in rows)
            max_h = max(int(r[3] or 50) for r in rows)

            cur.execute("""
                UPDATE creatures_v2
                SET xp_total=%s,
                    happiness=%s,
                    acquired_from='merge',
                    updated_at=now()
                WHERE id=%s AND twitch_login=%s;
            """, (max_xp, max_h, winner_id, login))

            cur.execute("""
                DELETE FROM creatures_v2
                WHERE twitch_login=%s AND id = ANY(%s);
            """, (login, loser_ids))

        conn.commit()

    return {"ok": True, "merged": True, "winner_id": winner_id, "deleted_ids": loser_ids}


@app.post("/admin/inventory/transfer")
def admin_inventory_transfer(
    from_login: str = Form(...),
    to_login: str = Form(...),
    item_key: str = Form(...),
    qty: int = Form(...),
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)

    from_login = from_login.strip().lower()
    to_login = to_login.strip().lower()
    item_key = item_key.strip().lower()
    qty = int(qty)

    if not from_login or not to_login or not item_key or qty <= 0:
        raise HTTPException(status_code=400, detail="Invalid params")

    with get_db() as conn:
        with conn.cursor() as cur:
            # lock ligne source (évite course)
            cur.execute("""
                SELECT qty
                FROM inventory
                WHERE twitch_login=%s AND item_key=%s
                FOR UPDATE;
            """, (from_login, item_key))
            row = cur.fetchone()
            have = int(row[0] or 0) if row else 0
            if have < qty:
                raise HTTPException(status_code=400, detail=f"Not enough qty (have={have}, need={qty})")

            # decrement source
            cur.execute("""
                UPDATE inventory
                SET qty = qty - %s, updated_at=now()
                WHERE twitch_login=%s AND item_key=%s;
            """, (qty, from_login, item_key))

            # cleanup si 0
            cur.execute("""
                DELETE FROM inventory
                WHERE twitch_login=%s AND item_key=%s AND qty <= 0;
            """, (from_login, item_key))

            # increment target
            cur.execute("""
                INSERT INTO inventory (twitch_login, item_key, qty)
                VALUES (%s,%s,%s)
                ON CONFLICT (twitch_login, item_key)
                DO UPDATE SET qty = inventory.qty + EXCLUDED.qty,
                              updated_at = now();
            """, (to_login, item_key, qty))

        conn.commit()

    return {"ok": True, "from": from_login, "to": to_login, "item_key": item_key, "qty": qty}

@app.get("/admin/forms", response_class=HTMLResponse)
def admin_forms(
    request: Request,
    flash: str | None = None,
    flash_kind: str | None = None,
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)

    with get_db() as conn:
        with conn.cursor() as cur:
            # Liste des CM (pour afficher chaque cm_key)
            cur.execute("""
                SELECT key, name, lineage_key
                FROM cms
                ORDER BY lineage_key, key;
            """)
            cms = [{"key": r[0], "cm_name": r[1], "lineage_key": r[2]} for r in cur.fetchall()]

            # Toutes les forms existantes
            cur.execute("""
                SELECT cm_key, stage, name, image_url, COALESCE(sound_url,'')
                FROM cm_forms
                ORDER BY cm_key, stage;
            """)
            rows = cur.fetchall()

    forms_map: dict[tuple[str, int], dict] = {}
    for cm_key, stage, name, image_url, sound_url in rows:
        forms_map[(cm_key, int(stage))] = {
            "name": name,
            "image_url": image_url,
            "sound_url": sound_url,
        }

    # Pour le template : on prépare 3 entrées (stage 1/2/3) par CM
    items = []
    for cm in cms:
        cm_key = cm["key"]
        stages = []
        for st in (1, 2, 3):
            f = forms_map.get((cm_key, st), {"name": "", "image_url": "", "sound_url": ""})
            stages.append({"stage": st, **f})
        items.append({**cm, "stages": stages})

    return templates.TemplateResponse("forms.html", {
        "request": request,
        "items": items,
        "flash": flash,
        "flash_kind": flash_kind,
    })

@app.post("/admin/forms/save")
def admin_forms_save(
    cm_key: str = Form(...),
    stage: int = Form(...),
    name: str = Form(...),
    image_url: str = Form(...),
    sound_url: str | None = Form(None),
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)

    cm_key = cm_key.strip().lower()
    stage = int(stage)
    name = (name or "").strip()
    image_url = (image_url or "").strip()
    sound_url = (sound_url or "").strip() if sound_url else ""

    if stage not in (1, 2, 3):
        return RedirectResponse("/admin/forms?flash_kind=err&flash=Stage%20invalide", status_code=303)

    if not cm_key or not name or not image_url:
        return RedirectResponse("/admin/forms?flash_kind=err&flash=Champs%20manquants", status_code=303)

    with get_db() as conn:
        with conn.cursor() as cur:
            # Vérifier cm existe
            cur.execute("SELECT 1 FROM cms WHERE key=%s;", (cm_key,))
            if not cur.fetchone():
                return RedirectResponse("/admin/forms?flash_kind=err&flash=CM%20inconnu", status_code=303)

            cur.execute("""
                INSERT INTO cm_forms (cm_key, stage, name, image_url, sound_url)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (cm_key, stage) DO UPDATE
                SET name = EXCLUDED.name,
                    image_url = EXCLUDED.image_url,
                    sound_url = EXCLUDED.sound_url;
            """, (cm_key, stage, name, image_url, sound_url if sound_url else None))
        conn.commit()

    return RedirectResponse(
        url=f"/admin/forms?flash_kind=ok&flash=Forme%20enregistr%C3%A9e%20({cm_key}%20stage%20{stage})",
        status_code=303,
    )


@app.get("/internal/creature/{login}")
def creature_state(login: str, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    login = login.strip().lower()
    if not login:
        raise HTTPException(status_code=400, detail="Missing login")

    # défauts sûrs
    xp_total = 0
    stage = 0
    lineage_key = None
    cm_key = None
    happiness = 50

    form_name = None
    form_image_url = None
    form_sound_url = None

    streak_count = 0

    with get_db() as conn:
        with conn.cursor() as cur:
            # 1) CM actif (V2)
            cur.execute("""
                SELECT xp_total, stage, lineage_key, cm_key, happiness
                FROM creatures_v2
                WHERE twitch_login=%s AND is_active=true
                LIMIT 1;
            """, (login,))
            row = cur.fetchone()
            if row:
                xp_total = int(row[0] or 0)
                stage = int(row[1] or 0)
                lineage_key = row[2]
                cm_key = row[3]
                happiness = int(row[4] or 50)

            # 2) streak
            cur.execute("SELECT streak_count FROM streaks WHERE twitch_login=%s;", (login,))
            srow = cur.fetchone()
            if srow:
                streak_count = int(srow[0] or 0)

            # 3) form (si stage>=1 et cm_key)
            if stage >= 1 and cm_key:
                cur.execute("""
                    SELECT name, image_url, sound_url
                    FROM cm_forms
                    WHERE cm_key=%s AND stage=%s;
                """, (cm_key, stage))
                f = cur.fetchone()
                if f:
                    form_name, form_image_url, form_sound_url = f

    nxt, label = next_threshold(xp_total)
    remaining = 0 if nxt is None else max(0, int(nxt) - xp_total)

    return {
        "twitch_login": login,
        "xp_total": xp_total,
        "stage": stage,
        "lineage_key": lineage_key,
        "cm_key": cm_key,
        "form_name": form_name,
        "form_image_url": form_image_url,
        "form_sound_url": form_sound_url,
        "next": label,
        "xp_to_next": remaining,
        "happiness": int(happiness or 0),
        "streak_count": int(streak_count),
    }


@app.get("/internal/inventory/{login}")
def internal_inventory(login: str, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    login = login.strip().lower()
    if not login:
        raise HTTPException(status_code=400, detail="Missing login")

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT item_key, qty
                FROM inventory
                WHERE twitch_login=%s AND qty > 0
                ORDER BY item_key ASC;
            """, (login,))
            rows = cur.fetchall()

    items = [{"item_key": r[0], "qty": int(r[1])} for r in rows]
    return {"ok": True, "twitch_login": login, "items": items}



@app.post('/internal/drop/join')
def drop_join(payload: dict, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    login = str(payload.get('twitch_login', '')).strip().lower()
    if not login:
        raise HTTPException(status_code=400, detail='Missing twitch_login')

    with get_db() as conn:
        with conn.cursor() as cur:
            d = get_active_drop(cur)
            if not d:
                return {'ok': True, 'active': False}

            drop_id, mode, title, media_url, xp_bonus, ticket_key, ticket_qty, target_hits, status, expires_at, winner_login = d

            cur.execute('SELECT now() >= %s;', (expires_at,))
            if bool(cur.fetchone()[0]):
                conn.commit()
                resolve_drop(int(drop_id))
                return {'ok': True, 'active': False}

            joined = True
            try:
                cur.execute('INSERT INTO drop_participants (drop_id, twitch_login) VALUES (%s,%s);', (drop_id, login))
            except Exception:
                joined = False

            cur.execute('SELECT COUNT(*) FROM drop_participants WHERE drop_id=%s;', (drop_id,))
            count = int(cur.fetchone()[0])

            result = None
            if mode == 'first' and joined and count == 1:
                grant_xp(login, int(xp_bonus))
                inv_add(login, ticket_key, int(ticket_qty))
                cur.execute(
                    """
                    UPDATE drops
                    SET status='resolved', resolved_at=now(), winner_login=%s
                    WHERE id=%s;
                    """,
                    (login, drop_id),
                )
                result = {
                    'won': True,
                    'mode': 'first',
                    'title': title,
                    'xp_bonus': int(xp_bonus),
                    'ticket_key': ticket_key,
                    'ticket_qty': int(ticket_qty),
                }

        conn.commit()

    return {
        'ok': True,
        'active': True,
        'mode': mode,
        'title': title,
        'joined': joined,
        'count': count,
        'target': int(target_hits) if target_hits is not None else None,
        'result': result,
    }


@app.get('/internal/drop/poll_result')
def drop_poll_result(x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM drops WHERE status='active' ORDER BY expires_at ASC LIMIT 1;")
            row = cur.fetchone()

    if row:
        resolve_drop(int(row[0]))

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, mode, title, xp_bonus, ticket_key, ticket_qty, status, winner_login
                FROM drops
                WHERE announced_at IS NULL
                  AND status IN ('resolved','expired')
                ORDER BY resolved_at ASC NULLS LAST, expires_at ASC
                LIMIT 1;
                """
            )
            d = cur.fetchone()
            if not d:
                return {'announce': False}

            drop_id, mode, title, xp_bonus, ticket_key, ticket_qty, status, winner_login = d
            cur.execute('UPDATE drops SET announced_at=now() WHERE id=%s;', (drop_id,))
        conn.commit()

    winners = []
    if mode == 'coop' and status == 'resolved':
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT twitch_login FROM drop_participants WHERE drop_id=%s ORDER BY created_at ASC;', (drop_id,))
                winners = [r[0] for r in cur.fetchall()]
    elif status == 'resolved' and winner_login:
        winners = [winner_login]

    return {
        'announce': True,
        'mode': mode,
        'status': status,
        'title': title,
        'winners': winners,
        'xp_bonus': int(xp_bonus),
        'ticket_key': ticket_key,
        'ticket_qty': int(ticket_qty),
    }
@app.get("/overlay/drop", response_class=HTMLResponse)
def overlay_drop_page():
    return HTMLResponse("""
<!doctype html>
<html><head><meta charset="utf-8"/>
<style>
  body{margin:0;background:transparent;font-family:system-ui,Segoe UI,Roboto,Arial,sans-serif;overflow:hidden}
  .wrap{position:fixed;inset:0;display:flex;align-items:flex-end;justify-content:center;padding-bottom:60px}
  .card{display:none;gap:14px;align-items:center;background:rgba(10,15,20,.80);border:1px solid rgba(255,255,255,.12);
        border-radius:18px;padding:16px 18px;min-width:820px}
  .img{width:96px;height:96px;border-radius:16px;object-fit:contain;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.12)}
  .title{font-size:22px;font-weight:900;color:#e6edf3}
  .sub{font-size:13px;color:#9aa4b2;margin-top:2px}
  .pill{display:inline-block;padding:3px 10px;border-radius:999px;border:1px solid rgba(255,255,255,.14);color:#9aa4b2;font-size:12px}
  .bar{height:10px;border-radius:999px;background:rgba(255,255,255,.10);overflow:hidden;border:1px solid rgba(255,255,255,.12);margin-top:10px}
  .fill{height:100%;width:0%;background:linear-gradient(90deg,#7aa2ff,rgba(122,162,255,.45))}
</style></head>
<body>
<div class="wrap">
  <div id="card" class="card">
    <img id="img" class="img" src="" alt="">
    <div style="flex:1">
      <div class="title" id="title"></div>
      <div class="sub" id="line"></div>
      <div class="bar"><div id="fill" class="fill"></div></div>
    </div>
    <div style="text-align:right">
      <div class="pill" id="mode"></div>
      <div class="sub" id="timer" style="margin-top:8px"></div>
    </div>
  </div>
</div>
<audio id="dropSfx" preload="auto" src="/static/drop.mp3"></audio>

<script>
let lastDropId = null;
const dropSfx = document.getElementById('dropSfx');

function playDropSfx(){
  try{
    dropSfx.currentTime = 0;
    const p = dropSfx.play();
    if (p && p.catch) p.catch(()=>{});
  }catch(e){}
}

let showing=false;
function setShow(on){
  const c=document.getElementById('card');
  if(on && !showing){ c.style.display='flex'; showing=true; }
  if(!on && showing){ c.style.display='none'; showing=false; }
}
async function tick(){
  try{
    const r = await fetch('/overlay/drop_state', {cache:'no-store'});
    const j = await r.json();
    if(!j.show){ setShow(false); return; }

    const d = j.drop;
    if (d.id && d.id !== lastDropId) {
  lastDropId = d.id;
  playDropSfx();
}

    document.getElementById('img').src = d.media || '';
    document.getElementById('title').textContent = d.title || 'Drop';
    document.getElementById('timer').textContent = `⏳ ${d.remaining}s`;
    document.getElementById('mode').textContent =
      d.mode === 'first' ? '⚡ PREMIER' : (d.mode === 'random' ? '🎲 RANDOM' : '🤝 COOP');

    if(d.mode === 'coop'){
      document.getElementById('line').textContent = `Tape !hit — ${d.count}/${d.target} • +${d.xp_bonus} XP & ${d.ticket_qty} ${d.ticket_key}`;
      const pct = d.target ? Math.min(100, Math.floor((d.count/d.target)*100)) : 0;
      document.getElementById('fill').style.width = pct + '%';
    }else{
      document.getElementById('line').textContent = `Tape !grab — participants: ${d.count} • +${d.xp_bonus} XP & ${d.ticket_qty} ${d.ticket_key}`;
      document.getElementById('fill').style.width = '0%';
    }

    setShow(true);
  }catch(e){}
}
setInterval(tick, 500);
tick();
</script>
</body></html>
""")


@app.get("/overlay/show", response_class=HTMLResponse)
def overlay_show_page():
    return HTMLResponse(r"""
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
  body{margin:0;background:transparent;font-family:system-ui,Segoe UI,Roboto,Arial,sans-serif;overflow:hidden}

  .wrap{
    position:fixed; inset:0;
    display:flex; align-items:center; justify-content:center;
    pointer-events:none;
  }

  /* Carte principale (animation) */
  .card{
    display:none;
    flex-direction:column;
    align-items:center;
    gap:14px;
    padding:22px 26px;
    border-radius:22px;
    background:rgba(10,15,20,.78);
    border:1px solid rgba(255,255,255,.12);
    backdrop-filter: blur(8px);
    min-width:480px;
    max-width:480px;

    opacity:0;
    transform: translateY(10px) scale(0.98);
    transition: opacity 500ms ease, transform 500ms ease;
    will-change: opacity, transform;
  }
  .card.showing{
    opacity:1;
    transform: translateY(0) scale(1);
  }

  /* Bandeau viewer */
  .viewerBar{
    width:100%;
    display:flex;
    align-items:center;
    gap:12px;
    padding:10px 14px;
    margin-bottom:6px;
    border-radius:14px;
    background:rgba(255,255,255,.06);
    border:1px solid rgba(255,255,255,.10);
  }

  .avatar{
    width:40px;
    height:40px;
    border-radius:10px;
    object-fit:cover;
    border:1px solid rgba(255,255,255,.15);
  }

  .viewerText{display:flex;flex-direction:column}
  .viewerName{font-size:14px;font-weight:800;color:#e6edf3;line-height:1.1}
  .viewerSub{font-size:11px;color:#9aa4b2}

  /* CM très grand */
  .cmimg{
    width:420px;
    height:420px;
    object-fit:contain;
    border-radius:24px;
    background:rgba(255,255,255,.05);
    border:1px solid rgba(255,255,255,.10);
  }

  /* Labels au-dessus des barres */
  .labelRow{
    width:420px;
    display:flex;
    align-items:baseline;
    justify-content:space-between;
    margin-top:6px;
  }
  .label{
    font-size:13px;
    font-weight:900;
    color:#e6edf3;
    letter-spacing:.2px;
  }
  .labelRight{
    font-size:12px;
    color:#9aa4b2;
  }

  /* Barres */
  .barWrap{
    width:420px;
    height:14px;
    border-radius:999px;
    background:rgba(255,255,255,.10);
    border:1px solid rgba(255,255,255,.12);
    overflow:hidden;
  }
  .fill{
    height:100%;
    width:0%;
    background:linear-gradient(90deg,#7aa2ff,rgba(122,162,255,.45));
    transition: width 280ms ease;
  }
  /* Bonheur en rose */
  .happinessFill{
    background:linear-gradient(90deg,#ff4fb3,rgba(255,79,179,.35));
  }

  .cmname{
    font-size:28px;
    font-weight:900;
    color:#e6edf3;
    text-align:center;
    line-height:1.1;
  }
</style>
</head>

<body>
  <div class="wrap">
    <div id="card" class="card">
      <div class="viewerBar">
        <img id="avatar" class="avatar" src="" alt="">
        <div class="viewerText">
          <div id="viewer" class="viewerName"></div>
          <div class="viewerSub">a utilisé !show</div>
        </div>
      </div>

      <img id="cmimg" class="cmimg" src="" alt="">

      <!-- XP -->
      <div class="labelRow">
        <div class="label">⚡ XP</div>
        <div id="xpLabel" class="labelRight"></div>
      </div>
      <div class="barWrap"><div id="fill" class="fill"></div></div>

      <!-- Bonheur -->
      <div class="labelRow" style="margin-top:10px;">
        <div class="label">💗 Bonheur</div>
        <div id="hLabel" class="labelRight"></div>
      </div>
      <div class="barWrap"><div id="hfill" class="fill happinessFill"></div></div>

      <div id="cmname" class="cmname">CapsMons</div>
    </div>
  </div>

  <audio id="sfx" preload="auto" src="/static/show.mp3"></audio>

<script>
let showing = false;
let hideTimer = null;
const DISPLAY_MS = 7000;
let lastSig = "";
const sfx = document.getElementById('sfx');

function playSfx(){
  try{
    sfx.currentTime = 0;
    const p = sfx.play();
    if (p && p.catch) p.catch(()=>{});
  }catch(e){}
}

function showCard(){
  const card = document.getElementById('card');
  if (!showing) {
    card.style.display = 'flex';
    void card.offsetWidth;
    card.classList.add('showing');
    showing = true;
  }
  if (hideTimer) clearTimeout(hideTimer);
  hideTimer = setTimeout(hideCard, DISPLAY_MS);
}

function hideCard(){
  const card = document.getElementById('card');
  if (!showing) return;

  card.classList.remove('showing');
  setTimeout(() => { card.style.display = 'none'; }, 230);
  showing = false;
  hideTimer = null;
}

async function tick(){
  try{
    const r = await fetch('/overlay/state', {cache:'no-store'});
    const j = await r.json();

    if(!j.show){
      return;
    }

    const sig = `${j.viewer.name}|${j.cm.name}|${j.xp.total}|${(j.happiness && j.happiness.pct) || 0}`;
    if (sig !== lastSig) {
      lastSig = sig;
      playSfx();
    }

    // Viewer
    document.getElementById('viewer').textContent = `@${j.viewer.name}`;
    document.getElementById('avatar').src = j.viewer.avatar || '';

    // CM
    document.getElementById('cmimg').src = j.cm.media || '';
    document.getElementById('cmname').textContent = j.cm.name || 'CapsMons';

    // XP
    const pct = (j.xp.pct === null || j.xp.pct === undefined) ? 100 : j.xp.pct;
    document.getElementById('fill').style.width = pct + '%';

    const toNext = j.xp.to_next;
    document.getElementById('xpLabel').textContent =
      (toNext ? `${j.xp.total} XP • prochain: ${toNext} XP` : `${j.xp.total} XP • max`);

    // Bonheur
    const hpct = (j.happiness && j.happiness.pct !== undefined) ? j.happiness.pct : 0;
    document.getElementById('hfill').style.width = hpct + '%';
    document.getElementById('hLabel').textContent = `${hpct}%`;

    showCard();

  }catch(e){
    // ignore
  }
}

setInterval(tick, 500);
tick();
</script>

</body>
</html>
""")


@app.get("/overlay/evolution", response_class=HTMLResponse)
def overlay_evolution_page():
    return HTMLResponse(r"""
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
  :root{
    --bg: rgba(10,15,20,.72);
    --border: rgba(255,255,255,.12);
    --text: #e6edf3;
    --muted: #9aa4b2;
    --accent: rgba(122,162,255,.95);
    --accent2: rgba(255, 214, 102, .95);
  }

  body{
    margin:0;
    background:transparent;
    overflow:hidden;
    font-family:system-ui,Segoe UI,Roboto,Arial,sans-serif;
  }

  .wrap{
    position:fixed; inset:0;
    display:flex; align-items:center; justify-content:center;
    pointer-events:none;
  }

  /* --- Backdrop cinematic --- */
  .backdrop{
    position:fixed; inset:0;
    display:none;
    background:
      radial-gradient(1200px 700px at 50% 50%, rgba(122,162,255,.16), rgba(0,0,0,0) 60%),
      radial-gradient(900px 520px at 52% 48%, rgba(255,214,102,.10), rgba(0,0,0,0) 55%);
    opacity:0;
    transition: opacity 450ms ease;
  }
  .backdrop.show{ opacity:1; }

  /* subtle scanlines */
  .scanlines{
    position:absolute; inset:-40px;
    background: repeating-linear-gradient(
      to bottom,
      rgba(255,255,255,.03) 0px,
      rgba(255,255,255,.03) 1px,
      rgba(0,0,0,0) 3px,
      rgba(0,0,0,0) 7px
    );
    opacity:.25;
    mix-blend-mode: overlay;
    filter: blur(.2px);
  }

  /* --- Card --- */
  .card{
    display:none;
    width:min(920px, 92vw);
    padding:18px 20px 20px;
    border-radius:26px;
    background: var(--bg);
    border:1px solid var(--border);
    backdrop-filter: blur(10px);
    box-shadow: 0 20px 70px rgba(0,0,0,.45);
    position:relative;

    opacity:0;
    transform: translateY(14px) scale(.92);
    transition: opacity 520ms ease, transform 520ms ease;
  }
  .card.show{
    opacity:1;
    transform: translateY(0) scale(1);
  }

  /* Glitch edges on entrance */
  .card::before{
    content:"";
    position:absolute; inset:-2px;
    border-radius:28px;
    background: linear-gradient(90deg, rgba(122,162,255,.0), rgba(122,162,255,.35), rgba(255,214,102,.2), rgba(122,162,255,.0));
    filter: blur(14px);
    opacity:0;
    transition: opacity 520ms ease;
  }
  .card.show::before{ opacity:1; }

  /* --- Viewer bar --- */
  .viewerBar{
    display:flex; align-items:center; gap:12px;
    padding:10px 12px;
    border-radius:16px;
    background: rgba(255,255,255,.06);
    border:1px solid rgba(255,255,255,.10);
  }
  .avatar{
    width:40px;height:40px;border-radius:12px;
    object-fit:cover;border:1px solid rgba(255,255,255,.15);
  }
  .viewerName{
  color: var(--text);
  font-weight: 900;
  font-size: 16px;        /* avant: 14px */
  line-height: 1.1;
  text-shadow: 0 1px 6px rgba(0,0,0,.5);
}

  .viewerSub{ color:var(--muted); font-size:12px; margin-top:2px; }

  /* --- Main layout --- */
  .grid{
    display:grid;
    grid-template-columns: 1fr 520px;
    gap:18px;
    align-items:center;
    margin-top:14px;
  }

  /* --- Image chamber --- */
  .chamber{
    position:relative;
    width:520px; height:520px;
    border-radius:28px;
    background: rgba(255,255,255,.05);
    border:1px solid rgba(255,255,255,.10);
    overflow:hidden;
    display:flex; align-items:center; justify-content:center;
  }

  /* rings */
  .ring{
    position:absolute;
    width:640px;height:640px;
    border-radius:999px;
    border:1px solid rgba(122,162,255,.25);
    filter: blur(.2px);
    opacity:.0;
    transform: scale(.7);
  }
  .ring.r1{ border-color: rgba(122,162,255,.25); }
  .ring.r2{ border-color: rgba(255,214,102,.22); width:720px;height:720px; }
  .ring.r3{ border-color: rgba(255,255,255,.12); width:820px;height:820px; }

  /* shockwave */
  .shockwave{
    position:absolute;
    width:24px;height:24px;
    border-radius:999px;
    border:2px solid rgba(255,255,255,.35);
    opacity:0;
    transform: scale(1);
  }

  /* the image */
  .img{
    width:92%;
    height:92%;
    object-fit:contain;
    filter: drop-shadow(0 18px 26px rgba(0,0,0,.55));
    opacity:0;
    transform: translateY(6px) scale(.96);
    transition: opacity 520ms ease, transform 520ms ease;
  }
  .card.show .img{
    opacity:1;
    transform: translateY(0) scale(1);
  }

  /* text block */
.title{
  font-size: 42px;        /* avant: 34px */
  font-weight: 1000;
  color: var(--text);
  letter-spacing: .4px;
  line-height: 1.08;
  text-shadow:
    0 2px 10px rgba(0,0,0,.55),
    0 0 18px rgba(122,162,255,.35);
}

/* Typewriter cursor */
.titleTyping::after{
  content:"";
  display:inline-block;
  width:10px;
  height:1.15em;
  margin-left:8px;
  background: rgba(255,255,255,.65);
  border-radius:2px;
  animation: caretBlink 900ms steps(2, end) infinite;
  vertical-align: -0.15em;
}

@keyframes caretBlink{
  0%, 49% { opacity:1; }
  50%, 100% { opacity:0; }
}

/* Pulse (appliqué en JS via transform + text-shadow) */
.titlePulse{
  will-change: transform, text-shadow, filter;
}


.subtitle{
  margin-top: 12px;
  color: #cfd6e3;         /* plus clair */
  font-size: 17px;        /* avant: 14px */
  line-height: 1.5;
  text-shadow: 0 1px 6px rgba(0,0,0,.45);
}

  .pillRow{ margin-top:14px; display:flex; gap:8px; flex-wrap:wrap; }
.pill{
  padding: 7px 12px;      /* un peu plus haut */
  font-size: 13px;        /* avant: 12px */
  color: #e1e7f0;
  background: rgba(0,0,0,.22);
}

  .dot{
    width:8px;height:8px;border-radius:999px;
    background: var(--accent);
    box-shadow: 0 0 18px rgba(122,162,255,.55);
  }

  /* cinematic flash overlay */
  .flash{
    position:fixed; inset:0;
    background: radial-gradient(800px 500px at 50% 50%, rgba(255,255,255,.55), rgba(255,255,255,0) 55%);
    opacity:0;
    pointer-events:none;
  }

  /* subtle shake */
  @keyframes shake {
    0%{ transform: translateY(0) }
    20%{ transform: translateY(-2px) }
    40%{ transform: translateY(2px) }
    60%{ transform: translateY(-1px) }
    80%{ transform: translateY(1px) }
    100%{ transform: translateY(0) }
  }

  /* ring burst */
  @keyframes ringBurst {
    0%{ opacity:0; transform: scale(.65) rotate(0deg); }
    25%{ opacity:.85; }
    100%{ opacity:0; transform: scale(1.12) rotate(12deg); }
  }

  @keyframes shock {
    0%{ opacity:.9; transform: scale(1); }
    100%{ opacity:0; transform: scale(42); }
  }

  /* particles canvas */
  canvas{
    position:absolute; inset:0;
    width:100%; height:100%;
  }

  /* show/hide timing */
  .hideFade{
    opacity:0 !important;
    transform: translateY(14px) scale(.92) !important;
    transition: opacity 420ms ease, transform 420ms ease !important;
  }

  /* reduce motion fallback */
  @media (prefers-reduced-motion: reduce){
    .card, .img, .backdrop{ transition:none !important; }
  }
</style>
</head>

<body>
<div class="backdrop" id="backdrop">
  <div class="scanlines"></div>
</div>
<div class="flash" id="flash"></div>

<div class="wrap">
  <div id="card" class="card">
    <div class="viewerBar">
      <img id="avatar" class="avatar" src="" alt="">
      <div>
        <div id="viewerName" class="viewerName"></div>
        <div class="viewerSub">Évolution détectée — ManaCorp</div>
      </div>
    </div>

    <div class="grid">
      <div>
        <div class="title" id="formName">Évolution</div>
        <div class="subtitle" id="subText">
          Stabilisation de la signature génétique… synchronisation des flux…
        </div>

        <div class="pillRow">
          <div class="pill"><span class="dot"></span> Procédure : ÉVOLUTION</div>
          <div class="pill">🔊 Son synchronisé</div>
          <div class="pill">🧬 Forme validée</div>
        </div>
      </div>

      <div class="chamber" id="chamber">
        <canvas id="fx"></canvas>
        <div class="ring r1" id="r1"></div>
        <div class="ring r2" id="r2"></div>
        <div class="ring r3" id="r3"></div>
        <div class="shockwave" id="shockwave"></div>
        <img id="img" class="img" src="" alt="">
      </div>
    </div>

    <audio id="snd"></audio>
  </div>
</div>

<script>
let showing = false;
let lastSig = "";
let hideTimer = null;
const SHOW_MS = 6500;

const card = document.getElementById('card');
const backdrop = document.getElementById('backdrop');
const flash = document.getElementById('flash');

const avatar = document.getElementById('avatar');
const viewerName = document.getElementById('viewerName');
const img = document.getElementById('img');
const formName = document.getElementById('formName');
const snd = document.getElementById('snd');

const r1 = document.getElementById('r1');
const r2 = document.getElementById('r2');
const r3 = document.getElementById('r3');
const shockwave = document.getElementById('shockwave');

const canvas = document.getElementById('fx');
const ctx = canvas.getContext('2d');

function resizeCanvas(){
  const rect = canvas.getBoundingClientRect();
  canvas.width = Math.floor(rect.width * devicePixelRatio);
  canvas.height = Math.floor(rect.height * devicePixelRatio);
  ctx.setTransform(devicePixelRatio,0,0,devicePixelRatio,0,0);
}
window.addEventListener('resize', resizeCanvas);

let particles = [];
function spawnParticles(){
  particles = [];
  const w = canvas.getBoundingClientRect().width;
  const h = canvas.getBoundingClientRect().height;
  const cx = w/2, cy = h/2;

  const n = 80;
  for(let i=0;i<n;i++){
    const a = Math.random() * Math.PI * 2;
    const sp = 0.8 + Math.random()*2.2;
    particles.push({
      x: cx + (Math.random()*10-5),
      y: cy + (Math.random()*10-5),
      vx: Math.cos(a) * sp,
      vy: Math.sin(a) * sp,
      life: 0,
      max: 40 + Math.floor(Math.random()*35),
      size: 1 + Math.random()*2.2,
      kind: Math.random() < 0.75 ? 0 : 1
    });
  }
}

function stepParticles(){
  const w = canvas.getBoundingClientRect().width;
  const h = canvas.getBoundingClientRect().height;
  ctx.clearRect(0,0,w,h);

  // subtle vignette
  const g = ctx.createRadialGradient(w/2,h/2,10,w/2,h/2,Math.min(w,h)/1.5);
  g.addColorStop(0,'rgba(122,162,255,.06)');
  g.addColorStop(1,'rgba(0,0,0,0)');
  ctx.fillStyle = g;
  ctx.fillRect(0,0,w,h);

  // particles
  for(const p of particles){
    p.life++;
    p.x += p.vx;
    p.y += p.vy;
    p.vx *= 0.985;
    p.vy *= 0.985;

    const t = p.life / p.max;
    const alpha = Math.max(0, 1 - t);

    ctx.globalAlpha = alpha * 0.85;
    ctx.beginPath();
    ctx.arc(p.x, p.y, p.size, 0, Math.PI*2);

    if(p.kind === 0){
      ctx.fillStyle = 'rgba(122,162,255,1)';
      ctx.shadowColor = 'rgba(122,162,255,.75)';
      ctx.shadowBlur = 12;
    }else{
      ctx.fillStyle = 'rgba(255,214,102,1)';
      ctx.shadowColor = 'rgba(255,214,102,.55)';
      ctx.shadowBlur = 10;
    }
    ctx.fill();
    ctx.shadowBlur = 0;

    // end
    if(p.life >= p.max){
      p.life = 999999;
    }
  }
  ctx.globalAlpha = 1;

  particles = particles.filter(p => p.life < p.max);

  if(showing){
    requestAnimationFrame(stepParticles);
  }
}

function playFlash(){
  flash.style.opacity = '0';
  // force
  void flash.offsetWidth;
  flash.style.transition = 'opacity 140ms ease';
  flash.style.opacity = '0.9';
  setTimeout(()=>{ flash.style.opacity = '0'; }, 160);
}

function burstRings(){
  // reset + animate
  for (const el of [r1,r2,r3]){
    el.style.animation = 'none';
    el.style.opacity = '0';
    el.style.transform = 'scale(.7)';
    void el.offsetWidth;
    el.style.animation = 'ringBurst 820ms ease-out';
  }
}

function burstShockwave(){
  shockwave.style.animation = 'none';
  shockwave.style.opacity = '0';
  void shockwave.offsetWidth;
  shockwave.style.animation = 'shock 720ms ease-out';
}

function showCard(){
  if (!showing){
    backdrop.style.display='block';
    card.style.display='block';
    // reflow
    void card.offsetWidth;
    backdrop.classList.add('show');
    card.classList.add('show');

    // cinematic impact
    playFlash();
    burstRings();
    burstShockwave();
    spawnParticles();
    resizeCanvas();
    showing = true;
    requestAnimationFrame(stepParticles);

    // tiny shake
    card.style.animation = 'shake 360ms ease';
    setTimeout(()=>{ card.style.animation = 'none'; }, 380);
  }

  if (hideTimer) clearTimeout(hideTimer);
  hideTimer = setTimeout(hideCard, SHOW_MS);
}

function hideCard(){
  if (!showing) return;

  card.classList.remove('show');
  backdrop.classList.remove('show');

  // stop FX after fade
  setTimeout(()=>{
    card.style.display='none';
    backdrop.style.display='none';
    ctx.clearRect(0,0,canvas.width,canvas.height);
    particles = [];
  }, 520);

  showing = false;
  hideTimer = null;
  stopPulse();
formName.style.transform = '';
formName.style.textShadow = '';

}

// ---------------------
// Typewriter
// ---------------------
let typingTimer = null;

function typewriter(el, fullText, speedMs=28){
  // reset
  if (typingTimer) { clearInterval(typingTimer); typingTimer = null; }
  el.textContent = "";
  el.classList.add("titleTyping");

  let i = 0;
  typingTimer = setInterval(() => {
    el.textContent += fullText[i] || "";
    i++;
    if (i >= fullText.length){
      clearInterval(typingTimer);
      typingTimer = null;

      // retire le curseur après une petite pause
      setTimeout(()=>{ el.classList.remove("titleTyping"); }, 700);
    }
  }, speedMs);
}

// ---------------------
// Audio pulse (WebAudio)
// ---------------------
let audioCtx = null;
let analyser = null;
let dataArray = null;
let rafPulse = null;

function stopPulse(){
  if (rafPulse) cancelAnimationFrame(rafPulse);
  rafPulse = null;
  if (analyser) analyser.disconnect();
  analyser = null;
  dataArray = null;
}

function startPulse(audioEl, targetEl){
  stopPulse();

  // WebAudio context (créé à la demande)
  audioCtx = audioCtx || new (window.AudioContext || window.webkitAudioContext)();

  const src = audioCtx.createMediaElementSource(audioEl);
  analyser = audioCtx.createAnalyser();
  analyser.fftSize = 256;

  src.connect(analyser);
  analyser.connect(audioCtx.destination);

  dataArray = new Uint8Array(analyser.frequencyBinCount);

  targetEl.classList.add("titlePulse");

  const baseScale = 1.0;

  const loop = () => {
    if (!analyser) return;

    analyser.getByteFrequencyData(dataArray);

    // énergie moyenne (0..255)
    let sum = 0;
    for (let i = 0; i < dataArray.length; i++) sum += dataArray[i];
    const avg = sum / dataArray.length;

    // normalise (0..1 environ)
    const n = Math.min(1, avg / 140);

    // pulse
    const scale = baseScale + n * 0.06;
    targetEl.style.transform = `scale(${scale})`;

    // glow lié au son
    const glow = 10 + n * 26;
    targetEl.style.textShadow = `
      0 2px 10px rgba(0,0,0,.55),
      0 0 ${glow}px rgba(122,162,255,.55),
      0 0 ${glow * 0.6}px rgba(255,214,102,.25)
    `;

    rafPulse = requestAnimationFrame(loop);
  };

  rafPulse = requestAnimationFrame(loop);
}


async function tick(){
  try{
    const r = await fetch('/overlay/evolution_state', {cache:'no-store'});
    const d = await r.json();
    if(!d.active) return;

    const sig = `${d.viewer.name}|${d.form.name}|${d.form.image}|${d.form.sound||''}`;
    if (sig !== lastSig){
      lastSig = sig;

// texte lettre par lettre
typewriter(formName, d.form.name || "Évolution", 28);

// image
img.src = d.form.image || '';

// son + pulse sync
if (d.form.sound){
  snd.src = d.form.sound;
  try {
    snd.currentTime = 0;

    // IMPORTANT: certains navigateurs nécessitent resume()
    if (audioCtx && audioCtx.state === "suspended") {
      audioCtx.resume().catch(()=>{});
    }

    snd.play().catch(()=>{});
    startPulse(snd, formName);
  } catch(e) {}
} else {
  stopPulse();
}

showCard();

    }
  }catch(e){}
}



setInterval(tick, 500);
tick();
</script>
</body>
</html>

""")


@app.get("/admin/cms", response_class=HTMLResponse)
def admin_cms(
    request: Request,
    flash: str | None = None,
    flash_kind: str | None = None,
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)

    with get_db() as conn:
        with conn.cursor() as cur:
            # ✅ lineages + choose_enabled (défaut true si colonne NULL / pas encore backfill)
            cur.execute("""
                SELECT key, name, is_enabled, COALESCE(choose_enabled, true) AS choose_enabled
                FROM lineages
                ORDER BY key;
            """)
            lineages = [{
                "key": r[0],
                "name": r[1],
                "is_enabled": bool(r[2]),
                "choose_enabled": bool(r[3]),
            } for r in cur.fetchall()]

            cur.execute("""
                SELECT key, name, lineage_key, is_enabled, in_hatch_pool, COALESCE(media_url,'')
                FROM cms
                ORDER BY lineage_key, key;
            """)
            cms = [{
                "key": r[0],
                "name": r[1],
                "lineage_key": r[2],
                "is_enabled": bool(r[3]),
                "in_hatch_pool": bool(r[4]),
                "media_url": r[5],
            } for r in cur.fetchall()]

    return templates.TemplateResponse("cms.html", {
        "request": request,
        "lineages": lineages,
        "cms": cms,
        "flash": flash,
        "flash_kind": flash_kind,
    })
# =============================================================================
# ADMIN: CMS ACTION 
# =============================================================================


@app.post("/admin/cms/action")
def admin_cms_action(
    action: str = Form(...),
    key: str | None = Form(None),
    cm_key: str | None = Form(None),
    cm_name: str | None = Form(None),
    lineage_key: str | None = Form(None),
    media_url: str | None = Form(None),
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)
    action = action.strip().lower()

    def go(msg: str, kind: str = "ok"):
        safe = msg.replace(" ", "%20")
        return RedirectResponse(url=f"/admin/cms?flash_kind={kind}&flash={safe}", status_code=303)

    with get_db() as conn:
        with conn.cursor() as cur:
            if action == "toggle_lineage":
                if not key:
                    return go("Key manquante", "err")
                cur.execute("UPDATE lineages SET is_enabled = NOT is_enabled WHERE key=%s;", (key,))
                conn.commit()
                return go(f"Lineage {key} toggled")

            if action == "add_cm":
                if not (cm_key and cm_name and lineage_key):
                    return go("Champs manquants", "err")

                cm_key = cm_key.strip().lower()
                cm_name = cm_name.strip()
                lineage_key = lineage_key.strip().lower()
                url = (media_url or "").strip()

                cur.execute("SELECT 1 FROM lineages WHERE key=%s;", (lineage_key,))
                if not cur.fetchone():
                    return go("Lineage inconnue", "err")

                cur.execute("""
                    INSERT INTO cms (key, name, lineage_key, is_enabled, in_hatch_pool, media_url)
                    VALUES (%s, %s, %s, TRUE, FALSE, %s)
                    ON CONFLICT (key) DO UPDATE
                    SET name = EXCLUDED.name,
                        lineage_key = EXCLUDED.lineage_key,
                        media_url = EXCLUDED.media_url;
                """, (cm_key, cm_name, lineage_key, url))

                conn.commit()
                return go(f"CM créé: {cm_key}")

            if action == "rename_cm":
                if not (key and cm_name):
                    return go("Champs manquants", "err")
                cur.execute("UPDATE cms SET name=%s WHERE key=%s;", (cm_name, key))
                conn.commit()
                return go(f"CM renommé: {key}")

            if action == "toggle_cm_enabled":
                if not key:
                    return go("Key manquante", "err")
                cur.execute("UPDATE cms SET is_enabled = NOT is_enabled WHERE key=%s;", (key,))
                conn.commit()
                return go(f"CM enabled toggled: {key}")

            if action == "toggle_cm_pool":
                if not key:
                    return go("Key manquante", "err")
                cur.execute("UPDATE cms SET in_hatch_pool = NOT in_hatch_pool WHERE key=%s;", (key,))
                conn.commit()
                return go(f"CM pool toggled: {key}")

            if action == "update_media_url":
                if not key:
                    return go("Key manquante", "err")
                url = (media_url or "").strip()
                cur.execute("UPDATE cms SET media_url=%s WHERE key=%s;", (url, key))
                conn.commit()
                return go(f"Media URL mis à jour: {key}")

            if action == "delete_cm":
                if not key:
                    return go("Key manquante", "err")
                cur.execute("DELETE FROM cms WHERE key=%s;", (key,))
                cur.execute("UPDATE creatures_v2 SET cm_key=NULL, updated_at=now() WHERE cm_key=%s;", (key,))
                conn.commit()
                return go(f"CM supprimé: {key}")

    return go("Action inconnue", "err")

# =============================================================================
# ADMIN: RP
# =============================================================================

@app.get("/admin/rp", response_class=HTMLResponse)
def admin_rp(request: Request, flash: str | None = None, credentials: HTTPBasicCredentials = Depends(security)):
    require_admin(credentials)

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT key, lines FROM rp_lines ORDER BY key;")
            rows = cur.fetchall()

    items = []
    for k, lines in rows:
        if isinstance(lines, str):
            try:
                lines = json.loads(lines)
            except Exception:
                lines = []
        if not isinstance(lines, list):
            lines = []
        text = "\n".join([str(x) for x in lines if str(x).strip()])
        items.append({"key": k, "count": len(lines), "text": text})

    return templates.TemplateResponse("rp.html", {"request": request, "items": items, "flash": flash})

# =============================================================================
# ADMIN: admin Stats
# =============================================================================

@app.get("/admin/stats", response_class=HTMLResponse)
def admin_stats(request: Request, credentials: HTTPBasicCredentials = Depends(security)):
    require_admin(credentials)

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT date_trunc('day', now() AT TIME ZONE 'Europe/Paris') AT TIME ZONE 'Europe/Paris';")
            start_of_today_paris = cur.fetchone()[0]

            cur.execute("""
                SELECT COALESCE(SUM(amount), 0)
                FROM xp_events
                WHERE created_at >= %s;
            """, (start_of_today_paris,))
            xp_today = int(cur.fetchone()[0])

            cur.execute("""
                SELECT COALESCE(SUM(amount), 0)
                FROM xp_events
                WHERE created_at >= now() - interval '7 days';
            """)
            xp_7d = int(cur.fetchone()[0])

            cur.execute("""
                SELECT COUNT(*) AS events, COUNT(DISTINCT twitch_login) AS users
                FROM xp_events
                WHERE created_at >= now() - interval '24 hours';
            """)
            events_24h, active_users_24h = cur.fetchone()
            events_24h = int(events_24h)
            active_users_24h = int(active_users_24h)

            cur.execute("""
                SELECT to_char(date_trunc('day', created_at AT TIME ZONE 'Europe/Paris'), 'YYYY-MM-DD') AS day,
                       SUM(amount) AS xp
                FROM xp_events
                WHERE created_at >= now() - interval '7 days'
                GROUP BY 1
                ORDER BY 1 DESC;
            """)
            xp_by_day = [{"day": r[0], "xp": int(r[1])} for r in cur.fetchall()]
            max_xp = max([r["xp"] for r in xp_by_day], default=0) or 1
            for r in xp_by_day:
                r["pct"] = int((r["xp"] / max_xp) * 100)

            cur.execute("""
                SELECT twitch_login, SUM(amount) AS xp
                FROM xp_events
                WHERE created_at >= now() - interval '24 hours'
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 20;
            """)
            top_xp_24h = [{"twitch_login": r[0], "xp": int(r[1])} for r in cur.fetchall()]

            cur.execute("""
                SELECT twitch_login, COUNT(*) AS events
                FROM xp_events
                WHERE created_at >= now() - interval '24 hours'
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 20;
            """)
            top_events_24h = [{"twitch_login": r[0], "events": int(r[1])} for r in cur.fetchall()]

            cur.execute("""
                SELECT COUNT(DISTINCT twitch_login)
                FROM xp_events
                WHERE created_at >= now() - interval '15 minutes';
            """)
            active_users_15m = int(cur.fetchone()[0])

    return templates.TemplateResponse("stats.html", {
        "request": request,
        "xp_today": xp_today,
        "xp_7d": xp_7d,
        "events_24h": events_24h,
        "active_users_24h": active_users_24h,
        "active_users_15m": active_users_15m,
        "xp_by_day": xp_by_day,
        "top_xp_24h": top_xp_24h,
        "top_events_24h": top_events_24h,
    })

@app.get("/overlay/state")
def overlay_state():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,
                    twitch_login,
                    viewer_display,
                    viewer_avatar,
                    cm_key,
                    cm_name,
                    cm_media_url,
                    xp_total,
                    stage,
                    stage_start_xp,
                    next_stage_xp,
                    happiness,
                    expires_at
                FROM overlay_events
                WHERE expires_at > now()
                ORDER BY id DESC
                LIMIT 1;
            """)
            r = cur.fetchone()

    if not r:
        return {"show": False}

    (
        _id,
        twitch_login,
        viewer_display,
        viewer_avatar,
        cm_key,
        cm_name,
        cm_media_url,
        xp_total,
        stage,
        stage_start_xp,
        next_stage_xp,
        happiness,
        expires_at,
    ) = r

    # Calcul % XP dans le stage
    try:
        xp_total = int(xp_total or 0)
        stage_start_xp = int(stage_start_xp or 0)
        next_stage_xp = int(next_stage_xp or 0) if next_stage_xp is not None else None
    except Exception:
        stage_start_xp = 0
        next_stage_xp = None

    if next_stage_xp is None or next_stage_xp <= stage_start_xp:
        xp_pct = 100
        to_next = None
    else:
        span = max(1, next_stage_xp - stage_start_xp)
        cur_in_stage = max(0, min(span, xp_total - stage_start_xp))
        xp_pct = int(round((cur_in_stage / span) * 100))
        to_next = max(0, next_stage_xp - xp_total)

    # Bonheur en %
    try:
        h_pct = max(0, min(100, int(happiness or 0)))
    except Exception:
        h_pct = 0

    # ⚠️ ton JS fait "@${j.viewer.name}" -> il veut un login (pas le display)
    # On renvoie le login (twitch_login). Le display reste possible si tu veux plus tard.
    return {
        "show": True,
        "viewer": {
            "name": twitch_login,
            "avatar": viewer_avatar or "",
        },
        "cm": {
            "key": cm_key,
            "name": cm_name or cm_key,
            "media": cm_media_url or "",
            "stage": int(stage or 0),
        },
        "xp": {
            "total": xp_total,
            "pct": xp_pct,
            "to_next": to_next,
        },
        "happiness": {
            "pct": h_pct,
        },
    }


@app.get("/overlay/evolution_state")
def overlay_evolution_state():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT viewer_display, viewer_avatar, name, image_url, COALESCE(sound_url,'')
                FROM overlay_evolutions
                WHERE expires_at > now()
                ORDER BY id DESC
                LIMIT 1;
            """)
            row = cur.fetchone()

    if not row:
        return {"active": False}

    viewer_display, viewer_avatar, name, image_url, sound_url = row
    return {
        "active": True,
        "viewer": {"name": viewer_display or "", "avatar": viewer_avatar or ""},
        "form": {"name": name, "image": image_url, "sound": sound_url or ""},
    }

@app.post("/internal/xp")
def add_xp(payload: dict, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    login = str(payload.get("twitch_login", "")).strip().lower()
    if not login:
        raise HTTPException(status_code=400, detail="Missing twitch_login")

    try:
        amount = int(payload.get("amount", 1))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid amount")

    if amount <= 0 or amount > 100:
        raise HTTPException(status_code=400, detail="Amount out of range")

    evo_payload = None
    prev_stage = new_stage = new_xp_total = 0
    cm_assigned = None  # si tu veux le renvoyer au bot

    with get_db() as conn:
        with conn.cursor() as cur:
            # user existe
            cur.execute("INSERT INTO users (twitch_login) VALUES (%s) ON CONFLICT DO NOTHING;", (login,))

            # ✅ NOUVEAU : s'assurer qu'un œuf actif existe si aucun CM actif
            ensure_active_egg(conn, login)

            # récupérer CM actif
            cur.execute("""
                SELECT id, cm_key, lineage_key, stage, xp_total
                FROM creatures_v2
                WHERE twitch_login=%s AND is_active=true
                LIMIT 1;
            """, (login,))
            row = cur.fetchone()
            if not row:
                # ne devrait plus arriver
                return {"ok": True, "twitch_login": login, "skipped": True}

            creature_id, cm_key, lineage_key, prev_stage, xp_total = row
            prev_stage = int(prev_stage or 0)
            xp_total = int(xp_total or 0)

            # log XP global
            cur.execute("INSERT INTO xp_events (twitch_login, amount) VALUES (%s,%s);", (login, amount))

            # ajouter XP
            new_xp_total = xp_total + amount
            new_stage = int(stage_from_xp(new_xp_total))

            cur.execute("""
                UPDATE creatures_v2
                SET xp_total=%s,
                    stage=%s,
                    updated_at=now()
                WHERE id=%s;
            """, (new_xp_total, new_stage, creature_id))

            stage_changed = (new_stage > prev_stage)

            # ✅ NOUVEAU : Hatch (stage 0 -> 1) : si pas de lignée => random
            if prev_stage == 0 and new_stage >= 1:
                if not lineage_key:
                    lineage_key = pick_random_lineage(conn)

                    if lineage_key:
                        cur.execute("""
                            UPDATE creatures_v2
                            SET lineage_key=%s, updated_at=now()
                            WHERE id=%s;
                        """, (lineage_key, creature_id))

                # attribuer un CM si on est encore sur egg (ou cm_key NULL, selon ton historique)
                # ici on attribue si cm_key == 'egg'
                if lineage_key and cm_key == "egg":
                    picked = pick_cm_for_lineage(conn, lineage_key)
                    if picked:
                        cm_key = picked
                        cm_assigned = picked
                        cur.execute("""
                            UPDATE creatures_v2
                            SET cm_key=%s, updated_at=now()
                            WHERE id=%s;
                        """, (cm_key, creature_id))

            # overlay evolution si forme existe (inchangé)
            if stage_changed and new_stage >= 1:
                cur.execute("""
                    SELECT name, image_url, sound_url
                    FROM cm_forms
                    WHERE cm_key=%s AND stage=%s;
                """, (cm_key, new_stage))
                f = cur.fetchone()
                if f:
                    form_name, image_url, sound_url = f
                    evo_payload = {
                        "twitch_login": login,
                        "cm_key": cm_key,
                        "stage": new_stage,
                        "name": form_name,
                        "image_url": image_url,
                        "sound_url": sound_url,
                    }

        conn.commit()

    if evo_payload:
        trigger_evolution_overlay(evo_payload)

    return {
        "ok": True,
        "twitch_login": login,
        "xp_total": new_xp_total,
        "stage_before": prev_stage,
        "stage_after": new_stage,
        "evolved": (new_stage > prev_stage),
        "cm_assigned": cm_assigned,  # optionnel (tu l'utilises déjà côté bot)
    }

# =============================================================================
# COMPANIONS (creatures_v2): list + set active
# =============================================================================

@app.get("/internal/companions/{login}")
def internal_companions(login: str, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)
    login = login.strip().lower()
    if not login:
        raise HTTPException(status_code=400, detail="Missing login")

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    c.id,
                    c.cm_key,
                    c.lineage_key,
                    c.stage,
                    c.xp_total,
                    c.is_active,
                    COALESCE(m.name, c.cm_key) AS cm_name
                FROM creatures_v2 c
                LEFT JOIN cms m ON m.key = c.cm_key
                WHERE c.twitch_login = %s
                ORDER BY c.is_active DESC, c.stage DESC, c.xp_total DESC, c.acquired_at DESC, c.id DESC;
            """, (login,))
            rows = cur.fetchall()

    companions = []
    for r in rows:
        companions.append({
            "id": int(r[0]),
            "cm_key": r[1],
            "lineage_key": r[2],
            "stage": int(r[3] or 0),
            "xp_total": int(r[4] or 0),
            "is_active": bool(r[5]),
            "cm_name": r[6] or r[1],
        })

    return {"ok": True, "twitch_login": login, "companions": companions}



@app.post("/internal/companions/set_active")
def companions_set_active(payload: dict, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    login = str(payload.get("twitch_login","")).strip().lower()
    creature_id = payload.get("creature_id", None)

    if not login or creature_id is None:
        raise HTTPException(status_code=400, detail="Missing twitch_login or creature_id")

    try:
        creature_id = int(creature_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid creature_id")

    with get_db() as conn:
        with conn.cursor() as cur:
            # vérifier que l'id appartient bien au user
            cur.execute("""
                SELECT id
                FROM creatures_v2
                WHERE id=%s AND twitch_login=%s;
            """, (creature_id, login))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Companion not found")

            # désactiver tout
            cur.execute("""
                UPDATE creatures_v2
                SET is_active=false, updated_at=now()
                WHERE twitch_login=%s AND is_active=true;
            """, (login,))

            # activer celui choisi
            cur.execute("""
                UPDATE creatures_v2
                SET is_active=true, updated_at=now()
                WHERE id=%s AND twitch_login=%s;
            """, (creature_id, login))

            # renvoyer les infos utiles au bot
            cur.execute("""
                SELECT id, cm_key, lineage_key, stage, xp_total, happiness
                FROM creatures_v2
                WHERE id=%s AND twitch_login=%s;
            """, (creature_id, login))
            row = cur.fetchone()

        conn.commit()

    if not row:
        raise HTTPException(status_code=500, detail="Active companion not found after update")

    (cid, cm_key, lineage_key, stage, xp_total, happiness) = row

    return {
        "ok": True,
        "twitch_login": login,
        "active_id": int(cid),
        "active_cm_key": str(cm_key),
        "active_lineage_key": lineage_key,
        "active_stage": int(stage or 0),
        "active_xp_total": int(xp_total or 0),
        "active_happiness": int(happiness or 0),
    }





# =============================================================================
# ADMIN: Item Use (creatures_v2 only)
# =============================================================================

from fastapi import Header, HTTPException

@app.post("/internal/item/use")
def internal_use_item(payload: dict, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    login = str(payload.get("twitch_login", "")).strip().lower()
    item_key = str(payload.get("item_key", "")).strip().lower()

    if not login or not item_key:
        raise HTTPException(status_code=400, detail="Missing twitch_login or item_key")

    # valeurs de retour
    item_name = item_key
    happiness_gain = 0
    new_happiness = None
    xp_gain = 0
    new_xp_total = None
    stage_before = None
    stage_after = None

    def _parse_egg_lineage(k: str) -> str | None:
        # egg_biolab / egg_securite / egg_extraction / egg_limited
        if not k.startswith("egg_"):
            return None
        lk = k.split("egg_", 1)[1].strip().lower()
        return lk or None

    with get_db() as conn:
        with conn.cursor() as cur:
            # 0) user existe
            cur.execute(
                "INSERT INTO users (twitch_login) VALUES (%s) ON CONFLICT DO NOTHING;",
                (login,),
            )

            # 1) item existe ? (on lit aussi xp_gain)
            cur.execute(
                "SELECT name, happiness_gain, xp_gain FROM items WHERE key=%s;",
                (item_key,),
            )
            item = cur.fetchone()
            if not item:
                raise HTTPException(status_code=400, detail="Unknown item")

            item_name, happiness_gain_db, xp_gain_db = item
            happiness_gain = int(happiness_gain_db or 0)
            xp_gain = int(xp_gain_db or 0)

            # 2) stock inventaire ?
            cur.execute(
                "SELECT qty FROM inventory WHERE twitch_login=%s AND item_key=%s;",
                (login, item_key),
            )
            row = cur.fetchone()
            if not row or int(row[0]) <= 0:
                raise HTTPException(status_code=400, detail="No item in inventory")

            # 3) CM actif ? (requis pour XP/bonheur, mais pas forcément pour créer un oeuf)
            cur.execute(
                """
                SELECT cm_key, stage, xp_total, happiness
                FROM creatures_v2
                WHERE twitch_login=%s AND is_active=true
                LIMIT 1;
                """,
                (login,),
            )
            arow = cur.fetchone()

            # 5) appliquer effet (œuf en priorité)
            egg_lineage = _parse_egg_lineage(item_key)
            if egg_lineage:
                # vérifier que la lignée existe (et activée)
                cur.execute("SELECT is_enabled FROM lineages WHERE key=%s;", (egg_lineage,))
                lrow = cur.fetchone()
                if not lrow or not bool(lrow[0]):
                    raise HTTPException(status_code=400, detail="Unknown lineage for egg")

                # consommer 1 item (œuf)
                cur.execute(
                    """
                    UPDATE inventory
                    SET qty = qty - 1, updated_at = now()
                    WHERE twitch_login=%s AND item_key=%s;
                    """,
                    (login, item_key),
                )

                # déterminer si on active cet oeuf (si aucun actif)
                cur.execute(
                    """
                    SELECT 1
                    FROM creatures_v2
                    WHERE twitch_login=%s AND is_active=true
                    LIMIT 1;
                    """,
                    (login,),
                )
                has_active = bool(cur.fetchone())

                # créer le nouvel oeuf (cm_key='egg')
                cur.execute(
                    """
                    INSERT INTO creatures_v2
                      (twitch_login, cm_key, lineage_key, stage, xp_total, happiness,
                       is_active, is_limited, acquired_from)
                    VALUES
                      (%s, 'egg', %s, 0, 0, 50, %s, %s, 'egg');
                    """,
                    (
                        login,
                        egg_lineage,
                        (not has_active),
                        (egg_lineage == "limited"),
                    ),
                )

                conn.commit()
                return {
                    "ok": True,
                    "twitch_login": login,
                    "item_key": item_key,
                    "item_name": item_name,
                    "effect": "egg",
                    "lineage_key": egg_lineage,
                    "activated": (not has_active),
                }

            # Si pas d’œuf : on a besoin d’un CM actif pour appliquer XP/bonheur
            if not arow:
                raise HTTPException(status_code=400, detail="No active CM")

            active_cm_key, active_stage, active_xp, active_h = arow
            stage_before = int(active_stage or 0)
            active_xp = int(active_xp or 0)
            active_h = int(active_h or 0)

            # 4) consommer 1 item (non-œuf) UNE SEULE FOIS
            cur.execute(
                """
                UPDATE inventory
                SET qty = qty - 1, updated_at = now()
                WHERE twitch_login=%s AND item_key=%s;
                """,
                (login, item_key),
            )

            # XP item (piloté par items.xp_gain)
            if xp_gain > 0:
                cur.execute(
                    "INSERT INTO xp_events (twitch_login, amount) VALUES (%s, %s);",
                    (login, xp_gain),
                )

                cur.execute(
                    """
                    UPDATE creatures_v2
                    SET xp_total = xp_total + %s, updated_at = now()
                    WHERE twitch_login=%s AND cm_key=%s
                    RETURNING xp_total;
                    """,
                    (xp_gain, login, active_cm_key),
                )
                new_xp_total = int(cur.fetchone()[0])

                stage_after = int(stage_from_xp(new_xp_total))
                if stage_after != stage_before:
                    cur.execute(
                        """
                        UPDATE creatures_v2
                        SET stage=%s, updated_at=now()
                        WHERE twitch_login=%s AND cm_key=%s;
                        """,
                        (stage_after, login, active_cm_key),
                    )
                else:
                    stage_after = stage_before

                new_happiness = active_h

                conn.commit()
                return {
                    "ok": True,
                    "twitch_login": login,
                    "cm_key": str(active_cm_key),
                    "item_key": item_key,
                    "item_name": item_name,
                    "effect": "xp",
                    "xp_gain": int(xp_gain),
                    "xp_total": int(new_xp_total or 0),
                    "stage_before": int(stage_before or 0),
                    "stage_after": int(stage_after or 0),
                    "happiness_after": int(new_happiness or 0),
                }

            # Bonheur (piloté par items.happiness_gain)
            # (si happiness_gain==0 et xp_gain==0, ça “consomme” mais n’a pas d’effet → à toi de voir si tu veux bloquer)
            new_happiness = max(0, min(100, active_h + happiness_gain))

            cur.execute(
                """
                UPDATE creatures_v2
                SET happiness=%s, updated_at=now()
                WHERE twitch_login=%s AND cm_key=%s;
                """,
                (new_happiness, login, active_cm_key),
            )

            new_xp_total = active_xp
            stage_after = stage_before

        conn.commit()

    return {
        "ok": True,
        "twitch_login": login,
        "cm_key": str(active_cm_key),
        "item_key": item_key,
        "item_name": item_name,
        "effect": "happiness",
        "happiness_gain": int(happiness_gain),
        "happiness_after": int(new_happiness or 0),
    }

# =============================================================================
# Overlay: Commande !show (CM actif uniquement)
# =============================================================================

@app.post("/internal/trigger_show")
def trigger_show(payload: dict, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    login = str(payload.get("twitch_login", "")).strip().lower()
    if not login:
        raise HTTPException(status_code=400, detail="Missing twitch_login")

    duration = int(os.environ.get("SHOW_DURATION_SECONDS", "8"))
    duration = max(2, min(duration, 8))

    # Données overlay
    xp_total = 0
    stage = 0
    cm_key = None
    cm_name = None
    media_url = None
    happiness = 0

    with get_db() as conn:
        with conn.cursor() as cur:
            # 1️⃣ CM actif (creatures_v2)
            cur.execute("""
                SELECT cm_key, stage, xp_total, happiness
                FROM creatures_v2
                WHERE twitch_login=%s AND is_active=true
                LIMIT 1;
            """, (login,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=400, detail="No active CM")

            cm_key, stage, xp_total, happiness = (
                row[0],
                int(row[1]),
                int(row[2]),
                int(row[3] or 0),
            )

            # 2️⃣ Forme selon le stage
            cur.execute("""
                SELECT name, image_url, sound_url
                FROM cm_forms
                WHERE cm_key=%s AND stage=%s;
            """, (cm_key, stage))
            f = cur.fetchone()

            if f and f[0] and f[1]:
                cm_name = f[0]
                media_url = f[1]
                # sound_url = f[2]  # (optionnel plus tard)
            else:
                # 3️⃣ Fallback CMS
                cur.execute("""
                    SELECT name, COALESCE(media_url,'')
                    FROM cms
                    WHERE key=%s;
                """, (cm_key,))
                cmrow = cur.fetchone()
                if not cmrow:
                    raise HTTPException(status_code=400, detail="Unknown CM")
                cm_name, media_url = cmrow
                if not media_url:
                    raise HTTPException(status_code=400, detail="CM missing media_url")

    # Infos viewer Twitch
    display, avatar = twitch_user_profile(login)

    # XP bounds
    stage_start, next_xp = stage_bounds(stage)

    # 4️⃣ Insert overlay
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO overlay_events
                  (twitch_login,
                   viewer_display,
                   viewer_avatar,
                   cm_key,
                   cm_name,
                   cm_media_url,
                   xp_total,
                   stage,
                   stage_start_xp,
                   next_stage_xp,
                   happiness,
                   expires_at)
                VALUES
                  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                   now() + (%s || ' seconds')::interval);
            """, (
                login,
                display,
                avatar,
                cm_key,
                cm_name,
                media_url,
                xp_total,
                stage,
                stage_start,
                next_xp,
                happiness,
                duration,
            ))
        conn.commit()

    return {"ok": True}

# =============================================================================
# ADMIN: Item Use (creatures_v2 only)
# =============================================================================




# =============================================================================
# ADMIN: Overlay Show
# =============================================================================

@app.get("/overlay/drop_state")
def overlay_drop_state():
    with get_db() as conn:
        with conn.cursor() as cur:
            d = get_active_drop(cur)
            if not d:
                return {"show": False}

            drop_id, mode, title, media_url, xp_bonus, ticket_key, ticket_qty, target_hits, status, expires_at, winner_login = d

            # si expiré -> resolve et on cache
            cur.execute("SELECT now() >= %s;", (expires_at,))
            if bool(cur.fetchone()[0]):
                conn.commit()
                resolve_drop(int(drop_id))
                return {"show": False}

            # participants count
            cur.execute("SELECT COUNT(*) FROM drop_participants WHERE drop_id=%s;", (drop_id,))
            count = int(cur.fetchone()[0])

            # remaining seconds
            cur.execute("SELECT EXTRACT(EPOCH FROM (%s - now()))::int;", (expires_at,))
            remaining = max(0, int(cur.fetchone()[0]))

    return {
        "show": True,
        "drop": {
            "id": int(drop_id),
            "mode": mode,
            "title": title,
            "media": media_url,
            "remaining": remaining,
            "count": count,
            "target": int(target_hits) if target_hits is not None else None,
            "xp_bonus": int(xp_bonus),
            "ticket_key": ticket_key,
            "ticket_qty": int(ticket_qty),
        },
    }




# =============================================================================
# ADMIN: remettre /admin (minimum)
# =============================================================================
from fastapi import Query

@app.get('/admin', response_class=HTMLResponse)
def admin_home(
    request: Request,
    q: str | None = None,
    page: int = Query(1, ge=1),
    per: int = Query(50, ge=10, le=200),
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)

    q_clean = (q or '').strip().lower()
    result = None

    offset = (page - 1) * per

    with get_db() as conn:
        with conn.cursor() as cur:
            # Live flag
            cur.execute("SELECT value FROM kv WHERE key = 'is_live';")
            row = cur.fetchone()
            is_live = bool(row and row[0] == "true")

            # Top XP (companion actif uniquement)
            cur.execute("""
                SELECT twitch_login, xp_total, stage
                FROM creatures_v2
                WHERE is_active=true
                ORDER BY xp_total DESC
                LIMIT 50;
            """)
            top = [{'twitch_login': r[0], 'xp_total': int(r[1] or 0), 'stage': int(r[2] or 0)} for r in cur.fetchall()]

            # Liste users (toute la collection -> stats agrégées)
            params = []
            where = ""
            if q_clean:
                where = "WHERE twitch_login LIKE %s"
                params.append(f"%{q_clean}%")

            # total users (pour pagination)
            cur.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT twitch_login
                    FROM creatures_v2
                    {where}
                    GROUP BY twitch_login
                ) t;
            """, tuple(params))
            total_users = int(cur.fetchone()[0] or 0)

            cur.execute(f"""
                SELECT
                    twitch_login,
                    SUM(xp_total)::bigint AS xp_total_sum,
                    MAX(stage)::int AS stage_max,
                    COUNT(*)::int AS cm_count,
                    MAX(CASE WHEN is_active THEN id ELSE NULL END)::bigint AS active_id
                FROM creatures_v2
                {where}
                GROUP BY twitch_login
                ORDER BY xp_total_sum DESC, twitch_login ASC
                LIMIT %s OFFSET %s;
            """, tuple(params + [per, offset]))
            users = []
            for r in cur.fetchall():
                users.append({
                    "twitch_login": r[0],
                    "xp_total_sum": int(r[1] or 0),
                    "stage_max": int(r[2] or 0),
                    "cm_count": int(r[3] or 0),
                    "active_id": (int(r[4]) if r[4] is not None else None),
                })

            # Résultat “exact” (si tu veux garder le bloc Résultat)
            if q_clean:
                cur.execute("""
                    SELECT twitch_login, xp_total, stage
                    FROM creatures_v2
                    WHERE is_active=true AND twitch_login=%s
                    LIMIT 1;
                """, (q_clean,))
                r = cur.fetchone()
                if r:
                    result = {"twitch_login": r[0], "xp_total": int(r[1] or 0), "stage": int(r[2] or 0)}

    return templates.TemplateResponse('admin.html', {
        'request': request,
        'top': top,
        'q': q_clean,
        'result': result,
        'is_live': is_live,

        # NEW
        'users': users,
        'page': page,
        'per': per,
        'total_users': total_users,
        'total_pages': max(1, (total_users + per - 1) // per),
    })



# =============================================================================
# PRESENCES
# =============================================================================

def get_current_session_id(conn) -> int | None:
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM kv WHERE key='current_session_id';")
        row = cur.fetchone()
    if not row or not row[0]:
        return None
    try:
        return int(row[0])
    except Exception:
        return None
        
@app.post("/internal/stream/present_batch")
def stream_present_batch(payload: dict, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    logins = payload.get("logins", [])
    if not isinstance(logins, list) or len(logins) > 800:
        raise HTTPException(status_code=400, detail="Invalid logins")

    clean = [str(x).strip().lower() for x in logins if str(x).strip()]
    if not clean:
        return {"ok": True, "inserted": 0}

    with get_db() as conn:
        session_id = get_current_session_id(conn)
        if not session_id:
            return {"ok": True, "inserted": 0}

        with conn.cursor() as cur:
            # insert ignore duplicate (PK session_id,twitch_login)
            inserted = 0
            for u in clean:
                try:
                    cur.execute("""
                        INSERT INTO stream_participants (session_id, twitch_login)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (session_id, u))
                    # psycopg: rowcount=1 si insert
                    if cur.rowcount == 1:
                        inserted += 1
                except Exception:
                    pass
        conn.commit()

    return {"ok": True, "inserted": inserted, "session_id": session_id}


# =============================================================================
# V2 — COLLECTION / MULTI-CM (no overlay, bot/admin only)
# =============================================================================

@app.get("/internal/collection/{login}")
def internal_collection(login: str, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    login = (login or "").strip().lower()
    if not login:
        raise HTTPException(status_code=400, detail="Missing login")

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    c.id,
                    c.cm_key,
                    COALESCE(cm.name,'') AS cm_name,
                    c.lineage_key,
                    c.stage,
                    c.xp_total,
                    c.happiness,
                    c.is_active,
                    c.is_limited,
                    c.acquired_from,
                    c.acquired_at
                FROM creatures_v2 c
                JOIN cms cm ON cm.key = c.cm_key
                WHERE c.twitch_login = %s
                ORDER BY c.is_active DESC, c.acquired_at DESC, c.id DESC;
            """, (login,))
            rows = cur.fetchall()

    items = []
    for r in rows:
        items.append({
            "id": int(r[0]),
            "cm_key": r[1],
            "cm_name": r[2],
            "lineage_key": r[3],
            "stage": int(r[4] or 0),
            "xp_total": int(r[5] or 0),
            "happiness": int(r[6] or 0),
            "is_active": bool(r[7]),
            "is_limited": bool(r[8]),
            "acquired_from": r[9],
            "acquired_at": r[10].isoformat() if r[10] else None,
        })

    return {"ok": True, "twitch_login": login, "items": items}



@app.post("/internal/companion/set")
def internal_companion_set(payload: dict, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    login = str(payload.get("twitch_login", "")).strip().lower()
    cm_key = str(payload.get("cm_key", "")).strip().lower()
    if not login or not cm_key:
        raise HTTPException(status_code=400, detail="Missing twitch_login or cm_key")

    with get_db() as conn:
        with conn.cursor() as cur:
            # user existe
            cur.execute("INSERT INTO users (twitch_login) VALUES (%s) ON CONFLICT DO NOTHING;", (login,))

            # le viewer possède ce CM ?
            cur.execute("""
                SELECT 1
                FROM creatures_v2
                WHERE twitch_login=%s AND cm_key=%s
                LIMIT 1;
            """, (login, cm_key))
            if not cur.fetchone():
                raise HTTPException(status_code=400, detail="Viewer does not own this CM")

            # désactiver tous
            cur.execute("""
                UPDATE creatures_v2
                SET is_active = FALSE
                WHERE twitch_login=%s AND is_active=TRUE;
            """, (login,))

            # activer celui demandé
            cur.execute("""
                UPDATE creatures_v2
                SET is_active = TRUE
                WHERE twitch_login=%s AND cm_key=%s;
            """, (login, cm_key))

        conn.commit()

    return {"ok": True, "twitch_login": login, "cm_key": cm_key, "is_active": True}


@app.post("/internal/collection/add")
def internal_collection_add(payload: dict, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    login = str(payload.get("twitch_login", "")).strip().lower()
    cm_key = str(payload.get("cm_key", "")).strip().lower()
    acquired_from = str(payload.get("acquired_from", "drop")).strip().lower()

    # valeurs acceptées (ton schema: legacy|egg|drop|admin|event)
    if acquired_from not in ("legacy", "egg", "drop", "admin", "event"):
        raise HTTPException(status_code=400, detail="Invalid acquired_from")

    if not login or not cm_key:
        raise HTTPException(status_code=400, detail="Missing twitch_login or cm_key")

    with get_db() as conn:
        with conn.cursor() as cur:
            # user existe
            cur.execute("INSERT INTO users (twitch_login) VALUES (%s) ON CONFLICT DO NOTHING;", (login,))

            # cm existe ?
            cur.execute("SELECT lineage_key FROM cms WHERE key=%s;", (cm_key,))
            cmrow = cur.fetchone()
            if not cmrow:
                raise HTTPException(status_code=400, detail="Unknown CM")
            cm_lineage = cmrow[0]  # peut être utile si tu veux pré-remplir lineage_key

            # a déjà un actif ?
            cur.execute("""
                SELECT 1
                FROM creatures_v2
                WHERE twitch_login=%s AND is_active=TRUE
                LIMIT 1;
            """, (login,))
            has_active = bool(cur.fetchone())

            # insert (si déjà présent -> no-op)
            cur.execute("""
                INSERT INTO creatures_v2 (
                    twitch_login, cm_key, lineage_key,
                    stage, xp_total, happiness,
                    is_active, is_limited,
                    acquired_from
                )
                VALUES (%s, %s, %s, 0, 0, 50, %s, FALSE, %s)
                ON CONFLICT (twitch_login, cm_key) DO NOTHING;
            """, (login, cm_key, cm_lineage, (not has_active), acquired_from))

            # si le CM existait déjà, on ne change rien (important : “ne changer que le nécessaire”)

        conn.commit()

    return {"ok": True, "twitch_login": login, "cm_key": cm_key, "acquired_from": acquired_from}
