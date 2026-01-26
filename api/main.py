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

from fastapi import FastAPI, Header, HTTPException, Request, Form, Depends
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

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users (twitch_login) VALUES (%s) ON CONFLICT DO NOTHING;", (login,))
            cur.execute(
                """
                INSERT INTO creatures (twitch_login, xp_total, stage)
                VALUES (%s, 0, 0)
                ON CONFLICT (twitch_login) DO NOTHING;
                """,
                (login,),
            )
            cur.execute("INSERT INTO xp_events (twitch_login, amount) VALUES (%s, %s);", (login, amount))

            cur.execute("SELECT stage FROM creatures WHERE twitch_login=%s;", (login,))
            prev_stage = int(cur.fetchone()[0])

            cur.execute(
                """
                UPDATE creatures
                SET xp_total = xp_total + %s, updated_at=now()
                WHERE twitch_login=%s
                RETURNING xp_total;
                """,
                (amount, login),
            )
            new_xp_total = int(cur.fetchone()[0])
            new_stage = stage_from_xp(new_xp_total)
            cur.execute("UPDATE creatures SET stage=%s, updated_at=now() WHERE twitch_login=%s;", (new_stage, login))

            if prev_stage == 0 and new_stage >= 1:
                cur.execute("SELECT lineage_key, cm_key FROM creatures WHERE twitch_login=%s;", (login,))
                lrow = cur.fetchone()
                lineage_key = lrow[0] if lrow else None
                current_cm = lrow[1] if lrow else None
                if lineage_key and current_cm is None:
                    cm_key = pick_cm_for_lineage(conn, lineage_key)
                    if cm_key:
                        cur.execute("UPDATE creatures SET cm_key=%s, updated_at=now() WHERE twitch_login=%s;", (cm_key, login))

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
        if sub_type in ("stream.online", "stream.offline"):
            is_live = "true" if sub_type == "stream.online" else "false"

            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO kv (key, value)
                        VALUES ('is_live', %s)
                        ON CONFLICT (key) DO UPDATE
                        SET value = EXCLUDED.value, updated_at = now();
                    """, (is_live,))
                conn.commit()

        return "ok"

    # 3) Revocation (Twitch désactive une sub)
    if msg_type == "revocation":
        return "ok"

    return "ok"


# =============================================================================
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
    duration = int(payload.get('duration_seconds', 25))
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

@app.post("/internal/choose_lineage")
def choose_lineage(payload: dict, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    login = str(payload.get("twitch_login", "")).strip().lower()
    lineage_key = str(payload.get("lineage_key", "")).strip().lower()
    if not login or not lineage_key:
        raise HTTPException(status_code=400, detail="Missing twitch_login or lineage_key")

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT is_enabled FROM lineages WHERE key=%s;", (lineage_key,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=400, detail="Unknown lineage")
            if not bool(row[0]):
                raise HTTPException(status_code=400, detail="Lineage disabled")

            cur.execute("""
                INSERT INTO creatures (twitch_login, xp_total, stage)
                VALUES (%s, 0, 0)
                ON CONFLICT (twitch_login) DO NOTHING;
            """, (login,))

            cur.execute("SELECT stage FROM creatures WHERE twitch_login=%s;", (login,))
            stage = int(cur.fetchone()[0])
            if stage != 0:
                raise HTTPException(status_code=400, detail="Choose only before hatching (egg stage)")

            cur.execute("""
                UPDATE creatures
                SET lineage_key=%s, updated_at=now()
                WHERE twitch_login=%s;
            """, (lineage_key, login))
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
            # ensure creature exists
            cur.execute("""
                INSERT INTO creatures (twitch_login, xp_total, stage)
                VALUES (%s, 0, 0)
                ON CONFLICT (twitch_login) DO NOTHING;
            """, (login,))

            if action == "reset":
                # reset complet (œuf + efface lignée + efface CM)
                cur.execute("""
                    UPDATE creatures
                    SET xp_total = 0,
                        stage = 0,
                        lineage_key = NULL,
                        cm_key = NULL,
                        updated_at = now()
                    WHERE twitch_login = %s;
                """, (login,))
                conn.commit()
                return RedirectResponse(
                    url=f"/admin/user/{login}?flash_kind=ok&flash=Reset%20complet",
                    status_code=303,
                )

            if action == "set":
                new_xp = max(0, int(amount))
            else:  # give
                cur.execute("SELECT xp_total FROM creatures WHERE twitch_login=%s;", (login,))
                current = int(cur.fetchone()[0])
                new_xp = current + max(0, int(amount))

            new_stage = stage_from_xp(int(new_xp))
            cur.execute("""
                UPDATE creatures
                SET xp_total=%s, stage=%s, updated_at=now()
                WHERE twitch_login=%s;
            """, (int(new_xp), int(new_stage), login))

        conn.commit()

    msg = "OK"
    if action == "set":
        msg = f"XP%20fix%C3%A9%20%C3%A0%20{new_xp}"
    elif action == "give":
        msg = f"+{amount}%20XP%20(total%20{new_xp})"

    return RedirectResponse(url=f"/admin/user/{login}?flash_kind=ok&flash={msg}", status_code=303)

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
            cur.execute("SELECT xp_total, stage, lineage_key, cm_key FROM creatures WHERE twitch_login=%s;", (login,))
            row = cur.fetchone()

    if not row:
        xp_total, stage, lineage_key, cm_key = 0, 0, None, None
    else:
        xp_total, stage, lineage_key, cm_key = row

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
    })



@app.get("/internal/creature/{login}")
def creature_state(login: str, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    login = login.strip().lower()
    if not login:
        raise HTTPException(status_code=400, detail="Missing login")

    xp_total, stage, lineage_key, cm_key = 0, 0, None, None
    form_name, form_image_url, form_sound_url = None, None, None

    with get_db() as conn:
        with conn.cursor() as cur:
            # 1) état de base
            cur.execute("""
                SELECT xp_total, stage, lineage_key, cm_key
                FROM creatures
                WHERE twitch_login=%s;
            """, (login,))
            row = cur.fetchone()
            if row:
                xp_total, stage, lineage_key, cm_key = row

            stage = int(stage or 0)
            xp_total = int(xp_total or 0)

            # 2) forme (uniquement si stage >= 1 et cm_key défini)
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
    }



@app.post("/internal/item/use")
def use_item(payload: dict, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    login = payload.get("twitch_login", "").strip().lower()
    item_key = payload.get("item_key", "").strip()

    if not login or not item_key:
        raise HTTPException(status_code=400, detail="Missing data")

    with get_db() as conn:
        with conn.cursor() as cur:
            # Vérifier inventaire
            cur.execute(
                """
                SELECT qty FROM inventory
                WHERE twitch_login=%s AND item_key=%s;
                """,
                (login, item_key),
            )
            row = cur.fetchone()
            if not row or row[0] <= 0:
                raise HTTPException(status_code=400, detail="No item")

            # Consommer l'objet
            cur.execute(
                """
                UPDATE inventory
                SET qty = qty - 1, updated_at = now()
                WHERE twitch_login=%s AND item_key=%s;
                """,
                (login, item_key),
            )

        conn.commit()

    # Effets (MVP)
    if item_key == "xp_capsule":
        grant_xp(login, 30)
        return {"ok": True, "effect": "xp", "amount": 30}

    return {"ok": True, "effect": "none"}


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
    # HTML simple (pas de template obligatoire)
    return HTMLResponse("""
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

    /* état animé */
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

  /* Barre XP */
  .barWrap{
    width:420px;
    height:32px;
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

  .cmname{
    font-size:28px;
    font-weight:900;
    color:#e6edf3;
    text-align:center;
    line-height:1.1;
  }
  .xptext{
    font-size:13px;
    color:#9aa4b2;
    text-align:center;
    margin-top:-6px;
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
      <div class="barWrap"><div id="fill" class="fill"></div></div>
      <div id="cmname" class="cmname">CapsMons</div>
      <div id="xptext" class="xptext"></div>
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
    void card.offsetWidth; // force reflow
    card.classList.add('showing');
    showing = true;
  }

  // reset timer
  if (hideTimer) clearTimeout(hideTimer);
  hideTimer = setTimeout(hideCard, DISPLAY_MS);
}

function hideCard(){
  const card = document.getElementById('card');
  if (!showing) return;

  card.classList.remove('showing');
  setTimeout(() => {
    card.style.display = 'none';
  }, 230);

  showing = false;
  hideTimer = null;
}

async function tick(){
  try{
    const r = await fetch('/overlay/state', {cache:'no-store'});
    const j = await r.json();

    if(!j.show){
      // on NE cache PLUS ici → timer only
      return;
    }

    const sig = `${j.viewer.name}|${j.cm.name}|${j.xp.total}`;

    if (sig !== lastSig) {
      lastSig = sig;
      playSfx();      // 🔊 SON SYNCHRONISÉ
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
    document.getElementById('xptext').textContent =
      toNext
        ? `${j.xp.total} XP • prochain palier dans ${toNext} XP`
        : `${j.xp.total} XP • stade max`;

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
            cur.execute("SELECT key, name, is_enabled FROM lineages ORDER BY key;")
            lineages = [{"key": r[0], "name": r[1], "is_enabled": bool(r[2])} for r in cur.fetchall()]

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
                cur.execute("UPDATE creatures SET cm_key=NULL, updated_at=now() WHERE cm_key=%s;", (key,))
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

# =============================================================================
# Overlay: Commande !show
# =============================================================================

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

    prev_stage = 0
    new_xp_total = 0
    new_stage = 0
    cm_assigned = None

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users (twitch_login) VALUES (%s) ON CONFLICT DO NOTHING;", (login,))
            cur.execute("""
                INSERT INTO creatures (twitch_login, xp_total, stage)
                VALUES (%s, 0, 0)
                ON CONFLICT (twitch_login) DO NOTHING;
            """, (login,))

            cur.execute("INSERT INTO xp_events (twitch_login, amount) VALUES (%s, %s);", (login, amount))

            cur.execute("SELECT stage FROM creatures WHERE twitch_login=%s;", (login,))
            prev_stage = int(cur.fetchone()[0])

            cur.execute("""
                UPDATE creatures
                SET xp_total = xp_total + %s, updated_at = now()
                WHERE twitch_login = %s
                RETURNING xp_total;
            """, (amount, login))
            new_xp_total = int(cur.fetchone()[0])
            new_stage = stage_from_xp(new_xp_total)

            cur.execute("UPDATE creatures SET stage=%s, updated_at=now() WHERE twitch_login=%s;", (new_stage, login))

            if prev_stage == 0 and new_stage >= 1:
                cur.execute("SELECT lineage_key, cm_key FROM creatures WHERE twitch_login=%s;", (login,))
                lrow = cur.fetchone()
                lineage_key = lrow[0] if lrow else None
                current_cm = lrow[1] if lrow else None

                if lineage_key and current_cm is None:
                    cm_key = pick_cm_for_lineage(conn, lineage_key)
                    if cm_key:
                        cur.execute("UPDATE creatures SET cm_key=%s, updated_at=now() WHERE twitch_login=%s;", (cm_key, login))
                        cm_assigned = cm_key

        conn.commit()

    return {
        "ok": True,
        "twitch_login": login,
        "xp_total": new_xp_total,
        "stage_before": prev_stage,
        "stage_after": new_stage,
        "cm_assigned": cm_assigned,
    }


@app.post("/internal/trigger_show")
def trigger_show(payload: dict, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    login = str(payload.get("twitch_login", "")).strip().lower()
    if not login:
        raise HTTPException(status_code=400, detail="Missing twitch_login")

    duration = int(os.environ.get("SHOW_DURATION_SECONDS", "7"))
    duration = max(2, min(duration, 15))

    # Données overlay
    xp_total = 0
    stage = 0
    cm_key = None
    cm_name = None
    media_url = None

    with get_db() as conn:
        with conn.cursor() as cur:
            # 1) état creature
            cur.execute("SELECT xp_total, stage, cm_key FROM creatures WHERE twitch_login=%s;", (login,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=400, detail="No creature")

            xp_total, stage, cm_key = int(row[0]), int(row[1]), row[2]

            # 2) oeuf
            if stage == 0 or not cm_key:
                cm_key = "egg"
                cm_name = "Œuf"
                media_url = os.environ.get("EGG_MEDIA_URL", "").strip()
                if not media_url:
                    raise HTTPException(status_code=400, detail="EGG_MEDIA_URL missing")

            else:
                # 3) forme par stage (prioritaire)
                cur.execute("""
                    SELECT name, image_url, sound_url
                    FROM cm_forms
                    WHERE cm_key=%s AND stage=%s;
                """, (cm_key, stage))
                f = cur.fetchone()

                if f and f[0] and f[1]:
                    cm_name = f[0]
                    media_url = f[1]
                    # sound_url = f[2]  # (optionnel plus tard dans overlay_events)
                else:
                    # 4) fallback: cms table
                    cur.execute("SELECT name, COALESCE(media_url,'') FROM cms WHERE key=%s;", (cm_key,))
                    cmrow = cur.fetchone()
                    if not cmrow:
                        raise HTTPException(status_code=400, detail="Unknown CM")
                    cm_name, media_url = cmrow[0], cmrow[1]
                    if not media_url:
                        raise HTTPException(status_code=400, detail="CM missing media_url")

    display, avatar = twitch_user_profile(login)
    stage_start, next_xp = stage_bounds(stage)

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO overlay_events
                  (twitch_login, viewer_display, viewer_avatar, cm_key, cm_name, cm_media_url,
                   xp_total, stage, stage_start_xp, next_stage_xp, expires_at)
                VALUES
                  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now() + (%s || ' seconds')::interval);
            """, (login, display, avatar, cm_key, cm_name, media_url, xp_total, stage, stage_start, next_xp, duration))
        conn.commit()

    return {"ok": True}


# =============================================================================
# ADMIN: Overlay Show
# =============================================================================

@app.get("/overlay/state")
def overlay_state():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT viewer_display, viewer_avatar, cm_name, cm_media_url,
                       xp_total, stage, stage_start_xp, next_stage_xp
                FROM overlay_events
                WHERE expires_at > now()
                ORDER BY id DESC
                LIMIT 1;
            """)
            row = cur.fetchone()

    if not row:
        return {"show": False}

    viewer_display, viewer_avatar, cm_name, cm_media_url, xp_total, stage, start_xp, next_xp = row
    xp_total = int(xp_total)
    start_xp = int(start_xp)
    next_xp = int(next_xp) if next_xp is not None else None

    pct = None
    if next_xp is not None and next_xp > start_xp:
        pct = int(((xp_total - start_xp) / (next_xp - start_xp)) * 100)
        pct = max(0, min(pct, 100))

    return {
        "show": True,
        "viewer": {"name": viewer_display, "avatar": viewer_avatar},
        "cm": {"name": cm_name, "media": cm_media_url},
        "xp": {
            "total": xp_total,
            "stage": int(stage),
            "pct": pct,
            "to_next": (next_xp - xp_total) if next_xp else None,
        },
    }


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
@app.get('/admin', response_class=HTMLResponse)
def admin_home(request: Request, q: str | None = None, credentials: HTTPBasicCredentials = Depends(security)):
    require_admin(credentials)

    q_clean = (q or '').strip().lower()
    result = None

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute('''
                SELECT twitch_login, xp_total, stage
                FROM creatures
                ORDER BY xp_total DESC
                LIMIT 50;
            ''')
            top = [{'twitch_login': r[0], 'xp_total': r[1], 'stage': r[2]} for r in cur.fetchall()]

            if q_clean:
                cur.execute('''
                    SELECT twitch_login, xp_total, stage
                    FROM creatures
                    WHERE twitch_login = %s;
                ''', (q_clean,))
                row = cur.fetchone()
                if row:
                    result = {'twitch_login': row[0], 'xp_total': row[1], 'stage': row[2]}

    return templates.TemplateResponse('admin.html', {'request': request, 'top': top, 'q': q_clean, 'result': result})
