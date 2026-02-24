
def column_exists(cur, table_name: str, column_name: str, schema: str = "public") -> bool:
    """Retourne True si la colonne existe (information_schema)."""
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s AND column_name=%s
        LIMIT 1;
        """,
        (schema, table_name, column_name),
    )
    return cur.fetchone() is not None

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
import asyncio
import random
import secrets
import hmac
import hashlib
import urllib.parse
import requests
import psycopg
import httpx

from fastapi import FastAPI, Header, HTTPException, Request, Form, Body, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

TWITCH_CLIENT_ID = os.environ["TWITCH_CLIENT_ID"]
TWITCH_CLIENT_SECRET = os.environ["TWITCH_CLIENT_SECRET"]
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "https://capsmons.devlooping.fr")
TWITCH_REDIRECT_URI = os.environ.get("TWITCH_REDIRECT_URI", f"{PUBLIC_BASE_URL}/admin/twitch/callback")

# minimum pour recevoir les redemptions; ajoute manage si tu veux pouvoir "FULFILL" ensuite
TWITCH_CP_SCOPES = "channel:read:redemptions channel:manage:redemptions"

# =============================================================================
# App / Static / Templates
# =============================================================================
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

security = HTTPBasic()
templates = Jinja2Templates(directory="templates")

_twitch_token_cache = {"token": None, "exp": 0.0}

# =============================================================================
# Root
# =============================================================================
@app.get("/", include_in_schema=False)
def root_redirect():
    # Petit confort: redirige vers l'admin (auth Basic demandée sur /admin)
    return RedirectResponse(url="/admin", status_code=302)


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



def eventsub_dedup(msg_id: str, msg_type: str, sub_type: str) -> bool:
    """Insère msg_id dans eventsub_deliveries. Retourne True si 1ère fois, False si déjà vu."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO eventsub_deliveries (msg_id, msg_type, sub_type, received_at)
                    VALUES (%s,%s,%s, now())
                    ON CONFLICT (msg_id) DO NOTHING;
                    """,
                    (msg_id, msg_type, sub_type),
                )
                inserted = (cur.rowcount == 1)
            conn.commit()
        return inserted
    except Exception:
        # en cas de souci DB, on ne bloque pas le webhook
        return True


def _announce(message: str) -> None:
    """Enregistre un message à annoncer par le bot."""
    if not message:
        return
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO bot_announcements (message)
                    VALUES (%s);
                    """,
                    (message,),
                )
            conn.commit()
    except Exception:
        pass


def _grant_item_db(cur, login: str, item_key: str, qty: int) -> None:
    login = (login or "").strip().lower()
    item_key = (item_key or "").strip()
    qty = int(qty or 0)
    if not login or not item_key or qty == 0:
        return

    # s'assure que l'utilisateur existe
    cur.execute("INSERT INTO users (twitch_login) VALUES (%s) ON CONFLICT DO NOTHING;", (login,))

    # upsert inventaire
    cur.execute(
        """
        INSERT INTO inventory (twitch_login, item_key, qty)
        VALUES (%s,%s,%s)
        ON CONFLICT (twitch_login, item_key)
        DO UPDATE SET qty = inventory.qty + EXCLUDED.qty, updated_at=now();
        """,
        (login, item_key, qty),
    )


def _pick_item_db(cur, kind: str) -> dict:
    kind = (kind or "any").strip().lower()
    where = "TRUE"
    if kind == "xp":
        where = "xp_gain > 0"
    elif kind == "candy":
        where = "happiness_gain > 0"
    elif kind == "egg":
        where = "key LIKE 'egg_%'"

    cur.execute(
        f"""
        SELECT key, name, COALESCE(icon_url,''), drop_weight, xp_gain, happiness_gain
        FROM items
        WHERE {where} AND COALESCE(drop_weight,0) > 0
        """
    )
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
        "item_key": key,
        "item_name": name,
        "icon_url": icon_url,
        "xp_gain": int(xp_gain or 0),
        "happiness_gain": int(happiness_gain or 0),
        "drop_weight": int(weight or 0),
    }


def _spawn_drop_db(cur, mode: str, title: str, media_url: str, duration: int, ticket_key: str, ticket_qty: int, target_hits: int | None = None, xp_bonus: int = 0) -> int:
    mode = (mode or "").strip().lower()
    title = (title or "").strip()
    media_url = (media_url or "").strip()
    duration = int(duration or 15)
    ticket_key = (ticket_key or "ticket_basic").strip()
    ticket_qty = int(ticket_qty or 1)
    xp_bonus = int(xp_bonus or 0)

    if mode not in ("first", "random", "coop"):
        raise HTTPException(status_code=400, detail="Invalid mode")
    if not title or not media_url:
        raise HTTPException(status_code=400, detail="Missing title/media_url")

    duration = max(5, min(duration, 30))
    ticket_qty = max(1, min(ticket_qty, 50))
    xp_bonus = max(0, min(xp_bonus, 1000))

    if mode == "coop":
        target_hits = int(target_hits or 10)
        target_hits = max(2, min(target_hits, 999))
    else:
        target_hits = None

    # expire l'ancien
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

    # Annonce au lancement
    if mode == "first":
        announce_msg = f"⚡ Drop '{title}' — tapez !grab, LE PREMIER gagne ! ({duration}s)"
    elif mode == "random":
        announce_msg = f"🎲 Drop '{title}' — tapez !grab pour tenter votre chance ! ({duration}s)"
    else:
        announce_msg = f"🤝 Drop COOP '{title}' — tapez !grab, tout le monde gagne du XP ! ({duration}s)"

    cur.execute("INSERT INTO bot_announcements (message) VALUES (%s);", (announce_msg,))

    return drop_id


def handle_channel_points_redemption(ev: dict) -> None:
    """Traite un achat via points de chaîne (EventSub redemption.add)."""
    redemption_id = str(ev.get("id", "") or "").strip()
    user_login = str(ev.get("user_login", "") or "").strip().lower()
    user_name = str(ev.get("user_name", "") or "").strip()
    reward = ev.get("reward", {}) or {}
    reward_id = str(reward.get("id", "") or "").strip()
    reward_title = str(reward.get("title", "") or "").strip()
    cost = int(reward.get("cost") or 0)

    if not redemption_id or not reward_id:
        return

    with get_db() as conn:
        with conn.cursor() as cur:
            # activé ?
            cp_enabled = (kv_get(cur, "cp_enabled", "false") == "true")
            if not cp_enabled:
                cur.execute(
                    """
                    INSERT INTO cp_redemptions (redemption_id, user_login, reward_id, reward_title, cost, status, detail, created_at)
                    VALUES (%s,%s,%s,%s,%s,'ignored','cp_disabled', now())
                    ON CONFLICT (redemption_id) DO NOTHING;
                    """,
                    (redemption_id, user_login, reward_id, reward_title, cost),
                )
                conn.commit()
                return

            # dédup redemption id
            cur.execute("SELECT 1 FROM cp_redemptions WHERE redemption_id=%s;", (redemption_id,))
            if cur.fetchone():
                conn.commit()
                return

            # mapping
            drop_reward_id = (kv_get(cur, "cp_reward_drop_coop_id", "") or "").strip()
            capsule_reward_id = (kv_get(cur, "cp_reward_capsule_id", "") or "").strip()
            candy_reward_id = (kv_get(cur, "cp_reward_candy_id", "") or "").strip()
            egg_reward_id = (kv_get(cur, "cp_reward_egg_id", "") or "").strip()

            action = None
            if drop_reward_id and reward_id == drop_reward_id:
                action = {"type": "drop_coop"}
            elif capsule_reward_id and reward_id == capsule_reward_id:
                action = {"type": "grant_capsule"}
            elif candy_reward_id and reward_id == candy_reward_id:
                action = {"type": "grant_candy"}
            elif egg_reward_id and reward_id == egg_reward_id:
                action = {"type": "grant_egg"}

            if not action:
                # non mappé -> on log seulement
                cur.execute(
                    """
                    INSERT INTO cp_redemptions (redemption_id, user_login, reward_id, reward_title, cost, status, detail, created_at)
                    VALUES (%s,%s,%s,%s,%s,'ignored','unmapped_reward', now())
                    ON CONFLICT (redemption_id) DO NOTHING;
                    """,
                    (redemption_id, user_login, reward_id, reward_title, cost),
                )
                conn.commit()
                return

            # Marque "processing" dès le départ pour éviter double traitement en cas de retry
            cur.execute(
                """
                INSERT INTO cp_redemptions (redemption_id, user_login, reward_id, reward_title, cost, status, detail, action, created_at)
                VALUES (%s,%s,%s,%s,%s,'processing','', %s::jsonb, now())
                ON CONFLICT (redemption_id) DO NOTHING;
                """,
                (redemption_id, user_login, reward_id, reward_title, cost, json.dumps(action)),
            )

            # Exécute action
            try:
                if action["type"] == "drop_coop":
                    pick_kind = (kv_get(cur, "cp_drop_pick_kind", kv_get(cur, "auto_drop_pick_kind", "any") or "any") or "any").strip().lower()
                    duration = int(kv_get(cur, "cp_drop_duration_seconds", kv_get(cur, "auto_drop_duration_seconds", "20") or "20") or 20)
                    target_hits = int(kv_get(cur, "cp_drop_target_hits", kv_get(cur, "auto_drop_target_hits", "10") or "10") or 10)
                    ticket_qty = int(kv_get(cur, "cp_drop_ticket_qty", kv_get(cur, "auto_drop_ticket_qty", "1") or "1") or 1)

                    picked = _pick_item_db(cur, pick_kind)
                    title = f"{reward_title or 'Achat drop'} — sponsorisé par {user_name or user_login}"
                    media_url = picked.get("icon_url") or ""
                    if not media_url:
                        # fallback: une icône générique si tu en as une
                        media_url = (kv_get(cur, "cp_drop_fallback_icon_url", "") or "").strip()
                    if not media_url:
                        raise HTTPException(status_code=500, detail="Missing icon_url for picked item")

                    drop_id = _spawn_drop_db(
                        cur,
                        mode="coop",
                        title=title,
                        media_url=media_url,
                        duration=duration,
                        ticket_key=picked["item_key"],
                        ticket_qty=ticket_qty,
                        target_hits=target_hits,
                        xp_bonus=0,
                    )
                    _announce(f"🤝 Drop COOP lancé par {user_name or user_login} ! Tapez !grab pour participer — XP progressif selon le nombre de participants !")

                    cur.execute(
                        "UPDATE cp_redemptions SET status='ok', detail=%s, drop_id=%s, processed_at=now() WHERE redemption_id=%s;",
                        (f"drop_id={drop_id}", drop_id, redemption_id),
                    )

                elif action["type"] == "grant_capsule":
                    item_key = (kv_get(cur, "cp_capsule_item_key", "grande_capsule") or "grande_capsule").strip()
                    _grant_item_db(cur, user_login, item_key, 1)
                    _announce(f"🪙 {user_name or user_login} a acheté {item_key} !")
                    cur.execute(
                        "UPDATE cp_redemptions SET status='ok', detail=%s, processed_at=now() WHERE redemption_id=%s;",
                        (f"granted:{item_key}", redemption_id),
                    )

                elif action["type"] == "grant_candy":
                    item_key = (kv_get(cur, "cp_candy_item_key", "bonbon_2") or "bonbon_2").strip()
                    _grant_item_db(cur, user_login, item_key, 1)
                    _announce(f"🪙 {user_name or user_login} a acheté {item_key} !")
                    cur.execute(
                        "UPDATE cp_redemptions SET status='ok', detail=%s, processed_at=now() WHERE redemption_id=%s;",
                        (f"granted:{item_key}", redemption_id),
                    )

                elif action["type"] == "grant_egg":
                    forced = (kv_get(cur, "cp_egg_item_key", "") or "").strip()
                    if forced:
                        item_key = forced
                    else:
                        picked = _pick_item_db(cur, "egg")
                        item_key = picked["item_key"]
                    _grant_item_db(cur, user_login, item_key, 1)
                    _announce(f"🪙 {user_name or user_login} a acheté un œuf ({item_key}) !")
                    cur.execute(
                        "UPDATE cp_redemptions SET status='ok', detail=%s, processed_at=now() WHERE redemption_id=%s;",
                        (f"granted:{item_key}", redemption_id),
                    )

                else:
                    cur.execute(
                        "UPDATE cp_redemptions SET status='ignored', detail='unknown_action', processed_at=now() WHERE redemption_id=%s;",
                        (redemption_id,),
                    )

            except Exception as e:
                cur.execute(
                    "UPDATE cp_redemptions SET status='error', detail=%s, processed_at=now() WHERE redemption_id=%s;",
                    (str(e)[:400], redemption_id),
                )
                # En cas d'erreur, on renvoie une exception -> Twitch retry le webhook.
                raise

        conn.commit()


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
    Si aucun => active un œuf existant (cm_key='egg') sinon en crée un.
    """
    login = (login or "").strip().lower()
    if not login:
        return

    with conn.cursor() as cur:
        # déjà un actif ?
        cur.execute(
            """
            SELECT 1
            FROM creatures_v2
            WHERE twitch_login=%s AND is_active=true
            LIMIT 1;
            """,
            (login,),
        )
        if cur.fetchone():
            return

        # désactiver d'éventuels autres (sécurité)
        cur.execute(
            """
            UPDATE creatures_v2
            SET is_active=false
            WHERE twitch_login=%s AND is_active=true;
            """,
            (login,),
        )

        # prendre un œuf existant si possible
        cur.execute(
            """
            SELECT id
            FROM creatures_v2
            WHERE twitch_login=%s AND cm_key='egg'
            ORDER BY acquired_at ASC NULLS LAST, id ASC
            LIMIT 1;
            """,
            (login,),
        )
        row = cur.fetchone()
        if row:
            egg_id = int(row[0])
            cur.execute(
                """
                UPDATE creatures_v2
                SET is_active=true, updated_at=now()
                WHERE id=%s;
                """,
                (egg_id,),
            )
            return

        # sinon, on crée un œuf actif
        cur.execute(
            """
            INSERT INTO creatures_v2
                (twitch_login, cm_key, lineage_key, stage, xp_total, happiness, is_active, acquired_from)
            VALUES
                (%s, 'egg', NULL, 0, 0, 50, TRUE, 'legacy');
            """,
            (login,),
        )

@app.get("/admin/twitch/connect")
def admin_twitch_connect(credentials: HTTPBasicCredentials = Depends(security)):
    require_admin(credentials)
    state = secrets.token_urlsafe(16)
    kv_set("twitch_oauth_state", state)

    params = {
        "client_id": TWITCH_CLIENT_ID,
        "redirect_uri": TWITCH_REDIRECT_URI,
        "response_type": "code",
        "scope": TWITCH_CP_SCOPES,
        "state": state,
        "force_verify": "true",
    }
    url = "https://id.twitch.tv/oauth2/authorize?" + urllib.parse.urlencode(params)
    return RedirectResponse(url)

@app.get("/admin/twitch/callback")
async def admin_twitch_callback(
    credentials: HTTPBasicCredentials = Depends(security),
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
):
    require_admin(credentials)

    if error:
        return HTMLResponse(f"OAuth error: {error}", status_code=400)

    # Lire le state attendu directement depuis la DB (connexion fraîche)
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM kv WHERE key='twitch_oauth_state';")
            row = cur.fetchone()
            expected = row[0] if row else None

    if not code or not state or not expected or state != expected:
        return HTMLResponse(
            f"Bad OAuth state/code (state={state!r}, expected={expected!r})",
            status_code=400
        )

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            "https://id.twitch.tv/oauth2/token",
            data={
                "client_id": TWITCH_CLIENT_ID,
                "client_secret": TWITCH_CLIENT_SECRET,
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": TWITCH_REDIRECT_URI,
            },
        )
    j = r.json()
    if r.status_code != 200:
        return HTMLResponse(f"Token exchange failed: {r.status_code} {j}", status_code=502)

    # Sauvegarder les tokens en DB (une seule connexion)
    with get_db() as conn:
        with conn.cursor() as cur:
            kv_set(cur, "twitch_user_access_token", j["access_token"])
            kv_set(cur, "twitch_user_refresh_token", j.get("refresh_token", ""))
            kv_set(cur, "twitch_user_scopes", json.dumps(j.get("scope", [])))
        conn.commit()

    # Récupérer user_id/login via validate
    async with httpx.AsyncClient(timeout=20) as client:
        vr = await client.get(
            "https://id.twitch.tv/oauth2/validate",
            headers={"Authorization": f"OAuth {j['access_token']}"},
        )
    vj = vr.json()
    if vr.status_code == 200:
        with get_db() as conn:
            with conn.cursor() as cur:
                kv_set(cur, "twitch_broadcaster_user_id", vj.get("user_id", ""))
                kv_set(cur, "twitch_broadcaster_login", vj.get("login", ""))
                # Aussi stocker dans broadcaster_user_id (utilisé par /admin/points)
                kv_set(cur, "broadcaster_user_id", vj.get("user_id", ""))
                kv_set(cur, "broadcaster_user_login", vj.get("login", ""))
            conn.commit()

    return RedirectResponse("/admin/points")


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


def _admin_esc(val) -> str:
    s = "" if val is None else str(val)
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
         .replace("'", "&#39;")
    )

def _admin_flash_html(flash: str | None, kind: str | None) -> str:
    if not flash:
        return ""
    k = (kind or "ok").lower()
    cls = "ok" if k == "ok" else ("warn" if k == "warn" else "err")
    return f"<div class='flash {cls}'>" + _admin_esc(flash) + "</div>"

@app.post("/admin/set_live")
def admin_set_live(
    request: Request,
    value: str | None = Form(default=None),
    payload: dict | None = Body(default=None),
    credentials: HTTPBasicCredentials = Depends(security),
):
    """
    Supporte:
    - Form: value=true/false (HTML form)
    - JSON: {"value": true/false} (fetch)
    """
    require_admin(credentials)

    v = value
    if v is None and isinstance(payload, dict):
        v = payload.get("value")

    if v is None:
        # fallback query param
        v = request.query_params.get("value")

    v_str = str(v or "false").strip().lower()
    is_live = v_str in ("true", "1", "yes", "on")

    if is_live:
        _stream_online_db()
    else:
        _stream_offline_db()

    return {"ok": True, "is_live": is_live}



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
# Live sync fallback (Helix) - si EventSub 'stream.offline' n'arrive pas
# =============================================================================
def twitch_is_live(broadcaster_user_id: str | None = None, broadcaster_login: str | None = None) -> bool:
    """Retourne True si le streamer est live (Helix /streams)."""
    cid = os.environ["TWITCH_CLIENT_ID"]
    token = twitch_app_token()

    params = {}
    if broadcaster_user_id:
        params["user_id"] = str(broadcaster_user_id).strip()
    elif broadcaster_login:
        params["user_login"] = str(broadcaster_login).strip().lower()
    else:
        return False

    r = requests.get(
        "https://api.twitch.tv/helix/streams",
        params=params,
        headers={"Authorization": f"Bearer {token}", "Client-Id": cid},
        timeout=5,
    )
    r.raise_for_status()
    data = r.json().get("data", [])
    return bool(data)


def _store_broadcaster_meta(cur, user_id: str | None, login: str | None, name: str | None) -> None:
    # best-effort
    try:
        if user_id:
            kv_set(cur, "broadcaster_user_id", str(user_id).strip())
        if login:
            kv_set(cur, "broadcaster_user_login", str(login).strip().lower())
        if name:
            kv_set(cur, "broadcaster_user_name", str(name).strip())
    except Exception:
        pass


def _stream_online_db(user_id: str | None = None, login: str | None = None, name: str | None = None) -> None:
    """Passe live en DB + crée une session si nécessaire (idempotent)."""
    with get_db() as conn:
        with conn.cursor() as cur:
            # déjà live ?
            cur.execute("SELECT value FROM kv WHERE key='is_live';")
            row = cur.fetchone()
            if row and row[0] == "true":
                _store_broadcaster_meta(cur, user_id, login, name)
                conn.commit()
                return

            kv_set(cur, "is_live", "true")
            _store_broadcaster_meta(cur, user_id, login, name)

            # créer une session
            cur.execute("INSERT INTO stream_sessions DEFAULT VALUES RETURNING id;")
            sid = int(cur.fetchone()[0])

            kv_set(cur, "current_session_id", str(sid))
        conn.commit()


def _stream_offline_db() -> None:
    """Passe offline en DB + clôture session + streak/bonus (idempotent).
    Compatibilité: si la colonne streaks.last_session_id n'existe pas, on n'y touche pas.
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            has_last_session_id = column_exists(cur, "streaks", "last_session_id")

            # 1) flag offline
            kv_set(cur, "is_live", "false")

            # 2) session courante
            cur.execute("SELECT value FROM kv WHERE key='current_session_id';")
            row = cur.fetchone()
            if not row or not row[0]:
                conn.commit()
                return
            sid = int(row[0])

            # 3) clôture session (idempotent)
            cur.execute("UPDATE stream_sessions SET ended_at=now() WHERE id=%s AND ended_at IS NULL;", (sid,))
            just_closed = (cur.rowcount == 1)
            if not just_closed:
                # évite boucle infinie: on nettoie quand même
                kv_set(cur, "current_session_id", "")
                conn.commit()
                return

            # 4) participants
            cur.execute("SELECT twitch_login FROM stream_participants WHERE session_id=%s;", (sid,))
            participants = [r[0] for r in cur.fetchall()]

            for login in participants:
                login = (login or "").strip().lower()
                if not login:
                    continue

                # --- streak ---
                prev_count = 0
                prev_sid = None

                if has_last_session_id:
                    cur.execute("SELECT streak_count, last_session_id FROM streaks WHERE twitch_login=%s;", (login,))
                    srow = cur.fetchone()
                    prev_count = int(srow[0]) if srow else 0
                    prev_sid = int(srow[1]) if (srow and srow[1] is not None) else None
                    new_count = (prev_count + 1) if (prev_sid == sid - 1) else 1
                else:
                    # Pas d'info de consécutivité: on repart à 1 (safe)
                    cur.execute("SELECT streak_count FROM streaks WHERE twitch_login=%s;", (login,))
                    srow = cur.fetchone()
                    prev_count = int(srow[0]) if srow else 0
                    new_count = 1

                # bonus bonheur par paliers
                bonus = 0
                if new_count == 1:
                    bonus = 2
                elif new_count == 3:
                    bonus = 5
                elif new_count == 5:
                    bonus = 10
                elif new_count == 10:
                    bonus = 20

                # upsert streak (schema-safe)
                if has_last_session_id:
                    cur.execute(
                        """
                        INSERT INTO streaks (twitch_login, streak_count, last_session_id)
                        VALUES (%s,%s,%s)
                        ON CONFLICT (twitch_login) DO UPDATE
                        SET streak_count=EXCLUDED.streak_count,
                            last_session_id=EXCLUDED.last_session_id,
                            updated_at=now();
                        """,
                        (login, new_count, sid),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO streaks (twitch_login, streak_count)
                        VALUES (%s,%s)
                        ON CONFLICT (twitch_login) DO UPDATE
                        SET streak_count=EXCLUDED.streak_count,
                            updated_at=now();
                        """,
                        (login, new_count),
                    )

                # appliquer bonus bonheur (cap 100) sur CM actif
                if bonus > 0:
                    cur.execute(
                        """
                        UPDATE creatures_v2
                        SET happiness = LEAST(100, GREATEST(0, COALESCE(happiness,50) + %s)),
                            updated_at=now()
                        WHERE twitch_login=%s AND is_active=true;
                        """,
                        (bonus, login),
                    )

            # 5) clear session
            kv_set(cur, "current_session_id", "")

        conn.commit()



@app.on_event("startup")
async def _startup_live_sync():
    async def loop():
        # interval configurable
        try:
            interval = int(os.environ.get("LIVE_SYNC_SECONDS", "30"))
        except Exception:
            interval = 30
        interval = max(10, min(interval, 300))

        while True:
            await asyncio.sleep(interval)

            # lire état DB + meta
            try:
                with get_db() as conn:
                    with conn.cursor() as cur:
                        db_live = (kv_get(cur, "is_live", "false") == "true")
                        uid = (kv_get(cur, "broadcaster_user_id", "") or "").strip() or None
                        login = (kv_get(cur, "broadcaster_user_login", "") or "").strip().lower() or None
            except Exception:
                continue

            # fallback env si jamais (optionnel)
            if not uid:
                uid_env = (os.environ.get("TWITCH_BROADCASTER_USER_ID", "") or "").strip()
                uid = uid_env or None
            if not login:
                login_env = (os.environ.get("TWITCH_BROADCASTER_LOGIN", "") or "").strip().lower()
                login = login_env or None

            if not uid and not login:
                continue

            # Helix check (dans un thread pour ne pas bloquer l'event loop)
            try:
                helix_live = await asyncio.to_thread(twitch_is_live, uid, login)
            except Exception:
                continue

            if helix_live and not db_live:
                _stream_online_db(uid, login, None)
            elif (not helix_live) and db_live:
                _stream_offline_db()

    asyncio.create_task(loop())


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

            _ensure_quests(cur, login)
            _quest_progress(cur, login, 'xp', amount)
            _quest_check_top10(cur, login)

        conn.commit()



# =============================================================================
# Quêtes hebdomadaires — helpers
# =============================================================================

import datetime as _dt

def _current_week_start() -> "_dt.date":
    today = _dt.date.today()
    return today - _dt.timedelta(days=today.weekday())

def _ensure_quests(cur, login: str) -> None:
    import random as _random
    week = _current_week_start()
    cur.execute("SELECT quest_key FROM quest_assignments WHERE twitch_login=%s AND week_start=%s;", (login, week))
    existing = {r[0] for r in cur.fetchall()}
    if len(existing) >= 3:
        return
    cur.execute("SELECT key, is_fixed FROM quest_catalog WHERE is_active=TRUE ORDER BY id;")
    catalog = cur.fetchall()
    fixed   = [r[0] for r in catalog if r[1] and r[0] not in existing]
    random_ = [r[0] for r in catalog if not r[1] and r[0] not in existing]
    to_assign = []
    for k in fixed[:2]:
        if k not in existing:
            to_assign.append(k)
    if random_:
        pick = _random.choice(random_)
        if pick not in existing and pick not in to_assign:
            to_assign.append(pick)
    for k in to_assign:
        cur.execute(
            "INSERT INTO quest_assignments (twitch_login, quest_key, week_start) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING;",
            (login, k, week)
        )

def _quest_progress(cur, login: str, quest_type: str, delta: int = 1) -> None:
    week = _current_week_start()
    cur.execute("""
        UPDATE quest_assignments qa
        SET progress = LEAST(
                (SELECT target FROM quest_catalog WHERE key = qa.quest_key),
                qa.progress + %s),
            completed = (qa.progress + %s >= (SELECT target FROM quest_catalog WHERE key = qa.quest_key)),
            completed_at = CASE
                WHEN NOT qa.completed
                 AND qa.progress + %s >= (SELECT target FROM quest_catalog WHERE key = qa.quest_key)
                THEN now() ELSE qa.completed_at END
        WHERE qa.twitch_login = %s
          AND qa.week_start   = %s
          AND qa.completed    = FALSE
          AND (SELECT type FROM quest_catalog WHERE key = qa.quest_key) = %s;
    """, (delta, delta, delta, login, week, quest_type))

def _quest_check_top10(cur, login: str) -> None:
    cur.execute("""
        SELECT rank FROM (
            SELECT twitch_login, RANK() OVER (ORDER BY xp_total DESC) as rank
            FROM creatures_v2 WHERE is_active = TRUE
        ) r WHERE twitch_login = %s;
    """, (login,))
    row = cur.fetchone()
    if row and int(row[0]) <= 10:
        _quest_progress(cur, login, 'top10', 1)

def _quest_reward(cur, login: str) -> list:
    week = _current_week_start()
    cur.execute("""
        SELECT qa.id, qa.quest_key, qc.reward_xp, qc.reward_item_key,
               qc.reward_item_qty, qc.reward_badge, qc.label
        FROM quest_assignments qa
        JOIN quest_catalog qc ON qc.key = qa.quest_key
        WHERE qa.twitch_login=%s AND qa.week_start=%s
          AND qa.completed=TRUE AND qa.rewarded=FALSE;
    """, (login, week))
    rows = cur.fetchall()
    rewards = []
    for row in rows:
        qa_id, quest_key, xp, item_key, item_qty, badge, label = row
        if xp and xp > 0:
            cur.execute("INSERT INTO xp_events (twitch_login, amount) VALUES (%s,%s);", (login, xp))
            cur.execute("UPDATE creatures_v2 SET xp_total=xp_total+%s, updated_at=now() WHERE twitch_login=%s AND is_active=TRUE;", (xp, login))
        if item_key and item_qty > 0:
            cur.execute("INSERT INTO inventory (twitch_login, item_key, qty) VALUES (%s,%s,%s) ON CONFLICT (twitch_login, item_key) DO UPDATE SET qty=inventory.qty+EXCLUDED.qty, updated_at=now();", (login, item_key, item_qty))
        if badge:
            cur.execute("INSERT INTO user_badges (twitch_login, badge_key) VALUES (%s,%s) ON CONFLICT DO NOTHING;", (login, badge))
        cur.execute("UPDATE quest_assignments SET rewarded=TRUE WHERE id=%s;", (qa_id,))
        rewards.append({"quest_key": quest_key, "label": label, "xp": xp, "item_key": item_key})
    return rewards


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

def coop_xp_for_count(count: int) -> int:
    """Retourne un montant d'XP aléatoire selon le nombre de participants."""
    if count <= 1:
        return random.randint(20, 30)
    elif count <= 3:
        return random.randint(30, 50)
    elif count <= 5:
        return random.randint(50, 75)
    else:
        return random.randint(100, 250)

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

            cur.execute(
                "SELECT twitch_login FROM drop_participants WHERE drop_id=%s ORDER BY created_at ASC;",
                (drop_id,),
            )
            participants = [r[0] for r in cur.fetchall()]

            winners = []

            if mode == 'first':
                if participants:
                    winners = [participants[0]]

            elif mode == 'random':
                if participants:
                    winners = [random.choice(participants)]

            elif mode == 'coop':
                # Nouveau système : tout le monde gagne, XP selon le nombre
                winners = participants[:]

            if winners:
                if mode == 'coop':
                    count = len(winners)
                    xp_details = []
                    for w in winners:
                        xp_amount = coop_xp_for_count(count)
                        grant_xp(w, xp_amount)
                        xp_details.append((w, xp_amount))

                    # Construire le message d'annonce
                    if len(xp_details) == 1:
                        w, xp = xp_details[0]
                        _announce(f"⏱️ Drop COOP '{title}' terminé ! @{w} gagne {xp} XP !")
                    elif len(xp_details) <= 5:
                        parts = ", ".join([f"@{w} +{xp}XP" for w, xp in xp_details])
                        _announce(f"⏱️ Drop COOP '{title}' terminé ! {parts}")
                    else:
                        # Trop de monde pour tout lister : résumé
                        total = sum(xp for _, xp in xp_details)
                        best_w, best_xp = max(xp_details, key=lambda x: x[1])
                        parts_short = ", ".join([f"@{w} +{xp}XP" for w, xp in xp_details[:5]])
                        _announce(f"🔥 Drop COOP '{title}' terminé ! {len(xp_details)} participants ! {parts_short}... | Meilleur tirage : @{best_w} +{best_xp}XP !")

                    winner_login = None  # pas de winner unique en coop

                else:
                    # first / random : comportement inchangé
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
                # Personne n'a participé
                if mode == 'coop':
                    _announce(f"⌛ Drop COOP '{title}' expiré sans participants.")
                cur.execute(
                    "UPDATE drops SET status='expired', resolved_at=now() WHERE id=%s;",
                    (drop_id,),
                )

        conn.commit()

    return {
        "mode": mode,
        "title": title,
        "winners": winners,
        "xp_bonus": int(xp_bonus),
        "ticket_key": ticket_key,
        "ticket_qty": int(ticket_qty),
    }


def kv_get(cur_or_key, key: str | None = None, default: str | None = None) -> str | None:
    """Get KV.
    Backward-compatible:
      - kv_get(cur, "k", "def")
      - kv_get("k", "def")
    """
    if key is None:
        # called as kv_get("k", default)
        k = str(cur_or_key)
        d = default
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM kv WHERE key=%s;", (k,))
                row = cur.fetchone()
            conn.commit()
        return (row[0] if row and row[0] is not None else d)

    # called as kv_get(cur, key, default)
    cur = cur_or_key
    cur.execute("SELECT value FROM kv WHERE key=%s;", (key,))
    row = cur.fetchone()
    return (row[0] if row and row[0] is not None else default)

def kv_set(cur_or_key, key: str | None = None, value: str | None = None) -> None:
    """Set KV.
    Backward-compatible:
      - kv_set(cur, "k", "v")
      - kv_set("k", "v")
    """
    if value is None:
        # called as kv_set("k", "v")
        k = str(cur_or_key)
        v = "" if key is None else str(key)
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO kv (key, value)
                    VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE
                    SET value = EXCLUDED.value, updated_at = now();
                    """,
                    (k, v),
                )
            conn.commit()
        return

    # called as kv_set(cur, key, value)
    cur = cur_or_key
    cur.execute(
        """
        INSERT INTO kv (key, value)
        VALUES (%s, %s)
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, updated_at = now();
        """,
        (key, "" if value is None else str(value)),
    )

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


            # --- Channel Points shop + EventSub dedup + annonces bot ---
            cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_announcements (
              id BIGSERIAL PRIMARY KEY,
              message TEXT NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              delivered_at TIMESTAMPTZ
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS cp_redemptions (
              redemption_id TEXT PRIMARY KEY,
              user_login TEXT,
              reward_id TEXT,
              reward_title TEXT,
              cost INT NOT NULL DEFAULT 0,
              status TEXT NOT NULL DEFAULT 'processing',
              detail TEXT NOT NULL DEFAULT '',
              action JSONB,
              drop_id INT,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              processed_at TIMESTAMPTZ
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS eventsub_deliveries (
              msg_id TEXT PRIMARY KEY,
              msg_type TEXT,
              sub_type TEXT,
              received_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS quest_catalog (
                id           SERIAL PRIMARY KEY,
                key          TEXT UNIQUE NOT NULL,
                label        TEXT NOT NULL,
                description  TEXT NOT NULL DEFAULT '',
                type         TEXT NOT NULL,
                target       INT NOT NULL DEFAULT 1,
                is_fixed     BOOLEAN NOT NULL DEFAULT FALSE,
                reward_xp    INT NOT NULL DEFAULT 0,
                reward_item_key  TEXT,
                reward_item_qty  INT NOT NULL DEFAULT 1,
                reward_badge TEXT,
                is_active    BOOLEAN NOT NULL DEFAULT TRUE,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE TABLE IF NOT EXISTS quest_assignments (
                id           BIGSERIAL PRIMARY KEY,
                twitch_login TEXT NOT NULL,
                quest_key    TEXT NOT NULL REFERENCES quest_catalog(key),
                week_start   DATE NOT NULL,
                progress     INT NOT NULL DEFAULT 0,
                completed    BOOLEAN NOT NULL DEFAULT FALSE,
                completed_at TIMESTAMPTZ,
                rewarded     BOOLEAN NOT NULL DEFAULT FALSE,
                UNIQUE (twitch_login, quest_key, week_start)
            );
            CREATE INDEX IF NOT EXISTS idx_quest_assignments_login_week
                ON quest_assignments(twitch_login, week_start);
            CREATE TABLE IF NOT EXISTS user_badges (
                twitch_login TEXT NOT NULL,
                badge_key    TEXT NOT NULL,
                earned_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (twitch_login, badge_key)
            );
            """)
            cur.execute("""
            INSERT INTO quest_catalog
              (key, label, description, type, target, is_fixed, reward_xp, reward_item_key, reward_item_qty, reward_badge)
            VALUES
              ('presence_60min',  'Présence 60 min',   'Rester au moins 60 min en stream cette semaine', 'presence', 3600, TRUE,  50, NULL,             0, 'badge_present'),
              ('drops_3',         'Chasseur de drops', 'Participer à 3 drops cette semaine',             'drops',       3, TRUE,  30, 'bonbon_2',       1, NULL),
              ('candy_5',         'Gourmand',          'Faire manger 5 bonbons à ton CM',                'candy',       5, FALSE, 40, 'grande_capsule',  1, NULL),
              ('xp_200',          'Grind XP',          'Gagner 200 XP en une semaine',                   'xp',        200, FALSE, 60, 'bonbon_2',       3, 'badge_grinder'),
              ('top10',           'Élite',             'Atteindre le top 10 du classement XP',           'top10',       1, FALSE,100, NULL,             0, 'badge_elite'),
              ('show_3',          'Vedette',           'Utiliser !show 3 fois cette semaine',             'show',        3, FALSE, 25, 'bonbon_2',       1, NULL),
              ('coop_2',          'Esprit d''équipe',  'Participer à 2 drops COOP',                      'coop',        2, FALSE, 35, 'bonbon_2',       2, 'badge_coop'),
              ('drops_5',         'Drop Addict',       'Participer à 5 drops cette semaine',             'drops',       5, FALSE, 50, 'grande_capsule',  1, NULL),
              ('candy_10',        'Très gourmand',     'Faire manger 10 bonbons à ton CM',               'candy',      10, FALSE, 80, 'grande_capsule',  2, 'badge_gourmand'),
              ('presence_120min', 'Fidèle',            'Rester au moins 2h en stream cette semaine',     'presence', 7200, FALSE, 75, NULL,             0, 'badge_loyal')
            ON CONFLICT (key) DO NOTHING;
            """)


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
        sub = payload.get("subscription", {}) or {}
        sub_type = str(sub.get("type", "") or "")
        ev = payload.get("event", {}) or {}

        broadcaster_user_id = str(ev.get("broadcaster_user_id", "") or "").strip()
        broadcaster_user_login = str(ev.get("broadcaster_user_login", "") or "").strip().lower()
        broadcaster_user_name = str(ev.get("broadcaster_user_name", "") or "").strip()

        # Déduplication (message id)
        msg_id = headers.get("twitch-eventsub-message-id", "")
        if msg_id and not eventsub_dedup(msg_id, "notification", sub_type):
            return "ok"

        if sub_type == "stream.online":
            _stream_online_db(broadcaster_user_id, broadcaster_user_login, broadcaster_user_name)
            return "ok"

        if sub_type == "stream.offline":
            _stream_offline_db()
            return "ok"

        if sub_type == "channel.channel_points_custom_reward_redemption.add":
            # achat via points de chaîne (boutique)
            handle_channel_points_redemption(ev)
            return "ok"

        return "ok"

    # 3) Revocation

    if msg_type == "revocation":
        return "ok"

    return "ok"

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


            # ← AJOUTER ICI
            if mode == "first":
                announce_msg = f"⚡ Drop '{title}' — tapez !grab, LE PREMIER gagne ! ({duration}s)"
            elif mode == "random":
                announce_msg = f"🎲 Drop '{title}' — tapez !grab pour tenter votre chance ! ({duration}s)"
            else:
                announce_msg = f"🤝 Drop COOP '{title}' — tapez !grab, tout le monde gagne du XP ! ({duration}s)"
            cur.execute("INSERT INTO bot_announcements (message) VALUES (%s);", (announce_msg,))
            # ← FIN AJOUT

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


@app.get("/admin/user/{login}/collection", response_class=HTMLResponse)
def admin_user_collection_page(
    request: Request,
    login: str,
    flash: str | None = None,
    flash_kind: str | None = None,
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)
    login = (login or "").strip().lower()
    if not login:
        return HTMLResponse("<h1>Missing login</h1>", status_code=400)

    rows: list[dict] = []
    active_id: int | None = None

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM creatures_v2
                WHERE twitch_login=%s AND is_active=true
                LIMIT 1;
            """, (login,))
            ar = cur.fetchone()
            active_id = int(ar[0]) if ar else None

            cur.execute(
                """
                SELECT
                    id,
                    is_active,
                    cm_key,
                    COALESCE(lineage_key,'') AS lineage_key,
                    stage,
                    xp_total,
                    happiness,
                    COALESCE(acquired_from,'') AS acquired_from,
                    acquired_at
                FROM creatures_v2
                WHERE twitch_login=%s
                ORDER BY is_active DESC, acquired_at ASC NULLS LAST, id ASC;
                """,
                (login,),
            )
            for r in cur.fetchall():
                rows.append({
                    "id": int(r[0]),
                    "is_active": bool(r[1]),
                    "cm_key": r[2],
                    "lineage_key": r[3] or "",
                    "stage": int(r[4] or 0),
                    "xp_total": int(r[5] or 0),
                    "happiness": int(r[6] or 0),
                    "acquired_from": r[7] or "",
                    "acquired_at": r[8].isoformat() if r[8] else "",
                })

    def row_actions_html(cid: int, is_active: bool, cm_key: str) -> str:
        btns = []
        if not is_active:
            btns.append(
                f"<form method='post' action='/admin/user_action' style='display:inline;margin:0'>"
                f"<input type='hidden' name='login' value='{_admin_esc(login)}'>"
                f"<input type='hidden' name='action' value='set_active'>"
                f"<input type='hidden' name='creature_id' value='{cid}'>"
                f"<input type='hidden' name='next' value='/admin/user/{_admin_esc(login)}/collection'>"
                f"<button class='btn' type='submit'>Activer</button></form>"
            )
        # delete egg (only if egg)
        if (cm_key or "").strip().lower() == "egg":
            btns.append(
                f"<form method='post' action='/admin/user_action' style='display:inline;margin:0'>"
                f"<input type='hidden' name='login' value='{_admin_esc(login)}'>"
                f"<input type='hidden' name='action' value='delete_egg'>"
                f"<input type='hidden' name='creature_id' value='{cid}'>"
                f"<input type='hidden' name='next' value='/admin/user/{_admin_esc(login)}/collection'>"
                f"<button class='btn' type='submit'>Suppr œuf</button></form>"
            )
        btns.append(
            f"<form method='post' action='/admin/user_action' style='display:inline;margin:0'>"
            f"<input type='hidden' name='login' value='{_admin_esc(login)}'>"
            f"<input type='hidden' name='action' value='delete_creature'>"
            f"<input type='hidden' name='creature_id' value='{cid}'>"
            f"<input type='hidden' name='next' value='/admin/user/{_admin_esc(login)}/collection'>"
            f"<button class='btn' type='submit'>Suppr</button></form>"
        )
        return " ".join(btns)

    html = f"""<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>CapsMons — Collection {_admin_esc(login)}</title>
  <style>
    :root {{
      --bg:#0b0f14; --text:#e6edf3; --muted:#9aa4b2; --link:#7aa2ff;
      --ok:#2bd576; --err:#ff6b6b; --warn:#ffd166; --radius:14px;
    }}
    body{{margin:0;font-family:system-ui,Segoe UI,Roboto,Arial,sans-serif;background:var(--bg);color:var(--text)}}
    a{{color:var(--link);text-decoration:none}} a:hover{{text-decoration:underline}}
    .wrap{{max-width:1300px;margin:0 auto;padding:20px}}
    .top{{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap}}
    .card{{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.12);border-radius:var(--radius);padding:14px}}
    .muted{{color:var(--muted)}}
    .row{{display:flex;gap:10px;flex-wrap:wrap;align-items:center}}
    .btn{{cursor:pointer;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.06);color:var(--text);border-radius:12px;padding:8px 10px}}
    .btn:hover{{background:rgba(255,255,255,.10)}}
    input,select{{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.16);color:var(--text);border-radius:10px;padding:8px 10px;outline:none}}
    table{{width:100%;border-collapse:collapse;margin-top:10px}}
    th,td{{border-bottom:1px solid rgba(255,255,255,.10);padding:10px 8px;text-align:left;font-size:14px;vertical-align:top}}
    th{{color:var(--muted);font-weight:800;font-size:12px;text-transform:uppercase;letter-spacing:.08em}}
    .flash{{margin:12px 0;padding:10px 12px;border-radius:12px;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.06)}}
    .flash.ok{{border-color:rgba(43,213,118,.35)}}
    .flash.err{{border-color:rgba(255,107,107,.35)}}
    .flash.warn{{border-color:rgba(255,209,102,.35)}}
    .pill{{display:inline-block;padding:3px 9px;border:1px solid rgba(255,255,255,.16);border-radius:999px;font-size:12px}}
    .pill.ok{{border-color:rgba(43,213,118,.45);color:var(--ok)}}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <div>
        <div class="muted">Utilisateur</div>
        <h1 style="margin:0;font-size:22px">📦 Collection — {_admin_esc(login)}</h1>
        <div class="muted" style="margin-top:4px">CM: <b>{len(rows)}</b> · Actif: <b>{active_id if active_id else '—'}</b></div>
      </div>
      <div class="row">
        <a class="btn" href="/admin">← Admin</a>
        <a class="btn" href="/admin/user/{_admin_esc(login)}">← Fiche user</a>
      </div>
    </div>

    {_admin_flash_html(flash, flash_kind)}

    <div class="card">
      <div style="font-weight:800">Actions rapides</div>
      <div class="muted" style="margin-top:4px">Ces actions ciblent le CM actif (si aucun ID fourni).</div>

      <div class="row" style="margin-top:10px">
        <form method="post" action="/admin/user_action" class="row" style="margin:0">
          <input type="hidden" name="login" value="{_admin_esc(login)}">
          <input type="hidden" name="action" value="add_xp">
          <input type="hidden" name="next" value="/admin/user/{_admin_esc(login)}/collection">
          <input name="creature_id" type="number" placeholder="id (optionnel)" style="width:140px">
          <input name="amount" type="number" placeholder="+XP" style="width:120px">
          <button class="btn" type="submit">Ajouter XP</button>
        </form>

        <form method="post" action="/admin/user_action" class="row" style="margin:0">
          <input type="hidden" name="login" value="{_admin_esc(login)}">
          <input type="hidden" name="action" value="set_stage">
          <input type="hidden" name="next" value="/admin/user/{_admin_esc(login)}/collection">
          <input name="creature_id" type="number" placeholder="id (optionnel)" style="width:140px">
          <input name="stage" type="number" placeholder="stage" style="width:120px">
          <button class="btn" type="submit">Fixer stage</button>
        </form>

        <form method="post" action="/admin/user_action" class="row" style="margin:0">
          <input type="hidden" name="login" value="{_admin_esc(login)}">
          <input type="hidden" name="action" value="reset_active">
          <input type="hidden" name="next" value="/admin/user/{_admin_esc(login)}/collection">
          <button class="btn" type="submit">Reset actif</button>
        </form>
      </div>

      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>Actif</th>
            <th>cm_key</th>
            <th>lignée</th>
            <th>stage</th>
            <th>XP</th>
            <th>bonheur</th>
            <th>acquired_from</th>
            <th>acquired_at</th>
            <th>actions</th>
          </tr>
        </thead>
        <tbody>
          {''.join([
            f"<tr>"
            f"<td>{r['id']}</td>"
            f"<td>{'✅' if r['is_active'] else ''}</td>"
            f"<td>{_admin_esc(r['cm_key'])}</td>"
            f"<td>{_admin_esc(r['lineage_key'])}</td>"
            f"<td>{r['stage']}</td>"
            f"<td>{r['xp_total']}</td>"
            f"<td>{r['happiness']}</td>"
            f"<td>{_admin_esc(r['acquired_from'])}</td>"
            f"<td style='white-space:nowrap'>{_admin_esc(r['acquired_at'])}</td>"
            f"<td style='white-space:nowrap'>{row_actions_html(r['id'], r['is_active'], r['cm_key'])}</td>"
            f"</tr>"
          for r in rows]) if rows else "<tr><td colspan='10' class='muted'>Aucun CM.</td></tr>"}
        </tbody>
      </table>
    </div>
  </div>
</body>
</html>"""
    return HTMLResponse(html)


@app.get("/admin/user/{login}/collection.json")
def admin_user_collection_json(
    login: str,
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)
    login = (login or "").strip().lower()
    if not login:
        raise HTTPException(status_code=400, detail="Missing login")

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id, is_active, cm_key, COALESCE(lineage_key,''),
                    stage, xp_total, happiness,
                    COALESCE(acquired_from,''), acquired_at
                FROM creatures_v2
                WHERE twitch_login=%s
                ORDER BY is_active DESC, acquired_at ASC NULLS LAST, id ASC;
                """,
                (login,),
            )
            out = []
            for r in cur.fetchall():
                out.append({
                    "id": int(r[0]),
                    "is_active": bool(r[1]),
                    "cm_key": r[2],
                    "lineage_key": r[3] or "",
                    "stage": int(r[4] or 0),
                    "xp_total": int(r[5] or 0),
                    "happiness": int(r[6] or 0),
                    "acquired_from": r[7] or "",
                    "acquired_at": r[8].isoformat() if r[8] else None,
                })
    return {"ok": True, "twitch_login": login, "collection": out}


@app.post("/admin/user_action")
def admin_user_action(
    login: str = Form(...),
    action: str = Form(...),
    creature_id: int | None = Form(None),
    amount: int | None = Form(None),
    stage: int | None = Form(None),
    happiness: int | None = Form(None),
    item_key: str | None = Form(None),
    qty: int | None = Form(None),
    to_login: str | None = Form(None),
    next: str | None = Form(None),
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)

    login = (login or "").strip().lower()
    action = (action or "").strip().lower()
    to_login = (to_login or "").strip().lower() if to_login else None
    item_key = (item_key or "").strip().lower() if item_key else None

    redirect_to = next or f"/admin/user/{login}"

    def go(kind: str, msg: str):
        from urllib.parse import quote
        return RedirectResponse(
            url=f"{redirect_to}?flash_kind={quote(kind)}&flash={quote(msg)}",
            status_code=303,
        )

    if not login:
        return go("err", "Missing login")

    with get_db() as conn:
        with conn.cursor() as cur:
            # ensure users exist
            cur.execute("INSERT INTO users (twitch_login) VALUES (%s) ON CONFLICT DO NOTHING;", (login,))

            def get_target_creature_id() -> int | None:
                if creature_id is not None:
                    try:
                        return int(creature_id)
                    except Exception:
                        return None
                cur.execute("""
                    SELECT id
                    FROM creatures_v2
                    WHERE twitch_login=%s AND is_active=true
                    LIMIT 1;
                """, (login,))
                r = cur.fetchone()
                return int(r[0]) if r else None

            # ===== CM actions =====
            if action == "set_active":
                if creature_id is None:
                    return go("err", "creature_id manquant")
                cid = int(creature_id)
                cur.execute("SELECT 1 FROM creatures_v2 WHERE id=%s AND twitch_login=%s;", (cid, login))
                if not cur.fetchone():
                    return go("err", "CM introuvable")
                cur.execute("UPDATE creatures_v2 SET is_active=false WHERE twitch_login=%s AND is_active=true;", (login,))
                cur.execute("UPDATE creatures_v2 SET is_active=true, updated_at=now() WHERE id=%s AND twitch_login=%s;", (cid, login))
                conn.commit()
                return go("ok", f"CM actif = {cid}")

            if action in ("add_xp", "set_xp", "set_stage", "set_happiness", "reset_active", "delete_creature", "delete_egg"):
                cid = get_target_creature_id()
                if cid is None and action not in ("delete_creature", "delete_egg"):
                    return go("err", "Aucun CM actif (et aucun ID fourni)")

                if action == "add_xp":
                    if amount is None:
                        return go("err", "amount manquant")
                    delta = int(amount)
                    if delta == 0:
                        return go("warn", "delta = 0")
                    cur.execute("SELECT xp_total FROM creatures_v2 WHERE id=%s AND twitch_login=%s;", (cid, login))
                    r = cur.fetchone()
                    if not r:
                        return go("err", "CM introuvable")
                    new_xp = max(0, int(r[0] or 0) + delta)
                    new_stage = stage_from_xp(int(new_xp))
                    cur.execute("""
                        UPDATE creatures_v2
                        SET xp_total=%s, stage=%s, updated_at=now()
                        WHERE id=%s AND twitch_login=%s;
                    """, (new_xp, new_stage, cid, login))
                    conn.commit()
                    return go("ok", f"XP = {new_xp} (stage {new_stage})")

                if action == "set_xp":
                    if amount is None:
                        return go("err", "amount manquant")
                    new_xp = max(0, int(amount))
                    new_stage = stage_from_xp(int(new_xp))
                    cur.execute("""
                        UPDATE creatures_v2
                        SET xp_total=%s, stage=%s, updated_at=now()
                        WHERE id=%s AND twitch_login=%s;
                    """, (new_xp, new_stage, cid, login))
                    if cur.rowcount <= 0:
                        return go("err", "CM introuvable")
                    conn.commit()
                    return go("ok", f"XP fixé à {new_xp} (stage {new_stage})")

                if action == "set_stage":
                    if stage is None:
                        return go("err", "stage manquant")
                    st = max(0, int(stage))
                    cur.execute("""
                        UPDATE creatures_v2
                        SET stage=%s, updated_at=now()
                        WHERE id=%s AND twitch_login=%s;
                    """, (st, cid, login))
                    if cur.rowcount <= 0:
                        return go("err", "CM introuvable")
                    conn.commit()
                    return go("ok", f"Stage fixé à {st}")

                if action == "set_happiness":
                    if happiness is None:
                        return go("err", "bonheur manquant")
                    h = max(0, min(100, int(happiness)))
                    cur.execute("""
                        UPDATE creatures_v2
                        SET happiness=%s, updated_at=now()
                        WHERE id=%s AND twitch_login=%s;
                    """, (h, cid, login))
                    if cur.rowcount <= 0:
                        return go("err", "CM introuvable")
                    conn.commit()
                    return go("ok", f"Bonheur fixé à {h}")

                if action == "reset_active":
                    cur.execute("""
                        UPDATE creatures_v2
                        SET xp_total=0, stage=0, lineage_key=NULL, happiness=50, updated_at=now()
                        WHERE id=%s AND twitch_login=%s;
                    """, (cid, login))
                    if cur.rowcount <= 0:
                        return go("err", "CM introuvable")
                    conn.commit()
                    return go("ok", "CM reset")

                if action == "delete_creature":
                    if creature_id is None:
                        return go("err", "creature_id manquant")
                    cid2 = int(creature_id)
                    cur.execute("DELETE FROM creatures_v2 WHERE id=%s AND twitch_login=%s;", (cid2, login))
                    if cur.rowcount <= 0:
                        return go("err", "CM introuvable")
                    conn.commit()
                    return go("ok", f"CM {cid2} supprimé")

                if action == "delete_egg":
                    if creature_id is None:
                        return go("err", "creature_id manquant")
                    cid2 = int(creature_id)
                    cur.execute("SELECT cm_key FROM creatures_v2 WHERE id=%s AND twitch_login=%s;", (cid2, login))
                    r = cur.fetchone()
                    if not r:
                        return go("err", "CM introuvable")
                    if str(r[0] or "").lower() != "egg":
                        return go("err", "Ce CM n'est pas un œuf (cm_key != egg)")
                    cur.execute("DELETE FROM creatures_v2 WHERE id=%s AND twitch_login=%s;", (cid2, login))
                    conn.commit()
                    return go("ok", f"Œuf {cid2} supprimé")

            # ===== Inventory actions =====
            if action in ("give_item", "take_item", "transfer_item"):
                if not item_key:
                    return go("err", "item_key manquant")
                if qty is None:
                    return go("err", "qty manquant")
                n = int(qty)
                if n <= 0:
                    return go("err", "qty doit être > 0")

                # item exists?
                cur.execute("SELECT 1 FROM items WHERE key=%s;", (item_key,))
                if not cur.fetchone():
                    return go("err", "Item inconnu")

                def add_item(target_login: str, delta: int):
                    cur.execute("""
                        UPDATE inventory
                        SET qty = qty + %s
                        WHERE twitch_login=%s AND item_key=%s;
                    """, (delta, target_login, item_key))
                    if cur.rowcount <= 0:
                        cur.execute("""
                            INSERT INTO inventory (twitch_login, item_key, qty)
                            VALUES (%s,%s,%s);
                        """, (target_login, item_key, delta))

                def remove_item(target_login: str, delta: int):
                    cur.execute("""
                        UPDATE inventory
                        SET qty = GREATEST(qty - %s, 0)
                        WHERE twitch_login=%s AND item_key=%s;
                    """, (delta, target_login, item_key))

                if action == "give_item":
                    add_item(login, n)
                    conn.commit()
                    return go("ok", f"+{n} {item_key}")

                if action == "take_item":
                    remove_item(login, n)
                    conn.commit()
                    return go("ok", f"-{n} {item_key}")

                if action == "transfer_item":
                    if not to_login:
                        return go("err", "to_login manquant")
                    cur.execute("INSERT INTO users (twitch_login) VALUES (%s) ON CONFLICT DO NOTHING;", (to_login,))
                    remove_item(login, n)
                    add_item(to_login, n)
                    conn.commit()
                    return go("ok", f"Transfert {n} {item_key} -> {to_login}")

            return go("err", "Action inconnue")

@app.get("/admin/user/{login}", response_class=HTMLResponse)
def admin_user(
    request: Request,
    login: str,
    flash: str | None = None,
    flash_kind: str | None = None,
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)

    login = (login or "").strip().lower()
    if not login:
        return HTMLResponse("<h1>Missing login</h1>", status_code=400)

    active = None
    summary = {"xp_total_sum": 0, "stage_max": 0, "cm_count": 0}
    inv: list[dict] = []
    items: list[dict] = []

    with get_db() as conn:
        with conn.cursor() as cur:
            # user exist
            cur.execute("INSERT INTO users (twitch_login) VALUES (%s) ON CONFLICT DO NOTHING;", (login,))

            # active
            cur.execute("""
                SELECT id, cm_key, COALESCE(lineage_key,''), stage, xp_total, happiness,
                       COALESCE(acquired_from,''), acquired_at
                FROM creatures_v2
                WHERE twitch_login=%s AND is_active=true
                LIMIT 1;
            """, (login,))
            r = cur.fetchone()
            if r:
                active = {
                    "id": int(r[0]),
                    "cm_key": r[1],
                    "lineage_key": r[2] or "",
                    "stage": int(r[3] or 0),
                    "xp_total": int(r[4] or 0),
                    "happiness": int(r[5] or 0),
                    "acquired_from": r[6] or "",
                    "acquired_at": r[7].isoformat() if r[7] else "",
                }

            # summary
            cur.execute("""
                SELECT COALESCE(SUM(xp_total),0), COALESCE(MAX(stage),0), COUNT(*)
                FROM creatures_v2
                WHERE twitch_login=%s;
            """, (login,))
            s = cur.fetchone()
            if s:
                summary = {"xp_total_sum": int(s[0] or 0), "stage_max": int(s[1] or 0), "cm_count": int(s[2] or 0)}

            # inventory (+ join items name si possible)
            cur.execute("""
                SELECT inv.item_key, inv.qty, COALESCE(it.name,''), COALESCE(it.icon_url,'')
                FROM inventory inv
                LEFT JOIN items it ON it.key = inv.item_key
                WHERE inv.twitch_login=%s AND inv.qty > 0
                ORDER BY inv.item_key ASC;
            """, (login,))
            for ir in cur.fetchall():
                inv.append({"item_key": ir[0], "qty": int(ir[1] or 0), "name": ir[2] or "", "icon_url": ir[3] or ""})

            # items list for selects
            cur.execute("SELECT key, name FROM items ORDER BY key ASC;")
            for it in cur.fetchall():
                items.append({"key": it[0], "name": it[1]})

    def opt_items(selected: str | None = None) -> str:
        out = ["<option value=''>—</option>"]
        for it in items:
            k = it["key"]
            nm = it["name"] or k
            sel = " selected" if selected and selected == k else ""
            out.append(f"<option value='{_admin_esc(k)}'{sel}>{_admin_esc(k)} — {_admin_esc(nm)}</option>")
        return "".join(out)

    html = f"""<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>CapsMons — Admin user {_admin_esc(login)}</title>
  <style>
    :root {{
      --bg:#0b0f14; --text:#e6edf3; --muted:#9aa4b2; --link:#7aa2ff;
      --ok:#2bd576; --err:#ff6b6b; --warn:#ffd166; --radius:14px;
    }}
    body{{margin:0;font-family:system-ui,Segoe UI,Roboto,Arial,sans-serif;background:var(--bg);color:var(--text)}}
    a{{color:var(--link);text-decoration:none}} a:hover{{text-decoration:underline}}
    .wrap{{max-width:1200px;margin:0 auto;padding:20px}}
    .top{{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap}}
    .card{{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.12);border-radius:var(--radius);padding:14px}}
    .grid{{display:grid;grid-template-columns:1fr;gap:12px;margin-top:12px}}
    @media (min-width: 980px){{ .grid{{grid-template-columns: 1fr 1fr;}} }}
    .muted{{color:var(--muted)}}
    .row{{display:flex;gap:10px;flex-wrap:wrap;align-items:center}}
    .btn{{cursor:pointer;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.06);color:var(--text);border-radius:12px;padding:8px 10px}}
    .btn:hover{{background:rgba(255,255,255,.10)}}
    input,select{{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.16);color:var(--text);border-radius:10px;padding:8px 10px;outline:none}}
    table{{width:100%;border-collapse:collapse;margin-top:10px}}
    th,td{{border-bottom:1px solid rgba(255,255,255,.10);padding:10px 8px;text-align:left;font-size:14px;vertical-align:top}}
    th{{color:var(--muted);font-weight:800;font-size:12px;text-transform:uppercase;letter-spacing:.08em}}
    .flash{{margin:12px 0;padding:10px 12px;border-radius:12px;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.06)}}
    .flash.ok{{border-color:rgba(43,213,118,.35)}}
    .flash.err{{border-color:rgba(255,107,107,.35)}}
    .flash.warn{{border-color:rgba(255,209,102,.35)}}
    .pill{{display:inline-block;padding:3px 9px;border:1px solid rgba(255,255,255,.16);border-radius:999px;font-size:12px}}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <div>
        <div class="muted">Utilisateur</div>
        <h1 style="margin:0;font-size:22px">👤 {_admin_esc(login)}</h1>
        <div class="muted" style="margin-top:4px">
          XP total: <b>{summary['xp_total_sum']}</b> · Stage max: <b>{summary['stage_max']}</b> · #CM: <b>{summary['cm_count']}</b>
        </div>
      </div>
      <div class="row">
        <a class="btn" href="/admin">← Admin</a>
        <a class="btn" href="/admin/user/{_admin_esc(login)}/collection">📦 Collection</a>
      </div>
    </div>

    {_admin_flash_html(flash, flash_kind)}

    <div class="grid">
      <div class="card">
        <div class="muted">CM actif</div>
        {(
            f"<div style='margin-top:8px'>"
            f"<div><span class='pill'>id {active['id']}</span> <b>{_admin_esc(active['cm_key'])}</b> {('· '+_admin_esc(active['lineage_key'])) if active.get('lineage_key') else ''}</div>"
            f"<div class='muted' style='margin-top:6px'>stage {active['stage']} · xp {active['xp_total']} · bonheur {active['happiness']}</div>"
            f"<div class='muted' style='margin-top:4px'>acquired_from: {_admin_esc(active['acquired_from'])} · acquired_at: {_admin_esc(active['acquired_at'])}</div>"
            f"</div>"
        ) if active else "<div class='muted' style='margin-top:8px'>Aucun CM actif.</div>"}

        <hr style="border:none;border-top:1px solid rgba(255,255,255,.10);margin:14px 0">

        <div style="font-weight:800;margin-bottom:6px">Actions sur le CM actif</div>

        <form method="post" action="/admin/user_action" class="row" style="margin:0">
          <input type="hidden" name="login" value="{_admin_esc(login)}">
          <input type="hidden" name="action" value="add_xp">
          <input type="hidden" name="next" value="/admin/user/{_admin_esc(login)}">
          <input name="amount" type="number" placeholder="+XP" style="width:120px">
          <button class="btn" type="submit">Ajouter XP</button>
        </form>

        <form method="post" action="/admin/user_action" class="row" style="margin-top:8px">
          <input type="hidden" name="login" value="{_admin_esc(login)}">
          <input type="hidden" name="action" value="set_xp">
          <input type="hidden" name="next" value="/admin/user/{_admin_esc(login)}">
          <input name="amount" type="number" placeholder="XP exact" style="width:120px">
          <button class="btn" type="submit">Fixer XP</button>
        </form>

        <form method="post" action="/admin/user_action" class="row" style="margin-top:8px">
          <input type="hidden" name="login" value="{_admin_esc(login)}">
          <input type="hidden" name="action" value="set_stage">
          <input type="hidden" name="next" value="/admin/user/{_admin_esc(login)}">
          <input name="stage" type="number" placeholder="stage" style="width:120px">
          <button class="btn" type="submit">Fixer stage</button>
        </form>

        <form method="post" action="/admin/user_action" class="row" style="margin-top:8px">
          <input type="hidden" name="login" value="{_admin_esc(login)}">
          <input type="hidden" name="action" value="set_happiness">
          <input type="hidden" name="next" value="/admin/user/{_admin_esc(login)}">
          <input name="happiness" type="number" placeholder="bonheur (0-100)" style="width:160px">
          <button class="btn" type="submit">Fixer bonheur</button>
        </form>

        <form method="post" action="/admin/user_action" class="row" style="margin-top:8px">
          <input type="hidden" name="login" value="{_admin_esc(login)}">
          <input type="hidden" name="action" value="reset_active">
          <input type="hidden" name="next" value="/admin/user/{_admin_esc(login)}">
          <button class="btn" type="submit">Reset CM actif</button>
        </form>

      </div>

      <div class="card">
        <div class="muted">Inventaire</div>

        <div class="row" style="margin-top:8px">
          <form method="post" action="/admin/user_action" class="row" style="margin:0">
            <input type="hidden" name="login" value="{_admin_esc(login)}">
            <input type="hidden" name="action" value="give_item">
            <input type="hidden" name="next" value="/admin/user/{_admin_esc(login)}">
            <select name="item_key" style="min-width:260px">{opt_items()}</select>
            <input name="qty" type="number" value="1" style="width:110px">
            <button class="btn" type="submit">Ajouter</button>
          </form>
        </div>

        <div class="row" style="margin-top:8px">
          <form method="post" action="/admin/user_action" class="row" style="margin:0">
            <input type="hidden" name="login" value="{_admin_esc(login)}">
            <input type="hidden" name="action" value="take_item">
            <input type="hidden" name="next" value="/admin/user/{_admin_esc(login)}">
            <select name="item_key" style="min-width:260px">{opt_items()}</select>
            <input name="qty" type="number" value="1" style="width:110px">
            <button class="btn" type="submit">Retirer</button>
          </form>
        </div>

        <div style="margin-top:14px;font-weight:800">Transférer un item</div>
        <form method="post" action="/admin/user_action" class="row" style="margin-top:8px">
          <input type="hidden" name="login" value="{_admin_esc(login)}">
          <input type="hidden" name="action" value="transfer_item">
          <input type="hidden" name="next" value="/admin/user/{_admin_esc(login)}">
          <input name="to_login" placeholder="vers login..." style="min-width:220px">
          <select name="item_key" style="min-width:260px">{opt_items()}</select>
          <input name="qty" type="number" value="1" style="width:110px">
          <button class="btn" type="submit">Transférer</button>
        </form>

        <table>
          <thead><tr><th>item_key</th><th>nom</th><th>qty</th></tr></thead>
          <tbody>
            {''.join([
                f"<tr><td>{_admin_esc(it['item_key'])}</td><td>{_admin_esc(it['name'])}</td><td>{it['qty']}</td></tr>"
            for it in inv]) if inv else "<tr><td colspan='3' class='muted'>Inventaire vide.</td></tr>"}
          </tbody>
        </table>

      </div>
    </div>

  </div>
</body>
</html>"""
    return HTMLResponse(html)


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



@app.post("/internal/inventory/grant")
def internal_inventory_grant(payload: dict, x_api_key: str | None = Header(default=None)):
    """Ajoute un item à l'inventaire d'un viewer (utilisable par le bot/admin)."""
    require_internal_key(x_api_key)

    login = str(payload.get("twitch_login", "") or "").strip().lower()
    item_key = str(payload.get("item_key", "") or "").strip()
    qty = int(payload.get("qty") or 1)

    if not login or not item_key:
        raise HTTPException(status_code=400, detail="Missing twitch_login or item_key")
    qty = max(1, min(qty, 9999))

    with get_db() as conn:
        with conn.cursor() as cur:
            # vérifie item
            cur.execute("SELECT 1 FROM items WHERE key=%s;", (item_key,))
            if not cur.fetchone():
                raise HTTPException(status_code=400, detail="Unknown item_key")

            _grant_item_db(cur, login, item_key, qty)
        conn.commit()

    return {"ok": True, "twitch_login": login, "item_key": item_key, "qty": qty}


@app.get("/internal/announcements/poll")
def internal_announcements_poll(limit: int = 5, x_api_key: str | None = Header(default=None)):
    """Le bot peut poll cette route pour annoncer des événements (points de chaîne, etc.)."""
    require_internal_key(x_api_key)

    limit = max(1, min(int(limit or 5), 20))
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, message
                FROM bot_announcements
                WHERE delivered_at IS NULL
                ORDER BY id ASC
                LIMIT %s;
                """,
                (limit,),
            )
            rows = cur.fetchall()
            ids = [int(r[0]) for r in rows]
            msgs = [str(r[1]) for r in rows]
            if ids:
                cur.execute(
                    "UPDATE bot_announcements SET delivered_at=now() WHERE id = ANY(%s);",
                    (ids,),
                )
        conn.commit()

    return {"messages": msgs}


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

            if joined:
                _ensure_quests(cur, login)
                _quest_progress(cur, login, 'drops', 1)
                if mode == 'coop':
                    _quest_progress(cur, login, 'coop', 1)

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
# =============================================================================
# OVERLAYS — Drop & Show
# Remplace dans main.py :
#   @app.get("/overlay/drop")  def overlay_drop_page()
#   @app.get("/overlay/show")  def overlay_show_page()
# =============================================================================

@app.get("/overlay/drop", response_class=HTMLResponse)
def overlay_drop_page():
    return HTMLResponse("""<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Rajdhani:wght@600;700&family=Orbitron:wght@700;900&family=Share+Tech+Mono&display=swap" rel="stylesheet">
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background: transparent;
    overflow: hidden;
    width: 100vw; height: 100vh;
    font-family: 'Rajdhani', sans-serif;
  }

  /* ── PANEL CONTENEUR ── */
  #panel {
    position: fixed;
    right: 0; top: 50%;
    transform: translateY(-50%) translateX(380px);
    width: 400px;
    transition: transform 0.55s cubic-bezier(0.22, 1, 0.36, 1);
    z-index: 10;
    will-change: transform;
  }
  #panel.visible {
    transform: translateY(-50%) translateX(0px);
  }

  /* ── CARD ── */
  .card {
    position: relative;
    background: linear-gradient(145deg, #050d1a 0%, #08152a 60%, #060e1c 100%);
    border-radius: 16px 0 0 16px;
    overflow: hidden;
    padding: 18px 16px 14px 18px;
    border-left: 1px solid rgba(0,200,255,0.25);
    border-top: 1px solid rgba(0,200,255,0.15);
    border-bottom: 1px solid rgba(0,200,255,0.1);
  }

  /* Trait néon gauche */
  .card::before {
    content: '';
    position: absolute;
    left: 0; top: 10%; bottom: 10%;
    width: 2px;
    background: linear-gradient(180deg, transparent, #00e5ff, #ff2d78, transparent);
    border-radius: 999px;
    animation: borderPulse 2s ease-in-out infinite;
  }
  @keyframes borderPulse {
    0%,100% { opacity: 0.6; }
    50%      { opacity: 1; box-shadow: 0 0 12px #00e5ff; }
  }

  /* Grille scanline */
  .card::after {
    content: '';
    position: absolute;
    inset: 0;
    background: repeating-linear-gradient(
      0deg,
      transparent,
      transparent 3px,
      rgba(0,229,255,0.015) 3px,
      rgba(0,229,255,0.015) 4px
    );
    pointer-events: none;
  }

  /* Coin décoratif top-right */
  .corner {
    position: absolute;
    top: 0; right: 0;
    width: 40px; height: 40px;
    border-bottom: 1px solid rgba(0,229,255,0.2);
    border-left: 1px solid rgba(0,229,255,0.2);
    border-radius: 0 0 0 14px;
  }

  /* ── MODE BADGE ── */
  .mode-badge {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    font-family: 'Orbitron', monospace;
    font-size: 9px;
    font-weight: 700;
    letter-spacing: .12em;
    padding: 3px 10px;
    border-radius: 999px;
    border: 1px solid;
    margin-bottom: 12px;
  }
  .mode-badge.first   { color: #ffd166; border-color: rgba(255,209,102,.4); background: rgba(255,209,102,.08); }
  .mode-badge.random  { color: #7aa2ff; border-color: rgba(122,162,255,.4); background: rgba(122,162,255,.08); }
  .mode-badge.coop    { color: #00ff9d; border-color: rgba(0,255,157,.4);   background: rgba(0,255,157,.08);  }
  .mode-dot {
    width: 5px; height: 5px;
    border-radius: 50%;
    animation: modeDot 1.2s ease-in-out infinite;
  }
  .first  .mode-dot { background: #ffd166; box-shadow: 0 0 6px #ffd166; }
  .random .mode-dot { background: #7aa2ff; box-shadow: 0 0 6px #7aa2ff; }
  .coop   .mode-dot { background: #00ff9d; box-shadow: 0 0 6px #00ff9d; }
  @keyframes modeDot { 0%,100%{opacity:1} 50%{opacity:.3} }

  /* ── BODY ROW ── */
  .body-row {
    display: flex;
    gap: 14px;
    align-items: center;
    margin-bottom: 12px;
  }

  /* ── IMAGE ── */
  .img-wrap {
    position: relative;
    flex-shrink: 0;
  }
  #img {
    width: 80px; height: 80px;
    border-radius: 12px;
    object-fit: contain;
    display: block;
    background: rgba(0,229,255,.04);
    border: 1px solid rgba(0,229,255,.2);
  }
  .img-glow {
    position: absolute;
    inset: -4px;
    border-radius: 16px;
    background: radial-gradient(circle, rgba(0,229,255,.25) 0%, transparent 70%);
    animation: imgGlow 1.8s ease-in-out infinite;
    pointer-events: none;
  }
  @keyframes imgGlow {
    0%,100% { opacity: 0.5; transform: scale(1); }
    50%      { opacity: 1;   transform: scale(1.05); }
  }

  /* ── TEXTS ── */
  .texts { flex: 1; min-width: 0; }
  .drop-title {
    font-family: 'Orbitron', monospace;
    font-size: 14px;
    font-weight: 900;
    color: #e8f4ff;
    line-height: 1.2;
    letter-spacing: .04em;
    text-shadow: 0 0 20px rgba(0,229,255,.4);
    margin-bottom: 4px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .drop-sub {
    font-family: 'Share Tech Mono', monospace;
    font-size: 11px;
    color: #5a7a9a;
    line-height: 1.4;
  }
  .drop-sub span { color: #00e5ff; }

  /* ── TIMER ── */
  .timer-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 8px;
  }
  .timer-label {
    font-family: 'Share Tech Mono', monospace;
    font-size: 11px;
    color: #3a5570;
    letter-spacing: .06em;
  }
  #timer-val {
    font-family: 'Orbitron', monospace;
    font-size: 13px;
    font-weight: 700;
    color: #00e5ff;
    text-shadow: 0 0 10px rgba(0,229,255,.6);
    transition: color .3s;
  }
  #timer-val.urgent { color: #ff2d78; text-shadow: 0 0 10px rgba(255,45,120,.6); animation: urgentFlash .5s ease-in-out infinite; }
  @keyframes urgentFlash { 0%,100%{opacity:1} 50%{opacity:.5} }

  /* ── PROGRESS BAR ── */
  .progress-track {
    height: 6px;
    background: rgba(255,255,255,.06);
    border-radius: 999px;
    overflow: hidden;
    border: 1px solid rgba(255,255,255,.06);
    margin-bottom: 10px;
  }
  #progress-fill {
    height: 100%;
    width: 100%;
    border-radius: 999px;
    background: linear-gradient(90deg, #00e5ff, #7aa2ff);
    transform-origin: left;
    transition: width 0.5s linear;
    box-shadow: 0 0 8px rgba(0,229,255,.5);
  }
  #progress-fill.urgent {
    background: linear-gradient(90deg, #ff2d78, #ff9d2d);
    box-shadow: 0 0 8px rgba(255,45,120,.6);
  }

  /* ── COOP HITS ── */
  .hits-row {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
  }
  .hits-label {
    font-family: 'Share Tech Mono', monospace;
    font-size: 10px;
    color: #3a5570;
    letter-spacing: .06em;
    flex-shrink: 0;
  }
  .hits-dots {
    display: flex;
    gap: 4px;
    flex-wrap: wrap;
  }
  .hit-dot {
    width: 8px; height: 8px;
    border-radius: 50%;
    border: 1px solid rgba(0,255,157,.3);
    background: rgba(0,255,157,.08);
    transition: all .2s;
  }
  .hit-dot.filled {
    background: #00ff9d;
    border-color: #00ff9d;
    box-shadow: 0 0 6px rgba(0,255,157,.8);
  }
  #hits-count {
    font-family: 'Orbitron', monospace;
    font-size: 11px;
    color: #00ff9d;
    margin-left: auto;
    flex-shrink: 0;
  }

  /* ── COMMANDE ── */
  .cmd-hint {
    font-family: 'Share Tech Mono', monospace;
    font-size: 10px;
    color: #2a4560;
    letter-spacing: .08em;
    text-align: center;
    padding-top: 8px;
    border-top: 1px solid rgba(255,255,255,.04);
  }
  .cmd-hint code {
    color: #00e5ff;
    font-family: 'Share Tech Mono', monospace;
    font-size: 11px;
    text-shadow: 0 0 8px rgba(0,229,255,.5);
  }

  /* ── PARTICULES ── */
  #particles {
    position: fixed;
    inset: 0;
    pointer-events: none;
    z-index: 5;
  }
  .particle {
    position: absolute;
    border-radius: 50%;
    pointer-events: none;
    animation: particleFly linear forwards;
  }
  @keyframes particleFly {
    0%   { opacity: 1; transform: translate(0,0) scale(1); }
    100% { opacity: 0; transform: var(--tx, translate(0,-80px)) scale(0.2); }
  }

  /* ── ENTRÉE / SORTIE ── */
  @keyframes slideIn {
    from { transform: translateY(-50%) translateX(380px); }
    to   { transform: translateY(-50%) translateX(0); }
  }
  @keyframes slideOut {
    from { transform: translateY(-50%) translateX(0); }
    to   { transform: translateY(-50%) translateX(400px); }
  }
</style>
</head>
<body>

<canvas id="particles"></canvas>

<div id="panel">
  <div class="card">
    <div class="corner"></div>

    <div id="mode-badge" class="mode-badge coop">
      <div class="mode-dot"></div>
      <span id="mode-label">COOP</span>
    </div>

    <div class="body-row">
      <div class="img-wrap">
        <img id="img" src="" alt="">
        <div class="img-glow"></div>
      </div>
      <div class="texts">
        <div class="drop-title" id="title">Capsule Mystère</div>
        <div class="drop-sub" id="sub">Tape <span>!grab</span> pour participer</div>
      </div>
    </div>

    <div class="timer-row">
      <div class="timer-label">// TEMPS RESTANT</div>
      <div id="timer-val">--s</div>
    </div>
    <div class="progress-track">
      <div id="progress-fill"></div>
    </div>

    <div id="hits-section" class="hits-row" style="display:none">
      <div class="hits-label">PARTICIPANTS</div>
      <div id="hits-dots" class="hits-dots"></div>
      <div id="hits-count">0</div>
    </div>

    <div class="cmd-hint" id="cmd-hint">
      Tape <code>!grab</code> dans le chat
    </div>
  </div>
</div>

<audio id="dropSfx" preload="auto" src="/static/drop.mp3"></audio>

<script>
const panel      = document.getElementById('panel');
const titleEl    = document.getElementById('title');
const subEl      = document.getElementById('sub');
const imgEl      = document.getElementById('img');
const timerEl    = document.getElementById('timer-val');
const fillEl     = document.getElementById('progress-fill');
const modeBadge  = document.getElementById('mode-badge');
const modeLabel  = document.getElementById('mode-label');
const hitsSection= document.getElementById('hits-section');
const hitsDots   = document.getElementById('hits-dots');
const hitsCount  = document.getElementById('hits-count');
const cmdHint    = document.getElementById('cmd-hint');
const dropSfx    = document.getElementById('dropSfx');
const canvas     = document.getElementById('particles');
const ctx        = canvas.getContext('2d');

let showing = false;
let lastDropId = null;
let totalDuration = null;
let spawnedInitial = false;

// ── Canvas particules ──────────────────────────────────────
function resizeCanvas() {
  canvas.width  = window.innerWidth;
  canvas.height = window.innerHeight;
}
resizeCanvas();
window.addEventListener('resize', resizeCanvas);

const particles = [];
function spawnParticles(x, y, count, colors) {
  for (let i = 0; i < count; i++) {
    const angle = Math.random() * Math.PI * 2;
    const speed = 1.5 + Math.random() * 3;
    particles.push({
      x, y,
      vx: Math.cos(angle) * speed,
      vy: Math.sin(angle) * speed - 2,
      radius: 2 + Math.random() * 3,
      color: colors[Math.floor(Math.random() * colors.length)],
      life: 1,
      decay: 0.018 + Math.random() * 0.02,
      gravity: 0.08,
    });
  }
}

function drawParticles() {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  for (let i = particles.length - 1; i >= 0; i--) {
    const p = particles[i];
    p.x += p.vx;
    p.y += p.vy;
    p.vy += p.gravity;
    p.life -= p.decay;
    if (p.life <= 0) { particles.splice(i, 1); continue; }
    ctx.save();
    ctx.globalAlpha = p.life;
    ctx.beginPath();
    ctx.arc(p.x, p.y, p.radius, 0, Math.PI * 2);
    ctx.fillStyle = p.color;
    ctx.shadowBlur = 6;
    ctx.shadowColor = p.color;
    ctx.fill();
    ctx.restore();
  }
  requestAnimationFrame(drawParticles);
}
drawParticles();

// ── SFX ───────────────────────────────────────────────────
function playDropSfx() {
  try {
    dropSfx.currentTime = 0;
    const p = dropSfx.play();
    if (p && p.catch) p.catch(() => {});
  } catch(e) {}
}

// ── Show / Hide ───────────────────────────────────────────
function showPanel() {
  if (showing) return;
  showing = true;
  panel.classList.add('visible');

  // burst de particules depuis le bord droit
  const px = window.innerWidth - 10;
  const py = window.innerHeight / 2;
  spawnParticles(px, py, 40, ['#00e5ff','#7aa2ff','#00ff9d','#ffffff']);
  spawnedInitial = true;
}

function hidePanel() {
  if (!showing) return;
  showing = false;
  panel.classList.remove('visible');
  spawnedInitial = false;
  totalDuration = null;
}

// ── Update mode badge ─────────────────────────────────────
function setMode(mode) {
  modeBadge.className = 'mode-badge ' + mode;
  if (mode === 'first')  modeLabel.textContent = '⚡ PREMIER';
  if (mode === 'random') modeLabel.textContent = '🎲 RANDOM';
  if (mode === 'coop')   modeLabel.textContent = '🤝 COOP';
}

// ── Update hits dots ──────────────────────────────────────
function updateHits(count, target) {
  const show = target > 0;
  hitsSection.style.display = show ? 'flex' : 'none';
  if (!show) return;

  const maxDots = Math.min(target, 20);
  if (hitsDots.children.length !== maxDots) {
    hitsDots.innerHTML = '';
    for (let i = 0; i < maxDots; i++) {
      const d = document.createElement('div');
      d.className = 'hit-dot';
      hitsDots.appendChild(d);
    }
  }
  const dots = hitsDots.querySelectorAll('.hit-dot');
  const prevFilled = hitsDots.querySelectorAll('.hit-dot.filled').length;
  dots.forEach((d, i) => {
    const filled = i < count;
    if (filled && !d.classList.contains('filled')) {
      d.classList.add('filled');
      // micro burst sur le nouveau dot
      const rect = d.getBoundingClientRect();
      spawnParticles(rect.left + 4, rect.top + 4, 8, ['#00ff9d','#ffffff']);
    } else if (!filled) {
      d.classList.remove('filled');
    }
  });
  hitsCount.textContent = count + (target ? '/' + target : '');
}

// ── Tick ──────────────────────────────────────────────────
async function tick() {
  try {
    const r = await fetch('/overlay/drop_state', { cache: 'no-store' });
    const j = await r.json();

    if (!j.show) { hidePanel(); return; }

    const d = j.drop;

    // Nouveau drop
    if (d.id && d.id !== lastDropId) {
      lastDropId = d.id;
      totalDuration = d.remaining; // on capture la durée initiale
      playDropSfx();
      showPanel();
    }

    if (!showing) showPanel();

    // Contenu
    imgEl.src = d.media || '';
    titleEl.textContent = d.title || 'DROP';

    const remaining = d.remaining ?? 0;
    const urgent = remaining <= 5;

    // Timer
    timerEl.textContent = remaining + 's';
    timerEl.className = urgent ? 'urgent' : '';

    // Progress (basé sur le temps restant)
    if (!totalDuration) totalDuration = remaining;
    const pct = totalDuration > 0 ? Math.max(0, (remaining / totalDuration) * 100) : 0;
    fillEl.style.width = pct + '%';
    fillEl.className = urgent ? 'urgent' : '';

    // Mode
    setMode(d.mode || 'coop');

    // Sous-texte & hits
    if (d.mode === 'coop') {
      subEl.innerHTML = `Tape <span>!grab</span> — XP progressif selon les participants`;
      cmdHint.innerHTML = `Tape <code>!grab</code> · ${d.count || 0} participant${(d.count||0) > 1 ? 's' : ''}`;
      updateHits(d.count || 0, 0);
    } else {
      subEl.innerHTML = `Tape <span>!grab</span> · ${d.count || 0} participant${(d.count||0) > 1 ? 's' : ''}`;
      cmdHint.innerHTML = d.mode === 'first'
        ? `<code>!grab</code> · LE PREMIER GAGNE`
        : `<code>!grab</code> · TIRAGE AU SORT`;
      hitsSection.style.display = 'none';
    }

    // Particules continues si urgence
    if (urgent && Math.random() < 0.3) {
      const px = window.innerWidth - 350 + Math.random() * 20;
      const py = window.innerHeight / 2 + (Math.random() - 0.5) * 100;
      spawnParticles(px, py, 3, ['#ff2d78','#ff9d2d']);
    }

  } catch(e) {}
}

setInterval(tick, 500);
tick();
</script>
</body>
</html>
""")


@app.get("/overlay/show", response_class=HTMLResponse)
def overlay_show_page():
    return HTMLResponse(r"""<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Rajdhani:wght@600;700&family=Orbitron:wght@700;900&family=Share+Tech+Mono&display=swap" rel="stylesheet">
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }

body {
  background: transparent;
  overflow: hidden;
  width: 100vw; height: 100vh;
  font-family: 'Rajdhani', sans-serif;
}

/* ── SCÈNE CENTRÉE ── */
.scene {
  position: fixed;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  pointer-events: none;
}

/* ── CARTE TCG ── */
.tcg-card {
  position: relative;
  width: 429px;
  /* ratio TCG standard ~1:1.4 */
  height: 600px;

  display: none;
  flex-direction: column;

  background: linear-gradient(160deg, #04090f 0%, #070e1d 40%, #050b18 100%);
  border-radius: 18px;
  border: 1px solid rgba(0,229,255,0.3);
  overflow: hidden;

  opacity: 0;
  transform: scale(0.85) rotateY(8deg);
  transition: opacity 0.4s ease, transform 0.5s cubic-bezier(0.34,1.56,0.64,1);
  will-change: transform, opacity;

  box-shadow:
    0 0 0 1px rgba(0,229,255,0.08),
    0 0 30px rgba(0,229,255,0.15),
    0 0 80px rgba(0,229,255,0.06),
    inset 0 0 60px rgba(0,0,0,0.5);
}
.tcg-card.showing {
  opacity: 1;
  transform: scale(1) rotateY(0deg);
}

/* Reflet holographique animé */
.tcg-card::before {
  content: '';
  position: absolute;
  inset: 0;
  background: linear-gradient(
    115deg,
    transparent 30%,
    rgba(0,229,255,.06) 40%,
    rgba(255,45,120,.06) 50%,
    rgba(0,255,157,.05) 60%,
    transparent 70%
  );
  background-size: 200% 200%;
  animation: holoShift 3s ease-in-out infinite;
  border-radius: 18px;
  pointer-events: none;
  z-index: 20;
}
@keyframes holoShift {
  0%   { background-position: 0% 0%; opacity: .6; }
  50%  { background-position: 100% 100%; opacity: 1; }
  100% { background-position: 0% 0%; opacity: .6; }
}

/* Scanlines */
.tcg-card::after {
  content: '';
  position: absolute;
  inset: 0;
  background: repeating-linear-gradient(
    0deg,
    transparent,
    transparent 2px,
    rgba(0,0,0,0.08) 2px,
    rgba(0,0,0,0.08) 3px
  );
  pointer-events: none;
  z-index: 21;
  border-radius: 18px;
}

/* ── HEADER CARTE ── */
.card-header {
  position: relative;
  padding: 12px 14px 8px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  z-index: 5;
  border-bottom: 1px solid rgba(0,229,255,0.1);
  background: linear-gradient(90deg, rgba(0,229,255,.04) 0%, transparent 100%);
}

.card-name {
  font-family: 'Orbitron', monospace;
  font-size: 13px;
  font-weight: 900;
  color: #e8f4ff;
  letter-spacing: .06em;
  text-shadow: 0 0 15px rgba(0,229,255,.5);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 180px;
}

.card-type {
  font-family: 'Share Tech Mono', monospace;
  font-size: 9px;
  color: #00e5ff;
  letter-spacing: .12em;
  padding: 2px 8px;
  border: 1px solid rgba(0,229,255,.3);
  border-radius: 999px;
  background: rgba(0,229,255,.06);
  white-space: nowrap;
}

/* ── IMAGE ZONE ── */
.card-img-wrap {
  position: relative;
  flex: 1;
  overflow: hidden;
  margin: 0;
}

#cmimg {
  width: 100%;
  height: 100%;
  object-fit: cover;
  display: block;
  filter: saturate(1.1) contrast(1.05);
}

/* Vignette sur l'image */
.card-img-wrap::after {
  content: '';
  position: absolute;
  inset: 0;
  background:
    linear-gradient(0deg, rgba(4,9,15,1) 0%, transparent 30%),
    linear-gradient(180deg, rgba(4,9,15,.4) 0%, transparent 20%);
  pointer-events: none;
}

/* Lignes de grille HUD sur l'image */
.card-hud {
  position: absolute;
  inset: 0;
  z-index: 3;
  pointer-events: none;
  opacity: .4;
}
.hud-line-h {
  position: absolute;
  left: 0; right: 0;
  height: 1px;
  background: linear-gradient(90deg, transparent 0%, rgba(0,229,255,.4) 20%, rgba(0,229,255,.4) 80%, transparent 100%);
}
.hud-line-v {
  position: absolute;
  top: 0; bottom: 0;
  width: 1px;
  background: linear-gradient(180deg, transparent 0%, rgba(0,229,255,.3) 20%, rgba(0,229,255,.3) 80%, transparent 100%);
}

/* Corner brackets */
.corner-tl, .corner-tr, .corner-bl, .corner-br {
  position: absolute;
  width: 14px; height: 14px;
  z-index: 4;
}
.corner-tl { top: 6px; left: 6px;   border-top: 1.5px solid #00e5ff; border-left: 1.5px solid #00e5ff; }
.corner-tr { top: 6px; right: 6px;  border-top: 1.5px solid #ff2d78; border-right: 1.5px solid #ff2d78; }
.corner-bl { bottom: 6px; left: 6px;  border-bottom: 1.5px solid #00e5ff; border-left: 1.5px solid #00e5ff; }
.corner-br { bottom: 6px; right: 6px; border-bottom: 1.5px solid #ff2d78; border-right: 1.5px solid #ff2d78; }

/* Viewer badge flottant sur l'image */
.viewer-badge {
  position: absolute;
  bottom: 10px;
  left: 10px;
  right: 10px;
  z-index: 5;
  display: flex;
  align-items: center;
  gap: 8px;
  background: rgba(4,9,15,.8);
  backdrop-filter: blur(8px);
  border: 1px solid rgba(0,229,255,.2);
  border-radius: 8px;
  padding: 6px 10px;
}
#avatar {
  width: 28px; height: 28px;
  border-radius: 6px;
  object-fit: cover;
  border: 1px solid rgba(0,229,255,.3);
  flex-shrink: 0;
}
.viewer-info { flex: 1; min-width: 0; }
#viewer-name {
  font-family: 'Orbitron', monospace;
  font-size: 10px;
  font-weight: 700;
  color: #e8f4ff;
  letter-spacing: .04em;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.viewer-sub {
  font-family: 'Share Tech Mono', monospace;
  font-size: 9px;
  color: #00e5ff;
  margin-top: 1px;
}

/* ── STATS ZONE ── */
.card-stats {
  padding: 10px 14px 14px;
  z-index: 5;
  position: relative;
  background: linear-gradient(0deg, rgba(0,229,255,.03) 0%, transparent 100%);
}

.stat-row {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-bottom: 7px;
}
.stat-row:last-child { margin-bottom: 0; }

.stat-label {
  font-family: 'Share Tech Mono', monospace;
  font-size: 9px;
  color: #3a6080;
  letter-spacing: .1em;
  width: 58px;
  flex-shrink: 0;
}
.stat-track {
  flex: 1;
  height: 5px;
  background: rgba(255,255,255,.06);
  border-radius: 999px;
  overflow: hidden;
  border: 1px solid rgba(255,255,255,.05);
}
.stat-fill {
  height: 100%;
  border-radius: 999px;
  transition: width 0.6s ease;
}
.stat-fill.xp  { background: linear-gradient(90deg, #7aa2ff, #00e5ff); box-shadow: 0 0 6px rgba(0,229,255,.4); }
.stat-fill.hp  { background: linear-gradient(90deg, #ff4fb3, #ff2d78); box-shadow: 0 0 6px rgba(255,45,120,.4); }

.stat-val {
  font-family: 'Orbitron', monospace;
  font-size: 9px;
  color: #c8d8f0;
  width: 54px;
  text-align: right;
  flex-shrink: 0;
}

/* Lineage / stage badges */
.card-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-top: 8px;
}
.tcg-badge {
  display: inline-block;
  font-family: 'Share Tech Mono', monospace;
  font-size: 8px;
  color: #5a8aaa;
  letter-spacing: .1em;
  padding: 2px 7px;
  border: 1px solid rgba(255,255,255,.08);
  border-radius: 999px;
  background: rgba(255,255,255,.03);
}
.tcg-badge.stage {
  color: #00ff9d;
  border-color: rgba(0,255,157,.3);
  background: rgba(0,255,157,.06);
  text-shadow: 0 0 8px rgba(0,255,157,.5);
}

/* ── GLITCH EFFECT (apparition) ── */
@keyframes glitchIn {
  0%   { clip-path: inset(40% 0 50% 0); transform: translate(-4px,0) skewX(-2deg); opacity:.8; }
  10%  { clip-path: inset(10% 0 80% 0); transform: translate(4px,0)  skewX(2deg);  }
  20%  { clip-path: inset(70% 0 10% 0); transform: translate(-2px,0); }
  30%  { clip-path: inset(0% 0 0% 0);   transform: translate(0,0);   opacity:1; }
  100% { clip-path: inset(0% 0 0% 0);   transform: translate(0,0);   opacity:1; }
}
.tcg-card.glitch-enter {
  animation: glitchIn 0.35s steps(1) forwards;
}

/* ── PARTICULES CANVAS ── */
#pcanvas {
  position: fixed;
  inset: 0;
  pointer-events: none;
  z-index: 30;
}
</style>
</head>
<body>

<canvas id="pcanvas"></canvas>

<div class="scene">
  <div id="card" class="tcg-card">

    <!-- Header -->
    <div class="card-header">
      <div class="card-name" id="cm-name">CAPSMÖNS</div>
      <div class="card-type" id="cm-type">LINEAGE · STAGE I</div>
    </div>

    <!-- Image -->
    <div class="card-img-wrap">
      <img id="cmimg" src="" alt="">
      <div class="card-hud">
        <div class="hud-line-h" style="top:33%"></div>
        <div class="hud-line-h" style="top:66%"></div>
        <div class="hud-line-v" style="left:25%"></div>
        <div class="hud-line-v" style="left:75%"></div>
        <div class="corner-tl"></div>
        <div class="corner-tr"></div>
        <div class="corner-bl"></div>
        <div class="corner-br"></div>
      </div>

      <!-- Viewer badge -->
      <div class="viewer-badge">
        <img id="avatar" src="" alt="">
        <div class="viewer-info">
          <div id="viewer-name">@viewer</div>
          <div class="viewer-sub">// !show</div>
        </div>
      </div>
    </div>

    <!-- Stats -->
    <div class="card-stats">
      <div class="stat-row">
        <div class="stat-label">XP</div>
        <div class="stat-track"><div id="xp-fill"  class="stat-fill xp"  style="width:0%"></div></div>
        <div class="stat-val"  id="xp-val">0 XP</div>
      </div>
      <div class="stat-row">
        <div class="stat-label">BONHEUR</div>
        <div class="stat-track"><div id="hp-fill" class="stat-fill hp" style="width:0%"></div></div>
        <div class="stat-val" id="hp-val">0%</div>
      </div>

      <div class="card-footer">
        <span class="tcg-badge" id="lineage-badge">—</span>
        <span class="tcg-badge stage" id="stage-badge">STAGE I</span>
      </div>
    </div>

  </div>
</div>

<audio id="sfx" preload="auto" src="/static/show.mp3"></audio>

<script>
const card      = document.getElementById('card');
const cmName    = document.getElementById('cm-name');
const cmType    = document.getElementById('cm-type');
const cmImg     = document.getElementById('cmimg');
const avatar    = document.getElementById('avatar');
const viewerName= document.getElementById('viewer-name');
const xpFill    = document.getElementById('xp-fill');
const xpVal     = document.getElementById('xp-val');
const hpFill    = document.getElementById('hp-fill');
const hpVal     = document.getElementById('hp-val');
const lineageBadge = document.getElementById('lineage-badge');
const stageBadge   = document.getElementById('stage-badge');
const sfx       = document.getElementById('sfx');
const pcanvas   = document.getElementById('pcanvas');
const pctx      = pcanvas.getContext('2d');

let showing = false;
let hideTimer = null;
let lastSig = '';
const DISPLAY_MS = 8000;

// ── Canvas ─────────────────────────────────────────────
function resizeCanvas() { pcanvas.width = window.innerWidth; pcanvas.height = window.innerHeight; }
resizeCanvas();
window.addEventListener('resize', resizeCanvas);

const particles = [];
function spawnBurst(cx, cy) {
  const colors = ['#00e5ff','#ff2d78','#00ff9d','#7aa2ff','#ffd166','#ffffff'];
  for (let i = 0; i < 60; i++) {
    const angle = Math.random() * Math.PI * 2;
    const speed = 2 + Math.random() * 5;
    particles.push({
      x: cx, y: cy,
      vx: Math.cos(angle) * speed,
      vy: Math.sin(angle) * speed - 1,
      r: 1.5 + Math.random() * 3,
      color: colors[Math.floor(Math.random() * colors.length)],
      life: 1,
      decay: 0.014 + Math.random() * 0.016,
      gravity: 0.06,
    });
  }
  // quelques "sparks" allongées
  for (let i = 0; i < 20; i++) {
    const angle = -Math.PI/2 + (Math.random()-0.5)*1.2;
    const speed = 4 + Math.random() * 8;
    particles.push({
      x: cx, y: cy,
      vx: Math.cos(angle) * speed,
      vy: Math.sin(angle) * speed,
      r: 1, rw: 4,
      color: '#00e5ff',
      life: 1, decay: 0.025, gravity: 0.1,
    });
  }
}

function drawParticles() {
  pctx.clearRect(0, 0, pcanvas.width, pcanvas.height);
  for (let i = particles.length - 1; i >= 0; i--) {
    const p = particles[i];
    p.x += p.vx; p.y += p.vy; p.vy += p.gravity;
    p.life -= p.decay;
    if (p.life <= 0) { particles.splice(i,1); continue; }
    pctx.save();
    pctx.globalAlpha = Math.pow(p.life, 1.5);
    pctx.beginPath();
    if (p.rw) {
      pctx.ellipse(p.x, p.y, p.rw, p.r, Math.atan2(p.vy, p.vx), 0, Math.PI*2);
    } else {
      pctx.arc(p.x, p.y, p.r, 0, Math.PI*2);
    }
    pctx.fillStyle = p.color;
    pctx.shadowBlur = 8;
    pctx.shadowColor = p.color;
    pctx.fill();
    pctx.restore();
  }
  requestAnimationFrame(drawParticles);
}
drawParticles();

// ── SFX ───────────────────────────────────────────────
function playSfx() {
  try { sfx.currentTime = 0; const p = sfx.play(); if (p?.catch) p.catch(()=>{}); } catch(e){}
}

// ── Carte ─────────────────────────────────────────────
function showCard() {
  if (!showing) {
    card.style.display = 'flex';
    void card.offsetWidth;
    card.classList.add('glitch-enter');
    setTimeout(() => card.classList.remove('glitch-enter'), 400);
    card.classList.add('showing');
    showing = true;

    // Burst centré
    const rect = card.getBoundingClientRect();
    spawnBurst(rect.left + rect.width/2, rect.top + rect.height/2);
  }
  if (hideTimer) clearTimeout(hideTimer);
  hideTimer = setTimeout(hideCard, DISPLAY_MS);
}

function hideCard() {
  if (!showing) return;
  card.classList.remove('showing');
  setTimeout(() => { card.style.display = 'none'; }, 400);
  showing = false;
  hideTimer = null;
}

// ── Stage label ────────────────────────────────────────
function stageLabel(s) {
  const labels = { 0:'ŒOEUF', 1:'STAGE I', 2:'STAGE II', 3:'STAGE III' };
  return labels[s] || ('STAGE ' + s);
}

// ── Tick ───────────────────────────────────────────────
async function tick() {
  try {
    const r = await fetch('/overlay/state', { cache: 'no-store' });
    const j = await r.json();

    if (!j.show) return;

    const sig = `${j.viewer?.name}|${j.cm?.name}|${j.xp?.total}|${j.happiness?.pct ?? 0}`;
    if (sig !== lastSig) {
      lastSig = sig;
      playSfx();
    }

    // Viewer
    viewerName.textContent = '@' + (j.viewer?.name || '?');
    avatar.src = j.viewer?.avatar || '';

    // CM
    cmImg.src  = j.cm?.media || '';
    cmName.textContent = (j.cm?.name || 'CAPSMÖNS').toUpperCase();

    const stage = j.cm?.stage ?? 0;
    const lineage = j.cm?.lineage_key || '—';
    lineageBadge.textContent = lineage.toUpperCase();
    stageBadge.textContent   = stageLabel(stage);
    cmType.textContent       = `${lineage.toUpperCase()} · ${stageLabel(stage)}`;

    // XP
    const xpPct = j.xp?.pct ?? 100;
    const xpTotal = j.xp?.total ?? 0;
    const toNext  = j.xp?.to_next;
    xpFill.style.width = xpPct + '%';
    xpVal.textContent  = toNext
      ? `${xpTotal} / ${xpTotal + toNext}`
      : `${xpTotal} MAX`;

    // Bonheur
    const hpPct = j.happiness?.pct ?? 0;
    hpFill.style.width = hpPct + '%';
    hpVal.textContent  = hpPct + '%';

    showCard();
  } catch(e) {}
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
# ADMIN: admin Stats
# =============================================================================


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
        trigger_evolution(evo_payload, x_api_key=os.environ.get("INTERNAL_API_KEY"))

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
            if happiness_gain > 0:
                _ensure_quests(cur, login)
                _quest_progress(cur, login, 'candy', 1)
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

    # Tracking quête !show
    with get_db() as conn:
        with conn.cursor() as cur:
            _ensure_quests(cur, login)
            _quest_progress(cur, login, 'show', 1)
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
                ;""", (login, cm_key, cm_lineage, (not has_active), acquired_from))

            # si le CM existait déjà, on ne change rien (important : “ne changer que le nécessaire”)

        conn.commit()

    return {"ok": True, "twitch_login": login, "cm_key": cm_key, "acquired_from": acquired_from}


# =============================================================================
# ADMIN: lancer un drop manuel (page + endpoint)
# =============================================================================
    
@app.post("/admin/drop/spawn")
def admin_drop_spawn(payload: dict, credentials: HTTPBasicCredentials = Depends(security)):
    require_admin(credentials)

    mode = str(payload.get("mode", "")).strip().lower()
    title = str(payload.get("title", "")).strip()
    media_url = str(payload.get("media_url", "")).strip()
    duration = int(payload.get("duration_seconds", 15))
    ticket_key = str(payload.get("ticket_key", "")).strip()
    ticket_qty = int(payload.get("ticket_qty", 1))
    target_hits = payload.get("target_hits", None)

    if mode not in ("first", "random", "coop"):
        raise HTTPException(status_code=400, detail="Invalid mode")
    if not title or not media_url:
        raise HTTPException(status_code=400, detail="Missing title/media_url")
    if not ticket_key:
        raise HTTPException(status_code=400, detail="Missing ticket_key")

    duration = max(5, min(duration, 60))
    ticket_qty = max(1, min(ticket_qty, 50))

    if mode == "coop":
        target_hits = int(target_hits or 10)
        target_hits = max(2, min(target_hits, 999))
    else:
        target_hits = None

    with get_db() as conn:
        with conn.cursor() as cur:
            # expire any previous active drop
            cur.execute("UPDATE drops SET status='expired', resolved_at=now() WHERE status='active';")

            cur.execute(
                """
                INSERT INTO drops (mode, title, media_url, xp_bonus, ticket_key, ticket_qty, target_hits, status, expires_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,'active', now() + (%s || ' seconds')::interval)
                RETURNING id;
                """,
                (mode, title, media_url, 0, ticket_key, ticket_qty, target_hits, duration),
            )
            drop_id = int(cur.fetchone()[0])

        conn.commit()

    return {"ok": True, "drop_id": drop_id}


@app.post("/admin/points/save")
def admin_points_save(
    cp_enabled: str | None = Form(None),
    cp_reward_drop_coop_id: str | None = Form(None),
    cp_reward_capsule_id: str | None = Form(None),
    cp_reward_candy_id: str | None = Form(None),
    cp_reward_egg_id: str | None = Form(None),
    cp_capsule_item_key: str | None = Form(None),
    cp_candy_item_key: str | None = Form(None),
    cp_egg_item_key: str | None = Form(None),
    cp_drop_pick_kind: str | None = Form(None),
    cp_drop_duration_seconds: str | None = Form(None),
    cp_drop_target_hits: str | None = Form(None),
    cp_drop_ticket_qty: str | None = Form(None),
    cp_drop_fallback_icon_url: str | None = Form(None),
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)

    enabled = "true" if (cp_enabled == "true") else "false"

    with get_db() as conn:
        with conn.cursor() as cur:
            kv_set(cur, "cp_enabled", enabled)
            kv_set(cur, "cp_reward_drop_coop_id", (cp_reward_drop_coop_id or "").strip())
            kv_set(cur, "cp_reward_capsule_id", (cp_reward_capsule_id or "").strip())
            kv_set(cur, "cp_reward_candy_id", (cp_reward_candy_id or "").strip())
            kv_set(cur, "cp_reward_egg_id", (cp_reward_egg_id or "").strip())
            kv_set(cur, "cp_capsule_item_key", (cp_capsule_item_key or "grande_capsule").strip())
            kv_set(cur, "cp_candy_item_key", (cp_candy_item_key or "bonbon_2").strip())
            kv_set(cur, "cp_egg_item_key", (cp_egg_item_key or "").strip())
            kv_set(cur, "cp_drop_pick_kind", (cp_drop_pick_kind or "any").strip().lower())
            kv_set(cur, "cp_drop_duration_seconds", str(int(cp_drop_duration_seconds or 20)))
            kv_set(cur, "cp_drop_target_hits", str(int(cp_drop_target_hits or 10)))
            kv_set(cur, "cp_drop_ticket_qty", str(int(cp_drop_ticket_qty or 1)))
            kv_set(cur, "cp_drop_fallback_icon_url", (cp_drop_fallback_icon_url or "").strip())
        conn.commit()

    return RedirectResponse(url="/admin/points?msg=Sauvegardé", status_code=302)


@app.post("/admin/eventsub/subscribe_channel_points")
def admin_eventsub_subscribe_channel_points(credentials: HTTPBasicCredentials = Depends(security)):
    require_admin(credentials)

    cid = os.environ.get("TWITCH_CLIENT_ID", "")
    if not cid:
        raise HTTPException(status_code=500, detail="Missing TWITCH_CLIENT_ID")

    token = twitch_app_token()

    with get_db() as conn:
        with conn.cursor() as cur:
            broadcaster_user_id = (kv_get(cur, "broadcaster_user_id", "") or "").strip()
        conn.commit()

    if not broadcaster_user_id:
        raise HTTPException(status_code=400, detail="broadcaster_user_id manquant (va live une fois, ou renseigne-le en DB)")

    # callback public
    base = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
    if not base:
        base = "https://capsmons.devlooping.fr"  # fallback raisonnable

    callback = f"{base}/eventsub"
    secret = os.environ.get("EVENTSUB_SECRET", "")

    body = {
        "type": "channel.channel_points_custom_reward_redemption.add",
        "version": "1",
        "condition": {"broadcaster_user_id": broadcaster_user_id},
        "transport": {"method": "webhook", "callback": callback, "secret": secret},
    }

    r = requests.post(
        "https://api.twitch.tv/helix/eventsub/subscriptions",
        json=body,
        headers={"Authorization": f"Bearer {token}", "Client-Id": cid, "Content-Type": "application/json"},
        timeout=10,
    )

    if r.status_code >= 400:
        raise HTTPException(status_code=500, detail=f"Twitch error {r.status_code}: {r.text[:500]}")

    return RedirectResponse(url="/admin/points?msg=Souscription créée", status_code=302)



# =============================================================================
# ADMIN SPA — Route unifiée + Stats par stream
# =============================================================================
# Ce fichier contient les remplacements à faire dans main.py :
#
# 1. Remplacer @app.get("/admin") et def admin_home(...)   → nouvelle version
# 2. Remplacer @app.get("/admin/stats") et def admin_stats(...) → nouvelle version
# 3. Remplacer @app.get("/admin/cms") et def admin_cms(...)    → nouvelle version
# 4. Remplacer @app.get("/admin/rp") et def admin_rp(...)     → nouvelle version
# 5. Remplacer @app.get("/admin/forms") et def admin_forms(...) → nouvelle version
# 6. Remplacer @app.get("/admin/points") et def admin_points(...) → nouvelle version
# 7. Remplacer @app.get("/admin/autodrop") et def admin_autodrop_page(...) → nouvelle version
# 8. Remplacer @app.get("/admin/drops") et def admin_drops_page(...) → nouvelle version
# 9. AJOUTER @app.get("/admin/stats/json") — nouveau endpoint JSON pour dashboard
#
# =============================================================================


# ─────────────────────────────────────────────────────────────────────────────
# HELPER : charge tout le contexte Jinja2 pour la SPA en une seule connexion DB
# ─────────────────────────────────────────────────────────────────────────────

def _build_admin_context(request: Request, flash: str | None = None, flash_kind: str | None = None,
                         q: str | None = None, page: int = 1, per: int = 50) -> dict:
    """Charge TOUTES les données nécessaires au template admin_spa.html en une seule passe DB."""
    import math

    q_clean = (q or "").strip().lower()
    try:    page = max(1, int(page or 1))
    except: page = 1
    try:    per = max(10, min(200, int(per or 50)))
    except: per = 50

    ctx: dict = {
        "request": request,
        "flash": flash,
        "flash_kind": flash_kind,
        "q": q_clean,
        "page": page,
        "per": per,
        # defaults (overwritten below)
        "is_live": False,
        "total_users": 0,
        "total_pages": 1,
        "users": [],
        # stats
        "xp_today": 0, "xp_7d": 0, "events_24h": 0,
        "active_users_24h": 0, "active_users_15m": 0,
        "xp_by_day": [], "top_xp_24h": [], "top_events_24h": [],
        "stream_stats": [],
        # cms / lineages / forms / rp
        "lineages": [], "cms": [], "items": [],
        # cp config
        "cp": {},
        "broadcaster_user_id": "",
    }

    with get_db() as conn:
        with conn.cursor() as cur:

            # ── LIVE ──────────────────────────────────────────────────────
            cur.execute("SELECT value FROM kv WHERE key='is_live';")
            row = cur.fetchone()
            ctx["is_live"] = bool(row and str(row[0]).lower() == "true")

            # ── VIEWERS ───────────────────────────────────────────────────
            where = "WHERE u.twitch_login ILIKE %s" if q_clean else ""
            params: list = ([f"%{q_clean}%"] if q_clean else [])

            cur.execute(f"SELECT COUNT(*) FROM users u {where};", params)
            total = int(cur.fetchone()[0] or 0)
            pages = max(1, math.ceil(total / per)) if total else 1
            if page > pages: page = pages
            ctx["total_users"] = total
            ctx["total_pages"] = pages
            ctx["page"] = page

            cur.execute(
                f"""
                SELECT u.twitch_login,
                    COALESCE(SUM(c.xp_total), 0),
                    COALESCE(MAX(c.stage), 0),
                    COUNT(c.id),
                    MAX(c.id) FILTER (WHERE c.is_active=true)
                FROM users u
                LEFT JOIN creatures_v2 c ON c.twitch_login = u.twitch_login
                {where}
                GROUP BY u.twitch_login
                ORDER BY MAX(c.acquired_at) DESC NULLS LAST, u.twitch_login ASC
                LIMIT %s OFFSET %s;
                """,
                params + [per, (page - 1) * per],
            )
            ctx["users"] = [
                {"twitch_login": r[0], "xp_total_sum": int(r[1] or 0),
                 "stage_max": int(r[2] or 0), "cm_count": int(r[3] or 0),
                 "active_id": int(r[4]) if r[4] is not None else None}
                for r in cur.fetchall()
            ]

            # ── STATS GLOBALES ────────────────────────────────────────────
            cur.execute("SELECT date_trunc('day', now() AT TIME ZONE 'Europe/Paris') AT TIME ZONE 'Europe/Paris';")
            today_paris = cur.fetchone()[0]

            cur.execute("SELECT COALESCE(SUM(amount),0) FROM xp_events WHERE created_at >= %s;", (today_paris,))
            ctx["xp_today"] = int(cur.fetchone()[0])

            cur.execute("SELECT COALESCE(SUM(amount),0) FROM xp_events WHERE created_at >= now() - interval '7 days';")
            ctx["xp_7d"] = int(cur.fetchone()[0])

            cur.execute("""
                SELECT COUNT(*), COUNT(DISTINCT twitch_login)
                FROM xp_events WHERE created_at >= now() - interval '24 hours';
            """)
            r = cur.fetchone()
            ctx["events_24h"] = int(r[0]); ctx["active_users_24h"] = int(r[1])

            cur.execute("""
                SELECT COUNT(DISTINCT twitch_login)
                FROM xp_events WHERE created_at >= now() - interval '15 minutes';
            """)
            ctx["active_users_15m"] = int(cur.fetchone()[0])

            cur.execute("""
                SELECT to_char(date_trunc('day', created_at AT TIME ZONE 'Europe/Paris'), 'YYYY-MM-DD'),
                       SUM(amount)
                FROM xp_events
                WHERE created_at >= now() - interval '7 days'
                GROUP BY 1 ORDER BY 1 DESC;
            """)
            xp_by_day = [{"day": r[0], "xp": int(r[1])} for r in cur.fetchall()]
            max_xp = max((r["xp"] for r in xp_by_day), default=0) or 1
            for r in xp_by_day: r["pct"] = int((r["xp"] / max_xp) * 100)
            ctx["xp_by_day"] = xp_by_day

            cur.execute("""
                SELECT twitch_login, SUM(amount) FROM xp_events
                WHERE created_at >= now() - interval '24 hours'
                GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
            """)
            ctx["top_xp_24h"] = [{"twitch_login": r[0], "xp": int(r[1])} for r in cur.fetchall()]

            cur.execute("""
                SELECT twitch_login, COUNT(*) FROM xp_events
                WHERE created_at >= now() - interval '24 hours'
                GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
            """)
            ctx["top_events_24h"] = [{"twitch_login": r[0], "events": int(r[1])} for r in cur.fetchall()]

            # ── STATS PAR STREAM ──────────────────────────────────────────
            cur.execute("""
                SELECT
                    ss.id,
                    ss.started_at,
                    ss.ended_at,
                    EXTRACT(EPOCH FROM COALESCE(ss.ended_at, now()) - ss.started_at)::int AS duration_s,
                    COALESCE(SUM(xe.amount), 0)::int AS xp_total,
                    COUNT(DISTINCT xe.twitch_login) AS viewers_uniques,
                    COUNT(xe.id) AS events_total
                FROM stream_sessions ss
                LEFT JOIN xp_events xe
                    ON xe.created_at BETWEEN ss.started_at AND COALESCE(ss.ended_at, now())
                GROUP BY ss.id
                ORDER BY ss.started_at DESC
                LIMIT 20;
            """)
            stream_rows = cur.fetchall()

            stream_stats = []
            for row in stream_rows:
                sid, started_at, ended_at, duration_s, xp_total, viewers_uniques, events_total = row
                duration_s = int(duration_s or 0)
                h, rem = divmod(duration_s, 3600)
                m, s = divmod(rem, 60)
                duration_str = f"{h}h{m:02d}" if h else f"{m}m{s:02d}s"

                # Top viewers pour ce stream
                cur.execute("""
                    SELECT xe.twitch_login, SUM(xe.amount)::int AS xp
                    FROM xp_events xe
                    WHERE xe.created_at BETWEEN %s AND COALESCE(%s, now())
                    GROUP BY 1 ORDER BY 2 DESC LIMIT 5;
                """, (started_at, ended_at))
                top_viewers = [{"login": r[0], "xp": r[1]} for r in cur.fetchall()]

                # Drops pendant ce stream
                cur.execute("""
                    SELECT COUNT(*) FROM drops
                    WHERE created_at BETWEEN %s AND COALESCE(%s, now());
                """, (started_at, ended_at))
                drops_count = int(cur.fetchone()[0])

                stream_stats.append({
                    "id": sid,
                    "started_at": started_at.strftime("%d/%m/%Y %H:%M") if started_at else "—",
                    "ended_at": ended_at.strftime("%H:%M") if ended_at else "en cours",
                    "is_live": ended_at is None,
                    "duration": duration_str,
                    "xp_total": int(xp_total),
                    "viewers_uniques": int(viewers_uniques),
                    "events_total": int(events_total),
                    "drops_count": drops_count,
                    "top_viewers": top_viewers,
                })

            ctx["stream_stats"] = stream_stats

            # ── CMS + LINEAGES ────────────────────────────────────────────
            cur.execute("""
                SELECT key, name, is_enabled, COALESCE(choose_enabled, true)
                FROM lineages ORDER BY key;
            """)
            ctx["lineages"] = [
                {"key": r[0], "name": r[1], "is_enabled": bool(r[2]), "choose_enabled": bool(r[3])}
                for r in cur.fetchall()
            ]

            cur.execute("""
                SELECT key, name, lineage_key, is_enabled, in_hatch_pool, COALESCE(media_url,'')
                FROM cms ORDER BY lineage_key, key;
            """)
            ctx["cms"] = [
                {"key": r[0], "name": r[1], "lineage_key": r[2],
                 "is_enabled": bool(r[3]), "in_hatch_pool": bool(r[4]), "media_url": r[5]}
                for r in cur.fetchall()
            ]

            # ── FORMS ─────────────────────────────────────────────────────
            cur.execute("SELECT key, name, lineage_key FROM cms ORDER BY lineage_key, key;")
            cms_list = [{"key": r[0], "cm_name": r[1], "lineage_key": r[2]} for r in cur.fetchall()]

            cur.execute("SELECT cm_key, stage, name, image_url, COALESCE(sound_url,'') FROM cm_forms ORDER BY cm_key, stage;")
            forms_map = {}
            for cm_key, stage, name, image_url, sound_url in cur.fetchall():
                forms_map[(cm_key, int(stage))] = {"name": name, "image_url": image_url, "sound_url": sound_url}

            # NOTE: "items" est utilisé par forms ET rp, on les met dans des clés séparées
            ctx["forms_items"] = [
                {**cm, "stages": [
                    {"stage": st, **forms_map.get((cm["key"], st), {"name": "", "image_url": "", "sound_url": ""})}
                    for st in (1, 2, 3)
                ]}
                for cm in cms_list
            ]

            # ── RP ────────────────────────────────────────────────────────
            cur.execute("SELECT key, lines FROM rp_lines ORDER BY key;")
            rp_items = []
            for k, lines in cur.fetchall():
                if isinstance(lines, str):
                    try:    lines = json.loads(lines)
                    except: lines = []
                if not isinstance(lines, list): lines = []
                text = "\n".join([str(x) for x in lines if str(x).strip()])
                rp_items.append({"key": k, "count": len(lines), "text": text})
            ctx["rp_items"] = rp_items

            # ── CHANNEL POINTS ────────────────────────────────────────────
            ctx["cp"] = {
                "cp_enabled":               kv_get(cur, "cp_enabled", "false") or "false",
                "cp_reward_drop_coop_id":   kv_get(cur, "cp_reward_drop_coop_id", "") or "",
                "cp_reward_capsule_id":     kv_get(cur, "cp_reward_capsule_id", "") or "",
                "cp_reward_candy_id":       kv_get(cur, "cp_reward_candy_id", "") or "",
                "cp_reward_egg_id":         kv_get(cur, "cp_reward_egg_id", "") or "",
                "cp_capsule_item_key":      kv_get(cur, "cp_capsule_item_key", "grande_capsule") or "grande_capsule",
                "cp_candy_item_key":        kv_get(cur, "cp_candy_item_key", "bonbon_2") or "bonbon_2",
                "cp_egg_item_key":          kv_get(cur, "cp_egg_item_key", "") or "",
                "cp_drop_pick_kind":        kv_get(cur, "cp_drop_pick_kind", "any") or "any",
                "cp_drop_duration_seconds": kv_get(cur, "cp_drop_duration_seconds", "20") or "20",
                "cp_drop_fallback_icon_url":kv_get(cur, "cp_drop_fallback_icon_url", "") or "",
            }
            ctx["broadcaster_user_id"] = kv_get(cur, "broadcaster_user_id", "") or ""

            # ── AUTODROP ──────────────────────────────────────────────────
            ad_keys = [
                "auto_drop_enabled", "auto_drop_min_seconds", "auto_drop_max_seconds",
                "auto_drop_duration_min_seconds", "auto_drop_duration_max_seconds",
                "auto_drop_pick_kind", "auto_drop_mode", "auto_drop_ticket_qty",
                "auto_drop_fallback_media_url",
            ]
            ctx["autodrop"] = kv_get_many(cur, ad_keys)

    return ctx


# ─────────────────────────────────────────────────────────────────────────────
# ROUTE PRINCIPALE — remplace toutes les routes GET admin qui renvoient le SPA
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/admin", response_class=HTMLResponse)
@app.get("/admin/stats", response_class=HTMLResponse)
@app.get("/admin/cms", response_class=HTMLResponse)
@app.get("/admin/rp", response_class=HTMLResponse)
@app.get("/admin/forms", response_class=HTMLResponse)
@app.get("/admin/drops", response_class=HTMLResponse)
@app.get("/admin/autodrop", response_class=HTMLResponse)
def admin_spa(
    request: Request,
    q: str | None = None,
    page: int = 1,
    per: int = 50,
    flash: str | None = None,
    flash_kind: str | None = None,
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)
    ctx = _build_admin_context(request, flash=flash, flash_kind=flash_kind, q=q, page=page, per=per)
    return templates.TemplateResponse("admin_spa.html", ctx)


# ─────────────────────────────────────────────────────────────────────────────
# /admin/points — garde son propre chargement (formulaire HTML spécifique)
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/admin/points", response_class=HTMLResponse)
def admin_points(request: Request, credentials: HTTPBasicCredentials = Depends(security)):
    require_admin(credentials)
    ctx = _build_admin_context(request)
    return templates.TemplateResponse("admin_spa.html", ctx)


# ─────────────────────────────────────────────────────────────────────────────
# /admin/stats/json — endpoint JSON pour les KPIs dashboard live
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/admin/stats/json")
def admin_stats_json(credentials: HTTPBasicCredentials = Depends(security)):
    require_admin(credentials)

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT date_trunc('day', now() AT TIME ZONE 'Europe/Paris') AT TIME ZONE 'Europe/Paris';")
            today_paris = cur.fetchone()[0]

            cur.execute("SELECT COALESCE(SUM(amount),0) FROM xp_events WHERE created_at >= %s;", (today_paris,))
            xp_today = int(cur.fetchone()[0])

            cur.execute("SELECT COALESCE(SUM(amount),0) FROM xp_events WHERE created_at >= now() - interval '7 days';")
            xp_7d = int(cur.fetchone()[0])

            cur.execute("""
                SELECT COUNT(*), COUNT(DISTINCT twitch_login)
                FROM xp_events WHERE created_at >= now() - interval '24 hours';
            """)
            r = cur.fetchone()
            events_24h, active_users_24h = int(r[0]), int(r[1])

            cur.execute("SELECT COUNT(DISTINCT twitch_login) FROM xp_events WHERE created_at >= now() - interval '15 minutes';")
            active_users_15m = int(cur.fetchone()[0])

            cur.execute("""
                SELECT to_char(date_trunc('day', created_at AT TIME ZONE 'Europe/Paris'), 'YYYY-MM-DD'),
                       SUM(amount)
                FROM xp_events WHERE created_at >= now() - interval '7 days'
                GROUP BY 1 ORDER BY 1 DESC;
            """)
            xp_by_day = [{"day": r[0], "xp": int(r[1])} for r in cur.fetchall()]
            max_xp = max((r["xp"] for r in xp_by_day), default=0) or 1
            for r in xp_by_day: r["pct"] = int((r["xp"] / max_xp) * 100)

    return {
        "xp_today": xp_today,
        "xp_7d": xp_7d,
        "events_24h": events_24h,
        "active_users_24h": active_users_24h,
        "active_users_15m": active_users_15m,
        "xp_by_day": xp_by_day,
    }


# ─────────────────────────────────────────────────────────────────────────────
# /admin/streams/json — stats par stream pour le panneau Streams du SPA
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/admin/streams/json")
def admin_streams_json(credentials: HTTPBasicCredentials = Depends(security)):
    require_admin(credentials)

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    ss.id,
                    ss.started_at,
                    ss.ended_at,
                    EXTRACT(EPOCH FROM COALESCE(ss.ended_at, now()) - ss.started_at)::int AS duration_s,
                    COALESCE(SUM(xe.amount), 0)::int AS xp_total,
                    COUNT(DISTINCT xe.twitch_login)::int AS viewers_uniques,
                    COUNT(xe.id)::int AS events_total
                FROM stream_sessions ss
                LEFT JOIN xp_events xe
                    ON xe.created_at BETWEEN ss.started_at AND COALESCE(ss.ended_at, now())
                GROUP BY ss.id
                ORDER BY ss.started_at DESC
                LIMIT 20;
            """)
            rows = cur.fetchall()

            result = []
            for row in rows:
                sid, started_at, ended_at, duration_s, xp_total, viewers_uniques, events_total = row
                duration_s = int(duration_s or 0)
                h, rem = divmod(duration_s, 3600)
                m, s = divmod(rem, 60)

                # top viewers
                cur.execute("""
                    SELECT twitch_login, SUM(amount)::int
                    FROM xp_events
                    WHERE created_at BETWEEN %s AND COALESCE(%s, now())
                    GROUP BY 1 ORDER BY 2 DESC LIMIT 5;
                """, (started_at, ended_at))
                top_viewers = [{"login": r[0], "xp": r[1]} for r in cur.fetchall()]

                # drops
                cur.execute("""
                    SELECT COUNT(*) FROM drops
                    WHERE created_at BETWEEN %s AND COALESCE(%s, now());
                """, (started_at, ended_at))
                drops_count = int(cur.fetchone()[0])

                # xp par source (ventilation)
                xp_by_source = []

                result.append({
                    "id": sid,
                    "started_at": started_at.isoformat() if started_at else None,
                    "ended_at": ended_at.isoformat() if ended_at else None,
                    "started_label": started_at.strftime("%d/%m %H:%M") if started_at else "—",
                    "ended_label": ended_at.strftime("%H:%M") if ended_at else "en cours 🔴",
                    "is_live": ended_at is None,
                    "duration": f"{h}h{m:02d}" if h else f"{m}m{s:02d}s",
                    "xp_total": xp_total,
                    "viewers_uniques": viewers_uniques,
                    "events_total": events_total,
                    "drops_count": drops_count,
                    "top_viewers": top_viewers,
                    "xp_by_source": xp_by_source,
                })

    return result


# =============================================================================
# USER PAGES — /u/{login}
# =============================================================================

def _stage_roman(n):
    return {0: "Œuf", 1: "I", 2: "II", 3: "III"}.get(n, str(n))

def _render_user_page(login: str, d: dict) -> str:
    active   = d["active_cm"]
    quests   = d["quests"]
    badges   = d["badges"]
    sections = d["album_sections"]
    stats    = d["stats"]

    if active and active.get("image_url"):
        cm_card = f"""
        <div class="cm-active-card">
          <div class="cm-active-img-wrap">
            <img src="{active['image_url']}" alt="{active['name']}" class="cm-active-img">
            <div class="cm-active-glow"></div>
          </div>
          <div class="cm-active-info">
            <div class="cm-active-name">{active['name']}</div>
            <div class="cm-active-meta">
              <span class="badge-pill c">{(active['lineage_key'] or '').upper()}</span>
              <span class="badge-pill g">STAGE {_stage_roman(active['stage'])}</span>
            </div>
            <div class="stat-row" style="margin-top:10px">
              <div class="stat-lbl">XP</div>
              <div class="stat-track"><div class="stat-fill xp" style="width:{min(100, int((active['xp_total'] % 500)/5))}%"></div></div>
              <div class="stat-val">{active['xp_total']}</div>
            </div>
            <div class="stat-row">
              <div class="stat-lbl">BONHEUR</div>
              <div class="stat-track"><div class="stat-fill hp" style="width:{active['happiness']}%"></div></div>
              <div class="stat-val">{active['happiness']}%</div>
            </div>
          </div>
        </div>"""
    else:
        cm_card = '<div class="cm-active-card empty"><div class="empty-msg">Pas encore de CM actif</div></div>'

    def quest_icon(t):
        return {"presence":"⏱","drops":"▽","candy":"🍬","xp":"⚡","top10":"🏆","show":"✨","coop":"🤝"}.get(t,"◈")

    quest_cards = ""
    for q in quests:
        pct = int((q["progress"] / max(1, q["target"])) * 100)
        done = "done" if q["completed"] else ""
        rewards = []
        if q["reward_xp"]:       rewards.append(f"<span class='r-xp'>+{q['reward_xp']} XP</span>")
        if q["reward_item_key"]: rewards.append(f"<span class='r-item'>{q['reward_item_qty']}× {q['reward_item_key']}</span>")
        if q["reward_badge"]:    rewards.append(f"<span class='r-badge'>🏅 badge</span>")
        quest_cards += f"""
        <div class="quest-card {done}">
          <div class="quest-header">
            <div class="quest-icon">{quest_icon(q['type'])}</div>
            <div class="quest-body">
              <div class="quest-label">{q['label']}</div>
              <div class="quest-desc">{q['description']}</div>
            </div>
            {'<div class="quest-check">✓</div>' if q["completed"] else ''}
          </div>
          <div class="quest-progress-row">
            <div class="quest-track"><div class="quest-fill" style="width:{pct}%"></div></div>
            <div class="quest-count">{q['progress']}/{q['target']}</div>
          </div>
          <div class="quest-rewards">{"".join(rewards)}</div>
        </div>"""

    badge_labels = {
        "badge_present":  ("⏱","Présent"),  "badge_grinder": ("⚡","Grinder"),
        "badge_elite":    ("🏆","Élite"),    "badge_coop":    ("🤝","Teamplayer"),
        "badge_loyal":    ("💙","Fidèle"),   "badge_gourmand":("🍬","Gourmand"),
    }
    if badges:
        badges_html = "".join(
            f'<div class="badge-item"><div class="badge-icon">{badge_labels.get(b["key"],("🏅",b["key"]))[0]}</div>'
            f'<div class="badge-name">{badge_labels.get(b["key"],("🏅",b["key"]))[1]}</div></div>'
            for b in badges)
    else:
        badges_html = '<div class="muted-sm">Aucun badge pour l\'instant</div>'

    album_html = ""
    for sec in sections:
        cms_grid = ""
        for c in sec["cms"]:
            owned_cls  = "owned" if c["owned"] else "locked"
            active_cls = "active" if c["is_active"] else ""
            best = next((f for f in reversed(c["forms"]) if f["image_url"] and c["owned"]), None)
            img_tag = f'<img src="{best["image_url"]}" alt="{c["name"]}">' if best else '<div class="silhouette">?</div>'
            dots = "".join(f'<div class="stage-dot {"has" if c["max_stage"] >= st and c["owned"] else ""}"></div>' for st in [1,2,3])
            cms_grid += f"""
            <div class="album-card {owned_cls} {active_cls}" title="{c['name']}">
              <div class="album-img-wrap">{img_tag}</div>
              <div class="album-name">{"???" if not c["owned"] else c["name"]}</div>
              <div class="album-stages">{dots}</div>
            </div>"""
        album_html += f"""
        <div class="album-section">
          <div class="section-header">
            <div class="section-title">{sec['lineage_name'].upper()}</div>
            <div class="section-count">{sec['owned_count']}/{len(sec['cms'])}</div>
          </div>
          <div class="album-grid">{cms_grid}</div>
        </div>"""

    rank_str = f"#{stats['xp_rank']}" if stats['xp_rank'] else "—"
    week_str = _current_week_start().strftime("%d/%m/%Y")
    completed_count = sum(1 for q in quests if q["completed"])
    album_pct = int((d["owned_total"] / max(1, d["total_cms"])) * 100)

    return f"""<!doctype html>
<html lang="fr"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>@{login} — CapsMons</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;600;700&family=Orbitron:wght@700;900&family=Share+Tech+Mono&display=swap" rel="stylesheet">
<style>
:root{{--bg:#060b12;--panel:#0a1220;--border:rgba(0,229,255,.12);--cyan:#00e5ff;--magenta:#ff2d78;--green:#00ff9d;--amber:#ffd166;--text:#d8eaf8;--muted:#4a6a88;--font-head:'Orbitron',monospace;--font-ui:'Rajdhani',sans-serif;--font-mono:'Share Tech Mono',monospace;--radius:14px}}
*{{box-sizing:border-box;margin:0;padding:0}}
html,body{{background:var(--bg);color:var(--text);font-family:var(--font-ui);min-height:100vh}}
a{{color:var(--cyan);text-decoration:none}}
.wrap{{max-width:1100px;margin:0 auto;padding:24px 16px 60px}}
.top-bar{{display:flex;align-items:center;justify-content:space-between;margin-bottom:28px;flex-wrap:wrap;gap:12px}}
.site-brand{{font-family:var(--font-head);font-size:11px;color:var(--muted);letter-spacing:.15em}}
.page-title{{font-family:var(--font-head);font-size:20px;color:var(--cyan);text-shadow:0 0 20px rgba(0,229,255,.4)}}
.grid-2{{display:grid;grid-template-columns:340px 1fr;gap:16px;margin-bottom:16px}}
@media(max-width:760px){{.grid-2{{grid-template-columns:1fr}}}}
.card{{background:var(--panel);border:1px solid var(--border);border-radius:var(--radius);padding:18px}}
.card-title{{font-family:var(--font-head);font-size:10px;letter-spacing:.14em;color:var(--muted);margin-bottom:14px}}
.cm-active-card{{display:flex;gap:16px;align-items:flex-start}}
.cm-active-card.empty{{justify-content:center;align-items:center;min-height:120px}}
.empty-msg{{color:var(--muted);font-family:var(--font-mono);font-size:13px}}
.cm-active-img-wrap{{position:relative;flex-shrink:0}}
.cm-active-img{{width:96px;height:96px;border-radius:12px;object-fit:contain;background:rgba(0,229,255,.04);border:1px solid var(--border);display:block}}
.cm-active-glow{{position:absolute;inset:-6px;border-radius:18px;background:radial-gradient(circle,rgba(0,229,255,.18) 0%,transparent 70%);animation:glow 2s ease-in-out infinite;pointer-events:none}}
@keyframes glow{{0%,100%{{opacity:.5}}50%{{opacity:1}}}}
.cm-active-info{{flex:1}}
.cm-active-name{{font-family:var(--font-head);font-size:15px;color:var(--text);margin-bottom:8px}}
.cm-active-meta{{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:2px}}
.badge-pill{{font-family:var(--font-mono);font-size:9px;padding:2px 8px;border-radius:999px;border:1px solid}}
.badge-pill.c{{color:var(--cyan);border-color:rgba(0,229,255,.3);background:rgba(0,229,255,.06)}}
.badge-pill.g{{color:var(--green);border-color:rgba(0,255,157,.3);background:rgba(0,255,157,.06)}}
.stat-row{{display:flex;align-items:center;gap:8px;margin-bottom:6px}}
.stat-lbl{{font-family:var(--font-mono);font-size:9px;color:var(--muted);width:60px;flex-shrink:0}}
.stat-track{{flex:1;height:5px;background:rgba(255,255,255,.06);border-radius:999px;overflow:hidden}}
.stat-fill{{height:100%;border-radius:999px;transition:width .5s ease}}
.stat-fill.xp{{background:linear-gradient(90deg,#7aa2ff,var(--cyan))}}
.stat-fill.hp{{background:linear-gradient(90deg,#ff4fb3,var(--magenta))}}
.stat-val{{font-family:var(--font-mono);font-size:10px;color:var(--muted);width:50px;text-align:right}}
.kpi-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:16px}}
.kpi-card{{background:var(--panel);border:1px solid var(--border);border-radius:var(--radius);padding:14px;text-align:center}}
.kpi-val{{font-family:var(--font-head);font-size:22px;font-weight:900;margin-bottom:4px}}
.kpi-val.c{{color:var(--cyan);text-shadow:0 0 15px rgba(0,229,255,.4)}}
.kpi-val.g{{color:var(--green);text-shadow:0 0 15px rgba(0,255,157,.3)}}
.kpi-val.m{{color:var(--magenta);text-shadow:0 0 15px rgba(255,45,120,.3)}}
.kpi-lbl{{font-family:var(--font-mono);font-size:9px;color:var(--muted);letter-spacing:.1em}}
.quests-grid{{display:flex;flex-direction:column;gap:10px}}
.quest-card{{background:rgba(0,229,255,.03);border:1px solid rgba(0,229,255,.08);border-radius:12px;padding:12px}}
.quest-card.done{{border-color:rgba(0,255,157,.25);background:rgba(0,255,157,.03)}}
.quest-header{{display:flex;gap:10px;align-items:flex-start;margin-bottom:8px}}
.quest-icon{{font-size:20px;width:28px;text-align:center;flex-shrink:0}}
.quest-body{{flex:1}}
.quest-label{{font-size:14px;font-weight:700;color:var(--text)}}
.quest-desc{{font-family:var(--font-mono);font-size:10px;color:var(--muted);margin-top:2px}}
.quest-check{{font-size:18px;color:var(--green);text-shadow:0 0 10px rgba(0,255,157,.6)}}
.quest-progress-row{{display:flex;align-items:center;gap:8px;margin-bottom:6px}}
.quest-track{{flex:1;height:4px;background:rgba(255,255,255,.06);border-radius:999px;overflow:hidden}}
.quest-fill{{height:100%;background:linear-gradient(90deg,var(--cyan),#7aa2ff);border-radius:999px;transition:width .5s}}
.quest-card.done .quest-fill{{background:linear-gradient(90deg,var(--green),#00e5ff)}}
.quest-count{{font-family:var(--font-mono);font-size:10px;color:var(--muted);flex-shrink:0}}
.quest-rewards{{display:flex;gap:6px;flex-wrap:wrap}}
.r-xp{{font-family:var(--font-mono);font-size:10px;color:var(--cyan);padding:2px 7px;border:1px solid rgba(0,229,255,.3);border-radius:999px}}
.r-item{{font-family:var(--font-mono);font-size:10px;color:var(--amber);padding:2px 7px;border:1px solid rgba(255,209,102,.3);border-radius:999px}}
.r-badge{{font-family:var(--font-mono);font-size:10px;color:var(--magenta);padding:2px 7px;border:1px solid rgba(255,45,120,.3);border-radius:999px}}
.badges-row{{display:flex;gap:10px;flex-wrap:wrap}}
.badge-item{{display:flex;flex-direction:column;align-items:center;gap:4px;padding:10px 12px;border:1px solid rgba(255,209,102,.2);border-radius:10px;background:rgba(255,209,102,.04);min-width:60px}}
.badge-icon{{font-size:22px}}
.badge-name{{font-family:var(--font-mono);font-size:9px;color:var(--amber);letter-spacing:.08em}}
.muted-sm{{font-family:var(--font-mono);font-size:11px;color:var(--muted)}}
.album-section{{margin-bottom:24px}}
.section-header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid var(--border)}}
.section-title{{font-family:var(--font-head);font-size:11px;color:var(--cyan);letter-spacing:.14em}}
.section-count{{font-family:var(--font-mono);font-size:11px;color:var(--muted)}}
.album-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(90px,1fr));gap:8px}}
.album-card{{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:8px;text-align:center;transition:border-color .2s,transform .15s;position:relative}}
.album-card.owned{{border-color:rgba(0,229,255,.2)}}
.album-card.owned.active{{border-color:rgba(0,255,157,.4);box-shadow:0 0 12px rgba(0,255,157,.15)}}
.album-card.locked{{opacity:.4;filter:grayscale(.5)}}
.album-card:hover{{transform:translateY(-2px)}}
.album-img-wrap{{width:60px;height:60px;margin:0 auto 6px;border-radius:8px;overflow:hidden;background:rgba(255,255,255,.04);display:flex;align-items:center;justify-content:center}}
.album-img-wrap img{{width:100%;height:100%;object-fit:contain}}
.silhouette{{font-size:24px;color:var(--muted)}}
.album-name{{font-family:var(--font-ui);font-size:10px;color:var(--text);margin-bottom:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.album-stages{{display:flex;justify-content:center;gap:3px}}
.stage-dot{{width:5px;height:5px;border-radius:50%;background:rgba(255,255,255,.1);border:1px solid rgba(255,255,255,.15)}}
.stage-dot.has{{background:var(--cyan);border-color:var(--cyan);box-shadow:0 0 4px rgba(0,229,255,.6)}}
.album-progress-bar{{height:6px;background:rgba(255,255,255,.06);border-radius:999px;overflow:hidden;margin:6px 0 4px}}
.album-progress-fill{{height:100%;background:linear-gradient(90deg,var(--cyan),var(--green));border-radius:999px;transition:width .8s ease}}
.album-stats-row{{display:flex;justify-content:space-between;font-family:var(--font-mono);font-size:11px;color:var(--muted);margin-bottom:20px}}
.week-badge{{font-family:var(--font-mono);font-size:10px;color:var(--muted);padding:4px 10px;border:1px solid var(--border);border-radius:999px}}
body::before{{content:'';position:fixed;inset:0;pointer-events:none;background:radial-gradient(ellipse 60% 40% at 50% 0%,rgba(0,229,255,.04) 0%,transparent 70%)}}
</style></head>
<body>
<div class="wrap">
  <div class="top-bar">
    <div><div class="site-brand">CAPSMONS</div><div class="page-title">@{login}</div></div>
    <div class="week-badge">Semaine du {week_str}</div>
  </div>
  <div class="kpi-grid">
    <div class="kpi-card"><div class="kpi-val c">{stats['xp_total']:,}</div><div class="kpi-lbl">XP TOTAL</div></div>
    <div class="kpi-card"><div class="kpi-val g">{stats['drops_total']}</div><div class="kpi-lbl">DROPS</div></div>
    <div class="kpi-card"><div class="kpi-val m">{rank_str}</div><div class="kpi-lbl">CLASSEMENT</div></div>
  </div>
  <div class="grid-2">
    <div style="display:flex;flex-direction:column;gap:16px">
      <div class="card"><div class="card-title">// MON CAPSMÖN ACTIF</div>{cm_card}</div>
      <div class="card"><div class="card-title">// BADGES</div><div class="badges-row">{badges_html}</div></div>
    </div>
    <div class="card">
      <div class="card-title" style="display:flex;align-items:center;justify-content:space-between">
        <span>// QUÊTES DE LA SEMAINE</span>
        <span style="color:var(--green);font-size:11px">{completed_count}/{len(quests)} complétées</span>
      </div>
      <div class="quests-grid">{quest_cards or '<div class="muted-sm">Aucune quête assignée</div>'}</div>
    </div>
  </div>
  <div class="card">
    <div class="card-title">// ALBUM CAPSMONS</div>
    <div class="album-progress-bar"><div class="album-progress-fill" style="width:{album_pct}%"></div></div>
    <div class="album-stats-row"><span>{d['owned_total']} possédés</span><span>{d['total_cms']} total</span></div>
    {album_html}
  </div>
</div>
<script>setInterval(()=>location.reload(),60000);</script>
</body></html>"""


@app.get("/u/{login}", response_class=HTMLResponse)
def user_profile_page(login: str):
    login = login.strip().lower()
    week  = _current_week_start()

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM users WHERE twitch_login=%s;", (login,))
            if not cur.fetchone():
                return HTMLResponse(f"<html><body style='background:#060b12;color:#d8eaf8;font-family:monospace;display:flex;align-items:center;justify-content:center;height:100vh'><div>Viewer introuvable : @{login}</div></body></html>", status_code=404)
            _ensure_quests(cur, login)
            _quest_reward(cur, login)
        conn.commit()

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.cm_key, c.lineage_key, c.stage, c.xp_total, c.happiness,
                       COALESCE(f.name, c.cm_key), COALESCE(f.image_url,'')
                FROM creatures_v2 c
                LEFT JOIN cm_forms f ON f.cm_key=c.cm_key AND f.stage=c.stage
                WHERE c.twitch_login=%s AND c.is_active=TRUE LIMIT 1;
            """, (login,))
            row = cur.fetchone()
            active_cm = {"cm_key":row[0],"lineage_key":row[1],"stage":int(row[2] or 0),"xp_total":int(row[3] or 0),"happiness":int(row[4] or 0),"name":row[5],"image_url":row[6]} if row else None

            cur.execute("SELECT cm_key FROM creatures_v2 WHERE twitch_login=%s;", (login,))
            owned_cms = {r[0] for r in cur.fetchall()}

            cur.execute("""
                SELECT c.cm_key, c.lineage_key, c.stage, c.xp_total, c.is_active,
                       COALESCE(f.name, c.cm_key), COALESCE(f.image_url,'')
                FROM creatures_v2 c
                LEFT JOIN cm_forms f ON f.cm_key=c.cm_key AND f.stage=c.stage
                WHERE c.twitch_login=%s ORDER BY c.is_active DESC, c.xp_total DESC;
            """, (login,))
            owned_list = [{"cm_key":r[0],"lineage_key":r[1],"stage":int(r[2] or 0),"xp_total":int(r[3] or 0),"is_active":bool(r[4]),"name":r[5],"image_url":r[6]} for r in cur.fetchall()]

            cur.execute("""
                SELECT c.key, c.name, c.lineage_key, l.name,
                       COALESCE(c.media_url,''),
                       f1.name, f1.image_url, f2.name, f2.image_url, f3.name, f3.image_url
                FROM cms c JOIN lineages l ON l.key=c.lineage_key
                LEFT JOIN cm_forms f1 ON f1.cm_key=c.key AND f1.stage=1
                LEFT JOIN cm_forms f2 ON f2.cm_key=c.key AND f2.stage=2
                LEFT JOIN cm_forms f3 ON f3.cm_key=c.key AND f3.stage=3
                WHERE c.is_enabled=TRUE ORDER BY c.lineage_key, c.key;
            """)
            all_cms = cur.fetchall()

            cur.execute("""
                SELECT qa.quest_key, qc.label, qc.description, qc.type, qc.target,
                       qa.progress, qa.completed, qa.rewarded,
                       qc.reward_xp, qc.reward_item_key, qc.reward_item_qty, qc.reward_badge
                FROM quest_assignments qa
                JOIN quest_catalog qc ON qc.key=qa.quest_key
                WHERE qa.twitch_login=%s AND qa.week_start=%s
                ORDER BY qc.is_fixed DESC, qa.completed ASC;
            """, (login, week))
            quests = [{"key":r[0],"label":r[1],"description":r[2],"type":r[3],"target":int(r[4]),"progress":int(r[5]),"completed":bool(r[6]),"rewarded":bool(r[7]),"reward_xp":int(r[8] or 0),"reward_item_key":r[9],"reward_item_qty":int(r[10] or 0),"reward_badge":r[11]} for r in cur.fetchall()]

            cur.execute("SELECT badge_key, earned_at FROM user_badges WHERE twitch_login=%s ORDER BY earned_at DESC;", (login,))
            badges = [{"key":r[0],"earned_at":r[1].strftime("%d/%m/%Y")} for r in cur.fetchall()]

            cur.execute("SELECT COALESCE(SUM(amount),0) FROM xp_events WHERE twitch_login=%s;", (login,))
            xp_total_all = int(cur.fetchone()[0])

            cur.execute("SELECT COUNT(DISTINCT drop_id) FROM drop_participants WHERE twitch_login=%s;", (login,))
            drops_total = int(cur.fetchone()[0])

            cur.execute("""
                SELECT rank FROM (SELECT twitch_login, RANK() OVER (ORDER BY xp_total DESC) as rank
                FROM creatures_v2 WHERE is_active=TRUE) r WHERE twitch_login=%s;
            """, (login,))
            rank_row = cur.fetchone()
            xp_rank = int(rank_row[0]) if rank_row else None

    from collections import defaultdict
    album_by_lineage = defaultdict(list)
    for r in all_cms:
        cm_key = r[0]
        entry = {
            "key": cm_key, "name": r[1], "lineage_key": r[2], "lineage_name": r[3], "media_url": r[4],
            "forms": [{"stage":1,"name":r[5] or "","image_url":r[6] or ""},{"stage":2,"name":r[7] or "","image_url":r[8] or ""},{"stage":3,"name":r[9] or "","image_url":r[10] or ""}],
            "owned": cm_key in owned_cms,
            "is_active": any(o["cm_key"]==cm_key and o["is_active"] for o in owned_list),
            "max_stage": max((o["stage"] for o in owned_list if o["cm_key"]==cm_key), default=0),
        }
        album_by_lineage[r[3]].append(entry)

    album_sections = [{"lineage_name":ln,"cms":cms_list,"owned_count":sum(1 for c in cms_list if c["owned"])} for ln,cms_list in sorted(album_by_lineage.items())]
    total_cms   = sum(len(s["cms"]) for s in album_sections)
    owned_total = sum(s["owned_count"] for s in album_sections)

    page_data = {
        "login": login, "active_cm": active_cm, "quests": quests, "badges": badges,
        "album_sections": album_sections, "total_cms": total_cms, "owned_total": owned_total,
        "stats": {"xp_total": xp_total_all, "drops_total": drops_total, "xp_rank": xp_rank},
    }
    return HTMLResponse(_render_user_page(login, page_data))


@app.get("/u/{login}/quests.json")
def user_quests_json(login: str):
    login = login.strip().lower()
    week  = _current_week_start()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM users WHERE twitch_login=%s;", (login,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="User not found")
            _ensure_quests(cur, login)
            rewards = _quest_reward(cur, login)
        conn.commit()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT qa.quest_key, qc.label, qc.type, qc.target, qa.progress,
                       qa.completed, qa.rewarded, qc.reward_xp, qc.reward_item_key, qc.reward_item_qty
                FROM quest_assignments qa JOIN quest_catalog qc ON qc.key=qa.quest_key
                WHERE qa.twitch_login=%s AND qa.week_start=%s ORDER BY qc.is_fixed DESC, qa.completed ASC;
            """, (login, week))
            quests = [{"key":r[0],"label":r[1],"type":r[2],"target":r[3],"progress":r[4],"completed":r[5],"rewarded":r[6],"reward_xp":r[7],"reward_item_key":r[8],"reward_item_qty":r[9]} for r in cur.fetchall()]
    return {"quests": quests, "rewards_given": rewards, "week_start": str(week)}
