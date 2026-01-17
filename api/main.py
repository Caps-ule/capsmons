import os
import json
import secrets
import hmac
import hashlib
import random
import time
import requests
import psycopg
from fastapi import FastAPI, Header, HTTPException, Request, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from starlette.templating import Jinja2Templates


app = FastAPI()
security = HTTPBasic()
templates = Jinja2Templates(directory="templates")
_twitch_token_cache = {"token": None, "exp": 0.0}


# -------------------------
# DB helpers
# -------------------------
def get_db():
    return psycopg.connect(
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host="db",
        port=5432,
    )


# -------------------------
# Auth helpers
# -------------------------
def require_internal_key(x_api_key: str | None):
    if x_api_key != os.environ.get("INTERNAL_API_KEY"):
        raise HTTPException(status_code=401, detail="Unauthorized")


def require_admin(creds: HTTPBasicCredentials):
    admin_user = os.environ.get("ADMIN_USER", "")
    admin_pass = os.environ.get("ADMIN_PASSWORD", "")
    ok_user = secrets.compare_digest(creds.username, admin_user)
    ok_pass = secrets.compare_digest(creds.password, admin_pass)
    if not (ok_user and ok_pass):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )

# -------------------------
# Auth app_twitch_show
# -------------------------

def stage_bounds(stage: int):
    hatch, evo1, evo2 = thresholds()
    if stage <= 0:
        return 0, hatch
    if stage == 1:
        return hatch, evo1
    if stage == 2:
        return evo1, evo2
    return evo2, None  # max

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
    """Returns (display_name, avatar_url)."""
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


# -------------------------
# XP / stages
# -------------------------
def thresholds():
    hatch = int(os.environ["XP_HATCH"])
    evo1 = int(os.environ["XP_EVOLVE_1"])
    evo2 = int(os.environ["XP_EVOLVE_2"])
    if not (0 < hatch < evo1 < evo2):
        raise RuntimeError("Invalid thresholds: expected 0 < XP_HATCH < XP_EVOLVE_1 < XP_EVOLVE_2")
    return hatch, evo1, evo2


def stage_from_xp(xp_total: int) -> int:
    hatch, evo1, evo2 = thresholds()
    if xp_total < hatch:
        return 0  # egg
    if xp_total < evo1:
        return 1  # hatchling
    if xp_total < evo2:
        return 2  # evolution 1
    return 3      # evolution 2


def next_threshold(xp_total: int):
    hatch, evo1, evo2 = thresholds()
    if xp_total < hatch:
        return hatch, "Éclosion"
    if xp_total < evo1:
        return evo1, "Évolution 1"
    if xp_total < evo2:
        return evo2, "Évolution 2"
    return None, "Max"


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


# -------------------------
# EventSub signature verify
# -------------------------
def verify_eventsub_signature(headers: dict, raw_body: bytes) -> bool:
    """
    Verifies Twitch EventSub signature.
    Twitch sends:
      - Twitch-Eventsub-Message-Id
      - Twitch-Eventsub-Message-Timestamp
      - Twitch-Eventsub-Message-Signature (sha256=...)
    Signature base string: message_id + message_timestamp + raw_body
    """
    secret = os.environ.get("EVENTSUB_SECRET", "")
    if not secret:
        # If you don't use EventSub yet, keep endpoint but refuse signature checks.
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


# -------------------------
# DB init
# -------------------------
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
                  in_hatch_pool BOOLEAN NOT NULL DEFAULT FALSE
                );

                CREATE INDEX IF NOT EXISTS idx_cms_lineage_pool
                  ON cms(lineage_key, in_hatch_pool, is_enabled
                );

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

                """
            )

            # default is_live
            cur.execute(
                """
                INSERT INTO kv (key, value) VALUES ('is_live', 'false')
                ON CONFLICT (key) DO NOTHING;
                """
            )

            # seed lineages (Limited disabled by default)
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
            cur.execute(
                """
                INSERT INTO rp_lines (key, lines) VALUES
                  ('creature.stage0', '["🥚 L’œuf vibre faiblement…", "🥚 Une chaleur étrange émane de l’œuf…"]'),
                  ('creature.stage1', '["🐣 *Crac !* Une nouvelle vie apparaît.", "🐣 Le CapsMons vient de naître."]'),
                  ('evolve.announce', '["✨ Transformation !", "⚡ Évolution en cours !"]'),
                  ('cm.assigned', '["👾 Un CM a été attribué !", "🧬 Signature génétique détectée…"]')
                ON CONFLICT (key) DO NOTHING;
                """
            )

        conn.commit()


# -------------------------
# Basic endpoints
# -------------------------
@app.get("/health")
def health():
    return {"ok": True}


# -------------------------
# Internal: live state
# -------------------------
@app.get("/internal/is_live")
def internal_is_live(x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM kv WHERE key='is_live';")
            row = cur.fetchone()
    return {"is_live": (row and row[0] == "true")}


# -------------------------
# Internal: choose lineage (ONLY egg stage)
# -------------------------
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

            # ensure creature exists
            cur.execute(
                """
                INSERT INTO creatures (twitch_login, xp_total, stage)
                VALUES (%s, 0, 0)
                ON CONFLICT (twitch_login) DO NOTHING;
                """,
                (login,),
            )

            cur.execute("SELECT stage FROM creatures WHERE twitch_login=%s;", (login,))
            stage = int(cur.fetchone()[0])

            if stage != 0:
                raise HTTPException(status_code=400, detail="Choose only before hatching (egg stage)")

            cur.execute(
                """
                UPDATE creatures
                SET lineage_key = %s, updated_at = now()
                WHERE twitch_login = %s;
                """,
                (lineage_key, login),
            )

        conn.commit()

    return {"ok": True, "twitch_login": login, "lineage_key": lineage_key}


# -------------------------
# Internal: add XP (+ stage change + assign CM on hatch)
# -------------------------
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
            # ensure user
            cur.execute(
                "INSERT INTO users (twitch_login) VALUES (%s) ON CONFLICT DO NOTHING;",
                (login,),
            )

            # ensure creature exists
            cur.execute(
                """
                INSERT INTO creatures (twitch_login, xp_total, stage)
                VALUES (%s, 0, 0)
                ON CONFLICT (twitch_login) DO NOTHING;
                """,
                (login,),
            )

            # log XP event
            cur.execute(
                "INSERT INTO xp_events (twitch_login, amount) VALUES (%s, %s);",
                (login, amount),
            )

            # read prev stage
            cur.execute("SELECT stage FROM creatures WHERE twitch_login=%s;", (login,))
            prev_row = cur.fetchone()
            prev_stage = int(prev_row[0]) if prev_row else 0

            # increment XP
            cur.execute(
                """
                UPDATE creatures
                SET xp_total = xp_total + %s,
                    updated_at = now()
                WHERE twitch_login = %s
                RETURNING xp_total;
                """,
                (amount, login),
            )
            new_xp_total = int(cur.fetchone()[0])
            new_stage = stage_from_xp(new_xp_total)

            # update stage
            cur.execute(
                """
                UPDATE creatures
                SET stage = %s,
                    updated_at = now()
                WHERE twitch_login = %s;
                """,
                (new_stage, login),
            )

            # assign CM at the exact hatch moment (0 -> 1+)
            if prev_stage == 0 and new_stage >= 1:
                cur.execute("SELECT lineage_key, cm_key FROM creatures WHERE twitch_login=%s;", (login,))
                lrow = cur.fetchone()
                lineage_key = lrow[0] if lrow else None
                current_cm = lrow[1] if lrow else None

                if lineage_key and current_cm is None:
                    cm_key = pick_cm_for_lineage(conn, lineage_key)
                    if cm_key:
                        cur.execute(
                            "UPDATE creatures SET cm_key=%s, updated_at=now() WHERE twitch_login=%s;",
                            (cm_key, login),
                        )
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


# -------------------------
# Internal: creature state (includes lineage/cm)
# -------------------------
@app.get("/internal/creature/{login}")
def creature_state(login: str, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    login = login.strip().lower()
    if not login:
        raise HTTPException(status_code=400, detail="Missing login")

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT twitch_login, xp_total, stage, lineage_key, cm_key
                FROM creatures
                WHERE twitch_login = %s;
                """,
                (login,),
            )
            row = cur.fetchone()

    if not row:
        xp_total, stage, lineage_key, cm_key = 0, 0, None, None
    else:
        _, xp_total, stage, lineage_key, cm_key = row

    nxt, label = next_threshold(int(xp_total))
    remaining = 0 if nxt is None else max(0, int(nxt) - int(xp_total))

    return {
        "twitch_login": login,
        "xp_total": int(xp_total),
        "stage": int(stage),
        "lineage_key": lineage_key,
        "cm_key": cm_key,
        "next": label,
        "xp_to_next": remaining,
    }


# -------------------------
# EventSub webhook (optional)
# -------------------------
@app.post("/eventsub", response_class=PlainTextResponse)
async def eventsub_handler(request: Request):
    body = await request.body()
    headers = {k.lower(): v for k, v in request.headers.items()}

    if not verify_eventsub_signature(headers, body):
        raise HTTPException(status_code=403, detail="Invalid signature")

    msg_type = headers.get("twitch-eventsub-message-type", "")
    payload = json.loads(body.decode("utf-8"))

    # Challenge
    if msg_type == "webhook_callback_verification":
        return payload.get("challenge", "")

    # Notification
    if msg_type == "notification":
        sub_type = payload.get("subscription", {}).get("type", "")
        if sub_type in ("stream.online", "stream.offline"):
            is_live = "true" if sub_type == "stream.online" else "false"
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO kv (key, value)
                        VALUES ('is_live', %s)
                        ON CONFLICT (key) DO UPDATE
                        SET value = EXCLUDED.value, updated_at = now();
                        """,
                        (is_live,),
                    )
                conn.commit()
        return "ok"

    # Revocation
    if msg_type == "revocation":
        return "ok"

    return "ok"


# -------------------------
# Admin pages
# -------------------------
@app.get("/admin", response_class=HTMLResponse)
def admin_home(
    request: Request,
    q: str | None = None,
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)

    q_clean = (q or "").strip().lower()
    result = None

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT twitch_login, xp_total, stage
                FROM creatures
                ORDER BY xp_total DESC
                LIMIT 50;
                """
            )
            top = [{"twitch_login": r[0], "xp_total": r[1], "stage": r[2]} for r in cur.fetchall()]

            if q_clean:
                cur.execute(
                    """
                    SELECT twitch_login, xp_total, stage
                    FROM creatures
                    WHERE twitch_login = %s;
                    """,
                    (q_clean,),
                )
                row = cur.fetchone()
                if row:
                    result = {"twitch_login": row[0], "xp_total": row[1], "stage": row[2]}

    return templates.TemplateResponse(
        "admin.html",
        {"request": request, "top": top, "q": q_clean, "result": result},
    )


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
            cur.execute(
                "SELECT xp_total, stage, lineage_key, cm_key FROM creatures WHERE twitch_login = %s;",
                (login,),
            )
            row = cur.fetchone()

    if not row:
        xp_total, stage, lineage_key, cm_key = 0, 0, None, None
    else:
        xp_total, stage, lineage_key, cm_key = row

    nxt, label = next_threshold(int(xp_total))
    xp_to_next = 0 if nxt is None else max(0, int(nxt) - int(xp_total))

    return templates.TemplateResponse(
        "user.html",
        {
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
        },
    )


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

    # assign_cm: no correlated SQL
    if action == "assign_cm":
        assigned = None
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT lineage_key, cm_key, stage FROM creatures WHERE twitch_login=%s;",
                    (login,),
                )
                row = cur.fetchone()
                if row:
                    lineage_key, cm_key, stage = row
                    if lineage_key and cm_key is None and int(stage) >= 1:
                        cur.execute(
                            """
                            SELECT key
                            FROM cms
                            WHERE lineage_key=%s AND is_enabled=TRUE AND in_hatch_pool=TRUE
                            ORDER BY random()
                            LIMIT 1;
                            """,
                            (lineage_key,),
                        )
                        pick = cur.fetchone()
                        assigned = pick[0] if pick else None
                        if assigned:
                            cur.execute(
                                "UPDATE creatures SET cm_key=%s, updated_at=now() WHERE twitch_login=%s AND cm_key IS NULL;",
                                (assigned, login),
                            )
            conn.commit()

        if assigned:
            return RedirectResponse(
                url=f"/admin/user/{login}?flash_kind=ok&flash=CM%20attribu%C3%A9%20:%20{assigned}",
                status_code=303,
            )
        return RedirectResponse(
            url=f"/admin/user/{login}?flash_kind=err&flash=Aucun%20CM%20attribu%C3%A9%20(check%20stage%2C%20lign%C3%A9e%2C%20pool)",
            status_code=303,
        )

    # give/set/reset
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO creatures (twitch_login, xp_total, stage)
                VALUES (%s, 0, 0)
                ON CONFLICT (twitch_login) DO NOTHING;
                """,
                (login,),
            )

            if action == "reset":
                # Reset complet (solution B) : retour Œuf + efface lignée + efface CM
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
                    url=f"/admin/user/{login}?flash_kind=ok&flash=Reset%20complet%20(oeuf%20+%20lign%C3%A9e%20effac%C3%A9e)",
                    status_code=303,
                )
            elif action == "set":
                new_xp = max(0, int(amount))
            else:  # give
                cur.execute("SELECT xp_total FROM creatures WHERE twitch_login=%s;", (login,))
                current = int(cur.fetchone()[0])
                new_xp = current + max(0, int(amount))

            new_stage = stage_from_xp(int(new_xp))

            cur.execute(
                """
                UPDATE creatures
                SET xp_total=%s, stage=%s, updated_at=now()
                WHERE twitch_login=%s;
                """,
                (int(new_xp), int(new_stage), login),
            )

        conn.commit()

    if action == "reset":
        msg = "Reset à 0"
    elif action == "set":
        msg = f"XP fixé à {new_xp}"
    else:
        msg = f"+{amount} XP (total {new_xp})"

    return RedirectResponse(url=f"/admin/user/{login}?flash_kind=ok&flash={msg.replace(' ', '%20')}", status_code=303)


# -------------------------
# Admin stats
# -------------------------
@app.get("/admin/stats", response_class=HTMLResponse)
def admin_stats(request: Request, credentials: HTTPBasicCredentials = Depends(security)):
    require_admin(credentials)

    with get_db() as conn:
        with conn.cursor() as cur:
            # start of today in Paris, converted back to timestamptz
            cur.execute("SELECT date_trunc('day', now() AT TIME ZONE 'Europe/Paris') AT TIME ZONE 'Europe/Paris';")
            start_of_today_paris = cur.fetchone()[0]

            cur.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM xp_events WHERE created_at >= %s;",
                (start_of_today_paris,),
            )
            xp_today = int(cur.fetchone()[0])

            cur.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM xp_events WHERE created_at >= now() - interval '7 days';"
            )
            xp_7d = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT COUNT(*) AS events, COUNT(DISTINCT twitch_login) AS users
                FROM xp_events
                WHERE created_at >= now() - interval '24 hours';
                """
            )
            events_24h, active_users_24h = cur.fetchone()
            events_24h = int(events_24h)
            active_users_24h = int(active_users_24h)

            cur.execute(
                """
                SELECT to_char(date_trunc('day', created_at AT TIME ZONE 'Europe/Paris'), 'YYYY-MM-DD') AS day,
                       SUM(amount) AS xp
                FROM xp_events
                WHERE created_at >= now() - interval '7 days'
                GROUP BY 1
                ORDER BY 1 DESC;
                """
            )
            xp_by_day = [{"day": r[0], "xp": int(r[1])} for r in cur.fetchall()]
            max_xp = max([r["xp"] for r in xp_by_day], default=0) or 1
            for r in xp_by_day:
                r["pct"] = int((r["xp"] / max_xp) * 100)

            cur.execute(
                """
                SELECT twitch_login, SUM(amount) AS xp
                FROM xp_events
                WHERE created_at >= now() - interval '24 hours'
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 20;
                """
            )
            top_xp_24h = [{"twitch_login": r[0], "xp": int(r[1])} for r in cur.fetchall()]

            cur.execute(
                """
                SELECT twitch_login, COUNT(*) AS events
                FROM xp_events
                WHERE created_at >= now() - interval '24 hours'
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 20;
                """
            )
            top_events_24h = [{"twitch_login": r[0], "events": int(r[1])} for r in cur.fetchall()]

            cur.execute(
                """
                SELECT COUNT(DISTINCT twitch_login)
                FROM xp_events
                WHERE created_at >= now() - interval '15 minutes';
                """
            )
            active_users_15m = int(cur.fetchone()[0])

    return templates.TemplateResponse(
        "stats.html",
        {
            "request": request,
            "xp_today": xp_today,
            "xp_7d": xp_7d,
            "events_24h": events_24h,
            "active_users_24h": active_users_24h,
            "active_users_15m": active_users_15m,
            "xp_by_day": xp_by_day,
            "top_xp_24h": top_xp_24h,
            "top_events_24h": top_events_24h,
        },
    )
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


@app.post("/admin/cms/action")
def admin_cms_action(
    action: str = Form(...),
    key: str | None = Form(None),
    cm_key: str | None = Form(None),
    cm_name: str | None = Form(None),
    lineage_key: str | None = Form(None),
    credentials: HTTPBasicCredentials = Depends(security),
    media_url: str | None = Form(None),
):
    require_admin(credentials)

    action = action.strip().lower()

    def go(msg: str, kind: str = "ok"):
        # encode minimal
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
            
                # Vérifier que la lignée existe
                cur.execute("SELECT 1 FROM lineages WHERE key=%s;", (lineage_key,))
                if not cur.fetchone():
                    return go("Lineage inconnue", "err")
            
                # Créer le CM (hors pool par défaut)
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

            if action == "delete_cm":
                if not key:
                    return go("Key manquante", "err")
                cur.execute("DELETE FROM cms WHERE key=%s;", (key,))
                # option : si des creatures pointent vers ce cm, on les null
                cur.execute("UPDATE creatures SET cm_key=NULL, updated_at=now() WHERE cm_key=%s;", (key,))
                conn.commit()
                return go(f"CM supprimé: {key}")
            if action == "update_media_url":
                if not key:
                    return go("Key manquante", "err")
                url = (media_url or "").strip()
                cur.execute("UPDATE cms SET media_url=%s WHERE key=%s;", (url, key))
                conn.commit()
                return go(f"Media URL mis à jour: {key}")


            return go("Action inconnue", "err")

@app.get("/internal/rp_bundle")
def rp_bundle(x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT key, lines FROM rp_lines;")
            rows = cur.fetchall()

    # rows: [(key, jsonb), ...]
    bundle = {}
    for k, lines in rows:
        # psycopg peut renvoyer dict/list directement (JSONB) ou string selon config
        if isinstance(lines, str):
            try:
                import json
                lines = json.loads(lines)
            except Exception:
                lines = []
        bundle[k] = lines if isinstance(lines, list) else []
    return {"rp": bundle}

@app.get("/admin/rp", response_class=HTMLResponse)
def admin_rp(request: Request, flash: str | None = None, credentials: HTTPBasicCredentials = Depends(security)):
    require_admin(credentials)

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT key, lines, updated_at FROM rp_lines ORDER BY key;")
            rows = cur.fetchall()

    items = []
    for k, lines, _ in rows:
        if isinstance(lines, str):
            try:
                lines = json.loads(lines)
            except Exception:
                lines = []
            lines = lines if isinstance(lines, list) else []
            text = "\n".join([str(x) for x in lines])
            items.append({"key": k, "count": len(lines), "text": text})

    return templates.TemplateResponse("rp.html", {"request": request, "items": items, "flash": flash})


@app.post("/admin/rp/save")
def admin_rp_save(
    key: str = Form(...),
    lines: str | None = Form(None),
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)
    key = key.strip()

    # 1 ligne = 1 phrase
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

    return RedirectResponse(url=f"/admin/rp?flash=Enregistr%C3%A9%20:{key}", status_code=303)


@app.post("/admin/rp/delete")
def admin_rp_delete(
    key: str = Form(...),
    credentials: HTTPBasicCredentials = Depends(security),
):
    require_admin(credentials)
    key = key.strip()

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM rp_lines WHERE key=%s;", (key,))
        conn.commit()

    return RedirectResponse(url=f"/admin/rp?flash=Supprim%C3%A9%20:{key}", status_code=303)

@app.post("/internal/trigger_show")
def trigger_show(payload: dict, x_api_key: str | None = Header(default=None)):
    require_internal_key(x_api_key)

    login = str(payload.get("twitch_login", "")).strip().lower()
    if not login:
        raise HTTPException(status_code=400, detail="Missing twitch_login")

    duration = int(os.environ.get("SHOW_DURATION_SECONDS", "5"))
    duration = max(2, min(duration, 15))  # clamp

    with get_db() as conn:
        with conn.cursor() as cur:
            # creature
            cur.execute("SELECT xp_total, stage, cm_key FROM creatures WHERE twitch_login=%s;", (login,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=400, detail="No creature")
            xp_total, stage, cm_key = int(row[0]), int(row[1]), row[2]
            if not cm_key:
                raise HTTPException(status_code=400, detail="No CM assigned")

            # cm info
            cur.execute("SELECT name, COALESCE(media_url,'') FROM cms WHERE key=%s;", (cm_key,))
            cmrow = cur.fetchone()
            if not cmrow:
                raise HTTPException(status_code=400, detail="Unknown CM")
            cm_name, media_url = cmrow[0], cmrow[1]
            if not media_url:
                raise HTTPException(status_code=400, detail="CM missing media_url")

        # Twitch profile (outside cursor, but inside conn is ok)
    display, avatar = twitch_user_profile(login)

    stage_start, next_xp = stage_bounds(stage)
    expires_sql = f"now() + interval '{duration} seconds'"

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO overlay_events
                  (twitch_login, viewer_display, viewer_avatar, cm_key, cm_name, cm_media_url,
                   xp_total, stage, stage_start_xp, next_stage_xp, expires_at)
                VALUES
                  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,{expires_sql});
            """, (login, display, avatar, cm_key, cm_name, media_url, xp_total, stage, stage_start, next_xp))
        conn.commit()

    return {"ok": True}

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
        "xp": {"total": xp_total, "stage": int(stage), "pct": pct, "to_next": (next_xp - xp_total) if next_xp else None},
    }


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

<script>
let showing = false;

function showCard(){
  const card = document.getElementById('card');
  if (showing) return;

  card.style.display = 'flex';
  // forcer un reflow pour que la transition s'applique
  void card.offsetWidth;
  card.classList.add('showing');
  showing = true;
}

function hideCard(){
  const card = document.getElementById('card');
  if (!showing) return;

  card.classList.remove('showing');
  // attendre la fin de transition avant de display:none
  setTimeout(() => {
    card.style.display = 'none';
  }, 230);
  showing = false;
}

async function tick(){
  try{
    const r = await fetch('/overlay/state', {cache:'no-store'});
    const j = await r.json();

    if(!j.show){
      hideCard();
      return;
    }

    // Data bind
    document.getElementById('viewer').textContent = `@${j.viewer.name}`;
    document.getElementById('avatar').src = j.viewer.avatar || '';

    document.getElementById('cmimg').src = j.cm.media || '';
    document.getElementById('cmname').textContent = j.cm.name || 'CapsMons';

    const pct = (j.xp.pct === null || j.xp.pct === undefined) ? 100 : j.xp.pct;
    document.getElementById('fill').style.width = pct + '%';

    const toNext = j.xp.to_next;
    document.getElementById('xptext').textContent =
      (toNext ? `${j.xp.total} XP • prochain palier dans ${toNext} XP`
              : `${j.xp.total} XP • stade max`);

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

