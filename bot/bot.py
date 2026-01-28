import os
import time
import asyncio
import random

import requests
import aiohttp
from twitchio.ext import commands

# ============================================================================
# CONFIG / URLs (services Docker)
# ============================================================================
API_CHOOSE_URL = "http://api:8000/internal/choose_lineage"
API_XP_URL = "http://api:8000/internal/xp"
API_STATE_URL = "http://api:8000/internal/creature"
API_LIVE_URL = "http://api:8000/internal/is_live"
API_RP_URL = "http://api:8000/internal/rp_bundle"
API_SHOW_URL = "http://api:8000/internal/trigger_show"

# Drops (nouveau système: join + poll_result)
API_DROP_JOIN_URL = "http://api:8000/internal/drop/join"
API_DROP_POLL_URL = "http://api:8000/internal/drop/poll_result"
API_DROP_SPAWN_URL = "http://api:8000/internal/drop/spawn"
API_INV_URL = "http://api:8000/internal/inventory"
API_HAPPINESS_BATCH_URL = "http://api:8000/internal/happiness/batch"
API_HAPPINESS_DECAY_URL = "http://api:8000/internal/happiness/decay"
API_STREAM_PRESENT_BATCH_URL = "http://api:8000/internal/stream/present_batch"

API_KEY = os.environ["INTERNAL_API_KEY"]

# ============================================================================
# STATE (mémoire RAM du bot)
# ============================================================================
_last_xp_at: dict[str, float] = {}          # cooldown XP chat par user
_active_until: dict[str, float] = {}        # fenêtre présence (user actif jusqu'à timestamp)
_show_last = {"global": 0.0, "users": {}}   # cooldown overlay show

_rp_cache = {"ts": 0.0, "rp": {}}           # cache RP (bundle)
_last_drop_announce = {"sig": None}         # anti double annonce drops


# ============================================================================
# HELPERS (texte / RP)
# ============================================================================
def stage_label(stage: int) -> str:
    return {
        0: "🥚 Œuf",
        1: "🐣 Forme 1",
        2: "🦴 Évolution 1",
        3: "👑 Évolution 2",
    }.get(stage, f"Stage {stage}")


def rp_format(text: str, **kw) -> str:
    """
    Remplace des placeholders {viewer} {title} {xp} etc.
    """
    out = text
    for k, v in kw.items():
        out = out.replace("{" + k + "}", str(v))
    return out


async def rp_get(key: str) -> str | None:
    """
    Renvoie une phrase RP aléatoire pour une clé.
    Cache 60s: évite d'appeler l'API à chaque message.
    """
    now = time.time()

    # refresh toutes les 60 secondes
    if now - _rp_cache["ts"] > 60:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    API_RP_URL,
                    headers={"X-API-Key": API_KEY},
                    timeout=aiohttp.ClientTimeout(total=2),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        _rp_cache["rp"] = data.get("rp", {}) or {}
                        _rp_cache["ts"] = now
        except Exception:
            # conserve le cache précédent si erreur
            pass

    lines = _rp_cache["rp"].get(key)
    if not isinstance(lines, list) or not lines:
        return None
    return random.choice(lines)


def happiness_multiplier(h: int) -> float:
    # 0–24  : un peu moins
    if h < 25:
        return 0.8
    # 25–74 : normal
    if h < 75:
        return 1.0
    # 75–89 : bonus
    if h < 90:
        return 1.3
    # 90–100 : gros bonus
    return 1.6


# ============================================================================
# BOT
# ============================================================================
class Bot(commands.Bot):
    def __init__(self):
        super().__init__(
            token=os.environ["TWITCH_OAUTH_TOKEN"],
            prefix="!",
            initial_channels=[os.environ["TWITCH_CHANNEL"]],
        )
    @commands.command(name="inv")
    async def inv(self, ctx: commands.Context):
        login = ctx.author.name.lower()
    
        try:
            r = requests.get(
                f"{API_INV_URL}/{login}",
                headers={"X-API-Key": API_KEY},
                timeout=2,
            )
            if r.status_code != 200:
                await ctx.send(f"@{ctx.author.name} ⚠️ Inventaire indisponible.")
                return
            data = r.json()
        except Exception:
            await ctx.send(f"@{ctx.author.name} ⚠️ Inventaire indisponible.")
            return
    
        items = data.get("items", []) or []
        if not items:
            await ctx.send(f"@{ctx.author.name} 🎒 Inventaire vide.")
            return
    
        # format compact : item xqty
        txt = ", ".join([f"{it['item_key']}×{it['qty']}" for it in items[:10]])
        await ctx.send(f"@{ctx.author.name} 🎒 {txt}")


    # ------------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------------
    async def event_ready(self):
        print(f"[BOT] Connected as {self.nick} | Joined: {os.environ['TWITCH_CHANNEL']}", flush=True)
    
        # XP passive (si tu veux la loop)
        self.loop.create_task(self.presence_loop())
    
        # Drops: annonce résultats
        self.loop.create_task(self.drop_announce_loop())

        # Drops auto
        self.loop.create_task(self.auto_drop_loop())

        # Decay Happiness auto
        self.loop.create_task(self.happiness_decay_loop())


    

    # ------------------------------------------------------------------------
    # Verif si mod ou broadcaster
    # ------------------------------------------------------------------------
    def _is_mod_or_broadcaster(self, ctx: commands.Context) -> bool:
        try:
            return bool(getattr(ctx.author, "is_broadcaster", False) or getattr(ctx.author, "is_mod", False))
        except Exception:
            return False
    # ------------------------------------------------------------------------
    # Commande !drop
    # ------------------------------------------------------------------------

    @commands.command(name="drop")
    async def drop_cmd(self, ctx: commands.Context):
        # Mod / streamer only
        if not self._is_mod_or_broadcaster(ctx):
            return
    
        # Usage:
        # !drop first "Titre" URL [durée] [xp] [ticket_key] [qty] [target_hits]
        # !drop random "Titre" URL [durée] [xp] [ticket_key] [qty]
        # !drop coop "Titre" URL [durée] [xp] [ticket_key] [qty] [target_hits]
        raw = ctx.message.content.strip()
    
        parts = raw.split()
        if len(parts) < 4:
            await ctx.send('Usage: !drop first|random|coop "Titre" URL [durée] [xp] [ticket_key] [qty] [target]')
            return
    
        mode = parts[1].lower()
        if mode not in ("first", "random", "coop"):
            await ctx.send("Modes: first, random, coop")
            return
    
        # Titre entre guillemets
        title = None
        rest = []
        if '"' in raw:
            try:
                i1 = raw.index('"')
                i2 = raw.index('"', i1 + 1)
                title = raw[i1 + 1:i2].strip()
                rest = raw[i2 + 1:].strip().split()
            except Exception:
                title = None
    
        if title is None:
            # fallback: 1 mot de titre
            title = parts[2]
            rest = parts[3:]
    
        if not rest:
            await ctx.send('Usage: !drop first|random|coop "Titre" URL [durée] [xp] [ticket_key] [qty] [target]')
            return
    
        media_url = rest[0]
        duration = int(rest[1]) if len(rest) >= 2 and rest[1].isdigit() else 10
        xp_bonus = int(rest[2]) if len(rest) >= 3 and rest[2].isdigit() else 50
        ticket_key = rest[3] if len(rest) >= 4 else "ticket_basic"
        ticket_qty = int(rest[4]) if len(rest) >= 5 and rest[4].isdigit() else 1
    
        payload = {
            "mode": mode,
            "title": title,
            "media_url": media_url,
            "duration_seconds": duration,
            "xp_bonus": xp_bonus,
            "ticket_key": ticket_key,
            "ticket_qty": ticket_qty,
        }
    
        if mode == "coop":
            target = int(rest[5]) if len(rest) >= 6 and rest[5].isdigit() else 10
            payload["target_hits"] = target
    
        try:
            r = requests.post(
                API_DROP_SPAWN_URL,
                headers={"X-API-Key": API_KEY},
                json=payload,
                timeout=3,
            )
            if r.status_code != 200:
                print("[BOT] drop spawn fail:", r.status_code, (r.text or "")[:200], flush=True)
                await ctx.send(f"⛔ drop fail ({r.status_code})")
                return
        except Exception as e:
            print("[BOT] drop spawn error:", e, flush=True)
            await ctx.send("⛔ drop error")
            return
    
        # RP spawn (si tu as les clés), sinon fallback
        rp_key = f"drop.spawn.{mode}"
        line = await rp_get(rp_key) or "✨ Drop lancé : {title}. \"!grab\" pour participer"
        msg = rp_format(line, title=title, xp=xp_bonus, ticket_key=ticket_key, ticket_qty=ticket_qty)
        await ctx.send(msg)




    # ------------------------------------------------------------------------
    # Message handler (XP chat + commands)
    # ------------------------------------------------------------------------
    async def event_message(self, message):
        if message.echo:
            return

        login = message.author.name.lower()

        # 1) Marquer actif (présence)
        window = int(os.environ.get("PRESENCE_ACTIVE_WINDOW_SECONDS", "900"))
        _active_until[login] = time.time() + window

        # 2) Donner XP chat sous cooldown
        now = time.time()
        cooldown = int(os.environ.get("CHAT_XP_COOLDOWN_SECONDS", "20"))
        last = _last_xp_at.get(login, 0.0)

        if now - last >= cooldown:
            _last_xp_at[login] = now

            resp = None
            try:
                resp = requests.post(
                    API_XP_URL,
                    headers={"X-API-Key": API_KEY},
                    json={"twitch_login": login, "amount": 1},
                    timeout=2,
                )

                if resp.status_code != 200:
                    print("[BOT] XP API status:", resp.status_code, (resp.text or "")[:200], flush=True)
                else:
                    try:
                        data = resp.json()
                    except Exception:
                        print("[BOT] XP API non-JSON:", (resp.text or "")[:200], flush=True)
                        data = None

                    if data:
                        before = int(data.get("stage_before", 0))
                        after = int(data.get("stage_after", before))

                        # annonce uniquement si évolution
                        if after > before:
                            intro = await rp_get("evolve.announce") or "✨ Évolution !"
                            msg = f"{intro} @{message.author.name} {stage_label(before)} ➜ {stage_label(after)}"

                            cm_assigned = data.get("cm_assigned")
                            if cm_assigned:
                                cm_line = await rp_get("cm.assigned") or "👾 CM attribué !"
                                msg += f" | {cm_line} {cm_assigned}"

                            await message.channel.send(msg)

            except Exception as e:
                body = ""
                if resp is not None:
                    body = (resp.text or "")[:200]
                print("[BOT] XP API error:", repr(e), body, flush=True)

        # 3) IMPORTANT: toujours laisser passer les commandes
        await self.handle_commands(message)

    def _is_mod_or_broadcaster(self, ctx: commands.Context) -> bool:
    # twitchio fournit généralement ces flags
        try:
            if getattr(ctx.author, "is_broadcaster", False):
                return True
            if getattr(ctx.author, "is_mod", False):
                return True
        except Exception:
            pass
        return False

    @commands.command(name="spawn")
    async def spawn(self, ctx: commands.Context):
        # sécurité: mod / broadcaster seulement
        if not self._is_mod_or_broadcaster(ctx):
            return
    
        # Usage:
        # !spawn first "Nom du drop" https://.../drop.png 10 50 ticket_basic 1
        # !spawn random "Nom du drop" https://.../drop.png 10 50 ticket_basic 1
        #
        # (durée=10s, xp=50, ticket=ticket_basic x1)
    
        parts = ctx.message.content.strip().split()
        if len(parts) < 4:
            await ctx.send('Usage: !spawn first|random "Titre" URL [durée] [xp] [ticket_key] [qty]')
            return
    
        mode = parts[1].lower()
        if mode not in ("first", "random", "coop"):
            await ctx.send("Modes: first, random, coop")
            return
    
        # --- Parsing simple mais robuste :
        # On accepte un titre entre guillemets ou sans guillemets.
        raw = ctx.message.content.strip()
    
        # essayer de récupérer le titre entre "..."
        title = None
        if '"' in raw:
            try:
                first = raw.index('"')
                second = raw.index('"', first + 1)
                title = raw[first + 1: second].strip()
                rest = raw[second + 1:].strip().split()
            except Exception:
                title = None
    
        if title is None:
            # fallback: titre = 1 mot (moins bien)
            title = parts[2]
            rest = parts[3:]
    
        if not rest:
            await ctx.send('Usage: !spawn first|random "Titre" URL [durée] [xp] [ticket_key] [qty]')
            return
    
        media_url = rest[0]
        duration = int(rest[1]) if len(rest) >= 2 and rest[1].isdigit() else 10
        xp_bonus = int(rest[2]) if len(rest) >= 3 and rest[2].isdigit() else 50
        ticket_key = rest[3] if len(rest) >= 4 else "ticket_basic"
        ticket_qty = int(rest[4]) if len(rest) >= 5 and rest[4].isdigit() else 1
    
        payload = {
            "mode": mode,
            "title": title,
            "media_url": media_url,
            "duration_seconds": duration,
            "xp_bonus": xp_bonus,
            "ticket_key": ticket_key,
            "ticket_qty": ticket_qty,
        }
    
        # coop : target_hits en option en dernier
        if mode == "coop":
            # exemple: ... ticket_basic 1 12
            target = int(rest[5]) if len(rest) >= 6 and rest[5].isdigit() else 10
            payload["target_hits"] = target
    
        try:
            r = requests.post(
                "http://api:8000/internal/drop/spawn",
                headers={"X-API-Key": API_KEY},
                json=payload,
                timeout=3,
            )
            if r.status_code != 200:
                print("[BOT] spawn fail:", r.status_code, (r.text or "")[:200], flush=True)
                await ctx.send(f"⛔ spawn fail ({r.status_code})")
                return
        except Exception as e:
            print("[BOT] spawn error:", e, flush=True)
            await ctx.send("⛔ spawn error")
            return
    
        # RP spawn (différent selon mode)
        rp_key = f"drop.spawn.{mode}"
        line = await rp_get(rp_key) or "✨ Drop lancé !"
        msg = rp_format(line, title=title, xp=xp_bonus, ticket_key=ticket_key, ticket_qty=ticket_qty)
        await ctx.send(msg)

    # ------------------------------------------------------------------------
    # Commande USE
    # ------------------------------------------------------------------------


    @commands.command(name="use")
    async def use_item(self, ctx: commands.Context):
        login = ctx.author.name.lower()
        parts = ctx.message.content.strip().split()
    
        if len(parts) < 2:
            await ctx.send(f"@{ctx.author.name} usage: !use <item_key>")
            return
    
        item_key = parts[1].strip().lower()
    
        try:
            r = requests.post(
                "http://api:8000/internal/item/use",
                headers={"X-API-Key": API_KEY},
                json={"twitch_login": login, "item_key": item_key},
                timeout=2,
            )
    
            # Erreurs API avec détail exploitable
            if r.status_code != 200:
                msg = "⛔ Objet indisponible."
                try:
                    d = r.json()
                    detail = str(d.get("detail", "")).lower()
                    if "unknown item" in detail:
                        msg = "⛔ Objet inconnu."
                    elif "no item" in detail:
                        msg = "⛔ Tu n’en as pas dans ton inventaire."
                except Exception:
                    pass
    
                await ctx.send(f"@{ctx.author.name} {msg}")
                return
    
            data = r.json()
    
        except Exception as e:
            print("[BOT] use error:", e, flush=True)
            await ctx.send(f"@{ctx.author.name} ⚠️ Erreur objet.")
            return
    
        effect = data.get("effect")
    
        # XP capsule
        if effect == "xp":
            gained = int(data.get("xp_gain", 0))
            await ctx.send(f"@{ctx.author.name} 💊 Capsule consommée ! +{gained} XP ⚡")
            return
    
        # Bonheur
        if effect == "happiness":
            gain_h = int(data.get("happiness_gain", 0))
            after_h = int(data.get("happiness_after", 0))
            name = data.get("item_name", item_key)
            await ctx.send(f"@{ctx.author.name} 🥰 {name} utilisé ! +{gain_h} bonheur (❤️ {after_h}%).")
            return
    
        # Fallback
        await ctx.send(f"@{ctx.author.name} ✔️ Objet utilisé.")

    async def happiness_decay_loop(self):
        # laisse le temps au bot/containers de démarrer
        await asyncio.sleep(60)
    
        while True:
            # on tente 1 fois / heure (l'API skip si déjà fait aujourd'hui)
            await asyncio.sleep(3600)
    
            try:
                r = requests.post(
                    API_HAPPINESS_DECAY_URL,
                    headers={"X-API-Key": API_KEY},
                    timeout=5,
                )
                if r.status_code != 200:
                    print("[BOT] happiness decay fail:", r.status_code, (r.text or "")[:160], flush=True)
                    continue
    
                data = r.json()
                if not data.get("skipped", False):
                    print("[BOT] happiness decay ran:", data.get("date"), flush=True)
    
            except Exception as e:
                print("[BOT] happiness decay error:", e, flush=True)


    # ------------------------------------------------------------------------
    # DROP AUTO
    # ------------------------------------------------------------------------


    async def auto_drop_loop(self):
        enabled = os.environ.get("AUTO_DROP_ENABLED", "false").lower() in ("1", "true", "yes", "on")
        if not enabled:
            return
    
        min_s = int(os.environ.get("AUTO_DROP_MIN_SECONDS", "900"))
        max_s = int(os.environ.get("AUTO_DROP_MAX_SECONDS", "1500"))
        min_s = max(60, min_s)
        max_s = max(min_s, max_s)
    
        while True:
            # attend un délai aléatoire
            await asyncio.sleep(random.randint(min_s, max_s))
    
            # 1) seulement si live
            try:
                r = requests.get(API_LIVE_URL, headers={"X-API-Key": API_KEY}, timeout=2)
                if r.status_code != 200 or not r.json().get("is_live", False):
                    continue
            except Exception:
                continue
    
            # 2) ne pas spawn s'il y a déjà un drop actif (on utilise l'endpoint overlay)
            try:
                s = requests.get("http://api:8000/overlay/drop_state", timeout=2)
                if s.status_code == 200 and s.json().get("show", False):
                    continue
            except Exception:
                # si on ne peut pas vérifier, on ne spawn pas (safe)
                continue
    
            # 3) spawn drop (capsule xp)
            payload = {
                "mode": os.environ.get("AUTO_DROP_MODE", "random").strip().lower(),
                "title": os.environ.get("AUTO_DROP_TITLE", "Capsule XP").strip(),
                "media_url": os.environ.get("AUTO_DROP_MEDIA_URL", "").strip(),
                "duration_seconds": int(os.environ.get("AUTO_DROP_DURATION_SECONDS", "25")),
                "xp_bonus": int(os.environ.get("AUTO_DROP_XP_BONUS", "30")),
                "ticket_key": os.environ.get("AUTO_DROP_TICKET_KEY", "xp_capsule").strip(),
                "ticket_qty": int(os.environ.get("AUTO_DROP_TICKET_QTY", "1")),
            }
    
            if not payload["media_url"]:
                print("[BOT] AUTO_DROP_MEDIA_URL missing, skip", flush=True)
                continue
    
            try:
                rr = requests.post(
                    API_DROP_SPAWN_URL,
                    headers={"X-API-Key": API_KEY},
                    json=payload,
                    timeout=3,
                )
                if rr.status_code != 200:
                    print("[BOT] auto drop spawn fail:", rr.status_code, (rr.text or "")[:200], flush=True)
                    continue
            except Exception as e:
                print("[BOT] auto drop spawn error:", e, flush=True)
                continue
    
            # 4) message RP (optionnel)
            try:
                chan = self.get_channel(os.environ["TWITCH_CHANNEL"])
                if chan:
                    rp_key = f"drop.spawn.{payload['mode']}"
                    line = await rp_get(rp_key) or "✨ Drop automatique : {title}. Attrape le avec !grab."
                    msg = rp_format(line, title=payload["title"], xp=payload["xp_bonus"],
                                    ticket_key=payload["ticket_key"], ticket_qty=payload["ticket_qty"])
                    await chan.send(msg)
            except Exception:
                pass


    # ------------------------------------------------------------------------
    # Presence loop (XP de présence)
    # ------------------------------------------------------------------------
    async def presence_loop(self):
        tick = int(os.environ.get("PRESENCE_TICK_SECONDS", "300"))
        base_amount = int(os.environ.get("PRESENCE_XP_AMOUNT", "2"))
    
        ignore_bots = set(
            x.strip().lower()
            for x in os.environ.get("PRESENCE_IGNORE_BOTS", "").split(",")
            if x.strip()
        )
    
        while True:
            await asyncio.sleep(tick)
    
            # 1) Ne donner la présence XP que si stream LIVE
            try:
                r = requests.get(API_LIVE_URL, headers={"X-API-Key": API_KEY}, timeout=2)
                if r.status_code != 200 or not r.json().get("is_live", False):
                    continue
            except Exception as e:
                print("[BOT] is_live check error:", e, flush=True)
                continue
    
            # 2) Récupérer la liste des chatters (présents dans le chat)
            chan = self.get_channel(os.environ["TWITCH_CHANNEL"])
            if not chan:
                continue
    
            try:
                chatters = getattr(chan, "chatters", None)
                if not chatters:
                    continue
    
                logins: list[str] = []
    
                if isinstance(chatters, dict):
                    logins = [str(k).strip().lower() for k in chatters.keys() if str(k).strip()]
                else:
                    for c in chatters:
                        name = None
                        if hasattr(c, "name") and c.name:
                            name = c.name
                        elif hasattr(c, "username") and c.username:
                            name = c.username
                        elif hasattr(c, "display_name") and c.display_name:
                            name = c.display_name
                        if name:
                            logins.append(str(name).strip().lower())
    
                logins = list({u for u in logins if u})
    
            except Exception as e:
                print("[BOT] chatters read error:", e, flush=True)
                continue
    
            if not logins:
                continue
    
            # 3) Ignorer le bot (et autres bots optionnels)
            bot_login = os.environ.get("TWITCH_BOT_LOGIN", "").strip().lower()
            if bot_login:
                logins = [u for u in logins if u != bot_login]
            if ignore_bots:
                logins = [u for u in logins if u not in ignore_bots]
    
            if not logins:
                continue
    
            print("[BOT] chatters=", 0 if not chatters else len(chatters), flush=True)
            print(f"[BOT] Presence tick: live OK, chatters={len(logins)}, base={base_amount} XP (bonheur scaling)", flush=True)
            print("[BOT] sample logins:", logins[:5], flush=True)
    
            # 3.5) 🔥 NOUVEAU : enregistrer les présents pour la session de stream (streak)
            # (silencieux, pas de spam)
            try:
                pb = requests.post(
                    API_STREAM_PRESENT_BATCH_URL,
                    headers={"X-API-Key": API_KEY},
                    json={"logins": logins},
                    timeout=3,
                )
                if pb.status_code != 200:
                    print("[BOT] present_batch failed:", pb.status_code, (pb.text or "")[:120], flush=True)
            except Exception as e:
                print("[BOT] present_batch error:", e, flush=True)
    
            # 4) Récupérer le bonheur de tout le monde en 1 appel (batch)
            try:
                rr = requests.post(
                    API_HAPPINESS_BATCH_URL,
                    headers={"X-API-Key": API_KEY},
                    json={"logins": logins},
                    timeout=3,
                )
                if rr.status_code == 200:
                    data = rr.json()
                    happ_map = data.get("happiness", {}) if isinstance(data, dict) else {}
                else:
                    print("[BOT] happiness batch failed:", rr.status_code, (rr.text or "")[:120], flush=True)
                    happ_map = {}
            except Exception as e:
                print("[BOT] happiness batch error:", e, flush=True)
                happ_map = {}
    
            # 5) Donner XP à tous les présents, modulé par bonheur
            for ulogin in logins:
                try:
                    h = int(happ_map.get(ulogin, 50))
                    mult = happiness_multiplier(h)
                    give = max(1, int(round(base_amount * mult)))
    
                    resp = requests.post(
                        API_XP_URL,
                        headers={"X-API-Key": API_KEY},
                        json={"twitch_login": ulogin, "amount": give},
                        timeout=2,
                    )
                    if resp.status_code != 200:
                        print("[BOT] Presence XP failed:", resp.status_code, (resp.text or "")[:120], "login=", ulogin, flush=True)
    
                except Exception as e:
                    print("[BOT] Presence API error:", e, flush=True)
    
        # ------------------------------------------------------------------------
        # Drops announce loop (poll_result)
        # ------------------------------------------------------------------------
    async def drop_announce_loop(self):
        await asyncio.sleep(2)
    
        while True:
            await asyncio.sleep(2)
    
            try:
                r = requests.get(
                    API_DROP_POLL_URL,
                    headers={"X-API-Key": API_KEY},
                    timeout=2,
                )
                if r.status_code != 200:
                    continue
                data = r.json()
            except Exception:
                continue
    
            if not data.get("announce", False):
                continue
    
            mode = data.get("mode", "random")
            status = data.get("status", "expired")
            title = data.get("title", "un objet")
            winners = data.get("winners", []) or []
            xp_bonus = int(data.get("xp_bonus", 0))
            ticket_key = data.get("ticket_key", "ticket_basic")
            ticket_qty = int(data.get("ticket_qty", 1))
    
            sig = f"{mode}|{status}|{title}|{','.join(winners)}|{xp_bonus}|{ticket_key}|{ticket_qty}"
            if _last_drop_announce["sig"] == sig:
                continue
            _last_drop_announce["sig"] = sig
    
            if status != "resolved":
                rp_key = "drop.fail.coop" if mode == "coop" else "drop.fail.timeout"
                line = await rp_get(rp_key) or "⌛ Trop tard…"
                msg = rp_format(line, title=title, xp=xp_bonus, ticket_key=ticket_key, ticket_qty=ticket_qty, count=len(winners), viewer="")
            else:
                if mode == "first":
                    rp_key = "drop.win.first"
                elif mode == "random":
                    rp_key = "drop.win.random"
                else:
                    rp_key = "drop.win.coop"
    
                line = await rp_get(rp_key) or "🏆 {viewer} gagne {title} !"
                viewer = f"@{winners[0]}" if winners else ""
                msg = rp_format(line, viewer=viewer, title=title, xp=xp_bonus, ticket_key=ticket_key, ticket_qty=ticket_qty, count=len(winners))
    
            try:
                chan = self.get_channel(os.environ["TWITCH_CHANNEL"])
                if chan:
                    await chan.send(msg)
            except Exception:
                pass

    # ------------------------------------------------------------------------
    # Commande: !creature
    # ------------------------------------------------------------------------
    @commands.command(name="creature")
    async def creature(self, ctx: commands.Context):
        login = ctx.author.name.lower()

        # API state
        try:
            r = requests.get(
                f"{API_STATE_URL}/{login}",
                headers={"X-API-Key": API_KEY},
                timeout=2,
            )
            if r.status_code != 200:
                await ctx.send(f"@{ctx.author.name} ⚠️ API indisponible ({r.status_code}).")
                return
            data = r.json()
        except Exception as e:
            print("[BOT] creature error:", e, flush=True)
            await ctx.send(f"@{ctx.author.name} ⚠️ Impossible de récupérer ta créature.")
            return

        stage = int(data.get("stage", 0))
        xp_total = int(data.get("xp_total", 0))
        nxt = data.get("next", "Max")
        xp_to_next = int(data.get("xp_to_next", 0))
        happy = int(data.get("happiness", 0))
        happy_bar = "█" * (happy // 10) + "░" * (10 - (happy // 10))
        streak = int(data.get("streak_count", 0))


        lineage = data.get("lineage_key")
        cm_key = data.get("cm_key")

        flavor = await rp_get(f"creature.stage{stage}")
        flavor_txt = f" | {flavor}" if flavor else ""

        if cm_key:
            extra = f" — CM: {cm_key} (lignée {str(lineage).upper() if lineage else '—'})"
        elif lineage:
            extra = f" — lignée: {str(lineage).upper()} (CM à l’éclosion)"
        else:
            extra = " — lignée: non choisie (utilise !choose)"

        header = f"👁️ @{ctx.author.name}"
        state = f"{stage_label(stage)} | {xp_total} XP"
        prog = "🏁 Stade max" if nxt == "Max" else f"⏳ {nxt} dans {xp_to_next} XP"
        streak_txt = f" 🔥 Fidélité ManaCorp : {streak}" if streak > 0 else " streams !"


        await ctx.send(f"{header} • {state} • {prog}{extra}{flavor_txt} | ❤️ {happy}% [{happy_bar}] | {streak_txt}")


    # ------------------------------------------------------------------------
    # Commande: !choose
    # ------------------------------------------------------------------------
    @commands.command(name="choose")
    async def choose(self, ctx: commands.Context):
        login = ctx.author.name.lower()
        parts = ctx.message.content.strip().split()

        if len(parts) < 2:
            await ctx.send(f"@{ctx.author.name} usage: !choose biolab|securite|extraction|limited")
            return

        lineage = parts[1].strip().lower()

        try:
            r = requests.post(
                API_CHOOSE_URL,
                headers={"X-API-Key": API_KEY},
                json={"twitch_login": login, "lineage_key": lineage},
                timeout=2,
            )
            if r.status_code != 200:
                await ctx.send(f"@{ctx.author.name} ⛔ {r.text}")
                return
        except Exception as e:
            print("[BOT] choose error:", e, flush=True)
            await ctx.send(f"@{ctx.author.name} ⚠️ choose indisponible.")
            return

        ok_line = await rp_get("choose.ok") or "✅ Lignée enregistrée."
        await ctx.send(f"@{ctx.author.name} {ok_line} ({lineage})")

    # ------------------------------------------------------------------------
    # Commande: !show (overlay CM/oeuf + son)
    # ------------------------------------------------------------------------
    @commands.command(name="show")
    async def show(self, ctx: commands.Context):
        now = time.time()
        login = ctx.author.name.lower()

        # cooldowns show
        if now - _show_last["global"] < 8:
            return
        if now - _show_last["users"].get(login, 0.0) < 30:
            return

        try:
            r = requests.post(
                API_SHOW_URL,
                headers={"X-API-Key": API_KEY},
                json={"twitch_login": login},
                timeout=2,
            )
            if r.status_code != 200:
                print("[BOT] show fail:", r.status_code, (r.text or "")[:200], flush=True)
                return
        except Exception as e:
            print("[BOT] show error:", e, flush=True)
            return

        _show_last["global"] = now
        _show_last["users"][login] = now
        await ctx.send(f"@{ctx.author.name} 👾 affichage du CapsMons !")

    # ------------------------------------------------------------------------
    # Commande: !grab (drops)
    # ------------------------------------------------------------------------
    @commands.command(name="grab")
    async def grab(self, ctx: commands.Context):
        login = ctx.author.name.lower()

        try:
            r = requests.post(
                API_DROP_JOIN_URL,
                headers={"X-API-Key": API_KEY},
                json={"twitch_login": login},
                timeout=2,
            )
            if r.status_code != 200:
                return
            data = r.json()
        except Exception:
            return

        if not data.get("active"):
            late = await rp_get("drop.claim.late") or "⌛ Trop tard…"
            await ctx.send(f"@{ctx.author.name} {late}")
            return

        # si déjà inscrit, on peut le dire
        joined = bool(data.get("joined", False))
        title = data.get("title", "un objet")

        if not joined:
            ok = await rp_get("drop.claim.ok") or "📡 Déjà enregistré."
            await ctx.send(f"@{ctx.author.name} {ok} ({title})")
            return

        # participation OK
        ok = await rp_get("drop.claim.ok") or "📡 Participation validée."
        await ctx.send(f"@{ctx.author.name} {ok} ({title})")

        # cas spécial: mode first peut renvoyer result.won
        result = data.get("result")
        if result and result.get("won"):
            # annonce immédiate (en plus de la loop)
            line = await rp_get("drop.win.first") or "⚡ {viewer} gagne {title} !"
            msg = rp_format(
                line,
                viewer=f"@{login}",
                title=result.get("title", title),
                xp=result.get("xp_bonus", 0),
                ticket_key=result.get("ticket_key", "ticket_basic"),
                ticket_qty=result.get("ticket_qty", 1),
                count=1,
            )
            await ctx.send(msg)

    # ------------------------------------------------------------------------
    # Commande: !hit (coop) -> MVP: identique à !grab
    # ------------------------------------------------------------------------
    @commands.command(name="hit")
    async def hit(self, ctx: commands.Context):
        await self.grab(ctx)




# ============================================================================
# RUN
# ============================================================================
bot = Bot()
bot.run()
