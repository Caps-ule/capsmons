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
        1: "🐣 Éclosion",
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

    # ------------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------------
    async def event_ready(self):
        print(f"[BOT] Connected as {self.nick} | Joined: {os.environ['TWITCH_CHANNEL']}", flush=True)

        # Loop présence (XP périodique)
        self.loop.create_task(self.presence_loop())

        # Loop drops (annonce résultat)
        self.loop.create_task(self.drop_announce_loop())

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

    # ------------------------------------------------------------------------
    # Presence loop (XP de présence)
    # ------------------------------------------------------------------------
    async def presence_loop(self):
        tick = int(os.environ.get("PRESENCE_TICK_SECONDS", "300"))
        amount = int(os.environ.get("PRESENCE_XP_AMOUNT", "2"))

        while True:
            await asyncio.sleep(tick)

            # Ne donner la présence XP que si stream LIVE
            try:
                r = requests.get(API_LIVE_URL, headers={"X-API-Key": API_KEY}, timeout=2)
                if r.status_code != 200 or not r.json().get("is_live", False):
                    continue
            except Exception as e:
                print("[BOT] is_live check error:", e, flush=True)
                continue

            now = time.time()

            # Liste des actifs
            actives = [u for u, until in _active_until.items() if until > now]

            # nettoyage
            for u in list(_active_until.keys()):
                if _active_until[u] <= now:
                    _active_until.pop(u, None)

            if not actives:
                continue

            # XP présence à tous les actifs
            for ulogin in actives:
                try:
                    requests.post(
                        API_XP_URL,
                        headers={"X-API-Key": API_KEY},
                        json={"twitch_login": ulogin, "amount": amount},
                        timeout=2,
                    )
                except Exception as e:
                    print("[BOT] Presence API error:", e, flush=True)

    # ------------------------------------------------------------------------
    # Drops announce loop (poll_result)
    # ------------------------------------------------------------------------
    async def drop_announce_loop(self):
        """
        Toutes les 2s:
        - appelle /internal/drop/poll_result
        - si announce=true => envoie un message RP dans le chat
        """
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
            status = data.get("status", "expired")  # resolved|expired
            title = data.get("title", "un objet")
            winners = data.get("winners", []) or []
            xp_bonus = int(data.get("xp_bonus", 0))
            ticket_key = data.get("ticket_key", "ticket_basic")
            ticket_qty = int(data.get("ticket_qty", 1))

            # anti double annonce (au cas où)
            sig = f"{mode}|{status}|{title}|{','.join(winners)}|{xp_bonus}|{ticket_key}|{ticket_qty}"
            if _last_drop_announce["sig"] == sig:
                continue
            _last_drop_announce["sig"] = sig

            # construire message RP
            if status != "resolved":
                rp_key = "drop.fail.coop" if mode == "coop" else "drop.fail.timeout"
                line = await rp_get(rp_key) or "⌛ Trop tard…"
                msg = rp_format(
                    line,
                    viewer="",
                    title=title,
                    xp=xp_bonus,
                    ticket_key=ticket_key,
                    ticket_qty=ticket_qty,
                    count=len(winners),
                )
            else:
                if mode == "first":
                    rp_key = "drop.win.first"
                elif mode == "random":
                    rp_key = "drop.win.random"
                else:
                    rp_key = "drop.win.coop"

                line = await rp_get(rp_key) or "🏆 {viewer} gagne {title} !"
                viewer = f"@{winners[0]}" if winners else ""
                msg = rp_format(
                    line,
                    viewer=viewer,
                    title=title,
                    xp=xp_bonus,
                    ticket_key=ticket_key,
                    ticket_qty=ticket_qty,
                    count=len(winners),
                )

            # envoyer dans le channel
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

        header = f"👁️ CapsMons — @{ctx.author.name}"
        state = f"{stage_label(stage)} | {xp_total} XP"
        prog = "🏁 Stade max" if nxt == "Max" else f"⏳ {nxt} dans {xp_to_next} XP"

        await ctx.send(f"{header} • {state} • {prog}{extra}{flavor_txt}")

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
