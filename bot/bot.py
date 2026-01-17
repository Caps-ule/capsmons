import os
import time
import asyncio
import random

import requests
import aiohttp
from twitchio.ext import commands

API_CHOOSE_URL = "http://api:8000/internal/choose_lineage"
API_XP_URL = "http://api:8000/internal/xp"
API_STATE_URL = "http://api:8000/internal/creature"
API_LIVE_URL = "http://api:8000/internal/is_live"
API_RP_URL = "http://api:8000/internal/rp_bundle"
API_SHOW_URL = "http://api:8000/internal/trigger_show"

API_KEY = os.environ["INTERNAL_API_KEY"]

_last_xp_at: dict[str, float] = {}       # chat xp cooldown
_active_until: dict[str, float] = {}     # presence window
_show_last = {"global": 0.0, "users": {}}  # show cooldowns

_rp_cache = {"ts": 0.0, "rp": {}}     # RP cache


def stage_label(stage: int) -> str:
    return {
        0: "🥚 Œuf",
        1: "🐣 Éclosion",
        2: "🦴 Évolution 1",
        3: "👑 Évolution 2",
    }.get(stage, f"Stage {stage}")


async def rp_get(key: str) -> str | None:
    """Return one random RP line for a given key, using a 60s cache."""
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
            # conserve le cache précédent
            pass

    lines = _rp_cache["rp"].get(key)
    if not isinstance(lines, list) or not lines:
        return None
    return random.choice(lines)


class Bot(commands.Bot):
    def __init__(self):
        super().__init__(
            token=os.environ["TWITCH_OAUTH_TOKEN"],
            prefix="!",
            initial_channels=[os.environ["TWITCH_CHANNEL"]],
        )

    async def event_ready(self):
        print(f"[BOT] Connected as {self.nick} | Joined: {os.environ['TWITCH_CHANNEL']}", flush=True)
        self.loop.create_task(self.presence_loop())

    async def event_message(self, message):
        if message.echo:
            return

        login = message.author.name.lower()

        # Marquer actif (présence)
        window = int(os.environ.get("PRESENCE_ACTIVE_WINDOW_SECONDS", "900"))
        _active_until[login] = time.time() + window

        # Cooldown XP chat
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
                    # ne bloque pas les commandes
                else:
                    try:
                        data = resp.json()
                    except Exception:
                        print("[BOT] XP API non-JSON:", (resp.text or "")[:200], flush=True)
                        data = None

                    if data:
                        before = int(data.get("stage_before", 0))
                        after = int(data.get("stage_after", before))

                        # Annonce uniquement si évolution
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

        # Toujours laisser passer les commandes
        await self.handle_commands(message)

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
            actives = [u for u, until in _active_until.items() if until > now]

            # nettoyage
            for u in list(_active_until.keys()):
                if _active_until[u] <= now:
                    _active_until.pop(u, None)

            if not actives:
                continue

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

        # RP stage line
        flavor = await rp_get(f"creature.stage{stage}")
        flavor_txt = f" | {flavor}" if flavor else ""

        # lineage/cm info
        if cm_key:
            extra = f" — CM: {cm_key} (lignée {str(lineage).upper() if lineage else '—'})"
        elif lineage:
            extra = f" — lignée: {str(lineage).upper()} (CM à l’éclosion)"
        else:
            extra = " — lignée: non choisie (utilise !choose)"

        header = f"👁️ CapsMons — @{ctx.author.name}"
        state = f"{stage_label(stage)} | {xp_total} XP"

        if nxt == "Max":
            prog = "🏁 Stade max"
        else:
            prog = f"⏳ {nxt} dans {xp_to_next} XP"

        await ctx.send(f"{header} • {state} • {prog}{extra}{flavor_txt}")

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

    @commands.command(name="show")
    async def show(self, ctx: commands.Context):
        now = time.time()
        login = ctx.author.name.lower()

        # cooldowns
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

        # message court en chat
        await ctx.send(f"@{ctx.author.name} 👾 affichage du CapsMons !")


bot = Bot()
bot.run()
