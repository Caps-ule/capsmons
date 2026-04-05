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
API_DROP_JOIN_URL = "http://api:8000/internal/drop/join"
API_DROP_POLL_URL = "http://api:8000/internal/drop/poll_result"
API_DROP_SPAWN_URL = "http://api:8000/internal/drop/spawn"
API_INV_URL = "http://api:8000/internal/inventory"
API_HAPPINESS_BATCH_URL = "http://api:8000/internal/happiness/batch"
API_HAPPINESS_DECAY_URL = "http://api:8000/internal/happiness/decay"
API_STREAM_PRESENT_BATCH_URL = "http://api:8000/internal/stream/present_batch"
API_ITEM_PICK_URL = "http://api:8000/internal/items/pick"
API_COLLECTION_URL = "http://api:8000/internal/collection"
API_COMPANION_SET_URL = "http://api:8000/internal/companion/set"
API_COMPANIONS_LIST_URL = "http://api:8000/internal/companions"
API_COMPANIONS_SET_ACTIVE_URL = "http://api:8000/internal/companions/set_active"
API_SET_ACTIVE_URL = "http://api:8000/internal/companions/set_active"
API_COMPANION_SET_BY_ID_URL = "http://api:8000/internal/companions/set_active"
API_AUTODROP_SETTINGS_URL = "http://api:8000/internal/settings/autodrop"
API_TRADE_EXECUTE_URL = "http://api:8000/internal/trade/execute"
API_ANNOUNCEMENTS_URL = "http://api:8000/internal/announcements/poll"
_autodrop_cache = {"ts": 0.0, "cfg": {}}

API_KEY = os.environ["INTERNAL_API_KEY"]
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "https://capsmons.devlooping.fr")

# ============================================================================
# STATE (mémoire RAM du bot)
# ============================================================================
_last_xp_at: dict[str, float] = {}          # cooldown XP chat par user
_active_until: dict[str, float] = {}        # fenêtre présence (user actif jusqu'à timestamp)
_show_last = {"global": 0.0, "users": {}}   # cooldown overlay show
_pending_trades: dict[str, dict] = {}

_rp_cache = {"ts": 0.0, "rp": {}}           # cache RP (bundle)
_last_drop_announce = {"sig": None}         # anti double annonce drops

# Events
API_EVENT_ACTIVE_URL  = "http://api:8000/internal/event/active"
API_EVENT_START_URL   = "http://api:8000/internal/event/start"
_current_event: dict | None = None          # cache RAM de l'event actif
_golden_hour_gains: dict[str, float] = {}   # bonheur gagné par user pendant Golden Hour
_event_drop_last: float = 0.0               # timestamp du dernier drop de "pluie_etoiles"
# Durée max d'un trade avant expiration automatique (secondes)
TRADE_TIMEOUT = 120


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

def emoji_number(n: int) -> str:
    digits = {
        "0": "0️⃣",
        "1": "1️⃣",
        "2": "2️⃣",
        "3": "3️⃣",
        "4": "4️⃣",
        "5": "5️⃣",
        "6": "6️⃣",
        "7": "7️⃣",
        "8": "8️⃣",
        "9": "9️⃣",
    }
    s = str(int(n))
    return "".join(digits.get(ch, ch) for ch in s)



def rp_format(text: str, **kw) -> str:
    """
    Remplace des placeholders {viewer} {title} {xp} etc.
    """
    out = text
    for k, v in kw.items():
        out = out.replace("{" + k + "}", str(v))
    return out


def _autodrop_get_cfg() -> dict:
    now = time.time()
    if now - _autodrop_cache["ts"] < 30:
        return _autodrop_cache["cfg"]

    try:
        r = requests.get(API_AUTODROP_SETTINGS_URL, headers={"X-API-Key": API_KEY}, timeout=2)
        if r.status_code == 200:
            cfg = (r.json() or {}).get("settings", {}) or {}
            _autodrop_cache["cfg"] = cfg
            _autodrop_cache["ts"] = now
            return cfg
    except Exception:
        pass

    return _autodrop_cache["cfg"]


def _cfg_bool(cfg: dict, key: str, default: bool = False) -> bool:
    v = str(cfg.get(key, "true" if default else "false")).strip().lower()
    return v in ("true", "1", "yes", "on")


def _cfg_int(cfg: dict, key: str, default: int) -> int:
    try:
        return int(str(cfg.get(key, default)).strip())
    except Exception:
        return int(default)


def _cfg_str(cfg: dict, key: str, default: str = "") -> str:
    return str(cfg.get(key, default) or default).strip()


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


def lineage_label(lk: str | None) -> str:
    if not lk:
        return "Inconnue"
    return {
        "biolab": "Biolab",
        "securite": "Sécurité",
        "extraction": "Extraction",
        "limited": "Limited",
    }.get(lk, lk)


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
    # Commande: !inv
    # ------------------------------------------------------------------------
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
    # Commande: !cmlist (legacy / debug)
    # ------------------------------------------------------------------------
    @commands.command(name="cmlist")
    async def cmlist(self, ctx: commands.Context):
        login = ctx.author.name.lower()

        try:
            r = requests.get(
                f"{API_COMPANIONS_LIST_URL}/{login}",
                headers={"X-API-Key": API_KEY},
                timeout=3,
            )
            if r.status_code != 200:
                await ctx.send(f"@{ctx.author.name} ⚠️ Liste CM indisponible.")
                return
            data = r.json()
        except Exception:
            await ctx.send(f"@{ctx.author.name} ⚠️ Liste CM indisponible.")
            return

        cms = data.get("companions", []) or []
        if not cms:
            await ctx.send(f"@{ctx.author.name} 👾 Tu n’as aucun CapsMon pour l’instant.")
            return

        parts = []
        for it in cms[:12]:
            star = "★" if it.get("is_active") else "·"
            cid = int(it.get("id", 0))
            cm_key = str(it.get("cm_key", ""))
            cm_name = str(it.get("cm_name", cm_key))
            lk = it.get("lineage_key", None)

            if cm_key == "egg":
                label = f"🥚 Œuf {lineage_label(lk)}"
            else:
                label = cm_name

            parts.append(f"{star} [ID : {cid}] {label} — S{int(it.get('stage',0))} • {int(it.get('xp_total',0))}xp")

        await ctx.send(f"@{ctx.author.name} 👾 Tes CapsMons: " + " | ".join(parts) + " — !setcm <id>")

    def _lineage_label(self, lk: str | None) -> str:
        lk = (lk or "").strip().lower()
        return {
            "biolab": "Biolab",
            "securite": "Sécurité",
            "extraction": "Extraction",
            "limited": "Limited",
            "egg": "Tech",
        }.get(lk, lk or "—")

    def _short_stage(self, s: int | None) -> str:
        try:
            s = int(s or 0)
        except Exception:
            s = 0
        return f"S{s}"

        # ------------------------------------------------------------------------
    # Helper : résoudre un numéro de CM -> (creature_id, label)
    # ------------------------------------------------------------------------
    async def _resolve_cm_number(self, login: str, num: int):
        """Retourne (creature_id, label) pour le numéro num (1-based) dans
        la collection de login, ou None si introuvable."""
        try:
            r = requests.get(
                f"{API_COLLECTION_URL}/{login}",
                headers={"X-API-Key": API_KEY},
                timeout=3,
            )
            if r.status_code != 200:
                return None
            items = (r.json() or {}).get("items", []) or []
            if num < 1 or num > len(items):
                return None
            it = items[num - 1]
            cid = int(it.get("id", 0))
            cm_key = (it.get("cm_key") or "").strip().lower()
            if cm_key == "egg":
                lk = self._lineage_label(it.get("lineage_key"))
                label = f"Oeuf {lk}"
            else:
                label = (it.get("cm_name") or cm_key).strip()
            stage = self._short_stage(it.get("stage", 0))
            xp = int(it.get("xp_total", 0) or 0)
            return cid, f"{label} {stage} {xp}xp"
        except Exception:
            return None

    # ------------------------------------------------------------------------
    # Helper : nettoyer les trades expirés
    # ------------------------------------------------------------------------
    def _cleanup_trades(self):
        now = time.time()
        expired = [k for k, v in _pending_trades.items() if v["expires_at"] < now]
        for k in expired:
            del _pending_trades[k]

    # ------------------------------------------------------------------------
    # Commande : !trade @cible <numéro>
    # ------------------------------------------------------------------------
    @commands.command(name="trade")
    async def trade_cmd(self, ctx: commands.Context):
        """Initie une proposition d'échange de CapsMöns."""
        self._cleanup_trades()
        login = ctx.author.name.lower()
        parts = (ctx.message.content or "").strip().split()

        # Usage : !trade @pseudo numéro
        if len(parts) < 3:
            await ctx.send(
                f"@{ctx.author.name} ℹ️ Usage: !trade @pseudo <n°>  "
                f"(ex: !trade @bob 2 pour proposer ton CapsMön n°2)"
            )
            return

        target_login = parts[1].lstrip("@").strip().lower()
        if target_login == login:
            await ctx.send(f"@{ctx.author.name} ⛔ Tu ne peux pas t'échanger avec toi-même.")
            return

        try:
            num = int(parts[2])
        except ValueError:
            await ctx.send(f"@{ctx.author.name} ⛔ Le numéro doit être un entier. Ex: !trade @bob 2")
            return

        # Vérifier si l'initiateur a déjà un trade en cours
        if login in _pending_trades:
            await ctx.send(
                f"@{ctx.author.name} ⛔ Tu as déjà un échange en cours. "
                f"Fais !tno pour l'annuler."
            )
            return

        # Vérifier si la cible est déjà impliquée dans un trade
        for t in _pending_trades.values():
            if t["target_login"] == target_login or t["initiator_login"] == target_login:
                await ctx.send(
                    f"@{ctx.author.name} ⛔ @{target_login} est déjà dans un échange en cours."
                )
                return

        # Résoudre le numéro vers un creature_id
        result = await self._resolve_cm_number(login, num)
        if result is None:
            await ctx.send(
                f"@{ctx.author.name} ⛔ CapsMön n°{num} introuvable dans ta collection. "
                f"Fais !companion pour voir ta liste."
            )
            return
        cid, label = result

        # Enregistrer le trade en attente
        _pending_trades[login] = {
            "step":            "waiting_answer",
            "initiator_login": login,
            "initiator_cid":   cid,
            "initiator_cm":    label,
            "target_login":    target_login,
            "target_cid":      None,
            "target_cm":       None,
            "ini_confirmed":   False,
            "tgt_confirmed":   False,
            "expires_at":      time.time() + TRADE_TIMEOUT,
        }

        await ctx.send(
            f"@{target_login} 🔀 @{ctx.author.name} te propose d'échanger son "
            f"[{label}] contre un de tes CapsMöns ! "
            f"Réponds avec !answer <n°> pour choisir lequel proposer. "
            f"(Vous avez {TRADE_TIMEOUT}s)"
        )

    # ------------------------------------------------------------------------
    # Commande : !answer <numéro>  (réponse de la cible)
    # ------------------------------------------------------------------------
    @commands.command(name="answer")
    async def answer_cmd(self, ctx: commands.Context):
        """La cible désigne son CapsMön pour l'échange."""
        self._cleanup_trades()
        login = ctx.author.name.lower()
        parts = (ctx.message.content or "").strip().split()

        # Trouver le trade qui attend une réponse de ce viewer
        trade = None
        ini_login = None
        for k, t in _pending_trades.items():
            if t["target_login"] == login and t["step"] == "waiting_answer":
                trade = t
                ini_login = k
                break

        if trade is None:
            await ctx.send(f"@{ctx.author.name} ℹ️ Aucune proposition d'échange en attente pour toi.")
            return

        if len(parts) < 2:
            await ctx.send(
                f"@{ctx.author.name} ℹ️ Usage: !answer <n°>  "
                f"(ex: !answer 1 pour proposer ton CapsMön n°1)"
            )
            return

        try:
            num = int(parts[1])
        except ValueError:
            await ctx.send(f"@{ctx.author.name} ⛔ Le numéro doit être un entier. Ex: !answer 1")
            return

        result = await self._resolve_cm_number(login, num)
        if result is None:
            await ctx.send(
                f"@{ctx.author.name} ⛔ CapsMön n°{num} introuvable dans ta collection. "
                f"Fais !companion pour voir ta liste."
            )
            return
        cid, label = result

        # Mettre à jour le trade
        trade["target_cid"] = cid
        trade["target_cm"]  = label
        trade["step"]       = "waiting_confirm"

        await ctx.send(
            f"@{ctx.author.name} propose [{label}] en échange de "
            f"[{trade['initiator_cm']}] de @{trade['initiator_login']} ! "
            f"@{trade['initiator_login']} @{ctx.author.name} "
            f"Faites !tyes pour accepter ou !tno pour annuler. "
            f"Il faut les deux !tyes pour que l'échange se fasse."
        )

    # ------------------------------------------------------------------------
    # Commande : !tyes  — confirmer l'échange
    # ------------------------------------------------------------------------
    @commands.command(name="tyes")
    async def tyes_cmd(self, ctx: commands.Context):
        """Confirme l'échange en cours."""
        self._cleanup_trades()
        login = ctx.author.name.lower()

        # Trouver le trade en phase waiting_confirm qui concerne ce viewer
        trade = None
        ini_login = None
        for k, t in _pending_trades.items():
            if t["step"] == "waiting_confirm":
                if t["initiator_login"] == login or t["target_login"] == login:
                    trade = t
                    ini_login = k
                    break

        if trade is None:
            await ctx.send(f"@{ctx.author.name} ℹ️ Aucun échange en attente de confirmation pour toi.")
            return

        # Enregistrer la confirmation
        if login == trade["initiator_login"]:
            trade["ini_confirmed"] = True
        else:
            trade["tgt_confirmed"] = True

        # Si les deux ont confirmé : exécuter le swap
        if trade["ini_confirmed"] and trade["tgt_confirmed"]:
            del _pending_trades[ini_login]
            try:
                r = requests.post(
                    API_TRADE_EXECUTE_URL,
                    headers={"X-API-Key": API_KEY},
                    json={
                        "initiator_login":       trade["initiator_login"],
                        "initiator_creature_id": trade["initiator_cid"],
                        "target_login":          trade["target_login"],
                        "target_creature_id":    trade["target_cid"],
                    },
                    timeout=5,
                )
                if r.status_code == 200:
                    await ctx.send(
                        f"✅ Échange réalisé ! "
                        f"@{trade['initiator_login']} reçoit [{trade['target_cm']}] "
                        f"et @{trade['target_login']} reçoit [{trade['initiator_cm']}]. "
                        f"Faites !companion pour voir votre nouvelle collection !"
                    )
                else:
                    detail = ""
                    try:
                        detail = r.json().get("detail", "")
                    except Exception:
                        pass
                    await ctx.send(
                        f"⛔ L'échange a échoué : {detail or r.status_code}. "
                        f"Réessayez avec !trade."
                    )
            except Exception as e:
                print(f"[BOT] trade execute error: {e}", flush=True)
                await ctx.send(f"⛔ Erreur réseau lors de l'échange. Réessayez.")
            return

        # Un seul a confirmé pour l'instant
        other = trade["target_login"] if login == trade["initiator_login"] else trade["initiator_login"]
        await ctx.send(
            f"@{ctx.author.name} ✅ Confirmation enregistrée. "
            f"En attente de @{other}…  (!tyes pour accepter / !tno pour annuler)"
        )

    # ------------------------------------------------------------------------
    # Commande : !tno  — annuler l'échange
    # ------------------------------------------------------------------------
    @commands.command(name="tno")
    async def tno_cmd(self, ctx: commands.Context):
        """Annule l'échange en cours."""
        self._cleanup_trades()
        login = ctx.author.name.lower()

        # Chercher tout trade (quelle que soit l'étape) impliquant ce viewer
        trade = None
        ini_login = None
        for k, t in _pending_trades.items():
            if t["initiator_login"] == login or t["target_login"] == login:
                trade = t
                ini_login = k
                break

        if trade is None:
            await ctx.send(f"@{ctx.author.name} ℹ️ Aucun échange en cours pour toi.")
            return

        del _pending_trades[ini_login]
        other = (
            trade["target_login"]
            if login == trade["initiator_login"]
            else trade["initiator_login"]
        )
        await ctx.send(
            f"🚫 @{ctx.author.name} a annulé l'échange. "
            f"@{other} L'échange est annulé, vous pouvez en relancer un avec !trade."
        )

    # ------------------------------------------------------------------------
    # Collection (helper interne) -> affiche une liste numérotée 1..N
    # ------------------------------------------------------------------------
    async def collection(self, ctx: commands.Context):
        login = ctx.author.name.lower()

        try:
            r = requests.get(
                f"{API_COLLECTION_URL}/{login}",
                headers={"X-API-Key": API_KEY},
                timeout=3,
            )
            if r.status_code != 200:
                await ctx.send(f"@{ctx.author.name} ⚠️ Collection indisponible.")
                return
            data = r.json()
        except Exception:
            await ctx.send(f"@{ctx.author.name} ⚠️ Collection indisponible.")
            return

        items = data.get("items", []) or []
        if not items:
            await ctx.send(f"@{ctx.author.name} 👾 Tu n’as aucun CapsMon pour l’instant.")
            return

        # Trouver l'actif
        active = None
        for it in items:
            if bool(it.get("is_active")):
                active = it
                break

        # Numéro de l'actif (1..)
        active_num = None
        if active:
            try:
                active_id = int(active.get("id"))
                for i, it in enumerate(items, start=1):
                    if int(it.get("id") or 0) == active_id:
                        active_num = i
                        break
            except Exception:
                active_num = None

        # Construire une liste user-friendly numérotée
        parts = []
        for idx, it in enumerate(items[:12], start=1):
            cm_key = (it.get("cm_key") or "").strip().lower()
            cm_name = (it.get("cm_name") or cm_key).strip()

            lk = self._lineage_label(it.get("lineage_key"))
            stage = self._short_stage(it.get("stage", 0))
            xp = int(it.get("xp_total", 0) or 0)

            star = "★" if bool(it.get("is_active")) else "·"

            if cm_key == "egg":
                label = f"🥚 Œuf {lk}"
            else:
                label = f"👾 {cm_name}"

            num = emoji_number(idx)
            parts.append(f"{star}{num} {label} {stage} {xp}xp")


        if active:
            a_key = (active.get("cm_key") or "").strip().lower()
            a_name = (active.get("cm_name") or a_key).strip()
            a_lk = self._lineage_label(active.get("lineage_key"))

            active_label = f"🥚 Œuf {a_lk}" if a_key == "egg" else f"👾 {a_name}"
            num_txt = f"{emoji_number(active_num)} " if active_num is not None else ""
            await ctx.send(
                f"@{ctx.author.name} ⭐ Actif: {num_txt}{active_label}  | Toute ta collection: " + " | ".join(parts)
            )
        else:
            await ctx.send(f"@{ctx.author.name} 📦 Aucun compagnon actif  |  Collection: " + " | ".join(parts))

    # ------------------------------------------------------------------------
    # Commande: !companion -> utilise un numéro 1..N (plus simple)
    # ------------------------------------------------------------------------
    @commands.command(name="companion")
    async def companion(self, ctx: commands.Context):
        login = ctx.author.name.lower()
        parts = (ctx.message.content or "").strip().split()

        # Sans argument => affiche la collection + rappel usage
        if len(parts) < 2:
            await self.collection(ctx)
            await ctx.send(f"@{ctx.author.name} ℹ️ Usage: !companion <numéro>  (ex: !companion 2)")
            return

        target = parts[1].strip().lower()
        if not target:
            await ctx.send(f"@{ctx.author.name} ℹ️ Usage: !companion <numéro> (ex: !companion 2)")
            return

        # Si numérique => numéro (1..)
        pick_index = None
        try:
            pick_index = int(target)
        except Exception:
            pick_index = None

        try:
            if pick_index is not None:
                if pick_index <= 0:
                    await ctx.send(f"@{ctx.author.name} ⛔ Numéro invalide. Exemple: !companion 1")
                    return

                rr = requests.get(
                    f"{API_COLLECTION_URL}/{login}",
                    headers={"X-API-Key": API_KEY},
                    timeout=3,
                )
                if rr.status_code != 200:
                    await ctx.send(f"@{ctx.author.name} ⚠️ Collection indisponible.")
                    return
                col = rr.json() or {}
                items = col.get("items", []) or []
                if not items:
                    await ctx.send(f"@{ctx.author.name} 👾 Tu n’as aucun CapsMon pour l’instant.")
                    return

                if pick_index > len(items):
                    await ctx.send(f"@{ctx.author.name} ⛔ Tu n'as que {len(items)} CapsMons. Exemple: !companion 1")
                    return

                creature_id = items[pick_index - 1].get("id", None)
                if creature_id is None:
                    await ctx.send(f"@{ctx.author.name} ⛔ Impossible (id manquant).")
                    return

                r = requests.post(
                    API_COMPANION_SET_BY_ID_URL,
                    headers={"X-API-Key": API_KEY},
                    json={"twitch_login": login, "creature_id": int(creature_id)},
                    timeout=3,
                )
            else:
                # Fallback legacy: cm_key
                r = requests.post(
                    API_COMPANION_SET_URL,
                    headers={"X-API-Key": API_KEY},
                    json={"twitch_login": login, "cm_key": target},
                    timeout=3,
                )

            if r.status_code != 200:
                msg = "⛔ Impossible."
                try:
                    d = r.json()
                    detail = str(d.get("detail", "")).strip()
                    if detail:
                        msg = f"⛔ {detail}"
                except Exception:
                    pass

                await ctx.send(f"@{ctx.author.name} {msg} (Astuce: fais !companion pour voir les numéros)")
                return

            data = r.json()

        except Exception as e:
            print("[BOT] companion set error:", e, flush=True)
            await ctx.send(f"@{ctx.author.name} ⚠️ Impossible de changer de compagnon.")
            return

        # 2) relire la collection pour afficher un résultat fiable + numéro
        active_id = data.get("active_id", None)
        try:
            rr = requests.get(
                f"{API_COLLECTION_URL}/{login}",
                headers={"X-API-Key": API_KEY},
                timeout=3,
            )
            if rr.status_code != 200:
                await ctx.send(f"@{ctx.author.name} ⭐ Compagnon actif mis à jour ✅")
                return

            col = rr.json() or {}
            items = col.get("items", []) or []

            active_item = None
            for it in items:
                if it.get("is_active"):
                    active_item = it
                    break

            if not active_item and active_id is not None:
                for it in items:
                    try:
                        if int(it.get("id") or 0) == int(active_id):
                            active_item = it
                            break
                    except Exception:
                        pass

            if not active_item:
                await ctx.send(f"@{ctx.author.name} ⭐ Compagnon actif mis à jour ✅")
                return

            # numéro 1..N
            active_num = None
            try:
                aid = int(active_item.get("id") or 0)
                for i, it in enumerate(items, start=1):
                    if int(it.get("id") or 0) == aid:
                        active_num = i
                        break
            except Exception:
                active_num = None

            cm_key = (active_item.get("cm_key") or "").strip().lower()
            cm_name = (active_item.get("cm_name") or cm_key).strip()
            lineage_key = (active_item.get("lineage_key") or "").strip().lower()

            if cm_key == "egg":
                lk_label = {
                    "biolab": "Biolab",
                    "securite": "Sécurité",
                    "extraction": "Extraction",
                    "limited": "Limited",
                }.get(lineage_key, lineage_key or "Inconnue")
                label = f"🥚 Œuf {lk_label}"
            else:
                label = f"👾 {cm_name}"

            num_txt = f"{active_num}" if active_num is not None else "?"
            await ctx.send(f"@{ctx.author.name} ⭐ Compagnon actif : {label} (n°{num_txt}) (c’est lui qui gagne l’XP)")

        except Exception as e:
            print("[BOT] companion refresh collection error:", e, flush=True)
            await ctx.send(f"@{ctx.author.name} ⭐ Compagnon actif mis à jour ✅")

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
        # Annonces bot
        self.loop.create_task(self.announcements_loop())
        # Events
        self.loop.create_task(self.event_loop())
        self.loop.create_task(self.event_sync_loop())
        self.loop.create_task(self.event_drop_loop())

    # ------------------------------------------------------------------------
    # Commande !drop
    # ------------------------------------------------------------------------
    @commands.command(name="drop")
    async def drop_cmd(self, ctx: commands.Context):
        # Mod / streamer only
        if not self._is_mod_or_broadcaster(ctx):
            return

        raw = (ctx.message.content or "").strip()
        parts = raw.split()

        if len(parts) < 4:
            await ctx.send('Usage: !drop first|random|coop "Titre" URL [durée] [xp] [ticket_key] [qty] [target]')
            return

        mode = (parts[1] or "").lower()
        if mode not in ("first", "random", "coop"):
            await ctx.send("Modes: first, random, coop")
            return

        # --- Parse titre + reste (supporte "Titre avec espaces")
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

        if not title:
            # fallback: 1 mot de titre
            title = parts[2]
            rest = parts[3:]

        if not rest:
            await ctx.send('Usage: !drop first|random|coop "Titre" URL [durée] [xp] [ticket_key] [qty] [target]')
            return

        media_url = rest[0]

        def _to_int(s: str, default: int) -> int:
            try:
                return int(s)
            except Exception:
                return default

        duration = _to_int(rest[1], 10) if len(rest) >= 2 else 10
        xp_bonus = _to_int(rest[2], 50) if len(rest) >= 3 else 50
        ticket_key = rest[3].strip().lower() if len(rest) >= 4 else "ticket_basic"
        ticket_qty = _to_int(rest[4], 1) if len(rest) >= 5 else 1

        payload = {
            "mode": mode,
            "title": title,
            "media_url": media_url,
            "duration_seconds": max(3, min(duration, 60)),
            "xp_bonus": max(0, min(xp_bonus, 100)),
            "ticket_key": ticket_key,
            "ticket_qty": max(1, min(ticket_qty, 999)),
        }

        if mode == "coop":
            target = _to_int(rest[5], 10) if len(rest) >= 6 else 10
            payload["target_hits"] = max(1, min(target, 999))

        # Spawn
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

        # Message RP (seulement pour first/random — en COOP, main.py insère dans bot_announcements)
        if mode != "coop":
            rp_key = f"drop.spawn.{mode}"
            line = await rp_get(rp_key) or '✨ Drop lancé : {title} — tape "!grab" pour participer'
            msg = rp_format(
                line,
                title=title,
                xp=payload["xp_bonus"],
                ticket_key=payload["ticket_key"],
                ticket_qty=payload["ticket_qty"],
            )
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

        # Golden Hour : +1% bonheur par message, max 5% cumulé
        global _current_event, _golden_hour_gains
        if _current_event and _current_event.get("key") == "golden_hour":
            gained = _golden_hour_gains.get(login, 0.0)
            if gained < 5.0:
                bonus_h = min(1.0, 5.0 - gained)
                _golden_hour_gains[login] = gained + bonus_h
                try:
                    requests.post(
                        "http://api:8000/internal/happiness/add_one",
                        headers={"X-API-Key": API_KEY},
                        json={"twitch_login": login, "amount": int(bonus_h)},
                        timeout=2,
                    )
                except Exception:
                    pass

        if now - last >= cooldown:
            _last_xp_at[login] = now

            resp = None
            try:
                resp = requests.post(
                    API_XP_URL,
                    headers={"X-API-Key": API_KEY},
                    json={"twitch_login": login, "amount": 1, "passive": True},
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
                        after  = int(data.get("stage_after",  before))

                        if after > before:
                            intro      = await rp_get("evolve.announce") or "✨ Évolution !"
                            old_fname  = (data.get("old_form_name") or "").strip()
                            new_fname  = (data.get("new_form_name") or "").strip()
                            cm_assigned = data.get("cm_assigned") or ""

                            if old_fname and new_fname:
                                msg = f"{intro} @{message.author.name} — {old_fname} ➜ {new_fname} !"
                            elif new_fname:
                                msg = f"{intro} @{message.author.name} — {new_fname} est né !"
                            else:
                                msg = f"{intro} @{message.author.name} {stage_label(before)} ➜ {stage_label(after)}"

                            if cm_assigned and not new_fname:
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
        try:
            if getattr(ctx.author, "is_broadcaster", False):
                return True
            if getattr(ctx.author, "is_mod", False):
                return True
        except Exception:
            pass
        return False

    # ------------------------------------------------------------------------
    # Commande !spawn (legacy)
    # ------------------------------------------------------------------------
    @commands.command(name="spawn")
    async def spawn(self, ctx: commands.Context):
        if not self._is_mod_or_broadcaster(ctx):
            return

        parts = ctx.message.content.strip().split()
        if len(parts) < 4:
            await ctx.send('Usage: !spawn first|random "Titre" URL [durée] [xp] [ticket_key] [qty]')
            return

        mode = parts[1].lower()
        if mode not in ("first", "random", "coop"):
            await ctx.send("Modes: first, random, coop")
            return

        raw = ctx.message.content.strip()

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

        if mode == "coop":
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
            await ctx.send(f"@{ctx.author.name} usage: !use <item_key> [count]")
            return

        item_key = parts[1].strip().lower()

        # Optionnel : nombre d'utilisations
        count = 1
        if len(parts) >= 3:
            try:
                count = int(parts[2])
            except ValueError:
                await ctx.send(f"@{ctx.author.name} ⛔ Le nombre doit être un entier. Ex: !use grande_capsule 3")
                return
            if count < 1:
                await ctx.send(f"@{ctx.author.name} ⛔ Le nombre doit être au moins 1.")
                return
            if count > 20:
                await ctx.send(f"@{ctx.author.name} ⛔ Maximum 20 items à la fois.")
                return

        total_xp = 0
        total_happiness = 0
        last_data = None
        used_count = 0

        for i in range(count):
            try:
                r = requests.post(
                    "http://api:8000/internal/item/use",
                    headers={"X-API-Key": API_KEY},
                    json={"twitch_login": login, "item_key": item_key},
                    timeout=2,
                )

                if r.status_code != 200:
                    err_msg = "⛔ Objet indisponible."
                    try:
                        d = r.json()
                        detail = str(d.get("detail", "")).lower()
                        if "unknown item" in detail:
                            err_msg = "⛔ Objet inconnu."
                        elif "no item" in detail:
                            if i == 0:
                                err_msg = "⛔ Tu n'en as pas dans ton inventaire."
                            else:
                                err_msg = None  # on résumera ce qui a été fait
                    except Exception:
                        pass

                    if i == 0:
                        await ctx.send(f"@{ctx.author.name} {err_msg}")
                        return
                    else:
                        # Stock épuisé en cours de route — on sort et on résume
                        break

                data = r.json()
                last_data = data
                used_count += 1
                effect = data.get("effect")

                if effect == "xp":
                    total_xp += int(data.get("xp_gain", data.get("amount", 0)))
                elif effect == "happiness":
                    total_happiness += int(data.get("happiness_gain", data.get("amount", 0)))
                elif effect == "egg":
                    # Les œufs créent un CM : on traite un par un
                    lk = data.get("lineage_key", "?")
                    activated = bool(data.get("activated", False))
                    if activated:
                        await ctx.send(f"@{ctx.author.name} 🥚 Œuf récupéré ({lk}) ! Il devient ton compagnon actif ✅")
                    else:
                        await ctx.send(f"@{ctx.author.name} 🥚 Œuf récupéré ({lk}) ! Ajouté à ta collection ✅")
                    return
                elif effect == "card_frame":
                    frame_key = data.get("card_frame", item_key)
                    _FRAME_LABELS = {
                        'card_frame_gold':   ('🟡', 'Cadre Doré'),
                        'card_frame_fire':   ('🔥', 'Cadre Feu'),
                        'card_frame_void':   ('🟣', 'Cadre Void'),
                        'card_frame_ice':    ('❄️', 'Cadre Glace'),
                        'card_frame_nature': ('🌿', 'Cadre Nature'),
                        'card_frame_shadow': ('🌑', 'Cadre Ombre'),
                        'card_frame_glitch': ('⚡', 'Cadre Glitch'),
                    }
                    emoji, label = _FRAME_LABELS.get(frame_key, ("🖼️", frame_key))
                    await ctx.send(f"@{ctx.author.name} {emoji} {label} appliqué à ton CM ! Fais !show pour voir le résultat ✨")
                    return

            except Exception as e:
                print("[BOT] use error:", e, flush=True)
                await ctx.send(f"@{ctx.author.name} ⚠️ Erreur objet.")
                return

        if last_data is None:
            return

        effect = last_data.get("effect")
        stock_warning = f" (stock insuffisant : {used_count}/{count})" if used_count < count else ""

        if effect == "xp":
            final_xp = int(last_data.get("xp_total", 0))
            stage_before = int(last_data.get("stage_before", 0))
            stage_after = int(last_data.get("stage_after", 0))
            suffix = f" ×{used_count}" if used_count > 1 else ""
            evo_msg = f" ✨ Évolution ! Stage {stage_after} !" if stage_after > stage_before else ""
            await ctx.send(
                f"@{ctx.author.name} 💊 Capsule{suffix} consommée !"
                f" +{total_xp} XP ⚡ (total : {final_xp} XP){evo_msg}{stock_warning}"
            )
            return

        if effect == "happiness":
            after_h = int(last_data.get("happiness_after", 0))
            name = last_data.get("item_name", item_key)
            suffix = f" ×{used_count}" if used_count > 1 else ""
            await ctx.send(
                f"@{ctx.author.name} 🥰 {name}{suffix} utilisé !"
                f" +{total_happiness} bonheur (❤️ {after_h}%){stock_warning}."
            )
            return

        await ctx.send(f"@{ctx.author.name} ✔️ Objet utilisé{stock_warning}.")

    # ------------------------------------------------------------------------
    # Annonces générales (channel points, drops, etc.)
    # ------------------------------------------------------------------------
    async def announcements_loop(self):
        await asyncio.sleep(3)
    
        while True:
            await asyncio.sleep(2)
    
            try:
                r = requests.get(
                    API_ANNOUNCEMENTS_URL,
                    headers={"X-API-Key": API_KEY},
                    timeout=2,
                )
                if r.status_code != 200:
                    continue
    
                data = r.json()
                messages = data.get("messages", []) or []
    
                if not messages:
                    continue
    
                chan = self.get_channel(os.environ["TWITCH_CHANNEL"])
                if not chan:
                    continue
    
                for msg in messages:
                    if msg and str(msg).strip():
                        await chan.send(str(msg).strip())
                        await asyncio.sleep(1)  # petite pause entre chaque message
    
            except Exception as e:
                print("[BOT] announcements_loop error:", e, flush=True)

    # ------------------------------------------------------------------------
    # Happiness decay loop
    # ------------------------------------------------------------------------
    async def happiness_decay_loop(self):
        await asyncio.sleep(60)

        while True:
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
        env_enabled = os.environ.get("AUTO_DROP_ENABLED", "false").lower() in ("1", "true", "yes", "on")

        while True:
            cfg = _autodrop_get_cfg()

            enabled = _cfg_bool(cfg, "auto_drop_enabled", env_enabled)
            if not enabled:
                await asyncio.sleep(5)
                continue

            min_s = _cfg_int(cfg, "auto_drop_min_seconds", int(os.environ.get("AUTO_DROP_MIN_SECONDS", "900")))
            max_s = _cfg_int(cfg, "auto_drop_max_seconds", int(os.environ.get("AUTO_DROP_MAX_SECONDS", "1500")))
            min_s = max(60, min_s)
            max_s = max(min_s, max_s)

            dmin = _cfg_int(cfg, "auto_drop_duration_min_seconds", int(os.environ.get("AUTO_DROP_DURATION_MIN_SECONDS", os.environ.get("AUTO_DROP_DURATION_SECONDS", "40"))))
            dmax = _cfg_int(cfg, "auto_drop_duration_max_seconds", int(os.environ.get("AUTO_DROP_DURATION_MAX_SECONDS", os.environ.get("AUTO_DROP_DURATION_SECONDS", "40"))))
            dmin = max(5, dmin)
            dmax = max(dmin, dmax)

            pick_kind = _cfg_str(cfg, "auto_drop_pick_kind", os.environ.get("AUTO_DROP_PICK_KIND", "any")).lower()
            if pick_kind not in ("any", "xp", "candy"):
                pick_kind = "any"

            mode = _cfg_str(cfg, "auto_drop_mode", os.environ.get("AUTO_DROP_MODE", "random")).lower()
            if mode not in ("first", "random", "coop"):
                mode = "random"

            ticket_qty = _cfg_int(cfg, "auto_drop_ticket_qty", int(os.environ.get("AUTO_DROP_TICKET_QTY", "1")))
            ticket_qty = max(1, min(ticket_qty, 50))

            fallback_media = _cfg_str(cfg, "auto_drop_fallback_media_url", os.environ.get("AUTO_DROP_MEDIA_URL", "")).strip()

            await asyncio.sleep(random.randint(min_s, max_s))

            try:
                r = requests.get(API_LIVE_URL, headers={"X-API-Key": API_KEY}, timeout=2)
                if r.status_code != 200 or not r.json().get("is_live", False):
                    continue
            except Exception:
                continue

            try:
                s = requests.get("http://api:8000/overlay/drop_state", timeout=2)
                if s.status_code == 200 and s.json().get("show", False):
                    continue
            except Exception:
                continue

            try:
                pr = requests.get(
                    f"{API_ITEM_PICK_URL}?kind={pick_kind}",
                    headers={"X-API-Key": API_KEY},
                    timeout=2,
                )
                if pr.status_code != 200:
                    print("[BOT] items/pick failed:", pr.status_code, (pr.text or "")[:200], flush=True)
                    continue
                picked = pr.json() or {}
            except Exception as e:
                print("[BOT] items/pick error:", e, flush=True)
                continue

            item_key = (picked.get("item_key") or "").strip().lower()
            item_name = (picked.get("item_name") or item_key).strip()
            icon_url = (picked.get("icon_url") or "").strip()

            if not item_key:
                print("[BOT] items/pick returned empty item_key:", picked, flush=True)
                continue

            if not icon_url:
                if not fallback_media:
                    print("[BOT] picked item has no icon_url and no fallback_media:", item_key, flush=True)
                    continue
                icon_url = fallback_media

            duration = random.randint(dmin, dmax)

            payload = {
                "mode": mode,
                "title": item_name,
                "media_url": icon_url,
                "duration_seconds": duration,
                "xp_bonus": 0,
                "ticket_key": item_key,
                "ticket_qty": ticket_qty,
            }

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

            # Message RP (seulement pour first/random — en COOP, main.py insère dans bot_announcements)
            if payload["mode"] != "coop":
                try:
                    chan = self.get_channel(os.environ["TWITCH_CHANNEL"])
                    if chan:
                        rp_key = f"drop.spawn.{payload['mode']}"
                        line = await rp_get(rp_key) or "✨ Drop automatique : {title}. Attrape le avec !grab."
                        msg = rp_format(
                            line,
                            title=payload["title"],
                            xp=payload["xp_bonus"],
                            ticket_key=payload["ticket_key"],
                            ticket_qty=payload["ticket_qty"],
                        )
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

            try:
                r = requests.get(API_LIVE_URL, headers={"X-API-Key": API_KEY}, timeout=2)
                if r.status_code != 200 or not r.json().get("is_live", False):
                    continue
            except Exception as e:
                print("[BOT] is_live check error:", e, flush=True)
                continue

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
    # ------------------------------------------------------------------------
    # Event loop (timer 30min + chance)
    # ------------------------------------------------------------------------
    async def event_loop(self):
        """Toutes les 30 min : chance de déclencher un event aléatoire."""
        global _current_event, _golden_hour_gains, _event_drop_last
        await asyncio.sleep(10)

        while True:
            await asyncio.sleep(1800)  # 30 minutes

            try:
                r = requests.get(API_LIVE_URL, headers={"X-API-Key": API_KEY}, timeout=2)
                if r.status_code != 200 or not r.json().get("is_live", False):
                    continue
            except Exception:
                continue

            # Vérifier si un event est déjà actif
            try:
                r = requests.get(API_EVENT_ACTIVE_URL, headers={"X-API-Key": API_KEY}, timeout=2)
                if r.ok and r.json().get("active"):
                    continue  # event déjà en cours
            except Exception:
                continue

            # Récupérer la chance configurée (défaut 30%)
            chance_pct = 30
            try:
                import requests as _req
                rc = _req.get(
                    "http://api:8000/admin/events/json",
                    auth=(os.environ.get("ADMIN_USER","admin"), os.environ.get("ADMIN_PASS","")),
                    timeout=2,
                )
                if rc.ok:
                    chance_pct = int(rc.json().get("config", {}).get("event_chance_pct", 30) or 30)
            except Exception:
                pass

            if random.randint(1, 100) > chance_pct:
                continue  # pas de chance cette fois

            # Choisir un event aléatoire
            import random as _rand
            event_keys = ["vent_ouest","pluie_etoiles","golden_hour","douce_chaleur","douce_brise","pluie_sucree"]
            chosen = _rand.choice(event_keys)

            try:
                r = requests.post(
                    API_EVENT_START_URL,
                    headers={"X-API-Key": API_KEY},
                    json={"event_key": chosen, "triggered_by": "auto"},
                    timeout=3,
                )
                if r.ok:
                    data = r.json()
                    # Si c'est un drop spécial, le lancer immédiatement
                    await self._maybe_launch_event_drop(chosen)
                    # Reset Golden Hour gains
                    if chosen == "golden_hour":
                        _golden_hour_gains = {}
                    # Mettre à jour cache RAM
                    _current_event = {"key": chosen}
                    _event_drop_last = time.time()
            except Exception as e:
                print("[BOT] event_loop error:", e, flush=True)

    async def event_sync_loop(self):
        """Synchronise le cache RAM de l'event actif depuis l'API toutes les 30 secondes."""
        global _current_event, _golden_hour_gains, _event_drop_last
        await asyncio.sleep(5)
        while True:
            await asyncio.sleep(5)
            try:
                r = requests.get(
                    "http://api:8000/internal/event/active_and_drop_needed",
                    headers={"X-API-Key": API_KEY}, timeout=2
                )
                if r.ok:
                    data = r.json()
                    prev_key = (_current_event or {}).get("key")
                    new_ev = data.get("event") if data.get("active") else None
                    new_key = (new_ev or {}).get("key")
                
                    if prev_key == "golden_hour" and new_key != "golden_hour":
                        _golden_hour_gains = {}
                
                    _current_event = new_ev
                
                    # Nouveau event détecté ou drop immédiat requis
                    if new_key and (prev_key != new_key or data.get("drop_needed")):
                        if new_key == "pluie_etoiles":
                            _event_drop_last = time.time()
                            await self._maybe_launch_event_drop("pluie_etoiles")
                        elif new_key in ("douce_chaleur", "douce_brise", "pluie_sucree"):
                            if data.get("drop_needed"):
                                await self._maybe_launch_event_drop(new_key)
                        elif new_key == "golden_hour":
                            _golden_hour_gains = {}
            except Exception:
                pass

    async def event_drop_loop(self):
        """Gère la 'Pluie d'Étoiles Filantes' : un drop toutes les 3 minutes."""
        global _current_event, _event_drop_last
        await asyncio.sleep(15)
        while True:
            await asyncio.sleep(30)
            try:
                ev = _current_event
                if not ev or ev.get("key") != "pluie_etoiles":
                    continue
                interval = 180
                try:
                    rc = requests.get(
                        "http://api:8000/admin/settings/json",
                        auth=(os.environ.get("ADMIN_USER","admin"), os.environ.get("ADMIN_PASS","")),
                        timeout=2,
                    )
                    if rc.ok:
                        interval = int(rc.json().get("coop_drop_interval_seconds", 180) or 180)
                except Exception:
                    pass
                if time.time() - _event_drop_last < interval:
                    continue
                _event_drop_last = time.time()
                await self._maybe_launch_event_drop("pluie_etoiles")
            except Exception as e:
                print("[BOT] event_drop_loop error:", e, flush=True)

    async def _maybe_launch_event_drop(self, event_key: str):
        """Lance un drop spécial selon le type d'event."""
        special_events = {"douce_chaleur": "egg", "douce_brise": "double_xp", "pluie_sucree": "happiness"}

        if event_key == "pluie_etoiles":
            # Drop normal aléatoire
            try:
                pr = requests.get(
                    f"http://api:8000/internal/items/pick?kind=any",
                    headers={"X-API-Key": API_KEY}, timeout=2,
                )
                if pr.status_code != 200:
                    return
                picked = pr.json()
                payload = {
                    "mode": "random",
                    "title": picked.get("item_name", "Étoile filante"),
                    "media_url": picked.get("icon_url", ""),
                    "duration_seconds": 60,
                    "xp_bonus": 0,
                    "ticket_key": picked.get("item_key", "ticket_basic"),
                    "ticket_qty": 1,
                }
                if not payload["media_url"]:
                    return
                rr = requests.post(API_DROP_SPAWN_URL, headers={"X-API-Key": API_KEY}, json=payload, timeout=3)
            except Exception as e:
                print("[BOT] pluie_etoiles drop error:", e, flush=True)

        elif event_key in special_events:
            drop_type = special_events[event_key]
            names = {
                "douce_chaleur": "🔥 Douce Chaleur",
                "douce_brise":   "💨 Douce Brise",
                "pluie_sucree":  "🍬 Pluie Sucrée",
            }
            # Durée configurable (défaut 60s)
            duration = 60
            target_hits = 10
            try:
                rc = requests.get(
                    "http://api:8000/admin/settings/json",
                    auth=(os.environ.get("ADMIN_USER","admin"), os.environ.get("ADMIN_PASS","")),
                    timeout=2,
                )
                if rc.ok:
                    cfg = rc.json()
                    duration    = int(cfg.get("coop_drop_duration_seconds", 60) or 60)
                    target_hits = int(cfg.get("event_special_drop_target_hits", 10) or 10)
            except Exception:
                pass
            payload = {
                "mode": "coop",
                "title": names.get(event_key, event_key),
                "media_url": "https://i.imgur.com/placeholder.png",
                "duration_seconds": duration,
                "xp_bonus": 0,
                "ticket_key": f"__event_{drop_type}",  # clé spéciale détectée dans resolve_drop
                "ticket_qty": 1,
                "target_hits": target_hits,
            }
            try:
                # Chercher une icône générique dans les items
                pr = requests.get(
                    f"http://api:8000/internal/items/pick?kind=any",
                    headers={"X-API-Key": API_KEY}, timeout=2,
                )
                if pr.ok:
                    payload["media_url"] = pr.json().get("icon_url", payload["media_url"]) or payload["media_url"]
            except Exception:
                pass

            try:
                rr = requests.post(API_DROP_SPAWN_URL, headers={"X-API-Key": API_KEY}, json=payload, timeout=3)
            except Exception as e:
                print(f"[BOT] {event_key} drop error:", e, flush=True)

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
                msg = rp_format(
                    line,
                    title=title,
                    xp=xp_bonus,
                    ticket_key=ticket_key,
                    ticket_qty=ticket_qty,
                    count=len(winners),
                    viewer="",
                )
            else:
                if mode == "first":
                    rp_key = "drop.win.first"
                    line = await rp_get(rp_key) or "⚡ {viewer} remporte {title} !"
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
                
                elif mode == "random":
                    rp_key = "drop.win.random"
                    line = await rp_get(rp_key) or "🎲 {viewer} remporte {title} !"
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
                
                else:
                    # COOP : l'annonce des gagnants et de l'XP est déjà faite par _announce()
                    # dans resolve_drop côté serveur (via bot_announcements). On ne renvoie pas
                    # un second message ici pour éviter le doublon.
                    msg = None
                
            try:
                if msg:
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
        happy = max(0, min(100, happy))
        happy_bar = "█" * (happy // 10) + "░" * (10 - (happy // 10))

        streak = int(data.get("streak_count", 0))
        streak_txt = f"🔥 Fidélité ManaCorp : {streak} stream(s)" if streak > 0 else "🔥 Fidélité ManaCorp : 0 stream"

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

        await ctx.send(
            f"{header} • {state} • {prog}{extra}{flavor_txt} | ❤️ {happy}% [{happy_bar}] | {streak_txt}"
        )

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
    # Commande: !show
    # ------------------------------------------------------------------------
    @commands.command(name="show")
    async def show(self, ctx: commands.Context):
        now = time.time()
        login = ctx.author.name.lower()

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

        mode = data.get("mode", "")
        joined = bool(data.get("joined", False))
        title = data.get("title", "un objet")

        # En COOP : silence total à la participation, seul le résultat final compte
        if mode == "coop":
            return

        if not data.get("active"):
            late = await rp_get("drop.claim.late") or "⌛ Trop tard…"
            await ctx.send(f"@{ctx.author.name} {late}")
            return

        if not joined:
            ok = await rp_get("drop.claim.ok") or "📡 Déjà enregistré."
            await ctx.send(f"@{ctx.author.name} {ok} ({title})")
            return

        ok = await rp_get("drop.claim.ok") or "📡 Participation validée."
        await ctx.send(f"@{ctx.author.name} {ok} ({title})")

        result = data.get("result")
        if result and result.get("won"):
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
    # Commande: !hit — frappe le boss actif
    # ------------------------------------------------------------------------
    @commands.command(name="hit")
    async def hit(self, ctx: commands.Context):
        login = ctx.author.name.lower()
        try:
            r = requests.post(
                "http://api:8000/internal/boss/hit",
                headers={"X-API-Key": API_KEY},
                json={"twitch_login": login},
                timeout=3,
            )
        except Exception as e:
            print("[BOT] boss hit error:", e, flush=True)
            return

        if r.status_code != 200:
            try:
                detail = r.json().get("detail", "")
            except Exception:
                detail = ""
            if "Déjà frappé" in detail:
                await ctx.send(f"@{ctx.author.name} Tu as déjà frappé ce boss !")
            elif "Aucun boss actif" in detail:
                await ctx.send(f"@{ctx.author.name} Aucun boss actif en ce moment.")
            elif "Oeuf ne peut pas frapper" in detail:
                await ctx.send(f"@{ctx.author.name} Ton œuf ne peut pas encore combattre !")
            elif "Pas de CM actif" in detail:
                await ctx.send(f"@{ctx.author.name} Tu n'as pas de CapsMön actif !")
            return

        data = r.json()
        damage   = data.get("damage", 0)
        hp_after = data.get("hp_after", 0)
        defeated = data.get("defeated", False)
        stage    = data.get("stage", 0)
        stage_lbl = {1: "I", 2: "II", 3: "III"}.get(stage, str(stage))

        if defeated:
            await ctx.send(f"@{ctx.author.name} ⚔️ COUP FINAL ! -{damage} PV (Stage {stage_lbl}) — le boss est vaincu ! 💥")
        else:
            await ctx.send(f"@{ctx.author.name} ⚔️ -{damage} PV (Stage {stage_lbl}) — Boss : {hp_after} PV restants.")

    # ------------------------------------------------------------------------
    # Commande: !commands
    # ------------------------------------------------------------------------
    @commands.command(name="commands")
    async def commands_cmd(self, ctx: commands.Context):
        msg = (
            f"@{ctx.author.name} 📜 Commandes viewers: "
            "!creature | !inv | !use <item_key> [count] | "
            "!companion (liste) / !companion <numéro> | "
            "!trade @pseudo <n°> | !answer <n°> | !tyes / !tno | "
            "!show | !grab | !hit (boss)"
        )
        await ctx.send(msg)


# ============================================================================
# RUN
# ============================================================================
    @commands.command(name="link")
    async def link_cmd(self, ctx: commands.Context):
        """Affiche le lien du site CapsMöns dans le chat."""
        await ctx.send(f"🌐 CapsMöns → {PUBLIC_BASE_URL}")

    @commands.command(name="planning")
    async def planning_cmd(self, ctx: commands.Context):
        """Affiche le lien vers le planning de stream de la semaine en cours."""
        import datetime
        today = datetime.date.today()
        iso = today.isocalendar()
        year, week = iso[0], iso[1]
        await ctx.send(f"📅 Planning stream → {PUBLIC_BASE_URL}/planning/{year}/{week}")

    @commands.command(name="mylink")
    async def mylink_cmd(self, ctx: commands.Context):
        """Affiche le lien de profil de l'utilisateur dans le chat."""
        login = ctx.author.name.lower()
        await ctx.send(f"👤 Ton profil CapsMöns → {PUBLIC_BASE_URL}/u/{login}")


bot = Bot()
bot.run()
