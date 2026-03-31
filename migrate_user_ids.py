#!/usr/bin/env python3
"""
Migration : remplit twitch_user_id pour tous les users existants
qui n'ont pas encore cette colonne renseignée.

Usage sur le VPS :
    docker compose exec api python3 /app/migrate_user_ids.py

Ou directement :
    TWITCH_CLIENT_ID=xxx TWITCH_CLIENT_SECRET=xxx DATABASE_URL=xxx python3 migrate_user_ids.py
"""

import os
import time
import psycopg
import requests

TWITCH_CLIENT_ID     = os.environ["TWITCH_CLIENT_ID"]
TWITCH_CLIENT_SECRET = os.environ["TWITCH_CLIENT_SECRET"]
# ─── DB connection (même logique que main.py) ─────────────────────────────────
def get_db():
    return psycopg.connect(
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host="db",
        port=5432,
    )

# ─── Twitch app token ─────────────────────────────────────────────────────────
def get_app_token():
    r = requests.post("https://id.twitch.tv/oauth2/token", data={
        "client_id":     TWITCH_CLIENT_ID,
        "client_secret": TWITCH_CLIENT_SECRET,
        "grant_type":    "client_credentials",
    })
    r.raise_for_status()
    return r.json()["access_token"]

# ─── Resolve logins → user_ids via Helix (100 par batch) ─────────────────────
def resolve_logins(logins: list[str], token: str) -> dict[str, str]:
    """Retourne {login: user_id}"""
    result = {}
    headers = {
        "Client-ID":     TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {token}",
    }
    # Helix accepte max 100 logins par requête
    for i in range(0, len(logins), 100):
        batch = logins[i:i+100]
        params = [("login", l) for l in batch]
        r = requests.get("https://api.twitch.tv/helix/users", headers=headers, params=params)
        if r.status_code == 429:
            print("Rate limit, attente 1s...")
            time.sleep(1)
            r = requests.get("https://api.twitch.tv/helix/users", headers=headers, params=params)
        r.raise_for_status()
        for user in r.json().get("data", []):
            result[user["login"].lower()] = user["id"]
        time.sleep(0.1)  # gentle rate limiting
    return result

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("=== Migration twitch_user_id ===")

    # 1. Ajouter la colonne si elle n'existe pas
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS twitch_user_id TEXT UNIQUE;
            """)
        conn.commit()
    print("Colonne twitch_user_id OK")

    # 2. Récupérer les logins sans user_id
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT twitch_login FROM users
                WHERE twitch_user_id IS NULL
                ORDER BY twitch_login;
            """)
            logins = [r[0] for r in cur.fetchall()]

    if not logins:
        print("Aucun user à migrer, tout est déjà renseigné.")
        return

    print(f"{len(logins)} users à migrer...")

    # 3. Résoudre via Helix
    token = get_app_token()
    mapping = resolve_logins(logins, token)
    print(f"{len(mapping)} user_ids résolus sur {len(logins)} logins")

    # 4. Mettre à jour en base
    updated = 0
    not_found = []
    with get_db() as conn:
        with conn.cursor() as cur:
            for login, user_id in mapping.items():
                cur.execute(
                    "UPDATE users SET twitch_user_id = %s WHERE twitch_login = %s;",
                    (user_id, login)
                )
                updated += 1
            # Logins non résolus (compte supprimé, ban, etc.)
            resolved_logins = set(mapping.keys())
            for login in logins:
                if login not in resolved_logins:
                    not_found.append(login)
        conn.commit()

    print(f"✓ {updated} users mis à jour")
    if not_found:
        print(f"✗ {len(not_found)} logins non résolus (comptes supprimés ?) : {not_found[:10]}")

    print("Migration terminée.")

if __name__ == "__main__":
    main()
