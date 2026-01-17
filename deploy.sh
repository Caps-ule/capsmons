#!/usr/bin/env bash
set -euo pipefail

# ===============================
# CapsMons - Deploy Script
# ===============================

# --- CONFIG ---
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BRANCH="main"
COMPOSE="docker compose"
DOMAIN="https://capsmons.devlooping.fr"
HEALTH_PATH="/health"
# --------------

cd "$APP_DIR"

echo "==============================="
echo "🚀 CapsMons deploy"
date -u +"🕒 UTC %Y-%m-%d %H:%M:%S"
echo "📁 Dir: $APP_DIR"
echo "==============================="

# --- Checks ---
if [[ ! -f docker-compose.yml ]]; then
  echo "❌ docker-compose.yml introuvable"
  exit 1
fi

if [[ ! -f .env ]]; then
  echo "❌ .env introuvable (normalement non versionné)"
  exit 1
fi

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "❌ Pas un dépôt git"
  exit 1
fi

# --- Git ---
echo
echo "🔄 Git: fetch / pull"
echo "➡️  Avant: $(git rev-parse --short HEAD) - $(git log -1 --pretty=%s)"

git fetch origin

LOCAL="$(git rev-parse HEAD)"
REMOTE="$(git rev-parse origin/${BRANCH})"

if [[ "$LOCAL" == "$REMOTE" ]]; then
  echo "✅ Repo déjà à jour"
else
  echo "⬇️  Mise à jour depuis origin/${BRANCH}"
  git pull --ff-only origin "$BRANCH"
  echo "➡️  Après: $(git rev-parse --short HEAD) - $(git log -1 --pretty=%s)"
fi

# --- Docker ---
echo
echo "🐳 Docker: build & up"
$COMPOSE up -d --build

echo
echo "📦 Containers:"
$COMPOSE ps

# --- Healthcheck ---
echo
echo "🩺 Healthcheck: ${DOMAIN}${HEALTH_PATH}"
sleep 2

HTTP_CODE="$(curl -k -s -o /dev/null -w "%{http_code}" "${DOMAIN}${HEALTH_PATH}" || true)"

if [[ "$HTTP_CODE" == "200" ]]; then
  echo "✅ OK (HTTP 200)"
else
  echo "⚠️  Problème détecté (HTTP $HTTP_CODE)"
  echo
  echo "---- logs API (80 lignes) ----"
  $COMPOSE logs --tail=80 api || true
  echo
  echo "---- logs BOT (80 lignes) ----"
  $COMPOSE logs --tail=80 bot || true
  exit 2
fi

echo
echo "🎉 Deploy terminé avec succès"
