#!/usr/bin/env bash
# update.sh — Met à jour Vigie AO depuis git
# Usage : ./update.sh [--no-restart]
#   --no-restart  ne redémarre pas le service systemd après mise à jour

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_BIN="$SCRIPT_DIR/venv/bin"
SERVICE="vigie-ao"
RESTART=true

for arg in "$@"; do
  [[ "$arg" == "--no-restart" ]] && RESTART=false
done

# ─── Couleurs ────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ─── Vérification venv ───────────────────────────────────────────────────────
[[ -f "$VENV_BIN/python" ]] || error "venv introuvable : $VENV_BIN"

# ─── Snapshot avant pull ─────────────────────────────────────────────────────
BEFORE=$(git rev-parse HEAD)

info "Vérification des mises à jour…"
git fetch origin main --quiet

REMOTE=$(git rev-parse origin/main)

if [[ "$BEFORE" == "$REMOTE" ]]; then
  info "Déjà à jour (${BEFORE:0:7}). Rien à faire."
  exit 0
fi

# ─── Pull ────────────────────────────────────────────────────────────────────
info "Mise à jour disponible : ${BEFORE:0:7} → ${REMOTE:0:7}"
git pull origin main --quiet
AFTER=$(git rev-parse HEAD)
info "Code mis à jour → ${AFTER:0:7}"

# Fichiers modifiés entre les deux commits
CHANGED=$(git diff --name-only "$BEFORE" "$AFTER")
echo ""
info "Fichiers modifiés :"
echo "$CHANGED" | sed 's/^/         /'
echo ""

# ─── requirements.txt ────────────────────────────────────────────────────────
if echo "$CHANGED" | grep -q "^requirements\.txt$"; then
  info "requirements.txt modifié → pip install…"
  "$VENV_BIN/pip" install -r requirements.txt --quiet
  info "Dépendances mises à jour."
fi

# ─── Traductions (.po) ───────────────────────────────────────────────────────
if echo "$CHANGED" | grep -qE "\.po$"; then
  info "Fichiers de traduction modifiés → compilation…"
  FLASK_APP=run.py "$VENV_BIN/flask" translate compile
  info "Traductions compilées."
fi

# ─── Redémarrage service ─────────────────────────────────────────────────────
if $RESTART; then
  info "Redémarrage du service $SERVICE…"
  sudo systemctl restart "$SERVICE"
  info "Service redémarré. Démarrage en cours…"
  echo ""

  # Jauge de 10 secondes
  WAIT=10
  BAR_WIDTH=40
  for ((i=1; i<=WAIT; i++)); do
    filled=$(( i * BAR_WIDTH / WAIT ))
    empty=$(( BAR_WIDTH - filled ))
    bar="${GREEN}$(printf '█%.0s' $(seq 1 $filled))${NC}$(printf '░%.0s' $(seq 1 $empty))"
    printf "\r  [%b] %2ds / %ds" "$bar" "$i" "$WAIT"
    sleep 1
  done
  echo ""
  echo ""

  # Statut du service
  info "Statut du service $SERVICE :"
  echo ""
  if systemctl is-active --quiet "$SERVICE"; then
    echo -e "  ${GREEN}●${NC} $(systemctl show "$SERVICE" --property=ActiveState --value | tr '[:lower:]' '[:upper:]') — $(systemctl show "$SERVICE" --property=MainPID --value | xargs -I{} sh -c 'echo "PID {}"')"
    systemctl status "$SERVICE" --no-pager --lines=5 | tail -n +3 | sed 's/^/  /'
  else
    echo -e "  ${RED}●${NC} SERVICE INACTIF"
    systemctl status "$SERVICE" --no-pager --lines=10 | tail -n +3 | sed 's/^/  /'
    echo ""
    error "Le service $SERVICE ne semble pas actif. Vérifiez les logs : journalctl -u $SERVICE -n 50"
  fi
else
  warn "Redémarrage ignoré. Pensez à relancer manuellement : sudo systemctl restart $SERVICE"
fi

echo ""
info "Mise à jour terminée."
