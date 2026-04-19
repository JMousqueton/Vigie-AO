# Vigie AO

> Veille automatisée des appels d'offres publics — BOAMP × TED × PLACE_ES

Application Flask de surveillance des marchés publics pertinents, agrégeant les sources françaises (BOAMP), espagnoles (PLACE_ES) et l'ensemble des pays européens via TED v3. Interface disponible en français et en anglais.

## Fonctionnalités

- **Dashboard** filtrable : tous les avis, rectificatifs, attributions, watchlist, dossiers masqués
- **Triple source** : BOAMP Opendatasoft API v2.1 · TED v3 (Tenders Electronic Daily) · PLACE_ES (Espagne)
- **Scoring de pertinence** : mots-clés pondérés (haute / moyenne / contexte) + codes CPV — globaux et par pays, configurables depuis le panel admin
- **Explication des déclencheurs** : détail des champs et extraits ayant déclenché chaque dossier
- **Carte interactive** : visualisation des marchés par département (France uniquement), chargée à la demande
- **Watchlist** personnelle avec notifications email sur nouveaux rectificatifs (immédiat / quotidien / hebdo)
- **Masquage** de dossiers non pertinents
- **Partage** de dossiers par lien temporaire
- **Statistiques** : page dédiée avec répartition par source, nature, score et évolution temporelle
- **Panel admin** : gestion des utilisateurs, sources, mots-clés, logs alertes
- **Internationalisation** : interface en français et en anglais (Flask-Babel)
- **Interface responsive** : menu adapté mobile
- **Thème clair / sombre**

## Stack

| Composant | Technologie |
|-----------|-------------|
| Backend | Python 3.12 · Flask 3 · SQLAlchemy |
| Base de données | SQLite |
| Auth | Flask-Login · Flask-Bcrypt · Flask-WTF |
| Scheduler | APScheduler |
| Email | Flask-Mail |
| Frontend | HTML5 · CSS custom · Font Awesome 7 · Vanilla JS |
| i18n | Flask-Babel · Français · English |

## Installation

```bash
# 1. Cloner le dépôt
git clone https://github.com/JMousqueton/Vigie-AO.git
cd Vigie-AO

# 2. Créer l'environnement virtuel
python3 -m venv venv
source venv/bin/activate

# 3. Installer les dépendances
pip install -r requirements.txt

# 4. Configurer l'environnement
cp .env.example .env
# Éditer .env avec vos valeurs

# 5. Lancer l'application
python run.py
```

L'application démarre sur [http://localhost:5001](http://localhost:5001).

Un compte admin est créé automatiquement au premier démarrage (voir les variables `ADMIN_EMAIL` / `ADMIN_DEFAULT_PASSWORD` dans `.env`).

## Configuration

Copier `.env.example` en `.env` et renseigner :

| Variable | Description |
|----------|-------------|
| `SECRET_KEY` | Clé secrète Flask |
| `ADMIN_EMAIL` | Email du compte admin initial |
| `ADMIN_DEFAULT_PASSWORD` | Mot de passe admin initial |
| `BOAMP_API_KEY` | Clé API BOAMP Opendatasoft |
| `TED_ENABLED` | `true` pour activer la source TED |
| `TED_API_KEY` | Clé API TED (gratuite sur developer.ted.europa.eu) |
| `MAIL_*` | Configuration SMTP pour les alertes email |

## Commandes CLI

Des commandes Flask permettent de déclencher manuellement les jobs sans attendre le cron.

### Rafraîchir les données

```bash
# Rafraîchir la source BOAMP (fetch + déduplication)
flask refresh-boamp

# Rafraîchir TED pour un ou plusieurs pays (codes ISO 2 lettres)
flask refresh-ted FR,BE,DE
flask refresh-ted ES --dry-run

# Rafraîchir PLACE_ES (Espagne)
flask refresh-place-es

# Déduplication manuelle BOAMP/TED
flask dedup
```

### Envoyer les digests email

```bash
# Digest quotidien (défaut) à tous les abonnés DAILY
flask send-digest

# Choisir le type
flask send-digest --type WEEKLY
flask send-digest --type IMMEDIATE

# Tester sur un seul utilisateur (ignore la fréquence configurée)
flask send-digest --user julien@example.com
flask send-digest --type WEEKLY --user julien@example.com

# Aperçu sans envoi
flask send-digest --dry-run
```

### Gestion des utilisateurs

```bash
# Définir le pays pour tous les utilisateurs sans pays
flask set-country FR

# Définir le pays pour des utilisateurs spécifiques
flask set-country BE --users alice@example.com,bob@example.com

# Appliquer à tous les utilisateurs actifs
flask set-country DE --all --dry-run
```

> **Note** : ces commandes nécessitent que les variables d'environnement soient chargées (`source venv/bin/activate` + `.env` présent).

## Sources de données

| Source | Pays | URL |
|--------|------|-----|
| **BOAMP** | France | [boamp-datadila.opendatasoft.com](https://boamp-datadila.opendatasoft.com) · API v2.1 |
| **TED v3** | Tous les pays européens | [ted.europa.eu](https://ted.europa.eu) · [developer.ted.europa.eu](https://developer.ted.europa.eu) |
| **PLACE_ES** | Espagne | Plateforme nationale des marchés publics espagnols |

## Licence

[MIT](LICENSE) © 2026 Julien Mousqueton
