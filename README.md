# Vigie AO

> Veille automatisée des appels d'offres publics — BOAMP × TED

Application Flask de surveillance des marchés publics pertinents pour Cohesity, agrégeant les sources françaises (BOAMP) et européennes (TED v3).

## Fonctionnalités

- **Dashboard** filtrable : tous les avis, rectificatifs, attributions, watchlist, dossiers masqués
- **Double source** : BOAMP Opendatasoft API v2.1 + TED v3 — Tenders Electronic Daily
- **Scoring de pertinence** : mots-clés pondérés (haute / moyenne / contexte) + codes CPV
- **Explication des déclencheurs** : détail des champs et extraits ayant déclenché chaque dossier
- **Watchlist** personnelle avec alertes email (immédiat / quotidien / hebdo)
- **Masquage** de dossiers non pertinents
- **Partage** de dossiers par lien temporaire
- **Panel admin** : gestion des utilisateurs, sources, mots-clés, logs alertes
- **Thème clair / sombre**

## Stack

| Composant | Technologie |
|-----------|-------------|
| Backend | Python 3.12 · Flask 3 · SQLAlchemy |
| Base de données | SQLite |
| Auth | Flask-Login · Flask-Bcrypt · Flask-WTF |
| Scheduler | APScheduler |
| Email | Flask-Mail |
| Frontend | HTML5 · CSS custom · Font Awesome 6 · Vanilla JS |

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

## Sources de données

- **BOAMP** — [boamp-datadila.opendatasoft.com](https://boamp-datadila.opendatasoft.com) · API v2.1
- **TED v3** — [ted.europa.eu](https://ted.europa.eu) · Tenders Electronic Daily · [developer.ted.europa.eu](https://developer.ted.europa.eu)

## Licence

[MIT](LICENSE) © 2026 Julien Mousqueton
