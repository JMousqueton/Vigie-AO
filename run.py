"""
Point d'entrée principal — BOAMP × Cohesity
Lance l'application, initialise la base SQLite et crée l'admin par défaut.

Premier démarrage :
  1. Copiez .env.example en .env et ajustez les variables
  2. python run.py
  3. Connectez-vous avec ADMIN_EMAIL / ADMIN_DEFAULT_PASSWORD
  4. Changez immédiatement le mot de passe depuis Mon profil

Port par défaut : 5001 (5000 est souvent occupé par AirPlay Receiver sur macOS)
"""
import os
from dotenv import load_dotenv
load_dotenv()

from app import create_app, db

app = create_app(os.environ.get('FLASK_ENV', 'development'))


def _migrate_db():
    """Migrations SQLite légères (ALTER TABLE + CREATE INDEX idempotents)."""
    from sqlalchemy import text
    migrations = [
        # Colonnes ajoutées après v1
        "ALTER TABLE dossier_cache ADD COLUMN source VARCHAR(10) NOT NULL DEFAULT 'BOAMP'",
        "ALTER TABLE user_seen_dossiers ADD COLUMN seen_at DATETIME",
        # Table user_hidden_dossiers (ajoutée après v1)
        """CREATE TABLE IF NOT EXISTS user_hidden_dossiers (
            user_id INTEGER NOT NULL REFERENCES users(id),
            idweb VARCHAR(20) NOT NULL,
            hidden_at DATETIME,
            PRIMARY KEY (user_id, idweb)
        )""",
        # Index de performance (CREATE INDEX IF NOT EXISTS est idempotent)
        "CREATE INDEX IF NOT EXISTS ix_dossier_cache_score_pertinence ON dossier_cache (score_pertinence)",
        "CREATE INDEX IF NOT EXISTS ix_dossier_cache_has_rectificatif ON dossier_cache (has_rectificatif)",
        "CREATE INDEX IF NOT EXISTS ix_dossier_cache_has_attribution ON dossier_cache (has_attribution)",
        "CREATE INDEX IF NOT EXISTS ix_dossier_cache_date_derniere_activite ON dossier_cache (date_derniere_activite)",
        "CREATE INDEX IF NOT EXISTS ix_dossier_cache_datelimitereponse ON dossier_cache (datelimitereponse)",
        "CREATE INDEX IF NOT EXISTS ix_dossier_cache_dateparution ON dossier_cache (dateparution)",
        "CREATE INDEX IF NOT EXISTS ix_dossier_cache_source ON dossier_cache (source)",
    ]
    with app.app_context():
        with db.engine.connect() as conn:
            for sql in migrations:
                try:
                    conn.execute(text(sql))
                    conn.commit()
                except Exception:
                    pass  # Colonne/index déjà existant → on ignore


def init_db():
    """Crée les tables et l'admin par défaut si la base est vide."""
    with app.app_context():
        db.create_all()

        from app.models import User
        from app import bcrypt

        if User.query.count() == 0:
            admin_email = app.config.get('ADMIN_EMAIL', 'admin@domain.com')
            admin_pwd = os.environ.get('ADMIN_DEFAULT_PASSWORD', 'ChangeMe!2024')

            admin = User(
                prenom='Admin',
                nom='BOAMP',
                email=admin_email,
                password_hash=bcrypt.generate_password_hash(admin_pwd).decode('utf-8'),
                role='ADMIN',
                is_active=True,
                email_confirmed=True,
            )
            db.session.add(admin)
            db.session.commit()

            port = int(os.environ.get('PORT', 5001))
            print()
            print('=' * 60)
            print('  BOAMP × Cohesity — Premier démarrage')
            print('=' * 60)
            print(f'  Compte admin créé :')
            print(f'    Email    : {admin_email}')
            print(f'    Password : {admin_pwd}')
            print(f'  → http://localhost:{port}/auth/login')
            print(f'  ⚠  Changez le mot de passe dès la première connexion !')
            print('=' * 60)
            print()
        else:
            count = User.query.count()
            print(f'[init] Base existante — {count} utilisateur(s) enregistré(s)')

    _migrate_db()


init_db()


# ─── Commandes CLI Flask ──────────────────────────────────────────────────────

import click
import subprocess
import sys


@app.cli.group('translate')
def translate():
    """Commandes de gestion des traductions i18n."""


@translate.command('extract')
def translate_extract():
    """Extrait les chaînes traduisibles (messages.pot)."""
    result = subprocess.run(
        ['pybabel', 'extract', '-F', 'babel.cfg', '-o', 'app/translations/messages.pot', '.'],
        capture_output=True, text=True,
    )
    click.echo(result.stdout)
    if result.returncode != 0:
        click.echo(result.stderr, err=True)
        sys.exit(result.returncode)
    click.echo('Extraction terminée → app/translations/messages.pot')


@translate.command('update')
def translate_update():
    """Met à jour les fichiers .po depuis messages.pot."""
    result = subprocess.run(
        ['pybabel', 'update', '-i', 'app/translations/messages.pot', '-d', 'app/translations'],
        capture_output=True, text=True,
    )
    click.echo(result.stdout)
    if result.returncode != 0:
        click.echo(result.stderr, err=True)
        sys.exit(result.returncode)
    click.echo('Fichiers .po mis à jour.')


@translate.command('compile')
def translate_compile():
    """Compile les fichiers .po en .mo (binaires utilisés par Flask-Babel)."""
    result = subprocess.run(
        ['pybabel', 'compile', '-d', 'app/translations'],
        capture_output=True, text=True,
    )
    click.echo(result.stdout)
    if result.returncode != 0:
        click.echo(result.stderr, err=True)
        sys.exit(result.returncode)
    click.echo('Compilation terminée — fichiers .mo générés.')


@app.cli.command('send-digest')
@click.option('--type', 'alert_type',
              type=click.Choice(['DAILY', 'WEEKLY', 'IMMEDIATE'], case_sensitive=False),
              default='DAILY', show_default=True,
              help='Type de digest à envoyer.')
@click.option('--user', 'user_email', default=None,
              help='Envoyer uniquement à cet email (bypass alert_enabled).')
@click.option('--dry-run', is_flag=True, default=False,
              help='Affiche ce qui serait envoyé sans envoyer.')
def send_digest_cmd(alert_type, user_email, dry_run):
    """Envoie manuellement les digests email sans attendre le cron."""
    alert_type = alert_type.upper()
    with app.app_context():
        from app.models import User
        from app.services.mailer import send_alert_digest, _get_new_dossiers_for_user, _get_watchlist_updates

        if user_email:
            users = User.query.filter_by(email=user_email, is_active=True).all()
            if not users:
                click.echo(f"Aucun utilisateur actif trouvé pour {user_email}.", err=True)
                raise SystemExit(1)
        else:
            users = User.query.filter_by(
                is_active=True,
                alert_enabled=True,
                alert_frequency=alert_type,
            ).all()

        if not users:
            click.echo(f"Aucun utilisateur éligible pour un digest {alert_type}.")
            return

        if dry_run:
            click.echo(f"[DRY-RUN] digest {alert_type} — {len(users)} utilisateur(s) :")
            for user in users:
                dossiers = _get_new_dossiers_for_user(user)
                watchlist = _get_watchlist_updates(user)
                click.echo(f"  {user.email} → {len(dossiers)} dossier(s), {len(watchlist)} màj watchlist")
            return

        click.echo(f"Envoi digest {alert_type} à {len(users)} utilisateur(s)…")
        ok = ko = 0
        for user in users:
            # --user bypasse alert_enabled pour les tests
            force = user_email is not None
            success = send_alert_digest(user, alert_type, force=force)
            if success:
                ok += 1
                click.echo(f"  [OK] {user.email}")
            else:
                ko += 1
                click.echo(f"  [KO] {user.email}", err=True)

        click.echo(f"Terminé — {ok} OK, {ko} erreur(s).")


@app.cli.command('refresh-ted')
@click.argument('countries')
@click.option('--dry-run', is_flag=True, default=False,
              help='Fetch and score records but do not write to the database.')
def refresh_ted_cmd(countries, dry_run):
    """Refresh TED data for the given countries, regardless of registered users.

    COUNTRIES is a comma-separated list of ISO 2-letter codes.

    Examples:

    \b
      flask refresh-ted FR,BE,DE
      flask refresh-ted ES --dry-run
    """
    valid_codes = {
        'FR','BE','CH','LU','DE','ES','IT','NL','PT','AT',
        'PL','SE','DK','FI','NO','GB','IE',
    }
    requested = [c.strip().upper() for c in countries.split(',') if c.strip()]
    invalid = [c for c in requested if c not in valid_codes]
    if invalid:
        click.echo(
            f"Unknown country code(s): {', '.join(invalid)}. "
            f"Valid codes: {', '.join(sorted(valid_codes))}",
            err=True,
        )
        raise SystemExit(1)

    with app.app_context():
        from app.services.ted_api import fetch_ted_records, compute_ted_score
        from app.models import DossierCache
        from app import db
        from datetime import datetime as _dt

        def parse_date(d):
            if not d:
                return None
            try:
                return _dt.strptime(str(d)[:10], '%Y-%m-%d').date()
            except ValueError:
                return None

        total_created = total_updated = 0

        for country in sorted(requested):
            click.echo(f"\n[TED] Fetching {country}…")
            try:
                records = fetch_ted_records(country)
            except Exception as exc:
                click.echo(f"  [ERROR] fetch failed: {exc}", err=True)
                continue

            if not records:
                click.echo(f"  No records returned for {country}.")
                continue

            click.echo(f"  {len(records)} record(s) retrieved.")
            if dry_run:
                scored = sum(1 for r in records if compute_ted_score(r)[0] > 0)
                click.echo(f"  [DRY-RUN] {scored} would be written (score > 0).")
                continue

            created = updated = 0
            for rec in records:
                score, mots_cles = compute_ted_score(rec)
                idweb = rec['idweb']
                rec_country = rec.get('country', country)
                is_attribution = rec.get('_ted_is_attribution', False)

                existing = DossierCache.query.filter_by(idweb=idweb).first()
                if score == 0 and not existing:
                    continue

                attribution_json = None
                if is_attribution:
                    import json
                    attribution_json = json.dumps({
                        'dateparution':    rec.get('dateparution', ''),
                        'urlgravure':      rec.get('urlgravure', ''),
                        'reference_boamp': rec.get('reference_boamp', ''),
                        'montant':         rec.get('montant', ''),
                    }, ensure_ascii=False)

                if existing:
                    existing.acheteur_nom            = rec.get('acheteur_nom')
                    existing.objet_marche            = rec.get('objet_marche')
                    existing.nature                  = rec.get('nature')
                    existing.type_marche             = rec.get('type_marche')
                    existing.famille_denomination    = rec.get('famille_denomination')
                    existing.descripteur_libelle     = rec.get('descripteur_libelle')
                    existing.code_departement        = rec.get('code_departement')
                    existing.lieu_execution          = rec.get('lieu_execution')
                    existing.dateparution            = parse_date(rec.get('dateparution'))
                    existing.datelimitereponse       = parse_date(rec.get('datelimitereponse'))
                    existing.urlgravure              = rec.get('urlgravure')
                    existing.reference_boamp_initial = rec.get('reference_boamp')
                    existing.score_pertinence        = score
                    existing.mots_cles_matches       = __import__('json').dumps(mots_cles, ensure_ascii=False)
                    existing.has_attribution         = is_attribution
                    if is_attribution:
                        existing.attribution_json    = attribution_json
                    existing.date_derniere_activite  = parse_date(rec.get('dateparution'))
                    existing.fetched_at              = _dt.utcnow()
                    existing.source                  = 'TED'
                    existing.country                 = rec_country
                    updated += 1
                else:
                    import json
                    db.session.add(DossierCache(
                        idweb=idweb,
                        acheteur_nom=rec.get('acheteur_nom'),
                        objet_marche=rec.get('objet_marche'),
                        nature=rec.get('nature'),
                        type_marche=rec.get('type_marche'),
                        famille_denomination=rec.get('famille_denomination'),
                        descripteur_libelle=rec.get('descripteur_libelle'),
                        code_departement=rec.get('code_departement'),
                        lieu_execution=rec.get('lieu_execution'),
                        dateparution=parse_date(rec.get('dateparution')),
                        datelimitereponse=parse_date(rec.get('datelimitereponse')),
                        urlgravure=rec.get('urlgravure'),
                        reference_boamp_initial=rec.get('reference_boamp'),
                        rectificatifs_json='[]',
                        attribution_json=attribution_json,
                        score_pertinence=score,
                        mots_cles_matches=json.dumps(mots_cles, ensure_ascii=False),
                        has_rectificatif=False,
                        has_attribution=is_attribution,
                        date_derniere_activite=parse_date(rec.get('dateparution')),
                        fetched_at=_dt.utcnow(),
                        is_new=True,
                        source='TED',
                        country=rec_country,
                    ))
                    created += 1

            db.session.commit()
            click.echo(f"  Done — {created} created, {updated} updated.")
            total_created += created
            total_updated += updated

        if not dry_run and len(requested) > 1:
            click.echo(f"\nTotal — {total_created} created, {total_updated} updated across {len(requested)} countries.")


@app.cli.command('set-country')
@click.argument('country')
@click.option('--users', 'user_emails', default=None,
              help='Comma-separated list of emails to update (default: all users without a country).')
@click.option('--all', 'update_all', is_flag=True, default=False,
              help='Apply to ALL active users, even those who already have a country set.')
@click.option('--dry-run', is_flag=True, default=False,
              help='Show what would be changed without saving.')
def set_country_cmd(country, user_emails, update_all, dry_run):
    """Set the country code for users.

    COUNTRY must be a valid ISO 2-letter code (e.g. FR, BE, DE, ES, GB).

    Examples:

    \b
      # Set FR for all users that have no country yet
      flask set-country FR

    \b
      # Set BE for specific users
      flask set-country BE --users alice@domain.com,bob@domain.com

    \b
      # Override country for ALL active users
      flask set-country DE --all

    \b
      # Preview without saving
      flask set-country IT --all --dry-run
    """
    country = country.upper()
    valid_codes = {
        'FR','BE','CH','LU','DE','ES','IT','NL','PT','AT',
        'PL','SE','DK','FI','NO','GB','IE',
    }
    if country not in valid_codes:
        click.echo(
            f"Unknown country code '{country}'. Valid codes: {', '.join(sorted(valid_codes))}",
            err=True,
        )
        raise SystemExit(1)

    with app.app_context():
        from app.models import User

        if user_emails:
            emails = [e.strip().lower() for e in user_emails.split(',') if e.strip()]
            users = User.query.filter(User.email.in_(emails)).all()
            not_found = set(emails) - {u.email.lower() for u in users}
            if not_found:
                click.echo(f"Warning: email(s) not found: {', '.join(sorted(not_found))}", err=True)
        elif update_all:
            users = User.query.filter_by(is_active=True).all()
        else:
            # Default: only users who have no country set
            users = User.query.filter(
                (User.country == None) | (User.country == '')
            ).all()

        if not users:
            click.echo("No matching users found.")
            return

        if dry_run:
            click.echo(f"[DRY-RUN] Would set country={country} for {len(users)} user(s):")
            for u in users:
                click.echo(f"  {u.email}  ({u.country or '—'} → {country})")
            return

        for u in users:
            old = u.country or '—'
            u.country = country
            click.echo(f"  {u.email}  {old} → {country}")

        from app import db
        db.session.commit()
        click.echo(f"\nDone — {len(users)} user(s) updated to {country}.")


if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=int(os.environ.get('PORT', 5001)),
        debug=app.config.get('DEBUG', True),
    )
