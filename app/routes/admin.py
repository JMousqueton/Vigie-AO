"""
Panel d'administration : gestion utilisateurs, import emails, stats, refresh.
"""
import secrets
import string
from datetime import datetime
from functools import wraps

from flask import (
    Blueprint, render_template, redirect, url_for,
    flash, request, current_app,
)
from flask_login import login_required, current_user

from app import db, bcrypt
from app.models import User, DossierCache, AlertLog, WatchlistItem, AppConfig

admin_bp = Blueprint('admin', __name__)


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash('Accès réservé aux administrateurs.', 'danger')
            return redirect(url_for('main.dashboard'))
        return f(*args, **kwargs)
    return decorated


def _generate_temp_password(length: int = 12) -> str:
    alphabet = string.ascii_letters + string.digits + '!@#$'
    while True:
        pwd = ''.join(secrets.choice(alphabet) for _ in range(length))
        if (any(c.isupper() for c in pwd)
                and any(c.islower() for c in pwd)
                and any(c.isdigit() for c in pwd)):
            return pwd


# ─── Dashboard admin ─────────────────────────────────────────────────────────

def _source_enabled(source: str) -> bool:
    """Retourne l'état activé/désactivé d'une source (stocké en AppConfig)."""
    row = AppConfig.query.filter_by(key=f'source_{source}_enabled').first()
    if row is not None:
        return row.value.lower() == 'true'
    # Fallback sur la config Flask pour TED
    if source == 'TED':
        from flask import current_app
        return current_app.config.get('TED_ENABLED', False)
    return True  # BOAMP activé par défaut


def _db_file_info() -> dict:
    """Retourne la taille et le chemin du fichier SQLite."""
    import os
    from flask import current_app
    db_uri = current_app.config.get('SQLALCHEMY_DATABASE_URI', '')
    # Extraire le chemin depuis sqlite:////absolute/path ou sqlite:///relative
    path = db_uri.replace('sqlite:///', '')
    if not os.path.isabs(path):
        path = os.path.join(current_app.instance_path, path)
    try:
        size_bytes = os.path.getsize(path)
        mtime = datetime.utcfromtimestamp(os.path.getmtime(path))
    except OSError:
        size_bytes = None
        mtime = None
    return {'path': path, 'size_bytes': size_bytes, 'mtime': mtime}


def _db_reminders_count() -> int:
    try:
        from app.models import Reminder
        return Reminder.query.count()
    except Exception:
        return 0


def _db_appconfig_count() -> int:
    try:
        return AppConfig.query.count()
    except Exception:
        return 0


def _admin_context(**extra):
    """Données communes à toutes les vues admin."""
    from app.services.keywords import get_search_keywords, get_scoring_keywords, get_exclude_keywords
    kws = get_scoring_keywords()

    # Stats par source pour le tab Sources
    sources_info = []
    for src in ['BOAMP', 'TED']:
        last_fetched = db.session.query(db.func.max(DossierCache.fetched_at)).filter(
            DossierCache.source == src
        ).scalar()
        last_parution = db.session.query(db.func.max(DossierCache.dateparution)).filter(
            DossierCache.source == src
        ).scalar()
        count = DossierCache.query.filter_by(source=src).count()
        sources_info.append({
            'source':        src,
            'count':         count,
            'last_fetched':  last_fetched,
            'last_parution': last_parution,
            'enabled':       _source_enabled(src),
        })

    ctx = dict(
        nb_users=User.query.count(),
        nb_active=User.query.filter_by(is_active=True).count(),
        nb_dossiers=DossierCache.query.count(),
        nb_alerts=AlertLog.query.count(),
        nb_alerts_ok=AlertLog.query.filter_by(success=True).count(),
        last_fetched=db.session.query(db.func.max(DossierCache.fetched_at)).scalar(),
        recent_logs=AlertLog.query.order_by(AlertLog.sent_at.desc()).limit(20).all(),
        users=User.query.order_by(User.created_at.desc()).all(),
        sources_info=sources_info,
        # Mots-clés (pour le tab Keywords)
        kw_search='\n'.join(get_search_keywords()),
        kw_haute='\n'.join(kws.get('haute', [])),
        kw_moyenne='\n'.join(kws.get('moyenne', [])),
        kw_contexte='\n'.join(kws.get('contexte', [])),
        kw_exclude='\n'.join(get_exclude_keywords()),
        # Base de données
        db_boamp=DossierCache.query.filter_by(source='BOAMP').count(),
        db_ted=DossierCache.query.filter_by(source='TED').count(),
        db_duplicates=DossierCache.query.filter_by(is_duplicate=True).count(),
        db_watchlist=WatchlistItem.query.count(),
        db_reminders=_db_reminders_count(),
        db_alert_logs=AlertLog.query.count(),
        db_appconfig=_db_appconfig_count(),
        db_file=_db_file_info(),
        active_tab=request.args.get('tab', 'users'),
    )
    ctx.update(extra)
    return ctx


@admin_bp.route('/')
@login_required
@admin_required
def index():
    return render_template('admin/users.html', **_admin_context())


# ─── Gestion utilisateurs ─────────────────────────────────────────────────────

@admin_bp.route('/users/activate/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def activate_user(user_id):
    user = User.query.get_or_404(user_id)
    user.is_active = True
    user.email_confirmed = True
    db.session.commit()
    flash(f'Compte {user.email} activé.', 'success')
    return redirect(url_for('admin.index'))


@admin_bp.route('/users/deactivate/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def deactivate_user(user_id):
    user = User.query.get_or_404(user_id)
    if user.id == current_user.id:
        flash('Vous ne pouvez pas désactiver votre propre compte.', 'danger')
        return redirect(url_for('admin.index'))
    user.is_active = False
    db.session.commit()
    flash(f'Compte {user.email} désactivé.', 'warning')
    return redirect(url_for('admin.index'))


@admin_bp.route('/users/promote/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def promote_user(user_id):
    user = User.query.get_or_404(user_id)
    user.role = 'ADMIN' if user.role == 'USER' else 'USER'
    db.session.commit()
    action = 'promu admin' if user.role == 'ADMIN' else 'rétrogradé utilisateur'
    flash(f'{user.email} {action}.', 'success')
    return redirect(url_for('admin.index'))


@admin_bp.route('/users/reset-password/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def reset_password(user_id):
    user = User.query.get_or_404(user_id)
    new_pwd = _generate_temp_password()
    user.password_hash = bcrypt.generate_password_hash(new_pwd).decode('utf-8')
    db.session.commit()
    try:
        from app.services.mailer import send_temp_password_email
        send_temp_password_email(user, new_pwd)
        flash(f'Mot de passe réinitialisé pour {user.email}. Les nouveaux identifiants ont été envoyés par email.', 'success')
    except Exception as exc:
        current_app.logger.error("Erreur envoi reset mdp : %s", exc)
        flash(f'Mot de passe réinitialisé pour {user.email}, mais l\'envoi email a échoué. Contactez l\'utilisateur directement.', 'warning')
    return redirect(url_for('admin.index'))


@admin_bp.route('/users/delete/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def delete_user(user_id):
    user = User.query.get_or_404(user_id)
    if user.id == current_user.id:
        flash('Vous ne pouvez pas supprimer votre propre compte.', 'danger')
        return redirect(url_for('admin.index'))
    email = user.email
    db.session.delete(user)
    db.session.commit()
    flash(f'Compte {email} supprimé.', 'warning')
    return redirect(url_for('admin.index'))


# ─── Import emails ────────────────────────────────────────────────────────────

@admin_bp.route('/import-emails', methods=['GET', 'POST'])
@login_required
@admin_required
def import_emails():
    preview = []
    errors = []

    if request.method == 'POST':
        raw = ''
        if 'email_file' in request.files:
            f = request.files['email_file']
            if f and f.filename:
                raw = f.read().decode('utf-8', errors='ignore')
        if not raw:
            raw = request.form.get('email_list', '')

        emails = [line.strip().lower() for line in raw.splitlines() if line.strip()]
        emails = list(set(emails))  # Dédupliquer

        # Validation et création
        if 'confirm' in request.form:
            created = 0
            skipped = 0
            for email in emails:
                if '@' not in email or '.' not in email.split('@')[-1]:
                    errors.append(f'Format invalide : {email}')
                    continue

                allowed_domain = current_app.config.get('ALLOWED_EMAIL_DOMAIN', '')
                if allowed_domain and not email.endswith(f'@{allowed_domain}'):
                    errors.append(f'Domaine non autorisé : {email}')
                    continue

                if User.query.filter_by(email=email).first():
                    skipped += 1
                    continue

                temp_pwd = _generate_temp_password()
                parts = email.split('@')[0].split('.')
                prenom = parts[0].capitalize() if parts else 'Invité'
                nom = parts[1].upper() if len(parts) > 1 else 'COHESITY'

                user = User(
                    prenom=prenom,
                    nom=nom,
                    email=email,
                    password_hash=bcrypt.generate_password_hash(temp_pwd).decode('utf-8'),
                    role='USER',
                    is_active=False,
                    email_confirmed=False,
                )
                db.session.add(user)
                try:
                    db.session.flush()
                    from app.services.mailer import send_invitation_email
                    send_invitation_email(user, temp_pwd)
                    created += 1
                except Exception as exc:
                    db.session.rollback()
                    errors.append(f'Erreur pour {email} : {exc}')
                    continue

            db.session.commit()
            flash(f'{created} compte(s) créé(s), {skipped} ignoré(s) (déjà existant).', 'success')
            if errors:
                for err in errors:
                    flash(err, 'warning')
            return redirect(url_for('admin.index'))
        else:
            # Mode prévisualisation
            for email in emails:
                exists = User.query.filter_by(email=email).first() is not None
                preview.append({'email': email, 'exists': exists})

    return render_template('admin/users.html',
                           **_admin_context(preview=preview, errors=errors,
                                            show_import=True, active_tab='users'))


# ─── Mots-clés ───────────────────────────────────────────────────────────────

@admin_bp.route('/keywords', methods=['POST'])
@login_required
@admin_required
def save_keywords():
    from app.services.keywords import save_keywords as _save

    def parse(field):
        return [l.strip() for l in request.form.get(field, '').splitlines() if l.strip()]

    search   = parse('kw_search')
    haute    = parse('kw_haute')
    moyenne  = parse('kw_moyenne')
    contexte = parse('kw_contexte')
    exclude  = parse('kw_exclude')

    # Détecter les retraits avant sauvegarde
    search_lower = {kw.lower() for kw in search}
    removed = [
        kw for kw in (haute + moyenne + contexte)
        if kw.lower() not in search_lower
    ]

    _save(search=search, haute=haute, moyenne=moyenne,
          contexte=contexte, exclude=exclude, updated_by=current_user.id)

    if removed:
        flash(
            f'Mots-clés retirés du scoring car absents de la liste de recherche : '
            f'{", ".join(removed)}',
            'warning',
        )

    # Recalculer les scores de tous les dossiers en cache
    try:
        from app.services.scoring import rescore_all_dossiers
        nb_updated, nb_total = rescore_all_dossiers()
        flash(
            f'Mots-clés mis à jour — {nb_updated}/{nb_total} dossiers rescorés.',
            'success',
        )
    except Exception as exc:
        current_app.logger.error("Erreur rescore : %s", exc)
        flash('Mots-clés mis à jour. Erreur lors du recalcul des scores.', 'warning')

    return redirect(url_for('admin.index', tab='keywords'))


# ─── Refresh manuel (toutes sources) ─────────────────────────────────────────

@admin_bp.route('/refresh', methods=['POST'])
@login_required
@admin_required
def manual_refresh():
    try:
        from app.services.scheduler import refresh_boamp_cache, refresh_ted_cache
        app = current_app._get_current_object()
        refresh_boamp_cache(app)
        flash('Cache BOAMP rafraîchi avec succès.', 'success')
        if app.config.get('TED_ENABLED', False):
            refresh_ted_cache(app)
            flash('Cache TED rafraîchi avec succès.', 'success')
        current_app.logger.info("Refresh admin par %s", current_user.email)
    except Exception as exc:
        flash(f'Erreur lors du refresh : {exc}', 'danger')
    return redirect(url_for('admin.index'))


# ─── Gestion sources ──────────────────────────────────────────────────────────

VALID_SOURCES = {'BOAMP', 'TED'}


@admin_bp.route('/sources/refresh/<source>', methods=['POST'])
@login_required
@admin_required
def refresh_source(source):
    if source not in VALID_SOURCES:
        flash('Source inconnue.', 'danger')
        return redirect(url_for('admin.index', tab='sources'))

    try:
        app = current_app._get_current_object()
        if source == 'BOAMP':
            from app.services.scheduler import refresh_boamp_cache
            refresh_boamp_cache(app)
        else:
            from app.services.scheduler import refresh_ted_cache
            refresh_ted_cache(app)
        flash(f'Cache {source} rafraîchi avec succès.', 'success')
        current_app.logger.info("Refresh %s par %s", source, current_user.email)
    except Exception as exc:
        flash(f'Erreur refresh {source} : {exc}', 'danger')

    return redirect(url_for('admin.index', tab='sources'))


@admin_bp.route('/sources/toggle/<source>', methods=['POST'])
@login_required
@admin_required
def toggle_source(source):
    if source not in VALID_SOURCES:
        flash('Source inconnue.', 'danger')
        return redirect(url_for('admin.index', tab='sources'))

    current_state = _source_enabled(source)
    new_state = not current_state

    key = f'source_{source}_enabled'
    row = AppConfig.query.filter_by(key=key).first()
    if row:
        row.value = str(new_state).lower()
        row.updated_at = datetime.utcnow()
        row.updated_by = current_user.id
    else:
        db.session.add(AppConfig(
            key=key,
            value=str(new_state).lower(),
            updated_by=current_user.id,
        ))
    db.session.commit()

    state_label = 'activée' if new_state else 'désactivée'
    flash(f'Source {source} {state_label}.', 'success')
    current_app.logger.info("Source %s %s par %s", source, state_label, current_user.email)
    return redirect(url_for('admin.index', tab='sources'))


@admin_bp.route('/sources/delete/<source>', methods=['POST'])
@login_required
@admin_required
def delete_source(source):
    if source not in VALID_SOURCES:
        flash('Source inconnue.', 'danger')
        return redirect(url_for('admin.index', tab='sources'))

    count = DossierCache.query.filter_by(source=source).count()
    DossierCache.query.filter_by(source=source).delete()
    db.session.commit()

    flash(f'{count} dossier(s) {source} supprimés du cache.', 'warning')
    current_app.logger.warning("Suppression cache %s (%d entrées) par %s",
                               source, count, current_user.email)
    return redirect(url_for('admin.index', tab='sources'))
