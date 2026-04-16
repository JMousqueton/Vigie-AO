"""
Service d'envoi d'emails : alertes BOAMP, confirmation, invitation.
"""
import logging
from datetime import datetime, timedelta

from flask import current_app, render_template, url_for
from flask_mail import Message
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature

from app import mail, db
from app.models import User, DossierCache, WatchlistItem, AlertLog

logger = logging.getLogger(__name__)


# ─── Helpers URL ─────────────────────────────────────────────────────────────

def _external_url(path: str) -> str:
    """
    Construit une URL absolue sans contexte de requête HTTP.
    Lit APP_BASE_URL depuis la config Flask, avec fallback sur SERVER_NAME.
    Exemple : _external_url('/dashboard') → 'https://vigie-ao.example.com/dashboard'
    """
    base = current_app.config.get('APP_BASE_URL', '').rstrip('/')
    if not base:
        server_name = current_app.config.get('SERVER_NAME', '')
        scheme = 'https' if current_app.config.get('SESSION_COOKIE_SECURE') else 'http'
        base = f'{scheme}://{server_name}' if server_name else ''
    return f'{base}{path}' if base else path


# ─── Tokens ──────────────────────────────────────────────────────────────────

def generate_token(email: str, salt: str = 'email-confirm') -> str:
    s = URLSafeTimedSerializer(current_app.config['SECRET_KEY'])
    return s.dumps(email, salt=salt)


def verify_token(token: str, salt: str = 'email-confirm', max_age: int = 86400):
    """Vérifie un token. Retourne l'email ou None si invalide/expiré."""
    s = URLSafeTimedSerializer(current_app.config['SECRET_KEY'])
    try:
        email = s.loads(token, salt=salt, max_age=max_age)
        return email
    except (SignatureExpired, BadSignature):
        return None


# ─── Emails applicatifs ───────────────────────────────────────────────────────

def send_confirmation_email(user: User) -> bool:
    """Envoie un email de confirmation d'inscription."""
    token = generate_token(user.email, salt='email-confirm')
    confirm_url = url_for('auth.confirm_email', token=token, _external=True)
    try:
        msg = Message(
            subject='Confirmez votre inscription — Vigie AO',
            recipients=[user.email],
            html=render_template(
                'email/confirm_email.html',
                user=user,
                confirm_url=confirm_url,
            ),
        )
        mail.send(msg)
        logger.info("Email de confirmation envoyé à %s", user.email)
        return True
    except Exception as exc:
        logger.error("Erreur envoi confirmation à %s : %s", user.email, exc)
        return False


def send_invitation_email(user: User, temp_password: str) -> bool:
    """Envoie un email d'invitation avec mot de passe temporaire."""
    token = generate_token(user.email, salt='email-confirm')
    activate_url = url_for('auth.confirm_email', token=token, _external=True)
    try:
        msg = Message(
            subject='Invitation — Vigie AO',
            recipients=[user.email],
            html=render_template(
                'email/confirm_email.html',
                user=user,
                confirm_url=activate_url,
                temp_password=temp_password,
                is_invitation=True,
            ),
        )
        mail.send(msg)
        logger.info("Email d'invitation envoyé à %s", user.email)
        return True
    except Exception as exc:
        logger.error("Erreur envoi invitation à %s : %s", user.email, exc)
        return False


def send_temp_password_email(user: User, temp_password: str) -> bool:
    """Envoie le mot de passe temporaire suite à une réinitialisation admin."""
    try:
        msg = Message(
            subject='Réinitialisation de votre mot de passe — Vigie AO',
            recipients=[user.email],
            html=render_template(
                'email/confirm_email.html',
                user=user,
                confirm_url=url_for('auth.login', _external=True),
                temp_password=temp_password,
                is_reset=True,
            ),
        )
        mail.send(msg)
        logger.info("Email de réinitialisation envoyé à %s", user.email)
        return True
    except Exception as exc:
        logger.error("Erreur envoi reset mdp à %s : %s", user.email, exc)
        return False


# ─── Alertes digest ──────────────────────────────────────────────────────────

def _get_new_dossiers_for_user(user: User) -> list[DossierCache]:
    """Retourne les dossiers nouveaux ou mis à jour depuis la dernière alerte."""
    cutoff = user.alert_last_sent or (datetime.utcnow() - timedelta(days=7))
    return DossierCache.query.filter(
        DossierCache.fetched_at >= cutoff
    ).order_by(DossierCache.score_pertinence.desc()).limit(50).all()


def _get_watchlist_updates(user: User) -> list[dict]:
    """Retourne les dossiers watchlistés avec de nouveaux rectificatifs."""
    updates = []
    for item in user.watchlist_items:
        dossier = DossierCache.query.filter_by(idweb=item.idweb).first()
        if dossier and dossier.nb_rectificatifs > item.nb_rectifs_at_add:
            updates.append({'dossier': dossier, 'item': item})
    return updates


def send_alert_digest(user: User, alert_type: str = 'DAILY') -> bool:
    """Envoie un digest d'alertes à un utilisateur."""
    if not user.alert_enabled or not user.is_active:
        return False

    new_dossiers = _get_new_dossiers_for_user(user)
    watchlist_updates = _get_watchlist_updates(user)

    if not new_dossiers and not watchlist_updates:
        return True  # Rien à envoyer

    log = AlertLog(user_id=user.id, type_alerte=alert_type, nb_dossiers=len(new_dossiers))

    try:
        msg = Message(
            subject=f'[BOAMP Cohesity] {len(new_dossiers)} nouveau(x) appel(s) d\'offres',
            recipients=[user.email],
            html=render_template(
                'email/alert_digest.html',
                user=user,
                new_dossiers=new_dossiers,
                watchlist_updates=watchlist_updates,
                alert_type=alert_type,
                base_url=_external_url('/'),
            ),
        )
        mail.send(msg)

        user.alert_last_sent = datetime.utcnow()
        log.success = True
        db.session.add(log)
        db.session.commit()

        logger.info("Digest %s envoyé à %s (%d dossiers)", alert_type, user.email, len(new_dossiers))
        return True

    except Exception as exc:
        logger.error("Erreur digest %s à %s : %s", alert_type, user.email, exc)
        log.success = False
        log.error_msg = str(exc)
        db.session.add(log)
        db.session.commit()
        return False
