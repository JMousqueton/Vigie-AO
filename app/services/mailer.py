"""
Service d'envoi d'emails : alertes BOAMP, confirmation, invitation.
"""
import logging
from datetime import timedelta

from app.utils import utc_now

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
    """Retourne les dossiers pertinents depuis la dernière alerte, filtrés par pays.

    - FR  → BOAMP (toujours FR) + TED France
    - EU  → tous les dossiers TED toutes sources
    - Autre → TED uniquement pour ce pays
    """
    from app import db
    from datetime import date
    cutoff_dt = user.alert_last_sent or (utc_now() - timedelta(days=7))
    cutoff_date = cutoff_dt.date() if hasattr(cutoff_dt, 'date') else cutoff_dt
    user_country = getattr(user, 'country', 'FR') or 'FR'

    base = DossierCache.query.filter(
        DossierCache.date_derniere_activite >= cutoff_date,
        DossierCache.score_pertinence > 0,
        DossierCache.is_duplicate == False,  # noqa: E712
    )

    if user_country == 'FR':
        # BOAMP (toujours France) + TED France
        base = base.filter(
            db.or_(
                DossierCache.source == 'BOAMP',
                db.and_(DossierCache.source == 'TED', DossierCache.country == 'FR'),
            )
        )
    elif user_country == 'EU':
        base = base.filter(db.or_(
            DossierCache.source == 'TED',
            DossierCache.source == 'PLACE_ES',
        ))
    else:
        # TED + sources nationales pour le pays de l'utilisateur
        base = base.filter(
            DossierCache.source != 'BOAMP',
            DossierCache.country == user_country,
        )

    # Mots-clés d'exclusion : global ∪ spécifiques au pays de l'utilisateur
    from app.services.keywords import get_exclude_keywords
    exclude_kws = get_exclude_keywords(country=user_country)
    if exclude_kws:
        for kw in exclude_kws:
            base = base.filter(
                DossierCache.objet_marche.notilike(f'%{kw}%'),
                db.or_(
                    DossierCache.descripteur_libelle.is_(None),
                    DossierCache.descripteur_libelle.notilike(f'%{kw}%'),
                ),
            )

    return base.order_by(DossierCache.score_pertinence.desc()).limit(50).all()


def _get_watchlist_updates(user: User) -> list[dict]:
    """Retourne les dossiers watchlistés avec de nouveaux rectificatifs."""
    items = list(user.watchlist_items)
    if not items:
        return []
    idwebs = [item.idweb for item in items]
    dossiers_map = {
        d.idweb: d
        for d in DossierCache.query.filter(DossierCache.idweb.in_(idwebs)).all()
    }
    return [
        {'dossier': d, 'item': item}
        for item in items
        if (d := dossiers_map.get(item.idweb)) and d.nb_rectificatifs > item.nb_rectifs_at_add
    ]


def send_alert_digest(user: User, alert_type: str = 'DAILY', force: bool = False) -> bool:
    """Envoie un digest d'alertes à un utilisateur.
    force=True bypasse la garde alert_enabled (usage CLI --user).
    """
    if not force and (not user.alert_enabled or not user.is_active):
        return False

    new_dossiers = _get_new_dossiers_for_user(user)
    watchlist_updates = _get_watchlist_updates(user)

    if not new_dossiers and not watchlist_updates:
        return 'empty'  # Rien à envoyer

    log = AlertLog(user_id=user.id, type_alerte=alert_type, nb_dossiers=len(new_dossiers))

    lang = 'fr' if (getattr(user, 'country', 'FR') or 'FR') == 'FR' else 'en'
    if lang == 'en':
        subject = f'[Vigie AO] {len(new_dossiers)} new tender(s)'
        template = 'email/alert_digest_en.html'
    else:
        subject = f'[Vigie AO] {len(new_dossiers)} nouveau(x) appel(s) d\'offres'
        template = 'email/alert_digest_fr.html'

    try:
        msg = Message(
            subject=subject,
            recipients=[user.email],
            html=render_template(
                template,
                user=user,
                new_dossiers=new_dossiers,
                watchlist_updates=watchlist_updates,
                alert_type=alert_type,
                base_url=_external_url('/'),
            ),
        )
        mail.send(msg)

        user.alert_last_sent = utc_now()
        log.success = True
        db.session.add(log)
        db.session.commit()

        logger.info("Digest %s envoyé à %s (%d dossiers)", alert_type, user.email, len(new_dossiers))
        return {'sent': True, 'new_dossiers': len(new_dossiers), 'watchlist': len(watchlist_updates)}

    except Exception as exc:
        logger.error("Erreur digest %s à %s : %s", alert_type, user.email, exc)
        log.success = False
        log.error_msg = str(exc)
        db.session.add(log)
        db.session.commit()
        return {'sent': False, 'error': str(exc)}
