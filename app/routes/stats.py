"""
Routes statistiques — superviseurs et admins uniquement.
"""
from datetime import timedelta

from app.utils import utc_now
from functools import wraps

from flask import Blueprint, flash, redirect, render_template, url_for
from flask_login import current_user, login_required
from sqlalchemy import func

from app import db
from app.models import AlertLog, DossierCache, Reminder, SharedLink, User, WatchlistItem

stats_bp = Blueprint('stats', __name__)

_COUNTRY_NAMES = {
    'FR': 'France', 'BE': 'Belgique', 'CH': 'Suisse',
    'LU': 'Luxembourg', 'DE': 'Allemagne', 'ES': 'Espagne',
    'IT': 'Italie', 'NL': 'Pays-Bas', 'PT': 'Portugal',
    'AT': 'Autriche', 'PL': 'Pologne', 'SE': 'Suède',
    'DK': 'Danemark', 'FI': 'Finlande', 'NO': 'Norvège',
    'GB': 'Royaume-Uni', 'IE': 'Irlande', 'EU': 'Europe (multi)',
}


def supervisor_or_admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or \
                not (current_user.is_admin or current_user.is_supervisor):
            flash('Access restricted to supervisors and administrators.', 'danger')
            return redirect(url_for('main.dashboard'))
        return f(*args, **kwargs)
    return decorated


@stats_bp.route('/stats')
@login_required
@supervisor_or_admin_required
def index():
    today = utc_now().date()

    # ── Dossiers ─────────────────────────────────────────────────────────────
    total_dossiers  = DossierCache.query.count()
    unique_dossiers = DossierCache.query.filter_by(is_duplicate=False).count()
    nb_duplicates   = total_dossiers - unique_dossiers
    nb_attributions = DossierCache.query.filter_by(has_attribution=True, is_duplicate=False).count()
    nb_rectifs      = DossierCache.query.filter_by(has_rectificatif=True, is_duplicate=False).count()
    nb_urgent = DossierCache.query.filter(
        DossierCache.datelimitereponse >= today,
        DossierCache.datelimitereponse <= today + timedelta(days=14),
        DossierCache.has_attribution == False,
        DossierCache.is_duplicate == False,
    ).count()
    nb_expired = DossierCache.query.filter(
        DossierCache.datelimitereponse < today,
        DossierCache.has_attribution == False,
        DossierCache.is_duplicate == False,
    ).count()
    new_this_week = DossierCache.query.filter(
        DossierCache.dateparution >= today - timedelta(days=7),
        DossierCache.is_duplicate == False,
    ).count()
    new_today = DossierCache.query.filter(
        DossierCache.dateparution == today,
        DossierCache.is_duplicate == False,
    ).count()

    last_fetch = db.session.query(func.max(DossierCache.fetched_at)).scalar()
    cache_age_h = None
    if last_fetch:
        cache_age_h = round((utc_now() - last_fetch).total_seconds() / 3600, 1)
    if cache_age_h is not None and cache_age_h < 1:
        cache_age_label = f"{int((utc_now() - last_fetch).total_seconds() / 60)} min"
    elif cache_age_h is not None:
        cache_age_label = f"{cache_age_h} h"
    else:
        cache_age_label = '—'

    attrib_pct = round(nb_attributions / unique_dossiers * 100) if unique_dossiers else 0
    rectif_pct = round(nb_rectifs / unique_dossiers * 100) if unique_dossiers else 0

    # ── By source ────────────────────────────────────────────────────────────
    by_source_raw = db.session.query(
        DossierCache.source, func.count()
    ).filter(DossierCache.is_duplicate == False).group_by(DossierCache.source).all()
    by_source = {row[0]: row[1] for row in by_source_raw}

    # ── By country (top 12) ──────────────────────────────────────────────────
    by_country_raw = db.session.query(
        DossierCache.country, func.count()
    ).filter(
        DossierCache.is_duplicate == False,
        DossierCache.country.isnot(None),
        DossierCache.country != '',
    ).group_by(DossierCache.country).order_by(func.count().desc()).limit(12).all()
    by_country = [
        {'code': row[0], 'name': _COUNTRY_NAMES.get(row[0], row[0]), 'count': row[1]}
        for row in by_country_raw
    ]

    # ── By nature ────────────────────────────────────────────────────────────
    by_nature_raw = db.session.query(
        DossierCache.nature, func.count()
    ).filter(DossierCache.is_duplicate == False).group_by(DossierCache.nature).order_by(func.count().desc()).all()
    by_nature = [
        {'name': (row[0] or 'Inconnu').replace('_', ' '), 'count': row[1]}
        for row in by_nature_raw
    ]

    # ── Score distribution ───────────────────────────────────────────────────
    score_ranges = [
        ('0 — non scoré', 0, 0),
        ('1–20', 1, 20),
        ('21–40', 21, 40),
        ('41–60', 41, 60),
        ('61–80', 61, 80),
        ('81–100', 81, 100),
    ]
    score_dist = []
    for label, low, high in score_ranges:
        c = DossierCache.query.filter(
            DossierCache.score_pertinence >= low,
            DossierCache.score_pertinence <= high,
            DossierCache.is_duplicate == False,
        ).count()
        score_dist.append({'label': label, 'count': c})

    # ── Publications timeline (30 days) ──────────────────────────────────────
    thirty_ago = today - timedelta(days=29)
    timeline_raw = db.session.query(
        DossierCache.dateparution, func.count()
    ).filter(
        DossierCache.dateparution >= thirty_ago,
        DossierCache.dateparution <= today,
        DossierCache.is_duplicate == False,
    ).group_by(DossierCache.dateparution).order_by(DossierCache.dateparution).all()
    timeline_map = {str(row[0]): row[1] for row in timeline_raw}
    timeline = []
    for i in range(30):
        d = thirty_ago + timedelta(days=i)
        timeline.append({'date': d.strftime('%d/%m'), 'count': timeline_map.get(str(d), 0)})

    # ── Users ────────────────────────────────────────────────────────────────
    total_users    = User.query.count()
    active_users   = User.query.filter_by(is_active=True).count()
    inactive_users = total_users - active_users
    nb_admin       = User.query.filter_by(role='ADMIN').count()
    nb_supervisor  = User.query.filter_by(role='SUPERVISEUR').count()
    nb_regular     = User.query.filter_by(role='USER').count()
    alerts_on      = User.query.filter_by(is_active=True, alert_enabled=True).count()
    recent_logins  = User.query.filter(
        User.last_login >= utc_now() - timedelta(days=7)
    ).count()
    freq_immediate = User.query.filter_by(is_active=True, alert_enabled=True, alert_frequency='IMMEDIATE').count()
    freq_daily     = User.query.filter_by(is_active=True, alert_enabled=True, alert_frequency='DAILY').count()
    freq_weekly    = User.query.filter_by(is_active=True, alert_enabled=True, alert_frequency='WEEKLY').count()

    # ── Alert logs ───────────────────────────────────────────────────────────
    total_alerts = AlertLog.query.count()
    alerts_ok    = AlertLog.query.filter_by(success=True).count()
    alerts_fail  = AlertLog.query.filter_by(success=False).count()
    alert_rate   = round(alerts_ok / total_alerts * 100) if total_alerts else 0

    month_start  = utc_now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    alerts_month = AlertLog.query.filter(AlertLog.sent_at >= month_start).count()

    # Alert activity timeline (30 days)
    alert_tl_raw = db.session.query(
        func.date(AlertLog.sent_at), func.count()
    ).filter(
        AlertLog.sent_at >= utc_now() - timedelta(days=29)
    ).group_by(func.date(AlertLog.sent_at)).order_by(func.date(AlertLog.sent_at)).all()
    alert_tl_map = {str(row[0]): row[1] for row in alert_tl_raw}
    alert_timeline = []
    for i in range(30):
        d = (utc_now() - timedelta(days=29 - i)).date()
        alert_timeline.append({'date': d.strftime('%d/%m'), 'count': alert_tl_map.get(str(d), 0)})

    # ── Engagement ───────────────────────────────────────────────────────────
    nb_watchlist = WatchlistItem.query.count()
    nb_reminders = Reminder.query.count()
    nb_shared    = SharedLink.query.count()

    return render_template('stats/index.html',
        # dossiers KPIs
        total_dossiers=total_dossiers,
        unique_dossiers=unique_dossiers,
        nb_duplicates=nb_duplicates,
        nb_attributions=nb_attributions,
        attrib_pct=attrib_pct,
        nb_rectifs=nb_rectifs,
        rectif_pct=rectif_pct,
        nb_urgent=nb_urgent,
        nb_expired=nb_expired,
        new_this_week=new_this_week,
        new_today=new_today,
        last_fetch=last_fetch,
        cache_age_h=cache_age_h,
        cache_age_label=cache_age_label,
        # chart data (passed as Python objects, serialised by tojson in template)
        by_source=by_source,
        by_country=by_country,
        by_nature=by_nature,
        score_dist=score_dist,
        timeline=timeline,
        alert_timeline=alert_timeline,
        # users
        total_users=total_users,
        active_users=active_users,
        inactive_users=inactive_users,
        nb_admin=nb_admin,
        nb_supervisor=nb_supervisor,
        nb_regular=nb_regular,
        alerts_on=alerts_on,
        recent_logins=recent_logins,
        freq_immediate=freq_immediate,
        freq_daily=freq_daily,
        freq_weekly=freq_weekly,
        # alerts
        total_alerts=total_alerts,
        alerts_ok=alerts_ok,
        alerts_fail=alerts_fail,
        alert_rate=alert_rate,
        alerts_month=alerts_month,
        # engagement
        nb_watchlist=nb_watchlist,
        nb_reminders=nb_reminders,
        nb_shared=nb_shared,
    )
