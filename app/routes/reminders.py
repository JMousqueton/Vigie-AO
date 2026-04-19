"""
Routes pense-bêtes : ajout, retrait, liste, export ICS.
"""
import uuid
from datetime import datetime, timedelta

from app.utils import utc_now

from urllib.parse import urlencode

from flask import Blueprint, render_template, redirect, url_for, flash, request, Response, abort
from flask_login import login_required, current_user
from sqlalchemy.exc import IntegrityError

from app import db
from app.models import Reminder, DossierCache

reminders_bp = Blueprint('reminders', __name__)


@reminders_bp.route('/')
@login_required
def index():
    items = (
        Reminder.query
        .filter_by(user_id=current_user.id)
        .order_by(Reminder.end_date.asc().nullslast(), Reminder.created_at.desc())
        .all()
    )
    return render_template('reminders/index.html', reminders=items, today=utc_now().date())


@reminders_bp.route('/add/<idweb>', methods=['POST'])
@login_required
def add(idweb):
    dossier = DossierCache.query.filter_by(idweb=idweb).first_or_404()

    end_date_str = request.form.get('end_date', '').strip()
    note         = request.form.get('note', '').strip()

    end_date = None
    if end_date_str:
        try:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        except ValueError:
            flash('Date invalide.', 'warning')
            return redirect(url_for('main.detail', idweb=idweb))

    existing = Reminder.query.filter_by(user_id=current_user.id, idweb=idweb).first()
    if existing:
        existing.end_date     = end_date
        existing.note         = note
        existing.objet_marche = dossier.objet_marche
        existing.acheteur_nom = dossier.acheteur_nom
        db.session.commit()
        flash('Pense-bête mis à jour.', 'success')
    else:
        reminder = Reminder(
            user_id      = current_user.id,
            idweb        = idweb,
            objet_marche = dossier.objet_marche,
            acheteur_nom = dossier.acheteur_nom,
            end_date     = end_date,
            note         = note,
        )
        try:
            db.session.add(reminder)
            db.session.commit()
            flash('Pense-bête ajouté.', 'success')
        except IntegrityError:
            db.session.rollback()
            flash('Ce dossier est déjà dans vos pense-bêtes.', 'info')

    return redirect(url_for('main.detail', idweb=idweb))


@reminders_bp.route('/remove/<idweb>', methods=['POST'])
@login_required
def remove(idweb):
    reminder = Reminder.query.filter_by(
        user_id=current_user.id, idweb=idweb
    ).first_or_404()
    db.session.delete(reminder)
    db.session.commit()
    flash('Pense-bête supprimé.', 'info')
    next_page = request.form.get('next') or url_for('reminders.index')
    return redirect(next_page)


@reminders_bp.route('/ics/<int:reminder_id>')
@login_required
def download_ics(reminder_id):
    """Génère un fichier ICS avec une alarme 365 jours avant la fin de marché."""
    reminder = Reminder.query.filter_by(
        id=reminder_id, user_id=current_user.id
    ).first_or_404()

    if not reminder.end_date:
        abort(400, 'Ce pense-bête n\'a pas de date de fin de marché.')

    alarm_trigger = reminder.end_date - timedelta(days=365)
    now_utc = utc_now().strftime('%Y%m%dT%H%M%SZ')
    dtstart = reminder.end_date.strftime('%Y%m%d')
    dtend   = (reminder.end_date + timedelta(days=1)).strftime('%Y%m%d')
    alarm_dt = alarm_trigger.strftime('%Y%m%d')

    summary = (reminder.objet_marche or reminder.idweb).replace('\n', ' ')
    description = f'Fin de marché — {reminder.idweb}'
    if reminder.acheteur_nom:
        description += f'\\nAcheteur : {reminder.acheteur_nom}'
    if reminder.note:
        description += f'\\nNote : {reminder.note}'

    ics = '\r\n'.join([
        'BEGIN:VCALENDAR',
        'VERSION:2.0',
        'PRODID:-//Vigie AO//Cohesity//FR',
        'CALSCALE:GREGORIAN',
        'METHOD:PUBLISH',
        'BEGIN:VEVENT',
        f'UID:{uuid.uuid4()}@vigie-ao',
        f'DTSTAMP:{now_utc}',
        f'DTSTART;VALUE=DATE:{dtstart}',
        f'DTEND;VALUE=DATE:{dtend}',
        f'SUMMARY:Fin de marché — {summary}',
        f'DESCRIPTION:{description}',
        f'URL:{reminder.idweb}',
        'BEGIN:VALARM',
        'ACTION:DISPLAY',
        f'TRIGGER;VALUE=DATE-TIME:{alarm_dt}T080000Z',
        f'DESCRIPTION:Rappel J-365 — Fin de marché dans 1 an : {summary}',
        'END:VALARM',
        'END:VEVENT',
        'END:VCALENDAR',
        '',
    ])

    filename = f'vigie-ao-{reminder.idweb}.ics'
    return Response(
        ics,
        mimetype='text/calendar; charset=utf-8',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'},
    )


@reminders_bp.route('/outlook/<int:reminder_id>')
@login_required
def outlook_event(reminder_id):
    """
    Redirige vers Outlook Web pour créer un événement J-365 avant la fin de marché.

    L'URL de composition Outlook ne supporte pas les alarmes via paramètres,
    donc l'événement est créé directement sur la date J-365 — il joue lui-même
    le rôle de rappel, avec un titre et un corps qui mentionnent la date de fin réelle.
    """
    reminder = Reminder.query.filter_by(
        id=reminder_id, user_id=current_user.id
    ).first_or_404()

    if not reminder.end_date:
        abort(400, 'Ce pense-bête n\'a pas de date de fin de marché.')

    event_date = reminder.end_date - timedelta(days=365)
    summary    = (reminder.objet_marche or reminder.idweb).replace('\n', ' ')

    body_lines = [
        f'⚠️ Ce marché se termine le {reminder.end_date.strftime("%d/%m/%Y")} (dans 1 an).',
        '',
        f'Référence : {reminder.idweb}',
    ]
    if reminder.acheteur_nom:
        body_lines.append(f'Acheteur : {reminder.acheteur_nom}')
    if reminder.note:
        body_lines.append(f'Note : {reminder.note}')

    params = urlencode({
        'subject':  f'⚠️ J-365 Fin de marché — {summary}',
        'startdt':  event_date.isoformat(),
        'enddt':    event_date.isoformat(),
        'allday':   'true',
        'body':     '\n'.join(body_lines),
    })

    outlook_url = f'https://outlook.office.com/calendar/action/compose?{params}'
    return redirect(outlook_url)
