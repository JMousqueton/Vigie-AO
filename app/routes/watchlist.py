"""
Routes watchlist : ajout, retrait, liste.
"""
from flask import Blueprint, render_template, redirect, url_for, flash, request, jsonify
from flask_login import login_required, current_user
from sqlalchemy.exc import IntegrityError

from app import db
from app.models import WatchlistItem, DossierCache

watchlist_bp = Blueprint('watchlist', __name__)


@watchlist_bp.route('/')
@login_required
def index():
    items = WatchlistItem.query.filter_by(user_id=current_user.id).order_by(
        WatchlistItem.added_at.desc()
    ).all()

    # Enrichir avec les données du cache — une seule requête IN()
    idwebs = [item.idweb for item in items]
    dossiers_map: dict[str, DossierCache] = {}
    if idwebs:
        dossiers_map = {
            d.idweb: d
            for d in DossierCache.query.filter(DossierCache.idweb.in_(idwebs)).all()
        }

    enriched = []
    for item in items:
        dossier = dossiers_map.get(item.idweb)
        new_rectifs = max(0, dossier.nb_rectificatifs - item.nb_rectifs_at_add) if dossier else 0
        enriched.append({
            'item': item,
            'dossier': dossier,
            'new_rectifs': new_rectifs,
        })

    return render_template('watchlist/index.html', enriched=enriched)


@watchlist_bp.route('/add/<idweb>', methods=['POST'])
@login_required
def add(idweb):
    dossier = DossierCache.query.filter_by(idweb=idweb).first_or_404()
    note = request.form.get('note', '').strip()

    existing = WatchlistItem.query.filter_by(
        user_id=current_user.id, idweb=idweb
    ).first()

    if existing:
        existing.note = note
        db.session.commit()
        flash('Note mise à jour dans votre watchlist.', 'success')
    else:
        item = WatchlistItem(
            user_id=current_user.id,
            idweb=idweb,
            note=note,
            nb_rectifs_at_add=dossier.nb_rectificatifs,
        )
        try:
            db.session.add(item)
            db.session.commit()
            flash('Dossier ajouté à votre watchlist.', 'success')
        except IntegrityError:
            db.session.rollback()
            flash('Ce dossier est déjà dans votre watchlist.', 'info')

    next_page = request.form.get('next') or url_for('main.detail', idweb=idweb)
    return redirect(next_page)


@watchlist_bp.route('/remove/<idweb>', methods=['POST'])
@login_required
def remove(idweb):
    item = WatchlistItem.query.filter_by(
        user_id=current_user.id, idweb=idweb
    ).first_or_404()

    db.session.delete(item)
    db.session.commit()
    flash('Dossier retiré de votre watchlist.', 'info')

    next_page = request.form.get('next') or url_for('watchlist.index')
    return redirect(next_page)


@watchlist_bp.route('/toggle/<idweb>', methods=['POST'])
@login_required
def toggle(idweb):
    """API JSON pour le toggle watchlist depuis les cards."""
    dossier = DossierCache.query.filter_by(idweb=idweb).first_or_404()
    existing = WatchlistItem.query.filter_by(
        user_id=current_user.id, idweb=idweb
    ).first()

    if existing:
        db.session.delete(existing)
        db.session.commit()
        return jsonify({'status': 'removed', 'idweb': idweb})
    else:
        item = WatchlistItem(
            user_id=current_user.id,
            idweb=idweb,
            nb_rectifs_at_add=dossier.nb_rectificatifs,
        )
        db.session.add(item)
        db.session.commit()
        return jsonify({'status': 'added', 'idweb': idweb})
