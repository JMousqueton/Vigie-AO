"""
Routes principales : dashboard, détail dossier, refresh manuel, partage.
"""
import json
import secrets
from datetime import datetime, date, timedelta

from flask import (
    Blueprint, render_template, redirect, url_for,
    flash, request, jsonify, current_app, session,
)
from flask_login import login_required, current_user
from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError

from app import db
from app.models import DossierCache, WatchlistItem, SharedLink, UserSeenDossier, UserHiddenDossier

main_bp = Blueprint('main', __name__)

PER_PAGE = 20


def _get_watchlist_ids(user_id: int) -> set[str]:
    items = WatchlistItem.query.filter_by(user_id=user_id).all()
    return {item.idweb for item in items}


@main_bp.route('/')
@login_required
def index():
    return redirect(url_for('main.dashboard'))


@main_bp.route('/dashboard')
@login_required
def dashboard():
    # Paramètres filtres
    filtre = request.args.get('filtre', 'tous')
    search = request.args.get('q', '').strip()
    dept = request.args.get('dept', '').strip()
    tri = request.args.get('tri', 'date_desc')
    page = request.args.get('page', 1, type=int)
    expire = request.args.get('expire', 'avec')    # 'avec' | 'sans'
    attribue = request.args.get('attribue', 'avec')  # 'avec' | 'sans'
    periode = request.args.get('periode', 'tous')    # 'tous' | 'actifs'
    sources = request.args.getlist('sources')        # ['BOAMP', 'TED'] — vide = toutes

    # Dossiers masqués par l'utilisateur (calculé avant le filtre)
    hidden_rows = UserHiddenDossier.query.filter_by(user_id=current_user.id).all()
    hidden_ids = {r.idweb for r in hidden_rows}

    query = DossierCache.query.filter(DossierCache.is_duplicate == False)

    # Filtre pays — superviseur uniquement (session), admin voit tout
    supervisor_country = None
    if current_user.is_supervisor:
        supervisor_country = session.get('supervisor_country', current_user.country or 'FR')
        if supervisor_country == 'FR':
            query = query.filter(
                or_(
                    DossierCache.source == 'BOAMP',
                    db.and_(DossierCache.source == 'TED', DossierCache.country == 'FR'),
                )
            )
        elif supervisor_country == 'EU':
            query = query.filter(DossierCache.source == 'TED')
        else:
            query = query.filter(
                DossierCache.source == 'TED',
                DossierCache.country == supervisor_country,
            )

    # Mots-clés d'exclusion (admin)
    from app.services.keywords import get_exclude_keywords
    exclude_kws = get_exclude_keywords()
    if exclude_kws and filtre not in ('watchlist', 'hidden'):
        for kw in exclude_kws:
            query = query.filter(DossierCache.objet_marche.notilike(f'%{kw}%'))

    # Filtre type
    if filtre == 'hidden':
        # Afficher uniquement les dossiers masqués
        if hidden_ids:
            query = query.filter(DossierCache.idweb.in_(hidden_ids))
        else:
            query = query.filter(DossierCache.id == -1)  # rien
    elif filtre == 'avis':
        query = query.filter(
            DossierCache.has_rectificatif == False,
            DossierCache.has_attribution == False,
        ).filter(DossierCache.idweb.notin_(hidden_ids) if hidden_ids else True)
    elif filtre == 'rectificatifs':
        query = query.filter(DossierCache.has_rectificatif == True)
        if hidden_ids:
            query = query.filter(DossierCache.idweb.notin_(hidden_ids))
    elif filtre == 'attributions':
        query = query.filter(DossierCache.has_attribution == True)
        if hidden_ids:
            query = query.filter(DossierCache.idweb.notin_(hidden_ids))
    elif filtre == 'watchlist':
        wl_ids = _get_watchlist_ids(current_user.id)
        if wl_ids:
            query = query.filter(DossierCache.idweb.in_(wl_ids))
        else:
            query = query.filter(DossierCache.id == -1)  # résultat vide
        if hidden_ids:
            query = query.filter(DossierCache.idweb.notin_(hidden_ids))
    else:  # 'tous'
        if hidden_ids:
            query = query.filter(DossierCache.idweb.notin_(hidden_ids))

    # Score minimum : exclure les dossiers sans aucun déclencheur pertinent
    # (sauf watchlist/hidden où l'utilisateur a fait un choix explicite)
    if filtre not in ('watchlist', 'hidden'):
        query = query.filter(DossierCache.score_pertinence > 0)

    # Filtre département
    if dept:
        query = query.filter(DossierCache.code_departement == dept)

    # Filtre expirés
    if expire == 'sans':
        today = date.today()
        query = query.filter(
            or_(
                DossierCache.datelimitereponse.is_(None),
                DossierCache.datelimitereponse >= today,
            )
        )

    # Filtre attribués
    if attribue == 'sans':
        query = query.filter(DossierCache.has_attribution == False)

    # Filtre période (actifs = parus dans les 90 derniers jours)
    if periode == 'actifs':
        cutoff = date.today() - timedelta(days=90)
        query = query.filter(DossierCache.dateparution >= cutoff)

    # Filtre source
    valid_sources = {'BOAMP', 'TED'}
    sources = [s for s in sources if s in valid_sources]
    if sources:
        query = query.filter(DossierCache.source.in_(sources))

    # Recherche textuelle
    if search:
        like_pattern = f'%{search}%'
        query = query.filter(
            or_(
                DossierCache.objet_marche.ilike(like_pattern),
                DossierCache.acheteur_nom.ilike(like_pattern),
                DossierCache.descripteur_libelle.ilike(like_pattern),
                DossierCache.reference_boamp_initial.ilike(like_pattern),
                DossierCache.idweb.ilike(like_pattern),
            )
        )

    # Tri
    if tri == 'score_desc':
        query = query.order_by(DossierCache.score_pertinence.desc(), DossierCache.date_derniere_activite.desc())
    elif tri == 'date_asc':
        query = query.order_by(DossierCache.date_derniere_activite.asc())
    elif tri == 'deadline_asc':
        query = query.order_by(
            DossierCache.datelimitereponse.asc().nullslast(),
            DossierCache.date_derniere_activite.desc(),
        )
    else:  # date_desc (défaut)
        query = query.order_by(DossierCache.date_derniere_activite.desc())

    pagination = query.paginate(page=page, per_page=PER_PAGE, error_out=False)

    # Rediriger vers la dernière page valide si page hors-limites
    if page > pagination.pages > 0:
        return redirect(url_for('main.dashboard',
            page=pagination.pages, filtre=filtre, q=search, dept=dept,
            tri=tri, expire=expire, attribue=attribue, periode=periode,
            sources=sources,
        ))

    dossiers = pagination.items

    # Watchlist IDs pour afficher les étoiles
    watchlist_ids = _get_watchlist_ids(current_user.id)

    # Dossiers déjà vus par l'utilisateur (pour le ruban NEW)
    page_idwebs = [d.idweb for d in dossiers]
    if page_idwebs:
        seen_rows = UserSeenDossier.query.filter(
            UserSeenDossier.user_id == current_user.id,
            UserSeenDossier.idweb.in_(page_idwebs),
        ).all()
        seen_ids = {r.idweb for r in seen_rows}
    else:
        seen_ids = set()

    # Stats générales — entièrement en SQL, sans charger les objets en mémoire
    today = date.today()
    deadline_cutoff = today + timedelta(days=14)
    total = DossierCache.query.count()
    nb_rectifs = DossierCache.query.filter(DossierCache.has_rectificatif == True).count()
    nb_attributions = DossierCache.query.filter(DossierCache.has_attribution == True).count()
    nb_urgents = DossierCache.query.filter(
        DossierCache.datelimitereponse >= today,
        DossierCache.datelimitereponse <= deadline_cutoff,
        DossierCache.has_attribution == False,
    ).count()

    # Dernière mise à jour cache + indicateur de fraîcheur
    last_fetched = db.session.query(db.func.max(DossierCache.fetched_at)).scalar()
    if last_fetched:
        age_hours = (datetime.utcnow() - last_fetched).total_seconds() / 3600
        if age_hours < 4:
            freshness = 'fresh'    # vert
        elif age_hours < 24:
            freshness = 'stale'    # orange
        else:
            freshness = 'old'      # rouge
    else:
        freshness = 'old'

    # Départements disponibles pour le filtre
    depts = db.session.query(DossierCache.code_departement).filter(
        DossierCache.code_departement.isnot(None)
    ).distinct().order_by(DossierCache.code_departement).all()
    dept_list = [d[0] for d in depts if d[0]]

    return render_template(
        'main/dashboard.html',
        dossiers=dossiers,
        pagination=pagination,
        watchlist_ids=watchlist_ids,
        seen_ids=seen_ids,
        hidden_ids=hidden_ids,
        nb_hidden=len(hidden_ids),
        filtre=filtre,
        search=search,
        dept=dept,
        tri=tri,
        expire=expire,
        attribue=attribue,
        periode=periode,
        sources=sources,
        all_sources=['BOAMP', 'TED'],
        total=total,
        nb_rectifs=nb_rectifs,
        nb_attributions=nb_attributions,
        nb_urgents=nb_urgents,
        last_fetched=last_fetched,
        freshness=freshness,
        dept_list=dept_list,
        today=today,
        supervisor_country=supervisor_country,
    )


@main_bp.route('/set-supervisor-country')
@login_required
def set_supervisor_country():
    """Permet à un superviseur de changer le pays affiché dans le dashboard."""
    if not current_user.is_supervisor:
        return redirect(url_for('main.dashboard'))
    from app.routes.auth import COUNTRY_CHOICES
    valid = {c[0] for c in COUNTRY_CHOICES}
    country = request.args.get('country', 'FR')
    if country in valid:
        session['supervisor_country'] = country
    return redirect(url_for('main.dashboard'))


@main_bp.route('/mark-seen', methods=['POST'])
@login_required
def mark_seen():
    """Marque une liste d'idwebs comme vus pour l'utilisateur courant."""
    data = request.get_json(silent=True) or {}
    idwebs = data.get('idwebs', [])
    if not isinstance(idwebs, list):
        return jsonify({'ok': False}), 400

    idwebs = idwebs[:50]  # sécurité : max 50 par appel
    now = datetime.utcnow()

    # Récupérer en une seule requête ceux déjà vus
    already_seen = {
        r.idweb for r in UserSeenDossier.query.filter(
            UserSeenDossier.user_id == current_user.id,
            UserSeenDossier.idweb.in_(idwebs),
        ).all()
    }

    for idweb in idwebs:
        if idweb not in already_seen:
            db.session.add(UserSeenDossier(user_id=current_user.id, idweb=idweb, seen_at=now))

    try:
        db.session.commit()
    except IntegrityError:
        # Race condition multi-onglets : on ignore, les lignes existent déjà
        db.session.rollback()
    return jsonify({'ok': True, 'marked': len(idwebs)})


@main_bp.route('/hide-toggle/<idweb>', methods=['POST'])
@login_required
def hide_toggle(idweb):
    """Masque ou démasque un dossier pour l'utilisateur courant."""
    existing = UserHiddenDossier.query.filter_by(user_id=current_user.id, idweb=idweb).first()
    if existing:
        db.session.delete(existing)
        db.session.commit()
        return jsonify({'status': 'shown', 'idweb': idweb})
    else:
        db.session.add(UserHiddenDossier(user_id=current_user.id, idweb=idweb))
        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
        return jsonify({'status': 'hidden', 'idweb': idweb})


@main_bp.route('/dossier/<idweb>')
@login_required
def detail(idweb):
    dossier = DossierCache.query.filter_by(idweb=idweb).first_or_404()

    # Calcul des diffs entre avis initial et rectificatifs
    from app.services.boamp_api import diff_rectificatif
    rectifs = dossier.rectificatifs
    diffs = []

    initial_data = {
        'objet_marche': dossier.objet_marche,
        'datelimitereponse': str(dossier.datelimitereponse) if dossier.datelimitereponse else None,
        'lieu_execution': dossier.lieu_execution,
        'acheteur_nom': dossier.acheteur_nom,
        'reference_boamp': dossier.reference_boamp_initial,
        'dateparution': str(dossier.dateparution) if dossier.dateparution else None,
    }
    precedents = [initial_data] + rectifs[:-1] if rectifs else [initial_data]

    for i, rectif in enumerate(rectifs):
        precedent = precedents[i] if i < len(precedents) else initial_data
        diff = diff_rectificatif(precedent, rectif)
        diffs.append({'rectificatif': rectif, 'diff': diff, 'index': i + 1})

    # Watchlist item
    wl_item = WatchlistItem.query.filter_by(
        user_id=current_user.id, idweb=idweb
    ).first()

    # Pense-bête
    from app.models import Reminder
    reminder_item = Reminder.query.filter_by(
        user_id=current_user.id, idweb=idweb
    ).first()

    # Données attribution enrichies
    attribution = dossier.attribution
    from app.services.boamp_api import extract_lots_titulaires, extract_contract_period
    lots_titulaires = extract_lots_titulaires(attribution) if attribution else []
    contract_periods = extract_contract_period(attribution) if attribution else []

    # Explication détaillée des déclencheurs (calculée à l'affichage, pas stockée)
    if dossier.source == 'TED':
        from app.services.ted_api import explain_ted_score
        trigger_details = explain_ted_score({
            'objet_marche':        dossier.objet_marche or '',
            'descripteur_libelle': dossier.descripteur_libelle or '',
        })
    else:
        from app.services.scoring import explain_score
        trigger_details = explain_score(
            objet_marche=dossier.objet_marche or '',
            descripteur_libelle=dossier.descripteur_libelle or '',
            famille_denomination=dossier.famille_denomination or '',
        )

    return render_template(
        'main/detail.html',
        dossier=dossier,
        diffs=diffs,
        wl_item=wl_item,
        attribution=attribution,
        lots_titulaires=lots_titulaires,
        contract_periods=contract_periods,
        reminder_item=reminder_item,
        trigger_details=trigger_details,
        today=date.today(),
    )


@main_bp.route('/refresh', methods=['POST'])
@login_required
def manual_refresh():
    if not current_user.is_admin:
        flash('Accès refusé.', 'danger')
        return redirect(url_for('main.dashboard'))

    try:
        from app.services.scheduler import refresh_boamp_cache
        refresh_boamp_cache(current_app._get_current_object())
        flash('Données BOAMP rafraîchies avec succès.', 'success')
        current_app.logger.info("Refresh manuel déclenché par %s", current_user.email)
    except Exception as exc:
        flash(f'Erreur lors du refresh : {exc}', 'danger')
        current_app.logger.error("Erreur refresh manuel : %s", exc)

    return redirect(url_for('main.dashboard'))


@main_bp.route('/set-theme', methods=['POST'])
@login_required
def set_theme():
    """Bascule entre dark et light, persiste en session + colonne User."""
    data = request.get_json(silent=True) or {}
    theme = data.get('theme', 'dark')
    if theme not in ('dark', 'light'):
        return jsonify({'error': 'invalid theme'}), 400

    session['theme'] = theme
    session.permanent = True

    current_user.theme = theme
    db.session.commit()

    return jsonify({'theme': theme})


@main_bp.route('/api/keywords')
@login_required
def api_keywords():
    """Retourne les mots-clés de détection en lecture seule (tous utilisateurs)."""
    from app.services.keywords import get_search_keywords, get_scoring_keywords
    kws = get_scoring_keywords()
    return jsonify({
        'search':  get_search_keywords(),
        'haute':   kws.get('haute', []),
        'moyenne': kws.get('moyenne', []),
        'contexte': kws.get('contexte', []),
    })


@main_bp.route('/api/stats')
@login_required
def api_stats():
    """Endpoint JSON pour les stats du dashboard."""
    total = DossierCache.query.count()
    nb_rectifs = DossierCache.query.filter(DossierCache.has_rectificatif == True).count()
    nb_attributions = DossierCache.query.filter(DossierCache.has_attribution == True).count()
    last_fetched = db.session.query(db.func.max(DossierCache.fetched_at)).scalar()

    return jsonify({
        'total': total,
        'nb_rectifs': nb_rectifs,
        'nb_attributions': nb_attributions,
        'last_fetched': last_fetched.isoformat() if last_fetched else None,
    })


# ─── Partage de dossier ───────────────────────────────────────────────────────

@main_bp.route('/dossier/<idweb>/share', methods=['POST'])
@login_required
def share_dossier(idweb):
    """Génère (ou retourne l'existant) un lien de partage pour ce dossier."""
    dossier = DossierCache.query.filter_by(idweb=idweb).first_or_404()

    # Réutiliser un lien existant non expiré créé par cet utilisateur
    existing = SharedLink.query.filter_by(
        idweb=idweb, created_by=current_user.id
    ).filter(
        (SharedLink.expires_at == None) | (SharedLink.expires_at > datetime.utcnow())
    ).first()

    if existing:
        token = existing.token
    else:
        token = secrets.token_urlsafe(32)   # 43 chars URL-safe, non-devinable
        link = SharedLink(
            token=token,
            idweb=idweb,
            created_by=current_user.id,
            expires_at=datetime.utcnow() + timedelta(days=90),
        )
        db.session.add(link)
        db.session.commit()

    share_url = url_for('main.view_shared', token=token, _external=True)
    return jsonify({'url': share_url, 'token': token})


@main_bp.route('/shared/<token>')
def view_shared(token):
    """Vue publique d'un dossier partagé — aucune authentification requise."""
    link = SharedLink.query.filter_by(token=token).first_or_404()

    if link.expires_at and link.expires_at < datetime.utcnow():
        return render_template('main/shared_expired.html'), 410

    dossier = DossierCache.query.filter_by(idweb=link.idweb).first_or_404()

    from app.services.boamp_api import extract_lots_titulaires
    attribution = dossier.attribution
    lots_titulaires = extract_lots_titulaires(attribution) if attribution else []

    return render_template(
        'main/shared.html',
        dossier=dossier,
        attribution=attribution,
        lots_titulaires=lots_titulaires,
        token=token,
    )
