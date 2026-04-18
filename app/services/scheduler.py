"""
Jobs APScheduler : refresh BOAMP et envoi d'alertes email.
"""
import json
import logging
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def _on_job_error(event):
    logger.error("Job APScheduler en erreur : %s — %s", event.job_id, event.exception)


# ─── Job : refresh cache BOAMP ────────────────────────────────────────────────

def refresh_boamp_cache(app=None):
    """Récupère les données BOAMP et met à jour le cache SQLite."""
    ctx_app = app or _get_app()
    if not ctx_app:
        return

    with ctx_app.app_context():
        from app.services.boamp_api import fetch_all_records, aggregate_into_dossiers
        from app.services.scoring import calculate_score
        from app.models import DossierCache
        from app import db

        logger.info("Début refresh BOAMP cache...")
        try:
            records = fetch_all_records()
            dossiers = aggregate_into_dossiers(records)

            # Marquer tous les dossiers existants comme non-nouveaux
            DossierCache.query.update({'is_new': False})

            updated = 0
            created = 0

            for dossier in dossiers:
                if not dossier.avis_initial and not dossier.rectificatifs and not dossier.attribution:
                    continue

                # Données de référence : avis initial > premier rectificatif > attribution
                # (les attributions BOAMP sont des notices INITIAL indépendantes — elles
                #  portent elles-mêmes toutes les infos : acheteur, objet, dates…)
                ref_data = (
                    dossier.avis_initial
                    or (dossier.rectificatifs[0] if dossier.rectificatifs else None)
                    or dossier.attribution
                    or {}
                )

                # Calcul du score
                score, mots_cles = calculate_score(
                    objet_marche=ref_data.get('objet_marche', ''),
                    descripteur_libelle=ref_data.get('descripteur_libelle', ''),
                    famille_denomination=ref_data.get('famille_denomination', ''),
                    country=ref_data.get('country') or None,
                )

                # Parsing de la date de dernière activité
                date_activite_str = dossier.date_derniere_activite
                try:
                    date_activite = datetime.strptime(date_activite_str, '%Y-%m-%d').date() if date_activite_str else None
                except ValueError:
                    date_activite = None

                def parse_date(d):
                    if not d:
                        return None
                    try:
                        return datetime.strptime(str(d)[:10], '%Y-%m-%d').date()
                    except ValueError:
                        return None

                existing = DossierCache.query.filter_by(idweb=dossier.idweb).first()
                if existing:
                    existing.acheteur_nom = ref_data.get('acheteur_nom')
                    existing.acheteur_siret = ref_data.get('acheteur_siret')
                    existing.objet_marche = ref_data.get('objet_marche')
                    existing.nature = ref_data.get('nature')
                    existing.type_marche = ref_data.get('type_marche')
                    existing.famille_denomination = ref_data.get('famille_denomination')
                    existing.descripteur_libelle = ref_data.get('descripteur_libelle')
                    existing.code_departement = ref_data.get('code_departement')
                    existing.lieu_execution = ref_data.get('lieu_execution')
                    existing.dateparution = parse_date(ref_data.get('dateparution'))
                    existing.datelimitereponse = parse_date(ref_data.get('datelimitereponse'))
                    existing.urlgravure = ref_data.get('urlgravure')
                    existing.reference_boamp_initial = ref_data.get('reference_boamp')
                    existing.contact_email = ref_data.get('contact_email') or ''
                    existing.rectificatifs_json = json.dumps(dossier.rectificatifs, ensure_ascii=False)
                    existing.attribution_json = json.dumps(dossier.attribution, ensure_ascii=False) if dossier.attribution else None
                    existing.score_pertinence = score
                    existing.mots_cles_matches = json.dumps(mots_cles, ensure_ascii=False)
                    existing.has_rectificatif = len(dossier.rectificatifs) > 0
                    existing.has_attribution = dossier.attribution is not None
                    existing.date_derniere_activite = date_activite
                    existing.fetched_at = datetime.utcnow()
                    existing.is_new = False
                    existing.source = 'BOAMP'
                    updated += 1
                else:
                    new_dossier = DossierCache(
                        idweb=dossier.idweb,
                        acheteur_nom=ref_data.get('acheteur_nom'),
                        acheteur_siret=ref_data.get('acheteur_siret'),
                        objet_marche=ref_data.get('objet_marche'),
                        nature=ref_data.get('nature'),
                        type_marche=ref_data.get('type_marche'),
                        famille_denomination=ref_data.get('famille_denomination'),
                        descripteur_libelle=ref_data.get('descripteur_libelle'),
                        code_departement=ref_data.get('code_departement'),
                        lieu_execution=ref_data.get('lieu_execution'),
                        dateparution=parse_date(ref_data.get('dateparution')),
                        datelimitereponse=parse_date(ref_data.get('datelimitereponse')),
                        urlgravure=ref_data.get('urlgravure'),
                        reference_boamp_initial=ref_data.get('reference_boamp'),
                        contact_email=ref_data.get('contact_email') or '',
                        rectificatifs_json=json.dumps(dossier.rectificatifs, ensure_ascii=False),
                        attribution_json=json.dumps(dossier.attribution, ensure_ascii=False) if dossier.attribution else None,
                        score_pertinence=score,
                        mots_cles_matches=json.dumps(mots_cles, ensure_ascii=False),
                        has_rectificatif=len(dossier.rectificatifs) > 0,
                        has_attribution=dossier.attribution is not None,
                        date_derniere_activite=date_activite,
                        fetched_at=datetime.utcnow(),
                        is_new=True,
                        source='BOAMP',
                    )
                    db.session.add(new_dossier)
                    created += 1

            db.session.commit()
            logger.info(
                "Refresh BOAMP terminé : %d créés, %d mis à jour",
                created, updated,
            )
            link_boamp_attributions(ctx_app)
            deduplicate_boamp_ted(ctx_app)
        except Exception as exc:
            db.session.rollback()
            logger.error("Erreur refresh BOAMP : %s", exc, exc_info=True)


# ─── Job : refresh cache TED ─────────────────────────────────────────────────

def refresh_ted_cache(app=None):
    """Récupère les données TED pour chaque pays des utilisateurs actifs."""
    ctx_app = app or _get_app()
    if not ctx_app:
        return

    with ctx_app.app_context():
        # Vérifier l'activation
        from app.models import AppConfig as _AppConfig
        row = _AppConfig.query.filter_by(key='source_TED_enabled').first()
        if row is not None:
            ted_on = row.value.lower() == 'true'
        else:
            ted_on = ctx_app.config.get('TED_ENABLED', False)
        if not ted_on:
            logger.info("TED désactivé — refresh ignoré.")
            return

        from app.services.ted_api import fetch_ted_records, compute_ted_score
        from app.models import DossierCache, User
        from app import db

        def parse_date(d):
            if not d:
                return None
            try:
                return datetime.strptime(str(d)[:10], '%Y-%m-%d').date()
            except ValueError:
                return None

        # Pays des utilisateurs actifs + pays déjà présents dans le cache TED
        active_countries = {
            u.country for u in User.query.filter_by(is_active=True).all()
            if u.country
        }
        # Inclure tous les pays déjà en base (TED) pour ne pas perdre les données existantes
        cached_countries = {
            row[0] for row in
            db.session.query(DossierCache.country)
            .filter_by(source='TED')
            .distinct()
            .all()
            if row[0]
        }
        active_countries |= cached_countries
        active_countries.add('FR')  # FR toujours inclus
        # Si PLACE_ES est activé, inclure ES dans les pays TED même sans utilisateur ES
        if ctx_app.config.get('PLACE_ES_ENABLED', False):
            active_countries.add('ES')

        logger.info("Refresh TED pour les pays : %s", sorted(active_countries))

        for country in sorted(active_countries):
            logger.info("Début refresh TED [%s]...", country)
            try:
                records = fetch_ted_records(country)
                if not records:
                    logger.info("TED [%s] : aucun avis récupéré.", country)
                    continue

                updated = 0
                created = 0

                for rec in records:
                    idweb = rec['idweb']
                    is_attribution = rec.get('_ted_is_attribution', False)
                    score, mots_cles = compute_ted_score(rec)
                    existing = DossierCache.query.filter_by(idweb=idweb).first()

                    if score == 0 and not existing:
                        logger.debug("TED : ignoré (score 0) %s — %s", idweb, rec.get('objet_marche', '')[:60])
                        continue

                    attribution_json = None
                    if is_attribution:
                        attribution_json = json.dumps({
                            'dateparution':    rec.get('dateparution', ''),
                            'urlgravure':      rec.get('urlgravure', ''),
                            'reference_boamp': rec.get('reference_boamp', ''),
                            'montant':         rec.get('montant', ''),
                        }, ensure_ascii=False)

                    rec_country = rec.get('country', country)
                    if existing:
                        existing.acheteur_nom             = rec.get('acheteur_nom')
                        existing.objet_marche             = rec.get('objet_marche')
                        existing.nature                   = rec.get('nature')
                        existing.type_marche              = rec.get('type_marche')
                        existing.famille_denomination     = rec.get('famille_denomination')
                        existing.descripteur_libelle      = rec.get('descripteur_libelle')
                        existing.code_departement         = rec.get('code_departement')
                        existing.lieu_execution           = rec.get('lieu_execution')
                        existing.dateparution             = parse_date(rec.get('dateparution'))
                        existing.datelimitereponse        = parse_date(rec.get('datelimitereponse'))
                        existing.urlgravure               = rec.get('urlgravure')
                        existing.reference_boamp_initial  = rec.get('reference_boamp')
                        existing.score_pertinence         = score
                        existing.mots_cles_matches        = json.dumps(mots_cles, ensure_ascii=False)
                        existing.has_attribution          = is_attribution
                        if is_attribution:
                            existing.attribution_json     = attribution_json
                        existing.date_derniere_activite   = parse_date(rec.get('dateparution'))
                        existing.fetched_at               = datetime.utcnow()
                        existing.source                   = 'TED'
                        existing.country                  = rec_country
                        updated += 1
                    else:
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
                            fetched_at=datetime.utcnow(),
                            is_new=True,
                            source='TED',
                            country=rec_country,
                        ))
                        created += 1

                db.session.commit()
                logger.info("Refresh TED [%s] terminé : %d créés, %d mis à jour", country, created, updated)

            except Exception as exc:
                db.session.rollback()
                logger.error("Erreur refresh TED [%s] : %s", country, exc, exc_info=True)

        link_boamp_attributions(ctx_app)
        deduplicate_boamp_ted(ctx_app)


# ─── Job : refresh cache PLACE_ES ────────────────────────────────────────────

def refresh_place_es_cache(app=None):
    """Récupère les données PLACE_ES (Espagne) et met à jour le cache SQLite."""
    ctx_app = app or _get_app()
    if not ctx_app:
        return

    with ctx_app.app_context():
        from app.services.place_es_api import (
            fetch_place_es_records, compute_place_es_score, _save_fetch_date,
        )
        from app.models import DossierCache
        from app import db

        logger.info("Début refresh PLACE_ES cache...")
        try:
            records = fetch_place_es_records()
            if not records:
                logger.info("PLACE_ES : aucun avis récupéré.")
                return

            def parse_date(d):
                if not d:
                    return None
                try:
                    return datetime.strptime(str(d)[:10], '%Y-%m-%d').date()
                except ValueError:
                    return None

            updated = created = 0

            # Séparer avis initiaux (PUB) et attributions (ADJ) pour deux passes distinctes.
            # Les PUB sont traités et flushés en base avant les ADJ, de sorte que la
            # recherche par ContractFolderID trouve bien les lignes existantes ou nouvelles.
            pub_records = [r for r in records if not r.get('_is_attribution')]
            adj_records = [r for r in records if r.get('_is_attribution')]

            # ── Passe 1 : avis initiaux (PUB) ────────────────────────────────────
            for rec in pub_records:
                idweb = rec['idweb']
                score, mots_cles = compute_place_es_score(rec)

                if score == 0 and not DossierCache.query.filter_by(idweb=idweb).first():
                    continue

                existing = DossierCache.query.filter_by(idweb=idweb).first()
                if existing:
                    existing.acheteur_nom            = rec.get('acheteur_nom')
                    existing.objet_marche            = rec.get('objet_marche')
                    existing.nature                  = rec.get('nature')
                    existing.type_marche             = rec.get('type_marche')
                    existing.famille_denomination    = rec.get('famille_denomination')
                    existing.descripteur_libelle     = rec.get('descripteur_libelle')
                    existing.lieu_execution          = rec.get('lieu_execution')
                    existing.dateparution            = parse_date(rec.get('dateparution'))
                    existing.datelimitereponse       = parse_date(rec.get('datelimitereponse'))
                    existing.urlgravure              = rec.get('urlgravure')
                    existing.reference_boamp_initial = rec.get('reference_boamp')
                    existing.score_pertinence        = score
                    existing.mots_cles_matches       = json.dumps(mots_cles, ensure_ascii=False)
                    existing.date_derniere_activite  = parse_date(rec.get('dateparution'))
                    existing.fetched_at              = datetime.utcnow()
                    existing.source                  = 'PLACE_ES'
                    existing.country                 = 'ES'
                    updated += 1
                else:
                    db.session.add(DossierCache(
                        idweb=idweb,
                        acheteur_nom=rec.get('acheteur_nom'),
                        objet_marche=rec.get('objet_marche'),
                        nature=rec.get('nature'),
                        type_marche=rec.get('type_marche'),
                        famille_denomination=rec.get('famille_denomination'),
                        descripteur_libelle=rec.get('descripteur_libelle'),
                        lieu_execution=rec.get('lieu_execution'),
                        dateparution=parse_date(rec.get('dateparution')),
                        datelimitereponse=parse_date(rec.get('datelimitereponse')),
                        urlgravure=rec.get('urlgravure'),
                        reference_boamp_initial=rec.get('reference_boamp'),
                        rectificatifs_json='[]',
                        attribution_json=None,
                        score_pertinence=score,
                        mots_cles_matches=json.dumps(mots_cles, ensure_ascii=False),
                        has_rectificatif=False,
                        has_attribution=False,
                        date_derniere_activite=parse_date(rec.get('dateparution')),
                        fetched_at=datetime.utcnow(),
                        is_new=True,
                        source='PLACE_ES',
                        country='ES',
                    ))
                    created += 1

            # Flush les PUB en base (sans commit) pour que les queries ADJ les trouvent
            db.session.flush()

            # ── Passe 2 : attributions (ADJ) ─────────────────────────────────────
            adj_matched = 0
            for rec in adj_records:
                contract_folder_id = rec.get('reference_boamp', '')
                if not contract_folder_id:
                    continue

                attribution_json = json.dumps({
                    'dateparution':    rec.get('dateparution', ''),
                    'urlgravure':      rec.get('urlgravure', ''),
                    'reference_boamp': contract_folder_id,
                    'acheteur_nom':    rec.get('acheteur_nom', ''),
                    'donnees': {
                        'PLACE_ES': {
                            'lots':    rec.get('_attribution_lots', []),
                            'periods': rec.get('_attribution_periods', []),
                        }
                    },
                }, ensure_ascii=False)

                pub_entry = (
                    DossierCache.query
                    .filter_by(source='PLACE_ES')
                    .filter(DossierCache.reference_boamp_initial == contract_folder_id)
                    .first()
                )
                if pub_entry:
                    pub_entry.has_attribution        = True
                    pub_entry.attribution_json       = attribution_json
                    pub_entry.date_derniere_activite = parse_date(rec.get('dateparution'))
                    pub_entry.fetched_at             = datetime.utcnow()
                    adj_matched += 1
                    updated += 1

            logger.info("PLACE_ES attributions : %d ADJ traités, %d reliés à un PUB",
                        len(adj_records), adj_matched)

            db.session.commit()
            _save_fetch_date()
            logger.info("Refresh PLACE_ES terminé : %d créés, %d mis à jour", created, updated)

        except Exception as exc:
            db.session.rollback()
            logger.error("Erreur refresh PLACE_ES : %s", exc, exc_info=True)


# ─── Liaison BOAMP ATTRIBUTION → APPEL_OFFRE ─────────────────────────────────

def link_boamp_attributions(app=None):
    """
    Relie les avis d'attribution BOAMP autonomes à leur appel d'offres initial.

    Un avis BOAMP nature=ATTRIBUTION sans référence externe (reference_boamp_initial
    pointe sur lui-même) est recherché parmi les avis APPEL_OFFRE en base via
    acheteur_nom + objet_marche (exact ou inclusion).

    Action :
      - L'avis APPEL_OFFRE reçoit has_attribution=True et attribution_json
      - L'avis ATTRIBUTION autonome est marqué is_duplicate=True (masqué)
    """
    ctx_app = app or _get_app()
    if not ctx_app:
        return

    with ctx_app.app_context():
        from app.models import DossierCache
        from app import db
        import json

        # Avis d'attribution autonomes (reference pointe sur eux-mêmes)
        attributions = DossierCache.query.filter(
            DossierCache.source == 'BOAMP',
            DossierCache.nature == 'ATTRIBUTION',
            DossierCache.is_duplicate == False,
            DossierCache.reference_boamp_initial == DossierCache.idweb,
        ).all()

        if not attributions:
            return

        # Index des appels d'offres BOAMP par (acheteur_norm, objet_norm)
        appels = DossierCache.query.filter(
            DossierCache.source == 'BOAMP',
            DossierCache.nature == 'APPEL_OFFRE',
            DossierCache.is_duplicate == False,
        ).all()

        appel_index: dict[str, list[tuple[str, DossierCache]]] = {}
        for a in appels:
            acheteur_norm = _normalize(a.acheteur_nom)
            objet_norm = _normalize(a.objet_marche)
            if acheteur_norm and objet_norm:
                appel_index.setdefault(acheteur_norm, []).append((objet_norm, a))

        linked = 0
        for attr in attributions:
            attr_acheteur = _normalize(attr.acheteur_nom)
            attr_objet = _normalize(attr.objet_marche)
            if not attr_acheteur or not attr_objet:
                continue

            candidates = appel_index.get(attr_acheteur, [])
            appel = None
            for appel_objet, a in candidates:
                if (appel_objet == attr_objet
                        or appel_objet in attr_objet
                        or attr_objet in appel_objet):
                    appel = a
                    break

            if not appel:
                continue

            attribution_json = json.dumps({
                'dateparution': attr.dateparution.isoformat() if attr.dateparution else '',
                'urlgravure':   attr.urlgravure or '',
                'reference_boamp': attr.idweb,
                'donnees': json.loads(attr.attribution_json) if attr.attribution_json else None,
            }, ensure_ascii=False)

            appel.has_attribution = True
            appel.attribution_json = attribution_json
            if attr.dateparution and (
                appel.date_derniere_activite is None
                or attr.dateparution > appel.date_derniere_activite
            ):
                appel.date_derniere_activite = attr.dateparution

            attr.is_duplicate = True
            linked += 1

        if linked:
            db.session.commit()
            logger.info("Liaison BOAMP attributions : %d avis reliés à leur appel d'offres.", linked)


# ─── Déduplication BOAMP ↔ TED ───────────────────────────────────────────────

def _normalize(s: str) -> str:
    """Normalise une chaîne pour la comparaison : minuscules + espaces réduits."""
    return ' '.join((s or '').lower().split())


def deduplicate_boamp_ted(app=None):
    """
    Identifie les avis TED qui sont des doublons d'un avis BOAMP existant.

    Critères de doublon (les trois doivent correspondre) :
      - datelimitereponse identique (non nulle)
      - objet_marche normalisé identique
      - acheteur_nom normalisé identique

    Action :
      - Le dossier TED est marqué is_duplicate=True (masqué du dashboard)
      - Le dossier BOAMP reçoit alt_source_url = URL de l'avis TED
    """
    ctx_app = app or _get_app()
    if not ctx_app:
        return

    with ctx_app.app_context():
        from app.models import DossierCache
        from app import db

        def _match_and_mark(ted_list, boamp_index, require_date):
            """Match TED records against BOAMP index and mark duplicates."""
            matched = 0
            for ted in ted_list:
                ted_objet = _normalize(ted.objet_marche)
                ted_acheteur = _normalize(ted.acheteur_nom)
                if not ted_objet or not ted_acheteur:
                    continue

                if require_date:
                    key = (ted.datelimitereponse, ted_acheteur)
                else:
                    key = (None, ted_acheteur)

                candidates = boamp_index.get(key, [])
                boamp = None
                for boamp_objet, b in candidates:
                    if (boamp_objet == ted_objet
                            or boamp_objet in ted_objet
                            or ted_objet in boamp_objet):
                        boamp = b
                        break

                if boamp:
                    if not ted.is_duplicate:
                        ted.is_duplicate = True
                    if ted.urlgravure and boamp.alt_source_url != ted.urlgravure:
                        boamp.alt_source_url = ted.urlgravure
                    matched += 1
            return matched

        # ── Passe 1 : avec date limite ────────────────────────────────────────
        ted_with_date = DossierCache.query.filter(
            DossierCache.source == 'TED',
            DossierCache.datelimitereponse.isnot(None),
        ).all()

        boamp_with_date = DossierCache.query.filter(
            DossierCache.source == 'BOAMP',
            DossierCache.datelimitereponse.isnot(None),
        ).all()

        boamp_index_date: dict[tuple, list[tuple[str, DossierCache]]] = {}
        for b in boamp_with_date:
            key = (b.datelimitereponse, _normalize(b.acheteur_nom))
            objet_norm = _normalize(b.objet_marche)
            if objet_norm and key[1]:
                boamp_index_date.setdefault(key, []).append((objet_norm, b))

        # ── Passe 2 : sans date limite (correspondance exacte objet + acheteur) ─
        ted_no_date = DossierCache.query.filter(
            DossierCache.source == 'TED',
            DossierCache.datelimitereponse.is_(None),
        ).all()

        boamp_no_date = DossierCache.query.filter(
            DossierCache.source == 'BOAMP',
            DossierCache.datelimitereponse.is_(None),
        ).all()

        boamp_index_no_date: dict[tuple, list[tuple[str, DossierCache]]] = {}
        for b in boamp_no_date:
            key = (None, _normalize(b.acheteur_nom))
            objet_norm = _normalize(b.objet_marche)
            if objet_norm and key[1]:
                boamp_index_no_date.setdefault(key, []).append((objet_norm, b))

        marked = 0
        marked += _match_and_mark(ted_with_date, boamp_index_date, require_date=True)
        marked += _match_and_mark(ted_no_date, boamp_index_no_date, require_date=False)

        if marked:
            db.session.commit()
            logger.info("Déduplication BOAMP/TED : %d doublon(s) TED identifié(s).", marked)


# ─── Jobs alertes email ───────────────────────────────────────────────────────

def send_immediate_alerts(app=None):
    ctx_app = app or _get_app()
    if not ctx_app:
        return
    with ctx_app.app_context():
        from app.models import User
        from app.services.mailer import send_alert_digest
        users = User.query.filter_by(is_active=True, alert_enabled=True, alert_frequency='IMMEDIATE').all()
        for user in users:
            send_alert_digest(user, 'IMMEDIATE')


def send_daily_digest(app=None):
    ctx_app = app or _get_app()
    if not ctx_app:
        return
    with ctx_app.app_context():
        from app.models import User
        from app.services.mailer import send_alert_digest
        users = User.query.filter_by(is_active=True, alert_enabled=True, alert_frequency='DAILY').all()
        for user in users:
            send_alert_digest(user, 'DAILY')


def send_weekly_digest(app=None):
    ctx_app = app or _get_app()
    if not ctx_app:
        return
    with ctx_app.app_context():
        from app.models import User
        from app.services.mailer import send_alert_digest
        users = User.query.filter_by(is_active=True, alert_enabled=True, alert_frequency='WEEKLY').all()
        for user in users:
            send_alert_digest(user, 'WEEKLY')


# ─── Initialisation scheduler ─────────────────────────────────────────────────

def _get_app():
    try:
        from flask import current_app
        return current_app._get_current_object()
    except RuntimeError:
        return None


def init_scheduler(app):
    global _scheduler
    if _scheduler and _scheduler.running:
        return _scheduler

    interval_hours = app.config.get('BOAMP_REFRESH_INTERVAL_HOURS', 4)

    _scheduler = BackgroundScheduler(timezone='Europe/Paris')
    _scheduler.add_listener(_on_job_error, EVENT_JOB_ERROR)

    _scheduler.add_job(
        lambda: refresh_boamp_cache(app),
        'interval',
        hours=interval_hours,
        id='boamp_refresh',
        replace_existing=True,
    )
    _scheduler.add_job(
        lambda: refresh_ted_cache(app),
        'interval',
        hours=interval_hours,
        id='ted_refresh',
        replace_existing=True,
    )
    _scheduler.add_job(
        lambda: refresh_place_es_cache(app),
        'interval',
        hours=interval_hours,
        id='place_es_refresh',
        replace_existing=True,
    )
    _scheduler.add_job(
        lambda: send_immediate_alerts(app),
        'interval',
        hours=1,
        id='alerts_immediate',
        replace_existing=True,
    )
    _scheduler.add_job(
        lambda: send_daily_digest(app),
        'cron',
        hour=8,
        minute=0,
        id='alerts_daily',
        replace_existing=True,
    )
    _scheduler.add_job(
        lambda: send_weekly_digest(app),
        'cron',
        day_of_week='mon',
        hour=8,
        id='alerts_weekly',
        replace_existing=True,
    )

    _scheduler.start()
    logger.info("Scheduler APScheduler démarré (refresh toutes les %dh)", interval_hours)
    return _scheduler
