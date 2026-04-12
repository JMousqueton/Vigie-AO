"""
Client API TED v3 (Tenders Electronic Daily — Journal Officiel de l'UE).
Récupère les appels d'offres et attributions européens pertinents pour Cohesity.

Documentation : https://developer.ted.europa.eu/docs
Endpoint       : POST https://api.ted.europa.eu/v3/notices/search
Auth           : header TED-API-Key (clé gratuite sur developer.ted.europa.eu)

Seuls les avis français (CY = FRA) sont récupérés.
Filtrage par mots-clés dans le titre + codes CPV IT/stockage/cybersécurité.

CPV pertinents :
  48710000 — Logiciels de sauvegarde / récupération
  48820000 — Serveurs
  72000000 — Services informatiques
  72300000 — Services de données
  48800000 — Systèmes d'information et serveurs
  72222300 — Services de technologies de l'information
  30233000 — Mémoires et lecteurs de supports

Format réel des champs TED v3 (vérifié sur l'API) :
  TI  — dict {lang_iso3_lower: str}  ex. {'fra': 'Titre...', 'eng': 'Title...'}
  AU  — dict {lang_iso3_lower: [str]} ex. {'fra': ['Acheteur SA']}
  PD  — str ISO date + tz  ex. '2025-04-14+02:00'  → on prend [:10]
  DT  — str date limite réponse (même format que PD)
  TD  — str type notice : '2'=AO, '3'=attribution, '7'=avis simplifié…
  PC  — list[str]  codes CPV
  ND  — str  identifiant unique (= publication-number)
  CY  — list[str]  ex. ['FRA']
  NUTS— list[str]  ex. ['FR10']
  PN  — str  référence acheteur
  links.html.FRA — URL vers la page française de l'avis
"""
import logging
import os
import time
import warnings
from datetime import date, timedelta

import requests
import urllib3

logger = logging.getLogger(__name__)

_SSL_VERIFY = not (os.environ.get('FLASK_DEBUG', '').lower() in ('1', 'true'))
if not _SSL_VERIFY:
    warnings.filterwarnings('ignore', category=urllib3.exceptions.InsecureRequestWarning)

TED_SEARCH_URL = 'https://api.ted.europa.eu/v3/notices/search'

# Codes CPV IT/stockage/cyber pertinents pour Cohesity
CPV_COHESITY = [
    '48710000',  # Logiciels de sauvegarde/récupération
    '48820000',  # Serveurs
    '72000000',  # Services IT
    '72300000',  # Services de données
    '48800000',  # Systèmes d'information
    '72222300',  # Services TIC
    '30233000',  # Mémoires et supports
    '72253200',  # Services de support système
    '72611000',  # Services de support technique
]

# Labels lisibles pour les codes CPV Cohesity
CPV_LABELS: dict[str, str] = {
    '48710000': 'Logiciels de sauvegarde / récupération',
    '48820000': 'Serveurs',
    '72000000': 'Services informatiques',
    '72300000': 'Services de données',
    '48800000': "Systèmes d'information et serveurs",
    '72222300': "Services de technologies de l'information",
    '30233000': 'Mémoires et lecteurs de supports',
    '72253200': 'Services de support système',
    '72611000': 'Services de support technique',
}

# Champs demandés à l'API TED (validés sur l'API v3)
TED_FIELDS = ['ND', 'TI', 'BT-21-Procedure', 'PD', 'DT', 'DS', 'AU', 'PC', 'NC', 'TD', 'CY', 'RC', 'PR', 'links']

# Types de documents TED → nature
# TD='2' = contract notice (AO), TD='3' = contract award (attribution)
# TD='7' = simplified contract notice, etc.
TD_ATTRIBUTION = {'3', '6', '13', '22', '29', '32'}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _cpv_base(code: str) -> str:
    """Retourne le code CPV sans le chiffre de contrôle (48710000-2 → 48710000)."""
    return code.split('-')[0].strip()


def _get_api_key() -> str:
    try:
        from flask import current_app
        return current_app.config.get('TED_API_KEY', '')
    except RuntimeError:
        return os.environ.get('TED_API_KEY', '')


def _is_enabled() -> bool:
    try:
        from flask import current_app
        return current_app.config.get('TED_ENABLED', False)
    except RuntimeError:
        return os.environ.get('TED_ENABLED', 'false').lower() == 'true'


def _multilang(field, *lang_prefs) -> str:
    """
    Extrait une valeur depuis un champ multilingue TED.
    field = str | list | dict{lang: str|list}
    Préfère les langues dans lang_prefs, puis prend la première disponible.
    """
    if not field:
        return ''
    if isinstance(field, str):
        return field
    if isinstance(field, list):
        return str(field[0]) if field else ''
    if isinstance(field, dict):
        for lang in lang_prefs:
            val = field.get(lang)
            if val:
                return val[0] if isinstance(val, list) else str(val)
        # Fallback sur n'importe quelle langue
        for val in field.values():
            if val:
                return val[0] if isinstance(val, list) else str(val)
    return ''


def _fmt_date(d) -> str:
    """Normalise une date TED vers YYYY-MM-DD. Gère str, list et None."""
    if not d:
        return ''
    if isinstance(d, list):
        d = d[0] if d else ''
    return str(d)[:10]  # '2025-04-14+02:00' → '2025-04-14'


def _ted_url(notice: dict) -> str:
    """Retourne l'URL française de l'avis TED."""
    nd = notice.get('ND', '')
    # Préférer le lien direct html français
    links = notice.get('links') or {}
    fra_html = (links.get('html') or {}).get('FRA', '')
    if fra_html:
        return fra_html
    if nd:
        return f'https://ted.europa.eu/fr/notice/-/detail/{nd}'
    return ''


def _get_last_fetch_date() -> str:
    """
    Retourne la date du dernier refresh TED réussi (depuis AppConfig).
    Fallback : today - 180 jours (premier fetch complet).
    Format retourné : YYYYMMDD pour la requête TED.
    """
    try:
        from app.models import AppConfig
        row = AppConfig.query.filter_by(key='ted_last_fetch_date').first()
        if row and row.value:
            # Recul d'1 jour pour éviter les trous en cas de décalage horaire
            from datetime import datetime as _dt
            last = _dt.strptime(row.value, '%Y-%m-%d').date() - timedelta(days=1)
            return last.strftime('%Y%m%d')
    except Exception:
        pass
    return (date.today() - timedelta(days=180)).strftime('%Y%m%d')


def _save_fetch_date() -> None:
    """Persiste la date du jour comme dernier refresh TED réussi."""
    try:
        from app.models import AppConfig
        from app import db
        from datetime import datetime as _dt
        today_str = date.today().isoformat()
        row = AppConfig.query.filter_by(key='ted_last_fetch_date').first()
        if row:
            row.value = today_str
            row.updated_at = _dt.utcnow()
        else:
            db.session.add(AppConfig(key='ted_last_fetch_date', value=today_str))
        db.session.commit()
    except Exception as exc:
        logger.warning("Impossible de sauvegarder ted_last_fetch_date : %s", exc)


def _build_ted_query() -> str:
    """
    Construit la requête TED combinant :
    - mots-clés dans le titre (depuis AppConfig)
    - OU codes CPV pertinents
    - filtrée sur les avis français des XX derniers jours
    """
    try:
        from app.services.keywords import get_search_keywords
        kws = get_search_keywords()
    except Exception:
        kws = ['sauvegarde', 'backup', 'stockage', 'cybersécurité', 'ransomware', 'Cohesity']

    since = (date.today() - timedelta(days=15)).strftime('%Y%m%d')

    kw_parts = [f'TI ~ "{kw}"' for kw in kws if kw.strip()]
    kw_clause = ' OR '.join(kw_parts) if kw_parts else ''
    cpv_clause = ' OR '.join(f'PC = {cpv}' for cpv in CPV_COHESITY)

    content = f'({kw_clause} OR {cpv_clause})' if kw_clause else f'({cpv_clause})'
    return f'{content} AND CY = FRA AND PD >= {since}'


# ─── Scoring TED ─────────────────────────────────────────────────────────────

def compute_ted_score(record: dict) -> tuple[int, list[str]]:
    """
    Calcule le score de pertinence Cohesity et liste les déclencheurs pour
    un avis TED normalisé.

    Déclencheurs possibles :
      - Mot-clé de scoring trouvé dans le titre   → poids identique à BOAMP
      - Mot-clé de recherche (non scoring) dans le titre → ajout sans points sup.
      - Code CPV Cohesity présent dans l'avis     → +10 pts chacun, préfixé "CPV"

    Le résultat est stocké dans mots_cles_matches exactement comme pour BOAMP,
    avec les entrées CPV préfixées "CPV XXXXXXXX — label" pour que le template
    puisse les distinguer.
    """
    try:
        from app.services.keywords import get_scoring_keywords, get_search_keywords
        kws = get_scoring_keywords()
        search_kws = get_search_keywords()
    except Exception:
        kws = {
            'haute':    ['sauvegarde', 'backup', 'ransomware', 'PRA', 'PCA'],
            'moyenne':  ['stockage', 'NAS', 'cloud', 'infogérance'],
            'contexte': ['cybersécurité', 'continuité', 'archivage'],
        }
        search_kws = ['sauvegarde', 'backup', 'stockage', 'cybersécurité', 'Cohesity']

    title = (record.get('objet_marche') or '').lower()

    # CPV codes présents dans cet avis (descripteur_libelle = "48710000, 48820000")
    cpv_field = record.get('descripteur_libelle') or ''
    notice_cpvs = {_cpv_base(c) for c in cpv_field.split(',') if c.strip()}

    score = 0
    triggers: list[str] = []
    seen_kws: set[str] = set()

    # Mots-clés de scoring (même logique que scoring.py)
    weights = {'haute': 20, 'moyenne': 10, 'contexte': 5}
    for category, weight in weights.items():
        for kw in kws.get(category, []):
            if kw.lower() in title and kw not in seen_kws:
                score += weight
                triggers.append(kw)
                seen_kws.add(kw)

    # Mots-clés de recherche présents dans le titre (pas déjà comptés)
    for kw in search_kws:
        if kw.lower() in title and kw not in seen_kws:
            triggers.append(kw)
            seen_kws.add(kw)

    # Bonus diversité (≥ 4 mots-clés texte)
    if len(seen_kws) >= 4:
        score += 10

    # Codes CPV Cohesity présents dans l'avis
    for cpv in sorted(notice_cpvs):
        if cpv in CPV_COHESITY:
            label = CPV_LABELS.get(cpv, cpv)
            triggers.append(f"CPV {cpv} — {label}")
            score += 10

    return min(score, 100), triggers


def explain_ted_score(record: dict) -> list[dict]:
    """
    Retourne la liste détaillée des déclencheurs pour un avis TED normalisé.

    Même format que explain_score() de scoring.py, avec en plus les entrées CPV :
      {
        'keyword'  : str,           # ex. 'sauvegarde' ou 'CPV 48710000'
        'field'    : str,           # 'Titre' ou 'Codes CPV'
        'field_key': str,           # 'objet_marche' ou 'cpv'
        'excerpt'  : str,           # extrait de texte ou label CPV complet
        'category' : str,           # 'haute'|'moyenne'|'contexte'|'cpv'
        'weight'   : int,
      }
    """
    try:
        from app.services.keywords import get_scoring_keywords, get_search_keywords
        kws = get_scoring_keywords()
        search_kws = get_search_keywords()
    except Exception:
        kws = {'haute': [], 'moyenne': [], 'contexte': []}
        search_kws = []

    title = record.get('objet_marche') or ''
    title_lower = title.lower()

    cpv_field = record.get('descripteur_libelle') or ''
    notice_cpvs = [_cpv_base(c) for c in cpv_field.split(',') if c.strip()]

    results: list[dict] = []
    seen_kws: set[str] = set()

    # Mots-clés de scoring dans le titre
    weights = {'haute': 20, 'moyenne': 10, 'contexte': 5}
    for category, weight in weights.items():
        for kw in kws.get(category, []):
            kw_lower = kw.lower()
            pos = title_lower.find(kw_lower)
            if pos != -1 and kw_lower not in seen_kws:
                seen_kws.add(kw_lower)
                start = max(0, pos - 35)
                end   = min(len(title), pos + len(kw) + 35)
                excerpt = title[start:end]
                if start > 0:
                    excerpt = '…' + excerpt
                if end < len(title):
                    excerpt += '…'
                results.append({
                    'keyword'  : kw,
                    'field'    : 'Titre',
                    'field_key': 'objet_marche',
                    'excerpt'  : excerpt,
                    'category' : category,
                    'weight'   : weight,
                })

    # Mots-clés de recherche présents dans le titre (non scoring)
    for kw in search_kws:
        kw_lower = kw.lower()
        pos = title_lower.find(kw_lower)
        if pos != -1 and kw_lower not in seen_kws:
            seen_kws.add(kw_lower)
            start = max(0, pos - 35)
            end   = min(len(title), pos + len(kw) + 35)
            excerpt = title[start:end]
            if start > 0:
                excerpt = '…' + excerpt
            if end < len(title):
                excerpt += '…'
            results.append({
                'keyword'  : kw,
                'field'    : 'Titre',
                'field_key': 'objet_marche',
                'excerpt'  : excerpt,
                'category' : 'search',
                'weight'   : 0,
            })

    # Codes CPV Cohesity présents dans l'avis
    for cpv in notice_cpvs:
        if cpv in CPV_COHESITY:
            label = CPV_LABELS.get(cpv, cpv)
            results.append({
                'keyword'  : f'CPV {cpv}',
                'field'    : 'Codes CPV',
                'field_key': 'cpv',
                'excerpt'  : label,
                'category' : 'cpv',
                'weight'   : 10,
            })

    return results


# ─── Normalisation ────────────────────────────────────────────────────────────

def _normalize_ted_record(notice: dict) -> dict:
    """
    Normalise un avis TED vers le format homogène utilisé par le scheduler.

    Champs réels TED v3 (vérifiés) :
      BT-21-Procedure — titre officiel de la procédure {fra: str, eng: str, ...}
      TI              — titre avec préfixe pays+CPV (moins précis)
      AU              — acheteur {fra: [str], ...}
      RC              — code région ex. ['FR10', '00']
      DS              — date de soumission / signature (selon type)
      DT              — deadline de réponse
    """
    nd = notice.get('ND', '')

    # Titre : BT-21-Procedure est le titre pur, TI contient le préfixe pays/CPV
    bt21 = notice.get('BT-21-Procedure') or {}
    title = _multilang(bt21, 'fra', 'eng') if bt21 else ''
    if not title:
        # Fallback sur TI en nettoyant le préfixe "France – Catégorie – Titre"
        raw_ti = _multilang(notice.get('TI'), 'fra', 'eng')
        parts = raw_ti.split(' – ')
        title = parts[-1].strip() if len(parts) >= 2 else raw_ti

    # Acheteur
    acheteur = _multilang(notice.get('AU'), 'fra', 'eng')

    # Type de document → nature
    td = str(notice.get('TD') or '')
    is_attribution = td in TD_ATTRIBUTION
    nature = 'ATTRIBUTION' if is_attribution else 'APPEL_OFFRE'

    # CPV
    pc = notice.get('PC', [])
    # Normaliser les codes CPV : retirer le chiffre de contrôle (48710000-2 → 48710000)
    pc_norm = [_cpv_base(c) for c in pc] if isinstance(pc, list) else []
    cpv_str = ', '.join(dict.fromkeys(pc_norm)) if pc_norm else str(pc or '')

    # Région (RC = ex. ['FR10', '00'] — '00' = national)
    rc = notice.get('RC', [])
    rc_str = rc[0] if isinstance(rc, list) and rc else str(rc or '')
    # Extraire un département approx depuis le code région NUTS (FR10→'10', FR211→'21')
    dept = ''
    if rc_str.startswith('FR') and len(rc_str) >= 4:
        dept = rc_str[2:4]

    # Deadline réponse : uniquement DT (DS = date signature, pas pertinent)
    deadline = _fmt_date(notice.get('DT'))

    return {
        'idweb':                f'TED-{nd}',
        'reference_boamp':      nd,
        'etat':                 'INITIAL',
        'nature':               nature,
        'type_marche':          str(notice.get('NC') or ''),
        'descripteur_libelle':  cpv_str,
        'famille_denomination': cpv_str,
        'acheteur_nom':         acheteur,
        'acheteur_siret':       '',
        'objet_marche':         title,
        'code_departement':     dept,
        'lieu_execution':       rc_str,
        'dateparution':         _fmt_date(notice.get('PD')),
        'datelimitereponse':    deadline,
        'urlgravure':           _ted_url(notice),
        'donnees':              None,
        'montant':              '',
        '_ted_nd':              nd,
        '_ted_is_attribution':  is_attribution,
    }


# ─── Requête API ──────────────────────────────────────────────────────────────

def _search_ted(query: str, page: int = 1, limit: int = 100) -> list[dict]:
    """Requête POST vers l'API TED v3. Retourne les résultats normalisés."""
    api_key = _get_api_key()
    headers = {'Content-Type': 'application/json'}
    if api_key:
        headers['TED-API-Key'] = api_key

    payload = {
        'query':  query,
        'fields': TED_FIELDS,
        'page':   page,
        'limit':  limit,
    }

    try:
        resp = requests.post(
            TED_SEARCH_URL,
            json=payload,
            headers=headers,
            timeout=30,
            verify=_SSL_VERIFY,
        )
        resp.raise_for_status()
        notices = resp.json().get('notices', [])
        return [_normalize_ted_record(n) for n in notices]
    except requests.RequestException as exc:
        logger.error("Erreur API TED : %s", exc)
        return []


def fetch_ted_records() -> list[dict]:
    """
    Récupère tous les avis TED pertinents (avec pagination).
    Retourne une liste de records normalisés prêts pour le scheduler.
    """
    if not _is_enabled():
        logger.info("TED désactivé (TED_ENABLED=False)")
        return []

    query = _build_ted_query()
    logger.info("TED query : %s", query[:120])

    all_records: list[dict] = []
    page = 1

    while True:
        records = _search_ted(query, page=page, limit=100)
        if not records:
            break
        all_records.extend(records)
        logger.info("TED page %d : +%d avis (total %d)", page, len(records), len(all_records))

        if len(records) < 100 or len(all_records) >= 2000:
            break
        page += 1
        time.sleep(0.3)

    # Dédupliquer par idweb
    seen: set[str] = set()
    unique = [r for r in all_records if r['idweb'] not in seen and not seen.add(r['idweb'])]  # type: ignore[func-returns-value]

    logger.info("TED : %d avis uniques récupérés", len(unique))
    return unique
