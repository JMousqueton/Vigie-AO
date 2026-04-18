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

# Mapping ISO 3166-1 alpha-3 (TED) → alpha-2 (notre système)
ISO3_TO_ISO2: dict[str, str] = {
    'FRA': 'FR', 'ESP': 'ES', 'DEU': 'DE', 'BEL': 'BE', 'CHE': 'CH',
    'LUX': 'LU', 'NLD': 'NL', 'PRT': 'PT', 'AUT': 'AT', 'POL': 'PL',
    'SWE': 'SE', 'DNK': 'DK', 'FIN': 'FI', 'NOR': 'NO', 'GBR': 'GB',
    'IRL': 'IE', 'ITA': 'IT', 'GRC': 'GR', 'CZE': 'CZ', 'HUN': 'HU',
    'ROU': 'RO', 'SVK': 'SK', 'SVN': 'SI', 'HRV': 'HR', 'BGR': 'BG',
}

# Inverse : ISO2 → ISO3 pour construire les queries TED
ISO2_TO_ISO3: dict[str, str] = {v: k for k, v in ISO3_TO_ISO2.items()}
if not _SSL_VERIFY:
    warnings.filterwarnings('ignore', category=urllib3.exceptions.InsecureRequestWarning)

TED_SEARCH_URL = 'https://api.ted.europa.eu/v3/notices/search'

# Codes CPV IT/stockage/cyber pertinents pour Cohesity avec poids par niveau
# Format: code → (label, catégorie, poids)
# Catégories: 'haute' (+20), 'moyenne' (+15), 'contexte' (+10)
CPV_COHESITY_WEIGHTED: dict[str, tuple[str, str, int]] = {
    # ── Haute (cœur de métier Cohesity) ─────────────────────────────────────
    '48710000': ('Logiciels de sauvegarde / récupération',   'haute',    20),
    '48711000': ('Logiciels de sauvegarde de fichiers',      'haute',    20),
    '48732000': ('Logiciels de chiffrement / sécurité data', 'haute',    20),
    # ── Moyenne (infrastructure proche) ─────────────────────────────────────
    '48820000': ('Serveurs',                                 'moyenne',  15),
    '30233000': ('Mémoires et lecteurs de supports',         'moyenne',  15),
    '30233100': ('Unités de disques durs',                   'moyenne',  15),
    '30233141': ('Baies de stockage redondantes (RAID)',      'moyenne',  15),
    '48800000': ("Systèmes d'information et serveurs",       'moyenne',  15),
    '72212710': ('Services de développement SW sauvegarde',  'moyenne',  15),
    # ── Contexte (IT général / cyber) ────────────────────────────────────────
    '72000000': ('Services informatiques',                   'contexte', 10),
    '72300000': ('Services de données',                      'contexte', 10),
    '72222300': ("Services de technologies de l'information",'contexte', 10),
    '72253200': ('Services de support système',              'contexte', 10),
    '72611000': ('Services de support technique',            'contexte', 10),
    '48900000': ('Logiciels divers et systèmes informatiques','contexte', 10),
    '72310000': ('Services de traitement de données',        'contexte', 10),
}

# Liste complète pour le scoring (toutes catégories)
CPV_COHESITY: list[str] = list(CPV_COHESITY_WEIGHTED.keys())
CPV_LABELS: dict[str, str] = {k: v[0] for k, v in CPV_COHESITY_WEIGHTED.items()}

# Sous-ensemble utilisé dans la requête TED (max ~8 codes pour rester sous la limite API)
# On garde les codes haute + les plus discriminants de moyenne uniquement
CPV_SEARCH: list[str] = [
    '48710000',  # Logiciels de sauvegarde / récupération
    '48711000',  # Logiciels de sauvegarde de fichiers
    '48820000',  # Serveurs
    '30233000',  # Mémoires et supports de stockage
    '48800000',  # Systèmes d'information et serveurs
    '72000000',  # Services informatiques
    '72300000',  # Services de données
    '72253200',  # Services de support système
]

# Préfixes CPV (4 premiers chiffres) → poids pour les codes de famille non listés
# ex. 4871xxxx = famille sauvegarde logicielle
CPV_PREFIX_WEIGHTS: dict[str, tuple[str, int]] = {
    '4871': ('haute',    20),   # sauvegarde/récupération SW
    '4872': ('haute',    20),   # sécurité logicielle
    '3023': ('moyenne',  15),   # supports de stockage
    '4882': ('moyenne',  15),   # serveurs et matériel proche
    '7221': ('contexte', 10),   # services SW
    '7225': ('contexte', 10),   # services support
    '7226': ('contexte', 10),   # services réseau / infra
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


def _sanitize_kw(kw: str) -> str:
    """Nettoie un mot-clé pour l'API TED : retire les guillemets, limite à 40 chars."""
    return kw.replace('"', '').replace("'", '').strip()[:40]


def _build_ted_query(country_iso2: str = 'FR') -> str:
    """
    Construit la requête TED combinant :
    - mots-clés de SCORING dans le titre (haute + moyenne uniquement, max 10)
    - OU codes CPV pertinents (CPV_SEARCH, max 8)
    - filtrée sur le pays demandé et les XX derniers jours

    On utilise les scoring keywords (pas tous les search keywords) car la liste
    de recherche peut être très longue et dépasser les limites de l'API TED.
    country_iso2 : code ISO2 ('FR', 'ES', 'DE'…) ou 'EU' pour tous les pays UE.
    """
    try:
        from app.services.keywords import get_scoring_keywords
        scoring = get_scoring_keywords()
        # Haute + moyenne uniquement (termes les plus discriminants), max 10 au total
        kws_raw = scoring.get('haute', []) + scoring.get('moyenne', [])
    except Exception:
        kws_raw = ['sauvegarde', 'backup', 'ransomware', 'stockage', 'NAS', 'Cohesity']

    since = (date.today() - timedelta(days=15)).strftime('%Y%m%d')

    # Sanitize + déduplique + limite à 10 mots-clés
    seen: set[str] = set()
    kws_clean: list[str] = []
    for kw in kws_raw:
        s = _sanitize_kw(kw)
        if s and s.lower() not in seen:
            seen.add(s.lower())
            kws_clean.append(s)
        if len(kws_clean) >= 10:
            break

    kw_parts = [f'TI ~ "{kw}"' for kw in kws_clean]
    kw_clause = ' OR '.join(kw_parts) if kw_parts else ''
    cpv_clause = ' OR '.join(f'PC = {cpv}' for cpv in CPV_SEARCH)

    content = f'({kw_clause} OR {cpv_clause})' if kw_clause else f'({cpv_clause})'

    if country_iso2 == 'EU':
        return f'{content} AND PD >= {since}'

    iso3 = ISO2_TO_ISO3.get(country_iso2.upper(), 'FRA')
    return f'{content} AND CY = {iso3} AND PD >= {since}'


# ─── Scoring TED ─────────────────────────────────────────────────────────────

def compute_ted_score(record: dict) -> tuple[int, list[str]]:
    """
    Calcule le score de pertinence Cohesity et liste les déclencheurs pour
    un avis TED normalisé.

    Déclencheurs possibles :
      - Mot-clé de scoring trouvé dans le titre   → poids identique à BOAMP
      - Mot-clé de recherche (non scoring) dans le titre → ajout sans points sup.
      - Code CPV Cohesity présent dans l'avis     → pondération hiérarchisée

    Le résultat est stocké dans mots_cles_matches exactement comme pour BOAMP,
    avec les entrées CPV préfixées "CPV XXXXXXXX — label" pour que le template
    puisse les distinguer.

    Les mots-clés globaux + spécifiques au pays du dossier sont combinés.
    """
    country = (record.get('country') or '').upper() or None
    try:
        from app.services.keywords import get_scoring_keywords, get_search_keywords
        kws = get_scoring_keywords(country=country)
        search_kws = get_search_keywords(country=country)
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

    # Codes CPV Cohesity présents dans l'avis — poids hiérarchisés
    seen_cpv_prefixes: set[str] = set()
    for cpv in sorted(notice_cpvs):
        cpv_pts = 0
        cpv_label = cpv

        if cpv in CPV_COHESITY_WEIGHTED:
            label, _cat, pts = CPV_COHESITY_WEIGHTED[cpv]
            cpv_pts, cpv_label = pts, label
        else:
            # Tentative de correspondance par préfixe (4 premiers chiffres)
            prefix = cpv[:4]
            if prefix not in seen_cpv_prefixes and prefix in CPV_PREFIX_WEIGHTS:
                _cat, pts = CPV_PREFIX_WEIGHTS[prefix]
                cpv_pts = pts
                seen_cpv_prefixes.add(prefix)

        if cpv_pts:
            triggers.append(f"CPV {cpv} — {cpv_label}")
            score += cpv_pts

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
    country = (record.get('country') or '').upper() or None
    try:
        from app.services.keywords import get_scoring_keywords, get_search_keywords
        kws = get_scoring_keywords(country=country)
        search_kws = get_search_keywords(country=country)
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

    # Codes CPV Cohesity présents dans l'avis — poids hiérarchisés
    seen_cpv_prefixes_exp: set[str] = set()
    for cpv in notice_cpvs:
        cpv_pts = 0
        cpv_label = cpv
        cpv_cat = 'cpv'

        if cpv in CPV_COHESITY_WEIGHTED:
            label, cat, pts = CPV_COHESITY_WEIGHTED[cpv]
            cpv_pts, cpv_label, cpv_cat = pts, label, cat
        else:
            prefix = cpv[:4]
            if prefix not in seen_cpv_prefixes_exp and prefix in CPV_PREFIX_WEIGHTS:
                cat, pts = CPV_PREFIX_WEIGHTS[prefix]
                cpv_pts, cpv_cat = pts, cat
                seen_cpv_prefixes_exp.add(prefix)

        if cpv_pts:
            results.append({
                'keyword'  : f'CPV {cpv}',
                'field'    : 'Codes CPV',
                'field_key': 'cpv',
                'excerpt'  : cpv_label,
                'category' : cpv_cat,
                'weight'   : cpv_pts,
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

    # Pays : CY = ['FRA'] ou ['ESP'] etc. → ISO2
    cy = notice.get('CY', [])
    iso3 = cy[0] if isinstance(cy, list) and cy else (cy if isinstance(cy, str) else 'FRA')
    country_iso2 = ISO3_TO_ISO2.get(str(iso3).upper(), 'FR')

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
        'country':              country_iso2,
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
        if not resp.ok:
            try:
                body = resp.json()
            except Exception:
                body = resp.text[:500]
            logger.error("Erreur API TED : %s %s — réponse : %s", resp.status_code, resp.reason, body)
            return []
        notices = resp.json().get('notices', [])
        return [_normalize_ted_record(n) for n in notices]
    except requests.RequestException as exc:
        logger.error("Erreur API TED : %s", exc)
        return []


def fetch_ted_records(country_iso2: str = 'FR') -> list[dict]:
    """
    Récupère tous les avis TED pertinents pour un pays (avec pagination).
    country_iso2 : 'FR', 'ES', 'DE'… ou 'EU' pour tous les pays.
    Retourne une liste de records normalisés prêts pour le scheduler.
    """
    if not _is_enabled():
        logger.info("TED désactivé (TED_ENABLED=False)")
        return []

    query = _build_ted_query(country_iso2)
    logger.info("TED [%s] query (%d chars) : %s", country_iso2, len(query), query)

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
