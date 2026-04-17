"""
Client PLACE_ES — Plataforma de Contratación del Sector Público (Espagne).

Récupère les appels d'offres espagnols pertinents via les flux ATOM officiels.

Documentation  : https://contrataciondelestado.es
Flux ATOM      : https://contrataciondelestado.es/sindicacion/sindicacion_{code}.atom
  Code 1044    → Licitaciones publicadas (appels d'offres)
  Code 1043    → Adjudicaciones provisionales (attributions)
  Code 1042    → Adjudicaciones definitivas
  Code 1048    → Anulaciones

Pagination     : paramètre GET  pagina=N  (1-based)
Auth           : aucune — flux publics
Source         : 'PLACE_ES'
Country        : 'ES'

Format des entrées ATOM :
  <entry>
    <id>         urn:uuid:…  ou URL avec idExpediente
    <title>      titre de l'avis
    <link href>  URL fiche sur PLACE
    <published>  date ISO 8601
    <summary>    HTML résumé (objet, organe, CPV, délai…)
    <content>    XML CODICE (optionnel — pas toujours présent)
  </entry>

Format CODICE (subset utilisé) :
  cac:ContractingParty/cac:Party/cac:PartyName/cbc:Name  → acheteur
  cac:ProcurementProject/cbc:Name                        → objet
  cac:ProcurementProject/cbc:TypeCode                    → type marché
  cac:ProcurementProject/.../cbc:ItemClassificationCode  → CPV
  cac:TenderingProcess/.../cbc:EndDate                   → date limite
  cbc:IssueDate                                          → date parution
  cac:ProcurementProjectLot/.../cac:RealizedLocation/
    cac:Address/cac:Country/cbc:IdentificationCode       → pays
"""

import logging
import os
import re
import time
import warnings
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from urllib.parse import urlparse, parse_qs

import requests
import urllib3

logger = logging.getLogger(__name__)

_SSL_VERIFY = not (os.environ.get('FLASK_DEBUG', '').lower() in ('1', 'true'))
if not _SSL_VERIFY:
    warnings.filterwarnings('ignore', category=urllib3.exceptions.InsecureRequestWarning)

# ─── Constantes ───────────────────────────────────────────────────────────────

BASE_URL   = 'https://contrataciondelestado.es'
SOURCE     = 'PLACE_ES'
COUNTRY    = 'ES'

# Flux ATOM par type d'avis
FEED_LICITACIONES   = f'{BASE_URL}/sindicacion/sindicacion_1044.atom'
FEED_ADJUDICACIONES = f'{BASE_URL}/sindicacion/sindicacion_1043.atom'

# Nombre de pages max par fetch (≈ 100 avis/page)
MAX_PAGES = 10

# Codes CPV Cohesity (identiques à ted_api.py)
CPV_COHESITY: set[str] = {
    '48710000',  # Logiciels de sauvegarde/récupération
    '48820000',  # Serveurs
    '72000000',  # Services informatiques
    '72300000',  # Services de données
    '48800000',  # Systèmes d'information
    '72222300',  # Services TIC
    '30233000',  # Mémoires et supports
    '72253200',  # Services de support système
    '72611000',  # Services de support technique
}

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

# Namespaces XML
NS_ATOM = 'http://www.w3.org/2005/Atom'
NS_CAC  = 'urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2'
NS_CBC  = 'urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2'
NS_MAP  = {'atom': NS_ATOM, 'cac': NS_CAC, 'cbc': NS_CBC}

# Préfixe idweb pour éviter les collisions avec BOAMP / TED
IDWEB_PREFIX = 'PLACE_ES-'

# Regex pour extraire des infos du résumé HTML (fallback)
_RE_CPV      = re.compile(r'\b(\d{8})\b')
_RE_DATE_ES  = re.compile(r'(\d{2})/(\d{2})/(\d{4})')
_RE_DATE_ISO = re.compile(r'(\d{4})-(\d{2})-(\d{2})')


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _tag(ns_uri: str, local: str) -> str:
    return f'{{{ns_uri}}}{local}'


def _find_text(element, path: str, ns: dict = NS_MAP) -> str:
    """Retourne le texte d'un sous-élément ou '' si absent."""
    el = element.find(path, ns)
    return (el.text or '').strip() if el is not None else ''


def _fmt_date(raw: str) -> str:
    """Normalise une date vers YYYY-MM-DD depuis les formats ES et ISO."""
    if not raw:
        return ''
    raw = raw.strip()
    m = _RE_DATE_ISO.search(raw)
    if m:
        return f'{m.group(1)}-{m.group(2)}-{m.group(3)}'
    m = _RE_DATE_ES.search(raw)
    if m:
        return f'{m.group(3)}-{m.group(2)}-{m.group(1)}'
    return raw[:10]


def _extract_id_from_url(url: str) -> str:
    """
    Extrait l'identifiant unique depuis une URL PLACE.
    Essaie idExpediente, idEvl, puis le path final.
    Ex. https://contrataciondelestado.es/wps/poc?uri=deeplink:licitacion&idEvl=ABC123
    → 'ABC123'
    """
    try:
        qs = parse_qs(urlparse(url).query)
        for key in ('idEvl', 'idExpediente', 'idPerfil'):
            if qs.get(key):
                return qs[key][0]
    except Exception:
        pass
    return url.split('/')[-1].split('?')[0]


def _cpv_base(code: str) -> str:
    """Retire le chiffre de contrôle CPV : 48710000-2 → 48710000."""
    return code.split('-')[0].strip()


# ─── Activation / config ──────────────────────────────────────────────────────

def _is_enabled() -> bool:
    try:
        from flask import current_app
        return current_app.config.get('PLACE_ES_ENABLED', False)
    except RuntimeError:
        return os.environ.get('PLACE_ES_ENABLED', 'false').lower() == 'true'


def _get_last_fetch_date() -> date:
    """Date du dernier refresh PLACE_ES réussi (depuis AppConfig). Fallback : -30 j."""
    try:
        from app.models import AppConfig
        row = AppConfig.query.filter_by(key='place_es_last_fetch_date').first()
        if row and row.value:
            return datetime.strptime(row.value, '%Y-%m-%d').date() - timedelta(days=1)
    except Exception:
        pass
    return date.today() - timedelta(days=30)


def _save_fetch_date() -> None:
    try:
        from app.models import AppConfig
        from app import db
        today_str = date.today().isoformat()
        row = AppConfig.query.filter_by(key='place_es_last_fetch_date').first()
        if row:
            row.value = today_str
            row.updated_at = datetime.utcnow()
        else:
            db.session.add(AppConfig(key='place_es_last_fetch_date', value=today_str))
        db.session.commit()
    except Exception as exc:
        logger.warning("Impossible de sauvegarder place_es_last_fetch_date : %s", exc)


# ─── Parsing CODICE XML ───────────────────────────────────────────────────────

def _parse_codice(xml_text: str) -> dict:
    """
    Parse le XML CODICE d'une entrée PLACE et retourne un dict normalisé.
    Retourne {} si le XML est invalide ou vide.
    """
    if not xml_text or not xml_text.strip().startswith('<'):
        return {}
    try:
        # Certains flux ne déclarent pas les namespaces au niveau racine
        # → enrober dans un élément racine neutre si nécessaire
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        try:
            wrapped = f'<root xmlns:cac="{NS_CAC}" xmlns:cbc="{NS_CBC}">{xml_text}</root>'
            root = ET.fromstring(wrapped)
        except ET.ParseError:
            return {}

    def find(path):
        return _find_text(root, path, NS_MAP)

    # Acheteur
    acheteur = (
        find('cac:ContractingParty/cac:Party/cac:PartyName/cbc:Name')
        or find('cac:ContractingPartyType/cac:Party/cac:PartyName/cbc:Name')
        or find('.//cbc:Name')
    )

    # Objet du marché
    objet = (
        find('cac:ProcurementProject/cbc:Name')
        or find('.//cbc:Description')
    )

    # Type de marché (1=Obras, 2=Servicios, 3=Suministros…)
    type_code = find('cac:ProcurementProject/cbc:TypeCode')
    type_labels = {'1': 'Travaux', '2': 'Services', '3': 'Fournitures', '4': 'Concession'}
    type_marche = type_labels.get(type_code, type_code)

    # CPV
    cpv_codes = []
    for el in root.findall('.//cbc:ItemClassificationCode', NS_MAP):
        code = _cpv_base((el.text or '').strip())
        if code and len(code) == 8 and code.isdigit():
            cpv_codes.append(code)
    cpv_str = ', '.join(dict.fromkeys(cpv_codes))

    # Date parution
    dateparution = (
        find('cbc:IssueDate')
        or find('cbc:IssueTime')
    )

    # Date limite réponse
    datelimite = (
        find('cac:TenderingProcess/cac:TenderSubmissionDeadlinePeriod/cbc:EndDate')
        or find('.//cac:TenderSubmissionDeadlinePeriod/cbc:EndDate')
        or find('.//cbc:EndDate')
    )

    # Localisation
    lieu = (
        find('.//cac:RealizedLocation/cac:Address/cbc:CityName')
        or find('.//cac:DeliveryAddress/cbc:CityName')
    )
    region = find('.//cac:RealizedLocation/cac:Address/cac:AddressLine/cbc:Line')

    return {
        'acheteur_nom':        acheteur,
        'objet_marche':        objet,
        'type_marche':         type_marche,
        'descripteur_libelle': cpv_str,
        'famille_denomination': cpv_str,
        'dateparution':        _fmt_date(dateparution),
        'datelimitereponse':   _fmt_date(datelimite),
        'lieu_execution':      lieu or region,
        'cpv_codes':           cpv_codes,
    }


# ─── Parsing résumé HTML (fallback) ──────────────────────────────────────────

def _parse_summary(summary: str) -> dict:
    """
    Extrait les champs clés depuis le résumé HTML d'une entrée ATOM PLACE.
    Utilisé quand le contenu CODICE XML n'est pas disponible.
    """
    if not summary:
        return {}

    # Nettoyer les balises HTML
    clean = re.sub(r'<[^>]+>', ' ', summary)
    clean = re.sub(r'\s+', ' ', clean).strip()

    # Acheteur : "Órgano de contratación: XXX"
    acheteur = ''
    m = re.search(r'[Óo]rgano de contrataci[oó]n[:\s]+([^.;\n<]+)', clean)
    if m:
        acheteur = m.group(1).strip()

    # Objet : "Objeto del contrato: XXX"
    objet = ''
    m = re.search(r'Objeto del contrato[:\s]+([^.;\n<]+)', clean)
    if m:
        objet = m.group(1).strip()

    # Date limite
    datelimite = ''
    m = re.search(
        r'[Ff]echa l[íi]mite[^:]*:\s*(\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2})', clean
    )
    if m:
        datelimite = _fmt_date(m.group(1))

    # CPV
    cpv_codes = list(dict.fromkeys(_RE_CPV.findall(clean)))

    # Type
    type_marche = ''
    m = re.search(r'[Tt]ipo de contrato[:\s]+([^.;\n<]+)', clean)
    if m:
        type_marche = m.group(1).strip()

    return {
        'acheteur_nom':        acheteur,
        'objet_marche':        objet,
        'type_marche':         type_marche,
        'descripteur_libelle': ', '.join(cpv_codes),
        'famille_denomination': ', '.join(cpv_codes),
        'datelimitereponse':   datelimite,
        'lieu_execution':      '',
        'cpv_codes':           cpv_codes,
    }


# ─── Parsing ATOM ─────────────────────────────────────────────────────────────

def _parse_atom_feed(xml_bytes: bytes, is_attribution: bool = False) -> list[dict]:
    """
    Parse un flux ATOM PLACE et retourne une liste de records normalisés.
    Chaque record est au format homogène attendu par le scheduler.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        logger.error("PLACE_ES : erreur parse ATOM XML : %s", exc)
        return []

    records: list[dict] = []
    entries = root.findall(_tag(NS_ATOM, 'entry'))

    for entry in entries:
        # ── Identifiant ──────────────────────────────────────────────────────
        entry_id  = _find_text(entry, 'atom:id', NS_MAP)
        link_el   = entry.find(_tag(NS_ATOM, 'link'))
        link_href = link_el.get('href', '') if link_el is not None else ''

        raw_id = _extract_id_from_url(link_href) if link_href else entry_id
        # Fallback : utiliser les derniers caractères de l'ID ATOM
        if not raw_id or raw_id.startswith('urn:'):
            raw_id = entry_id.split(':')[-1] if ':' in entry_id else entry_id
        idweb = f'{IDWEB_PREFIX}{raw_id}'

        # ── Dates ATOM ───────────────────────────────────────────────────────
        published = _fmt_date(_find_text(entry, 'atom:published', NS_MAP))
        updated   = _fmt_date(_find_text(entry, 'atom:updated', NS_MAP))
        dateparution = published or updated or date.today().isoformat()

        # ── Titre ATOM ───────────────────────────────────────────────────────
        title_raw  = _find_text(entry, 'atom:title', NS_MAP)
        # Supprimer éventuellement le préfixe "Licitación: " ou "Adjudicación: "
        title = re.sub(r'^(Licitaci[oó]n|Adjudicaci[oó]n)[:\s]+', '', title_raw).strip()

        # ── Contenu XML CODICE ────────────────────────────────────────────────
        content_el = entry.find(_tag(NS_ATOM, 'content'))
        content_text = ''
        if content_el is not None:
            # Le contenu peut être du XML embarqué dans CDATA ou directement
            content_text = (content_el.text or '').strip()
            # Si c'est du XML encodé en attribut type="text/xml"
            if not content_text:
                # Essayer de sérialiser les enfants XML
                children = list(content_el)
                if children:
                    content_text = ET.tostring(children[0], encoding='unicode')

        codice = _parse_codice(content_text) if content_text else {}

        # ── Résumé HTML (fallback) ────────────────────────────────────────────
        summary_el = entry.find(_tag(NS_ATOM, 'summary'))
        summary_text = ''
        if summary_el is not None:
            summary_text = summary_el.text or ET.tostring(summary_el, encoding='unicode', method='text')

        fallback = _parse_summary(summary_text) if not codice else {}
        data = codice or fallback

        # ── Assemblage final ──────────────────────────────────────────────────
        objet = data.get('objet_marche') or title
        acheteur = data.get('acheteur_nom', '')
        cpv_codes: list[str] = data.get('cpv_codes', [])
        cpv_str = data.get('descripteur_libelle', '')
        datelimite = data.get('datelimitereponse', '')
        datep = _fmt_date(data.get('dateparution', '')) or dateparution
        lieu = data.get('lieu_execution', '')
        type_marche = data.get('type_marche', '')

        nature = 'ATTRIBUTION' if is_attribution else 'APPEL_OFFRE'

        records.append({
            'idweb':                idweb,
            'country':              COUNTRY,
            'reference_boamp':      raw_id,
            'etat':                 'INITIAL',
            'nature':               nature,
            'type_marche':          type_marche,
            'descripteur_libelle':  cpv_str,
            'famille_denomination': cpv_str,
            'acheteur_nom':         acheteur,
            'acheteur_siret':       '',
            'objet_marche':         objet,
            'code_departement':     '',
            'lieu_execution':       lieu,
            'dateparution':         datep,
            'datelimitereponse':    datelimite,
            'urlgravure':           link_href,
            'donnees':              None,
            'contact_email':        '',
            '_cpv_codes':           cpv_codes,
            '_is_attribution':      is_attribution,
        })

    return records


# ─── Requête HTTP ─────────────────────────────────────────────────────────────

def _fetch_feed_page(url: str, page: int = 1) -> bytes:
    """Télécharge une page d'un flux ATOM PLACE. Retourne les bytes bruts."""
    params = {'pagina': page} if page > 1 else {}
    try:
        resp = requests.get(
            url,
            params=params,
            timeout=30,
            verify=_SSL_VERIFY,
            headers={'Accept': 'application/atom+xml, application/xml, text/xml, */*'},
        )
        resp.raise_for_status()
        return resp.content
    except requests.RequestException as exc:
        logger.error("PLACE_ES : erreur HTTP [%s page=%d] : %s", url, page, exc)
        return b''


# ─── Scoring ──────────────────────────────────────────────────────────────────

def compute_place_es_score(record: dict) -> tuple[int, list[str]]:
    """
    Calcule le score de pertinence Cohesity pour un avis PLACE_ES.
    Même logique que compute_ted_score() dans ted_api.py.
    """
    try:
        from app.services.keywords import get_scoring_keywords, get_search_keywords
        kws = get_scoring_keywords()
        search_kws = get_search_keywords()
    except Exception:
        kws = {
            'haute':    ['sauvegarde', 'backup', 'ransomware'],
            'moyenne':  ['stockage', 'NAS', 'cloud'],
            'contexte': ['cybersécurité', 'continuité', 'archivage'],
        }
        search_kws = ['sauvegarde', 'backup', 'stockage', 'cybersécurité', 'Cohesity']

    title = (record.get('objet_marche') or '').lower()
    cpv_codes: list[str] = record.get('_cpv_codes', [])
    if not cpv_codes:
        # Reconstruire depuis descripteur_libelle
        cpv_raw = record.get('descripteur_libelle') or ''
        cpv_codes = [_cpv_base(c) for c in cpv_raw.split(',') if c.strip()]

    score = 0
    triggers: list[str] = []
    seen: set[str] = set()

    weights = {'haute': 20, 'moyenne': 10, 'contexte': 5}
    for category, weight in weights.items():
        for kw in kws.get(category, []):
            if kw.lower() in title and kw not in seen:
                score += weight
                triggers.append(kw)
                seen.add(kw)

    for kw in search_kws:
        if kw.lower() in title and kw not in seen:
            triggers.append(kw)
            seen.add(kw)

    if len(seen) >= 4:
        score += 10

    for cpv in sorted(set(cpv_codes)):
        if cpv in CPV_COHESITY:
            label = CPV_LABELS.get(cpv, cpv)
            triggers.append(f'CPV {cpv} — {label}')
            score += 10

    return min(score, 100), triggers


def explain_place_es_score(record: dict) -> list[dict]:
    """
    Retourne le détail des déclencheurs (même format que explain_ted_score).
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
    cpv_raw = record.get('descripteur_libelle') or ''
    cpv_codes = [_cpv_base(c) for c in cpv_raw.split(',') if c.strip()]

    results: list[dict] = []
    seen: set[str] = set()

    weights = {'haute': 20, 'moyenne': 10, 'contexte': 5}
    for category, weight in weights.items():
        for kw in kws.get(category, []):
            kw_lower = kw.lower()
            pos = title_lower.find(kw_lower)
            if pos != -1 and kw_lower not in seen:
                seen.add(kw_lower)
                start, end = max(0, pos - 35), min(len(title), pos + len(kw) + 35)
                excerpt = ('…' if start > 0 else '') + title[start:end] + ('…' if end < len(title) else '')
                results.append({'keyword': kw, 'field': 'Titre', 'field_key': 'objet_marche',
                                 'excerpt': excerpt, 'category': category, 'weight': weight})

    for kw in search_kws:
        kw_lower = kw.lower()
        pos = title_lower.find(kw_lower)
        if pos != -1 and kw_lower not in seen:
            seen.add(kw_lower)
            start, end = max(0, pos - 35), min(len(title), pos + len(kw) + 35)
            excerpt = ('…' if start > 0 else '') + title[start:end] + ('…' if end < len(title) else '')
            results.append({'keyword': kw, 'field': 'Titre', 'field_key': 'objet_marche',
                             'excerpt': excerpt, 'category': 'search', 'weight': 0})

    for cpv in cpv_codes:
        if cpv in CPV_COHESITY:
            label = CPV_LABELS.get(cpv, cpv)
            results.append({'keyword': f'CPV {cpv}', 'field': 'Codes CPV', 'field_key': 'cpv',
                             'excerpt': label, 'category': 'cpv', 'weight': 10})

    return results


# ─── Fetch principal ──────────────────────────────────────────────────────────

def fetch_place_es_records() -> list[dict]:
    """
    Récupère tous les avis PLACE_ES pertinents (licitaciones + adjudicaciones).
    Retourne une liste de records normalisés prêts pour le scheduler.
    Filtre par mots-clés et CPV Cohesity pour limiter le bruit.
    """
    if not _is_enabled():
        logger.info("PLACE_ES désactivé (PLACE_ES_ENABLED=False)")
        return []

    since = _get_last_fetch_date()
    logger.info("PLACE_ES : fetch depuis %s", since.isoformat())

    all_records: list[dict] = []

    for feed_url, is_attribution in [
        (FEED_LICITACIONES,   False),
        (FEED_ADJUDICACIONES, True),
    ]:
        label = 'adjudicaciones' if is_attribution else 'licitaciones'
        logger.info("PLACE_ES : récupération %s…", label)

        for page in range(1, MAX_PAGES + 1):
            raw = _fetch_feed_page(feed_url, page)
            if not raw:
                break

            records = _parse_atom_feed(raw, is_attribution=is_attribution)
            if not records:
                logger.info("PLACE_ES [%s] page %d : aucune entrée", label, page)
                break

            # Filtrer par date depuis le dernier fetch
            new_this_page: list[dict] = []
            stop = False
            for rec in records:
                rec_date_str = rec.get('dateparution', '')
                if rec_date_str:
                    try:
                        rec_date = datetime.strptime(rec_date_str[:10], '%Y-%m-%d').date()
                        if rec_date < since:
                            stop = True  # Les flux sont triés par date desc → on peut s'arrêter
                            break
                    except ValueError:
                        pass
                new_this_page.append(rec)

            all_records.extend(new_this_page)
            logger.info("PLACE_ES [%s] page %d : +%d (total %d)",
                        label, page, len(new_this_page), len(all_records))

            if stop or len(records) < 10:
                break

            time.sleep(0.5)  # courtoisie serveur

    # Dédupliquer par idweb
    seen: set[str] = set()
    unique = [r for r in all_records if r['idweb'] not in seen and not seen.add(r['idweb'])]  # type: ignore[func-returns-value]

    # Filtrer : ne garder que les avis avec un score > 0
    try:
        from app.services.keywords import get_search_keywords
        search_kws = [kw.lower() for kw in get_search_keywords()]
    except Exception:
        search_kws = ['sauvegarde', 'backup', 'stockage', 'cybersécurité', 'cohesity',
                      'ransomware', 'nas', 'netbackup', 'veeam', 'commvault']

    def _is_relevant(rec: dict) -> bool:
        title = (rec.get('objet_marche') or '').lower()
        cpv_codes = rec.get('_cpv_codes', [])
        if any(kw in title for kw in search_kws):
            return True
        if any(cpv in CPV_COHESITY for cpv in cpv_codes):
            return True
        return False

    relevant = [r for r in unique if _is_relevant(r)]
    logger.info("PLACE_ES : %d avis uniques pertinents (sur %d récupérés)", len(relevant), len(unique))
    return relevant
