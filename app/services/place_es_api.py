"""
Client PLACE_ES — Plataforma de Contratación del Sector Público (Espagne).

Sources de données (sans authentification) :
  1. Flux ATOM temps réel  : https://contrataciondelsectorpublico.gob.es/sindicacion/
                              sindicacion_1044/PlataformasAgregadasSinMenores.atom
     Pagination via <link rel="next"> (timestamp-based, même domaine requis)

  2. Archives ZIP mensuelles (backfill) :
     https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1044/
     PlataformasAgregadasSinMenores_YYYYMM.zip
     → chaque ZIP contient des fichiers .atom au même format

Format des entrées ATOM :
  <entry>
    <id>        URL se terminant par l'identifiant numérique PLACE
    <link href> URL de la fiche sur la plateforme régionale
    <title>     Titre du marché
    <summary>   Résumé texte
    <updated>   Horodatage de dernière modification (ISO 8601)
    <cac-place-ext:ContractFolderStatus>
      <cbc:ContractFolderID>           numéro d'expédient
      <cac-place-ext:LocatedContractingParty/cac:Party/cac:PartyName/cbc:Name>
      <cac:ProcurementProject>
        <cbc:Name>                     objet du marché
        <cbc:TypeCode>                 1=Obras 2=Servicios 3=Suministros
        <cac:RequiredCommodityClassification/cbc:ItemClassificationCode>  CPV
        <cac:RealizedLocation/cbc:CountrySubentityCode>  NUTS
      </cac:ProcurementProject>
      <cac:TenderingProcess/cac:TenderSubmissionDeadlinePeriod/cbc:EndDate>
      <cac-place-ext:ValidNoticeInfo/cac-place-ext:AdditionalPublicationStatus/
        cac-place-ext:AdditionalPublicationDocumentReference/cbc:IssueDate>
    </cac-place-ext:ContractFolderStatus>
  </entry>
"""

import io
import logging
import os
import re
import time
import warnings
import zipfile
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from urllib.parse import urlparse

import requests
import urllib3

logger = logging.getLogger(__name__)

_SSL_VERIFY = not (os.environ.get('FLASK_DEBUG', '').lower() in ('1', 'true'))
if not _SSL_VERIFY:
    warnings.filterwarnings('ignore', category=urllib3.exceptions.InsecureRequestWarning)

# ─── Constantes ───────────────────────────────────────────────────────────────

# Domaine public (sans certificat requis)
BASE_DOMAIN = 'contrataciondelsectorpublico.gob.es'
BASE_URL    = f'https://{BASE_DOMAIN}'

# Domaine alternatif utilisé dans les liens <next> du feed — réécrit à la volée
_LEGACY_DOMAIN = 'contrataciondelestado.es'

FEED_BASE   = f'{BASE_URL}/sindicacion/sindicacion_1044'
FEED_URL    = f'{FEED_BASE}/PlataformasAgregadasSinMenores.atom'

SOURCE  = 'PLACE_ES'
COUNTRY = 'ES'

# Nombre de pages max par fetch (25 avis/page)
MAX_PAGES = 20

# Préfixe idweb pour éviter les collisions avec BOAMP / TED
IDWEB_PREFIX = 'PLACE_ES-'

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

# Namespaces XML du flux PLACE
NS_ATOM    = 'http://www.w3.org/2005/Atom'
NS_CAC     = 'urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2'
NS_CBC     = 'urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2'
NS_CAC_EXT = 'urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2'
NS_CBC_EXT = 'urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2'

# Regex dates
_RE_DATE_ISO = re.compile(r'(\d{4})-(\d{2})-(\d{2})')
_RE_DATE_ES  = re.compile(r'(\d{2})/(\d{2})/(\d{4})')
_RE_CPV      = re.compile(r'\b(\d{8})\b')

# Types de marché
TYPE_LABELS = {'1': 'Travaux', '2': 'Services', '3': 'Fournitures', '4': 'Concession'}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _tag(ns_uri: str, local: str) -> str:
    return f'{{{ns_uri}}}{local}'


def _find_text(el, tag: str) -> str:
    found = el.find(tag)
    return (found.text or '').strip() if found is not None else ''


def _fmt_date(raw: str) -> str:
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


def _cpv_base(code: str) -> str:
    return code.split('-')[0].strip()


def _rewrite_url(url: str) -> str:
    """Réécrit les liens du domaine legacy (cert requis) vers le domaine public."""
    if _LEGACY_DOMAIN in url:
        return url.replace(f'https://{_LEGACY_DOMAIN}', BASE_URL).replace(
            f'http://{_LEGACY_DOMAIN}', BASE_URL
        )
    return url


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
            # Recul d'un jour pour éviter les trous
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


# ─── Parsing d'une entrée ATOM ────────────────────────────────────────────────

def _parse_entry(entry) -> dict | None:
    """
    Parse un élément <entry> du flux PLACE et retourne un record normalisé.
    Retourne None si l'entrée ne peut pas être analysée.
    """
    # ── Identifiant ATOM ─────────────────────────────────────────────────────
    atom_id = _find_text(entry, _tag(NS_ATOM, 'id'))
    # L'ID ressemble à : https://.../sindicacion/PlataformasAgregadasSinMenores/18612570
    raw_id = atom_id.rstrip('/').rsplit('/', 1)[-1] if atom_id else ''
    if not raw_id:
        return None
    idweb = f'{IDWEB_PREFIX}{raw_id}'

    # ── Lien et titre ATOM ───────────────────────────────────────────────────
    link_el = entry.find(_tag(NS_ATOM, 'link'))
    link_href = link_el.get('href', '') if link_el is not None else ''

    title = _find_text(entry, _tag(NS_ATOM, 'title'))
    summary = _find_text(entry, _tag(NS_ATOM, 'summary'))
    updated_raw = _find_text(entry, _tag(NS_ATOM, 'updated'))
    updated_date = _fmt_date(updated_raw)

    # ── ContractFolderStatus (namespace cac-place-ext) ────────────────────────
    cfs = entry.find(_tag(NS_CAC_EXT, 'ContractFolderStatus'))
    if cfs is None:
        # Fallback : résumé texte uniquement
        return _record_from_summary(idweb, raw_id, link_href, title, summary, updated_date)

    # Acheteur
    acheteur = _find_text(cfs,
        f'{_tag(NS_CAC_EXT,"LocatedContractingParty")}'
        f'/{_tag(NS_CAC,"Party")}'
        f'/{_tag(NS_CAC,"PartyName")}'
        f'/{_tag(NS_CBC,"Name")}',
    )

    # Objet — préférer le nom du ProcurementProject au titre ATOM
    pp = cfs.find(_tag(NS_CAC, 'ProcurementProject'))
    objet = ''
    cpv_codes: list[str] = []
    type_marche = ''
    lieu = ''
    datelimite = ''
    dateparution = ''

    if pp is not None:
        objet = _find_text(pp, _tag(NS_CBC, 'Name')) or title
        type_code = _find_text(pp, _tag(NS_CBC, 'TypeCode'))
        type_marche = TYPE_LABELS.get(type_code, type_code)

        # CPV
        for cpv_el in pp.findall(
            f'{_tag(NS_CAC,"RequiredCommodityClassification")}/{_tag(NS_CBC,"ItemClassificationCode")}'
        ):
            code = _cpv_base((cpv_el.text or '').strip())
            if code and len(code) == 8 and code.isdigit():
                cpv_codes.append(code)

        # Localisation (code NUTS → texte)
        nuts_el = pp.find(
            f'{_tag(NS_CAC,"RealizedLocation")}/{_tag(NS_CBC,"CountrySubentityCode")}'
        )
        if nuts_el is not None:
            lieu = (nuts_el.text or '').strip()

    # Date limite (TenderingProcess)
    tp = cfs.find(_tag(NS_CAC, 'TenderingProcess'))
    if tp is not None:
        datelimite = _find_text(tp,
            f'{_tag(NS_CAC,"TenderSubmissionDeadlinePeriod")}/{_tag(NS_CBC,"EndDate")}'
        )

    # Date de parution (ValidNoticeInfo → IssueDate)
    vni = cfs.find(_tag(NS_CAC_EXT, 'ValidNoticeInfo'))
    if vni is not None:
        aps = vni.find(_tag(NS_CAC_EXT, 'AdditionalPublicationStatus'))
        if aps is not None:
            apdr = aps.find(_tag(NS_CAC_EXT, 'AdditionalPublicationDocumentReference'))
            if apdr is not None:
                dateparution = _find_text(apdr, _tag(NS_CBC, 'IssueDate'))

    if not dateparution:
        dateparution = updated_date or date.today().isoformat()

    if not objet:
        objet = title

    cpv_codes = list(dict.fromkeys(cpv_codes))  # dédupliquer en gardant l'ordre
    cpv_str = ', '.join(cpv_codes)

    return {
        'idweb':                idweb,
        'country':              COUNTRY,
        'reference_boamp':      raw_id,
        'etat':                 'INITIAL',
        'nature':               'APPEL_OFFRE',
        'type_marche':          type_marche,
        'descripteur_libelle':  cpv_str,
        'famille_denomination': cpv_str,
        'acheteur_nom':         acheteur,
        'acheteur_siret':       '',
        'objet_marche':         objet,
        'code_departement':     '',
        'lieu_execution':       lieu,
        'dateparution':         _fmt_date(dateparution),
        'datelimitereponse':    _fmt_date(datelimite),
        'urlgravure':           link_href,
        'donnees':              None,
        'contact_email':        '',
        '_cpv_codes':           cpv_codes,
        '_is_attribution':      False,
    }


def _record_from_summary(
    idweb: str, raw_id: str, link_href: str,
    title: str, summary: str, updated_date: str,
) -> dict:
    """Construit un record minimal depuis le résumé texte (fallback sans CODICE)."""
    cpv_codes = list(dict.fromkeys(_RE_CPV.findall(summary or '')))

    # Acheteur depuis "Órgano de contratación: XXX;"
    acheteur = ''
    m = re.search(r'[Óo]rgano de contrataci[oó]n:\s*([^;]+)', summary or '')
    if m:
        acheteur = m.group(1).strip()

    return {
        'idweb':                idweb,
        'country':              COUNTRY,
        'reference_boamp':      raw_id,
        'etat':                 'INITIAL',
        'nature':               'APPEL_OFFRE',
        'type_marche':          '',
        'descripteur_libelle':  ', '.join(cpv_codes),
        'famille_denomination': ', '.join(cpv_codes),
        'acheteur_nom':         acheteur,
        'acheteur_siret':       '',
        'objet_marche':         title,
        'code_departement':     '',
        'lieu_execution':       '',
        'dateparution':         updated_date or date.today().isoformat(),
        'datelimitereponse':    '',
        'urlgravure':           link_href,
        'donnees':              None,
        'contact_email':        '',
        '_cpv_codes':           cpv_codes,
        '_is_attribution':      False,
    }


# ─── Parsing d'un flux ATOM complet ──────────────────────────────────────────

def _parse_atom_feed(xml_bytes: bytes) -> tuple[list[dict], str]:
    """
    Parse un flux ATOM PLACE et retourne (records, next_url).
    next_url est '' s'il n'y a pas de page suivante.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        logger.error("PLACE_ES : erreur parse ATOM XML : %s", exc)
        preview = xml_bytes[:400].decode('utf-8', errors='replace').replace('\n', ' ')
        logger.error("PLACE_ES : début contenu reçu : %s", preview)
        return [], ''

    records: list[dict] = []
    for entry in root.findall(_tag(NS_ATOM, 'entry')):
        rec = _parse_entry(entry)
        if rec:
            records.append(rec)

    # Lien vers la page suivante (réécriture du domaine legacy si nécessaire)
    next_url = ''
    for link in root.findall(_tag(NS_ATOM, 'link')):
        if link.get('rel') == 'next':
            next_url = _rewrite_url(link.get('href', ''))
            break

    return records, next_url


# ─── Requêtes HTTP ────────────────────────────────────────────────────────────

def _fetch_url(url: str) -> bytes:
    """Télécharge une URL. Retourne b'' en cas d'erreur ou de réponse HTML."""
    try:
        resp = requests.get(
            url,
            timeout=30,
            verify=_SSL_VERIFY,
            headers={
                'Accept': 'application/atom+xml, application/xml, text/xml, */*',
                'User-Agent': 'Mozilla/5.0 (compatible; VigieAO/1.0)',
            },
        )
        resp.raise_for_status()
        ct = resp.headers.get('Content-Type', '')
        if 'html' in ct.lower():
            logger.warning(
                "PLACE_ES : réponse HTML reçue (Content-Type: %s) — URL indisponible : %s",
                ct, url,
            )
            return b''
        return resp.content
    except requests.RequestException as exc:
        logger.error("PLACE_ES : erreur HTTP [%s] : %s", url, exc)
        return b''


def _fetch_zip(yyyymm: str) -> list[dict]:
    """
    Télécharge l'archive ZIP mensuelle et retourne tous les records qu'elle contient.
    yyyymm : ex. '202604'
    """
    url = f'{FEED_BASE}/PlataformasAgregadasSinMenores_{yyyymm}.zip'
    logger.info("PLACE_ES : téléchargement archive ZIP %s…", yyyymm)
    raw = _fetch_url(url)
    if not raw:
        return []

    records: list[dict] = []
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            atom_files = [n for n in zf.namelist() if n.endswith('.atom')]
            logger.info("PLACE_ES ZIP %s : %d fichiers .atom", yyyymm, len(atom_files))
            for fname in atom_files:
                data = zf.read(fname)
                page_records, _ = _parse_atom_feed(data)
                records.extend(page_records)
    except zipfile.BadZipFile as exc:
        logger.error("PLACE_ES : ZIP invalide pour %s : %s", yyyymm, exc)

    return records


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
    Récupère les avis PLACE_ES pertinents.

    Stratégie :
      - Si la date du dernier fetch remonte à plus de 2 jours → télécharge les
        archives ZIP mensuelles (backfill).
      - Sinon → utilise le flux ATOM paginé (incrémental).

    Filtre par mots-clés et CPV Cohesity, déduplique par idweb.
    Retourne une liste de records normalisés prêts pour le scheduler.
    """
    if not _is_enabled():
        logger.info("PLACE_ES désactivé (PLACE_ES_ENABLED=False)")
        return []

    since = _get_last_fetch_date()
    days_since = (date.today() - since).days
    logger.info("PLACE_ES : fetch depuis %s (%d jours)", since.isoformat(), days_since)

    all_records: list[dict] = []

    if days_since > 2:
        # ── Backfill via archives ZIP ─────────────────────────────────────────
        logger.info("PLACE_ES : mode backfill — téléchargement des archives ZIP")
        months_needed = min((days_since // 28) + 1, 3)  # max 3 mois
        today = date.today()
        for delta in range(months_needed):
            year = today.year
            month = today.month - delta
            if month <= 0:
                month += 12
                year -= 1
            yyyymm = f'{year}{month:02d}'
            zip_records = _fetch_zip(yyyymm)
            all_records.extend(zip_records)
            if delta < months_needed - 1:
                time.sleep(1)
    else:
        # ── Incrémental via flux ATOM paginé ─────────────────────────────────
        next_url = FEED_URL
        page = 0

        while next_url and page < MAX_PAGES:
            page += 1
            raw = _fetch_url(next_url)
            if not raw:
                break

            page_records, next_url = _parse_atom_feed(raw)
            if not page_records:
                logger.info("PLACE_ES ATOM page %d : aucune entrée", page)
                break

            # Arrêter si on atteint des entrées antérieures à since
            stop = False
            new_this_page: list[dict] = []
            for rec in page_records:
                rec_date_str = rec.get('dateparution', '')
                if rec_date_str:
                    try:
                        rec_date = datetime.strptime(rec_date_str[:10], '%Y-%m-%d').date()
                        if rec_date < since:
                            stop = True
                            break
                    except ValueError:
                        pass
                new_this_page.append(rec)

            all_records.extend(new_this_page)
            logger.info("PLACE_ES ATOM page %d : +%d (total %d)",
                        page, len(new_this_page), len(all_records))

            if stop:
                break
            if next_url:
                time.sleep(0.5)  # courtoisie serveur

    # Dédupliquer par idweb
    seen_ids: set[str] = set()
    unique = [
        r for r in all_records
        if r['idweb'] not in seen_ids and not seen_ids.add(r['idweb'])  # type: ignore[func-returns-value]
    ]

    # Filtrer par pertinence
    try:
        from app.services.keywords import get_search_keywords
        search_kws = [kw.lower() for kw in get_search_keywords()]
    except Exception:
        search_kws = ['sauvegarde', 'backup', 'stockage', 'cybersécurité', 'cohesity',
                      'ransomware', 'nas', 'netbackup', 'veeam', 'commvault']

    def _is_relevant(rec: dict) -> bool:
        title = (rec.get('objet_marche') or '').lower()
        if any(kw in title for kw in search_kws):
            return True
        if any(cpv in CPV_COHESITY for cpv in rec.get('_cpv_codes', [])):
            return True
        return False

    relevant = [r for r in unique if _is_relevant(r)]
    logger.info(
        "PLACE_ES : %d avis pertinents (sur %d uniques / %d bruts)",
        len(relevant), len(unique), len(all_records),
    )
    return relevant
