"""
Client API BOAMP v2.1 (Opendatasoft)
Gestion des requêtes parallèles, agrégation en dossiers, diff rectificatifs.

Champs réels du dataset BOAMP (vérifiés sur l'API) :
  objet            — titre du marché   (≠ objet_marche dans la doc)
  nomacheteur      — acheteur          (≠ acheteur_nom)
  url_avis         — lien BOAMP        (≠ urlgravure)
  etat             — statut            (INITIAL | RECTIFICATIF | MODIFICATION | ANNULATION)
                                        (≠ gestion qui est un objet JSON)
  nature           — APPEL_OFFRE | ATTRIBUTION | MODIFICATION | ...
  code_departement — tableau de str
  type_marche      — tableau de str
  descripteur_libelle — tableau de str
  idweb, dateparution, datelimitereponse, gestion(JSON), donnees(JSON)

Sémantique des états :
  etat=INITIAL  + nature≠ATTRIBUTION  → appel d'offres initial (marché ouvert)
  etat=RECTIFICATIF                   → amendement avant attribution (modif délai, objet…)
  etat=INITIAL  + nature=ATTRIBUTION  → résultat de marché (attribution au prestataire)
  etat=MODIFICATION                   → avenant post-attribution (contrat déjà signé)
  etat=ANNULATION                     → avis d'annulation
"""
import json
import logging
import os
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional

import requests
import urllib3

logger = logging.getLogger(__name__)

# En mode debug (ex: zScaler), désactiver la vérification SSL
_SSL_VERIFY = not (os.environ.get('FLASK_DEBUG', '').lower() in ('1', 'true'))
if not _SSL_VERIFY:
    warnings.filterwarnings('ignore', category=urllib3.exceptions.InsecureRequestWarning)
    logger.warning("FLASK_DEBUG=True — vérification SSL désactivée (proxy zScaler).")

BASE_URL = (
    "https://boamp-datadila.opendatasoft.com/api/explore/v2.1"
    "/catalog/datasets/boamp/records"
)

# Champs réellement disponibles dans le dataset BOAMP
SELECT_FIELDS = (
    "idweb,etat,nature,objet,nomacheteur,"
    "dateparution,datelimitereponse,url_avis,"
    "code_departement,type_marche,descripteur_libelle,"
    "gestion,donnees"
)

# Mots-clés ODSQL — construits dynamiquement depuis la base (AppConfig)
# Fallback statique utilisé si le service keywords n'est pas disponible
_KEYWORDS_ODSQL_FALLBACK = (
    'objet like "%sauvegarde%" OR objet like "%backup%" OR '
    'objet like "%cybersécurité%" OR objet like "%stockage%" OR '
    'objet like "%NAS%" OR objet like "%ransomware%" OR '
    'objet like "%Cohesity%"'
)


def _build_keywords_odsql() -> str:
    """Construit la clause ODSQL depuis les mots-clés en base."""
    try:
        from app.services.keywords import get_search_keywords
        kws = get_search_keywords()
        if kws:
            return ' OR '.join(f'objet like "%{kw}%"' for kw in kws)
    except Exception as exc:
        logger.warning("Impossible de charger les mots-clés : %s", exc)
    return _KEYWORDS_ODSQL_FALLBACK

# Valeurs réelles du champ `etat` dans l'API BOAMP
ETAT_INITIAL       = "INITIAL"
ETAT_RECTIFICATIF  = "RECTIFICATIF"
ETAT_MODIFICATION  = "MODIFICATION"   # avenant post-attribution (contrat signé)
ETAT_ANNULATION    = "ANNULATION"

# Valeurs du champ `nature` pour distinguer les INITIAL
NATURE_ATTRIBUTION = "ATTRIBUTION"    # résultat de marché (prestataire retenu)


# ─── Modèle DossierMarche ────────────────────────────────────────────────────

@dataclass
class DossierMarche:
    idweb: str
    avis_initial: Optional[dict] = None
    rectificatifs: list = field(default_factory=list)
    attribution: Optional[dict] = None  # MODIFICATION ou ANNULATION

    @property
    def date_derniere_activite(self) -> str:
        dates = []
        if self.avis_initial:
            dates.append(self.avis_initial.get('dateparution', '') or '')
        for r in self.rectificatifs:
            dates.append(r.get('dateparution', '') or '')
        if self.attribution:
            dates.append(self.attribution.get('dateparution', '') or '')
        return max((d for d in dates if d), default='')

    @property
    def acheteur_nom(self) -> str:
        src = self.avis_initial or {}
        return src.get('nomacheteur', '')


# ─── Helpers de normalisation ────────────────────────────────────────────────

def _first(value) -> str:
    """Retourne le premier élément si c'est une liste, sinon la valeur elle-même."""
    if isinstance(value, list):
        return value[0] if value else ''
    return value or ''


def _join(value, sep=', ') -> str:
    """Joint un tableau en chaîne, ou retourne la valeur directement."""
    if isinstance(value, list):
        return sep.join(str(v) for v in value if v)
    return value or ''


def _extract_reference(record: dict) -> str:
    """Extrait la référence BOAMP depuis le JSON gestion ou retourne idweb."""
    gestion = record.get('gestion')
    if gestion:
        if isinstance(gestion, str):
            try:
                gestion = json.loads(gestion)
            except Exception:
                pass
        if isinstance(gestion, dict):
            idweb_ref = (
                gestion.get('REFERENCE', {}).get('IDWEB')
                or gestion.get('reference', {}).get('idweb')
            )
            if idweb_ref:
                return str(idweb_ref)
    return record.get('idweb', '')


def normalize_record(record: dict) -> dict:
    """
    Normalise un enregistrement brut de l'API vers un dict homogène
    utilisé partout dans l'application (scheduler, templates…).
    Les noms de clés normalisés correspondent aux colonnes DossierCache.
    """
    return {
        # Identifiants
        'idweb':                record.get('idweb', ''),
        'reference_boamp':      _extract_reference(record),
        # Statut (champ réel : etat)
        'gestion':              record.get('etat', ''),          # on garde la clé 'gestion' en interne
        'etat':                 record.get('etat', ''),
        # Nature / type — APPEL_OFFRE | ATTRIBUTION | MODIFICATION | …
        'nature':               record.get('nature', ''),
        'type_marche':          _first(record.get('type_marche')),
        'descripteur_libelle':  _join(record.get('descripteur_libelle')),
        'famille_denomination': _join(record.get('descripteur_libelle')),  # compat
        # Acheteur
        'acheteur_nom':         record.get('nomacheteur', ''),
        'acheteur_siret':       '',                              # pas exposé dans l'API
        # Objet
        'objet_marche':         record.get('objet', ''),
        # Localisation
        'code_departement':     _first(record.get('code_departement')),
        'lieu_execution':       '',                              # extrait de donnees si besoin
        # Dates
        'dateparution':         (record.get('dateparution') or '')[:10],
        'datelimitereponse':    (record.get('datelimitereponse') or '')[:10],
        # Lien
        'urlgravure':           record.get('url_avis', ''),
        # JSON brut
        'donnees':              record.get('donnees'),
    }


# ─── Appel API ───────────────────────────────────────────────────────────────

def _fetch_records(params: dict, delay: float = 0.0) -> list[dict]:
    """Requête paginée sur l'API BOAMP. Retourne une liste de records normalisés."""
    if delay:
        time.sleep(delay)

    all_records: list[dict] = []
    offset = 0
    limit = params.get('limit', 100)

    while True:
        query_params = {**params, 'offset': offset, 'limit': limit}
        try:
            resp = requests.get(
                BASE_URL, params=query_params,
                timeout=30, verify=_SSL_VERIFY,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            logger.error("Erreur API BOAMP : %s", exc)
            break

        results = data.get('results', [])
        all_records.extend(normalize_record(r) for r in results)

        total = data.get('total_count', 0)
        offset += len(results)
        if offset >= total or not results or offset >= 500:
            break

    return all_records


def fetch_all_records() -> list[dict]:
    """
    4 requêtes parallèles couvrant tout le cycle de vie d'un marché :
      Q1 – Appels d'offres initiaux (etat=INITIAL, nature≠ATTRIBUTION)
      Q2 – Rectificatifs avant attribution (etat=RECTIFICATIF)
      Q3 – Résultats de marché / attributions (etat=INITIAL, nature=ATTRIBUTION)
      Q4 – Avenants post-attribution (etat=MODIFICATION)
    Résultats fusionnés et dédupliqués par (idweb, etat, nature).
    """
    kw_odsql = _build_keywords_odsql()
    queries = [
        {
            # Q1 : appels d'offres ouverts
            "where":    f'({kw_odsql}) AND etat="{ETAT_INITIAL}" AND nature!="{NATURE_ATTRIBUTION}"',
            "order_by": "dateparution DESC",
            "limit":    100,
            "select":   SELECT_FIELDS,
        },
        {
            # Q2 : rectificatifs (amendements avant attribution)
            "where":    f'({kw_odsql}) AND etat="{ETAT_RECTIFICATIF}"',
            "order_by": "dateparution DESC",
            "limit":    100,
            "select":   SELECT_FIELDS,
        },
        {
            # Q3 : résultats de marché (attributions réelles)
            "where":    f'({kw_odsql}) AND etat="{ETAT_INITIAL}" AND nature="{NATURE_ATTRIBUTION}"',
            "order_by": "dateparution DESC",
            "limit":    100,
            "select":   SELECT_FIELDS,
        },
        {
            # Q4 : avenants post-attribution (modifications de contrat)
            "where":    f'({kw_odsql}) AND etat="{ETAT_MODIFICATION}"',
            "order_by": "dateparution DESC",
            "limit":    100,
            "select":   SELECT_FIELDS,
        },
    ]

    all_records: list[dict] = []
    delays = [0.0, 0.2, 0.4, 0.6]

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(_fetch_records, q, delays[i]): i
            for i, q in enumerate(queries)
        }
        for future in as_completed(futures):
            try:
                all_records.extend(future.result())
            except Exception as exc:
                logger.error("Erreur thread BOAMP : %s", exc)

    # Dédupliquer : une clé = (idweb, etat, nature)
    # nature différencie les INITIAL appel-d'offres des INITIAL attribution
    seen: set[tuple] = set()
    unique: list[dict] = []
    for rec in all_records:
        key = (rec.get('idweb', ''), rec.get('etat', ''), rec.get('nature', ''))
        if key not in seen:
            seen.add(key)
            unique.append(rec)

    logger.info("BOAMP : %d enregistrements uniques récupérés", len(unique))
    return unique


# ─── Agrégation en dossiers ───────────────────────────────────────────────────

def aggregate_into_dossiers(all_records: list[dict]) -> list[DossierMarche]:
    dossiers: dict[str, DossierMarche] = {}

    # Trier par date croissante pour traiter les INITIAL avant les RECTIFICATIF
    records_sorted = sorted(all_records, key=lambda r: r.get('dateparution', '') or '')

    for record in records_sorted:
        idweb = (record.get('idweb') or '').strip()
        if not idweb:
            continue
        etat   = (record.get('etat')   or '').upper()
        nature = (record.get('nature') or '').upper()

        if idweb not in dossiers:
            dossiers[idweb] = DossierMarche(idweb=idweb)

        if etat == ETAT_INITIAL and nature == NATURE_ATTRIBUTION.upper():
            # Résultat de marché : prestataire retenu
            dossiers[idweb].attribution = record
        elif etat == ETAT_INITIAL:
            # Appel d'offres initial
            dossiers[idweb].avis_initial = record
        elif etat == ETAT_RECTIFICATIF:
            # Amendement avant attribution (modif délai, objet…)
            dossiers[idweb].rectificatifs.append(record)
        elif etat == ETAT_MODIFICATION:
            # Avenant post-attribution : on écrase uniquement si pas encore d'attribution
            if dossiers[idweb].attribution is None:
                dossiers[idweb].attribution = record
        elif etat == ETAT_ANNULATION:
            dossiers[idweb].attribution = record
        else:
            if dossiers[idweb].avis_initial is None:
                dossiers[idweb].avis_initial = record

    for d in dossiers.values():
        d.rectificatifs.sort(key=lambda r: r.get('dateparution', '') or '')

    return sorted(
        dossiers.values(),
        key=lambda d: d.date_derniere_activite,
        reverse=True,
    )


# ─── Extraction titulaires EForms ────────────────────────────────────────────

def _eforms_text(value) -> str:
    """Extrait la valeur textuelle d'un champ EForms (str ou {'#text': ...})."""
    if isinstance(value, dict):
        return value.get('#text', '')
    return str(value or '')


def extract_lots_titulaires(attribution: dict) -> list[dict]:
    """
    Parse le JSON EForms d'un avis d'attribution et retourne la liste des lots
    avec leur lauréat.

    Retourne une liste de dicts :
      [{'lot_num': '1', 'lot_id': 'LOT-0001', 'titulaire': 'Acme SA', 'montant': '12000'}]

    Fonctionne avec attributions mono-lot et multi-lots.
    Retourne [] si le format n'est pas reconnu.
    """
    if not attribution:
        return []

    # Le champ `donnees` peut être une str JSON ou déjà un dict
    donnees = attribution.get('donnees')
    if not donnees:
        return []
    if isinstance(donnees, str):
        try:
            donnees = json.loads(donnees)
        except Exception:
            return []

    try:
        notice = donnees['EFORMS']['ContractAwardNotice']
    except (KeyError, TypeError):
        return []

    # Naviguer jusqu'aux extensions EForms
    try:
        ext = (notice['ext:UBLExtensions']['ext:UBLExtension']
               ['ext:ExtensionContent']['efext:EformsExtension'])
    except (KeyError, TypeError):
        return []

    notice_result = ext.get('efac:NoticeResult', {})

    # ── 1. Index des organisations : ORG-xxx → nom ───────────────────────────
    orgs_raw = ext.get('efac:Organizations', {}).get('efac:Organization', [])
    if isinstance(orgs_raw, dict):
        orgs_raw = [orgs_raw]
    org_index: dict[str, str] = {}
    for org in orgs_raw:
        company = org.get('efac:Company', {})
        oid = _eforms_text(company.get('cac:PartyIdentification', {}).get('cbc:ID', ''))
        name = _eforms_text(company.get('cac:PartyName', {}).get('cbc:Name', ''))
        if oid:
            org_index[oid] = name

    # ── 2. Index des TenderingParty : TPA-xxx → (nom_court, ORG-xxx) ─────────
    tpa_list = notice_result.get('efac:TenderingParty', [])
    if isinstance(tpa_list, dict):
        tpa_list = [tpa_list]
    tpa_index: dict[str, dict] = {}
    for tpa in tpa_list:
        tpa_id = _eforms_text(tpa.get('cbc:ID', ''))
        tpa_name = _eforms_text(tpa.get('cbc:Name', ''))
        org_id = _eforms_text(tpa.get('efac:Tenderer', {}).get('cbc:ID', ''))
        if tpa_id:
            tpa_index[tpa_id] = {'name': tpa_name, 'org_id': org_id}

    # ── 3. Index des LotTender : TEN-xxx → (LOT-xxx, TPA-xxx, montant) ───────
    tender_list = notice_result.get('efac:LotTender', [])
    if isinstance(tender_list, dict):
        tender_list = [tender_list]
    tender_index: dict[str, dict] = {}
    for lt in tender_list:
        ten_id = _eforms_text(lt.get('cbc:ID', ''))
        lot_id = _eforms_text(lt.get('efac:TenderLot', {}).get('cbc:ID', ''))
        tpa_id = _eforms_text(lt.get('efac:TenderingParty', {}).get('cbc:ID', ''))
        montant = _eforms_text(
            lt.get('cac:LegalMonetaryTotal', {}).get('cbc:PayableAmount', '')
        )
        if ten_id:
            tender_index[ten_id] = {'lot_id': lot_id, 'tpa_id': tpa_id, 'montant': montant}

    # ── 4. Parcourir LotResult pour assembler le résultat ────────────────────
    lot_results = notice_result.get('efac:LotResult', [])
    if isinstance(lot_results, dict):
        lot_results = [lot_results]

    # Filtrer uniquement les lots attribués (selec-w = winner selected)
    result: list[dict] = []
    seen_lots: set[str] = set()

    for lr in lot_results:
        status = _eforms_text(lr.get('cbc:TenderResultCode', ''))
        if status and status != 'selec-w':
            continue  # lot sans attributaire (infructueux, etc.)

        ten_id = _eforms_text(lr.get('efac:LotTender', {}).get('cbc:ID', ''))
        lot_id = _eforms_text(lr.get('efac:TenderLot', {}).get('cbc:ID', ''))

        if not lot_id or lot_id in seen_lots:
            continue
        seen_lots.add(lot_id)

        # Numéro de lot lisible (LOT-0001 → 1)
        lot_num = lot_id.replace('LOT-', '').lstrip('0') or lot_id

        # Retrouver le titulaire via la chaîne TEN → TPA → ORG
        titulaire = ''
        montant = ''
        if ten_id and ten_id in tender_index:
            td = tender_index[ten_id]
            montant = td['montant']
            tpa_id = td['tpa_id']
            if tpa_id and tpa_id in tpa_index:
                tpa = tpa_index[tpa_id]
                org_name = org_index.get(tpa['org_id'], '')
                titulaire = org_name or tpa['name']

        result.append({
            'lot_num':   lot_num,
            'lot_id':    lot_id,
            'titulaire': titulaire,
            'montant':   montant,
        })

    # Trier par numéro de lot
    result.sort(key=lambda x: x['lot_id'])
    return result


# ─── Diff rectificatifs ───────────────────────────────────────────────────────

def diff_rectificatif(avis_precedent: dict, rectificatif: dict) -> dict:
    """Retourne les champs modifiés entre deux avis (clés normalisées)."""
    champs = ['objet_marche', 'datelimitereponse', 'lieu_execution', 'acheteur_nom']
    diff = {}
    for champ in champs:
        ancien = str(avis_precedent.get(champ, '') or '')
        nouveau = str(rectificatif.get(champ, '') or '')
        if ancien != nouveau:
            diff[champ] = {'avant': ancien, 'apres': nouveau}
    return diff


def compute_diffs_for_dossier(dossier: DossierMarche) -> list[dict]:
    result = []
    avis_list = []
    if dossier.avis_initial:
        avis_list.append(dossier.avis_initial)
    avis_list.extend(dossier.rectificatifs)

    for i, rectif in enumerate(dossier.rectificatifs):
        precedent = avis_list[i] if i < len(avis_list) else (dossier.avis_initial or {})
        diff = diff_rectificatif(precedent, rectif)
        result.append({'rectificatif': rectif, 'diff': diff, 'index': i + 1})

    return result
