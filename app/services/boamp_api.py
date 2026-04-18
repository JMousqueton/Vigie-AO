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
    """
    Extrait la référence BOAMP (idweb de l'avis initial) depuis le JSON du record.

    Priorités :
      1. gestion.REFERENCE.IDWEB  — présent sur les avis INITIAL
      2. donnees.RECTIF.ANNONCE_ANTERIEUR.REFERENCE.IDWEB  — présent sur les RECTIFICATIF
      3. Recherche récursive d'un IDWEB dans donnees — pour les ATTRIBUTION et MODIFICATION
      4. Fallback : idweb du record lui-même
    """
    own_idweb = record.get('idweb', '')

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
            # Ne garder que si c'est une vraie référence externe (pas une auto-référence)
            if idweb_ref and str(idweb_ref) != own_idweb:
                return str(idweb_ref)

    # Pour les RECTIFICATIF : la référence de l'avis antérieur est dans donnees
    etat = (record.get('etat') or '').upper()
    if etat == ETAT_RECTIFICATIF:
        donnees = record.get('donnees')
        if donnees:
            if isinstance(donnees, str):
                try:
                    donnees = json.loads(donnees)
                except Exception:
                    donnees = {}
            if isinstance(donnees, dict):
                try:
                    parent_idweb = (
                        donnees.get('RECTIF', {})
                               .get('ANNONCE_ANTERIEUR', {})
                               .get('REFERENCE', {})
                               .get('IDWEB', '')
                    )
                    if parent_idweb:
                        return str(parent_idweb)
                except Exception:
                    pass

    # Pour les ATTRIBUTION / MODIFICATION : recherche récursive d'un IDWEB
    # dans le JSON donnees qui diffère de l'idweb propre.
    nature = (record.get('nature') or '').upper()
    if nature == 'ATTRIBUTION' or etat in (ETAT_MODIFICATION, ETAT_ANNULATION):
        donnees = record.get('donnees')
        if donnees:
            if isinstance(donnees, str):
                try:
                    donnees = json.loads(donnees)
                except Exception:
                    donnees = {}
            if isinstance(donnees, dict):
                found = _find_idweb_in_donnees(donnees, own_idweb)
                if found:
                    return found

    return own_idweb


def _find_idweb_in_donnees(obj, own_idweb: str, _depth: int = 0) -> str:
    """
    Parcourt récursivement un dict/liste issu de donnees et retourne le premier
    IDWEB (clé 'IDWEB' ou 'idweb') dont la valeur diffère de own_idweb.
    Limité à 10 niveaux de profondeur pour éviter les boucles infinies.
    """
    if _depth > 10:
        return ''
    if isinstance(obj, dict):
        for key, val in obj.items():
            if key.upper() == 'IDWEB' and val and str(val) != own_idweb:
                return str(val)
        for val in obj.values():
            if isinstance(val, (dict, list)):
                found = _find_idweb_in_donnees(val, own_idweb, _depth + 1)
                if found:
                    return found
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                found = _find_idweb_in_donnees(item, own_idweb, _depth + 1)
                if found:
                    return found
    return ''


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
        # Contact email (extrait du JSON EForms)
        'contact_email':        extract_contact_email(record.get('donnees')),
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
    if not attribution or not isinstance(attribution, dict):
        return []

    try:
        # Le champ `donnees` peut être une str JSON ou déjà un dict
        donnees = attribution.get('donnees')
        if not donnees:
            return []
        if isinstance(donnees, str):
            try:
                donnees = json.loads(donnees)
            except Exception:
                return []

        # ── Format PLACE_ES ──────────────────────────────────────────────────
        if isinstance(donnees, dict) and 'PLACE_ES' in donnees:
            lots = donnees['PLACE_ES'].get('lots', [])
            result = []
            for i, lot in enumerate(lots):
                if lot.get('titulaire') or lot.get('montant') is not None:
                    lot_num = lot.get('lot_num') or str(i + 1)
                    result.append({
                        'lot_num':   lot_num,
                        'lot_id':    lot_num,
                        'titulaire': lot.get('titulaire', ''),
                        'montant':   str(lot['montant']) if lot.get('montant') is not None else '',
                    })
            return result

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
        if not isinstance(notice_result, dict):
            return []

        # ── 1. Index des organisations : ORG-xxx → nom ───────────────────────────
        orgs_container = ext.get('efac:Organizations', {})
        orgs_raw = orgs_container.get('efac:Organization', []) if isinstance(orgs_container, dict) else []
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

    except Exception:
        return []


# ─── Diff rectificatifs ───────────────────────────────────────────────────────

def extract_contract_period(attribution: dict) -> list[dict]:
    """
    Extrait la durée du marché depuis le JSON EForms d'un avis d'attribution.

    Retourne une liste (un élément par lot) de dicts :
      {
        'lot_id'        : 'LOT-0001' | None,
        'start_date'    : 'YYYY-MM-DD' | None,
        'end_date'      : 'YYYY-MM-DD' | None,
        'duration_value': '48' | None,
        'duration_unit' : 'MONTH' | 'YEAR' | 'DAY' | None,
      }

    Retourne [] si non disponible.
    """
    if not attribution:
        return []

    donnees = attribution.get('donnees')
    if not donnees:
        return []
    if isinstance(donnees, str):
        try:
            donnees = json.loads(donnees)
        except Exception:
            return []

    # ── Format PLACE_ES ──────────────────────────────────────────────────────
    if isinstance(donnees, dict) and 'PLACE_ES' in donnees:
        return donnees['PLACE_ES'].get('periods', [])

    try:
        notice = donnees['EFORMS']['ContractAwardNotice']
    except (KeyError, TypeError):
        return []

    lots = notice.get('cac:ProcurementProjectLot', [])
    if isinstance(lots, dict):
        lots = [lots]

    # ── Build lot_id → contract IssueDate fallback from SettledContract ──────
    # When PlannedPeriod has no StartDate we use the contract notification date.
    lot_issue_date: dict[str, str] = {}
    try:
        ext = (notice['ext:UBLExtensions']['ext:UBLExtension']
               ['ext:ExtensionContent']['efext:EformsExtension'])
        nr = ext.get('efac:NoticeResult', {})

        # Index: contract-id → IssueDate
        sc_list = nr.get('efac:SettledContract', [])
        if isinstance(sc_list, dict):
            sc_list = [sc_list]
        contract_issue: dict[str, str] = {}
        for sc in sc_list:
            con_id = _eforms_text(sc.get('cbc:ID', ''))
            raw = sc.get('cbc:IssueDate') or sc.get('cbc:AwardDate') or ''
            issue = _eforms_text(raw)[:10] if _eforms_text(raw) else ''
            if con_id and issue:
                contract_issue[con_id] = issue

        # Map lot_id → contract issue date via LotResult
        lr_list = nr.get('efac:LotResult', [])
        if isinstance(lr_list, dict):
            lr_list = [lr_list]
        for lr in lr_list:
            lot_id_raw = lr.get('efac:TenderLot', {}).get('cbc:ID', '')
            lot_id_val = _eforms_text(lot_id_raw)
            con_id_raw = lr.get('efac:SettledContract', {}).get('cbc:ID', '')
            con_id_val = _eforms_text(con_id_raw)
            if lot_id_val and con_id_val and con_id_val in contract_issue:
                lot_issue_date[lot_id_val] = contract_issue[con_id_val]
    except (KeyError, TypeError, AttributeError):
        pass

    results = []
    seen_periods: set[tuple] = set()

    for lot in lots:
        if not isinstance(lot, dict):
            continue
        lot_id_raw = lot.get('cbc:ID', '')
        lot_id = _eforms_text(lot_id_raw)
        pp = lot.get('cac:ProcurementProject', {}).get('cac:PlannedPeriod', {})
        if not pp:
            continue

        # DurationMeasure
        dm = pp.get('cbc:DurationMeasure', {})
        duration_value = None
        duration_unit = None
        if isinstance(dm, dict):
            duration_value = dm.get('#text') or dm.get('@unitCode')
            duration_unit = dm.get('@unitCode')
            duration_value = dm.get('#text')
        elif isinstance(dm, str) and dm:
            duration_value = dm

        # Dates
        start_raw = _eforms_text(pp.get('cbc:StartDate', ''))
        end_raw   = _eforms_text(pp.get('cbc:EndDate', ''))
        start_date = start_raw[:10] if start_raw else None
        end_date   = end_raw[:10]   if end_raw   else None

        # Fallback: use SettledContract IssueDate as start_date when absent
        if not start_date and lot_id and lot_id in lot_issue_date:
            start_date = lot_issue_date[lot_id]

        # Calculer end_date depuis start_date + duration si end_date absent
        if not end_date and duration_value and duration_unit and start_date:
            try:
                from datetime import date as _date
                import datetime as _dt
                sd = _dt.date.fromisoformat(start_date)
                n  = int(duration_value)
                if duration_unit == 'DAY':
                    computed = sd + _dt.timedelta(days=n)
                elif duration_unit == 'MONTH':
                    # Décalage mensuel robuste
                    month = sd.month - 1 + n
                    year  = sd.year + month // 12
                    month = month % 12 + 1
                    import calendar as _cal
                    day   = min(sd.day, _cal.monthrange(year, month)[1])
                    computed = _date(year, month, day)
                elif duration_unit == 'YEAR':
                    computed = _date(sd.year + n, sd.month, sd.day)
                else:
                    computed = None
                if computed:
                    end_date = computed.isoformat()
            except Exception:
                pass

        key = (duration_value, duration_unit, start_date, end_date)
        if key == (None, None, None, None) or key in seen_periods:
            continue
        seen_periods.add(key)

        results.append({
            'lot_id':         lot_id or None,
            'start_date':     start_date,
            'end_date':       end_date,
            'duration_value': duration_value,
            'duration_unit':  duration_unit,
            'end_date_computed': bool(
                end_date and duration_value and not _eforms_text(pp.get('cbc:EndDate', ''))
            ),
        })

    return results


def extract_contact_email(donnees) -> str:
    """
    Extrait l'email de contact de l'acheteur depuis le JSON EForms BOAMP.

    Essaie dans l'ordre :
      1. Chemins EForms standards (ContractNotice, ContractAwardNotice, etc.)
      2. Fallback regex sur la représentation JSON brute (pour anciens formats).

    Retourne la première adresse email trouvée, ou '' si aucune.
    """
    import re as _re

    if not donnees:
        return ''

    # Désérialiser si c'est une chaîne
    if isinstance(donnees, str):
        try:
            donnees = json.loads(donnees)
        except Exception:
            emails = _re.findall(
                r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', donnees
            )
            return emails[0] if emails else ''

    if not isinstance(donnees, dict):
        return ''

    # Parcourir les types de notice EForms connus
    eforms = donnees.get('EFORMS', {})
    if isinstance(eforms, dict):
        for notice_type in (
            'ContractNotice', 'ContractAwardNotice',
            'PriorInformationNotice', 'Modification', 'ExAnte',
        ):
            notice = eforms.get(notice_type)
            if not isinstance(notice, dict):
                continue

            # cac:ContractingParty peut être un dict ou une liste
            parties = notice.get('cac:ContractingParty', [])
            if isinstance(parties, dict):
                parties = [parties]

            for cp in parties:
                if not isinstance(cp, dict):
                    continue
                party = cp.get('cac:Party', {})
                contact = party.get('cac:Contact', {})
                email_raw = contact.get('cbc:ElectronicMail', '')
                email = _eforms_text(email_raw).strip()
                if email and '@' in email:
                    return email

    # Fallback : regex sur le JSON brut (anciens formats BOAMP non-EForms)
    try:
        donnees_str = json.dumps(donnees, ensure_ascii=False)
        emails = _re.findall(
            r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', donnees_str
        )
        return emails[0] if emails else ''
    except Exception:
        return ''


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
