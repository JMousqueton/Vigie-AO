"""
Gestion centralisée des mots-clés BOAMP (recherche API + scoring).
Stockés dans AppConfig, chargés avec un cache TTL de 60 s.
Si la base est vide, les valeurs par défaut sont utilisées.
"""
import json
import time
import logging

logger = logging.getLogger(__name__)

# ─── Valeurs par défaut ───────────────────────────────────────────────────────

DEFAULTS: dict[str, list[str]] = {
    'keywords_search': [
        'sauvegarde', 'backup', 'cybersécurité', 'protection des données',
        'stockage', 'NAS', 'ransomware', 'continuité', 'PRA', 'PCA',
        'hyperconverg', 'data management', 'archivage', 'réplication',
        'infogérance', 'Cohesity',
    ],
    'keywords_scoring_haute': [
        'sauvegarde', 'backup', 'ransomware', 'protection des données',
        'PRA', 'PCA', 'cyberattaque', 'cyber-attaque',
    ],
    'keywords_scoring_moyenne': [
        'stockage', 'NAS', 'cloud', 'infogérance', 'hyperconvergé',
        'hyperconvergence', 'data management', 'baie de stockage',
    ],
    'keywords_scoring_contexte': [
        'cybersécurité', 'continuité', 'archivage', 'réplication',
        'snapshot', 'déduplication', 'SIEM', 'SOC', "reprise d'activité",
        'plan de reprise', 'plan de continuité',
    ],
}

_cache: dict = {}
_CACHE_TTL = 60  # secondes


# ─── Lecture ─────────────────────────────────────────────────────────────────

def _load(key: str) -> list[str]:
    """Charge depuis AppConfig avec cache TTL. Fallback sur DEFAULTS."""
    now = time.time()
    entry = _cache.get(key)
    if entry and now - entry['ts'] < _CACHE_TTL:
        return entry['data']

    try:
        from app.models import AppConfig
        row = AppConfig.query.filter_by(key=key).first()
        if row:
            data = json.loads(row.value)
            _cache[key] = {'data': data, 'ts': now}
            return data
    except Exception as exc:
        logger.warning("keywords._load(%s) : %s", key, exc)

    return list(DEFAULTS.get(key, []))


def get_search_keywords() -> list[str]:
    """Mots-clés utilisés dans la requête ODSQL vers l'API BOAMP."""
    return _load('keywords_search')


def get_scoring_keywords() -> dict[str, list[str]]:
    """Mots-clés par catégorie pour le calcul du score de pertinence."""
    return {
        'haute':    _load('keywords_scoring_haute'),
        'moyenne':  _load('keywords_scoring_moyenne'),
        'contexte': _load('keywords_scoring_contexte'),
    }


def get_exclude_keywords() -> list[str]:
    """Mots-clés d'exclusion : tout dossier dont l'objet contient l'un d'eux est masqué."""
    return _load('keywords_exclude')


# ─── Écriture ─────────────────────────────────────────────────────────────────

def save_keywords(
    search: list[str],
    haute: list[str],
    moyenne: list[str],
    contexte: list[str],
    exclude: list[str] | None = None,
    updated_by: int | None = None,
) -> dict[str, list[str]]:
    """
    Persiste les cinq listes en base et vide le cache.
    Les mots-clés de scoring absents de la liste de recherche sont retirés
    automatiquement. Retourne les listes après nettoyage.
    """
    from app.models import AppConfig
    from app import db
    from datetime import datetime

    if exclude is None:
        exclude = []

    # Normaliser en minuscules pour la comparaison
    search_lower = {kw.lower() for kw in search}

    def _filter(lst: list[str]) -> list[str]:
        kept = [kw for kw in lst if kw.lower() in search_lower]
        removed = [kw for kw in lst if kw.lower() not in search_lower]
        if removed:
            logger.info("Mots-clés scoring retirés (absents de la recherche) : %s", removed)
        return kept

    haute    = _filter(haute)
    moyenne  = _filter(moyenne)
    contexte = _filter(contexte)

    for key, value in [
        ('keywords_search',           search),
        ('keywords_scoring_haute',    haute),
        ('keywords_scoring_moyenne',  moyenne),
        ('keywords_scoring_contexte', contexte),
        ('keywords_exclude',          exclude),
    ]:
        row = AppConfig.query.filter_by(key=key).first()
        if row:
            row.value = json.dumps(value, ensure_ascii=False)
            row.updated_at = datetime.utcnow()
            row.updated_by = updated_by
        else:
            db.session.add(AppConfig(
                key=key,
                value=json.dumps(value, ensure_ascii=False),
                updated_by=updated_by,
            ))

    db.session.commit()
    invalidate_cache()
    logger.info("Mots-clés mis à jour par user_id=%s", updated_by)
    return {'search': search, 'haute': haute, 'moyenne': moyenne, 'contexte': contexte, 'exclude': exclude}


def invalidate_cache() -> None:
    _cache.clear()
