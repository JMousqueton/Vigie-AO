"""
Gestion centralisée des mots-clés (recherche API + scoring).
Stockés dans AppConfig, chargés avec un cache TTL de 60 s.

Logique globale + par pays :
  - Les mots-clés globaux s'appliquent à toutes les sources et tous les pays.
  - Les mots-clés spécifiques à un pays s'ajoutent (union) aux globaux
    lorsque le dossier provient de ce pays.
  - Clés AppConfig : keywords_search, keywords_search:FR, keywords_search:BE …
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


# ─── Lecture bas niveau ───────────────────────────────────────────────────────

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


def _merge(base: list[str], extra: list[str]) -> list[str]:
    """Fusionne deux listes sans doublons (insensible à la casse), préserve l'ordre."""
    seen = {kw.lower() for kw in base}
    return base + [kw for kw in extra if kw.lower() not in seen]


# ─── API publique ─────────────────────────────────────────────────────────────

def get_search_keywords(country: str | None = None) -> list[str]:
    """Mots-clés utilisés dans la requête vers l'API source (BOAMP, TED…).
    Retourne global ∪ country-specific si country est fourni."""
    base = _load('keywords_search')
    if country:
        extra = _load(f'keywords_search:{country.upper()}')
        return _merge(base, extra)
    return base


def get_scoring_keywords(country: str | None = None) -> dict[str, list[str]]:
    """Mots-clés par catégorie pour le calcul du score de pertinence.
    Retourne global ∪ country-specific pour chaque catégorie."""
    result = {}
    for cat in ('haute', 'moyenne', 'contexte'):
        base = _load(f'keywords_scoring_{cat}')
        if country:
            extra = _load(f'keywords_scoring_{cat}:{country.upper()}')
            result[cat] = _merge(base, extra)
        else:
            result[cat] = base
    return result


def get_exclude_keywords(country: str | None = None) -> list[str]:
    """Mots-clés d'exclusion. Retourne global ∪ country-specific."""
    base = _load('keywords_exclude')
    if country:
        extra = _load(f'keywords_exclude:{country.upper()}')
        return _merge(base, extra)
    return base


# ─── Accès aux mots-clés spécifiques à un pays (pour l'éditeur admin) ────────

def get_country_keywords(country: str) -> dict[str, list[str]]:
    """Retourne UNIQUEMENT les mots-clés spécifiques au pays (sans les globaux)."""
    c = country.upper()
    return {
        'search':   _load(f'keywords_search:{c}'),
        'haute':    _load(f'keywords_scoring_haute:{c}'),
        'moyenne':  _load(f'keywords_scoring_moyenne:{c}'),
        'contexte': _load(f'keywords_scoring_contexte:{c}'),
        'exclude':  _load(f'keywords_exclude:{c}'),
    }


def list_keyword_countries() -> list[str]:
    """Retourne la liste des pays qui ont des mots-clés spécifiques configurés."""
    try:
        from app.models import AppConfig
        rows = AppConfig.query.filter(
            AppConfig.key.like('keywords_%:%')
        ).all()
        countries = set()
        for row in rows:
            parts = row.key.split(':')
            if len(parts) == 2 and parts[1]:
                # Only include if list is non-empty
                try:
                    data = json.loads(row.value)
                    if data:
                        countries.add(parts[1].upper())
                except Exception:
                    pass
        return sorted(countries)
    except Exception:
        return []


# ─── Écriture globale ─────────────────────────────────────────────────────────

def save_keywords(
    search: list[str],
    haute: list[str],
    moyenne: list[str],
    contexte: list[str],
    exclude: list[str] | None = None,
    updated_by: int | None = None,
) -> dict[str, list[str]]:
    """
    Persiste les cinq listes globales en base et vide le cache.
    Les mots-clés de scoring absents de la liste de recherche sont retirés.
    """
    from app.models import AppConfig
    from app import db

    if exclude is None:
        exclude = []

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

    _persist_keys({
        'keywords_search':           search,
        'keywords_scoring_haute':    haute,
        'keywords_scoring_moyenne':  moyenne,
        'keywords_scoring_contexte': contexte,
        'keywords_exclude':          exclude,
    }, updated_by=updated_by)

    invalidate_cache()
    logger.info("Mots-clés globaux mis à jour par user_id=%s", updated_by)
    return {'search': search, 'haute': haute, 'moyenne': moyenne,
            'contexte': contexte, 'exclude': exclude}


# ─── Écriture par pays ────────────────────────────────────────────────────────

def save_country_keywords(
    country: str,
    search: list[str],
    haute: list[str],
    moyenne: list[str],
    contexte: list[str],
    exclude: list[str] | None = None,
    updated_by: int | None = None,
) -> None:
    """Persiste les mots-clés spécifiques à un pays."""
    if exclude is None:
        exclude = []
    c = country.upper()
    _persist_keys({
        f'keywords_search:{c}':           search,
        f'keywords_scoring_haute:{c}':    haute,
        f'keywords_scoring_moyenne:{c}':  moyenne,
        f'keywords_scoring_contexte:{c}': contexte,
        f'keywords_exclude:{c}':          exclude,
    }, updated_by=updated_by)
    invalidate_cache()
    logger.info("Mots-clés pays %s mis à jour par user_id=%s", c, updated_by)


def delete_country_keywords(country: str) -> int:
    """Supprime tous les mots-clés spécifiques à un pays. Retourne le nb de clés supprimées."""
    from app.models import AppConfig
    from app import db
    c = country.upper()
    rows = AppConfig.query.filter(AppConfig.key.like(f'keywords_%:{c}')).all()
    count = len(rows)
    for row in rows:
        db.session.delete(row)
    db.session.commit()
    invalidate_cache()
    logger.info("Mots-clés pays %s supprimés (%d clés) par admin", c, count)
    return count


# ─── Helpers internes ─────────────────────────────────────────────────────────

def _persist_keys(mapping: dict[str, list[str]], updated_by: int | None = None) -> None:
    from app.models import AppConfig
    from app import db
    from app.utils import utc_now

    for key, value in mapping.items():
        row = AppConfig.query.filter_by(key=key).first()
        if row:
            row.value = json.dumps(value, ensure_ascii=False)
            row.updated_at = utc_now()
            row.updated_by = updated_by
        else:
            db.session.add(AppConfig(
                key=key,
                value=json.dumps(value, ensure_ascii=False),
                updated_by=updated_by,
            ))
    db.session.commit()


def invalidate_cache() -> None:
    _cache.clear()
