"""
Calcul du score de pertinence Cohesity pour un dossier BOAMP.
Les mots-clés sont lus dynamiquement depuis AppConfig (service keywords).
"""

MAX_SCORE = 100


def calculate_score(
    objet_marche: str = '',
    descripteur_libelle: str = '',
    famille_denomination: str = '',
    country: str | None = None,
) -> tuple[int, list[str]]:
    """
    Calcule le score de pertinence (0-100) et retourne les mots-clés matchés.
    Si country est fourni, les mots-clés spécifiques au pays sont ajoutés aux globaux.
    """
    try:
        from app.services.keywords import get_scoring_keywords
        kws = get_scoring_keywords(country=country)
    except Exception:
        # Fallback inline si le service n'est pas disponible (hors contexte app)
        kws = {
            'haute':    ['sauvegarde', 'backup', 'ransomware', 'PRA', 'PCA'],
            'moyenne':  ['stockage', 'NAS', 'cloud', 'infogérance'],
            'contexte': ['cybersécurité', 'continuité', 'archivage'],
        }

    text = f"{objet_marche} {descripteur_libelle} {famille_denomination}".lower()
    score = 0
    matched: list[str] = []

    weights = {'haute': 20, 'moyenne': 10, 'contexte': 5}
    for category, weight in weights.items():
        for kw in kws.get(category, []):
            if kw.lower() in text:
                score += weight
                matched.append(kw)

    if len(matched) >= 4:
        score += 10  # Bonus diversité

    return min(score, MAX_SCORE), list(set(matched))


def explain_score(
    objet_marche: str = '',
    descripteur_libelle: str = '',
    famille_denomination: str = '',
    country: str | None = None,
) -> list[dict]:
    """
    Retourne la liste détaillée des déclencheurs avec leur champ source et
    un extrait de contexte autour de la correspondance.

    Chaque entrée :
      {
        'keyword'  : str,   # mot-clé trouvé
        'field'    : str,   # label lisible du champ
        'field_key': str,   # clé technique
        'excerpt'  : str,   # texte autour du match (≤ 80 chars)
        'category' : str,   # 'haute' | 'moyenne' | 'contexte'
        'weight'   : int,   # points apportés
      }
    """
    try:
        from app.services.keywords import get_scoring_keywords
        kws = get_scoring_keywords(country=country)
    except Exception:
        kws = {
            'haute':    ['sauvegarde', 'backup', 'ransomware', 'PRA', 'PCA'],
            'moyenne':  ['stockage', 'NAS', 'cloud', 'infogérance'],
            'contexte': ['cybersécurité', 'continuité', 'archivage'],
        }

    fields = [
        ('objet_marche',        'Objet du marché',         objet_marche),
        ('descripteur_libelle', 'Descripteurs CPV',        descripteur_libelle),
        ('famille_denomination','Famille / Dénomination',  famille_denomination),
    ]

    results: list[dict] = []
    seen: set[tuple] = set()

    weights = {'haute': 20, 'moyenne': 10, 'contexte': 5}
    for category, weight in weights.items():
        for kw in kws.get(category, []):
            kw_lower = kw.lower()
            for field_key, field_label, field_value in fields:
                if not field_value:
                    continue
                pos = field_value.lower().find(kw_lower)
                if pos != -1 and (kw_lower, field_key) not in seen:
                    seen.add((kw_lower, field_key))
                    start = max(0, pos - 35)
                    end   = min(len(field_value), pos + len(kw) + 35)
                    excerpt = field_value[start:end]
                    if start > 0:
                        excerpt = '…' + excerpt
                    if end < len(field_value):
                        excerpt += '…'
                    results.append({
                        'keyword'  : kw,
                        'field'    : field_label,
                        'field_key': field_key,
                        'excerpt'  : excerpt,
                        'category' : category,
                        'weight'   : weight,
                    })

    return results


def rescore_all_dossiers() -> tuple[int, int]:
    """
    Recalcule le score et les mots-clés matchés pour tous les dossiers en cache.
    À appeler après une modification des mots-clés de scoring.
    Retourne (nb_updated, nb_total).
    """
    import json
    import logging
    logger = logging.getLogger(__name__)

    from app.models import DossierCache
    from app import db

    dossiers = DossierCache.query.all()
    nb_total = len(dossiers)
    nb_updated = 0

    for dossier in dossiers:
        new_score, new_mots = calculate_score(
            objet_marche=dossier.objet_marche or '',
            descripteur_libelle=dossier.descripteur_libelle or '',
            famille_denomination=dossier.famille_denomination or '',
            country=dossier.country or None,
        )
        new_mots_json = json.dumps(new_mots, ensure_ascii=False)

        if dossier.score_pertinence != new_score or dossier.mots_cles_matches != new_mots_json:
            dossier.score_pertinence = new_score
            dossier.mots_cles_matches = new_mots_json
            nb_updated += 1

    if nb_updated:
        db.session.commit()

    logger.info("Rescore terminé : %d/%d dossiers mis à jour", nb_updated, nb_total)
    return nb_updated, nb_total


def score_stars(score: int) -> int:
    """Convertit un score 0-100 en nombre d'étoiles (0-5)."""
    if score >= 80:
        return 5
    if score >= 60:
        return 4
    if score >= 40:
        return 3
    if score >= 20:
        return 2
    if score > 0:
        return 1
    return 0
