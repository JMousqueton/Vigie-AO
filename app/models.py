from app import db
from flask_login import UserMixin
from datetime import datetime
import json


class User(db.Model, UserMixin):
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    prenom = db.Column(db.String(50), nullable=False)
    nom = db.Column(db.String(50), nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(256), nullable=False)
    role = db.Column(db.String(10), default='USER')  # 'USER' ou 'ADMIN'
    is_active = db.Column(db.Boolean, default=False)
    email_confirmed = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_login = db.Column(db.DateTime)

    # Préférences alertes
    alert_enabled = db.Column(db.Boolean, default=True)
    alert_frequency = db.Column(db.String(10), default='DAILY')  # IMMEDIATE, DAILY, WEEKLY
    alert_last_sent = db.Column(db.DateTime)

    # Préférences interface
    theme = db.Column(db.String(10), default='light')  # 'dark' ou 'light'

    # Relations
    watchlist_items = db.relationship(
        'WatchlistItem', backref='user', lazy=True, cascade='all, delete-orphan'
    )

    def get_id(self):
        return str(self.id)

    @property
    def full_name(self):
        return f"{self.prenom} {self.nom}"

    @property
    def is_admin(self):
        return self.role == 'ADMIN'

    def __repr__(self):
        return f'<User {self.email}>'


class DossierCache(db.Model):
    __tablename__ = 'dossier_cache'

    id = db.Column(db.Integer, primary_key=True)
    idweb = db.Column(db.String(20), unique=True, nullable=False, index=True)

    # Données avis initial
    acheteur_nom = db.Column(db.String(255))
    acheteur_siret = db.Column(db.String(20))
    objet_marche = db.Column(db.Text)
    nature = db.Column(db.String(50))
    type_marche = db.Column(db.String(50))
    famille_denomination = db.Column(db.String(255))
    descripteur_libelle = db.Column(db.Text)
    code_departement = db.Column(db.String(5))
    lieu_execution = db.Column(db.String(255))
    dateparution = db.Column(db.Date, index=True)
    datelimitereponse = db.Column(db.Date, index=True)
    urlgravure = db.Column(db.String(500))
    reference_boamp_initial = db.Column(db.String(30))
    contact_email = db.Column(db.String(255))

    # Données JSON
    rectificatifs_json = db.Column(db.Text, default='[]')
    attribution_json = db.Column(db.Text)

    # Source
    source = db.Column(db.String(10), default='BOAMP', nullable=False, index=True)  # 'BOAMP' | 'TED'

    # Métadonnées scoring
    score_pertinence = db.Column(db.Integer, default=0, index=True)
    mots_cles_matches = db.Column(db.Text, default='[]')
    has_rectificatif = db.Column(db.Boolean, default=False, index=True)
    has_attribution = db.Column(db.Boolean, default=False, index=True)
    date_derniere_activite = db.Column(db.Date, index=True)
    fetched_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_new = db.Column(db.Boolean, default=True)

    @property
    def rectificatifs(self):
        return json.loads(self.rectificatifs_json or '[]')

    @property
    def attribution(self):
        return json.loads(self.attribution_json) if self.attribution_json else None

    @property
    def jours_restants(self):
        if self.datelimitereponse:
            delta = self.datelimitereponse - datetime.utcnow().date()
            return delta.days
        return None

    @property
    def is_urgent(self):
        j = self.jours_restants
        return j is not None and 0 <= j <= 14

    @property
    def is_expired(self):
        j = self.jours_restants
        return j is not None and j < 0

    @property
    def mots_cles(self):
        return json.loads(self.mots_cles_matches or '[]')

    @property
    def nb_rectificatifs(self):
        return len(self.rectificatifs)

    def __repr__(self):
        return f'<DossierCache {self.idweb}>'


class WatchlistItem(db.Model):
    __tablename__ = 'watchlist'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    idweb = db.Column(db.String(20), nullable=False, index=True)
    note = db.Column(db.Text)
    added_at = db.Column(db.DateTime, default=datetime.utcnow)
    notified_at = db.Column(db.DateTime)
    nb_rectifs_at_add = db.Column(db.Integer, default=0)

    __table_args__ = (db.UniqueConstraint('user_id', 'idweb', name='uq_user_idweb'),)

    def __repr__(self):
        return f'<WatchlistItem user={self.user_id} idweb={self.idweb}>'


class AppConfig(db.Model):
    """Paires clé/valeur pour la configuration runtime (mots-clés, etc.)."""
    __tablename__ = 'app_config'

    key = db.Column(db.String(50), primary_key=True)
    value = db.Column(db.Text, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_by = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)

    def __repr__(self):
        return f'<AppConfig {self.key}>'


class SharedLink(db.Model):
    __tablename__ = 'shared_links'

    id = db.Column(db.Integer, primary_key=True)
    token = db.Column(db.String(64), unique=True, nullable=False, index=True)
    idweb = db.Column(db.String(20), nullable=False, index=True)
    created_by = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    expires_at = db.Column(db.DateTime, nullable=True)

    def __repr__(self):
        return f'<SharedLink {self.token[:8]}… → {self.idweb}>'


class UserSeenDossier(db.Model):
    """Trace les dossiers déjà vus par chaque utilisateur (pour le ruban NEW)."""
    __tablename__ = 'user_seen_dossiers'

    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), primary_key=True)
    idweb   = db.Column(db.String(20), primary_key=True)
    seen_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f'<UserSeenDossier user={self.user_id} idweb={self.idweb}>'


class UserHiddenDossier(db.Model):
    """Dossiers masqués par un utilisateur (bouton œil barré sur les cards)."""
    __tablename__ = 'user_hidden_dossiers'

    user_id   = db.Column(db.Integer, db.ForeignKey('users.id'), primary_key=True)
    idweb     = db.Column(db.String(20), primary_key=True)
    hidden_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f'<UserHiddenDossier user={self.user_id} idweb={self.idweb}>'


class AlertLog(db.Model):
    __tablename__ = 'alert_logs'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    sent_at = db.Column(db.DateTime, default=datetime.utcnow)
    nb_dossiers = db.Column(db.Integer, default=0)
    type_alerte = db.Column(db.String(20))  # IMMEDIATE, DAILY, WEEKLY
    success = db.Column(db.Boolean, default=True)
    error_msg = db.Column(db.Text)

    def __repr__(self):
        return f'<AlertLog user={self.user_id} type={self.type_alerte}>'
