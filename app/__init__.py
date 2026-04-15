"""
App factory Flask — BOAMP × Cohesity
"""
import logging
import os
from logging.handlers import RotatingFileHandler

from flask import Flask
from flask_bcrypt import Bcrypt
from flask_login import LoginManager
from flask_mail import Mail
from flask_sqlalchemy import SQLAlchemy
from flask_wtf.csrf import CSRFProtect
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_talisman import Talisman

db = SQLAlchemy()
bcrypt = Bcrypt()
login_manager = LoginManager()
mail = Mail()
csrf = CSRFProtect()
limiter = Limiter(key_func=get_remote_address)
talisman = Talisman()


def create_app(config_name: str | None = None) -> Flask:
    app = Flask(__name__, instance_relative_config=True)

    # Config
    from config import config
    env = config_name or os.environ.get('FLASK_ENV', 'development')
    app.config.from_object(config.get(env, config['default']))

    # Ensure instance folder
    os.makedirs(app.instance_path, exist_ok=True)

    # Extensions
    db.init_app(app)
    bcrypt.init_app(app)
    mail.init_app(app)
    csrf.init_app(app)
    limiter.init_app(app)

    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message = 'Veuillez vous connecter pour accéder à cette page.'
    login_manager.login_message_category = 'warning'

    # Talisman (sécurité HTTP headers)
    is_prod = env == 'production'
    csp = {
        'default-src': ["'self'"],
        'script-src': ["'self'", "'unsafe-inline'", 'cdnjs.cloudflare.com'],
        'style-src': ["'self'", "'unsafe-inline'", 'fonts.googleapis.com', 'cdnjs.cloudflare.com'],
        'font-src': ["'self'", 'fonts.gstatic.com', 'cdnjs.cloudflare.com'],
        'img-src': ["'self'", 'data:'],
        'connect-src': ["'self'"],
    }
    talisman.init_app(
        app,
        force_https=is_prod,
        strict_transport_security=is_prod,
        content_security_policy=csp,
        frame_options='DENY',
    )

    # SQLite WAL mode
    with app.app_context():
        from sqlalchemy import event, text
        from sqlalchemy.engine import Engine
        import sqlite3

        @event.listens_for(Engine, 'connect')
        def set_sqlite_pragma(dbapi_connection, connection_record):
            if isinstance(dbapi_connection, sqlite3.Connection):
                cursor = dbapi_connection.cursor()
                cursor.execute('PRAGMA journal_mode=WAL')
                cursor.execute('PRAGMA foreign_keys=ON')
                cursor.close()

    # Migrations légères : colonnes ajoutées après la création initiale
    with app.app_context():
        _apply_schema_migrations()

    # Blueprints
    from app.routes.auth import auth_bp
    from app.routes.main import main_bp
    from app.routes.watchlist import watchlist_bp
    from app.routes.admin import admin_bp

    app.register_blueprint(auth_bp, url_prefix='/auth')
    app.register_blueprint(main_bp)
    app.register_blueprint(watchlist_bp, url_prefix='/watchlist')
    app.register_blueprint(admin_bp, url_prefix='/admin')

    # User loader
    from app.models import User

    @login_manager.user_loader
    def load_user(user_id):
        return User.query.get(int(user_id))

    # Context processor : thème actif
    @app.context_processor
    def inject_theme():
        from flask import session
        from flask_login import current_user
        # Priorité : préférence User > session > dark par défaut
        theme = 'light'
        try:
            if current_user.is_authenticated and current_user.theme:
                theme = current_user.theme
            else:
                theme = session.get('theme', 'light')
        except Exception:
            theme = session.get('theme', 'light')
        return {'active_theme': theme}

    # Jinja2 helpers
    from app.services.scoring import score_stars
    from app.services.boamp_api import extract_lots_titulaires
    import json

    @app.template_filter('score_stars')
    def score_stars_filter(score):
        return score_stars(score)

    @app.template_filter('extract_lots')
    def extract_lots_filter(attribution):
        """Filtre Jinja2 : extrait les lots+lauréats d'un dict attribution."""
        return extract_lots_titulaires(attribution)

    @app.template_filter('from_json')
    def from_json_filter(s):
        if not s:
            return []
        try:
            return json.loads(s)
        except Exception:
            return []

    @app.template_filter('format_date')
    def format_date_filter(d):
        if not d:
            return '—'
        try:
            if hasattr(d, 'strftime'):
                return d.strftime('%d/%m/%Y')
            from datetime import datetime
            return datetime.strptime(str(d)[:10], '%Y-%m-%d').strftime('%d/%m/%Y')
        except Exception:
            return str(d)

    # Logging
    _setup_logging(app)

    # Scheduler (seulement si pas en test et pas en mode CLI)
    if not app.testing and os.environ.get('WERKZEUG_RUN_MAIN') != 'false':
        try:
            from app.services.scheduler import init_scheduler
            init_scheduler(app)
        except Exception as exc:
            app.logger.warning("Scheduler non démarré : %s", exc)

    return app


def _apply_schema_migrations():
    """
    Applique les migrations de schéma SQLite manquantes (colonnes ajoutées
    après la création initiale de la base).
    Idempotent : sans effet si la colonne existe déjà.
    """
    _migrations = [
        # (table, colonne, définition SQL)
        ('dossier_cache', 'contact_email', 'VARCHAR(255)'),
    ]

    with db.engine.connect() as conn:
        for table, column, col_def in _migrations:
            # Lire les colonnes existantes via PRAGMA
            result = conn.execute(
                db.text(f'PRAGMA table_info("{table}")')
            )
            existing_columns = {row[1] for row in result}
            if column not in existing_columns:
                conn.execute(
                    db.text(f'ALTER TABLE "{table}" ADD COLUMN "{column}" {col_def}')
                )
                conn.commit()
                logging.getLogger(__name__).info(
                    "Migration DB : colonne '%s.%s' ajoutée.", table, column
                )


def _setup_logging(app: Flask):
    log_dir = app.config.get('LOG_DIR', 'logs')
    os.makedirs(log_dir, exist_ok=True)

    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s %(name)s : %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    file_handler = RotatingFileHandler(
        os.path.join(log_dir, 'app.log'),
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding='utf-8',
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    app.logger.addHandler(file_handler)
    app.logger.setLevel(logging.INFO)

    # Propager aux services
    for name in ('app.services.boamp_api', 'app.services.scheduler', 'app.services.mailer'):
        logging.getLogger(name).addHandler(file_handler)
