import os
from dotenv import load_dotenv

load_dotenv()

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    # Flask
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-prod')
    FLASK_ENV = os.environ.get('FLASK_ENV', 'development')

    # SQLite
    SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'instance', 'vigie-ao.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {
        'connect_args': {'check_same_thread': False},
        'pool_pre_ping': True,
    }

    # Session
    SESSION_COOKIE_SECURE = os.environ.get('FLASK_ENV') == 'production'
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    PERMANENT_SESSION_LIFETIME = 30 * 24 * 3600  # 30 jours

    # Flask-Mail
    MAIL_SERVER = os.environ.get('MAIL_SERVER', 'smtp.office365.com')
    MAIL_PORT = int(os.environ.get('MAIL_PORT', 587))
    MAIL_USE_TLS = os.environ.get('MAIL_USE_TLS', 'True').lower() == 'true'
    MAIL_USE_SSL = os.environ.get('MAIL_USE_SSL', 'False').lower() == 'true'
    MAIL_USERNAME = os.environ.get('MAIL_USERNAME')
    MAIL_PASSWORD = os.environ.get('MAIL_PASSWORD')
    MAIL_DEFAULT_SENDER = os.environ.get(
        'MAIL_DEFAULT_SENDER', 'Vigie AO<alerts@example.com>'
    )

    # Application
    # URL publique de l'application — utilisée pour les liens dans les emails
    # envoyés par le scheduler (hors contexte de requête HTTP).
    # Exemple : APP_BASE_URL=https://vigie-ao.example.com
    APP_BASE_URL = os.environ.get('APP_BASE_URL', '')
    ALLOWED_EMAIL_DOMAIN = os.environ.get('ALLOWED_EMAIL_DOMAIN', '')
    AUTO_ACTIVATE = os.environ.get('AUTO_ACTIVATE', 'True').lower() == 'true'
    ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', 'admin@domain.com')

    # BOAMP
    BOAMP_REFRESH_INTERVAL_HOURS = int(os.environ.get('BOAMP_REFRESH_INTERVAL_HOURS', 4))
    BOAMP_CACHE_TTL_HOURS = int(os.environ.get('BOAMP_CACHE_TTL_HOURS', 4))

    # TED (Tenders Electronic Daily — UE)
    TED_API_KEY = os.environ.get('TED_API_KEY', '')
    TED_ENABLED = os.environ.get('TED_ENABLED', 'False').lower() == 'true'

    # PLACE_ES (Plataforma de Contratación del Sector Público — Espagne)
    PLACE_ES_ENABLED = os.environ.get('PLACE_ES_ENABLED', 'False').lower() == 'true'

    # Flask-Limiter
    RATELIMIT_STORAGE_URI = 'memory://'
    RATELIMIT_DEFAULT = '200 per day;50 per hour'

    # Logs
    LOG_DIR = os.path.join(basedir, 'logs')


class DevelopmentConfig(Config):
    DEBUG = True
    SESSION_COOKIE_SECURE = False


class ProductionConfig(Config):
    DEBUG = False
    TESTING = False
    SESSION_COOKIE_SECURE = True


config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig,
}
