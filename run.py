"""
Point d'entrée principal — BOAMP × Cohesity
Lance l'application, initialise la base SQLite et crée l'admin par défaut.

Premier démarrage :
  1. Copiez .env.example en .env et ajustez les variables
  2. python run.py
  3. Connectez-vous avec ADMIN_EMAIL / ADMIN_DEFAULT_PASSWORD
  4. Changez immédiatement le mot de passe depuis Mon profil

Port par défaut : 5001 (5000 est souvent occupé par AirPlay Receiver sur macOS)
"""
import os
from app import create_app, db

app = create_app(os.environ.get('FLASK_ENV', 'development'))


def _migrate_db():
    """Migrations SQLite légères (ALTER TABLE + CREATE INDEX idempotents)."""
    from sqlalchemy import text
    migrations = [
        # Colonnes ajoutées après v1
        "ALTER TABLE dossier_cache ADD COLUMN source VARCHAR(10) NOT NULL DEFAULT 'BOAMP'",
        "ALTER TABLE user_seen_dossiers ADD COLUMN seen_at DATETIME",
        # Table user_hidden_dossiers (ajoutée après v1)
        """CREATE TABLE IF NOT EXISTS user_hidden_dossiers (
            user_id INTEGER NOT NULL REFERENCES users(id),
            idweb VARCHAR(20) NOT NULL,
            hidden_at DATETIME,
            PRIMARY KEY (user_id, idweb)
        )""",
        # Index de performance (CREATE INDEX IF NOT EXISTS est idempotent)
        "CREATE INDEX IF NOT EXISTS ix_dossier_cache_score_pertinence ON dossier_cache (score_pertinence)",
        "CREATE INDEX IF NOT EXISTS ix_dossier_cache_has_rectificatif ON dossier_cache (has_rectificatif)",
        "CREATE INDEX IF NOT EXISTS ix_dossier_cache_has_attribution ON dossier_cache (has_attribution)",
        "CREATE INDEX IF NOT EXISTS ix_dossier_cache_date_derniere_activite ON dossier_cache (date_derniere_activite)",
        "CREATE INDEX IF NOT EXISTS ix_dossier_cache_datelimitereponse ON dossier_cache (datelimitereponse)",
        "CREATE INDEX IF NOT EXISTS ix_dossier_cache_dateparution ON dossier_cache (dateparution)",
        "CREATE INDEX IF NOT EXISTS ix_dossier_cache_source ON dossier_cache (source)",
    ]
    with app.app_context():
        with db.engine.connect() as conn:
            for sql in migrations:
                try:
                    conn.execute(text(sql))
                    conn.commit()
                except Exception:
                    pass  # Colonne/index déjà existant → on ignore


def init_db():
    """Crée les tables et l'admin par défaut si la base est vide."""
    with app.app_context():
        db.create_all()

        from app.models import User
        from app import bcrypt

        if User.query.count() == 0:
            admin_email = app.config.get('ADMIN_EMAIL', 'admin@domain.com')
            admin_pwd = os.environ.get('ADMIN_DEFAULT_PASSWORD', 'ChangeMe!2024')

            admin = User(
                prenom='Admin',
                nom='BOAMP',
                email=admin_email,
                password_hash=bcrypt.generate_password_hash(admin_pwd).decode('utf-8'),
                role='ADMIN',
                is_active=True,
                email_confirmed=True,
            )
            db.session.add(admin)
            db.session.commit()

            port = int(os.environ.get('PORT', 5001))
            print()
            print('=' * 60)
            print('  BOAMP × Cohesity — Premier démarrage')
            print('=' * 60)
            print(f'  Compte admin créé :')
            print(f'    Email    : {admin_email}')
            print(f'    Password : {admin_pwd}')
            print(f'  → http://localhost:{port}/auth/login')
            print(f'  ⚠  Changez le mot de passe dès la première connexion !')
            print('=' * 60)
            print()
        else:
            count = User.query.count()
            print(f'[init] Base existante — {count} utilisateur(s) enregistré(s)')

    _migrate_db()


init_db()

if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=int(os.environ.get('PORT', 5001)),
        debug=app.config.get('DEBUG', True),
    )
