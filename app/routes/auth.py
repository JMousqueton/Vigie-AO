"""
Routes d'authentification : register, login, logout, profil, confirmation email.
"""
import re
from datetime import datetime

from urllib.parse import urlparse, urljoin

from flask import (
    Blueprint, render_template, redirect, url_for,
    flash, request, current_app,
)
from flask_login import login_user, logout_user, login_required, current_user
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, BooleanField, SelectField, SubmitField
from wtforms.validators import DataRequired, Email, Length, EqualTo, ValidationError

from app import db, bcrypt, limiter
from app.models import User
from app.services.mailer import send_confirmation_email, verify_token

auth_bp = Blueprint('auth', __name__)


# ─── Formulaires ──────────────────────────────────────────────────────────────

class LoginForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Email()])
    password = PasswordField('Mot de passe', validators=[DataRequired()])
    remember = BooleanField('Se souvenir de moi')
    submit = SubmitField('Connexion')


class RegisterForm(FlaskForm):
    prenom = StringField('Prénom', validators=[DataRequired(), Length(2, 50)])
    nom = StringField('Nom', validators=[DataRequired(), Length(2, 50)])
    email = StringField('Email professionnel', validators=[DataRequired(), Email()])
    password = PasswordField('Mot de passe', validators=[DataRequired(), Length(8, 128)])
    password2 = PasswordField(
        'Confirmer le mot de passe',
        validators=[DataRequired(), EqualTo('password', message='Les mots de passe ne correspondent pas.')],
    )
    submit = SubmitField("S'inscrire")

    def validate_email(self, field):
        allowed_domain = current_app.config.get('ALLOWED_EMAIL_DOMAIN', '')
        if allowed_domain and not field.data.lower().endswith(f'@{allowed_domain}'):
            raise ValidationError(f'Seuls les emails @{allowed_domain} sont autorisés.')
        if User.query.filter_by(email=field.data.lower()).first():
            raise ValidationError('Cet email est déjà enregistré.')

    def validate_password(self, field):
        pwd = field.data
        if not re.search(r'[A-Z]', pwd):
            raise ValidationError('Le mot de passe doit contenir au moins une majuscule.')
        if not re.search(r'[0-9]', pwd):
            raise ValidationError('Le mot de passe doit contenir au moins un chiffre.')


class ProfileForm(FlaskForm):
    prenom = StringField('Prénom', validators=[DataRequired(), Length(2, 50)])
    nom = StringField('Nom', validators=[DataRequired(), Length(2, 50)])
    alert_enabled = BooleanField('Recevoir les alertes email')
    alert_frequency = SelectField(
        'Fréquence',
        choices=[
            ('IMMEDIATE', 'Immédiate (toutes les heures)'),
            ('DAILY', 'Quotidienne (chaque matin à 8h)'),
            ('WEEKLY', 'Hebdomadaire (lundi matin)'),
        ],
    )
    submit = SubmitField('Enregistrer')


class ChangePasswordForm(FlaskForm):
    current_password = PasswordField('Mot de passe actuel', validators=[DataRequired()])
    new_password = PasswordField('Nouveau mot de passe', validators=[DataRequired(), Length(8, 128)])
    new_password2 = PasswordField(
        'Confirmer',
        validators=[DataRequired(), EqualTo('new_password', message='Les mots de passe ne correspondent pas.')],
    )
    submit = SubmitField('Changer le mot de passe')


# ─── Routes ───────────────────────────────────────────────────────────────────

@auth_bp.route('/login', methods=['GET', 'POST'])
@limiter.limit('10 per minute')
def login():
    if current_user.is_authenticated:
        return redirect(url_for('main.dashboard'))

    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(email=form.email.data.lower()).first()
        if user and bcrypt.check_password_hash(user.password_hash, form.password.data):
            if not user.is_active:
                flash('Votre compte est en attente d\'activation. Vérifiez vos emails.', 'warning')
                return render_template('auth/login.html', form=form)

            login_user(user, remember=form.remember.data)
            user.last_login = datetime.utcnow()
            db.session.commit()

            next_page = request.args.get('next')
            if next_page:
                ref = urlparse(request.host_url)
                test = urlparse(urljoin(request.host_url, next_page))
                if not (test.scheme in ('http', 'https') and ref.netloc == test.netloc):
                    next_page = None
            current_app.logger.info("Connexion : %s", user.email)
            return redirect(next_page or url_for('main.dashboard'))
        else:
            flash('Email ou mot de passe incorrect.', 'danger')

    return render_template('auth/login.html', form=form)


@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('main.dashboard'))

    form = RegisterForm()
    if form.validate_on_submit():
        auto_activate = current_app.config.get('AUTO_ACTIVATE', True)
        user = User(
            prenom=form.prenom.data.strip(),
            nom=form.nom.data.strip().upper(),
            email=form.email.data.lower().strip(),
            password_hash=bcrypt.generate_password_hash(form.password.data).decode('utf-8'),
            role='USER',
            is_active=auto_activate,
            email_confirmed=auto_activate,
        )
        db.session.add(user)
        db.session.commit()

        if not auto_activate:
            send_confirmation_email(user)
            flash(
                'Inscription réussie ! Un email de confirmation vous a été envoyé.',
                'success',
            )
        else:
            flash('Inscription réussie ! Vous pouvez vous connecter.', 'success')

        current_app.logger.info("Inscription : %s", user.email)
        return redirect(url_for('auth.login'))

    return render_template('auth/register.html', form=form)


@auth_bp.route('/confirm/<token>')
def confirm_email(token):
    email = verify_token(token, salt='email-confirm', max_age=86400)
    if not email:
        flash('Le lien de confirmation est invalide ou expiré.', 'danger')
        return redirect(url_for('auth.login'))

    user = User.query.filter_by(email=email).first()
    if not user:
        flash('Utilisateur introuvable.', 'danger')
        return redirect(url_for('auth.login'))

    if user.email_confirmed:
        flash('Votre compte est déjà activé.', 'info')
    else:
        user.email_confirmed = True
        user.is_active = True
        db.session.commit()
        flash('Compte activé avec succès ! Vous pouvez vous connecter.', 'success')

    return redirect(url_for('auth.login'))


@auth_bp.route('/logout')
@login_required
def logout():
    current_app.logger.info("Déconnexion : %s", current_user.email)
    logout_user()
    flash('Vous êtes déconnecté.', 'info')
    return redirect(url_for('auth.login'))


@auth_bp.route('/profile', methods=['GET', 'POST'])
@login_required
def profile():
    form = ProfileForm(obj=current_user)
    pwd_form = ChangePasswordForm()

    if form.validate_on_submit() and 'submit' in request.form:
        current_user.prenom = form.prenom.data.strip()
        current_user.nom = form.nom.data.strip().upper()
        current_user.alert_enabled = form.alert_enabled.data
        current_user.alert_frequency = form.alert_frequency.data
        db.session.commit()
        flash('Profil mis à jour.', 'success')
        return redirect(url_for('auth.profile'))

    return render_template('auth/profile.html', form=form, pwd_form=pwd_form)


@auth_bp.route('/change-password', methods=['POST'])
@login_required
def change_password():
    pwd_form = ChangePasswordForm()
    if pwd_form.validate_on_submit():
        if not bcrypt.check_password_hash(current_user.password_hash, pwd_form.current_password.data):
            flash('Mot de passe actuel incorrect.', 'danger')
        else:
            current_user.password_hash = bcrypt.generate_password_hash(
                pwd_form.new_password.data
            ).decode('utf-8')
            db.session.commit()
            flash('Mot de passe modifié avec succès.', 'success')
    else:
        for field, errors in pwd_form.errors.items():
            for error in errors:
                flash(error, 'danger')

    return redirect(url_for('auth.profile'))
