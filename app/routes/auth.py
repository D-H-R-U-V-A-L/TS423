from flask import Blueprint, render_template, redirect, url_for, request, flash
from flask_login import login_user, logout_user, login_required, current_user
from ..models import User, ROLE_HOME

bp = Blueprint('auth', __name__)


@bp.route('/login', methods=['GET', 'POST'])
def login():
    # Already logged in → go to their home portal
    if current_user.is_authenticated:
        return redirect(current_user.portal)

    error = None
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')

        user = User.query.filter_by(username=username).first()

        if user and user.check_password(password):
            login_user(user, remember=False)
            # Always redirect to the user's designated portal, ignore next param
            return redirect(user.portal)
        else:
            error = 'Invalid username or password.'

    return render_template('login.html', error=error)


@bp.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('auth.login'))
