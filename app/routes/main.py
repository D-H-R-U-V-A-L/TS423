from flask import Blueprint, render_template, abort
from flask_login import login_required, current_user
from ..models import ROLE_DFO, ROLE_VERIFIER, ROLE_ADMIN, ROLE_AUDIT, ROLE_HOME

bp = Blueprint('main', __name__)


def require_role(*roles):
    """Decorator: user must be logged in AND have one of the allowed roles."""
    from functools import wraps
    def decorator(f):
        @wraps(f)
        @login_required
        def decorated_function(*args, **kwargs):
            if current_user.role not in roles:
                abort(403)
            return f(*args, **kwargs)
        return decorated_function
    return decorator


@bp.route('/')
@require_role(ROLE_DFO)
def dashboard():
    return render_template('dfo_dashboard.html')


@bp.route('/verifier')
@require_role(ROLE_VERIFIER)
def verifier():
    return render_template('verifier_app.html')


@bp.route('/heatmap')
@require_role(ROLE_ADMIN)
def heatmap():
    """State Admin — full control: run engine, view charts, manage alerts."""
    return render_template('admin_heatmap.html')


@bp.route('/audit')
@require_role(ROLE_AUDIT)
def audit():
    """Audit Team — read-only compliance report, no action buttons."""
    return render_template('audit_report.html')


# ── 403 handler ──────────────────────────────────────────────────────────────
@bp.app_errorhandler(403)
def forbidden(e):
    return render_template('403.html'), 403
