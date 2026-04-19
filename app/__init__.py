from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from config import Config

db = SQLAlchemy()
login_manager = LoginManager()

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    db.init_app(app)
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'          # redirect unauthenticated users here
    login_manager.login_message = 'Please log in to access this page.'
    login_manager.login_message_category = 'warning'

    with app.app_context():
        from .models import User, Beneficiary, Transaction, Alert, Verification

        @login_manager.user_loader
        def load_user(user_id):
            return User.query.get(int(user_id))

        from .routes import main, api, auth as auth_bp
        app.register_blueprint(auth_bp.bp)
        app.register_blueprint(main.bp)
        app.register_blueprint(api.bp, url_prefix='/api')

        db.create_all()

    return app
