from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import UserMixin
from . import db

# --- Role constants ---
ROLE_DFO     = 'dfo'           # District Finance Officer → /
ROLE_VERIFIER = 'verifier'     # Field Verifier → /verifier
ROLE_ADMIN   = 'state_admin'   # State DBT Admin → /heatmap
ROLE_AUDIT   = 'audit'         # Audit Team → /heatmap (read-only)

ROLE_HOME = {
    ROLE_DFO:      '/',
    ROLE_VERIFIER: '/verifier',
    ROLE_ADMIN:    '/heatmap',
    ROLE_AUDIT:    '/audit',
}

class User(UserMixin, db.Model):
    id       = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=False)
    full_name = db.Column(db.String(120))
    role      = db.Column(db.String(30), nullable=False)  # see constants above

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    @property
    def initials(self):
        parts = (self.full_name or self.username).split()
        return ''.join(p[0].upper() for p in parts[:2])

    @property
    def portal(self):
        return ROLE_HOME.get(self.role, '/')



class Beneficiary(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    aadhaar_number = db.Column(db.String(12), index=True)
    name_english = db.Column(db.String(100)) # e.g. "Ramesh Patel"
    name_transliterated = db.Column(db.String(100)) # e.g. variants, or simulated DB variants like "Rames Patal"
    bank_account_no = db.Column(db.String(20), index=True)
    ifsc_code = db.Column(db.String(11))
    district = db.Column(db.String(50))
    is_deceased = db.Column(db.Boolean, default=False)
    death_date = db.Column(db.Date, nullable=True)
    # Bank feedback for undrawn funds
    last_withdrawal_date = db.Column(db.Date, nullable=True)
    current_balance = db.Column(db.Float, default=0.0)
    
    def to_dict(self):
        return {
            'id': self.id,
            'aadhaar_number': self.aadhaar_number,
            'name_english': self.name_english,
            'bank_account_no': self.bank_account_no,
            'district': self.district
        }

class Transaction(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    beneficiary_id = db.Column(db.Integer, db.ForeignKey('beneficiary.id'))
    scheme_id = db.Column(db.String(20)) # e.g. 'SCHEME_A', 'SCHEME_B'
    amount = db.Column(db.Float)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    status = db.Column(db.String(20), default='SUCCESS') # SUCCESS, FAILED
    
    beneficiary = db.relationship('Beneficiary', backref=db.backref('transactions', lazy=True))

class Alert(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    transaction_id = db.Column(db.Integer, db.ForeignKey('transaction.id'), nullable=True) # Could be null if it's a general beneficiary alert
    beneficiary_id = db.Column(db.Integer, db.ForeignKey('beneficiary.id'))
    leakage_type = db.Column(db.String(50)) # e.g. DECEASED, DUPLICATE_IDENTITY, UNDRAWN_FUNDS, CROSS_SCHEME
    risk_score = db.Column(db.Integer) # 0 to 100
    evidence = db.Column(db.Text) # JSON string with detail
    status = db.Column(db.String(20), default='PENDING') # PENDING, ASSIGNED, VERIFIED, FALSE_POSITIVE
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    transaction = db.relationship('Transaction')
    beneficiary = db.relationship('Beneficiary')
    
    def to_dict(self):
        return {
            'id': self.id,
            'transaction_id': self.transaction_id,
            'beneficiary_id': self.beneficiary_id,
            'beneficiary_name': self.beneficiary.name_english,
            'district': self.beneficiary.district,
            'leakage_type': self.leakage_type,
            'risk_score': self.risk_score,
            'evidence': self.evidence,
            'status': self.status,
            'created_at': self.created_at.isoformat()
        }

class Verification(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    alert_id = db.Column(db.Integer, db.ForeignKey('alert.id'))
    verifier_id = db.Column(db.String(50)) # e.g. "user_123"
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    comments = db.Column(db.Text)
    verified_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    alert = db.relationship('Alert', backref=db.backref('verifications', lazy=True))
