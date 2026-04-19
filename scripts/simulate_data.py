import sys
import os
import random
from datetime import datetime, timedelta
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from faker import Faker
from app import create_app, db
from app.models import Beneficiary, Transaction, User, ROLE_DFO, ROLE_VERIFIER, ROLE_ADMIN, ROLE_AUDIT

fake = Faker('en_IN')
GUJARAT_DISTRICTS = ['Ahmedabad', 'Surat', 'Vadodara', 'Rajkot', 'Bhavnagar', 'Jamnagar', 'Junagadh', 'Gandhinagar']
SCHEMES = ['Vidholia_Pension', 'Kisan_Sahay', 'Student_Scholarship']

# ---------------------------------------------------------------------------
# Gujarati transliteration variant rules
# Each entry: (find, replace). Applied randomly to generate realistic variants.
# These mirror the exact rules in name_matcher.SUBS — so the matcher can
# detect what this generator intentionally obscures.
# ---------------------------------------------------------------------------
_VARIANT_RULES = [
    ('sh', 's'),     # Sharma ↔ Sarma
    ('bh', 'b'),     # Bhavesh ↔ Bavesh
    ('th', 't'),     # Thakkar ↔ Takkar
    ('dh', 'd'),     # Dhruv ↔ Druv
    ('kh', 'k'),     # Khanna ↔ Kanna
    ('ch', 'c'),     # Chandrika ↔ Candrika
    ('v',  'w'),     # Varun ↔ Warun  (most common swap!)
    ('oo', 'u'),     # Pooja ↔ Puja
    ('aa', 'a'),     # Sharma ↔ Sarma (after sh→s: Sharmaa→ Sarma)
    ('ai', 'e'),     # Naik ↔ Nek
]

# Known Gujarati name pairs (canonical, variant) for the phonetic-bucket demo
# These use DIFFERENT first/last name order to demonstrate token-sort robustness
GUJARATI_PAIRS = [
    ("Ramesh Sharma",      "Rames Sarma"),
    ("Bhavesh Patel",      "Bavesh Patel"),
    ("Varun Thakkar",      "Warun Takkar"),
    ("Pooja Desai",        "Puja Desai"),
    ("Naik Mahendra",      "Nek Maendra"),
    ("Dhruv Vora",         "Druv Wora"),
    ("Chandrika Shah",     "Candrika Sa"),
    ("Thakur Lalji",       "Takur Lalji"),
    ("Khushboo Mehta",     "Kusbu Meta"),
    ("Vishnu Trivedi",     "Wisnu Trivedi"),
    ("Shobha Rani",        "Soba Rani"),
    ("Bharat Kumar",       "Barat Kumar"),
    ("Aakhil Shah",        "Akil Sa"),
    ("Khodabhai Chauhan",  "Kodabai Cauan"),
    ("Preethi Joshi",      "Priti Joshi"),
    ("Dhanraj Solanki",    "Danraj Solanki"),
    ("Harshad Bhatt",      "Harsat Bat"),
    ("Ghanshyam Patel",    "Gansyam Patel"),
    ("Vipul Lakhani",      "Wipul Lakani"),
    ("Ashok Bhai Vasava",  "Asok Vasava"),
]


def get_transliterated_variant(name: str) -> str:
    """
    Apply 1–3 random Gujarati transliteration rules to produce a realistic
    variant. Uses the same rule set as name_matcher so the engine can detect them.
    """
    result = name
    rules = random.sample(_VARIANT_RULES, k=random.randint(1, 3))
    for find, replace in rules:
        result = result.replace(find, replace)
    return result


def seed_users():
    """Create demo user accounts for each role. Idempotent."""
    demo_users = [
        {'username': 'dfo_officer',    'password': 'dfo123',    'full_name': 'Priya Sharma',       'role': ROLE_DFO},
        {'username': 'field_verifier', 'password': 'verify123', 'full_name': 'Rajan Patel',        'role': ROLE_VERIFIER},
        {'username': 'state_admin',    'password': 'admin123',  'full_name': 'Anil Mehta',         'role': ROLE_ADMIN},
        {'username': 'audit_user',     'password': 'audit123',  'full_name': 'Sneha Joshi',        'role': ROLE_AUDIT},
    ]
    for u_data in demo_users:
        existing = User.query.filter_by(username=u_data['username']).first()
        if not existing:
            u = User(username=u_data['username'], full_name=u_data['full_name'], role=u_data['role'])
            u.set_password(u_data['password'])
            db.session.add(u)
    db.session.commit()
    print("Demo users seeded OK.")


def run():
    app = create_app()
    with app.app_context():
        print("Dropping and recreating database...")
        db.drop_all()
        db.create_all()

        print("Seeding user accounts...")
        seed_users()

        print("Generating 2000 beneficiaries...")
        beneficiaries = []
        for i in range(2000):
            eng_name = fake.name()
            b = Beneficiary(
                aadhaar_number=str(fake.random_number(digits=12, fix_len=True)),
                name_english=eng_name,
                name_transliterated=eng_name if random.choice([True, False]) else get_transliterated_variant(eng_name),
                bank_account_no=str(fake.random_number(digits=10, fix_len=True)),
                ifsc_code="SBIN" + str(fake.random_number(digits=7, fix_len=True)),
                district=random.choice(GUJARAT_DISTRICTS),
                is_deceased=False,
                last_withdrawal_date=fake.date_between(start_date='-1y', end_date='today'),
                current_balance=random.uniform(100.0, 5000.0)
            )
            beneficiaries.append(b)

        # Inject Anomalies
        print("Injecting Scenarios...")
        # 1. Deceased
        for b in beneficiaries[:50]:
            b.is_deceased = True
            b.death_date = fake.date_between(start_date='-2y', end_date='-1m')
        
        # 2. Duplicate Identity (Same bank account, slightly different name, different aadhaar)
        for i in range(50, 100):
            original = beneficiaries[i]
            duplicate = Beneficiary(
                aadhaar_number=str(fake.random_number(digits=12, fix_len=True)),
                name_english=get_transliterated_variant(original.name_english),
                name_transliterated=get_transliterated_variant(original.name_english),
                bank_account_no=original.bank_account_no, # Same Bank
                ifsc_code=original.ifsc_code,
                district=original.district,
                is_deceased=False,
                last_withdrawal_date=original.last_withdrawal_date,
                current_balance=original.current_balance
            )
            beneficiaries.append(duplicate)
            
        # 3. Undrawn Funds (Stagnant account - last withdrawal was 2 years ago, huge balance)
        for i in range(100, 150):
            b = beneficiaries[i]
            b.last_withdrawal_date = fake.date_between(start_date='-5y', end_date='-3y')
            b.current_balance = random.uniform(50000.0, 150000.0)

        # 2b. Phonetic-bucket duplicates — DIFFERENT bank accounts, DIFFERENT districts.
        #     Only Pattern 2b (vectorized phonetic-bucket scan) can catch these.
        print("Injecting Gujarati phonetic-bucket duplicates (Scenario 2b)...")
        phonetic_pairs = []
        districts = GUJARAT_DISTRICTS.copy()
        from app.services.name_matcher import DEMO_PAIRS as name_pairs
        pairs_to_use = name_pairs  # 10 curated pairs from the module

        for canonical, variant, _rule in pairs_to_use:
            dist_a, dist_b = random.sample(districts, 2)  # different districts

            b_canonical = Beneficiary(
                aadhaar_number=str(fake.random_number(digits=12, fix_len=True)),
                name_english=canonical,
                name_transliterated=canonical,
                bank_account_no=str(fake.random_number(digits=10, fix_len=True)),
                ifsc_code="BARB" + str(fake.random_number(digits=7, fix_len=True)),
                district=dist_a,
                is_deceased=False,
                last_withdrawal_date=fake.date_between(start_date='-6m', end_date='today'),
                current_balance=random.uniform(500.0, 3000.0)
            )
            b_variant = Beneficiary(
                aadhaar_number=str(fake.random_number(digits=12, fix_len=True)),
                name_english=variant,
                name_transliterated=variant,
                bank_account_no=str(fake.random_number(digits=10, fix_len=True)),  # different bank
                ifsc_code="HDFC" + str(fake.random_number(digits=7, fix_len=True)),
                district=dist_b,   # different district
                is_deceased=False,
                last_withdrawal_date=fake.date_between(start_date='-6m', end_date='today'),
                current_balance=random.uniform(500.0, 3000.0)
            )
            phonetic_pairs.extend([b_canonical, b_variant])

        beneficiaries.extend(phonetic_pairs)
        print(f"  -> Injected {len(phonetic_pairs)} phonetic-pair beneficiaries ({len(phonetic_pairs)//2} pairs)")

        db.session.bulk_save_objects(beneficiaries)
        db.session.commit()


        print("Generating 10000+ Transactions...")
        all_b = Beneficiary.query.all()
        transactions = []
        start_date = datetime.now() - timedelta(days=365)
        
        # Give everyone some normal transactions
        for i in range(8000):
            b = random.choice(all_b)
            t = Transaction(
                beneficiary_id=b.id,
                scheme_id=random.choice(SCHEMES),
                amount=random.choice([1000.0, 2000.0, 5000.0]),
                timestamp=fake.date_time_between(start_date=start_date, end_date='now'),
                status='SUCCESS'
            )
            transactions.append(t)
            
        # 4. Cross Scheme Duplication (Exclusive schemes received by same person)
        # Assuming Student_Scholarship and Kisan_Sahay shouldn't ordinarily both be received by exactly same person in same month
        greedy_beneficiaries = beneficiaries[150:200]
        for b in greedy_beneficiaries:
            dt = fake.date_time_between(start_date='-1m', end_date='now')
            t1 = Transaction(beneficiary_id=b.id, scheme_id='Student_Scholarship', amount=5000.0, timestamp=dt, status='SUCCESS')
            t2 = Transaction(beneficiary_id=b.id, scheme_id='Kisan_Sahay', amount=6000.0, timestamp=dt, status='SUCCESS')
            transactions.append(t1)
            transactions.append(t2)

        # Also add recent transactions for deceased to ensure they trigger
        for b in all_b:
            if b.is_deceased:
                transactions.append(Transaction(
                    beneficiary_id=b.id,
                    scheme_id=random.choice(SCHEMES),
                    amount=2000.0,
                    timestamp=fake.date_time_between(start_date='-10d', end_date='now'),
                    status='SUCCESS'
                ))
            # Undrawn gets recent transaction too
            if b.current_balance > 40000:
                transactions.append(Transaction(
                    beneficiary_id=b.id,
                    scheme_id=random.choice(SCHEMES),
                    amount=2000.0,
                    timestamp=fake.date_time_between(start_date='-10d', end_date='now'),
                    status='SUCCESS'
                ))

        db.session.bulk_save_objects(transactions)
        db.session.commit()
        print(f"Database seeded with {len(all_b)} beneficiaries and {len(transactions)} transactions!")

if __name__ == '__main__':
    run()
