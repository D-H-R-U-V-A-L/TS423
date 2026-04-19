"""
app/services/name_matcher.py
────────────────────────────
Gujarati-transliteration-aware name normalisation and similarity scoring.

Why this matters
────────────────
When Gujarat government data is entered by different clerks the same
person's name can appear in many romanised forms, e.g.

  Ramesh  ↔ Rames  ↔ Ramesh   (silent 'sh' → 's')
  Bhavesh ↔ Bavesh             (aspirated 'bh' → 'b')
  Varun   ↔ Warun              ('v' ↔ 'w', very common in Gujarati)
  Pooja   ↔ Puja  ↔ Poojaa    (vowel length: 'oo' → 'u', double-a → a)
  Thakkar ↔ Takkar ↔ Thakur   (aspirated 'th' → 't', double → single)
  Mehul   ↔ Mehool ↔ Mehull   (vowel + double-letter collapses)
  Naik    ↔ Naek               ('ai' → 'e' diphthong)
  Vora    ↔ Wora  ↔ Vorra     (v/w + double letter)

Strategy (multi-layer, ranked by strength)
────────────────────────────────────────────
  1.  Normalise both names with identical Gujarati-specific rules →
      exact match on the normalised form scores 100.
  2.  token_sort_ratio on normalised forms handles first/last name
      order swaps ("Patel Ramesh" == "Ramesh Patel").
  3.  plain ratio on normalised forms handles partial string diffs.
  4.  token_sort_ratio on raw lowercase text as a fallback.
  Final score = max across all four methods.

Phonetic bucketing (for O(n log n) cross-dataset search)
──────────────────────────────────────────────────────────
  normalize() returns a deterministic canonical key. Two names that
  normalise to the same key are guaranteed to be similar, so we can
  group by this key in Pandas with a single .groupby() call instead
  of O(n²) pairwise comparisons.
"""

import re
import unicodedata
from thefuzz import fuzz


# ── 1.  Suffix / honorific tokens to strip ────────────────────────────────────
# These attach to names in Gujarati writing and should not affect identity.
_STRIP_TOKENS: set[str] = {
    # gender / relational
    'bhai', 'bha', 'ben', 'bhen', 'bai', 'bahen',
    # caste / status suffixes
    'kumar', 'kumari', 'kumarji', 'lal', 'lall',
    'devi', 'shri', 'shree',
    # titles / salutations
    'smt', 'km', 'mr', 'mrs', 'dr', 'ji',
}


# ── 2.  Phonetic substitution rules (order is critical – longest first) ───────
#
# Each tuple is (raw_regex_pattern, replacement).
# Applied to lowercase ASCII text.  Applying the same rules to BOTH sides of
# a comparison is what makes symmetric swaps (v↔w) collapse to one form.
#
_RAW_SUBS: list[tuple[str, str]] = [
    # ── Aspirated consonant clusters → unaspirated ─────────────────────────
    # This is the single most impactful Gujarati romanisation rule.
    (r'chh',  'c'),     # chh → c  (Chhatrala → Catrala)
    (r'ch',   'c'),     # ch  → c  (Chandrika → Candrika)
    (r'bh',   'b'),     # bh  → b  (Bhavesh → Bavesh)
    (r'dh',   'd'),     # dh  → d  (Dhruv → Druv)
    (r'gh',   'g'),     # gh  → g  (Ghosh → Gos)
    (r'jh',   'j'),     # jh  → j
    (r'kh',   'k'),     # kh  → k  (Khamar → Kamar)
    (r'ph',   'p'),     # ph  → p  (Phulchand → Pulcand)
    (r'sh',   's'),     # sh  → s  (Sharma → Sarma, Sheth → Set)
    (r'th',   't'),     # th  → t  (Thakkar → Takkar)

    # ── v / w are phonetically identical in Gujarati ───────────────────────
    (r'w',    'v'),     # normalise w → v  (Warun → Varun, Wora → Vora)

    # ── Vowel length collapsing ────────────────────────────────────────────
    (r'aa',   'a'),     # Poojaa → Puja  (after next rule)
    (r'ee',   'i'),     # Preethi → Priti
    (r'ii',   'i'),
    (r'oo',   'u'),     # Pooja → Puja
    (r'ou',   'u'),     # Gour → Gur
    (r'ai',   'e'),     # Naik → Nek  (Kutchi/Gujarati diphthong)
    (r'au',   'o'),     # Kaul → Kol

    # ── Silent / dropped 'h' in many Gujarati words ────────────────────────
    # Apply AFTER aspirated clusters are already collapsed above.
    (r'h',    ''),      # Mahesh → maes → mes,  Shah → Sa

    # ── Double letters → single ────────────────────────────────────────────
    (r'(.)\1+', r'\1'), # Thakkar → Takar, Lall → Lal, Mehull → Meul
]

# Pre-compile for speed
_SUBS: list[tuple[re.Pattern, str]] = [
    (re.compile(p), r) for p, r in _RAW_SUBS
]


# ── 3.  Core normalisation function ───────────────────────────────────────────

def _to_ascii_lower(text: str) -> str:
    """Lowercase → strip diacritics → keep only a–z and spaces."""
    text = unicodedata.normalize('NFKD', text.lower())
    text = text.encode('ascii', 'ignore').decode('ascii')
    return re.sub(r'[^a-z ]', '', text)


def normalize(name: str) -> str:
    """
    Produce a deterministic phonetic key for a Gujarati romanised name.

    Examples
    --------
    >>> normalize("Ramesh Bhai Sharma")
    'rames sarma'
    >>> normalize("Rames Sarma")           # same! ← exact phonetic match
    'rames sarma'
    >>> normalize("Varun Thakkar")
    'varun takar'
    >>> normalize("Warun Takkar")          # same! ← v/w + double letter
    'varun takar'
    >>> normalize("Bhavesh Patel")
    'baves patel'
    >>> normalize("Bavesh Patel")          # same! ← aspirated 'bh'
    'baves patel'
    """
    if not name:
        return ''
    s = _to_ascii_lower(str(name))

    # Remove known suffix / honorific tokens
    tokens = [t for t in s.split() if t not in _STRIP_TOKENS]
    s = ' '.join(tokens)

    # Apply phonetic substitutions
    for pattern, replacement in _SUBS:
        s = pattern.sub(replacement, s)

    # Collapse whitespace
    return re.sub(r'\s+', ' ', s).strip()


# ── 4.  Composite similarity scorer ───────────────────────────────────────────

def gujarati_similarity(name_a: str, name_b: str) -> int:
    """
    Return a 0–100 similarity score between two Gujarati-romanised names.

    The score is the MAXIMUM of four strategies so we never miss a
    match due to one method's blind spot:

      - 100 if normalised forms are identical        (exact phonetic match)
      - token_sort_ratio on normalised text          (handles A↔B order swap)
      - plain ratio on normalised text               (partial-string match)
      - token_sort_ratio on raw lowercase            (already-correct spellings)

    Parameters
    ----------
    name_a, name_b : str
        Romanised Gujarati names (may contain mixed case, honorifics, etc.)

    Returns
    -------
    int
        Similarity score in [0, 100].
    """
    if not name_a or not name_b:
        return 0

    norm_a = normalize(name_a)
    norm_b = normalize(name_b)

    # Exact phonetic key match → guaranteed same person
    if norm_a == norm_b:
        return 100

    score = max(
        fuzz.token_sort_ratio(norm_a, norm_b),   # norm + order-invariant
        fuzz.ratio(norm_a, norm_b),               # norm + positional
        fuzz.token_sort_ratio(                    # raw fallback
            name_a.lower(), name_b.lower()
        ),
    )
    return score


# ── 5.  Batch similarity helper (for Pandas apply) ────────────────────────────

def batch_normalize(series: 'pd.Series') -> 'pd.Series':
    """Apply normalize() to every element of a Pandas Series."""
    return series.fillna('').apply(normalize)


# ── 6.  Demonstration table (useful for hackathon live demo) ──────────────────

DEMO_PAIRS: list[tuple[str, str, str]] = [
    # (name_a,              name_b,               rule demonstrated)
    ("Ramesh Sharma",       "Rames Sarma",        "sh→s"),
    ("Bhavesh Patel",       "Bavesh Patel",        "bh→b"),
    ("Varun Thakkar",       "Warun Takkar",        "v↔w  +  th→t  +  double→single"),
    ("Pooja Desai",         "Puja Desai",          "oo→u"),
    ("Naik Mahendra",       "Nek Maendra",         "ai→e  +  h-drop"),
    ("Priya Ben Shah",      "Priya Sa",            "honorific strip + h-drop"),
    ("Dhruv Vora",          "Druv Wora",           "dh→d  +  v↔w"),
    ("Thakur Rajan",        "Takur Rajan",         "th→t"),
    ("Chandrika Mehta",     "Candrika Meta",       "ch→c  +  h-drop"),
    ("Mehul Lall Trivedi",  "Mehul Trivedi",       "honorific-lal strip"),
    ("Vipul Bhai Patel", "Vipul Patel", "honorific-bhai strip"), 
]
