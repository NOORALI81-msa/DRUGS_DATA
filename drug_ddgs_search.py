import re
import html
import json
import sqlite3
import requests
import concurrent.futures
from urllib.parse import quote_plus, urlparse
from flask import Flask, request, jsonify, Response
from ddgs import DDGS
from selectolax.parser import HTMLParser

app = Flask(__name__)
app.json.sort_keys = False

# ---------------------------------------------------------
# Config
# ---------------------------------------------------------

TIMEOUT = 12
SCRAPE_TIMEOUT = 10
DDGS_MAX_RESULTS = 12
SCRAPE_WORKERS = 10
SOURCE_WORKERS = 6
USE_DAILYMED_FALLBACK = False
LOCAL_DB_PATH = "drug_search.db"

INDIAN_PHARMACY_DOMAINS = [
    "1mg.com",
    "pharmeasy.in",
    "netmeds.com",
    "apollopharmacy.in",
    "medplusmart.com",
    "truemeds.in",
    "medindia.net",
]

GLOBAL_PHARMACY_DOMAINS = [
    "goodrx.com",
    "rxlist.com",
]

SKIP_URL_FRAGMENTS = [
    "/article", "/blog", "/news/", "/medical-answer", "/molecules/",
    "/salt/", "/faq", "/ingredient", "/category", "/search",
    "/health-topic", "/information", "/about", "/contact",
    "interaction-checker", "/interaction/", "/checker",
    "/rxlist_site_map", "/drugs/alpha_", "/sitemap",
]

JUNK_MARKERS = [
    "uses", "side effect", "faq", "news digest", "blog", "article",
    "pricing for", "top 10", "top 30", "brands in india", "what is",
    "how to", "compare", "substitute", "interaction checker",
    "site map", "drug interaction", "drugs a",
]

GENERIC_STOP_WORDS = {
    "read more", "view", "manufacturer", "brands", "for", "shelf life",
    "months", "pharmacology", "name", "india", "medicine", "tablet",
    "tablets", "capsule", "capsules", "strip", "pack", "brand"
}

GENERIC_CANONICAL_MAP = {
    "acetaminophen": "paracetamol",
    "paracetamol": "paracetamol",
}

STRENGTH_RE = re.compile(r"\b(\d+\.?\d*\s*(?:mg|mcg|ml|g|%|iu))\b", re.I)
FORM_RE = re.compile(
    r"\b(tablet|capsule|cream|ointment|gel|syrup|injection|drops|powder|"
    r"lotion|solution|suspension|spray|inhaler|strip|sachet)\b",
    re.I,
)
PLAIN_STRENGTH_NUMBER_RE = re.compile(r"\b(\d{2,4})\b")
BRACKET_BRAND_RE = re.compile(r"\[([^\]]{2,60})\]")
STRENGTH_SUFFIX_RE = re.compile(r"\s+\d+\.?\d*\s*(?:mg|mcg|ml|g|%|iu)\b.*$", re.I)
FORM_SUFFIX_RE = re.compile(
    r"\s+(?:tablet|capsule|injection|syrup|cream|gel|drops|lotion|solution|"
    r"suspension|powder|ointment|spray|strip|pack|sr|er|xr|cr|od|iv|im|sc)[s]?\b.*$",
    re.I,
)

SCRAPE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

FORM_ALIASES = {
    "tab": "Tablet",
    "tabs": "Tablet",
    "tablet": "Tablet",
    "tablets": "Tablet",
    "cap": "Capsule",
    "caps": "Capsule",
    "capsule": "Capsule",
    "capsules": "Capsule",
    "syrup": "Syrup",
    "inj": "Injection",
    "injection": "Injection",
    "drops": "Drops",
    "drop": "Drops",
    "cream": "Cream",
    "ointment": "Ointment",
    "gel": "Gel",
    "solution": "Solution",
    "suspension": "Suspension",
    "spray": "Spray",
    "infusion": "Injection",
}

BAD_BRAND_TOKENS = {
    "store", "medicine", "medicines", "healthcare", "home", "offers", "cart", "search"
}


# ---------------------------------------------------------
# SQLite persistence
# ---------------------------------------------------------

def init_sqlite_db():
    conn = sqlite3.connect(LOCAL_DB_PATH)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS search_api_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query TEXT NOT NULL,
                query_type TEXT,
                generic_name TEXT,
                brand_name TEXT,
                strength TEXT,
                form TEXT,
                source_urls_json TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def save_results_to_sqlite(query, query_type, generic_name, results):
    if not results:
        return 0

    conn = sqlite3.connect(LOCAL_DB_PATH)
    try:
        rows = []
        for row in results:
            rows.append(
                (
                    (query or "").strip(),
                    (query_type or "").strip(),
                    (generic_name or "").strip(),
                    (row.get("brand_name") or "").strip(),
                    (row.get("strength") or "").strip(),
                    (row.get("form") or "").strip(),
                    json.dumps(row.get("source_urls") or [], ensure_ascii=False),
                )
            )

        conn.executemany(
            """
            INSERT INTO search_api_results
            (query, query_type, generic_name, brand_name, strength, form, source_urls_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def _norm(text):
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _canonical_generic(name):
    n = _norm(name)
    return GENERIC_CANONICAL_MAP.get(n, n)


def _normalize_strength_text(text):
    raw = html.unescape(text or "")
    pairs = re.findall(r"(\d+\.?\d*)\s*(mg|mcg|g|%|iu)", raw, re.I)
    parts = []
    for num_txt, unit in pairs:
        try:
            num = float(num_txt)
        except ValueError:
            continue
        if not (0 < num <= 2000):
            continue
        num_norm = str(int(num)) if num.is_integer() else str(num).rstrip("0").rstrip(".")
        item = f"{num_norm}{unit.lower()}"
        if item not in parts:
            parts.append(item)
    return "+".join(parts)


def _is_reasonable_strength(strength):
    if not strength:
        return False
    vals = re.findall(r"(\d+\.?\d*)\s*(mg|mcg|g|%|iu)", strength, re.I)
    if not vals:
        return False
    for num_txt, _ in vals:
        try:
            num = float(num_txt)
        except ValueError:
            return False
        if not (0 < num <= 2000):
            return False
    return True


def parse_strength_form(text):
    t = text or ""
    sm = STRENGTH_RE.search(t)
    fm = FORM_RE.search(t)
    return (sm.group(1) if sm else ""), (fm.group(1).title() if fm else "")


def _normalize_form_token(form_text):
    token = _norm(form_text)
    return FORM_ALIASES.get(token, form_text.title() if form_text else "")


def _infer_strength_form(brand_name, generic_text, page_text, url):
    url_text = urlparse(url or "").path.replace("-", " ").replace("_", " ")
    primary_candidates = [brand_name or "", generic_text or "", url_text]
    fallback_candidates = [(page_text or "")[:1200]]

    strength = ""
    form = ""
    for text in primary_candidates + fallback_candidates:
        s, f = parse_strength_form(text)
        if not strength and s:
            strength = s.lower().replace(" ", "")
        if not form and f:
            form = _normalize_form_token(f)
        if strength and form:
            break

    if not form:
        for token, normalized in FORM_ALIASES.items():
            if re.search(rf"\b{re.escape(token)}\b", _norm(url_text)):
                form = normalized
                break

    if not strength:
        m = PLAIN_STRENGTH_NUMBER_RE.search(_norm(brand_name) + " " + _norm(url_text))
        if m:
            num = int(m.group(1))
            if 50 <= num <= 1200:
                strength = f"{num}mg"

    combo_strength = _normalize_strength_text(" ".join(primary_candidates))
    if combo_strength and ("+" in combo_strength or not strength):
        strength = combo_strength

    if strength and not _is_reasonable_strength(strength):
        strength = ""

    return strength, form


def _clean_brand_text(raw):
    s = html.unescape((raw or "")).strip()
    s = re.sub(r"\bdrug\s+price\s+and\s+information\b.*$", "", s, flags=re.I).strip()
    s = re.sub(r"\bbottle\s+of\s+\d+\s*(?:ml|g)?\b.*$", "", s, flags=re.I).strip()
    s = re.sub(r"\bstrip\s+of\s+\d+\b.*$", "", s, flags=re.I).strip()
    s = re.sub(r"\s+\([^)]*\)", "", s).strip()
    s = re.sub(r"\s*\(.*?\)\s*$", "", s).strip()
    s = STRENGTH_SUFFIX_RE.sub("", s).strip()
    s = FORM_SUFFIX_RE.sub("", s).strip()
    s = re.sub(r"[-|:,]\s*.*$", "", s).strip()
    return s


def is_indian_domain(site):
    s = (site or "").lower()
    return s.endswith(".in") or ".co.in" in s or any(d in s for d in INDIAN_PHARMACY_DOMAINS)


def _api_get(url):
    try:
        return requests.get(url, headers=SCRAPE_HEADERS, timeout=TIMEOUT).json()
    except Exception:
        return None


def _clean_generic_text(raw, fallback):
    text = html.unescape((raw or "")).strip().lower()
    if not text:
        return _canonical_generic(fallback)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"^(?:generic\s*name|generic|composition|salt\s*composition|active\s*ingredient)\s*[:\-]\s*", "", text)
    text = text.replace(" / ", "+").replace("/", "+")
    text = re.split(r"[|;,]", text)[0]
    text = re.sub(r"\b(?:read more|view .*|manufacturer.*|shelf life.*)$", "", text).strip()
    tokens = [t for t in re.split(r"\s+", text) if t and t not in GENERIC_STOP_WORDS]
    cleaned = " ".join(tokens[:6]).strip()
    if not cleaned:
        return _canonical_generic(fallback)
    cleaned = cleaned.replace("acetaminophen", "paracetamol")
    cleaned = cleaned.replace(" + ", "+")
    return _canonical_generic(cleaned)


def _extract_labelled_names(page_text):
    text = html.unescape(page_text or "")

    def _pick(patterns):
        for pat in patterns:
            m = re.search(pat, text, re.I)
            if m:
                val = m.group(1).strip()
                if val:
                    return val
        return ""

    brand_val = _pick([
        r"(?:brand\s*name|product\s*name|trade\s*name)\s*[:\-]\s*([^\n|;,]{2,100})",
        r"\bbrand\s*[:\-]\s*([^\n|;,]{2,100})",
    ])
    generic_val = _pick([
        r"(?:generic\s*name|non\s*proprietary\s*name|active\s*ingredient)\s*[:\-]\s*([^\n|;,]{2,120})",
        r"salt\s*composition\s*[:\-]\s*([^\n|;,]{2,120})",
        r"composition\s*[:\-]\s*([^\n|;,]{2,120})",
    ])

    return _clean_brand_text(brand_val), _clean_generic_text(generic_val, "")


def _extract_from_embedded_json(html_text, fallback_generic):
    text = html_text or ""

    # Fast raw regex pass for common ecommerce payload keys.
    raw_patterns = [
        r'"saltComposition"\s*:\s*"([^"]{2,180})"',
        r'"genericName"\s*:\s*"([^"]{2,180})"',
        r'"composition"\s*:\s*"([^"]{2,180})"',
        r'"activeIngredient"\s*:\s*"([^"]{2,180})"',
    ]
    for pat in raw_patterns:
        m = re.search(pat, text, re.I)
        if m:
            return _clean_generic_text(html.unescape(m.group(1)), fallback_generic)

    # Parse JSON blocks like __NEXT_DATA__ for nested generic/composition fields.
    for m in re.finditer(r"<script[^>]*>(.*?)</script>", text, re.S | re.I):
        block = (m.group(1) or "").strip()
        if not block or len(block) < 2:
            continue
        if not (block.startswith("{") or block.startswith("[")):
            continue
        try:
            data = json.loads(block)
        except Exception:
            continue

        stack = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                for k, v in node.items():
                    lk = str(k).lower()
                    if lk in {"saltcomposition", "genericname", "composition", "activeingredient"}:
                        if isinstance(v, (str, int, float)):
                            val = str(v).strip()
                            if val:
                                return _clean_generic_text(val, fallback_generic)
                    if isinstance(v, (dict, list)):
                        stack.append(v)
            elif isinstance(node, list):
                for item in node:
                    if isinstance(item, (dict, list)):
                        stack.append(item)

    return ""


DETAIL_SECTION_SPECS = [
    ("titles", "Title", [r"\btitle\b", r"\bproduct\s*title\b", r"\bmedicine\s*title\b"]),
    ("introductions", "Introduction", [r"\bintroduction\b", r"\bproduct introduction\b", r"\boverview\b"]),
    ("descriptions", "Description", [r"\bdescription\b", r"\babout\b", r"\bwhat is\b"]),
    ("indications", "Indications", [r"\bindications\b", r"\bused for\b", r"\buses of\b", r"\buses\b"]),
    ("benefits", "Benefits", [r"\bbenefits\b", r"\bkey benefits\b"]),
    ("ingredients", "Ingredients", [r"\bingredients\b", r"\bcomposition\b", r"\bsalt composition\b", r"\bactive ingredient\b"]),
    ("mechanisms", "Mechanism", [r"\bmechanism\b", r"\bmechanism of action\b", r"\bhow it works\b", r"\bhow .* works\b"]),
    ("pharmacokinetics", "Pharmacokinetics", [r"\bpharmacokinetics\b", r"\bkinetics\b", r"\bpk\b"]),
    ("pharmacodynamics", "Pharmacodynamics", [r"\bpharmacodynamics\b", r"\bdynamics\b", r"\bpd\b"]),
    ("dosage", "Dosage", [r"\bdosage\b", r"\bdose\b", r"\bposology\b"]),
    ("adult_dosage", "Adult dosage", [r"\badult dosage\b", r"\bdosage for adults\b"]),
    ("pediatric_dosage", "Pediatric dosage", [r"\bpediatric dosage\b", r"\bpaediatric dosage\b", r"\bdosage for children\b", r"\bchildren dosage\b"]),
    ("geriatric_dosage", "Geriatric dosage", [r"\bgeriatric dosage\b", r"\bdosage for elderly\b", r"\belderly dosage\b"]),
    ("dosage_form", "Dosage form", [r"\bdosage form\b", r"\bform\b", r"\bavailable forms\b"]),
    ("administration", "Administration", [r"\badministration\b", r"\bhow to use\b", r"\bdirection for use\b", r"\bhow to take\b"]),
    ("side_effects", "Side effects", [r"\bside effects\b", r"\bcommon side effects\b"]),
    ("adverse_effects", "Adverse effects", [r"\badverse effects\b", r"\badverse reactions\b"]),
    ("contraindications", "Contraindications", [r"\bcontraindications\b", r"\bwhen not to use\b", r"\bdo not use\b"]),
    ("interactions", "Interactions", [r"\binteractions\b", r"\bdrug interactions\b"]),
    ("food_interactions", "Food interactions", [r"\bfood interactions\b", r"\binteractions with food\b"]),
    ("drug_interactions", "Drug interactions", [r"\bdrug interactions\b", r"\binteractions with other medicines\b"]),
    ("precautions", "Precautions", [r"\bprecautions\b", r"\bcareful\b", r"\bcaution\b"]),
    ("warnings", "Warnings", [r"\bwarnings\b", r"\bwarning\b"]),
    ("safety_advice", "Safety advice", [r"\bsafety advice\b", r"\bsafety information\b"]),
    ("pregnancy_concerns", "Pregnancy concerns", [r"\bpregnancy\b", r"\buse in pregnancy\b", r"\bpregnant\b"]),
    ("pediatric_concerns", "Pediatric concerns", [r"\bpediatric concerns\b", r"\bpaediatric concerns\b", r"\buse in children\b"]),
    ("storage", "Storage", [r"\bstorage\b", r"\bstore\b", r"\bstoring\b"]),
    ("tips", "Tips", [r"\btips\b", r"\bquick tips\b", r"\bhelpful tips\b"]),
    ("fact_boxes", "Fact box", [r"\bfact box\b", r"\bfacts\b", r"\bkey facts\b"]),
]

ALLOWED_DRUG_DETAIL_KEYS = {
    "title", "brand_name", "generic_name", "strength", "form", "source_site", "source_url",
    "titles", "introductions", "descriptions", "indications", "benefits", "ingredients",
    "mechanisms", "pharmacokinetics", "pharmacodynamics", "dosage", "adult_dosage",
    "pediatric_dosage", "geriatric_dosage", "dosage_form", "administration", "side_effects",
    "adverse_effects", "contraindications", "interactions", "food_interactions", "drug_interactions",
    "precautions", "warnings", "safety_advice", "pregnancy_concerns", "pediatric_concerns",
    "storage", "tips", "fact_boxes", "sections",
    "summary",
}

NOISE_SECTION_MARKERS_RE = re.compile(
    r"__next_f\.push|og:title|og:description|twitter:|viewport-fit|canonical|application/ld\+json|meta\s+name=|meta\s+property=",
    re.I,
)

NAV_NOISE_RE = re.compile(
    r"express delivery|select pincode|hello,?\s*log in|offers\s+cart|shop by category|"
    r"must haves|summer store|pet care|top-selling|top-searched|frequently bought together|"
    r"explore more at pharmeasy|quick links|author details|written by|reviewed by|"
    r"mrp\s*₹|out of stock|made by|offer applicable",
    re.I,
)

MEDICAL_SIGNAL_RE = re.compile(
    r"pain|inflammation|dose|dosage|tablet|capsule|contraindication|side effect|"
    r"interaction|warning|precaution|pregnan|child|pediatric|geriatric|storage|"
    r"active ingredient|composition|mechanism|therapy|analgesic|antipyretic",
    re.I,
)

SECTION_TAIL_CUTOFF_RE = re.compile(
    r"\b(Frequently Asked Questions|FAQs|Articles|Chronic Condition Articles|Vendor Details|"
    r"In Case of Any Issues|Disclaimer|Did you find this helpful|Explore More at Pharmeasy|"
    r"Author Details|Written by|Reviewed by|3 Step Quality)\b",
    re.I,
)


def _trim_section_tail(content):
    c = (content or "").strip()
    if not c:
        return c
    m = SECTION_TAIL_CUTOFF_RE.search(c)
    if m:
        c = c[:m.start()].strip(" :-")
    return c


def _is_high_confidence_section(heading, content, section_key=""):
    h = (heading or "").strip()
    c = (content or "").strip()
    if not h or not c:
        return False

    # Ignore obvious payload/config/script noise.
    if NOISE_SECTION_MARKERS_RE.search(h) or NOISE_SECTION_MARKERS_RE.search(c):
        return False

    if NAV_NOISE_RE.search(c):
        return False

    if re.search(r"<[^>]+>", c):
        return False

    # Require enough natural language text.
    word_count = len(re.findall(r"[A-Za-z]{3,}", c))
    if word_count < 10:
        return False

    # Reject mostly symbolic payload text.
    symbol_count = len(re.findall(r"[\[\]{}\\|]", c))
    if symbol_count > 40:
        return False

    # A practical range for section bodies.
    if len(c) < 50 or len(c) > 2200:
        return False

    if section_key in {"introductions", "descriptions", "titles"} and not MEDICAL_SIGNAL_RE.search(c):
        return False

    if section_key == "interactions" and not re.search(r"interaction", c, re.I):
        return False

    if section_key == "warnings" and not re.search(r"pregnan|breast|driving|alcohol|warning|caution", c, re.I):
        return False

    return True


def _split_into_sections(text, max_sections=60):
    """Split page text into (heading, content) sections using markdown-style headings.
    Falls back to a single Overview section when no headings found.
    """
    src = (text or "").strip()
    if not src:
        return []

    candidates = []

    # 1) Markdown headings: '#', '##', '###'
    for m in re.finditer(r'(?m)^(#{1,3})\s*(.+)\s*$', src):
        candidates.append((m.start(), m.end(), m.group(2).strip()))

    # 2) HTML headings: <h1>..</h1>, <h2>..</h2>, <h3>..</h3>
    for m in re.finditer(r'(?is)<h([1-3])[^>]*>(.*?)</h\1>', src):
        heading_text = re.sub(r'<[^>]+>', '', m.group(2)).strip()
        candidates.append((m.start(), m.end(), heading_text))

    # 3) Underlined headings (line followed by === or ---)
    for m in re.finditer(r'(?m)^([^\r\n]{1,80})\r?\n[=\-]{3,}\r?\n', src):
        candidates.append((m.start(), m.end(), m.group(1).strip()))

    # 4) Short lines ending with colon (e.g., 'Uses:', 'Side effects:')
    for m in re.finditer(r'(?m)^[ \t]*([A-Za-z][A-Za-z0-9 &()\-]{0,80}):\s*$', src):
        candidates.append((m.start(), m.end(), m.group(1).strip()))

    # 5) Lines that contain known keywords (best-effort) as standalone lines
    keyword_pattern = r'Uses|Side effects|Side-effects|Interactions|How to use|Dosage|Indications|Composition|Contraindications|Precautions|Warnings|Adverse reactions|Overdose|Mechanism of action|Ingredients|Substitutes|Storage|How to take|When not to take'
    for m in re.finditer(r'(?im)^[ \t]*(.{1,80}(' + keyword_pattern + r').{0,40})\s*$', src):
        candidates.append((m.start(), m.end(), m.group(1).strip()))

    # Normalize / deduplicate by start index and sort
    unique = {}
    for s, e, h in candidates:
        if s in unique:
            # prefer longer heading text
            if len(h) > len(unique[s][1]):
                unique[s] = (e, h)
        else:
            unique[s] = (e, h)

    headers = sorted([(s, v[0], v[1]) for s, v in unique.items()], key=lambda x: x[0])

    if not headers:
        return [("Overview", src[:20000])]

    sections = []
    for i, (h_start, h_end, h_text) in enumerate(headers):
        start = h_end
        end = headers[i + 1][0] if i + 1 < len(headers) else len(src)
        content = src[start:end].strip()
        content = re.sub(r"\s+", " ", content)
        if len(content) > 20000:
            content = content[:20000].rstrip() + "..."
        sections.append((h_text, content))
        if len(sections) >= max_sections:
            break

    return sections


def _extract_sections_by_markers(text, max_chars=180000):
    """Fallback extractor for pages where heading structure is collapsed.
    Scans for known medical section labels and slices text between labels.
    """
    src = (text or "")[:max_chars]
    if not src:
        return {}

    scan = re.sub(r"\s+", " ", src)

    marker_specs = [
        ("descriptions", [r"\bmedical description\b", r"\bdescription\b", r"\bproduct summary\b"]),
        ("indications", [r"\buses\b", r"\bindications\b", r"\bused for\b"]),
        ("contraindications", [r"\bcontraindications\b"]),
        ("side_effects", [r"\bside effects\b", r"\badverse effects\b", r"\badverse reactions\b"]),
        ("precautions", [r"\bprecautions and warnings\b", r"\bprecautions\b"]),
        ("administration", [r"\bdirections for use\b", r"\bhow to use\b"]),
        ("storage", [r"\bstorage and disposal\b", r"\bstorage\b"]),
        ("dosage", [r"\bdosage\b"]),
        ("mechanisms", [r"\bmode of action\b", r"\bhow does it work\b", r"\bmechanism of action\b"]),
        ("interactions", [r"\binteractions\b", r"\binteractions with other medicines\b"]),
    ]

    found = []
    for key, pats in marker_specs:
        for pat in pats:
            for m in re.finditer(pat, scan, re.I):
                found.append((m.start(), m.end(), key))

    if not found:
        return {}

    found.sort(key=lambda x: x[0])
    # dedupe nearby duplicate markers
    deduped = []
    for item in found:
        if deduped and item[2] == deduped[-1][2] and abs(item[0] - deduped[-1][0]) < 30:
            continue
        deduped.append(item)

    out = {}
    for i, (start, end, key) in enumerate(deduped):
        if key in out:
            continue
        next_start = deduped[i + 1][0] if i + 1 < len(deduped) else len(scan)
        body = scan[end:next_start].strip(" :-")
        if len(body) > 1800:
            body = body[:1800].rstrip() + "..."
        body = _trim_section_tail(body)
        if not body:
            continue
        if not _is_high_confidence_section(key, body, key):
            continue
        out[key] = body

    return out


def _build_drug_details_block(row):
    text = (row.get("full_content") or row.get("snippet") or "").strip()
    def _clean_js_serialized_text(raw):
        if not raw:
            return raw
        s = str(raw)
        # unescape common sequences
        s = s.replace('\\n', '\n').replace('\\r', '\n').replace('\\t', ' ')
        s = s.replace('\\"', '"').replace("\\'", "'")
        # remove JS-style comment fragments
        s = re.sub(r'//[^\r\n]*', ' ', s)
        # remove regex-like escape groups like \b...\b
        s = re.sub(r'\\b[^\\]*\\b', ' ', s)
        # remove JSON-like structural tokens and common array markers
        s = re.sub(r'\[\"?\$\"?,|\],\[|\],|\[|\]', ' ', s)
        # remove leftover key:value pairs "key":"value"
        s = re.sub(r'\"[^\"]+\"\s*:\s*\"[^\"]*\"', ' ', s)
        # strip HTML tags
        s = re.sub(r'<[^>]+>', ' ', s)
        # remove stray backslashes and quotes
        s = s.replace('\\', ' ').replace('\"', ' ').replace('"', ' ').replace("'", ' ')
        # collapse spaces while preserving line boundaries for heading detection
        lines = [re.sub(r'[ \t]+', ' ', ln).strip() for ln in s.splitlines()]
        s = "\n".join([ln for ln in lines if ln]).strip()
        return s
    # clean noisy JS/JSON payloads that sometimes get embedded in page text
    text = _clean_js_serialized_text(text)
    if not text:
        return {}

    brand_name = (row.get("brand_name") or "").strip()
    generic_name = (row.get("generic_name") or "").strip()
    page_title = (row.get("page_title") or "").strip()
    page_title = re.sub(r'\\s+', ' ', (page_title or '')).strip()

    details = {
        "title": page_title or brand_name,
        "brand_name": brand_name,
        "generic_name": generic_name,
        "strength": (row.get("strength") or "").strip(),
        "form": (row.get("form") or "").strip(),
        "dosage_form": (row.get("form") or "").strip(),
        "source_site": (row.get("source_urls") or [{}])[0].get("site", ""),
        "source_url": (row.get("source_urls") or [{}])[0].get("url", ""),
        "sections": [],
    }

    # Seed only stable canonical fields.
    if details["title"]:
        details["titles"] = details["title"]
    if details.get("generic_name"):
        details["ingredients"] = details["generic_name"]

    # First pass: marker-based extraction works better on collapsed ecommerce pages.
    marker_sections = _extract_sections_by_markers(text)
    for key, content in marker_sections.items():
        title = next((t for k, t, _ in DETAIL_SECTION_SPECS if k == key), key.replace("_", " ").title())
        details[key] = content
        # estimate a simple confidence score based on length
        wc = len(re.findall(r"[A-Za-z]{3,}", content))
        conf = min(0.99, max(0.45, wc / 200.0))
        details["sections"].append({"key": key, "title": title, "content": content, "confidence": round(conf, 2)})

    used_keys = {s.get("key") for s in details["sections"]}

    # Second pass: heading-based extraction fills any missing keys.
    secs = _split_into_sections(text)

    # Map detected sections to our desired detail keys using heading names
    for heading, content in secs:
        hlow = (heading or "").lower()
        assigned = False
        for key, title, patterns in DETAIL_SECTION_SPECS:
            if key in used_keys:
                continue
            # match heading text against spec patterns (fast)
            for pat in patterns:
                try:
                    if re.search(pat, heading, re.I):
                        normalized_content = _trim_section_tail(content[:2000].strip())
                        if not normalized_content:
                            break
                        if not _is_high_confidence_section(heading, normalized_content, key):
                            break
                        details[key] = normalized_content
                        wc = len(re.findall(r"[A-Za-z]{3,}", normalized_content))
                        conf = min(0.99, max(0.45, wc / 200.0))
                        details["sections"].append({"key": key, "title": title, "content": normalized_content, "confidence": round(conf, 2)})
                        used_keys.add(key)
                        assigned = True
                        break
                except re.error:
                    continue
            if assigned:
                break

    # Keep sections bounded even when many headings are found.
    if len(details["sections"]) > 12:
        details["sections"] = details["sections"][:12]

    if len(details["sections"]) > 12:
        details["sections"] = details["sections"][:12]

    # Build a short summary from top sections if available.
    def _make_summary(d):
        parts = []
        for k in ("descriptions", "introductions"):
            if d.get(k):
                parts.append(d.get(k))
        if not parts and d.get("sections"):
            parts.append(d["sections"][0].get("content", ""))
        txt = "\n\n".join(parts)[:1200]
        # extract first 2 sentences as a short summary
        sents = re.split(r"(?<=[.!?])\\s+", txt)
        return (sents[0] + (" " + sents[1] if len(sents) > 1 else "")) if sents and sents[0] else txt

    summary = _make_summary(details)
    if summary:
        details["summary"] = summary

    # Keep only requested/curated keys.
    details = {k: v for k, v in details.items() if k in ALLOWED_DRUG_DETAIL_KEYS and v}

    return details


def _sanitize_result_row(row):
    details = row.get("drug_details") or {}
    details = {k: v for k, v in details.items() if k in ALLOWED_DRUG_DETAIL_KEYS and v}
    return {
        "brand_name": (row.get("brand_name") or "").strip(),
        "generic_name": (row.get("generic_name") or "").strip(),
        "strength": (row.get("strength") or "").strip(),
        "dosage_form": (row.get("form") or "").strip(),
        "form": (row.get("form") or "").strip(),
        "source_urls": row.get("source_urls") or [],
        "drug_details": details,
    }


# ---------------------------------------------------------
# RxNorm resolver
# ---------------------------------------------------------

def _get_rxcui(query):
    data = _api_get(f"https://rxnav.nlm.nih.gov/REST/rxcui.json?name={quote_plus(query)}")
    ids = (data or {}).get("idGroup", {}).get("rxnormId", [])
    if ids:
        return ids[0]
    data = _api_get(
        f"https://rxnav.nlm.nih.gov/REST/approximateTerm.json?term={quote_plus(query)}&maxEntries=1"
    )
    cands = (data or {}).get("approximateGroup", {}).get("candidate", [])
    return cands[0].get("rxcui") if cands else None


def _get_tty_name(rxcui):
    data = _api_get(f"https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/properties.json")
    props = (data or {}).get("properties", {})
    return props.get("tty", ""), props.get("name", "")


def _get_related_names(rxcui, tty):
    data = _api_get(f"https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/related.json?tty={tty}")
    names, pairs = [], []
    for group in (data or {}).get("relatedGroup", {}).get("conceptGroup", []):
        for c in group.get("conceptProperties", []):
            nm = (c.get("name") or "").strip()
            if nm:
                names.append(nm.title())
                pairs.append((c.get("rxcui", ""), nm.title()))
    return names, pairs


def resolve_query(query):
    q = (query or "").strip()
    rxcui = _get_rxcui(q)
    if not rxcui:
        return "unknown", q.title(), []
    tty, rx_name = _get_tty_name(rxcui)
    rx_name = rx_name or q
    if tty in ("IN", "MIN", "PIN"):
        brands, _ = _get_related_names(rxcui, "BN")
        return "generic", rx_name.title(), brands
    if tty in ("BN", "SBD", "BPCK"):
        min_names, min_pairs = _get_related_names(rxcui, "MIN")
        in_names, in_pairs = _get_related_names(rxcui, "IN")
        generic_names = min_names if min_names else in_names
        generic = " / ".join(generic_names) if generic_names else rx_name.title()
        all_pairs = min_pairs + in_pairs
        ingredient_rxcui = all_pairs[0][0] if all_pairs else ""
        brands = []
        if ingredient_rxcui:
            brands, _ = _get_related_names(ingredient_rxcui, "BN")
        if not brands:
            brands = [rx_name.title()]
        return "brand", generic, brands
    return "unknown", rx_name.title(), []


# ---------------------------------------------------------
# DDGS -> product page URL collection
# ---------------------------------------------------------

def _ddgs_query(query_text, max_results):
    try:
        with DDGS() as ddgs:
            return list(ddgs.text(query_text, max_results=max_results))
    except Exception:
        return []


def _is_product_url(url):
    u = (url or "").lower()
    if not u.startswith("http"):
        return False
    if any(frag in u for frag in SKIP_URL_FRAGMENTS):
        return False
    path = urlparse(url).path.lower()
    domain = urlparse(url).netloc.lower().replace("www.", "")

    if "1mg.com" in domain:
        return bool(re.search(r"/drugs/[a-z0-9][a-z0-9\-]+\-\d+", path))
    if "pharmeasy.in" in domain:
        return bool(re.search(r"/online-medicine-order/[a-z0-9][a-z0-9\-]+", path))
    if "netmeds.com" in domain:
        return bool(re.search(r"/product/[a-z0-9][a-z0-9\-]+", path))
    if "apollopharmacy.in" in domain:
        return bool(re.search(r"/medicine/[a-z0-9][a-z0-9\-]+", path))
    if "medplusmart.com" in domain:
        return bool(re.search(r"/product/[a-z0-9][a-z0-9\-]+", path))
    if "truemeds.in" in domain:
        return bool(re.search(r"/medicine/[a-z0-9][a-z0-9\-]+", path))
    if "medindia.net" in domain:
        return bool(re.search(r"/drug-price/[a-z0-9\-]+/[a-z0-9\-]+\.htm$", path))
    if "goodrx.com" in domain:
        return bool(re.search(r"/(?:drugs|conditions)/[a-z0-9\-]+", path))
    if "rxlist.com" in domain:
        return bool(re.search(r"/drugs/[a-z0-9\-]+\.htm$", path))

    # Generic: any site with a deep path (2+ segments)
    segments = [s for s in path.split("/") if s]
    return len(segments) >= 2


def collect_ddgs_urls(generic, query):
    queries = [
        (
            f"{generic} tablet india "
            "site:pharmeasy.in OR site:1mg.com OR site:netmeds.com "
            "OR site:apollopharmacy.in OR site:medplusmart.com OR site:truemeds.in"
        ),
        (
            f"{query} medicine brand "
            "site:1mg.com OR site:pharmeasy.in OR site:netmeds.com OR site:apollopharmacy.in"
        ),
        (
            f"{query} medicine "
            "site:truemeds.in OR site:medplusmart.com OR site:medindia.net"
        ),
        f"{generic} brand list site:rxlist.com OR site:goodrx.com",
        f"site:1mg.com/drugs {query}",
        f"site:netmeds.com/product {query}",
        f"site:pharmeasy.in/online-medicine-order {query}",
        f"site:apollopharmacy.in/medicine {query}",
        f"site:truemeds.in/medicine {query}",
        f"site:medplusmart.com/product {query}",
    ]
    all_hits = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(queries)) as ex:
        futs = [ex.submit(_ddgs_query, q, DDGS_MAX_RESULTS) for q in queries]
        for f in concurrent.futures.as_completed(futs):
            try:
                all_hits.extend(f.result())
            except Exception:
                pass

    seen = set()
    urls = []
    for h in all_hits:
        url = (h.get("href") or "").strip()
        if not url or url in seen:
            continue
        if _is_product_url(url):
            seen.add(url)
            urls.append(url)
    return urls


# ---------------------------------------------------------
# Page scraper
# ---------------------------------------------------------

def _fetch_html(url):
    try:
        r = requests.get(url, headers=SCRAPE_HEADERS, timeout=SCRAPE_TIMEOUT, allow_redirects=True)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass
    return None


def _extract_from_jsonld(html_text):
    brand_name = ""
    ingredient = ""
    for m in re.finditer(
        r'<script[^>]+type=["\x27]application/ld\+json["\x27][^>]*>(.*?)</script>',
        html_text, re.S | re.I
    ):
        try:
            data = json.loads(m.group(1))
            if isinstance(data, list):
                data = data[0]
            dtype = str(data.get("@type") or "").lower()
            if dtype in ("product", "drug", "medicalentity", "medicaltherapy"):
                brand_name = brand_name or str(data.get("name") or "").strip()
                ai = data.get("activeIngredient") or data.get("nonProprietaryName") or ""
                if isinstance(ai, list):
                    ai = " / ".join(str(x) for x in ai)
                ingredient = ingredient or str(ai).strip()
        except Exception:
            continue
    return brand_name, ingredient


def _extract_composition_from_text(page_text):
    for pat in [
        r"salt\s+composition[:\s]+([^\n.!?(]{3,100})",
        r"composition[:\s]+([^\n.!?(]{3,100})",
        r"generic name[:\s]+([^\n.!?(]{3,100})",
        r"active ingredient[:\s]+([^\n.!?(]{3,100})",
        r"contains[:\s]+([^\n.!?(]{3,100})",
    ]:
        m = re.search(pat, page_text, re.I)
        if m:
            val = m.group(1).strip()
            if val and len(val) < 120 and not any(j in val.lower() for j in JUNK_MARKERS):
                return _clean_generic_text(val, "")
    return ""


def _scrape_page(url, generic):
    html = _fetch_html(url)
    if not html:
        return None

    site = urlparse(url).netloc.replace("www.", "").lower()
    tree = HTMLParser(html)
    body_node = tree.css_first("body")
    page_text = body_node.text(separator="\n", strip=True) if body_node else html

    page_title = ""
    title_node = tree.css_first("title")
    if title_node:
        page_title = title_node.text(strip=True)

    brand_name = ""
    scraped_generic = ""

    # 1) JSON-LD structured data
    brand_name, scraped_generic = _extract_from_jsonld(html)
    if _norm(brand_name) in BAD_BRAND_TOKENS:
        brand_name = ""

    # 1b) Explicit label-driven extraction from page text.
    # If the page says "Brand Name" or "Generic Name", prefer those fields.
    labelled_brand, labelled_generic = _extract_labelled_names(page_text)
    if labelled_brand and _norm(labelled_brand) not in BAD_BRAND_TOKENS:
        brand_name = labelled_brand
    if labelled_generic:
        scraped_generic = labelled_generic

    # 1c) Embedded JSON payload extraction (common on SPA ecommerce sites).
    if not scraped_generic:
        embedded_generic = _extract_from_embedded_json(html, generic)
        if embedded_generic:
            scraped_generic = embedded_generic

    # 2) Site-specific CSS selectors for product title
    if not brand_name:
        title_selectors = [
            "[class*='medicineName']",
            "[class*='ProductTitle']",
            "[class*='DrugTitle']",
            "[class*='drug-name']",
            "[class*='product-name']",
            "[class*='ProductName']",
            "h1",
        ]
        for sel in title_selectors:
            node = tree.css_first(sel)
            if node:
                raw = node.text(strip=True)
                cleaned = _clean_brand_text(raw)
                if _norm(cleaned) in BAD_BRAND_TOKENS:
                    continue
                if cleaned and _norm(cleaned) != _norm(generic):
                    brand_name = cleaned
                    break

    # 3) <title> tag fallback
    if not brand_name:
        title_node = tree.css_first("title")
        if title_node:
            raw = title_node.text(strip=True)
            cleaned = _clean_brand_text(re.split(r"[:|]", raw)[0])
            if _norm(cleaned) in BAD_BRAND_TOKENS:
                cleaned = ""
            if cleaned and _norm(cleaned) != _norm(generic):
                brand_name = cleaned

    # 4) Extract composition / generic name
    if not scraped_generic:
        composition_selectors = [
            "[class*='composition']",
            "[class*='Composition']",
            "[class*='salt']",
            "[class*='Salt']",
            "[class*='generic']",
            "[class*='GenericName']",
            "[class*='ingredient']",
        ]
        for sel in composition_selectors:
            node = tree.css_first(sel)
            if node:
                val = node.text(strip=True)[:200]
                if val and _norm(val) != _norm(brand_name):
                    scraped_generic = val
                    break
        if not scraped_generic:
            scraped_generic = _extract_composition_from_text(page_text)

    brand_name = _clean_brand_text(brand_name)
    if not brand_name or _norm(brand_name) == _norm(generic):
        return None
    if any(j in _norm(brand_name) for j in JUNK_MARKERS):
        return None
    if scraped_generic and _norm(brand_name) == _norm(scraped_generic):
        return None

    if re.search(r"\b(?:inc|llc|gmbh|corporation|distributors|pharmaceutical|pharmaceutics)\b", _norm(brand_name)):
        return None

    strength, form = _infer_strength_form(brand_name, scraped_generic, page_text, url)
    resolved_generic = (
        _clean_generic_text(scraped_generic, generic) if scraped_generic
        else _canonical_generic(generic)
    )
    if _norm(resolved_generic) == _norm(brand_name):
        resolved_generic = _canonical_generic(generic)

    return {
        "brand_name": brand_name,
        "generic_name": resolved_generic,
        "strength": strength,
        "form": form,
        "page_title": page_title,
        "full_content": page_text,
        "snippet": page_text[:600],
        "source_site": site,
        "source_url": url,
    }


def scrape_urls(urls, generic):
    rows = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=SCRAPE_WORKERS) as ex:
        futs = {ex.submit(_scrape_page, url, generic): url for url in urls}
        for f in concurrent.futures.as_completed(futs):
            try:
                result = f.result()
                if result:
                    rows.append(result)
            except Exception:
                pass
    return rows


# ---------------------------------------------------------
# RxNorm + DailyMed supplementary sources
# ---------------------------------------------------------

def search_rxnorm(generic, brand_names):
    results = []
    data = _api_get(f"https://rxnav.nlm.nih.gov/REST/drugs.json?name={quote_plus(generic)}")
    groups = (data or {}).get("drugGroup", {}).get("conceptGroup", [])
    for g in groups:
        if g.get("tty") not in ("SBD", "BN", "BPCK"):
            continue
        for c in g.get("conceptProperties", []):
            name = (c.get("name") or "").strip()
            if not name:
                continue
            bm = BRACKET_BRAND_RE.search(name)
            brand = bm.group(1).strip().title() if bm else ""
            if not brand and g.get("tty") == "BN":
                brand = name.strip().title()
            if not brand or _norm(brand) == _norm(generic):
                continue
            strength, form = parse_strength_form(name)
            results.append({
                "brand_name": brand,
                "generic_name": _canonical_generic(generic),
                "strength": strength,
                "form": form,
                "source_site": "rxnav.nlm.nih.gov",
                "source_url": "https://rxnav.nlm.nih.gov/",
            })
    return results


def search_dailymed(generic, brand_names):
    if not USE_DAILYMED_FALLBACK:
        return []
    results = []
    data = _api_get(
        f"https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.json"
        f"?drug_name={quote_plus(generic)}&pagesize=40"
    )
    for item in (data or {}).get("data", []):
        title = (item.get("title") or "").strip()
        if not title:
            continue
        bm = BRACKET_BRAND_RE.search(title)
        brand = bm.group(1).strip().title() if bm else ""
        if not brand or _norm(brand) == _norm(generic):
            continue
        if any(j in title.lower() for j in JUNK_MARKERS):
            continue
        strength, form = parse_strength_form(title)
        results.append({
            "brand_name": brand,
            "generic_name": _canonical_generic(generic),
            "strength": strength,
            "form": form,
            "source_site": "dailymed.nlm.nih.gov",
            "source_url": f"https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid={item.get('setid')}",
        })
    return results


# ---------------------------------------------------------
# Main search + merge
# ---------------------------------------------------------

def search_variants(query):
    qtype, generic, brand_names = resolve_query(query)
    if not generic:
        generic = query.strip().title()
    generic = _canonical_generic(generic)

    # Step 1: DDGS search + RxNorm (DailyMed optional) — all in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=SOURCE_WORKERS) as ex:
        fut_urls = ex.submit(collect_ddgs_urls, generic, query)
        fut_rxnorm = ex.submit(search_rxnorm, generic, brand_names)
        fut_dailymed = ex.submit(search_dailymed, generic, brand_names) if USE_DAILYMED_FALLBACK else None

        ddgs_urls = fut_urls.result()
        rxnorm_rows = fut_rxnorm.result()
        dailymed_rows = fut_dailymed.result() if fut_dailymed else []

    # Step 2: Scrape product pages found by DDGS
    scraped_rows = scrape_urls(ddgs_urls, generic) if ddgs_urls else []

    # Step 3: Merge — scraped pages provide primary data, RxNorm/DailyMed supplement
    merged = {}
    for r in (scraped_rows + rxnorm_rows + dailymed_rows):
        b = _norm(r.get("brand_name", ""))
        if not b:
            continue
        key = f"{b}|{_norm(r.get('generic_name', generic))}"
        source = {"site": r.get("source_site", ""), "url": r.get("source_url", "")}

        if key not in merged:
            merged[key] = {
                "brand_name": r.get("brand_name", "").strip(),
                "generic_name": (r.get("generic_name") or generic).strip(),
                "strength": r.get("strength", "").strip(),
                "form": r.get("form", "").strip(),
                "page_title": r.get("page_title", "").strip(),
                "full_content": r.get("full_content", "").strip(),
                "snippet": r.get("snippet", "").strip(),
                "source_urls": [source] if source.get("site") else [],
            }
        else:
            existing = merged[key]
            known = {(s.get("site"), s.get("url")) for s in existing["source_urls"]}
            if source.get("site") and (source["site"], source.get("url")) not in known:
                existing["source_urls"].append(source)
            for field in ("strength", "form"):
                if not existing.get(field) and r.get(field):
                    existing[field] = r[field]
            for field in ("page_title", "snippet"):
                if not existing.get(field) and r.get(field):
                    existing[field] = r[field]
            incoming_content = (r.get("full_content") or "").strip()
            if incoming_content and len(incoming_content) > len(existing.get("full_content", "")):
                existing["full_content"] = incoming_content

    results = list(merged.values())

    for row in results:
        row["drug_details"] = _build_drug_details_block(row)

    def _sort_key(row):
        first_site = (row.get("source_urls") or [{}])[0].get("site", "")
        return (
            0 if is_indian_domain(first_site) else 1,
            0 if row.get("full_content") else 1,
            -len(row.get("full_content", "")),
            _norm(row.get("brand_name")),
        )

    results.sort(key=_sort_key)

    if qtype == "unknown" and results:
        qn = _norm(query)
        brand_hits = sum(1 for row in results if qn and qn in _norm(row.get("brand_name", "")))
        if brand_hits >= max(2, len(results) // 2):
            qtype = "brand"

        generic_counts = {}
        for row in results:
            gn = _norm(row.get("generic_name", ""))
            if not gn or gn == qn:
                continue
            generic_counts[gn] = generic_counts.get(gn, 0) + 1
        if generic_counts:
            best_generic = max(generic_counts.items(), key=lambda x: x[1])[0]
            generic = best_generic

    qn = _norm(query)
    if qtype == "brand" and results:
        token_counts = {}
        for row in results:
            gtxt = _norm(row.get("generic_name", ""))
            for tok in re.findall(r"[a-z]{4,}", gtxt):
                if tok in {"generic", "tablet", "capsule", "solution", "suspension", "drops"}:
                    continue
                token_counts[tok] = token_counts.get(tok, 0) + 1
        if token_counts:
            top_token = max(token_counts.items(), key=lambda x: x[1])[0]
            generic = _canonical_generic(top_token)

    if qtype == "brand" and generic:
        inferred_generic = _norm(generic)
        for row in results:
            row_generic = _norm(row.get("generic_name", ""))
            if not row_generic or row_generic == qn:
                row["generic_name"] = inferred_generic

        filtered = []
        for row in results:
            b = _norm(row.get("brand_name", ""))
            g = _norm(row.get("generic_name", ""))
            if qn and qn.split()[0] not in b:
                continue
            if inferred_generic and g and inferred_generic not in g:
                if any(x in g for x in ("vitamin", "ascorbic", "cholecalciferol")):
                    continue
            filtered.append(row)
        if filtered:
            results = filtered

    return qtype, generic, results


# ---------------------------------------------------------
# Flask API
# ---------------------------------------------------------

@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "missing query. Use /api/search?q=ibuprofen"}), 400

    qtype, generic, results = search_variants(q)
    init_sqlite_db()
    saved_count = save_results_to_sqlite(q, qtype, generic, results)
    clean_results = [_sanitize_result_row(r) for r in results]

    # Build a dictionary-to-dictionary style chain so each variant has a "next" block.
    variants = []
    for i, row in enumerate(clean_results):
        next_row = clean_results[i + 1] if i + 1 < len(clean_results) else None
        row_with_next = dict(row)
        row_with_next["next"] = {
            "brand_name": (next_row.get("brand_name") or "").strip(),
            "generic_name": (next_row.get("generic_name") or "").strip(),
            "strength": (next_row.get("strength") or "").strip(),
            "form": (next_row.get("form") or "").strip(),
            "source_urls": next_row.get("source_urls") or [],
        } if next_row else None
        variants.append(row_with_next)

    response = {
        "query": q,
        "query_type": qtype,
        "generic_name": generic,
        "total_variants": len(variants),
        "saved_to_sqlite": saved_count,
        "sqlite_db": LOCAL_DB_PATH,
        "featured_result": variants[0] if variants else {},
        "drug_details": ((variants[0] or {}).get("drug_details") if variants else {}),
        "results": variants,
    }
    return Response(json.dumps(response, ensure_ascii=False, indent=2), mimetype="application/json")


@app.route("/")
def home():
    return "<h2>Drug Variant Search API</h2><p>Use /api/search?q=ibuprofen</p>"


if __name__ == "__main__":
    init_sqlite_db()
    print("Server running at http://localhost:5000")
    print("API: http://localhost:5000/api/search?q=ibuprofen")
    app.run(port=5000, debug=True)