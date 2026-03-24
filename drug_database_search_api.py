import re
import json
import sqlite3
import requests
import concurrent.futures
from urllib.parse import quote_plus
from collections import Counter
from flask import Flask, request, jsonify

app = Flask(__name__)

TIMEOUT = 10
DB_SOURCE_WORKERS = 8

LOCAL_DB_PATH = "drug_search.db"
MONGO_URI = "mongodb://localhost:27017"
MONGO_DATABASE = "geometric_crawler"
MONGO_COLLECTION = "spider_items"
INDIAN_SOURCE_HINTS = (
    "pharmeasy",
    "1mg",
    "netmeds",
    "apollo",
    "medplus",
    "truemeds",
    "medindia",
    "practo",
    "india",
    ".in",
)


def _api_get(url):
    try:
        return requests.get(url, timeout=TIMEOUT).json()
    except Exception:
        return None


def _norm(text):
    return re.sub(r"\s+", " ", (text or "").strip().lower())


GENERIC_CANONICAL_MAP = {
    "acetaminophen": "paracetamol",
    "paracetamol": "paracetamol",
}


def _canonical_generic_name(name):
    n = _norm(name)
    if not n:
        return ""
    return GENERIC_CANONICAL_MAP.get(n, n)


def _looks_like_url_text(value):
    v = (value or "").strip().lower()
    if not v:
        return False
    return "http://" in v or "https://" in v or "/" in v or ".com" in v or ".in" in v


def _clean_brand_from_url_like_text(brand_name, source_url, generic_name):
    """Convert URL-ish brand text into a readable brand name."""
    raw = (brand_name or "").strip()
    if not raw:
        return ""

    candidate = raw
    if _looks_like_url_text(raw):
        # Prefer deriving from canonical URL path slug.
        u = (source_url or raw).strip()
        m = re.search(r"/([^/?#]+)$", u)
        if m:
            candidate = m.group(1)
        else:
            candidate = re.sub(r"^https?://", "", u, flags=re.I)
            candidate = candidate.split("/", 1)[-1] if "/" in candidate else candidate

        candidate = re.sub(r"[-_]+", " ", candidate)
        candidate = re.sub(r"\b\d{4,}\b", "", candidate)
        candidate = re.sub(r"\s+", " ", candidate).strip()

    candidate = _extract_brand_name(candidate.title(), generic_name)
    return candidate


# ---------------------------------------------------------
# Strength + Form parser
# ---------------------------------------------------------

def parse_strength_form(text):
    strength = ""
    form = ""

    if not text:
        return strength, form

    s = re.search(r"(\d+\.?\d*\s*(?:mg|mcg|ml|g|%))", text, re.I)
    if s:
        strength = s.group(1)

    f = re.search(
        r"(tablet|capsule|cream|ointment|gel|syrup|injection|drops|powder|lotion|solution|suspension)",
        text,
        re.I,
    )
    if f:
        form = f.group(1).title()

    return strength, form


# ---------------------------------------------------------
# RxNorm query resolve
# ---------------------------------------------------------

def _get_rxcui(query):
    data = _api_get(f"https://rxnav.nlm.nih.gov/REST/rxcui.json?name={quote_plus(query)}")
    ids = (data or {}).get("idGroup", {}).get("rxnormId", [])
    if ids:
        return ids[0]

    data = _api_get(
        f"https://rxnav.nlm.nih.gov/REST/approximateTerm.json?term={quote_plus(query)}&maxEntries=5"
    )
    candidates = (data or {}).get("approximateGroup", {}).get("candidate", [])
    if candidates:
        return candidates[0].get("rxcui")
    return None


def _get_tty_name(rxcui):
    data = _api_get(f"https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/properties.json")
    props = (data or {}).get("properties", {})
    return props.get("tty", ""), props.get("name", "")


def _get_related_names(rxcui, tty):
    data = _api_get(f"https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/related.json?tty={tty}")
    names = []
    pairs = []
    for group in (data or {}).get("relatedGroup", {}).get("conceptGroup", []):
        for c in group.get("conceptProperties", []):
            name = c.get("name", "")
            cid = c.get("rxcui", "")
            if name:
                clean = name.title().strip()
                names.append(clean)
                pairs.append((cid, clean))
    return names, pairs


def _dedupe_preserve_order(values):
    seen = set()
    out = []
    for v in values:
        clean = (v or "").strip()
        k = _norm(clean)
        if not clean or not k or k in seen:
            continue
        seen.add(k)
        out.append(clean)
    return out


def _dedupe_source_urls(source_urls):
    seen = set()
    out = []
    for s in source_urls or []:
        site = (s or {}).get("site", "")
        url = (s or {}).get("url", "")
        key = (_norm(site), (url or "").strip().lower())
        if key in seen:
            continue
        seen.add(key)
        out.append({"site": site, "url": url})
    return out


def _dedupe_data_sections(items):
    seen = set()
    out = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        try:
            key = json.dumps(item, sort_keys=True, ensure_ascii=True)
        except Exception:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _json_safe(value):
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _fallback_classify_query(query):
    """Fallback classifier when RxNorm cannot resolve a query."""
    q = (query or "").strip()
    if not q:
        return "unknown", "", []

    # 1) Try OpenFDA as brand lookup.
    brand_data = _api_get(
        "https://api.fda.gov/drug/label.json?"
        f"search=openfda.brand_name:\"{quote_plus(q)}\"&limit=20"
    )
    brand_results = (brand_data or {}).get("results", [])
    if brand_results:
        generic_counts = Counter()
        related_brands = []
        for item in brand_results:
            openfda = item.get("openfda", {}) or {}
            for g in openfda.get("generic_name", []) or []:
                cg = _canonical_generic_name(g)
                if cg:
                    generic_counts[cg] += 1
            related_brands.extend([str(b).strip().title() for b in (openfda.get("brand_name", []) or [])])

        if generic_counts:
            generic = generic_counts.most_common(1)[0][0]
            brands = _dedupe_preserve_order(related_brands)
            if not brands:
                brands = [q.title()]
            return "brand", generic, brands

    # 2) Try OpenFDA as generic lookup.
    generic_data = _api_get(
        "https://api.fda.gov/drug/label.json?"
        f"search=openfda.generic_name:\"{quote_plus(q)}\"&limit=20"
    )
    generic_results = (generic_data or {}).get("results", [])
    if generic_results:
        canonical = _canonical_generic_name(q)
        brands = []
        for item in generic_results:
            openfda = item.get("openfda", {}) or {}
            brands.extend([str(b).strip().title() for b in (openfda.get("brand_name", []) or [])])
        return "generic", canonical if canonical else q.lower(), _dedupe_preserve_order(brands)

    # 3) DailyMed weak fallback: check if query appears as known drug entries.
    dailymed_data = _api_get(
        f"https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.json?drug_name={quote_plus(q)}&pagesize=25"
    )
    titles = [str(x.get("title") or "").strip() for x in (dailymed_data or {}).get("data", [])]
    if titles:
        inferred_brands = []
        for t in titles:
            m = re.search(r"\[([^\]]+)\]", t)
            if m:
                inferred_brands.append(m.group(1).strip().title())
        # If title has [BRAND], DailyMed usually indexed by generic term in drug_name query.
        return "generic", _canonical_generic_name(q) or q.lower(), _dedupe_preserve_order(inferred_brands)

    return "unknown", "", []


def resolve_query(query):
    """Returns (query_type, generic_name, brand_names)."""
    q = (query or "").strip()
    rxcui = _get_rxcui(q)
    if not rxcui:
        return _fallback_classify_query(q)

    tty, rx_name = _get_tty_name(rxcui)
    if not rx_name:
        return _fallback_classify_query(q)

    if tty in ("IN", "MIN", "PIN"):
        brands, _ = _get_related_names(rxcui, "BN")
        return "generic", rx_name.title().strip(), brands

    if tty in ("BN", "SBD", "BPCK"):
        min_names, min_pairs = _get_related_names(rxcui, "MIN")
        in_names, in_pairs = _get_related_names(rxcui, "IN")

        generic_names = min_names if min_names else in_names
        generic = " / ".join(generic_names).strip() if generic_names else ""

        ingredient_rxcui = ""
        if min_pairs:
            ingredient_rxcui = min_pairs[0][0]
        elif in_pairs:
            ingredient_rxcui = in_pairs[0][0]

        brands = []
        if ingredient_rxcui:
            brands, _ = _get_related_names(ingredient_rxcui, "BN")
        if not brands:
            brands = [rx_name.title().strip()]

        return "brand", generic, brands

    return _fallback_classify_query(q)


# ---------------------------------------------------------
# Database-backed sources only (no web search)
# ---------------------------------------------------------

def _guess_brand_from_text(text, brand_names):
    tl = _norm(text)
    for b in brand_names:
        if _norm(b) and _norm(b) in tl:
            return b

    m = re.search(r"\[([^\]]+)\]", text or "")
    if m:
        candidate = m.group(1).strip()
        if candidate:
            return candidate
    return ""


def _is_relevant_text(text, query, generic, brand_names):
    t = _norm(text)
    if not t:
        return False

    if _norm(query) and _norm(query) in t:
        return True
    if _norm(generic) and _norm(generic) in t:
        return True

    for b in brand_names:
        if _norm(b) and _norm(b) in t:
            return True
    return False


def search_rxnorm_database(query, generic, brand_names):
    rows = []
    data = _api_get(f"https://rxnav.nlm.nih.gov/REST/drugs.json?name={quote_plus(generic)}")
    groups = (data or {}).get("drugGroup", {}).get("conceptGroup", [])

    for group in groups:
        tty = group.get("tty", "")
        if tty not in ("BN", "SBD", "SCD", "BPCK", "GPCK"):
            continue

        for c in group.get("conceptProperties", []):
            name = (c.get("name") or "").strip()
            allow_direct_brand_tty = tty in ("BN", "SBD", "BPCK")
            if not allow_direct_brand_tty and not _is_relevant_text(name, query, generic, brand_names):
                continue

            brand = _guess_brand_from_text(name, brand_names)
            if not brand:
                # RxNorm BN rows are explicit brand names.
                if tty == "BN":
                    brand = name
                # SBD often starts with brand token before strength/form text.
                elif tty == "SBD":
                    brand = (name.split(" ")[0] if name else "").strip()
            strength, form = parse_strength_form(name)

            rows.append({
                "brand_name": brand if brand else _extract_brand_name(name, generic),
                "generic_name": generic,
                # "salt": "",
                "strength": strength,
                "form": form,
                "source_site": "rxnav.nlm.nih.gov",
                "source_url": "https://rxnav.nlm.nih.gov/",
            })

    return rows


def search_dailymed_database(query, generic, brand_names):
    rows = []
    data = _api_get(
        f"https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.json?drug_name={quote_plus(generic)}&pagesize=100"
    )

    for item in (data or {}).get("data", []):
        title = (item.get("title") or "").strip()
        if not _is_relevant_text(title, query, generic, brand_names):
            continue

        brand = _guess_brand_from_text(title, brand_names)
        strength, form = parse_strength_form(title)

        rows.append({
            "brand_name": brand if brand else _extract_brand_name(title, generic),
            "generic_name": generic,
            "salt": "",
            "strength": strength,
            "form": form,
            "source_site": "dailymed.nlm.nih.gov",
            "source_url": f"https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid={item.get('setid')}",
        })

    return rows


def search_openfda_database(query, generic, brand_names):
    rows = []
    # OpenFDA label endpoint is structured and avoids noisy web pages.
    url = (
        "https://api.fda.gov/drug/label.json?"
        f"search=openfda.generic_name:\"{quote_plus(generic)}\"&limit=100"
    )
    data = _api_get(url)

    for item in (data or {}).get("results", []):
        openfda = item.get("openfda", {}) or {}
        brand_list = openfda.get("brand_name", []) or []
        generic_list = openfda.get("generic_name", []) or []
        product_list = openfda.get("product_type", []) or []

        generic_name = " / ".join([str(x).strip() for x in generic_list if str(x).strip()])
        if not generic_name:
            generic_name = generic
        generic_name = _canonical_generic_name(generic_name)

        candidate_brand = ""
        for b in brand_list:
            bs = str(b).strip()
            if not bs:
                continue
            if _is_relevant_text(bs, query, generic_name, brand_names):
                candidate_brand = bs
                break
        if not candidate_brand and brand_list:
            candidate_brand = str(brand_list[0]).strip()

        drug_name = candidate_brand or generic_name
        strength, form = parse_strength_form(
            " ".join([
                drug_name,
                " ".join([str(x) for x in (item.get("dosage_and_administration") or [])[:1]]),
                " ".join([str(x) for x in product_list]),
            ])
        )

        set_id = str(item.get("set_id") or "").strip()
        source_url = f"https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid={set_id}" if set_id else ""

        rows.append({
            # "drug_name": drug_name,
            "brand_name": candidate_brand,
            "generic_name": generic_name,
            "salt": generic_name,
            "strength": strength,
            "form": form,
            "source_site": "openfda.gov",
            "source_url": source_url,
        })

    return rows


def _is_indian_source(source_site):
    site = _norm(source_site)
    if not site:
        return False
    return any(h in site for h in INDIAN_SOURCE_HINTS)


def search_local_indian_database(query, generic, brand_names):
    rows = []
    try:
        conn = sqlite3.connect(LOCAL_DB_PATH)
        cur = conn.cursor()

        terms = [query, generic] + list(brand_names or [])
        terms = [t for t in terms if (t or "").strip()]
        if not terms:
            conn.close()
            return rows

        like_terms = []
        for t in terms[:6]:
            like_terms.append(f"%{_norm(t)}%")

        sql = """
        SELECT drug_name, brand_name, generic_name, strength, form, source_site, source_url,
               COALESCE(composition, '') AS composition
        FROM drug_variants
        WHERE source_site IS NOT NULL
          AND (
            LOWER(source_site) LIKE '%pharmeasy%'
            OR LOWER(source_site) LIKE '%1mg%'
            OR LOWER(source_site) LIKE '%netmeds%'
            OR LOWER(source_site) LIKE '%apollo%'
            OR LOWER(source_site) LIKE '%medplus%'
            OR LOWER(source_site) LIKE '%truemeds%'
            OR LOWER(source_site) LIKE '%medindia%'
            OR LOWER(source_site) LIKE '%practo%'
            OR LOWER(source_site) LIKE '%.in%'
            OR LOWER(source_site) LIKE '%india%'
          )
          AND (
            LOWER(COALESCE(drug_name, '')) LIKE ?
            OR LOWER(COALESCE(brand_name, '')) LIKE ?
            OR LOWER(COALESCE(generic_name, '')) LIKE ?
            OR LOWER(COALESCE(composition, '')) LIKE ?
          )
        ORDER BY id DESC
        LIMIT 500
        """

        # Strong first term + fallback terms.
        first = like_terms[0]
        cur.execute(sql, (first, first, first, first))
        fetched = cur.fetchall()

        if len(fetched) < 120 and len(like_terms) > 1:
            seen = set((r[0], r[1], r[5], r[6]) for r in fetched)
            for lt in like_terms[1:]:
                cur.execute(sql, (lt, lt, lt, lt))
                for r in cur.fetchall():
                    key = (r[0], r[1], r[5], r[6])
                    if key not in seen:
                        seen.add(key)
                        fetched.append(r)

        conn.close()

        for drug_name, brand_name, generic_name, strength, form, source_site, source_url, composition in fetched:
            normalized_generic = _canonical_generic_name(generic_name or generic)
            normalized_salt = _canonical_generic_name(composition or normalized_generic)
            rows.append({
                "brand_name": (brand_name or "").strip(),
                "generic_name": normalized_generic,
                "salt": normalized_salt,
                "strength": (strength or "").strip(),
                "form": (form or "").strip(),
                "source_site": (source_site or "").strip(),
                "source_url": (source_url or "").strip(),
            })
    except Exception:
        return []

    return rows


def search_mongo_database(query, generic, brand_names):
    """Optional Mongo source from extracted spider data."""
    try:
        from pymongo import MongoClient
    except Exception:
        return []

    rows = []
    terms = [query, generic] + list(brand_names or [])
    terms = [t for t in terms if (t or "").strip()]
    if not terms:
        return rows

    term_pattern = "|".join(re.escape(t) for t in terms[:6])
    if not term_pattern:
        return rows

    normalized_terms = [_norm(t) for t in terms if _norm(t)]

    def _doc_text_blob(doc):
        data = doc.get("data") if isinstance(doc.get("data"), dict) else {}
        parts = [
            doc.get("brand_name"),
            doc.get("drug_name"),
            doc.get("generic_name"),
            doc.get("salt"),
            doc.get("title"),
            doc.get("url"),
            doc.get("domain"),
            doc.get("site_domain"),
            data.get("brand_name"),
            data.get("drug_name"),
            data.get("generic_name"),
            data.get("salt"),
            data.get("title"),
            data.get("url"),
            data.get("drug_url"),
            data.get("description"),
            data.get("ingredients"),
            data.get("full_content"),
        ]
        if data:
            try:
                parts.append(json.dumps(data, ensure_ascii=True, default=str))
            except Exception:
                pass
        return _norm(" ".join(str(p) for p in parts if p))

    query_filter = {
        "$or": [
            {"brand_name": {"$regex": term_pattern, "$options": "i"}},
            {"drug_name": {"$regex": term_pattern, "$options": "i"}},
            {"generic_name": {"$regex": term_pattern, "$options": "i"}},
            {"salt": {"$regex": term_pattern, "$options": "i"}},
            {"title": {"$regex": term_pattern, "$options": "i"}},
            {"data.brand_name": {"$regex": term_pattern, "$options": "i"}},
            {"data.drug_name": {"$regex": term_pattern, "$options": "i"}},
            {"data.generic_name": {"$regex": term_pattern, "$options": "i"}},
            {"data.salt": {"$regex": term_pattern, "$options": "i"}},
            {"data.title": {"$regex": term_pattern, "$options": "i"}},
            {"data.description": {"$regex": term_pattern, "$options": "i"}},
            {"data.ingredients": {"$regex": term_pattern, "$options": "i"}},
            {"data.full_content": {"$regex": term_pattern, "$options": "i"}},
        ]
    }

    client = None
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=4000)
        collection = client[MONGO_DATABASE][MONGO_COLLECTION]
        # First, try indexed/targeted matching on common fields.
        matched_docs = list(collection.find(query_filter).sort("_id", -1).limit(1000))

        # Fallback: if nothing found, scan recent docs and match keyword in full stored content.
        if not matched_docs:
            scan_cursor = collection.find({}).sort("_id", -1).limit(3000)
            for doc in scan_cursor:
                blob = _doc_text_blob(doc)
                if any(t in blob for t in normalized_terms):
                    matched_docs.append(doc)

        for doc in matched_docs:
            data = doc.get("data") if isinstance(doc.get("data"), dict) else {}
            brand_name = (
                doc.get("brand_name")
                or doc.get("drug_name")
                or data.get("brand_name")
                or data.get("drug_name")
                or data.get("title")
                or doc.get("title")
                or ""
            )
            brand_name = str(brand_name).strip()
            if not brand_name:
                continue

            generic_name = (
                doc.get("generic_name")
                or data.get("generic_name")
                or data.get("salt")
                or doc.get("salt")
                or generic
            )

            strength = str(doc.get("strength") or data.get("strength") or "").strip()
            form = str(doc.get("form") or data.get("form") or "").strip()
            source_site = str(doc.get("site_domain") or doc.get("domain") or "mongodb").strip()
            source_url = str(doc.get("url") or data.get("url") or "").strip()
            if not source_url:
                source_url = str(data.get("drug_url") or "").strip()

            rows.append({
                "brand_name": brand_name,
                "generic_name": _canonical_generic_name(generic_name),
                "salt": _canonical_generic_name(doc.get("salt") or data.get("salt") or generic_name),
                "strength": strength,
                "form": form,
                "source_site": source_site,
                "source_url": source_url,
                "data_section": _json_safe(data),
            })
    except Exception:
        return []
    finally:
        if client:
            try:
                client.close()
            except Exception:
                pass

    return rows


def _is_complete(row):
    if not (row.get("generic_name") or "").strip():
        return False
    if not (row.get("brand_name") or "").strip():
        return False
    source_site = (row.get("source_site") or "").strip()
    source_url = (row.get("source_url") or "").strip()
    if not source_site and row.get("source_urls"):
        source_site = str((row.get("source_urls")[0] or {}).get("site") or "").strip()
    if not source_url and row.get("source_urls"):
        source_url = str((row.get("source_urls")[0] or {}).get("url") or "").strip()
    if not source_site:
        return False
    if not source_url:
        return False
    return True


_NOISY_RE = re.compile(
    r"pricing for \d|top \d+|at best|best price|substitute|drugprice|"
    r"tradeindia|indiamart|cinerea|list of \d|brands in|city of|city-\d|"
    r"in chandigarh|in mumbai|in delhi|in bangalore|in bengaluru|in hyderabad",
    re.I,
)


def _is_clean_variant_name(name):
    n = _norm(name)
    if not n:
        return False
    noisy_markers = (
        " uses",
        " side effects",
        " price",
        " buy ",
        " online",
        " compared",
        " news",
        "highest-selling",
        "how to",
        "dosage and safety",
    )
    if any(m in n for m in noisy_markers):
        return False
    return not _NOISY_RE.search(n)


_STRENGTH_SUFFIX_RE = re.compile(r"\s+\d+\.?\d*\s*(?:mg|mcg|ml|g|%|iu)\b.*$", re.I)
_FORM_SUFFIX_RE = re.compile(
    r"\s+(?:tablet|capsule|injection|syrup|cream|gel|drops|powder|lotion|"
    r"solution|suspension|ointment|spray|inhaler|strip|pack|kit|"
    r"pr|sr|dsr|er|xr|cr|od|bd|tds|iv|im|sc)[s]?\b.*$",
    re.I,
)


def _extract_brand_name(product_name, generic_name):
    """Strip strength and dosage-form suffix to get a clean brand name."""
    cleaned = (product_name or "").strip()
    cleaned = _STRENGTH_SUFFIX_RE.sub("", cleaned).strip()
    cleaned = _FORM_SUFFIX_RE.sub("", cleaned).strip()
    if not cleaned:
        return ""
    if _norm(cleaned) == _norm(generic_name or ""):
        return ""
    return cleaned


def _score_row(row, query, generic):
    b = _norm(row.get("brand_name"))
    q = _norm(query)
    g = _norm(generic)

    score = 0
    if b == q or b == g:
        score += 100
    elif b.startswith(q) or b.startswith(g):
        score += 70

    if row.get("strength"):
        score += 10
    if row.get("form"):
        score += 5
    source_site = row.get("source_site", "")
    if not source_site and row.get("source_urls"):
        source_site = (row.get("source_urls")[0] or {}).get("site", "")
    if _is_indian_source(source_site):
        score += 120

    return score


def search_variants_from_databases(query):
    q = (query or "").strip()
    qtype = "unknown"
    generic = _canonical_generic_name(q)
    brand_names = []

    # Mongo-only mode: return variants already stored in MongoDB.
    raw = search_mongo_database(q, generic, brand_names)

    all_data_sections = []
    for row in raw:
        data_section = row.get("data_section")
        if isinstance(data_section, dict) and data_section:
            all_data_sections.append(data_section)
    all_data_sections = _dedupe_data_sections(all_data_sections)

    merged = {}
    for row in raw:
        key = f"{_norm(row.get('brand_name'))}|{_norm(row.get('generic_name'))}"
        if key == "|":
            continue

        source = {
            "site": row.get("source_site", ""),
            "url": row.get("source_url", ""),
        }

        if key in merged:
            existing = merged[key]
            known_sources = {(_norm(s.get("site", "")), (s.get("url", "") or "").strip().lower()) for s in existing["source_urls"]}
            src_key = (_norm(source.get("site", "")), (source.get("url", "") or "").strip().lower())
            if source["site"] and src_key not in known_sources:
                existing["source_urls"].append(source)

            for field in ("strength", "form", "salt"):
                if not existing.get(field) and row.get(field):
                    existing[field] = row[field]

            data_section = row.get("data_section")
            if isinstance(data_section, dict) and data_section:
                existing["data_sections"].append(data_section)
        else:
            merged[key] = {
                "brand_name": row.get("brand_name", "").strip(),
                "generic_name": row.get("generic_name", "").strip(),
                "salt": row.get("salt", "").strip(),
                "strength": row.get("strength", "").strip(),
                "form": row.get("form", "").strip(),
                "source_site": source.get("site", ""),
                "source_url": source.get("url", ""),
                "source_urls": [source] if (source.get("site") or source.get("url")) else [],
                "data_sections": [row.get("data_section")] if isinstance(row.get("data_section"), dict) and row.get("data_section") else [],
            }

    all_rows = list(merged.values())

    # Ensure brand_name is clean (strip trailing strength/form suffixes).
    for row in all_rows:
        brand_nm = (row.get("brand_name") or "").strip()
        row["generic_name"] = _canonical_generic_name(row.get("generic_name", ""))
        row["salt"] = _canonical_generic_name(row.get("salt", "")) or row.get("generic_name", "")
        row["source_urls"] = _dedupe_source_urls(row.get("source_urls", []))
        row["data_sections"] = _dedupe_data_sections(row.get("data_sections", []))

        if brand_nm:
            cleaned = _clean_brand_from_url_like_text(
                brand_nm,
                row.get("source_url", ""),
                row.get("generic_name", ""),
            )
            if cleaned:
                row["brand_name"] = cleaned

        # Avoid false positive "1mg" strength originating from 1mg.com slug/domain.
        if _norm(row.get("strength")) == "1mg" and "1mg" in _norm(row.get("source_site")):
            row["strength"] = ""

    filtered = [
        r for r in all_rows
        if _is_complete(r)
        and _is_clean_variant_name(r.get("brand_name", ""))
        and _norm(r.get("brand_name", "")) != _norm(r.get("generic_name", ""))
    ]
    removed_count = len(all_rows) - len(filtered)

    filtered.sort(key=lambda r: (-_score_row(r, q, generic), _norm(r.get("brand_name"))))

    indian_only = [r for r in filtered if _is_indian_source(r.get("source_site", ""))]
    final_rows = indian_only if indian_only else filtered

    if final_rows:
        # Infer generic from stored rows for brand-first queries.
        qn = _norm(q)
        exact_or_prefix = [
            r for r in final_rows
            if _norm(r.get("brand_name", "")) == qn or _norm(r.get("brand_name", "")).startswith(qn)
        ]
        candidate_rows = exact_or_prefix if exact_or_prefix else final_rows
        generic_counts = Counter(
            _norm(r.get("generic_name", "")) for r in candidate_rows if _norm(r.get("generic_name", ""))
        )
        if generic_counts:
            generic = generic_counts.most_common(1)[0][0]
            qtype = "brand"
        else:
            qtype = "generic"

    # Output shape: only source_urls list to avoid repeating source fields multiple times.
    response_rows = []
    for row in final_rows[:50]:
        response_rows.append({
            "brand_name": row.get("brand_name", ""),
            "generic_name": row.get("generic_name", ""),
            "salt": row.get("salt", "") or row.get("generic_name", ""),
            "strength": row.get("strength", ""),
            "form": row.get("form", ""),
            "source_urls": _dedupe_source_urls(row.get("source_urls", [])),
            "data_sections": row.get("data_sections", []),
        })

    return qtype, generic, response_rows, removed_count, all_data_sections


# ---------------------------------------------------------
# API
# ---------------------------------------------------------

@app.route("/api/search-database")
def api_search_database():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "missing query. Use /api/search-database?q=pantoprazole"}), 400

    _, _, _, _, all_data_sections = search_variants_from_databases(q)

    return jsonify({
        "query": q,
        "total_found": len(all_data_sections),
        "data": all_data_sections,
    })


@app.route("/")
def home():
    return "<h2>Drug Database Search API</h2><p>Use /api/search-database?q=pantoprazole</p>"


if __name__ == "__main__":
    print("Server running at http://localhost:5001")
    print("API: http://localhost:5001/api/search-database?q=pantoprazole")
    app.run(port=5001, debug=True)
