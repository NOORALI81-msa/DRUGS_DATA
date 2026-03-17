import re
import requests
import concurrent.futures
from urllib.parse import quote_plus, urlparse
from flask import Flask, request, jsonify
from ddgs import DDGS

app = Flask(__name__)

TIMEOUT = 10
SOURCE_WORKERS = 6
WEB_QUERY_WORKERS = 6
DDGS_MAX_RESULTS_PER_QUERY = 5


def _api_get(url):
    try:
        return requests.get(url, timeout=TIMEOUT).json()
    except Exception:
        return None


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
# RxNorm Generic + Brand handling
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
    if cands:
        return cands[0].get("rxcui")
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
                names.append(name.title())
                pairs.append((cid, name.title()))
    return names, pairs


def resolve_query(query):
    """Returns (query_type, generic_name, brand_names).

    Fix: if query is already ingredient (IN/MIN/PIN), keep it as generic and do not
    expand through related IN+MIN from combination products.
    """
    q = query.strip()
    rxcui = _get_rxcui(q)
    if not rxcui:
        return "unknown", q.title(), []

    tty, rx_name = _get_tty_name(rxcui)
    if not rx_name:
        rx_name = q

    # Already generic ingredient
    if tty in ("IN", "MIN", "PIN"):
        brands, _ = _get_related_names(rxcui, "BN")
        return "generic", rx_name.title(), brands

    # Brand-like terms
    if tty in ("BN", "SBD", "BPCK"):
        min_names, min_pairs = _get_related_names(rxcui, "MIN")
        in_names, in_pairs = _get_related_names(rxcui, "IN")
        generic_names = min_names if min_names else in_names
        generic = " / ".join(generic_names) if generic_names else rx_name.title()

        ingredient_rxcui = ""
        if min_pairs:
            ingredient_rxcui = min_pairs[0][0]
        elif in_pairs:
            ingredient_rxcui = in_pairs[0][0]

        brands = []
        if ingredient_rxcui:
            brands, _ = _get_related_names(ingredient_rxcui, "BN")
        if not brands:
            brands = [rx_name.title()]

        return "brand", generic, brands

    return "unknown", rx_name.title(), []


# ---------------------------------------------------------
# Search Sources
# ---------------------------------------------------------

def _is_relevant_title(title, generic, brand_names):
    t = (title or "").lower()
    if not t:
        return False
    if _contains_term(t, generic):
        return True
    for b in brand_names:
        if _contains_term(t, b):
            return True
    return False


def _guess_brand_from_title(title, brand_names):
    tl = (title or "").lower()
    for b in brand_names:
        if _contains_term(tl, b):
            return b
    m = re.search(r"\[([^\]]+)\]", title or "")
    if m:
        candidate = m.group(1).strip()
        if candidate:
            return candidate
    return ""


def _contains_term(text, term):
    if not text or not term:
        return False
    escaped = re.escape((term or "").strip().lower())
    if not escaped:
        return False
    # Word-safe matching avoids false hits like brand "Ibu" inside "Ibuprofen".
    return bool(re.search(rf"(?<![a-z0-9]){escaped}(?![a-z0-9])", text.lower()))


def search_rxnorm(generic, brand_names):
    results = []
    data = _api_get(f"https://rxnav.nlm.nih.gov/REST/drugs.json?name={quote_plus(generic)}")
    groups = (data or {}).get("drugGroup", {}).get("conceptGroup", [])
    for g in groups:
        g_tty = g.get("tty", "")
        if g_tty not in ("BN", "SBD", "SCD"):
            continue
        for c in g.get("conceptProperties", []):
            name = c.get("name", "")
            if not _is_relevant_title(name, generic, brand_names):
                continue
            strength, form = parse_strength_form(name)
            results.append({
                "drug_name": name,
                "brand_name": _guess_brand_from_title(name, brand_names),
                "strength": strength,
                "form": form,
                "source_site": "rxnav.nlm.nih.gov",
                "source_url": "https://rxnav.nlm.nih.gov/",
            })
    return results


def search_dailymed(generic, brand_names):
    results = []
    data = _api_get(
        f"https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.json?drug_name={quote_plus(generic)}&pagesize=40"
    )
    for item in (data or {}).get("data", []):
        title = item.get("title", "")
        if not _is_relevant_title(title, generic, brand_names):
            continue
        strength, form = parse_strength_form(title)
        results.append({
            "drug_name": title,
            "brand_name": _guess_brand_from_title(title, brand_names),
            "strength": strength,
            "form": form,
            "source_site": "dailymed.nlm.nih.gov",
            "source_url": f"https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid={item.get('setid')}",
        })
    return results


JUNK_WORDS = [
    "news", "blog", "story", "covid", "portfolio", "research", "article",
    "linkedin", "wiki", "randomized", "peptic ulcer",
]

NON_PRODUCT_TITLE_PATTERNS = [
    r"\buses?\b",
    r"\bside effects?\b",
    r"\bfaqs?\b",
    r"\bwhat(?:\'s| is)\b",
    r"\bnews digest\b",
    r"\bbrands? in india\b",
    r"\btop\s+\d+\b",
    r"\bpricing for\b",
]

NON_PRODUCT_URL_HINTS = [
    "/articles/", "/article/", "/blog/", "/news/", "/medical-answers/", "/molecules/", "/salt/"
]

INDIAN_DOMAIN_HINTS = [
    "1mg.com",
    "pharmeasy.in",
    "netmeds.com",
    "apollopharmacy.in",
    "medplusmart.com",
    "truemeds.in",
    "medindia.net",
    "practo.com",
    "pharmacyindia.com",
    "medlife.com",
    "medibuddy.in"
]

TRUSTED_GLOBAL_DOMAIN_HINTS = [
    "drugs.com",
    "webmd.com",
    "goodrx.com",
    "rxlist.com",
    "medscape.com",
    "mayoclinic.org",
    "drugbank.com",
]

BLOCKED_DOMAINS = {
    "1colony.com", "20fr.com", "ponfish.com", "brunodubner.com", "eyedock.com",
    "refreshinghomes.co.uk", "johnny-brady.com", "srp.aero", "pjmhsonline.com",
}

MEDICAL_HINTS = {
    "drug", "medicine", "tablet", "capsule", "dosage", "uses", "side effects",
    "price", "generic", "brand", "hcl", "er",
}


def valid_web(title, generic, brand_names, body="", site=""):
    t = (title or "").lower()
    b = (body or "").lower()
    s = (site or "").lower()
    if not t:
        return False
    if any(s.endswith(d) for d in BLOCKED_DOMAINS):
        return False
    for j in JUNK_WORDS:
        if j in t:
            return False
    if any(re.search(p, t, re.I) for p in NON_PRODUCT_TITLE_PATTERNS):
        return False
    if any(h in b for h in (" side effects", " uses", " faq", " dosage")):
        return False
    if not _is_relevant_title(title, generic, brand_names):
        return False
    if not any(h in t or h in b for h in MEDICAL_HINTS):
        return False
    return True


def is_indian_domain(site):
    s = (site or "").lower()
    return s.endswith(".in") or ".co.in" in s or s in INDIAN_DOMAIN_HINTS


def _ddgs_text_query(query_text, max_results):
    """Run a single DDGS query in its own worker for safe parallel fan-out."""
    try:
        with DDGS() as ddgs:
            return list(ddgs.text(query_text, max_results=max_results))
    except Exception:
        return []


def search_web(generic, brand_names):
    results = []
    seen_urls = set()

    # Focused queries: Indian pharmacy sites first, then one global fallback.
    queries = [f"{generic} brand name tablet capsule india"]
    for domain in INDIAN_DOMAIN_HINTS[:5]:
        queries.append(f"site:{domain} {generic}")
    queries.append(f"site:drugs.com {generic} brand")
    hits = []
    max_workers = min(WEB_QUERY_WORKERS, len(queries)) if queries else 1
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(_ddgs_text_query, q, DDGS_MAX_RESULTS_PER_QUERY)
            for q in queries
        ]
        for f in concurrent.futures.as_completed(futures):
            try:
                hits.extend(f.result())
            except Exception:
                pass

    for h in hits:
        title = h.get("title", "")
        url = h.get("href", "")
        body = h.get("body", "")
        if not url or url in seen_urls:
            continue
        ul = url.lower()
        if any(hint in ul for hint in NON_PRODUCT_URL_HINTS):
            continue
        site = urlparse(url).netloc.replace("www.", "")
        if not valid_web(title, generic, brand_names, body=body, site=site):
            continue
        seen_urls.add(url)
        strength, form = parse_strength_form(f"{title} {body}")
        results.append({
            "drug_name": title.split(" - ")[0].split(" | ")[0].strip(),
            "brand_name": _guess_brand_from_title(title, brand_names),
            "strength": strength,
            "form": form,
            "source_site": site,
            "source_url": url,
        })

    results.sort(key=lambda r: (0 if is_indian_domain(r.get("source_site", "")) else 1, r.get("drug_name", "").lower()))
    return results


def _enrich_missing_fields(rows):
    """Fill missing brand/strength/form using same generic+strength+form constraints."""
    if not rows:
        return rows

    def norm(v):
        return re.sub(r"\s+", " ", (v or "").strip().lower())

    brand_by_key = {}
    for r in rows:
        b = (r.get("brand_name") or "").strip()
        if not b:
            continue
        key = (norm(r.get("strength")), norm(r.get("form")))
        brand_by_key.setdefault(key, set()).add(b)

    sf_by_brand = {}
    for r in rows:
        b = (r.get("brand_name") or "").strip()
        if not b:
            continue
        sf_by_brand.setdefault(norm(b), {"strength": {}, "form": {}})
        s = (r.get("strength") or "").strip()
        f = (r.get("form") or "").strip()
        if s:
            sf_by_brand[norm(b)]["strength"][s] = sf_by_brand[norm(b)]["strength"].get(s, 0) + 1
        if f:
            sf_by_brand[norm(b)]["form"][f] = sf_by_brand[norm(b)]["form"].get(f, 0) + 1

    for r in rows:
        key = (norm(r.get("strength")), norm(r.get("form")))

        if not (r.get("brand_name") or "").strip() and key in brand_by_key and len(brand_by_key[key]) == 1:
            r["brand_name"] = list(brand_by_key[key])[0]

        bnorm = norm(r.get("brand_name"))
        if bnorm and bnorm in sf_by_brand:
            if not (r.get("strength") or "").strip() and sf_by_brand[bnorm]["strength"]:
                r["strength"] = max(sf_by_brand[bnorm]["strength"], key=sf_by_brand[bnorm]["strength"].get)
            if not (r.get("form") or "").strip() and sf_by_brand[bnorm]["form"]:
                r["form"] = max(sf_by_brand[bnorm]["form"], key=sf_by_brand[bnorm]["form"].get)

    return rows


# ---------------------------------------------------------
# Search + Merge
# ---------------------------------------------------------

def search_variants(query):
    qtype, generic, brand_names = resolve_query(query)

    raw = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=SOURCE_WORKERS) as executor:
        futures = [
            executor.submit(search_rxnorm, generic, brand_names),
            executor.submit(search_dailymed, generic, brand_names),
            executor.submit(search_web, generic, brand_names),
        ]
        for f in concurrent.futures.as_completed(futures):
            try:
                raw.extend(f.result())
            except Exception:
                pass

    merged = {}
    for r in raw:
        key = re.sub(r"\s+", " ", r.get("drug_name", "").lower().strip())
        if not key:
            continue
        source = {"site": r.get("source_site", ""), "url": r.get("source_url", "")}

        if key in merged:
            existing = merged[key]
            existing_sites = {s["site"] for s in existing["source_urls"]}
            if source["site"] and source["site"] not in existing_sites:
                existing["source_urls"].append(source)
            for field in ["brand_name", "strength", "form"]:
                if not existing.get(field) and r.get(field):
                    existing[field] = r[field]
        else:
            merged[key] = {
                "drug_name": r.get("drug_name", ""),
                "brand_name": r.get("brand_name", ""),
                "generic_name": generic,
                "strength": r.get("strength", ""),
                "form": r.get("form", ""),
                "source_urls": [source] if source["site"] else [],
            }

    clean = list(merged.values())
    ql = query.lower().strip()
    gl = generic.lower().strip()

    def has_indian_source(row):
        for s in row.get("source_urls", []):
            if is_indian_domain(s.get("site", "")):
                return True
        return False

    clean.sort(key=lambda r: (
        0 if has_indian_source(r) else 1,
        0 if r["drug_name"].lower().strip() in (ql, gl) else
        1 if r["drug_name"].lower().strip().startswith(ql) or r["drug_name"].lower().strip().startswith(gl) else 2,
        r["drug_name"].lower(),
    ))

    clean = _enrich_missing_fields(clean)
    return qtype, generic, clean


# ---------------------------------------------------------
# API
# ---------------------------------------------------------

@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "missing query. Use /api/search?q=pantoprazole"}), 400

    qtype, generic, results = search_variants(q)

    return jsonify({
        "query": q,
        "query_type": qtype,
        "generic_name": generic,
        "total_variants": len(results),
        "results": results,
    })


@app.route("/")
def home():
    return "<h2>Drug Variant Search API</h2><p>Use /api/search?q=pantoprazole</p>"


if __name__ == "__main__":
    print("Server running at http://localhost:5000")
    print("API: http://localhost:5000/api/search?q=")
    app.run(port=5000, debug=True)
