# """
# Drug Search API - Standalone Flask API
# Searches public drug databases for drug variants, generic & brand names.

# Sources:
#   - RxNorm (NIH) : Brand names, formulations, strengths, related drugs
#   - OpenFDA      : Manufacturers, brand/generic names, routes, dosage forms
#   - DailyMed     : FDA drug labels, all marketed products
#   - MedlinePlus  : Patient drug information pages
#   - 1mg.com      : Indian brand variants, prices, compositions
#   - PharmEasy    : Indian pharmacy products, molecule names
#   - Web Search   : Search the entire web (like Google/Bing) for drug info

# Usage:
#     python drug_search_api.py
#     Then open: http://localhost:5000

# Endpoints:
#     GET  /api/search?q=pantoprazole   -> Search drug variants
#     GET  /api/history                 -> View past searches
#     GET  /api/saved                   -> View saved results
#     POST /api/save                    -> Save a search result
#     GET  /api/sites                   -> List available sources
#     GET  /                            -> Web search UI
# """

# import json
# import re
# import sqlite3
# from datetime import datetime
# from pathlib import Path
# from urllib.parse import quote_plus

# import requests
# from ddgs import DDGS
# from flask import Flask, jsonify, request, render_template_string

# # ============================================================
# # CONFIG
# # ============================================================
# DB_PATH = Path(__file__).parent / "drug_search.db"
# RESULTS_DIR = Path(__file__).parent / "drug_searches"
# RESULTS_DIR.mkdir(exist_ok=True)

# HEADERS = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
#                   "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
#     "Accept": "application/json,text/html,*/*;q=0.8",
# }

# REQUEST_TIMEOUT = 15


# # ============================================================
# # DATABASE (SQLite - local, zero config)
# # ============================================================

# def init_db():
#     conn = sqlite3.connect(str(DB_PATH))
#     conn.execute("""
#         CREATE TABLE IF NOT EXISTS search_history (
#             id INTEGER PRIMARY KEY AUTOINCREMENT,
#             query TEXT NOT NULL,
#             searched_at TEXT NOT NULL,
#             total_results INTEGER DEFAULT 0
#         )
#     """)
#     conn.execute("""
#         CREATE TABLE IF NOT EXISTS drug_variants (
#             id INTEGER PRIMARY KEY AUTOINCREMENT,
#             search_id INTEGER,
#             drug_name TEXT NOT NULL,
#             brand_name TEXT,
#             generic_name TEXT,
#             manufacturer TEXT,
#             composition TEXT,
#             strength TEXT,
#             form TEXT,
#             price TEXT,
#             source_site TEXT,
#             source_url TEXT,
#             scraped_at TEXT,
#             FOREIGN KEY (search_id) REFERENCES search_history(id)
#         )
#     """)
#     conn.execute("""
#         CREATE TABLE IF NOT EXISTS saved_searches (
#             id INTEGER PRIMARY KEY AUTOINCREMENT,
#             query TEXT NOT NULL,
#             results_json TEXT NOT NULL,
#             saved_at TEXT NOT NULL
#         )
#     """)
#     conn.commit()
#     conn.close()


# def db_save_search(query, results):
#     conn = sqlite3.connect(str(DB_PATH))
#     cur = conn.cursor()
#     cur.execute(
#         "INSERT INTO search_history (query, searched_at, total_results) VALUES (?, ?, ?)",
#         (query, datetime.now().isoformat(), len(results)),
#     )
#     search_id = cur.lastrowid
#     for r in results:
#         source_urls = r.get("source_urls", [])
#         source_sites = ", ".join(s["site"] for s in source_urls)
#         source_url_str = " | ".join(s["url"] for s in source_urls if s.get("url"))
#         cur.execute(
#             """INSERT INTO drug_variants
#             (search_id, drug_name, brand_name, generic_name, manufacturer,
#              composition, strength, form, price, source_site, source_url, scraped_at)
#             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
#             (
#                 search_id, r.get("drug_name", ""), r.get("brand_name", ""),
#                 r.get("generic_name", ""), r.get("manufacturer", ""),
#                 r.get("composition", ""), r.get("strength", ""),
#                 r.get("form", ""), r.get("price", ""),
#                 source_sites, source_url_str,
#                 datetime.now().isoformat(),
#             ),
#         )
#     conn.commit()
#     conn.close()
#     return search_id


# def get_history(limit=50):
#     conn = sqlite3.connect(str(DB_PATH))
#     conn.row_factory = sqlite3.Row
#     rows = conn.execute(
#         "SELECT * FROM search_history ORDER BY id DESC LIMIT ?", (limit,)
#     ).fetchall()
#     conn.close()
#     return [dict(r) for r in rows]


# def get_saved():
#     conn = sqlite3.connect(str(DB_PATH))
#     conn.row_factory = sqlite3.Row
#     rows = conn.execute(
#         "SELECT * FROM saved_searches ORDER BY id DESC"
#     ).fetchall()
#     conn.close()
#     return [dict(r) for r in rows]


# # ============================================================
# # DATA SOURCES - Public APIs
# # ============================================================

# def _api_get(url, as_json=True):
#     """Safe HTTP GET with timeout"""
#     try:
#         resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
#         resp.raise_for_status()
#         return resp.json() if as_json else resp.text
#     except Exception:
#         return None


# def _parse_strength_form(name):
#     """Extract strength and dosage form from a drug name string"""
#     strength = ""
#     form = ""
#     m = re.search(r'(\d+\.?\d*\s*(?:mg|mcg|ml|g|iu|%|MG/ML)\b)', name, re.IGNORECASE)
#     if m:
#         strength = m.group(1).strip()
#     m = re.search(
#         r'\b(tablet|capsule|syrup|injection|cream|gel|drops|suspension|'
#         r'ointment|solution|powder|inhaler|granule|oral|intravenous|'
#         r'delayed.release|extended.release|topical|suppository)\b',
#         name, re.IGNORECASE,
#     )
#     if m:
#         form = m.group(1).strip().title()
#     return strength, form


# def _resolve_generic_name(query):
#     """Use RxNorm to resolve a drug query to its actual generic ingredient name.
#     e.g. 'augmentin' -> 'Amoxicillin / Clavulanate'
#          'pantoprazole' -> 'Pantoprazole'
#     Returns (generic_name, is_brand) tuple.
#     """
#     q = query.strip()
#     # Step 1: Get RXCUI
#     data = _api_get(
#         f"https://rxnav.nlm.nih.gov/REST/rxcui.json?name={quote_plus(q)}"
#     )
#     rxcui = None
#     if data:
#         ids = data.get("idGroup", {}).get("rxnormId", [])
#         if ids:
#             rxcui = ids[0]

#     if not rxcui:
#         # Try approximate match
#         data = _api_get(
#             f"https://rxnav.nlm.nih.gov/REST/approximateTerm.json?term={quote_plus(q)}&maxEntries=1"
#         )
#         if data:
#             candidates = data.get("approximateGroup", {}).get("candidate", [])
#             if candidates:
#                 rxcui = candidates[0].get("rxcui")

#     if not rxcui:
#         return q, False

#     # Step 2: Check TTY (term type) to see if it's a brand name
#     data = _api_get(
#         f"https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/properties.json"
#     )
#     tty = ""
#     if data:
#         props = data.get("properties", {})
#         tty = props.get("tty", "")

#     is_brand = tty in ("BN", "SBD", "BPCK")

#     # Step 3: Get the ingredient(s) - the actual generic name
#     data = _api_get(
#         f"https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/related.json?tty=IN+MIN"
#     )
#     if data:
#         groups = data.get("relatedGroup", {}).get("conceptGroup", [])
#         # Prefer MIN (Multiple Ingredient Name) over individual IN entries
#         min_names = []
#         in_names = []
#         for group in groups:
#             tty = group.get("tty", "")
#             for c in group.get("conceptProperties", []):
#                 name = c.get("name", "")
#                 if name:
#                     if tty == "MIN":
#                         min_names.append(name.title())
#                     elif tty == "IN":
#                         in_names.append(name.title())
#         ingredients = min_names if min_names else in_names
#         if ingredients:
#             return " / ".join(ingredients), is_brand

#     return q, is_brand


# # ------------------------------------------------------------------
# # Web Search - Search the entire web (DuckDuckGo)
# # ------------------------------------------------------------------
# def search_web(query, generic=""):
#     """Search the web for drug info - returns results from ALL sites."""
#     results = []
#     generic = generic or query
#     seen_urls = set()

#     try:
#         with DDGS() as ddgs:
#             hits = list(ddgs.text(
#                 f"{query} drug brand generic tablet price alternatives",
#                 max_results=40,
#             ))
#     except Exception:
#         hits = []

#     for hit in hits:
#         title = hit.get("title", "")
#         href = hit.get("href", "")
#         body = hit.get("body", "")

#         if not title or href in seen_urls:
#             continue
#         seen_urls.add(href)

#         from urllib.parse import urlparse
#         domain = urlparse(href).netloc.replace("www.", "")

#         drug_name = title.split(" - ")[0].split(" | ")[0].strip()

#         strength, form = _parse_strength_form(title + " " + body)

#         price = ""
#         price_match = re.search(r'[₹$]\s*[\d,]+\.?\d*', body)
#         if price_match:
#             price = price_match.group(0)

#         results.append({
#             "drug_name": drug_name,
#             "brand_name": drug_name,
#             "generic_name": generic,
#             "manufacturer": "",
#             "composition": "",
#             "strength": strength,
#             "form": form,
#             "price": price,
#             "source_site": domain,
#             "source_url": href,
#         })

#     return results

#     return results


# # ============================================================
# # SEARCH ENGINE - Parallel multi-source search
# # ============================================================

# def search_all_sources(query):
#     """Search the web for drug variants, resolve generic name via RxNorm"""
#     query = query.strip()
#     if not query:
#         return [], {}, query

#     # Resolve the actual generic ingredient name via RxNorm
#     resolved_generic, _is_brand = _resolve_generic_name(query)

#     errors = {}
#     try:
#         all_results = search_web(query, generic=resolved_generic)
#     except Exception as e:
#         all_results = []
#         errors["Web Search"] = str(e)

#     # Merge duplicates: same drug name from different sites → one result with combined source URLs
#     merged = {}
#     for r in all_results:
#         key = re.sub(r'\s+', ' ', r["drug_name"].lower().strip())
#         source_entry = {"site": r["source_site"], "url": r["source_url"]}

#         if key in merged:
#             existing = merged[key]
#             site_names = {s["site"] for s in existing["source_urls"]}
#             if source_entry["site"] not in site_names:
#                 existing["source_urls"].append(source_entry)
#             for field in ["brand_name", "generic_name", "manufacturer",
#                           "composition", "strength", "form", "price"]:
#                 if not existing.get(field) and r.get(field):
#                     existing[field] = r[field]
#         else:
#             merged[key] = {
#                 "drug_name": r["drug_name"],
#                 "brand_name": r.get("brand_name", ""),
#                 "generic_name": r.get("generic_name", ""),
#                 "manufacturer": r.get("manufacturer", ""),
#                 "composition": r.get("composition", ""),
#                 "strength": r.get("strength", ""),
#                 "form": r.get("form", ""),
#                 "price": r.get("price", ""),
#                 "source_urls": [source_entry],
#             }

#     unique = list(merged.values())

#     # Sort: exact matches first, then starts-with, then alphabetical
#     ql = query.lower()
#     unique.sort(key=lambda r: (
#         0 if ql == r["drug_name"].lower().strip() else
#         1 if r["drug_name"].lower().strip().startswith(ql) else 2,
#         r["drug_name"].lower(),
#     ))

#     # Persist to DB
#     if unique:
#         db_save_search(query, unique)

#     # Save to JSON file
#     ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#     safe_q = re.sub(r'[^\w\-]', '_', query)
#     out_file = RESULTS_DIR / f"{safe_q}_{ts}.json"
#     with open(out_file, "w", encoding="utf-8") as f:
#         json.dump(
#             {
#                 "query": query,
#                 "searched_at": datetime.now().isoformat(),
#                 "total_results": len(unique),
#                 "errors": errors,
#                 "results": unique,
#             },
#             f,
#             indent=2,
#             ensure_ascii=False,
#         )

#     return unique, errors, resolved_generic


# # ============================================================
# # FLASK APP
# # ============================================================

# app = Flask(__name__)

# SEARCH_PAGE = """
# <!DOCTYPE html>
# <html lang="en">
# <head>
#     <meta charset="UTF-8">
#     <meta name="viewport" content="width=device-width, initial-scale=1.0">
#     <title>Drug Variant Search</title>
#     <style>
#         * { box-sizing: border-box; margin: 0; padding: 0; }
#         body { font-family: 'Segoe UI', Tahoma, sans-serif; background: #0f1117; color: #e0e0e0; }
#         .container { max-width: 1300px; margin: 0 auto; padding: 20px; }
#         h1 { text-align: center; color: #4fc3f7; margin: 30px 0; font-size: 2em; }
#         h1 span { color: #81c784; }

#         .search-box { display: flex; gap: 10px; margin: 20px 0; justify-content: center; }
#         .search-box input {
#             width: 420px; padding: 12px 16px; border: 2px solid #333;
#             border-radius: 8px; background: #1e1e2e; color: #fff; font-size: 16px;
#         }
#         .search-box input:focus { border-color: #4fc3f7; outline: none; }
#         .search-box button {
#             padding: 12px 24px; background: #4fc3f7; color: #000; border: none;
#             border-radius: 8px; cursor: pointer; font-size: 16px; font-weight: bold;
#         }
#         .search-box button:hover { background: #29b6f6; }
#         .search-box button:disabled { background: #555; cursor: wait; }

#         .sources { text-align: center; margin: 10px 0 20px; }
#         .sources label { margin: 0 10px; color: #aaa; cursor: pointer; font-size: 14px; }
#         .sources input[type=checkbox] { accent-color: #4fc3f7; }

#         .status { text-align: center; color: #ffd54f; margin: 15px 0; font-size: 14px; }
#         .stats { display: flex; gap: 20px; justify-content: center; margin: 15px 0; flex-wrap: wrap; }
#         .stat { background: #1e1e2e; padding: 12px 20px; border-radius: 8px; text-align: center; min-width: 120px; }
#         .stat .num { font-size: 24px; color: #4fc3f7; font-weight: bold; }
#         .stat .label { font-size: 12px; color: #888; }

#         table { width: 100%; border-collapse: collapse; margin: 20px 0; }
#         th { background: #1a237e; color: #fff; padding: 10px 12px; text-align: left; font-size: 13px; position: sticky; top: 0; }
#         td { padding: 8px 12px; border-bottom: 1px solid #333; font-size: 13px; }
#         tr:hover { background: #1e1e2e; }
#         td a { color: #4fc3f7; text-decoration: none; }
#         td a:hover { text-decoration: underline; }

#         .tag { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; }
#         .tag-rxnorm { background: #1565c0; color: #fff; }
#         .tag-openfda { background: #e65100; color: #fff; }
#         .tag-dailymed { background: #2e7d32; color: #fff; }
#         .tag-medline { background: #6a1b9a; color: #fff; }
#         .tag-1mg { background: #e91e63; color: #fff; }
#         .tag-pharmeasy { background: #00897b; color: #fff; }
#         .tag-web { background: #ff6f00; color: #fff; }

#         .actions { display: flex; gap: 10px; justify-content: center; margin: 20px 0; }
#         .actions button {
#             padding: 8px 16px; border: 1px solid #555; background: #1e1e2e;
#             color: #e0e0e0; border-radius: 6px; cursor: pointer; font-size: 13px;
#         }
#         .actions button:hover { background: #333; }

#         .empty { text-align: center; color: #888; padding: 40px; }
#         .error { color: #ef5350; }
#         .loading { display: none; text-align: center; color: #ffd54f; padding: 20px; }
#         .loading.active { display: block; }
#     </style>
# </head>
# <body>
# <div class="container">
#     <h1>Drug <span>Variant</span> Search</h1>

#     <div class="search-box">
#         <input type="text" id="query" placeholder="Enter drug name (e.g. pantoprazole, amoxicillin, augmentin)"
#                onkeydown="if(event.key==='Enter') doSearch()">
#         <button id="searchBtn" onclick="doSearch()">Search</button>
#     </div>

#     <div class="loading" id="loading">Searching the web for drug variants... This may take a few seconds.</div>
#     <div id="status" class="status"></div>
#     <div id="stats"></div>
#     <div id="results"></div>
# </div>

# <script>
# let lastResults = [];

# function getSiteTag(site) {
#     const map = {
#         'RxNorm (NIH)': 'tag-rxnorm', 'OpenFDA': 'tag-openfda',
#         'DailyMed (FDA)': 'tag-dailymed', 'MedlinePlus': 'tag-medline',
#         '1mg.com': 'tag-1mg', 'PharmEasy': 'tag-pharmeasy'
#     };
#     const cls = map[site] || (site.startsWith('Web (') ? 'tag-web' : '');
#     return '<span class="tag ' + cls + '">' + site + '</span>';
# }

# function getSourcesHtml(sources) {
#     if (!sources || !sources.length) return '-';
#     return sources.map(function(s) {
#         const tag = getSiteTag(s.site);
#         return s.url ? '<a href="' + escapeHtml(s.url) + '" target="_blank">' + tag + '</a>' : tag;
#     }).join(' ');
# }

# function escapeHtml(s) {
#     if (!s) return '';
#     return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
# }

# async function doSearch() {
#     const q = document.getElementById('query').value.trim();
#     if (!q) return;

#     const btn = document.getElementById('searchBtn');
#     const loading = document.getElementById('loading');

#     btn.disabled = true;
#     loading.classList.add('active');
#     document.getElementById('results').innerHTML = '';
#     document.getElementById('stats').innerHTML = '';
#     document.getElementById('status').textContent = '';

#     try {
#         const url = '/api/search?q=' + encodeURIComponent(q);
#         const resp = await fetch(url);
#         const data = await resp.json();
#         lastResults = data.results || [];

#         // Stats
#         const allSites = new Set();
#         lastResults.forEach(r => { (r.source_urls || []).forEach(s => allSites.add(s.site)); });

#         let statsHtml = '<div class="stats">';
#         statsHtml += '<div class="stat"><div class="num">' + lastResults.length + '</div><div class="label">Total Variants</div></div>';
#         statsHtml += '<div class="stat"><div class="num">' + allSites.size + '</div><div class="label">Sources</div></div>';
#         const brands = new Set(lastResults.map(r => (r.brand_name||'').toLowerCase()).filter(Boolean));
#         statsHtml += '<div class="stat"><div class="num">' + brands.size + '</div><div class="label">Unique Brands</div></div>';
#         const mfgs = new Set(lastResults.map(r => (r.manufacturer||'').toLowerCase()).filter(Boolean));
#         statsHtml += '<div class="stat"><div class="num">' + mfgs.size + '</div><div class="label">Manufacturers</div></div>';
#         statsHtml += '</div>';
#         document.getElementById('stats').innerHTML = statsHtml;

#         // Errors
#         if (data.errors && Object.keys(data.errors).length > 0) {
#             const errs = Object.entries(data.errors).map(function(e) { return e[0] + ': ' + e[1]; }).join(', ');
#             document.getElementById('status').innerHTML = '<span class="error">Errors: ' + escapeHtml(errs) + '</span>';
#         }

#         if (lastResults.length === 0) {
#             document.getElementById('results').innerHTML = '<div class="empty">No variants found. Try a different drug name.</div>';
#             return;
#         }

#         let html = '<table><thead><tr><th>#</th><th>Drug Name</th><th>Brand</th><th>Generic</th>';
#         html += '<th>Composition</th><th>Manufacturer</th><th>Strength</th><th>Form</th><th>Sources</th></tr></thead><tbody>';

#         lastResults.forEach(function(r, i) {
#             html += '<tr>';
#             html += '<td>' + (i+1) + '</td>';
#             html += '<td>' + escapeHtml(r.drug_name) + '</td>';
#             html += '<td>' + escapeHtml(r.brand_name || '-') + '</td>';
#             html += '<td>' + escapeHtml(r.generic_name || '-') + '</td>';
#             html += '<td>' + escapeHtml(r.composition || '-') + '</td>';
#             html += '<td>' + escapeHtml(r.manufacturer || '-') + '</td>';
#             html += '<td>' + escapeHtml(r.strength || '-') + '</td>';
#             html += '<td>' + escapeHtml(r.form || '-') + '</td>';
#             html += '<td>' + getSourcesHtml(r.source_urls) + '</td>';
#             html += '</tr>';
#         });

#         html += '</tbody></table>';
#         html += '<div class="actions">';
#         html += '<button onclick="saveResults()">Save Results</button>';
#         html += '<button onclick="downloadCSV()">Download CSV</button>';
#         html += '<button onclick="downloadJSON()">Download JSON</button>';
#         html += '</div>';

#         document.getElementById('results').innerHTML = html;

#     } catch(e) {
#         document.getElementById('results').innerHTML = '<div class="error">Error: ' + escapeHtml(e.message) + '</div>';
#     } finally {
#         btn.disabled = false;
#         loading.classList.remove('active');
#     }
# }

# async function saveResults() {
#     const q = document.getElementById('query').value.trim();
#     const resp = await fetch('/api/save', {
#         method: 'POST',
#         headers: {'Content-Type': 'application/json'},
#         body: JSON.stringify({query: q, results: lastResults})
#     });
#     const data = await resp.json();
#     document.getElementById('status').textContent = data.message || 'Saved!';
# }

# function downloadCSV() {
#     if (!lastResults.length) return;
#     const headers = ['drug_name','brand_name','generic_name','manufacturer','composition','strength','form','sources','source_urls'];
#     let csv = headers.join(',') + '\\n';
#     lastResults.forEach(function(r) {
#         csv += headers.map(function(h) {
#             let val = '';
#             if (h === 'sources') {
#                 val = (r.source_urls || []).map(function(s) { return s.site; }).join(', ');
#             } else if (h === 'source_urls') {
#                 val = (r.source_urls || []).map(function(s) { return s.url; }).filter(Boolean).join(' | ');
#             } else {
#                 val = r[h] || '';
#             }
#             return '"' + val.replace(/"/g,'""') + '"';
#         }).join(',') + '\\n';
#     });
#     const blob = new Blob([csv], {type:'text/csv'});
#     const a = document.createElement('a');
#     a.href = URL.createObjectURL(blob);
#     a.download = 'drug_variants_' + document.getElementById('query').value.trim() + '.csv';
#     a.click();
# }

# function downloadJSON() {
#     if (!lastResults.length) return;
#     const blob = new Blob([JSON.stringify(lastResults, null, 2)], {type:'application/json'});
#     const a = document.createElement('a');
#     a.href = URL.createObjectURL(blob);
#     a.download = 'drug_variants_' + document.getElementById('query').value.trim() + '.json';
#     a.click();
# }
# </script>
# </body>
# </html>
# """


# @app.route("/")
# def index():
#     return render_template_string(SEARCH_PAGE)


# @app.route("/api/search")
# def api_search():
#     query = request.args.get("q", "").strip()
#     if not query:
#         return jsonify({"error": "Missing query parameter 'q'"}), 400

#     results, errors, resolved_generic = search_all_sources(query)

#     return jsonify({
#         "query": query,
#         "generic_name": resolved_generic,
#         "total": len(results),
#         "errors": errors,
#         "results": results,
#     })


# @app.route("/api/history")
# def api_history():
#     limit = request.args.get("limit", 50, type=int)
#     return jsonify(get_history(limit))


# @app.route("/api/save", methods=["POST"])
# def api_save():
#     data = request.get_json()
#     if not data:
#         return jsonify({"error": "No JSON body"}), 400

#     query = data.get("query", "")
#     results = data.get("results", [])

#     conn = sqlite3.connect(str(DB_PATH))
#     conn.execute(
#         "INSERT INTO saved_searches (query, results_json, saved_at) VALUES (?, ?, ?)",
#         (query, json.dumps(results, ensure_ascii=False), datetime.now().isoformat()),
#     )
#     conn.commit()
#     conn.close()

#     return jsonify({"message": f"Saved {len(results)} results for '{query}'"})


# @app.route("/api/saved")
# def api_saved():
#     return jsonify(get_saved())


# # ============================================================
# # RUN
# # ============================================================

# if __name__ == "__main__":
#     init_db()
#     print("=" * 60)
#     print("  Drug Variant Search API")
#     print("=" * 60)
#     print("  Web UI:     http://localhost:5000")
#     print("  API Search: http://localhost:5000/api/search?q=pantoprazole")
#     print("  History:    http://localhost:5000/api/history")
#     print("=" * 60)
#     app.run(host="0.0.0.0", port=5000, debug=True)



# """
# Drug Variant Search API
# Robust parallel search version

# Run:
#     python drug_variant_search_api.py

# Open:
#     http://localhost:5000

# API Example:
#     http://localhost:5000/api/search?q=clotrimazole
# """

# import re
# import requests
# import concurrent.futures
# from urllib.parse import quote_plus
# from flask import Flask, jsonify, request
# from ddgs import DDGS

# app = Flask(__name__)

# HEADERS = {"User-Agent": "Mozilla/5.0"}
# TIMEOUT = 10


# # -----------------------------------------------------
# # Extract strength + dosage form
# # -----------------------------------------------------

# def parse_strength_form(text):

#     strength = ""
#     form = ""

#     s = re.search(r'(\d+\.?\d*\s*(mg|mcg|ml|g|%))', text, re.I)
#     if s:
#         strength = s.group(1)

#     f = re.search(
#         r'(tablet|capsule|cream|ointment|gel|syrup|injection|drops|powder|lotion)',
#         text,
#         re.I
#     )

#     if f:
#         form = f.group(1).title()

#     return strength, form


# # -----------------------------------------------------
# # Resolve generic ingredient via RxNorm
# # -----------------------------------------------------

# def resolve_generic(query):

#     try:

#         url = f"https://rxnav.nlm.nih.gov/REST/rxcui.json?name={quote_plus(query)}"
#         data = requests.get(url, timeout=TIMEOUT).json()

#         ids = data.get("idGroup", {}).get("rxnormId", [])

#         if not ids:
#             return query

#         rxcui = ids[0]

#         data = requests.get(
#             f"https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/related.json?tty=IN+MIN",
#             timeout=TIMEOUT
#         ).json()

#         groups = data.get("relatedGroup", {}).get("conceptGroup", [])

#         for g in groups:
#             for c in g.get("conceptProperties", []):
#                 name = c.get("name")
#                 if name:
#                     return name

#     except:
#         pass

#     return query


# # -----------------------------------------------------
# # RxNorm brand variants
# # -----------------------------------------------------

# def search_rxnorm(query):

#     results = []

#     try:

#         url = f"https://rxnav.nlm.nih.gov/REST/drugs.json?name={quote_plus(query)}"
#         data = requests.get(url, timeout=TIMEOUT).json()

#         groups = data.get("drugGroup", {}).get("conceptGroup", [])

#         for g in groups:

#             if g.get("tty") not in ["SBD", "SCD"]:
#                 continue

#             for c in g.get("conceptProperties", []):

#                 name = c.get("name")

#                 strength, form = parse_strength_form(name)

#                 results.append({
#                     "drug_name": name,
#                     "brand_name": name,
#                     "strength": strength,
#                     "form": form,
#                     "source_url": "https://rxnav.nlm.nih.gov/"
#                 })

#     except:
#         pass

#     return results


# # -----------------------------------------------------
# # OpenFDA search
# # -----------------------------------------------------

# def search_openfda(query):

#     results = []

#     try:

#         url = f"https://api.fda.gov/drug/label.json?search=openfda.generic_name:{quote_plus(query)}&limit=50"
#         data = requests.get(url, timeout=TIMEOUT).json()

#         for item in data.get("results", []):

#             openfda = item.get("openfda", {})

#             brands = openfda.get("brand_name", [])
#             forms = openfda.get("dosage_form", [])
#             strengths = openfda.get("strength", [])

#             brand = brands[0] if brands else ""
#             form = forms[0] if forms else ""
#             strength = strengths[0] if strengths else ""

#             if not brand:
#                 continue

#             results.append({
#                 "drug_name": brand,
#                 "brand_name": brand,
#                 "strength": strength,
#                 "form": form,
#                 "source_url": "https://api.fda.gov"
#             })

#     except:
#         pass

#     return results


# # -----------------------------------------------------
# # DailyMed search
# # -----------------------------------------------------

# def search_dailymed(query):

#     results = []

#     try:

#         url = f"https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.json?drug_name={quote_plus(query)}"
#         data = requests.get(url, timeout=TIMEOUT).json()

#         for item in data.get("data", []):

#             title = item.get("title")

#             if not title:
#                 continue

#             strength, form = parse_strength_form(title)

#             results.append({
#                 "drug_name": title,
#                 "brand_name": title,
#                 "strength": strength,
#                 "form": form,
#                 "source_url": f"https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid={item.get('setid')}"
#             })

#     except:
#         pass

#     return results


# # -----------------------------------------------------
# # Web search using DuckDuckGo
# # -----------------------------------------------------

# WEB_QUERIES = [
#     "{drug} tablet brand",
#     "{drug} capsule brand",
#     "{drug} cream brand",
#     "{drug} medicine brands"
# ]


# def search_web(query):

#     results = []

#     try:

#         with DDGS() as ddgs:

#             for q in WEB_QUERIES:

#                 search_query = q.format(drug=query)

#                 hits = ddgs.text(search_query, max_results=15)

#                 for h in hits:

#                     title = h.get("title", "")
#                     url = h.get("href", "")

#                     if not title:
#                         continue

#                     strength, form = parse_strength_form(title)

#                     results.append({
#                         "drug_name": title,
#                         "brand_name": title,
#                         "strength": strength,
#                         "form": form,
#                         "source_url": url
#                     })

#     except:
#         pass

#     return results


# # -----------------------------------------------------
# # Deduplicate results
# # -----------------------------------------------------

# def dedupe(results):

#     seen = set()
#     clean = []

#     for r in results:

#         key = (
#             r["brand_name"].lower(),
#             r.get("strength"),
#             r.get("form")
#         )

#         if key in seen:
#             continue

#         seen.add(key)
#         clean.append(r)

#     return clean


# # -----------------------------------------------------
# # Parallel search across sources
# # -----------------------------------------------------

# SOURCES = [
#     search_rxnorm,
#     search_openfda,
#     search_dailymed,
#     search_web
# ]


# def search_all_sources(query):

#     results = []

#     with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:

#         futures = [executor.submit(src, query) for src in SOURCES]

#         for f in concurrent.futures.as_completed(futures):

#             try:
#                 results.extend(f.result())
#             except:
#                 pass

#     return results


# # -----------------------------------------------------
# # Main search function
# # -----------------------------------------------------

# def search_variants(query):

#     generic = resolve_generic(query)

#     raw_results = search_all_sources(generic)

#     clean_results = dedupe(raw_results)

#     for r in clean_results:
#         r["generic_name"] = generic

#     return generic, clean_results


# # -----------------------------------------------------
# # API endpoint
# # -----------------------------------------------------

# @app.route("/api/search")

# def api_search():

#     query = request.args.get("q", "").strip()

#     if not query:
#         return jsonify({"error": "missing query"}), 400

#     generic, results = search_variants(query)

#     return jsonify({
#         "query": query,
#         "generic_name": generic,
#         "total_variants": len(results),
#         "results": results
#     })


# # -----------------------------------------------------
# # Home page
# # -----------------------------------------------------

# @app.route("/")

# def home():

#     return """
#     <h2>Drug Variant Search API</h2>
#     <p>Example:</p>
#     <a href="/api/search?q=clotrimazole">/api/search?q=clotrimazole</a>
#     """


# # -----------------------------------------------------

# if __name__ == "__main__":

#     print("="*50)
#     print("Drug Variant Search API Running")
#     print("http://localhost:5000")
#     print("="*50)

#     app.run(host="0.0.0.0", port=5000, debug=True)


import re
import requests
import concurrent.futures
from urllib.parse import quote_plus, urlparse
from flask import Flask, request, jsonify
from ddgs import DDGS

app = Flask(__name__)

TIMEOUT = 10


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

    s = re.search(r'(\d+\.?\d*\s*(?:mg|mcg|ml|g|%))', text, re.I)
    if s:
        strength = s.group(1)

    f = re.search(
        r'(tablet|capsule|cream|ointment|gel|syrup|injection|drops|powder|lotion|solution|suspension)',
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
    if generic.lower() in t:
        return True
    for b in brand_names:
        if b.lower() in t:
            return True
    return False


def _guess_brand_from_title(title, brand_names):
    tl = (title or "").lower()
    for b in brand_names:
        if b.lower() in tl:
            return b
    # Common RxNorm-like format: "... [BrandName]"
    m = re.search(r'\[([^\]]+)\]', title or "")
    if m:
        candidate = m.group(1).strip()
        if candidate:
            return candidate
    return ""


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
    if not _is_relevant_title(title, generic, brand_names):
        return False
    if not any(h in t or h in b for h in MEDICAL_HINTS):
        return False
    return True


def is_indian_domain(site):
    s = (site or "").lower()
    return s.endswith(".in") or ".co.in" in s or s in INDIAN_DOMAIN_HINTS


def search_web(generic, brand_names):
    results = []
    seen_urls = set()

    queries = [f"{generic} drug brand tablet capsule"]
    for domain in INDIAN_DOMAIN_HINTS:
        # Indian domains get heavier query coverage.
        queries.append(f"site:{domain} {generic} medicine price")
        queries.append(f"site:{domain} {generic} brand name")
        queries.append(f"site:{domain} {generic} tablet capsule")
        queries.append(f"site:{domain} {generic} uses dosage")
    for domain in TRUSTED_GLOBAL_DOMAIN_HINTS:
        queries.append(f"site:{domain} {generic} brand name dosage")
    try:
        with DDGS() as ddgs:
            hits = []
            for q in queries:
                try:
                    per_query_max = 20 if q.startswith("site:") and any(d in q for d in INDIAN_DOMAIN_HINTS) else 10
                    hits.extend(list(ddgs.text(q, max_results=per_query_max)))
                except Exception:
                    continue
    except Exception:
        hits = []

    for h in hits:
        title = h.get("title", "")
        url = h.get("href", "")
        body = h.get("body", "")
        if not url or url in seen_urls:
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

    # India-first ordering in raw web results.
    results.sort(key=lambda r: (0 if is_indian_domain(r.get("source_site", "")) else 1, r.get("drug_name", "").lower()))
    return results


def _enrich_missing_fields(rows):
    """Fill missing brand/strength/form using same generic+strength+form constraints.

    If a unique brand exists for a given (strength, form), propagate it to rows
    missing brand_name under the same constraints.
    """
    if not rows:
        return rows

    def norm(v):
        return re.sub(r'\s+', ' ', (v or "").strip().lower())

    # Unique brand per (strength, form)
    brand_by_key = {}
    for r in rows:
        b = (r.get("brand_name") or "").strip()
        if not b:
            continue
        key = (norm(r.get("strength")), norm(r.get("form")))
        brand_by_key.setdefault(key, set()).add(b)

    # Most common strength/form per brand for reverse fill
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

        # Fill brand from same strength+form when unique.
        if not (r.get("brand_name") or "").strip() and key in brand_by_key and len(brand_by_key[key]) == 1:
            r["brand_name"] = list(brand_by_key[key])[0]

        # Fill strength/form from known most-common values for that brand.
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
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
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

    # Merge duplicates and keep only requested fields
    merged = {}
    for r in raw:
        key = re.sub(r'\s+', ' ', r.get("drug_name", "").lower().strip())
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

    # Keep payload focused and manageable.
    clean = clean[:50]

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
    print("API: http://localhost:5000/api/search?q=pantoprazole")
    app.run(port=5000, debug=True)