# geometric_crawler/spiders/drug_spider.py
"""
Universal Drug Spider - Works on ANY drug information website.
Auto-detects drug links, sections, and organizes data into standard columns.
"""

import scrapy
import hashlib
import json
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from parsel import Selector
from pathlib import Path
from urllib.parse import urlparse, urljoin, urldefrag, parse_qs, urlencode
from ..items import ScrapedItem

# Optional Playwright support
try:
    from scrapy_playwright.page import PageMethod
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    PageMethod = None

# Import fast parser
try:
    from ..fast_parser import fast_parser
    FAST_PARSER_AVAILABLE = True
except ImportError:
    FAST_PARSER_AVAILABLE = False
    fast_parser = None


class DrugSpider(scrapy.Spider):
    """
    UNIVERSAL Drug Spider - Works on ANY drug website.
    
    Features:
    - Auto-detects drug links on listing pages
    - Auto-detects drug info sections (side effects, dosage, warnings, etc.)
    - Organizes data into standard columns
    - Works on: RxList, Drugs.com, WebMD, DailyMed, MedlinePlus, etc.
    """
    
    name = "drug"
    
    # UNIVERSAL drug link patterns - matches common drug page URLs
    DRUG_LINK_PATTERNS = [
        re.compile(r'/[a-z0-9-]+-drug\.htm', re.IGNORECASE),  # RxList
        re.compile(r'/drugs/[a-z0-9-]+\.html', re.IGNORECASE),  # Drugs.com
        re.compile(r'/drug/[a-z0-9-]+', re.IGNORECASE),  # Generic
        re.compile(r'/medicine/[a-z0-9-]+', re.IGNORECASE),  # Medicine
        re.compile(r'/druginfo/', re.IGNORECASE),  # MedlinePlus
        re.compile(r'/monograph/', re.IGNORECASE),  # Monographs
        re.compile(r'/spl/', re.IGNORECASE),  # DailyMed SPL
        # Indian Pharmacy Sites
        re.compile(r'/prescriptions/[a-z0-9-]+', re.IGNORECASE),  # netmeds prescriptions
        re.compile(r'/otc/[a-z0-9-]+', re.IGNORECASE),  # netmeds OTC
        re.compile(r'/non-prescriptions/[a-z0-9-]+', re.IGNORECASE),  # netmeds non-Rx
        re.compile(r'/product/[a-z0-9-]+', re.IGNORECASE),  # 1mg, pharmeasy products
        re.compile(r'/otc-[a-z]+-[a-z0-9-]+', re.IGNORECASE),  # 1mg OTC format
        re.compile(r'/drugs-medication/[a-z0-9-]+', re.IGNORECASE),  # 1mg drugs
        re.compile(r'/pharmeasy/[a-z0-9-]+', re.IGNORECASE),  # pharmeasy
        re.compile(r'/apollo[/-][a-z0-9-]+product', re.IGNORECASE),  # apollo pharmacy
        re.compile(r'/medicine-detail/[a-z0-9-]+', re.IGNORECASE),  # Generic medicine detail
    ]
    
    # Category/listing page patterns - these are NOT drug pages
    CATEGORY_PAGE_PATTERNS = [
        re.compile(r'/collection/', re.IGNORECASE),  # netmeds collection
        re.compile(r'/sections/', re.IGNORECASE),  # netmeds sections
        re.compile(r'/categories/', re.IGNORECASE),  # Category listings
        re.compile(r'/category/', re.IGNORECASE),  # Category page
        re.compile(r'/browse/', re.IGNORECASE),  # Browse pages
        re.compile(r'/listing-page', re.IGNORECASE),  # Generic listing
        re.compile(r'/search\?', re.IGNORECASE),  # Search results
        re.compile(r'/alphabetical-list', re.IGNORECASE),  # Alphabetical listings
    ]
    
    # UNIVERSAL section keywords - maps keywords to standard column names
    # Comprehensive pharmacological categories
    SECTION_KEYWORDS = {
        # === BASIC INFO ===
        'description': ['description', 'overview', 'about', 'summary', 'introduction', 'what is this'],
        'quick_tips': ['quick tips', 'tips', 'important tips', 'helpful tips'],
        'fact_box': ['fact box', 'facts', 'key facts', 'important facts'],
        'patient_concerns': ['patient concerns', 'concerns', 'common concerns', 'patient questions'],
        'user_feedback': ['user feedback', 'feedback', 'reviews', 'user reviews', 'ratings'],
        'faqs': ['faqs', 'faq', 'questions', 'frequently asked questions', 'common questions'],
        'uses': ['uses', 'indications', 'used for', 'treats', 'treatment', 'therapeutic use'],
        'brand_names': ['brand names', 'brand', 'trade names', 'also known as', 'other names'],
        'generic_name': ['generic name', 'generic', 'chemical name', 'inn', 'usan'],
        'drug_class': ['drug class', 'class', 'category', 'therapeutic class', 'pharmacologic class'],
        
        # === DOSAGE & ADMINISTRATION ===
        'dosage': ['dosage', 'dose', 'administration', 'how to take', 'how to use', 'directions', 'dosing'],
        'adult_dosage': ['adult dose', 'adult dosage', 'usual adult', 'adults'],
        'pediatric_dosage': ['pediatric dose', 'pediatric dosage', 'children', 'child dose', 'pediatric use', 'infants', 'neonatal'],
        'geriatric_dosage': ['geriatric dose', 'geriatric dosage', 'elderly', 'older adults', 'geriatric use'],
        'renal_dosing': ['renal dose', 'renal impairment', 'kidney', 'renal adjustment', 'creatinine clearance'],
        'hepatic_dosing': ['hepatic dose', 'hepatic impairment', 'liver', 'hepatic adjustment', 'cirrhosis'],
        'route_of_administration': ['route', 'administration route', 'oral', 'intravenous', 'iv', 'im', 'subcutaneous'],
        
        # === PHARMACOKINETICS (ADME) ===
        'pharmacokinetics': ['pharmacokinetics', 'pk', 'adme'],
        'absorption': ['absorption', 'bioavailability', 'oral absorption', 'tmax', 'time to peak'],
        'distribution': ['distribution', 'vd', 'volume of distribution', 'protein binding', 'plasma protein', 'tissue distribution'],
        'metabolism': ['metabolism', 'biotransformation', 'hepatic metabolism', 'cyp', 'cytochrome', 'first pass', 'metabolites'],
        'elimination': ['elimination', 'excretion', 'half-life', 't1/2', 'clearance', 'renal excretion', 'fecal excretion'],
        
        # === PHARMACODYNAMICS ===
        'pharmacodynamics': ['pharmacodynamics', 'pd', 'drug action'],
        'mechanism_of_action': ['mechanism of action', 'moa', 'how it works', 'mode of action', 'mechanism'],
        'receptor_binding': ['receptor', 'binding', 'affinity', 'selectivity', 'agonist', 'antagonist'],
        'onset_of_action': ['onset', 'time to effect', 'onset of action', 'begins working'],
        'duration_of_action': ['duration', 'duration of action', 'how long', 'effect duration'],
        'clinical_pharmacology': ['clinical pharmacology', 'pharmacology'],
        
        # === SAFETY ===
        'side_effects': ['side effects', 'adverse', 'reactions', 'adverse reactions', 'adverse events', 'unwanted effects'],
        'common_side_effects': ['common side effects', 'most common', 'frequent'],
        'serious_side_effects': ['serious side effects', 'severe', 'serious adverse', 'life-threatening'],
        'warnings': ['warnings', 'warning', 'caution', 'alert', 'important safety', 'boxed warning', 'black box'],
        'precautions': ['precautions', 'precaution', 'before taking', 'before using', 'special care'],
        'contraindications': ['contraindications', 'contraindicated', 'do not use', 'not recommended', 'avoid'],
        'allergic_reactions': ['allergic', 'allergy', 'hypersensitivity', 'anaphylaxis'],
        
        # === DRUG INTERACTIONS ===
        'drug_interactions': ['drug interactions', 'interactions', 'interacts with', 'drug-drug'],
        'food_interactions': ['food interactions', 'food', 'take with food', 'empty stomach', 'grapefruit'],
        'alcohol_interactions': ['alcohol', 'ethanol', 'drinking'],
        
        # === SPECIAL POPULATIONS ===
        'pregnancy': ['pregnancy', 'pregnant', 'pregnancy category', 'teratogenic', 'fetal', 'trimester'],
        'lactation': ['lactation', 'breastfeeding', 'nursing', 'breast milk', 'nursing mothers'],
        'fertility': ['fertility', 'reproductive', 'sperm', 'conception'],
        'pediatric': ['pediatric', 'children', 'infants', 'neonates', 'adolescents', 'pediatric safety'],
        'geriatric': ['geriatric', 'elderly', 'older adults', 'aging', 'geriatric considerations'],
        
        # === OVERDOSE & TOXICOLOGY ===
        'overdose': ['overdose', 'overdosage', 'too much', 'toxicity', 'poisoning'],
        'overdose_symptoms': ['overdose symptoms', 'signs of overdose', 'toxic effects'],
        'overdose_treatment': ['overdose treatment', 'antidote', 'management of overdose', 'hemodialysis'],
        'ld50': ['ld50', 'lethal dose', 'toxic dose'],
        
        # === FORMULATION ===
        'how_supplied': ['how supplied', 'available forms', 'forms', 'strengths', 'packaging', 'formulations'],
        'ingredients': ['ingredients', 'contains', 'composition', 'active ingredient', 'inactive ingredients', 'excipients'],
        'storage': ['storage', 'store', 'how to store', 'keep', 'stability', 'shelf life'],
        
        # === REGULATORY ===
        'fda_info': ['fda', 'approval', 'label', 'prescribing information', 'product label'],
        'dea_schedule': ['dea', 'schedule', 'controlled', 'controlled substance', 'narcotic'],
        'patient_info': ['patient information', 'patient counseling', 'medication guide', 'patient education'],
        
        # === CLINICAL ===
        'clinical_trials': ['clinical trials', 'studies', 'efficacy', 'clinical studies', 'trials'],
        'evidence': ['evidence', 'research', 'data', 'outcomes'],
        
        # === INDIAN PHARMACY SITES (netmeds, 1mg, pharmeasy, apollo) ===
        'salt_composition': ['salt composition', 'salt', 'composition', 'active ingredients', 'content', 'salt information'],
        'product_details': ['product details', 'product information', 'medicine details', 'drug details'],
        'benefits': ['benefits', 'advantages', 'key benefits', 'why use'],
        'how_to_use': ['how to use', 'direction for use', 'usage', 'taking', 'method of use'],
        'safety_advice': ['safety advice', 'safety information', 'safety tips', 'expert advice'],
        'missed_dose': ['missed dose', 'if you miss', 'forgot dose'],
        'expert_advice': ['expert advice', 'doctor advice', 'pharmacist tips'],
        'faqs': ['faq', 'frequently asked', 'questions', 'common questions'],
        'substitutes': ['substitutes', 'alternatives', 'similar medicines', 'generic alternatives'],
        'manufacturer': ['manufacturer', 'made by', 'marketed by', 'company', 'manufactured by'],
        'price': ['price', 'mrp', 'cost', 'offers', 'discount'],
    }
    
    # Sub-page URL patterns
    SUBPAGE_PATTERNS = [
        re.compile(r'side.?effect', re.IGNORECASE),
        re.compile(r'dosage', re.IGNORECASE),
        re.compile(r'interaction', re.IGNORECASE),
        re.compile(r'precaution', re.IGNORECASE),
        re.compile(r'warning', re.IGNORECASE),
        re.compile(r'overdose', re.IGNORECASE),
        re.compile(r'pharma', re.IGNORECASE),
        re.compile(r'fda', re.IGNORECASE),
        re.compile(r'patient', re.IGNORECASE),
        re.compile(r'professional', re.IGNORECASE),
        re.compile(r'consumer', re.IGNORECASE),
        re.compile(r'monograph', re.IGNORECASE),
        re.compile(r'pediatric', re.IGNORECASE),
        re.compile(r'pregnancy', re.IGNORECASE),
        re.compile(r'clinical', re.IGNORECASE),
        re.compile(r'mechanism', re.IGNORECASE),
    ]

    # 1mg true drug detail URLs look like /drugs/<slug>-<numeric-id>
    ONE_MG_DETAIL_PATTERN = re.compile(r'/drugs/[a-z0-9-]+-\d+/?$', re.IGNORECASE)
    # MedlinePlus true drug detail URLs are under /druginfo/meds/
    MEDLINEPLUS_DETAIL_PATTERN = re.compile(r'/druginfo/meds/[a-z0-9_\-]+\.html$', re.IGNORECASE)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.jobdir = crawler.settings.get("JOBDIR")
        return spider
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Get settings - support both start_url (single) and urls (multiple)
        if 'start_url' in kwargs:
            self.start_urls = [kwargs['start_url']]
        elif 'urls' in kwargs:
            self.start_urls = kwargs.get('urls', '').split(',')
        else:
            self.start_urls = []
        
        # max_drugs=0 means unlimited
        self.max_drugs = int(kwargs.get('max_drugs', 0))
        self.follow_fda = kwargs.get('follow_fda', 'true').lower() == 'true'
        
        # SAFETY LIMITS - prevent infinite loops
        self.max_subpage_depth = int(kwargs.get('max_subpage_depth', 3))  # Max depth for sub-pages
        self.max_subpages_per_drug = int(kwargs.get('max_subpages', 20))  # Max sub-pages per drug
        self.max_retries_per_link = 3  # Max retries before skipping a link
        self.max_consecutive_empty = 3  # Max empty pages before backtracking
        
        # USER-CONFIGURABLE LINK PATTERNS (works for ANY site)
        # link_pattern: regex to match drug/product links (e.g., "/prescriptions/" for netmeds)
        # exclude_pattern: regex to exclude category pages (e.g., "/collection/|/sections/")
        # is_listing: force URL to be treated as listing page (true) or direct drug page (false)
        self.user_link_pattern = kwargs.get('link_pattern', '')
        self.user_exclude_pattern = kwargs.get('exclude_pattern', '')
        self.force_listing = kwargs.get('is_listing', '').lower()  # 'true', 'false', or '' (auto)
        
        # Compile user patterns if provided
        self.custom_link_regex = re.compile(self.user_link_pattern, re.IGNORECASE) if self.user_link_pattern else None
        self.custom_exclude_regex = re.compile(self.user_exclude_pattern, re.IGNORECASE) if self.user_exclude_pattern else None
        
        # State tracking
        self.drugs_extracted = 0
        self.completed_drug_items = 0
        self.current_drug = None  # Current drug being processed
        self.current_drug_data = {}  # Aggregated data for current drug
        self.pending_subpages = []  # Sub-pages to visit for current drug
        self.visited_subpages = set()  # Track visited sub-pages
        self.global_visited = set()  # Global visited URLs to prevent loops
        self.failed_urls = {}  # Track failed attempt counts per URL
        self.extracted_drug_urls = set()  # Persistent same-site dedupe cache
        self.extracted_urls_file = None
        self.extracted_urls_loaded = False
        self.resume_from_jobdir = False
        
        # Parallel extraction
        self.num_cores = int(kwargs.get('cores', 4))
        self.thread_pool = ThreadPoolExecutor(max_workers=self.num_cores)
        
        # Domain detection
        self.domain = None
        
        if self.user_link_pattern:
            self.logger.info(f" Custom link pattern: {self.user_link_pattern}")
        if self.user_exclude_pattern:
            self.logger.info(f" Custom exclude pattern: {self.user_exclude_pattern}")
        max_drugs_text = 'unlimited' if self.max_drugs <= 0 else str(self.max_drugs)
        self.logger.info(f" Drug Spider initialized - Max drugs: {max_drugs_text}, Max depth: {self.max_subpage_depth}, Max sub-pages: {self.max_subpages_per_drug}")

    def _normalize_drug_url(self, url: str) -> str:
        clean_url, _ = urldefrag((url or '').strip())
        parsed = urlparse(clean_url)
        path = parsed.path.rstrip('/')
        return f"{parsed.scheme}://{parsed.netloc}{path}" if parsed.scheme and parsed.netloc else clean_url

    def _init_extracted_urls_file(self):
        """Initialize per-site extracted URL archive path."""
        if self.extracted_urls_file is None:
            index_dir = Path("outputs") / self.name / "json"
            index_dir.mkdir(parents=True, exist_ok=True)
            site_key = "default"
            if self.start_urls:
                site_key = re.sub(r"[^a-z0-9]+", "_", urlparse(self.start_urls[0]).netloc.lower()).strip("_") or "default"
            self.extracted_urls_file = index_dir / f"_extracted_urls_{site_key}.txt"

    def _load_extracted_url_index(self):
        """Load same-site extracted URLs once so duplicates are skipped before requests are queued."""
        if self.extracted_urls_loaded:
            return

        self._init_extracted_urls_file()
        if self.extracted_urls_file.exists():
            try:
                with open(self.extracted_urls_file, "r", encoding="utf-8") as f:
                    for line in f:
                        url = line.strip()
                        if url:
                            self.extracted_drug_urls.add(url)
            except Exception as exc:
                self.logger.debug(f"Could not load extracted URL index: {exc}")

        self.extracted_urls_loaded = True
        self.logger.info(
            f"♻️ Loaded {len(self.extracted_drug_urls)} known drug URLs for site dedupe"
        )

    def _remember_extracted_url(self, url: str):
        """Record extracted URL for same-site duplicate prevention."""
        normalized = self._normalize_drug_url(url)
        if not normalized or normalized in self.extracted_drug_urls:
            return

        self.extracted_drug_urls.add(normalized)
        self._init_extracted_urls_file()
        try:
            with open(self.extracted_urls_file, "a", encoding="utf-8") as f:
                f.write(normalized + "\n")
        except Exception as exc:
            self.logger.debug(f"Could not persist extracted URL: {exc}")

    def _write_resume_progress(self, item_url: str, drug_name: str):
        """Write human-readable progress files into JOBDIR for resume observability."""
        if not getattr(self, "jobdir", None):
            return

        try:
            jobdir_path = Path(self.jobdir)
            jobdir_path.mkdir(parents=True, exist_ok=True)
            now_iso = datetime.now().isoformat()

            latest = {
                "timestamp": now_iso,
                "drug_no": self.completed_drug_items,
                "drug_name": drug_name,
                "url": item_url,
            }

            latest_file = jobdir_path / "latest_processed.json"
            with open(latest_file, "w", encoding="utf-8") as f:
                json.dump(latest, f, ensure_ascii=False, indent=2)

            log_file = jobdir_path / "processed_urls.log"
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(f"{self.completed_drug_items}\t{now_iso}\t{drug_name}\t{item_url}\n")
        except Exception as exc:
            self.logger.debug(f"Could not write JOBDIR progress files: {exc}")

    def _get_jobdir_resume_state(self):
        """Inspect JOBDIR to decide whether this run should resume from queued requests."""
        state = {
            "has_state": False,
            "has_pending_queue": False,
            "url_match": False,
            "latest_url": "",
            "latest_drug_name": "",
            "latest_drug_no": 0,
        }
        if not getattr(self, "jobdir", None) or not self.start_urls:
            return state

        try:
            jobdir_path = Path(self.jobdir)
            if not jobdir_path.exists():
                return state

            has_requests_seen = (jobdir_path / "requests.seen").exists()
            has_queue = (jobdir_path / "requests.queue").exists()
            has_spider_state = (jobdir_path / "spider.state").exists()
            state["has_state"] = has_requests_seen or has_queue or has_spider_state
            state["has_pending_queue"] = self._jobdir_has_pending_requests(jobdir_path)

            context_file = jobdir_path / "resume_context.json"
            if context_file.exists():
                with open(context_file, "r", encoding="utf-8") as f:
                    ctx = json.load(f)
                previous_url = str(ctx.get("start_url", "")).strip()
                state["url_match"] = previous_url == self.start_urls[0].strip()

            latest_file = jobdir_path / "latest_processed.json"
            if latest_file.exists():
                with open(latest_file, "r", encoding="utf-8") as f:
                    latest = json.load(f)
                state["latest_url"] = str(latest.get("url", "")).strip()
                state["latest_drug_name"] = str(latest.get("drug_name", "")).strip()
                state["latest_drug_no"] = int(latest.get("drug_no", 0) or 0)
        except Exception as exc:
            self.logger.debug(f"Could not inspect JOBDIR resume state: {exc}")

        return state

    def _jobdir_has_pending_requests(self, jobdir_path: Path) -> bool:
        """Return True only when JOBDIR contains actual pending queued requests."""
        queue_dir = jobdir_path / "requests.queue"
        if not queue_dir.exists():
            return False

        try:
            for file_path in queue_dir.rglob("*"):
                if not file_path.is_file():
                    continue
                if file_path.name == "active.json":
                    try:
                        raw = file_path.read_text(encoding="utf-8").strip()
                    except Exception:
                        raw = ""
                    if raw and raw not in {"{}", "[]"}:
                        return True
                    continue
                if file_path.stat().st_size > 0:
                    return True
        except Exception as exc:
            self.logger.debug(f"Could not inspect JOBDIR queue contents: {exc}")

        return False

    def _is_special_listing_url(self, url):
        """Site-specific listing roots that should never be treated as drug detail pages."""
        parsed = urlparse(url)
        path = parsed.path.lower().rstrip('/')
        return '1mg.com' in parsed.netloc.lower() and path.endswith('/drugs-all-medicines')

    def _is_unlimited_drugs(self):
        return self.max_drugs <= 0

    def _remaining_drug_quota(self):
        if self._is_unlimited_drugs():
            return None
        return max(0, self.max_drugs - self.drugs_extracted)

    def _reached_drug_limit(self):
        return (not self._is_unlimited_drugs()) and self.drugs_extracted >= self.max_drugs
    
    async def start(self):
        """Start from listing page OR direct drug page - HTTP first, Playwright fallback (Scrapy 2.13+)"""
        async for request in self._generate_start_requests():
            yield request
    
    def start_requests(self):
        """Fallback for Scrapy < 2.13 compatibility"""
        yield from self._generate_start_requests_sync()
    
    def _generate_start_requests_sync(self):
        """Synchronous start request generator"""
        self._init_extracted_urls_file()
        resume_state = self._get_jobdir_resume_state()
        if resume_state["has_state"] and resume_state["url_match"] and resume_state["has_pending_queue"]:
            self.resume_from_jobdir = True
            latest_url = resume_state["latest_url"] or "unknown"
            latest_name = resume_state["latest_drug_name"] or "unknown"
            latest_no = resume_state["latest_drug_no"]
            self.logger.info(
                f"♻️ Resuming from existing JOBDIR queue. Last completed drug #{latest_no}: {latest_name} | {latest_url}"
            )
            self.logger.info("♻️ Pending queued URLs will continue from saved state; seed listing URL will also be fetched as fallback")
        if resume_state["has_state"] and resume_state["url_match"] and not resume_state["has_pending_queue"]:
            self.logger.info("🆕 JOBDIR exists for this URL but has no pending queue; starting fresh from seed URL")

        self._load_extracted_url_index()

        for url in self.start_urls:
            if not url.strip():
                continue
            
            self.domain = urlparse(url).netloc
            
            # Determine if URL is a listing page or direct drug page
            # Priority: 1) User override (is_listing), 2) Custom pattern, 3) Built-in patterns
            if self._is_special_listing_url(url):
                is_drug_page = False
            elif self.force_listing == 'true':
                is_drug_page = False
            elif self.force_listing == 'false':
                is_drug_page = True
            elif self.custom_link_regex:
                # Use custom pattern if provided
                is_drug_page = bool(self.custom_link_regex.search(url))
            else:
                # Fall back to built-in patterns
                is_drug_page = False
                for pattern in self.DRUG_LINK_PATTERNS:
                    if pattern.search(url):
                        is_drug_page = True
                        break
            
            if is_drug_page:
                # Direct drug page - process it directly
                self.logger.info(f" Starting direct drug page extraction: {url}")
                self.current_drug = url
                self.current_drug_data = {}
                self.visited_subpages = set()
                self.pending_subpages = []
                
                yield scrapy.Request(
                    url,
                    callback=self.parse_drug_main,
                    meta={
                        'use_playwright': False,
                        'retry_with_playwright': True,
                        'is_main_page': True,
                        'depth': 0,
                    },
                    errback=self.handle_http_error,
                    # Seed URL must always be fetched even when JOBDIR has prior seen fingerprints.
                    dont_filter=True,
                )
            else:
                # Listing page - extract drug links
                self.logger.info(f" Starting drug listing extraction from: {url} (HTTP first)")
                
                yield scrapy.Request(
                    url,
                    callback=self.parse_listing,
                    meta={
                        'use_playwright': False,
                        'retry_with_playwright': True,
                    },
                    errback=self.handle_http_error,
                    # Seed URL must always be fetched even when JOBDIR has prior seen fingerprints.
                    dont_filter=True,
                )
    
    async def _generate_start_requests(self):
        """Async start request generator for Scrapy 2.13+"""
        for request in self._generate_start_requests_sync():
            yield request
    
    def parse_listing(self, response):
        """Parse drug listing page - UNIVERSAL drug link detection and robust pagination crawling"""
        self.logger.info(f" Parsing listing page: {response.url}")

        html = response.text
        selector = Selector(text=html)
        current_domain = urlparse(response.url).netloc

        # Find all drug links using UNIVERSAL detection
        drug_links = []
        for link in selector.css('a::attr(href)').getall():
            full_url = urljoin(response.url, link)
            clean_url, _ = urldefrag(full_url)
            if urlparse(clean_url).netloc != current_domain:
                continue
            if any(ext in clean_url.lower() for ext in ['.pdf', '.jpg', '.png', '.gif', '.css', '.js']):
                continue
            if self._is_category_page(clean_url):
                continue
            is_drug_link = False
            if self.custom_link_regex:
                if self.custom_link_regex.search(clean_url):
                    is_drug_link = True
            else:
                for pattern in self.DRUG_LINK_PATTERNS:
                    if pattern.search(clean_url):
                        is_drug_link = True
                        break
                if not is_drug_link:
                    link_text = selector.xpath(f'//a[@href="{link}"]/text()').get() or ''
                    if self._looks_like_drug_name(link_text):
                        is_drug_link = True
            if is_drug_link and clean_url not in drug_links:
                drug_links.append(clean_url)
        if self._is_special_listing_url(response.url):
            # For 1mg index pages, keep only strict /drugs/<slug>-<id> detail links.
            strict_links = []
            for link in selector.css('a::attr(href)').getall():
                full_url = urljoin(response.url, link)
                clean_url, _ = urldefrag(full_url)
                if self._is_1mg_drug_detail_url(clean_url) and clean_url not in strict_links:
                    strict_links.append(clean_url)
            drug_links = strict_links

        if not drug_links and not self._is_special_listing_url(response.url):
            drug_links = self._detect_drug_links_heuristic(selector, response.url)
        self.logger.info(f"🔗 Found {len(drug_links)} drug links on listing page")
        # Same-site dedupe: skip URLs already extracted earlier for this site.
        original_count = len(drug_links)
        drug_links = [u for u in drug_links if self._normalize_drug_url(u) not in self.extracted_drug_urls]
        skipped_existing = original_count - len(drug_links)
        if skipped_existing > 0:
            self.logger.info(f"♻️ Skipping {skipped_existing} already-extracted URLs for this site")

        remaining_quota = self._remaining_drug_quota()
        drugs_to_process = drug_links if remaining_quota is None else drug_links[:remaining_quota]
        listing_drill_links = []
        for drug_url in drugs_to_process:
            if self._reached_drug_limit():
                self.logger.info(f" Reached max drugs limit ({self.max_drugs})")
                return
            if not self._is_probable_drug_detail_url(drug_url):
                if self._is_listing_like_url(drug_url):
                    listing_drill_links.append(drug_url)
                    self.logger.info(f" Listing/master URL queued for drill-down: {drug_url}")
                else:
                    self.logger.info(f" Skipping non-drug URL: {drug_url}")
                continue
            self.logger.info(f" Queuing drug: {drug_url}")
            yield scrapy.Request(
                drug_url,
                callback=self.parse_drug_main,
                meta={
                    'use_playwright': False,
                    'retry_with_playwright': True,
                    'drug_url': drug_url,
                },
                errback=self.handle_http_error,
                priority=100,
                dont_filter=True,
            )

        for listing_url in listing_drill_links:
            if listing_url in self.global_visited:
                continue
            self.global_visited.add(listing_url)
            yield scrapy.Request(
                listing_url,
                callback=self.parse_listing,
                meta={
                    'use_playwright': False,
                    'retry_with_playwright': True,
                },
                errback=self.handle_http_error,
                priority=20,
                dont_filter=True,
            )

        # === PAGINATION CRAWLING: Always follow A-Z, Next, and pagination links ===
        pagination_links = set()
        # Common patterns: A-Z, Next, page numbers, etc.
        for a in selector.css('a'):
            text = (a.xpath('normalize-space(text())').get() or '').strip().lower()
            href = a.attrib.get('href', '')
            if not href:
                continue
            full_url = urljoin(response.url, href)
            clean_url, _ = urldefrag(full_url)
            if urlparse(clean_url).netloc != current_domain:
                continue
            # A-Z links (single letter or 'all')
            if (len(text) == 1 and text.isalpha()) or text in {'all', 'next', 'previous', 'prev', '>>', '>'}:
                pagination_links.add(clean_url)
            # Numeric pagination (e.g., 1, 2, 3, ...)
            if text.isdigit() or re.match(r'page \d+', text):
                pagination_links.add(clean_url)
            # Heuristic: 'next' in rel attribute
            rel = a.attrib.get('rel', '')
            if 'next' in rel:
                pagination_links.add(clean_url)
        # Remove self-link and already visited
        pagination_links.discard(response.url)

        # Some 1mg listing controls are rendered as href="#". Build real URLs explicitly.
        # IMPORTANT: Pass original_count (before dedup filtering) so pagination continues even if all drugs on this page were already extracted
        pagination_links.update(self._build_1mg_listing_links(response, original_count > 0))

        for page_url in pagination_links:
            if page_url not in self.global_visited:
                self.logger.info(f" Following pagination link: {page_url}")
                self.global_visited.add(page_url)
                yield scrapy.Request(
                    page_url,
                    callback=self.parse_listing,
                    meta={
                        'use_playwright': False,
                        'retry_with_playwright': True,
                    },
                    errback=self.handle_http_error,
                    priority=10,
                    dont_filter=True,
                )

    def _build_1mg_listing_links(self, response, has_drug_links_on_page):
        """Create synthetic listing URLs for 1mg A-Z and pagination when anchors use href="#".
        
        Args:
            has_drug_links_on_page: boolean indicating if this page had drug links (before dedup filtering).
                                   Used to continue pagination even if all drugs were already extracted.
        """
        parsed = urlparse(response.url)
        domain = parsed.netloc.lower()
        path = parsed.path.lower().rstrip('/')
        if '1mg.com' not in domain or not path.endswith('/drugs-all-medicines'):
            return set()

        links = set()
        query = parse_qs(parsed.query)
        current_label = (query.get('label', ['a'])[0] or 'a').lower()
        current_page = int((query.get('page', ['1'])[0] or '1'))

        # Seed label pages from the root listing page so crawl covers A-Z.
        if 'label' not in query and current_page == 1:
            for letter in 'abcdefghijklmnopqrstuvwxyz':
                if letter == 'a':
                    continue
                links.add(self._build_1mg_listing_url(parsed, letter, 1))

        # Follow next page for current label while this page had drug links (even if filtered as duplicates).
        # This ensures we discover new drugs on subsequent pages even when the current page only had duplicates.
        if has_drug_links_on_page:
            links.add(self._build_1mg_listing_url(parsed, current_label, current_page + 1))

        return links

    def _build_1mg_listing_url(self, parsed_url, label, page):
        """Build normalized listing URL for 1mg alphabet page."""
        base = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
        query = urlencode({'label': label.lower(), 'page': str(page)})
        return f"{base}?{query}"

    def _is_1mg_domain(self, url):
        return '1mg.com' in urlparse(url).netloc.lower()

    def _is_medlineplus_domain(self, url):
        return 'medlineplus.gov' in urlparse(url).netloc.lower()

    def _is_1mg_drug_detail_url(self, url):
        parsed = urlparse(url)
        if not self._is_1mg_domain(url):
            return False
        return bool(self.ONE_MG_DETAIL_PATTERN.search(parsed.path.lower()))

    def _is_medlineplus_drug_detail_url(self, url):
        parsed = urlparse(url)
        if not self._is_medlineplus_domain(url):
            return False
        return bool(self.MEDLINEPLUS_DETAIL_PATTERN.search(parsed.path.lower()))

    def _is_listing_like_url(self, url):
        """Return True for pages that should be parsed as listing pages for deeper links."""
        parsed = urlparse(url)
        path = parsed.path.lower().rstrip('/')

        if self._is_special_listing_url(url):
            return True

        if self._is_medlineplus_domain(url):
            # Only MedlinePlus drug-info index roots should drill down as listings.
            if self._is_medlineplus_drug_detail_url(url):
                return False
            if path in {'/druginformation', '/druginfo', '/druginfo/herb_all'}:
                return True
            return path.startswith('/druginfo')

        return self._is_category_page(url)

    def _is_probable_drug_detail_url(self, url):
        """Guardrail to avoid treating listing/navigation pages as drug detail pages."""
        parsed = urlparse(url)
        path = parsed.path.lower().rstrip('/')

        if self._is_1mg_domain(url):
            if self._is_special_listing_url(url):
                return False
            if path in {'/cart', '/offers', '/help', '/aboutus', '/contactus'}:
                return False
            return self._is_1mg_drug_detail_url(url)

        if self._is_medlineplus_domain(url):
            return self._is_medlineplus_drug_detail_url(url)

        if self._is_category_page(url):
            return False
        return True
    
    def parse_drug_main(self, response):
        """Parse main drug page - extract content and find sub-pages"""
        drug_url = response.meta.get('drug_url', response.url)
        normalized_drug_url = self._normalize_drug_url(drug_url)
        if normalized_drug_url in self.extracted_drug_urls:
            self.logger.info(f"♻️ Already extracted for this site, skipping: {drug_url}")
            return
        if not self._is_probable_drug_detail_url(drug_url):
            self.logger.info(f" Skipping non-drug detail page in parse_drug_main: {drug_url}")
            return
        self.logger.info(f" Parsing drug main page: {response.url}")
        
        # Add to global visited to prevent revisits
        self.global_visited.add(response.url)
        
        html = response.text
        selector = Selector(text=html)
        
        # Extract drug name from h1 or title
        drug_name = selector.xpath('normalize-space(//h1)').get() or ''
        if not drug_name:
            drug_name = selector.xpath('normalize-space(//title)').get() or ''
        drug_name = re.sub(r'\s*\|.*$', '', drug_name).strip()
        
        # Initialize drug data
        drug_data = {
            'drug_name': drug_name,
            'drug_url': drug_url,
            'main_content': {},
            'sub_pages': {},
            'fda_info': {},
        }
        
        # Extract main page content
        main_content = self._extract_page_content(html, response.url)
        drug_data['main_content'] = main_content
        
        # Find sub-page links (limit to max_subpages_per_drug)
        sub_links = self._find_drug_subpages(selector, response.url, drug_name)
        sub_links = sub_links[:self.max_subpages_per_drug]  # SAFETY LIMIT
        
        self.logger.info(f" Found {len(sub_links)} sub-pages for {drug_name} (max: {self.max_subpages_per_drug})")
        
        # Store state for aggregation
        response.meta['drug_data'] = drug_data
        response.meta['pending_subpages'] = sub_links.copy()
        response.meta['visited_subpages'] = {response.url}
        response.meta['current_depth'] = 1  # Start at depth 1
        response.meta['subpage_count'] = 0  # Track sub-pages visited
        
        if sub_links:
            # Visit first sub-page (HTTP first)
            next_url = sub_links.pop(0)
            response.meta['pending_subpages'] = sub_links
            
            yield scrapy.Request(
                next_url,
                callback=self.parse_drug_subpage,
                meta={
                    'use_playwright': False,
                    'retry_with_playwright': True,
                    'drug_data': drug_data,
                    'pending_subpages': sub_links,
                    'visited_subpages': response.meta['visited_subpages'],
                    'drug_url': drug_url,
                    'current_depth': 1,
                    'subpage_count': 1,
                },
                errback=self.handle_error,
                priority=90,
            )
        else:
            # No sub-pages, yield the drug item
            yield self._create_drug_item(drug_data, response.url)
    
    def parse_drug_subpage(self, response):
        """Parse drug sub-page and continue to next or yield final item"""
        drug_data = response.meta.get('drug_data', {})
        pending_subpages = response.meta.get('pending_subpages', [])
        visited_subpages = response.meta.get('visited_subpages', set())
        drug_url = response.meta.get('drug_url', '')
        current_depth = response.meta.get('current_depth', 1)
        subpage_count = response.meta.get('subpage_count', 0)
        consecutive_empty = response.meta.get('consecutive_empty', 0)
        
        # SAFETY: Add to global visited
        self.global_visited.add(response.url)
        
        # Mark as visited
        visited_subpages.add(response.url)
        subpage_count += 1
        
        # Determine sub-page type from URL
        subpage_type = self._get_subpage_type(response.url)
        self.logger.info(f" [{subpage_count}/{self.max_subpages_per_drug}] Parsing sub-page ({subpage_type}) depth={current_depth}: {response.url}")
        
        # Extract content
        content = self._extract_page_content(response.text, response.url)
        
        # CHECK: Is content empty/no data?
        has_data = self._has_meaningful_data(content)
        
        if not has_data:
            consecutive_empty += 1
            self.logger.warning(f" No data found on page ({consecutive_empty}/{self.max_consecutive_empty}): {response.url}")
            
            # If too many empty pages in a row, backtrack
            if consecutive_empty >= self.max_consecutive_empty:
                self.logger.warning(f"🔙 {self.max_consecutive_empty} empty pages - backtracking to try next link")
                # Skip deeper exploration, continue with next sibling
                for req in self._try_next_link(drug_data, pending_subpages, visited_subpages, drug_url, current_depth - 1, subpage_count, 0):
                    yield req
                return
        else:
            consecutive_empty = 0  # Reset on successful extraction
            
            # Check if it's FDA info
            if 'fda' in response.url.lower() or 'fda' in subpage_type.lower():
                drug_data['fda_info'].update(content)
                self.logger.info(f" FDA info extracted for: {drug_data.get('drug_name', 'Unknown')}")
            else:
                drug_data['sub_pages'][subpage_type] = content
        
        # SAFETY CHECK: Stop if depth or count limits reached
        if current_depth >= self.max_subpage_depth:
            self.logger.info(f"Max depth ({self.max_subpage_depth}) reached - backtracking")
            # Don't stop completely, try next sibling link
            for req in self._try_next_link(drug_data, pending_subpages, visited_subpages, drug_url, current_depth - 1, subpage_count, consecutive_empty):
                yield req
            return
        
        if subpage_count >= self.max_subpages_per_drug:
            self.logger.info(f"Max sub-pages ({self.max_subpages_per_drug}) reached - stopping sub-page crawl")
            self.drugs_extracted += 1
            self.logger.info(f" Drug {self.drugs_extracted} complete: {drug_data.get('drug_name', 'Unknown')} ({len(visited_subpages)} pages)")
            yield self._create_drug_item(drug_data, drug_url)
            return
        
        # Find additional sub-links on this page (only if not at max depth)
        if current_depth < self.max_subpage_depth - 1:
            selector = Selector(text=response.text)
            new_links = self._find_drug_subpages(selector, response.url, drug_data.get('drug_name', ''))
            
            # Add new links that haven't been visited (filter globally visited too)
            for link in new_links:
                if (link not in visited_subpages and 
                    link not in pending_subpages and 
                    link not in self.global_visited):
                    pending_subpages.append(link)
        
        # Continue to next sub-page or yield final item
        for req in self._try_next_link(drug_data, pending_subpages, visited_subpages, drug_url, current_depth, subpage_count, consecutive_empty):
            yield req
    
    def _try_next_link(self, drug_data, pending_subpages, visited_subpages, drug_url, current_depth, subpage_count, consecutive_empty):
        """Try next available link, backtracking if needed"""
        # Filter pending to remove visited and failed URLs
        pending_subpages = [
            url for url in pending_subpages 
            if url not in visited_subpages 
            and url not in self.global_visited
            and self.failed_urls.get(url, 0) < self.max_retries_per_link
        ]
        
        if pending_subpages:
            next_url = pending_subpages.pop(0)
            self.logger.info(f" Trying next link (HTTP): {next_url}")
            
            yield scrapy.Request(
                next_url,
                callback=self.parse_drug_subpage,
                meta={
                    'use_playwright': False,
                    'retry_with_playwright': True,
                    'drug_data': drug_data,
                    'pending_subpages': pending_subpages,
                    'visited_subpages': visited_subpages,
                    'drug_url': drug_url,
                    'current_depth': current_depth + 1,
                    'subpage_count': subpage_count,
                    'consecutive_empty': consecutive_empty,
                },
                errback=self._handle_subpage_error,
                priority=90,
                dont_filter=False,
            )
        else:
            # All sub-pages visited or failed, yield complete drug item
            self.drugs_extracted += 1
            self.logger.info(f" Drug {self.drugs_extracted} complete: {drug_data.get('drug_name', 'Unknown')} ({len(visited_subpages)} pages)")
            yield self._create_drug_item(drug_data, drug_url)
    
    def _has_meaningful_data(self, content):
        """Check if extracted content has meaningful data"""
        if not content:
            return False
        
        # Count non-empty string values
        text_content = ''
        for key, value in content.items():
            if isinstance(value, str):
                text_content += value
        
        # Need at least 100 chars of actual content
        return len(text_content.strip()) >= 100
    
    def _handle_subpage_error(self, failure):
        """Handle sub-page errors - try Playwright fallback first, then backtrack"""
        request = failure.request
        url = request.url
        meta = request.meta.copy()
        
        # First try Playwright fallback if not already used and Playwright is available
        if PLAYWRIGHT_AVAILABLE and meta.get('retry_with_playwright') and not meta.get('used_playwright'):
            self.logger.warning(f"HTTP failed for {url} - trying Playwright")
            
            meta['playwright'] = True
            meta['playwright_include_page'] = True
            meta['playwright_page_methods'] = [
                PageMethod('wait_for_load_state', 'networkidle'),
            ]
            meta['used_playwright'] = True
            meta['retry_with_playwright'] = False
            
            yield scrapy.Request(
                url,
                callback=self.parse_drug_subpage,
                meta=meta,
                errback=self._handle_subpage_error,
                priority=90,
                dont_filter=True,
            )
            return
        
        # Playwright also failed - backtrack
        self.failed_urls[url] = self.failed_urls.get(url, 0) + 1
        self.logger.warning(f"Both HTTP and Playwright failed for {url}")
        
        # Add to global visited to prevent retrying
        self.global_visited.add(url)
        
        # Try next link (backtrack)
        drug_data = meta.get('drug_data', {})
        pending_subpages = meta.get('pending_subpages', [])
        visited_subpages = meta.get('visited_subpages', set())
        drug_url = meta.get('drug_url', '')
        current_depth = meta.get('current_depth', 1)
        subpage_count = meta.get('subpage_count', 0)
        consecutive_empty = meta.get('consecutive_empty', 0)
        
        # Continue with next link
        for req in self._try_next_link(drug_data, pending_subpages, visited_subpages, drug_url, current_depth - 1, subpage_count, consecutive_empty):
            yield req
    
    def _find_drug_subpages(self, selector, current_url, drug_name):
        """Find sub-page links related to current drug - only actual sub-pages, not main page"""
        sub_links = []
        current_domain = urlparse(current_url).netloc
        
        # Get clean current URL for comparison
        current_clean, _ = urldefrag(current_url)
        
        # Get drug slug from URL for matching
        drug_slug = ''
        match = re.search(r'/([a-z0-9-]+)-drug', current_url, re.IGNORECASE)
        if match:
            drug_slug = match.group(1).lower()
        
        for link in selector.css('a::attr(href)').getall():
            full_url = urljoin(current_url, link)
            clean_url, _ = urldefrag(full_url)
            
            # SKIP if same as current URL (this was the bug!)
            if clean_url == current_clean:
                continue
            
            # Must be same domain
            if urlparse(clean_url).netloc != current_domain:
                continue
            
            # Skip external and media links
            if any(ext in clean_url.lower() for ext in ['.pdf', '.jpg', '.png', '.gif']):
                continue
            
            # Check if it's a sub-page - ONLY if it matches known sub-page patterns
            is_subpage = False
            
            # Check against UNIVERSAL sub-page patterns
            for pattern in self.SUBPAGE_PATTERNS:
                if pattern.search(clean_url):
                    # Also verify it's for this drug (if we have a slug)
                    if not drug_slug or drug_slug in clean_url.lower():
                        is_subpage = True
                        break
            
            # Check for FDA links
            if 'fda' in clean_url.lower():
                is_subpage = True
            
            if is_subpage and clean_url not in sub_links:
                sub_links.append(clean_url)
        
        return sub_links
    
    def _looks_like_drug_name(self, text):
        """Check if text looks like a drug name"""
        if not text or len(text) < 3 or len(text) > 100:
            return False
        
        text = text.strip()
        
        # Drug names are typically capitalized words with possible numbers/hyphens
        if re.match(r'^[A-Z][a-zA-Z0-9\-\s]+$', text):
            return True
        
        # Check for common drug suffixes
        drug_suffixes = ['-in', '-ol', '-ide', '-ate', '-ine', '-one', '-ase', '-mab', '-nib', '-vir']
        for suffix in drug_suffixes:
            if text.lower().endswith(suffix):
                return True
        
        return False
    
    def _is_category_page(self, url):
        """Check if URL is a category/listing page (not an individual drug page)"""
        # PRIORITY 1: Use custom exclude pattern if provided
        if self.custom_exclude_regex:
            return bool(self.custom_exclude_regex.search(url))
        
        # PRIORITY 2: Use built-in category patterns
        for pattern in self.CATEGORY_PAGE_PATTERNS:
            if pattern.search(url):
                return True
        return False
    
    def _detect_drug_links_heuristic(self, selector, base_url):
        """Heuristic detection for drug links when patterns don't match"""
        drug_links = []
        current_domain = urlparse(base_url).netloc
        
        # Look for links in common listing containers
        containers = selector.css('ul.drug-list, .drugs-list, .medicine-list, .drug-index, '
                                   'table.drug-list, #drug-list, .alphabetical-list, '
                                   '.content-list, main ul, article ul, '
                                   # Indian pharmacy site containers
                                   '.product-list, .medicine-grid, .product-grid, '
                                   '[class*="ProductCard"], [class*="drugsIndex"]')
        
        if containers:
            for link in containers.css('a::attr(href)').getall():
                full_url = urljoin(base_url, link)
                clean_url, _ = urldefrag(full_url)
                # Skip category pages
                if self._is_category_page(clean_url):
                    continue
                if urlparse(clean_url).netloc == current_domain and clean_url not in drug_links:
                    drug_links.append(clean_url)
        
        return drug_links[:100]  # Limit to 100
    
    def _get_subpage_type(self, url):
        """Determine sub-page type from URL - UNIVERSAL"""
        url_lower = url.lower()
        
        if 'side-effects' in url_lower:
            return 'side_effects'
        elif 'dosage' in url_lower:
            return 'dosage'
        elif 'interactions' in url_lower:
            return 'interactions'
        elif 'precautions' in url_lower:
            return 'precautions'
        elif 'warnings' in url_lower:
            return 'warnings'
        elif 'overdose' in url_lower:
            return 'overdose'
        elif 'clinical' in url_lower:
            return 'clinical_info'
        elif 'fda' in url_lower:
            return 'fda_info'
        elif 'patient' in url_lower:
            return 'patient_info'
        elif 'consumer' in url_lower:
            return 'consumer_info'
        elif 'professional' in url_lower:
            return 'professional_info'
        elif 'storage' in url_lower:
            return 'storage'
        else:
            return 'additional_info'
    
    def _extract_page_content(self, html, url):
        """UNIVERSAL content extraction - works on ANY drug website"""
        data = {}
        selector = Selector(text=html)
        
        # Always use universal section parser
        data = self._parse_universal_sections(selector, html, url)
        
        return data
    
    def _parse_universal_sections(self, selector, html, url):
        """UNIVERSAL drug page parser - extracts sections into standard columns"""
        data = {}
        
        # Basic meta info
        data['title'] = selector.xpath('normalize-space(//h1)').get() or \
                       selector.xpath('normalize-space(//title)').get() or ''
        data['description'] = selector.xpath('normalize-space(//meta[@name="description"]/@content)').get() or ''
        
        # Extract drug name from title
        title = data['title']
        drug_name_match = re.match(r'^([A-Za-z0-9\-]+)', title)
        if drug_name_match:
            data['drug_name_extracted'] = drug_name_match.group(1)
        
        # Find all headings and their content
        # Extended to include Indian pharmacy site elements (tabs, accordion headers, section divs)
        headings = selector.css('''
            h1, h2, h3, h4, h5,
            .section-title, .heading, [class*="title"], [class*="Title"],
            [class*="heading"], [class*="Heading"],
            [class*="tab-title"], [class*="TabTitle"],
            [class*="accordion-header"], [class*="AccordionHeader"],
            [role="tab"], [data-toggle="tab"],
            [class*="ProductDescription__heading"],
            [class*="product-description-heading"],
            [class*="DrugHeader"], [class*="drug-header"],
            [class*="SectionHeader"], [class*="section-header"],
            .nav-link, .tab-link
        ''')
        
        # Process headings in parallel using thread pool
        section_keywords = self.SECTION_KEYWORDS

        def _process_heading(heading):
            """Process a single heading - can run in parallel"""
            heading_text = heading.xpath('normalize-space()').get() or ''
            heading_lower = heading_text.lower()

            if len(heading_text) < 3:
                return None

            matched_column = None
            for col_name, keywords in section_keywords.items():
                for keyword in keywords:
                    if keyword in heading_lower:
                        matched_column = col_name
                        break
                if matched_column:
                    break

            if not matched_column:
                return None

            content_parts = []
            for sibling in heading.xpath('following-sibling::*'):
                tag = sibling.xpath('name()').get() or ''
                if tag.lower() in ['h1', 'h2', 'h3', 'h4']:
                    break
                if sibling.xpath('@class'):
                    classes = sibling.xpath('@class').get() or ''
                    if 'title' in classes.lower() or 'heading' in classes.lower():
                        break
                text = sibling.xpath('normalize-space()').get()
                if text and len(text) > 20:
                    content_parts.append(text)
                if len('\n'.join(content_parts)) > 10000:
                    break

            if content_parts:
                return (matched_column, '\n'.join(content_parts))
            return None

        heading_results = list(self.thread_pool.map(_process_heading, headings))
        for result in heading_results:
            if result:
                col, content = result
                if col not in data or len(data.get(col, '')) <= 100:
                    data[col] = content
        
        # Also try to extract from common section IDs/classes
        # Now includes Indian pharmacy sites (netmeds, 1mg, pharmeasy, apollo, medplus)
        section_selectors = {
            # Basic
            'uses': [
                '#uses', '#indications', '.uses', '.indications', '[data-section="uses"]',
                # Indian sites
                '[class*="ProductDescription"] [class*="uses"]',
                '[class*="product-description"] [class*="uses"]',
                '.drug-uses', '.medicine-uses', '[data-tab="uses"]',
                'div[class*="Uses"]', 'section[class*="uses"]',
            ],
            'description': [
                '#description', '#overview', '.description', '.overview',
                '[class*="Overview"]', '[class*="overview"]', '[data-section="overview"]',
                # Indian sites  
                '[class*="ProductDescription"]', '[class*="product-description"]',
                '.drug-description', '.medicine-description', '[data-tab="description"]',
                'div[class*="Description"]', '.product-details',
            ],
            'quick_tips': [
                '#quick-tips', '.quick-tips', '[class*="QuickTips"]', '[class*="quick-tips"]', '[data-section="quick-tips"]', '[data-tab="quick-tips"]',
            ],
            'fact_box': [
                '#fact-box', '.fact-box', '[class*="FactBox"]', '[class*="fact-box"]', '[data-section="fact-box"]', '[data-tab="fact-box"]',
            ],
            'patient_concerns': [
                '#patient-concerns', '.patient-concerns', '[class*="PatientConcerns"]', '[class*="patient-concerns"]', '[data-section="patient-concerns"]', '[data-tab="patient-concerns"]',
            ],
            'user_feedback': [
                '#user-feedback', '.user-feedback', '[class*="UserFeedback"]', '[class*="user-feedback"]', '[data-section="user-feedback"]', '[data-tab="user-feedback"]',
            ],
            'faqs': [
                '#faqs', '.faqs', '[class*="FAQs"]', '[class*="faqs"]', '[data-section="faqs"]', '[data-tab="faqs"]',
            ],
            'dosage': [
                '#dosage', '#dosing', '.dosage', '.dosing', '#administration',
                # Indian sites
                '[class*="dosage"]', '[class*="Dosage"]',
                '.drug-dosage', '.medicine-dosage', '[data-tab="dosage"]',
            ],
            
            # Safety
            'side_effects': [
                '#side-effects', '#adverse', '.side-effects', '.adverse-reactions', '#adverse-events',
                # Indian sites
                '[class*="SideEffects"]', '[class*="side-effects"]', '[class*="side_effects"]',
                '.drug-side-effects', '[data-tab="side-effects"]', '[data-tab="sideeffects"]',
                'div[class*="Adverse"]', 'section[class*="effect"]',
            ],
            'warnings': [
                '#warnings', '#boxed-warning', '.warnings', '.black-box', '#important-safety',
                # Indian sites
                '[class*="Warning"]', '[class*="warning"]', '[data-tab="warnings"]',
                '.drug-warnings', '.safety-info',
            ],
            'precautions': [
                '#precautions', '.precautions', '#before-taking',
                '[class*="Precaution"]', '[class*="precaution"]', '[data-tab="precautions"]',
            ],
            'contraindications': [
                '#contraindications', '.contraindications', '#do-not-use',
                '[class*="Contraindication"]', '[class*="contraindication"]',
            ],
            'drug_interactions': [
                '#interactions', '#drug-interactions', '.interactions', '#drug-drug-interactions',
                # Indian sites
                '[class*="Interaction"]', '[class*="interaction"]', '[data-tab="interactions"]',
                '.drug-interactions',
            ],
            
            # Pharmacokinetics (ADME)
            'pharmacokinetics': ['#pharmacokinetics', '.pharmacokinetics', '#pk', '#adme'],
            'absorption': ['#absorption', '.absorption', '#bioavailability'],
            'distribution': ['#distribution', '.distribution', '#protein-binding'],
            'metabolism': ['#metabolism', '.metabolism', '#hepatic-metabolism', '#cyp450'],
            'elimination': ['#elimination', '#excretion', '.elimination', '#half-life', '#clearance'],
            
            # Pharmacodynamics
            'pharmacodynamics': ['#pharmacodynamics', '.pharmacodynamics', '#pd'],
            'mechanism_of_action': [
                '#mechanism', '#mechanism-of-action', '.mechanism', '#moa', '#how-it-works',
                '[class*="Mechanism"]', '[class*="mechanism"]', '[data-tab="mechanism"]',
            ],
            'clinical_pharmacology': ['#clinical-pharmacology', '.clinical-pharmacology'],
            
            # Special Populations
            'pregnancy': [
                '#pregnancy', '.pregnancy', '#pregnant', '#pregnancy-category',
                '[class*="Pregnancy"]', '[class*="pregnancy"]', '[data-tab="pregnancy"]',
            ],
            'lactation': ['#lactation', '#breastfeeding', '.lactation', '#nursing'],
            'pediatric': ['#pediatric', '#children', '.pediatric-use', '#pediatric-use'],
            'pediatric_dosage': ['#pediatric-dosage', '#pediatric-dose', '.pediatric-dosing'],
            'geriatric': ['#geriatric', '#elderly', '.geriatric-use', '#geriatric-use'],
            'geriatric_dosage': ['#geriatric-dosage', '#geriatric-dose'],
            
            # Dosing by population
            'adult_dosage': ['#adult-dosage', '#adult-dose', '.adult-dosing', '#usual-adult-dose'],
            'renal_dosing': ['#renal-dosing', '#renal-impairment', '.renal-adjustment'],
            'hepatic_dosing': ['#hepatic-dosing', '#hepatic-impairment', '.hepatic-adjustment'],
            
            # Overdose
            'overdose': [
                '#overdose', '#overdosage', '.overdose', '#toxicity',
                '[class*="Overdose"]', '[class*="overdose"]',
            ],
            'overdose_symptoms': ['#overdose-symptoms', '#signs-of-overdose'],
            'overdose_treatment': ['#overdose-treatment', '#antidote', '#management-of-overdose'],
            
            # Formulation
            'how_supplied': [
                '#how-supplied', '.how-supplied', '#available-forms', '#formulations',
                '[class*="HowSupplied"]', '[class*="available-forms"]',
            ],
            'ingredients': [
                '#ingredients', '.ingredients', '#composition', '#active-ingredients',
                '[class*="Ingredient"]', '[class*="ingredient"]', '[class*="Composition"]',
                '[data-tab="composition"]', '.salt-composition', '.active-ingredient',
            ],
            'storage': [
                '#storage', '.storage', '#how-to-store',
                '[class*="Storage"]', '[class*="storage"]',
            ],
            
            # Regulatory
            'fda_info': ['#fda', '#fda-approval', '.fda-info', '#prescribing-information'],
            'dea_schedule': ['#dea', '#controlled-substance', '#schedule'],
            'patient_info': ['#patient-information', '.patient-info', '#medication-guide'],
            
            # Clinical
            'clinical_trials': ['#clinical-trials', '#studies', '#clinical-studies'],
            
            # === INDIAN PHARMACY SITE SPECIFIC ===
            # Netmeds.com
            'salt_composition': [
                '.salt-composition', '[class*="SaltComposition"]', '[class*="salt-composition"]',
                '[data-tab="salt"]', '.product-salt', '.medicine-salt',
            ],
            'substitute_medicines': [
                '[class*="Substitute"]', '[class*="substitute"]', '[data-tab="substitutes"]',
                '.drug-substitutes', '.alternative-medicines',
            ],
            'manufacturer': [
                '[class*="Manufacturer"]', '[class*="manufacturer"]',
                '.drug-manufacturer', '.company-name', '.marketed-by',
            ],
            'price': [
                '[class*="Price"]', '[class*="price"]', '.drug-price', '.medicine-price',
                '.final-price', '.selling-price', '[data-price]',
            ],
        }
        
        # Process section selectors in parallel for columns not yet found
        def _process_selector_group(item):
            col_name, sels = item
            for sel in sels:
                section = selector.css(sel)
                if section:
                    text = section.xpath('normalize-space()').get()
                    if text and len(text) > 50:
                        return (col_name, text)
            return None

        items_to_check = [(col, sels) for col, sels in section_selectors.items() if col not in data]
        selector_results = list(self.thread_pool.map(_process_selector_group, items_to_check))
        for result in selector_results:
            if result:
                data[result[0]] = result[1]
        
        return data
    
    def _parse_rxlist_sections(self, selector, html):
        """Parse RxList drug page sections into organized columns"""
        data = {}
        
        # Meta info
        data['title'] = selector.xpath('normalize-space(//h1)').get() or ''
        data['description'] = selector.xpath('normalize-space(//meta[@name="description"]/@content)').get() or ''
        
        # RxList uses h2 headings with pattern "Section for DrugName"
        section_keywords = {
            'description': ['Description for', 'Drug Description'],
            'uses': ['Uses for', 'Indications'],
            'dosage': ['Dosage for', 'Dosage and Administration'],
            'side_effects': ['Side Effects for', 'Adverse Reactions'],
            'warnings': ['Warnings for', 'Warning'],
            'precautions': ['Precautions for', 'Precaution'],
            'drug_interactions': ['Drug Interactions for', 'Interactions'],
            'overdose': ['Overdosage for', 'Overdose'],
            'clinical_pharmacology': ['Clinical Pharmacology', 'Pharmacology'],
            'contraindications': ['Contraindications for', 'Contraindication'],
            'how_supplied': ['HOW SUPPLIED', 'How Supplied'],
            'patient_info': ['Patient Information', 'Medication Guide'],
        }
        
        # Find all h2 headings and extract their content
        for heading in selector.css('h2'):
            heading_text = heading.xpath('normalize-space()').get() or ''
            
            for col_name, keywords in section_keywords.items():
                if col_name in data:
                    continue  # Already found
                
                for keyword in keywords:
                    if keyword.lower() in heading_text.lower():
                        # Found the section - extract content until next h2
                        content_parts = []
                        for sibling in heading.xpath('following-sibling::*'):
                            tag = sibling.xpath('name()').get()
                            if tag == 'h2':
                                break  # Stop at next section
                            
                            text = sibling.xpath('normalize-space()').get()
                            if text and len(text) > 20:
                                content_parts.append(text)
                        
                        if content_parts:
                            data[col_name] = '\n'.join(content_parts)
                        break
        
        # Also extract summary from drug summary section
        summary_section = selector.css('.drug-summary, .drugSummary, #drugSummary')
        if summary_section:
            summary_text = summary_section.xpath('normalize-space()').get()
            if summary_text and len(summary_text) > 50:
                data['summary'] = summary_text
        
        # Get FDA info section
        fda_section = selector.xpath("//h2[contains(text(), 'FDA')]/following-sibling::*[position() < 20]")
        if fda_section:
            fda_parts = []
            for elem in fda_section:
                text = elem.xpath('normalize-space()').get()
                if text and len(text) > 20:
                    fda_parts.append(text)
                if elem.xpath('self::h2'):
                    break
            if fda_parts:
                data['fda_info'] = '\n'.join(fda_parts)
        
        return data
    
    def _extract_section_content(self, selector, section_selectors):
        """Extract content from section by ID or class"""
        for sel in section_selectors:
            if sel.startswith('#'):
                section = selector.css(f'{sel}, [id="{sel[1:]}"]')
            else:
                section = selector.css(sel)
            
            if section:
                text_parts = []
                for elem in section.css('p, li, dd, span.content'):
                    text = elem.xpath('normalize-space()').get()
                    if text and len(text) > 10:
                        text_parts.append(text)
                
                if text_parts:
                    return '\n'.join(text_parts)
                
                all_text = section.xpath('normalize-space()').get()
                if all_text and len(all_text) > 50:
                    return all_text
        
        return ''
    
    def _create_drug_item(self, drug_data, url):
        """Create final aggregated drug item with standardized CSV columns"""
        # 1. Initialize strictly defined columns to ensure unified CSV structure
        flat_data = {
            'drug_name': drug_data.get('drug_name', ''),
            'drug_url': drug_data.get('drug_url', url),
            'other_info': '',
            'fda_summary': '',
            'full_content': ''
        }
        
        # Initialize all known sections
        for col_name in self.SECTION_KEYWORDS.keys():
            flat_data[col_name] = ''
            
        def append_data(col, content):
            if not content:
                return
            if isinstance(content, (dict, list)):
                content = json.dumps(content, ensure_ascii=False)
            content = str(content).strip()
            
            if col in flat_data and col not in ['drug_name', 'drug_url', 'other_info', 'fda_summary', 'full_content']:
                if flat_data[col]:
                    flat_data[col] += f"\n\n---\n\n{content}"  # Combine sources
                else:
                    flat_data[col] = content
            else:
                prefix = f"[{col.upper()}]:\n" if col else ""
                if flat_data['other_info']:
                    flat_data['other_info'] += f"\n\n---\n\n{prefix}{content}"
                else:
                    flat_data['other_info'] = f"{prefix}{content}"

        # 2. Map Main Page Content
        for key, value in drug_data.get('main_content', {}).items():
            append_data(key, value)
            
        # 3. Map Sub-Page Content
        for page_type, content in drug_data.get('sub_pages', {}).items():
            if isinstance(content, dict):
                for key, value in content.items():
                    append_data(key, value)
            else:
                append_data(page_type, content)
                
        # 4. Map FDA Content
        fda_info = drug_data.get('fda_info', {})
        if fda_info:
            for key, value in fda_info.items():
                append_data(key, value)
                
            # Create FDA summary specifically
            fda_summary_parts = []
            for key in ['warnings', 'contraindications', 'adverse_reactions', 'drug_interactions', 'side_effects']:
                if key in fda_info and isinstance(fda_info[key], str):
                    fda_summary_parts.append(f"**{key.upper()}**: {fda_info[key][:500]}...")
            if fda_summary_parts:
                flat_data['fda_summary'] = '\n\n'.join(fda_summary_parts)
                
        # 5. Create Full Summary
        content_parts = []
        for key in ['description', 'overview', 'uses', 'side_effects', 'dosage', 'warnings', 'quick_tips', 'fact_box', 'patient_concerns', 'user_feedback', 'faqs']:
            if flat_data.get(key):
                content_parts.append(f"## {key.replace('_', ' ').title()}\n{flat_data[key]}")
        flat_data['full_content'] = '\n\n'.join(content_parts) if content_parts else ''
        
        # 6. Remove completely empty strict columns to declutter CSV a bit 
        # (Though keeping them is also fine, removing ensures we still drop completely useless columns)
        empty_keys = [k for k, v in flat_data.items() if not v and k not in ['drug_name', 'drug_url']]
        for k in empty_keys:
            del flat_data[k]
        
        # Build item
        item = ScrapedItem()
        item['url'] = url
        item['domain'] = urlparse(url).netloc
        item['container_type'] = 'drug_complete'
        # item['confidence'] = 0.95
        item['scraped_at'] = datetime.now().isoformat()
        # item['layout_hash'] = hashlib.md5(url.encode()).hexdigest()
        # item['repair_count'] = 0
        item['data'] = flat_data

        # Persist latest completed item so interrupted runs can be inspected quickly.
        self.completed_drug_items += 1
        self._write_resume_progress(
            item_url=flat_data.get('drug_url', url),
            drug_name=flat_data.get('drug_name', ''),
        )
        self._remember_extracted_url(flat_data.get('drug_url', url))
        
        return item
    
    def handle_error(self, failure):
        """Handle request errors with backtracking"""
        request = failure.request
        url = request.url
        
        # Track failures
        self.failed_urls[url] = self.failed_urls.get(url, 0) + 1
        self.logger.error(f" Error on {url} (attempt {self.failed_urls[url]}/{self.max_retries_per_link}): {failure.value}")
        
        # If max retries reached, add to global visited
        if self.failed_urls[url] >= self.max_retries_per_link:
            self.global_visited.add(url)
            self.logger.warning(f"🔙 Skipping {url} after {self.max_retries_per_link} failures")
    
    def handle_http_error(self, failure):
        """Handle HTTP error - fallback to Playwright"""
        request = failure.request
        url = request.url
        meta = request.meta.copy()
        
        # Check if we should retry with Playwright (only if Playwright is available)
        if PLAYWRIGHT_AVAILABLE and meta.get('retry_with_playwright') and not meta.get('used_playwright'):
            self.logger.warning(f"HTTP failed for {url} - retrying with Playwright")
            
            # Mark as using playwright
            meta['playwright'] = True
            meta['playwright_include_page'] = True
            meta['playwright_page_methods'] = [
                PageMethod('wait_for_load_state', 'networkidle'),
            ]
            meta['used_playwright'] = True
            meta['retry_with_playwright'] = False
            
            yield scrapy.Request(
                url,
                callback=request.callback,
                meta=meta,
                errback=self.handle_error,
                priority=request.priority,
                dont_filter=True,
            )
        else:
            # Track as failed
            self.failed_urls[url] = self.failed_urls.get(url, 0) + 1
            self.logger.error(f" Error on {url}: {failure.value}")
            
            if self.failed_urls[url] >= self.max_retries_per_link:
                self.global_visited.add(url)

    def closed(self, reason):
        """Cleanup when spider closes"""
        if hasattr(self, 'thread_pool'):
            self.thread_pool.shutdown(wait=False)
        self.logger.info(f"🔬 Drug Spider closed: {reason}")
        self.logger.info(f"   Drugs extracted: {self.drugs_extracted}")
        self.logger.info(f"   Parallel cores: {self.num_cores}")
