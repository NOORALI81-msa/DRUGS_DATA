# geometric_crawler/spiders/geometric_spider.py - ENHANCED VERSION
# Features: Selectolax fast parsing, multi-threading, better error handling,
# site-specific extraction, improved timeout handling

import scrapy
import hashlib
import json
import time
import asyncio
import re
from parsel import Selector
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin
from ..geometry import GeometricExtractor
from ..repair import RepairEngine
from ..items import ScrapedItem
from ..config import Config

# Optional Playwright support
try:
    from scrapy_playwright.page import PageMethod
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    PageMethod = None

# Import fast parser (selectolax-based)
try:
    from ..fast_parser import FastParser, fast_parser
    SELECTOLAX_AVAILABLE = True
except ImportError:
    SELECTOLAX_AVAILABLE = False
    fast_parser = None

class GeometricSpider(scrapy.Spider):
    """
    Universal spider with:
    - Selectolax fast extraction (10-20x faster)
    - Multi-threaded parallel crawling
    - All protection techniques
    - Geometric extraction
    - Self-healing repair
    - Site-specific optimizations
    """
    name = "geometric"
    
    # Site-specific timeout settings (some sites are slower)
    SITE_TIMEOUTS = {
        '1mg.com': 30000,
        'amazon': 45000,
        'zomato.com': 30000,
        'medlineplus.gov': 20000,
        'rxlist.com': 25000,
        'drugs.com': 30000,
    }
    
    # Site-specific rate limits (requests per second)
    SITE_RATE_LIMITS = {
        '1mg.com': 0.5,     # 1 request per 2 seconds
        'amazon': 1.0,       # 1 request per second
        'zomato.com': 0.5,
        'medlineplus.gov': 2.0,  # More lenient
        'rxlist.com': 1.0,
        'drugs.com': 0.5,
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Initialize follow_patterns early (needed by config loader)
        self.follow_patterns_raw = kwargs.get('follow_patterns', '').strip()
        self.follow_patterns = []
        
        # Load config file if provided
        config_file = kwargs.get('config', '')
        self.spider_config = None
        
        if config_file:
            self._load_config_file(config_file)
        
        # Get settings from command line (override config file)
        self.start_urls = kwargs.get('urls', '').split(',') if 'urls' in kwargs else []
        self.max_pages = int(kwargs.get('max_pages', Config.MAX_PAGES))
        self.max_depth = int(kwargs.get('max_depth', Config.MAX_DEPTH))
        self.use_llm = kwargs.get('use_llm', str(Config.ENABLE_LLM_REPAIR).lower()) == 'true'
        
        # Apply config file settings if loaded and not overridden
        if self.spider_config:
            if not self.start_urls or self.start_urls == ['']:
                self.start_urls = self.spider_config.get('start_urls', [])
            if 'max_pages' not in kwargs:
                self.max_pages = self.spider_config.get('max_pages', self.max_pages)
            if 'max_depth' not in kwargs:
                self.max_depth = self.spider_config.get('max_depth', self.max_depth)
        
        # LLM provider settings
        self.llm_provider = kwargs.get('llm_provider', 'ollama')
        self.llm_model = kwargs.get('llm_model', '')
        self.llm_api_key = kwargs.get('llm_api_key', '')
        
        self.use_http_after_first = kwargs.get('use_http_after_first', 'true').lower() == 'true'
        self.http_only = kwargs.get('http_only', 'false').lower() == 'true'
        
        # Parse follow patterns from command line (in addition to any from config)
        if self.follow_patterns_raw:
            for pattern in self.follow_patterns_raw.split(','):
                cleaned = pattern.strip()
                if not cleaned:
                    continue
                try:
                    self.follow_patterns.append((cleaned, re.compile(cleaned)))
                except re.error:
                    self.follow_patterns.append((cleaned, None))
        
        # Enhanced parallel processing settings
        self.num_cores = int(kwargs.get('cores', 4))
        self.use_fast_parser = kwargs.get('use_fast_parser', 'true').lower() == 'true'
        
        # Output format - controls which pipelines are active (json, csv, both)
        self.output_format = kwargs.get('output_format', 'json').lower()
        
        # Print debug info
        print(f" DEBUG - Received URLs: {self.start_urls}")
        print(f" DEBUG - Max pages: {self.max_pages}, Max depth: {self.max_depth}")
        print(f" DEBUG - Cores: {self.num_cores}, Fast parser: {self.use_fast_parser and SELECTOLAX_AVAILABLE}")
        if self.http_only:
            print(" DEBUG - HTTP only mode: enabled (no Playwright)")
        elif self.use_http_after_first:
            print(" DEBUG - HTTP after first layout: enabled")
        if self.follow_patterns_raw:
            print(f" DEBUG - Follow patterns: {self.follow_patterns_raw}")
        if self.use_llm:
            print(f" LLM Repair enabled - Provider: {self.llm_provider}, Model: {self.llm_model or 'default'}")
        
        # Initialize engines
        self.geometry = GeometricExtractor()
        self.repair = RepairEngine(
            use_llm=self.use_llm,
            llm_provider=self.llm_provider,
            llm_model=self.llm_model,
            api_key=self.llm_api_key
        )
        
        # Enhanced parallel processing with configurable cores
        self.thread_pool = ThreadPoolExecutor(max_workers=self.num_cores)
        self.parallel_enabled = Config.PARALLEL_EXTRACTION
        
        # Tracking
        self.visited_urls = set()
        self.pages_crawled = 0
        self.items_extracted = 0
        self.domain = None
        self.failed_domains = set()  # Track domains with errors
        self.retry_counts = {}  # Track retries per URL
        
        # Fast parser (selectolax)
        self.fast_parser = fast_parser if SELECTOLAX_AVAILABLE and self.use_fast_parser else None
        self.patterns_learned = {}  # domain -> True once patterns are learned
        
        # Cache
        self.layout_cache = {}
        self.extraction_cache = {}
        self.known_layout_hashes = set()
        
        # Error tracking for adaptive behavior
        self.timeout_count = 0
        self.playwright_errors = 0
        self.throttle_count = 0
        
        # Fields we want to extract
        self.extraction_fields = getattr(Config, 'EXTRACTION_FIELDS', {
            'title': {
                'description': 'Page or product title',
                'keywords': ['title', 'name', 'product', 'heading']
            },
            'price': {
                'description': 'Price',
                'keywords': ['price', 'cost', 'mrp', '₹', '$', '£']
            },
            'description': {
                'description': 'Description',
                'keywords': ['description', 'about', 'details']
            },
            'manufacturer': {
                'description': 'Manufacturer/Brand',
                'keywords': ['manufacturer', 'brand', 'company', 'by']
            }
        })
        
        if self.start_urls and self.start_urls[0]:
            self.domain = urlparse(self.start_urls[0]).netloc
            self.logger.info(f" Starting geometric crawl of {self.domain}")
            self.logger.info(f" Parallel extraction: {'ON' if self.parallel_enabled else 'OFF'}")
            self.logger.info(f" Fast parser (selectolax): {'ON' if self.fast_parser else 'OFF'}")
            self.logger.info(f" Cores: {self.num_cores}")
            if self.spider_config:
                self.logger.info(f" Using config: {self.spider_config.get('name', 'custom')}")
    
    def _load_config_file(self, config_path: str):
        """Load spider configuration from JSON file"""
        import os
        from pathlib import Path
        
        # Handle relative paths
        if not os.path.isabs(config_path):
            # Try relative to current dir and project root
            candidates = [
                config_path,
                Path(__file__).resolve().parents[2] / config_path,
            ]
        else:
            candidates = [config_path]
        
        for path in candidates:
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    self.spider_config = json.load(f)
                    self.logger.info(f" Loaded config from: {path}")
                    
                    # Apply config to spider
                    config = self.spider_config
                    
                    # Set start URLs if not already set
                    if 'start_urls' in config:
                        self.start_urls = config['start_urls']
                    elif 'base_url' in config:
                        self.start_urls = [config['base_url']]
                    
                    # Set follow patterns from config
                    if 'follow_patterns' in config and config['follow_patterns']:
                        self.follow_patterns_raw = ','.join(config['follow_patterns'])
                        for pattern in config['follow_patterns']:
                            try:
                                self.follow_patterns.append((pattern, re.compile(pattern)))
                            except re.error:
                                self.follow_patterns.append((pattern, None))
                    
                    # Set extraction fields from config
                    if 'fields' in config:
                        self.extraction_fields = {}
                        for field_name, field_config in config['fields'].items():
                            self.extraction_fields[field_name] = {
                                'description': field_config.get('description', field_name),
                                'keywords': [field_name],
                                'selectors': field_config.get('selectors', []),
                                'type': field_config.get('type', 'text'),
                                'required': field_config.get('required', False),
                            }
                    
                    # Other settings
                    if 'max_pages' in config:
                        self.max_pages = config['max_pages']
                    if 'max_depth' in config:
                        self.max_depth = config['max_depth']
                    if 'delay' in config:
                        # Will be used for rate limiting
                        self.config_delay = config['delay']
                    
                    return
            except FileNotFoundError:
                continue
            except json.JSONDecodeError as e:
                self.logger.error(f" Invalid JSON in config file: {e}")
                return
            except Exception as e:
                self.logger.error(f" Error loading config: {e}")
                return
        
        self.logger.warning(f" Config file not found: {config_path}")

    async def start(self):
        """Start with HTTP first, use Playwright as fallback for JS-heavy sites (Scrapy 2.13+)"""
        for url in self.start_urls:
            if url and url.strip():
                self.logger.info(f" Creating request for: {url}")
                # Always try HTTP first (faster), Playwright will be fallback if needed
                yield self.create_http_request(url.strip(), is_initial=True)
    
    def start_requests(self):
        """Fallback for Scrapy < 2.13 compatibility"""
        for url in self.start_urls:
            if url and url.strip():
                self.logger.info(f" Creating request for: {url}")
                yield self.create_http_request(url.strip(), is_initial=True)

    def create_request(self, url, callback=None, depth=0, force_playwright=False):
        """Create request - HTTP first with Playwright as fallback for JS-heavy sites."""
        # HTTP-only mode - never use Playwright
        if self.http_only:
            return self.create_http_request(url, callback=callback, depth=depth)
            
        # Force Playwright if explicitly requested
        if force_playwright:
            return self.create_playwright_request(url, callback=callback, depth=depth)

        # Default: Try HTTP first, Playwright fallback happens in parse_page
        return self.create_http_request(url, callback=callback, depth=depth)
    
    def create_playwright_request(self, url, callback=None, depth=0):
        """Create request with Playwright and all protections + stealth mode + adaptive timeouts"""
        # If Playwright is not available, fall back to HTTP
        if not PLAYWRIGHT_AVAILABLE:
            self.logger.warning(f"⚠️ Playwright not available, using HTTP for: {url}")
            return self.create_http_request(url, callback=callback, depth=depth)
        
        import random
        
        # Get site-specific timeout
        timeout = 45000  # default
        for site_key, site_timeout in self.SITE_TIMEOUTS.items():
            if site_key in url:
                timeout = site_timeout
                break
        
        # Increase timeout if we've had many failures
        if self.timeout_count > 3:
            timeout = min(timeout * 1.5, 90000)
        
        # Stealth script to bypass bot detection
        stealth_script = """
        // Override navigator properties
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        
        // Override chrome property
        window.chrome = { runtime: {} };
        
        // Override permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );
        """
        
        meta = {
            'playwright': True,
            'playwright_include_page': True,
            'playwright_context_kwargs': {
                'viewport': {'width': random.choice([1920, 1366, 1536, 1440]), 'height': random.choice([1080, 768, 864, 900])},
                'user_agent': random.choice(Config.USER_AGENTS),
                'locale': 'en-US',
                'timezone_id': random.choice(['America/New_York', 'America/Los_Angeles', 'America/Chicago', 'Europe/London']),
                'permissions': ['geolocation'],
                'java_script_enabled': True,
            },
            'playwright_page_goto_kwargs': {
                'wait_until': 'domcontentloaded',
                'timeout': timeout,
            },
            'playwright_page_methods': [
                # Inject stealth script before page loads
                PageMethod('add_init_script', stealth_script),
                # Use domcontentloaded instead of networkidle for fast-loading sites
                PageMethod('wait_for_load_state', 'domcontentloaded'),
                PageMethod('wait_for_timeout', 2000),  # Reduced from 3s to 2s
                PageMethod('evaluate', 'window.scrollTo(0, document.body.scrollHeight)'),
                PageMethod('wait_for_timeout', 500),  # Reduced from 1s to 0.5s
            ],
            'depth': depth,
            'crawl_time': time.time(),
            'download_timeout': timeout / 1000,  # Scrapy uses seconds
        }
        
        # Add resource blocking if enabled
        block_resources = []
        if Config.BLOCK_IMAGES:
            block_resources.append('image')
        if Config.BLOCK_FONTS:
            block_resources.append('font')
        if Config.BLOCK_CSS:
            block_resources.append('stylesheet')
        
        if block_resources:
            meta['playwright_page_methods'].insert(1,  # After stealth script
                PageMethod('route', '**/*', 
                    lambda route: route.abort() 
                    if route.request.resource_type in block_resources 
                    else route.continue_()
                )
            )
        
        return scrapy.Request(
            url=url,
            callback=callback or self.parse_page,
            errback=self.handle_error,
            meta=meta
        )

    def create_http_request(self, url, callback=None, depth=0, is_initial=False):
        """Create request without Playwright (fast path)."""
        meta = {
            'playwright': False,
            'depth': depth,
            'crawl_time': time.time(),
            'is_initial': is_initial,  # Track if this is first request (for fallback)
            'tried_playwright': False,  # Track if we already tried Playwright
        }
        return scrapy.Request(
            url=url,
            callback=callback or self.parse_page,
            errback=self.handle_error,
            meta=meta
        )
    
    def _needs_javascript(self, html):
        """
        Detect if page needs JavaScript rendering.
        Returns True if content appears to be JS-heavy/empty.
        """
        from parsel import Selector
        sel = Selector(text=html)
        
        # Check for common JS-rendered page indicators
        body_text = sel.xpath('//body//text()').getall()
        visible_text = ' '.join(t.strip() for t in body_text if t.strip())
        
        # Minimum text threshold (JS pages often have very little visible text)
        if len(visible_text) < 500:
            return True
        
        # Check for common "loading" indicators
        loading_indicators = [
            'loading...', 'please wait', 'javascript required',
            'enable javascript', 'noscript', '__NEXT_DATA__',
            'window.__INITIAL_STATE__', 'window.__PRELOADED_STATE__'
        ]
        html_lower = html.lower()
        for indicator in loading_indicators:
            if indicator in html_lower and len(visible_text) < 2000:
                return True
        
        # Check ratio of script tags to content
        scripts = sel.xpath('//script').getall()
        script_size = sum(len(s) for s in scripts)
        if script_size > len(html) * 0.7 and len(visible_text) < 2000:
            return True
        
        # Check for empty main content containers
        main_selectors = ['//main', '//article', '//*[@id="content"]', '//*[@id="main"]', '//*[@class="content"]']
        for sel_path in main_selectors:
            main_content = sel.xpath(f'{sel_path}//text()').getall()
            main_text = ' '.join(t.strip() for t in main_content if t.strip())
            if sel.xpath(sel_path) and len(main_text) < 100:
                return True
        
        return False
    
    # ========== FIXED: Moved this method to proper location ==========
    def _clean_extracted_data(self, data):
        """Clean extracted data by removing JavaScript and HTML"""
        cleaned = {}
        for key, value in data.items():
            if key == '_repair_count':
                cleaned[key] = value
                continue
                
            if isinstance(value, str):
                # Remove JavaScript code
                if 'document.getElementsByTagName' in value:
                    value = value.split('document.')[0].strip()
                if 'function(' in value:
                    value = value.split('function(')[0].strip()
                
                # Remove HTML tags
                import re
                value = re.sub(r'<[^>]+>', ' ', value)
                
                # Clean up whitespace
                value = re.sub(r'\s+', ' ', value).strip()
                
                # Limit length
                if len(value) > 1000:
                    value = value[:1000] + '...'
            
            cleaned[key] = value
        
        return cleaned

    # ========== LAYER 2: JSON-LD Extraction (The Scout) ==========
    def _extract_json_ld(self, html):
        """
        Quick JSON-LD extraction if available
        Layer 2: Fast-path for structured data
        """
        json_ld_items = []
        selector = Selector(text=html)
        
        # Find all JSON-LD script tags
        script_tags = selector.xpath('//script[@type="application/ld+json"]/text()').getall()
        
        for script_content in script_tags:
            try:
                data = json.loads(script_content)
                
                # Handle @graph structure (multiple schemas in one tag)
                items = data.get('@graph', [data]) if isinstance(data, dict) else [data]
                
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    
                    item_type = item.get('@type', '').lower()
                    
                    # Extract Product schema
                    if 'product' in item_type:
                        extracted = {
                            'title': item.get('name', ''),
                            'description': item.get('description', ''),
                            'brand': item.get('brand', {}).get('name', '') if isinstance(item.get('brand'), dict) else item.get('brand', ''),
                            'manufacturer': item.get('manufacturer', {}).get('name', '') if isinstance(item.get('manufacturer'), dict) else item.get('manufacturer', ''),
                            'image': item.get('image', ''),
                            'url': item.get('url', ''),
                            'sku': item.get('sku', ''),
                            'price': '',
                            'currency': '',
                        }
                        
                        # Extract price from offers
                        offers = item.get('offers', {})
                        if isinstance(offers, dict):
                            extracted['price'] = offers.get('price', '')
                            extracted['currency'] = offers.get('priceCurrency', '')
                        elif isinstance(offers, list) and offers:
                            extracted['price'] = offers[0].get('price', '')
                            extracted['currency'] = offers[0].get('priceCurrency', '')
                        
                        # Filter out empty values
                        extracted = {k: v for k, v in extracted.items() if v}
                        if extracted:
                            json_ld_items.append({
                                'source': 'jsonld_product',
                                'data': extracted,
                                'confidence': 0.95
                            })
                    
                    # Extract Article schema
                    elif 'article' in item_type:
                        extracted = {
                            'title': item.get('headline', '') or item.get('name', ''),
                            'description': item.get('description', ''),
                            'body_content': item.get('articleBody', ''),
                            'author': item.get('author', {}).get('name', '') if isinstance(item.get('author'), dict) else item.get('author', ''),
                            'date_published': item.get('datePublished', ''),
                            'image': item.get('image', ''),
                        }
                        extracted = {k: v for k, v in extracted.items() if v}
                        if extracted:
                            json_ld_items.append({
                                'source': 'jsonld_article',
                                'data': extracted,
                                'confidence': 0.95
                            })
                    
                    # Extract Medicine/Drug schema
                    elif 'medicine' in item_type or 'drug' in item_type:
                        extracted = {
                            # Basic Info
                            'title': item.get('name', ''),
                            'description': item.get('description', ''),
                            'brand': item.get('brand', ''),
                            'manufacturer': item.get('manufacturer', ''),
                            
                            # Composition & Strength
                            'active_ingredient': item.get('activeIngredient', ''),
                            'strength': item.get('strength', ''),
                            'composition': item.get('composition', ''),
                            'dosage_form': item.get('dosageForm', '') or item.get('dosage', ''),
                            
                            # Administration
                            'route_of_administration': item.get('routeOfAdministration', ''),
                            'method_of_administration': item.get('methodOfAdministration', ''),
                            
                            # Usage & Indications
                            'uses': item.get('uses', ''),
                            'indication': item.get('indication', ''),
                            'therapeutic_indication': item.get('therapeuticIndication', ''),
                            
                            # Dosage Info
                            'recommended_dose': item.get('recommendedDose', ''),
                            'dose': item.get('dose', ''),
                            'unit': item.get('unit', ''),
                            
                            # Safety & Status
                            'prescription_status': item.get('prescriptionStatus', ''),
                            'legal_status': item.get('legalStatus', ''),
                            
                            # Safety Information
                            'side_effects': item.get('sideEffects', '') or item.get('adverseEffect', ''),
                            'contraindications': item.get('contraindications', '') or item.get('contraindication', ''),
                            'warnings': item.get('warnings', '') or item.get('precautions', ''),
                            'interactions': item.get('drugInteractions', '') or item.get('interactions', ''),
                            
                            # Storage & Handling
                            'storage_conditions': item.get('storageConditions', ''),
                            'storage': item.get('storage', ''),
                            'shelf_life': item.get('shelfLife', ''),
                            
                            # Additional Info
                            'price': '',
                            'currency': '',
                            'batch_number': item.get('batchNumber', ''),
                            'expiry_date': item.get('expiryDate', ''),
                            'generic_name': item.get('genericName', ''),
                            'image': item.get('image', ''),
                            'url': item.get('url', ''),
                        }
                        
                        # Extract price from offers if available
                        offers = item.get('offers', {})
                        if isinstance(offers, dict):
                            extracted['price'] = offers.get('price', '')
                            extracted['currency'] = offers.get('priceCurrency', '')
                        elif isinstance(offers, list) and offers:
                            extracted['price'] = offers[0].get('price', '')
                            extracted['currency'] = offers[0].get('priceCurrency', '')
                        
                        # Handle list/array fields (convert to string)
                        for key in ['active_ingredient', 'side_effects', 'contraindications', 'warnings', 'interactions']:
                            if key in extracted and isinstance(extracted[key], list):
                                extracted[key] = ' | '.join(str(x) for x in extracted[key])
                        
                        extracted = {k: v for k, v in extracted.items() if v}
                        if extracted:
                            json_ld_items.append({
                                'source': 'jsonld_medicine',
                                'data': extracted,
                                'confidence': 0.98
                            })
            
            except json.JSONDecodeError:
                self.logger.debug(f"Failed to parse JSON-LD: {script_content[:100]}")
                continue
            except Exception as e:
                self.logger.debug(f"Error processing JSON-LD: {e}")
                continue
        
        return json_ld_items

    def _merge_json_ld(self, base_data, json_ld_results):
        """Merge JSON-LD fields into structured page data."""
        merged = dict(base_data or {})
        for result in json_ld_results:
            for key, value in result.get('data', {}).items():
                if key not in merged or not merged.get(key):
                    merged[key] = value
        return merged

    def _get_site_type(self, url) -> str:
        """Detect site type - used for logging only, not hardcoded behavior"""
        return urlparse(url).netloc
    
    def _extract_site_specific_fast(self, html: str, url: str) -> dict:
        """
        Use selectolax universal fast parser for ANY website.
        Dynamically learns patterns - no hardcoding.
        10-20x faster than parsel/lxml.
        """
        if not self.fast_parser:
            return {}
        
        try:
            # Use universal extraction that works on ANY website
            return self.fast_parser.extract_universal(html, url)
        except Exception as e:
            self.logger.debug(f"Fast parser error: {e}")
            return {}

    def _extract_all_sections_universal(self, html: str) -> dict:
        """
        UNIVERSAL section extraction that works on ANY website.
        Extracts ALL sections by following heading structure.
        Filters out unrelated, duplicate, or boilerplate content.
        """
        selector = Selector(text=html)
        data = {}

        # Title from first h1
        title = selector.xpath('normalize-space(//h1)').get()
        if title:
            data['title'] = title

        # Find main content area
        main_candidates = [
            '//main', '//article', '//*[@role="main"]', '//*[@id="content"]', '//body'
        ]
        root = None
        for candidate in main_candidates:
            nodes = selector.xpath(candidate)
            if nodes:
                root = nodes[0]
                break
        if root is None:
            root = selector  # fallback

        sections = {}
        current_heading = 'Overview'
        current_content = []
        seen_texts = set()

        # Iterate headings and paragraphs inside main content only
        for node in root.xpath('.//*[self::h1 or self::h2 or self::h3 or self::h4 or self::h5 or self::p or self::li]'):
            tag = node.root.tag.lower() if hasattr(node, 'root') else ''
            if tag in ['h1', 'h2', 'h3', 'h4', 'h5']:
                heading_text = node.xpath('string()').get().strip()
                if heading_text and 2 < len(heading_text) < 200:
                    # Save previous section if meaningful
                    if current_content:
                        section_text = '\n'.join(current_content).strip()
                        if len(section_text) > 20 and section_text not in seen_texts:
                            sections[current_heading] = section_text
                            seen_texts.add(section_text)
                    current_heading = heading_text
                    current_content = []
            elif tag in ['p', 'li']:
                text = node.xpath('string()').get().strip()
                # Only save meaningful, non-duplicate content
                if text and len(text) > 20 and text not in seen_texts:
                    current_content.append(text)
                    seen_texts.add(text)
        # Save last section
        if current_content:
            section_text = '\n'.join(current_content).strip()
            if len(section_text) > 20 and section_text not in seen_texts:
                sections[current_heading] = section_text

        # Convert sections to data fields
        if sections:
            data['sections'] = sections
            data['full_content'] = '\n\n'.join([f"## {h}\n\n{c}" for h, c in sections.items()])
            data['summary'] = ' '.join(list(sections.values())[:3])

        return data
                
       

    def _extract_structured_page_content(self, html):
        """
        Extract all drug information section-by-section from pages.
        Captures: Overview, How to use, Drug works, Side effects, etc.
        """
        selector = Selector(text=html)

        # Get title
        title = (
            selector.xpath('normalize-space(//h1[1])').get()
            or selector.xpath('normalize-space(//h2[1])').get()
            or selector.xpath('normalize-space(//title)').get()
            or ''
        ).strip()

        data = {}
        if title:
            data["title"] = title

        # Find main content area
        container_candidates = [
            '//main',
            '//article',
            '//*[@role="main"]',
            '//*[@id="mplus-content"]',
            '//*[@id="main-content"]',
            '//*[@id="content"]',
            '//body',
        ]

        root = None
        for candidate in container_candidates:
            nodes = selector.xpath(candidate)
            if nodes:
                root = nodes[0]
                break

        if root is None:
            return data

        # Extract all sections with their content
        sections = []
        current_section = None
        seen_sections = set()  # Track section headings to avoid duplicates
        seen_exact_texts = set()  # Track EXACT full text to avoid duplicate elements
        
        # Get all heading and content elements in order
        all_nodes = root.xpath('.//*[self::h1 or self::h2 or self::h3 or self::h4 or self::p or self::ul or self::ol]')
        
        for node in all_nodes:
            try:
                tag = node.root.tag.lower()
                
                # New section heading
                if tag in ["h1", "h2", "h3", "h4"]:
                    heading_text = (node.xpath('string()').get() or '').strip()
                    if heading_text and len(heading_text) > 2:
                        # Skip irrelevant sections
                        skip_keywords = ['reference', 'read more', 'advertisement', 'related', 'similar', 'review', 'rating', 'comment', 'author', 'written by']
                        if not any(kw in heading_text.lower() for kw in skip_keywords):
                            # Skip if heading is inside a nav, list, or tooltip
                            parents = node.xpath('ancestor-or-self::nav | ancestor-or-self::ul | ancestor-or-self::ol | ancestor-or-self::*[@role="navigation"] | ancestor-or-self::*[@class and contains(@class, "toc")]')
                            if parents:
                                continue
                            
                            # Skip duplicate section headings (page has duplicate DOM elements)
                            if heading_text.lower() not in seen_sections:
                                # Save previous section if it has content
                                if current_section and current_section.get("content"):
                                    sections.append(current_section)
                                
                                # Start new section
                                current_section = {
                                    "heading": heading_text,
                                    "content": []
                                }
                                seen_sections.add(heading_text.lower())
                            else:
                                # Duplicate heading found, skip but don't reset section
                                pass
                        continue
                
                # Add content to current section
                if current_section is None:
                    current_section = {
                        "heading": "Overview",
                        "content": []
                    }
                
                # Extract paragraphs (deduplicate EXACT matches only)
                if tag == "p":
                    text = (node.xpath('string()').get() or '').strip()
                    if text and len(text) > 10:
                        # Use hash of full text for exact duplicate detection
                        text_hash = hash(text)
                        if text_hash not in seen_exact_texts:
                            seen_exact_texts.add(text_hash)
                            current_section["content"].append({
                                "type": "paragraph",
                                "text": text
                            })
                
                # Extract lists (deduplicate EXACT matches only)
                elif tag in ["ul", "ol"]:
                    items = []
                    for li in node.xpath('./li'):
                        item_text = (li.xpath('string()').get() or '').strip()
                        if item_text and len(item_text) > 2:
                            item_hash = hash(item_text)
                            if item_hash not in seen_exact_texts:
                                items.append(item_text)
                                seen_exact_texts.add(item_hash)
                    
                    if items:
                        current_section["content"].append({
                            "type": "list",
                            "items": items
                        })
            
            except Exception as e:
                self.logger.debug(f"Error extracting node: {e}")
                continue
        
        # Save last section
        if current_section and current_section.get("content"):
            sections.append(current_section)

        # Format sections for output
        if sections:
            data["sections"] = {}
            for section in sections:
                heading = section.get("heading", "").strip()
                if heading:
                    # Flatten section content
                    content_parts = []
                    for part in section.get("content", []):
                        if part.get("type") == "paragraph":
                            content_parts.append(part.get("text", ""))
                        elif part.get("type") == "list":
                            for item in part.get("items", []):
                                content_parts.append(f"• {item}")
                    
                    if content_parts:
                        data["sections"][heading] = "\n".join(content_parts)
        
        # 🔧 Create summary and full_content for CSV columns (already deduplicated)
        if sections:
            # Extract first meaningful content for summary
            summary_parts = []
            full_content_parts = []
            
            for section in sections:
                heading = section.get("heading", "").strip()
                if heading:
                    full_content_parts.append(f"## {heading}")
                
                for part in section.get("content", []):
                    if part.get("type") == "paragraph":
                        text = part.get("text", "").strip()
                        if text:
                            full_content_parts.append(text)
                            # Add to summary if still collecting
                            if len(summary_parts) < 3:
                                summary_parts.append(text)
                    elif part.get("type") == "list":
                        for item in part.get("items", []):
                            full_content_parts.append(f"• {item}")
            
            if full_content_parts:
                data["full_content"] = "\n\n".join(full_content_parts)
            if summary_parts:
                data["summary"] = " ".join(summary_parts)
        
        return data

    def _extract_all_links(self, html, base_url):
        """
        Extract ALL types of links from a page - handles various link patterns:
        - Regular anchor links (<a href>)
        - Card/tile links (links inside card containers)
        - Data attribute links (data-href, data-url, data-link)
        - Button/onclick links
        - Pagination links
        - Image map links
        """
        from parsel import Selector
        import re
        
        sel = Selector(text=html)
        links = set()
        
        # 1. Regular anchor links
        for href in sel.xpath('//a/@href').getall():
            if href:
                links.add(href)
        
        # 2. Data attribute links (common in card systems)
        data_attrs = ['data-href', 'data-url', 'data-link', 'data-navigate', 'data-target-url']
        for attr in data_attrs:
            for link in sel.xpath(f'//*[@{attr}]/@{attr}').getall():
                if link:
                    links.add(link)
        
        # 3. Links inside card/tile containers (common patterns)
        card_selectors = [
            '//div[contains(@class, "card")]//a/@href',
            '//div[contains(@class, "tile")]//a/@href',
            '//div[contains(@class, "product")]//a/@href',
            '//div[contains(@class, "item")]//a/@href',
            '//li[contains(@class, "card")]//a/@href',
            '//article//a/@href',
            '//*[contains(@class, "listing")]//a/@href',
            '//*[contains(@class, "grid")]//a/@href',
        ]
        for selector in card_selectors:
            for link in sel.xpath(selector).getall():
                if link:
                    links.add(link)
        
        # 4. Links with specific patterns (drugs, products, medicines, etc.)
        content_patterns = [
            '//a[contains(@href, "/drug")]/@href',
            '//a[contains(@href, "/product")]/@href',
            '//a[contains(@href, "/medicine")]/@href',
            '//a[contains(@href, "/otc")]/@href',
            '//a[contains(@href, "/pharmacology")]/@href',
            '//a[contains(@href, "/generic")]/@href',
            '//a[contains(@href, "/article")]/@href',
            '//a[contains(@href, "/detail")]/@href',
        ]
        for pattern in content_patterns:
            for link in sel.xpath(pattern).getall():
                if link:
                    links.add(link)
        
        # 5. Onclick handlers with URLs (parse window.location, href patterns)
        onclick_elements = sel.xpath('//*[@onclick]/@onclick').getall()
        url_pattern = re.compile(r"(?:window\.location|location\.href|href)\s*=\s*['\"]([^'\"]+)['\"]")
        for onclick in onclick_elements:
            match = url_pattern.search(onclick)
            if match:
                links.add(match.group(1))
        
        # 6. Pagination links
        pagination_selectors = [
            '//a[contains(@class, "page")]/@href',
            '//a[contains(@class, "pagination")]/@href',
            '//*[contains(@class, "pager")]//a/@href',
            '//a[contains(@href, "page=")]/@href',
            '//a[contains(@href, "?p=")]/@href',
            '//a[@rel="next"]/@href',
            '//a[@rel="prev"]/@href',
        ]
        for selector in pagination_selectors:
            for link in sel.xpath(selector).getall():
                if link:
                    links.add(link)
        
        # 7. Area map links
        for link in sel.xpath('//area/@href').getall():
            if link:
                links.add(link)
        
        # 8. Form action URLs (sometimes used for navigation)
        for link in sel.xpath('//form/@action').getall():
            if link and not link.startswith('#'):
                links.add(link)
        
        # Clean and normalize links
        cleaned_links = []
        for link in links:
            # Skip empty, anchors, javascript
            if not link or link.startswith('#') or link.startswith('javascript:'):
                continue
            # Skip mailto, tel links
            if link.startswith('mailto:') or link.startswith('tel:'):
                continue
            # Skip data URIs
            if link.startswith('data:'):
                continue
            
            # Make absolute URL
            full_url = urljoin(base_url, link)
            cleaned_links.append(full_url)
        
        return list(set(cleaned_links))
    
    async def parse_page(self, response):
        """Main parsing method with parallel extraction option"""
        page = response.meta.get('playwright_page')
        url = response.url
        depth = response.meta.get('depth', 0)
        
        #  STOP if we've reached max pages (prevents queue overflow)
        if self.pages_crawled >= self.max_pages:
            self.logger.info(f" Max pages ({self.max_pages}) reached, closing spider: {url}")
            if page:
                try:
                    await page.close()
                except:
                    pass
            # Force spider shutdown
            from scrapy.exceptions import CloseSpider
            raise CloseSpider(f"Max pages ({self.max_pages}) reached")
            return
        
        self.pages_crawled += 1
        self.logger.info(f" [{self.pages_crawled}/{self.max_pages}] Crawling: {url}")
        
        try:
            # Get HTML content
            if page:
                html = await page.content()
            else:
                html = response.text
            
            # ============================================================
            # JS DETECTION FALLBACK - Try Playwright if HTTP response looks JS-heavy
            # ============================================================
            is_initial = response.meta.get('is_initial', False)
            tried_playwright = response.meta.get('tried_playwright', False)
            
            if not page and not tried_playwright and not self.http_only and PLAYWRIGHT_AVAILABLE:
                if self._needs_javascript(html):
                    self.logger.info(f" JS-heavy page detected, retrying with Playwright: {url}")
                    self.pages_crawled -= 1  # Don't count this failed attempt
                    meta = {
                        'playwright': True,
                        'playwright_include_page': True,
                        'playwright_page_goto_kwargs': {
                            'wait_until': 'domcontentloaded',
                            'timeout': 60000,
                        },
                        'playwright_page_methods': [
                            PageMethod('wait_for_load_state', 'domcontentloaded'),
                            PageMethod('wait_for_timeout', 3000),
                            PageMethod('evaluate', 'window.scrollTo(0, document.body.scrollHeight)'),
                            PageMethod('wait_for_timeout', 1000),
                        ],
                        'depth': depth,
                        'crawl_time': time.time(),
                        'tried_playwright': True,
                    }
                    yield scrapy.Request(
                        url=url,
                        callback=self.parse_page,
                        errback=self.handle_error,
                        meta=meta,
                        dont_filter=True
                    )
                    return
            
            # ============================================================
            # HASH GATE - Skip duplicate content only
            # ============================================================
            layout_hash = hashlib.md5(html[:5000].encode()).hexdigest()
            if url in self.layout_cache and self.layout_cache[url] == layout_hash:
                self.logger.info(f" Layout unchanged, skipping: {url}")
                return

    

            self.layout_cache[url] = layout_hash
            self.known_layout_hashes.add(layout_hash)

            # ============================================================
            # LAYER 2: JSON-LD EXTRACTION (The Scout)
            # ============================================================
            try:
                json_ld_results = self._extract_json_ld(html)
                if json_ld_results:
                    sources = ", ".join(sorted({r.get('source', '') for r in json_ld_results if r.get('source')}))
                    self.logger.info(f" JSON-LD extracted ({sources})")
            except Exception as e:
                self.logger.error(f"Error extracting JSON-LD from {url}: {e}", exc_info=True)
                json_ld_results = []

            # ============================================================
            # FAST SITE-SPECIFIC EXTRACTION (Using selectolax - 10-20x faster)
            # ============================================================
            page_detail_extracted = False
            structured_data = {}
            site_type = self._get_site_type(url)
            
            try:
                # Use fast parser if available and patterns learned
                if self.fast_parser and (self.domain in self.patterns_learned or self.pages_crawled > 3):
                    self.logger.debug(f"⚡ Using fast parser for {site_type}")
                    structured_data = self._extract_site_specific_fast(html, url)
                    if structured_data:
                        self.patterns_learned[self.domain] = True
                
                # Fallback to parsel-based universal extraction
                if not structured_data:
                    # Try universal section extraction first
                    structured_data = self._extract_all_sections_universal(html)
                    
                    # If that didn't get much, try the structured page content
                    if not structured_data or len(structured_data) < 3:
                        fallback_data = self._extract_structured_page_content(html)
                        # Merge fallback data
                        for key, value in fallback_data.items():
                            if key not in structured_data or not structured_data.get(key):
                                structured_data[key] = value
                
                # Merge JSON-LD data
                structured_data = self._merge_json_ld(structured_data, json_ld_results)
                if not structured_data and json_ld_results:
                    structured_data = self._merge_json_ld({}, json_ld_results)
                
                if structured_data:
                    # Remove sections from output (keep only flat data)
                    structured_data.pop('sections', None)
                    
                    page_item = ScrapedItem()
                    page_item['url'] = url
                    page_item['domain'] = self.domain
                    page_item['container_type'] = 'page_detail'
                    page_item['data'] = structured_data
                    page_item['confidence'] = 0.9
                    page_item['layout_hash'] = hashlib.md5(html[:1000].encode()).hexdigest()
                    page_item['repair_count'] = 0

                    self.items_extracted += 1
                    page_detail_extracted = True
                    yield page_item
            except Exception as e:
                self.logger.error(f"Error extracting structured content from {url}: {e}", exc_info=True)

            # ============================================================
            # UNIVERSAL LINK DISCOVERY - Follow all types of links
            # (cards, tiles, anchors, data-attributes, pagination, etc.)
            # ============================================================
            try:
                if depth < self.max_depth and self.pages_crawled < self.max_pages:
                    all_links = self._extract_all_links(html, url)
                    self.logger.info(f" Found {len(all_links)} total links on page")
                    
                    followed_count = 0
                    for full_url in all_links:
                        if self.should_follow_link(full_url, depth):
                            if full_url not in self.visited_urls:
                                self.visited_urls.add(full_url)
                                followed_count += 1
                                yield self.create_request(
                                    full_url,
                                    depth=depth + 1
                                )
                                
                                # Limit how many links we follow per page
                                if followed_count >= 50:
                                    break
                    
                    if followed_count > 0:
                        self.logger.info(f" Queued {followed_count} links to follow")
            except Exception as e:
                self.logger.error(f"Error discovering links from {url}: {e}", exc_info=True)
            
            # ============================================================
            # GEOMETRIC EXTRACTION (Skip if page_detail already extracted)
            # ============================================================
            if page_detail_extracted:
                self.logger.info(f"  Skipping geometric extraction (page_detail already extracted)")
            else:
                try:
                    containers = self.geometry.extract_all_elements(html)
                    # 🔧 FIX: Filter out None containers
                    containers = [c for c in containers if c is not None]
                    self.logger.info(f"Found {len(containers)} containers")
                    
                    # Process containers in parallel or sequentially
                    if self.parallel_enabled and len(containers) > 1:
                        # Parallel processing
                        tasks = []
                        for container in containers:
                            if container is not None:
                                task = self._process_container(page, html, container, url, depth)
                                tasks.append(task)
                        
                        results = await asyncio.gather(*tasks)
                        
                        for items_list, requests_list in results:
                            # Yield items
                            for item in items_list:
                                self.items_extracted += 1
                                yield item
                            
                            # Yield follow-up requests
                            for req in requests_list:
                                yield req
                                        
                    else:
                        # Sequential processing
                        for container in containers:
                            if container is not None:
                                items_list, requests_list = await self._process_container(page, html, container, url, depth)
                                
                                for item in items_list:
                                    self.items_extracted += 1
                                    yield item
                                
                                for req in requests_list:
                                    yield req
                except Exception as e:
                    self.logger.error(f"Error in geometric extraction from {url}: {e}", exc_info=True)
            
            # ============================================================
            # FIND PAGINATION
            # ============================================================
            try:
                next_url = await self.find_next_page(page, html)
                if next_url and depth < self.max_depth and self.pages_crawled < self.max_pages:
                    full_url = urljoin(url, next_url)
                    # Avoid duplicates
                    if full_url not in self.visited_urls:
                        self.visited_urls.add(full_url)
                        yield self.create_request(
                            full_url,
                            depth=depth + 1
                        )
            except Exception as e:
                self.logger.error(f"Error finding next page from {url}: {e}", exc_info=True)
        
        except Exception as e:
            self.logger.error(f"Error parsing page {url}: {e}", exc_info=True)
        finally:
            if page:
                await page.close()
    
    async def _process_container(self, page, html, container, url, depth):
        """Process a single container - returns list of items and requests"""
        items = []
        requests = []
        
        try:
            container_data = self.geometry.extract_container_data(container)
            
            if container_data['type'] == 'generic_container' and not container_data['elements']:
                return items, requests
            
            # Extract fields with repair
            extracted = await self._extract_with_repair(html, container_data)
            
            if extracted:
                item = ScrapedItem()
                item['url'] = url
                item['domain'] = self.domain
                item['container_type'] = container_data['type']
                item['data'] = extracted
                item['confidence'] = container_data.get('confidence', 0.5)
                item['layout_hash'] = hashlib.md5(html[:1000].encode()).hexdigest()
                item['repair_count'] = extracted.get('_repair_count', 0)
                
                items.append(item)
                
                # Collect links to follow
                for link in container_data.get('links', []):
                    if self.should_follow_link(link['url'], depth):
                        full_url = urljoin(url, link['url'])
                        if full_url not in self.visited_urls:
                            self.visited_urls.add(full_url)
                            requests.append(self.create_request(
                                full_url,
                                depth=depth + 1
                            ))
        except Exception as e:
            self.logger.error(f"Error processing container: {e}")
        
        return items, requests
    
    async def _extract_with_repair(self, html, container_data):
        """Extract fields with automatic repair"""
        result = {}
        repair_count = 0
        
        # Check cache first
        cache_key = hashlib.md5(html[:1000].encode()).hexdigest()
        if cache_key in self.extraction_cache:
            return self.extraction_cache[cache_key]
        
        # Parse HTML for selector-based extraction
        selector = Selector(text=html)
        
        # Try to extract each field
        for field_name, field_config in self.extraction_fields.items():
            value = None
            confidence = 0
            
            # PRIORITY 1: Try CSS selectors from config file first
            if 'selectors' in field_config and field_config['selectors']:
                for css_sel in field_config['selectors']:
                    try:
                        # Try CSS selector
                        elements = selector.css(css_sel)
                        if elements:
                            # Get text content
                            text = elements.xpath('string()').get()
                            if text and text.strip():
                                value = text.strip()
                                confidence = 0.9  # High confidence for direct CSS match
                                break
                    except Exception:
                        # Try as XPath if CSS fails
                        try:
                            elements = selector.xpath(css_sel)
                            if elements:
                                text = elements.xpath('string()').get()
                                if text and text.strip():
                                    value = text.strip()
                                    confidence = 0.9
                                    break
                        except:
                            continue
            
            # PRIORITY 2: Check if container already has this field (geometric extraction)
            if not value and field_name in container_data.get('elements', {}):
                elem_data = container_data['elements'][field_name]
                value = elem_data.get('value', '')
                confidence = elem_data.get('confidence', 0)
            
            # PRIORITY 3: If not found or low confidence, try repair
            if (not value or confidence < 0.6) and hasattr(Config, 'ENABLE_PARENT_TRAP') and Config.ENABLE_PARENT_TRAP:
                context = {
                    'container_type': container_data.get('type', 'unknown'),
                    'failed_selector': f"field_{field_name}"
                }
                
                repair_result = self.repair.repair_field(
                    html, field_name, field_config, context
                )
                
                if repair_result.get('success'):
                    value = repair_result.get('value', '')
                    confidence = repair_result.get('confidence', 0.5)
                    repair_count += 1
                    self.logger.info(f"🔧 Repaired {field_name} with {repair_result.get('method', 'unknown')}")
            
            if value:
                result[field_name] = value
        
        result['_repair_count'] = repair_count
        
        # Cache result
        self.extraction_cache[cache_key] = result
        
        # Clean the results
        result = self._clean_extracted_data(result)
        
        return result
    
    async def find_next_page(self, page, html=None):
        """Find next page link using multiple strategies"""
        if page:
            # Strategy 1: Look for rel="next"
            try:
                next_link = await page.query_selector('a[rel="next"]')
                if next_link:
                    return await next_link.get_attribute('href')
            except:
                pass

            # Strategy 2: Look for "Next" text
            try:
                next_link = await page.query_selector('a:has-text("Next"), a:has-text("next")')
                if next_link:
                    return await next_link.get_attribute('href')
            except:
                pass

            # Strategy 3: Look for arrow symbols
            try:
                next_link = await page.query_selector('a:has-text("›"), a:has-text("»")')
                if next_link:
                    return await next_link.get_attribute('href')
            except:
                pass

            # Strategy 4: Look for pagination numbers
            try:
                page_links = await page.query_selector_all('.pagination a, .pagination-next, .next-page')
                if page_links and len(page_links) > 0:
                    # Get the last link (often "Next")
                    last = page_links[-1]
                    return await last.get_attribute('href')
            except:
                pass

            return None

        if not html:
            return None

        selector = Selector(text=html)
        href = selector.xpath('//a[@rel="next"]/@href').get()
        if href:
            return href

        href = selector.xpath('//a[contains(translate(normalize-space(string(.)), "NEXT", "next"), "next")]/@href').get()
        if href:
            return href

        href = selector.xpath('//a[contains(string(.), "›") or contains(string(.), "»")]/@href').get()
        if href:
            return href

        hrefs = selector.xpath('//a[contains(@class, "pagination") or contains(@class, "next")]/@href').getall()
        if hrefs:
            return hrefs[-1]

        return None
    
    def should_follow_link(self, link_url, current_depth):
        """Decide whether to follow a link"""
        if not link_url:
            return False
            
        # Don't follow if too deep
        if current_depth >= self.max_depth:
            return False
        
        # Skip empty or javascript links
        if link_url.startswith('#') or link_url.startswith('javascript:'):
            return False
        
        # Make absolute URL
        try:
            base_url = self.start_urls[0] if self.start_urls and self.start_urls[0] else ''
            absolute_url = urljoin(base_url, link_url)
        except:
            return False
        
        # Parse URL
        try:
            parsed = urlparse(absolute_url)
            # Don't follow external links if domain is set
            if self.domain and parsed.netloc and parsed.netloc != self.domain:
                return False
        except:
            pass

        # Apply follow patterns if provided
        if self.follow_patterns:
            matched = False
            for raw_pattern, compiled in self.follow_patterns:
                if compiled:
                    if compiled.search(absolute_url):
                        matched = True
                        break
                else:
                    if raw_pattern in absolute_url:
                        matched = True
                        break
            if not matched:
                return False
        
        # Don't follow links we've seen
        if absolute_url in self.visited_urls:
            return False
        
        # Don't follow file downloads
        if any(ext in absolute_url.lower() for ext in ['.pdf', '.jpg', '.png', '.zip', '.mp4', '.mp3']):
            return False
        
        return True
    
    def handle_error(self, failure):
        """Handle request errors with adaptive behavior tracking"""
        url = failure.request.url if hasattr(failure, 'request') else 'unknown'
        error_str = str(failure.value).lower()
        
        # Track error types for adaptive behavior
        if 'timeout' in error_str or 'timed out' in error_str:
            self.timeout_count += 1
            self.logger.warning(f" Timeout #{self.timeout_count}: {url}")
        elif 'playwright' in error_str or 'browser' in error_str:
            self.playwright_errors += 1
            self.logger.warning(f" Playwright error #{self.playwright_errors}: {url}")
        elif '429' in error_str or 'too many' in error_str or 'rate limit' in error_str:
            self.throttle_count += 1
            self.logger.warning(f" Throttled #{self.throttle_count}: {url}")
        else:
            self.logger.error(f" Error: {failure.value}")
        
        # Track retry counts per URL
        self.retry_counts[url] = self.retry_counts.get(url, 0) + 1
        
        # If too many errors, add domain to failed domains
        if self.retry_counts.get(url, 0) >= 3:
            domain = urlparse(url).netloc
            self.failed_domains.add(domain)
            self.logger.warning(f" Domain marked as problematic: {domain}")
    
    def closed(self, reason):
        """Log final statistics"""
        self.logger.info("=" * 70)
        self.logger.info(" CRAWL STATISTICS")
        self.logger.info("=" * 70)
        self.logger.info(f"Pages crawled: {self.pages_crawled}")
        self.logger.info(f"Items extracted: {self.items_extracted}")
        self.logger.info(f"Unique URLs found: {len(self.visited_urls)}")
        
        # Protection status
        self.logger.info("\n PROTECTION STATUS:")
        self.logger.info(f"  User Agent Rotation: { '' if getattr(Config, 'ENABLE_USER_AGENT_ROTATION', False) else ''}")
        self.logger.info(f"  Proxy Rotation: {'' if getattr(Config, 'ENABLE_PROXY_ROTATION', False) else ''}")
        self.logger.info(f"  Request Signing: {'' if getattr(Config, 'ENABLE_REQUEST_SIGNING', False) else ''}")
        self.logger.info(f"  Random Delay: {'' if getattr(Config, 'ENABLE_RANDOM_DELAY', False) else ''}")
        self.logger.info(f"  Fingerprinting: {'' if getattr(Config, 'ENABLE_FINGERPRINTING', False) else ''}")
        self.logger.info(f"  Cookie Rotation: {'' if getattr(Config, 'ENABLE_COOKIE_ROTATION', False) else ''}")
        self.logger.info(f"  Smart Retry: {'' if getattr(Config, 'ENABLE_RETRY_MECHANISM', False) else ''}")
        self.logger.info(f"  Parallel Extraction: {'' if getattr(Config, 'PARALLEL_EXTRACTION', False) else ''}")
        
        # Repair stats
        if hasattr(self, 'repair') and self.repair:
            repair_stats = self.repair.get_stats()
            self.logger.info(f"\n🔧 REPAIR STATISTICS:")
            self.logger.info(f"  Level 1 (Parent Trap): {repair_stats.get('level1_success', 0)}")
            self.logger.info(f"  Level 2 (Keyword Hunt): {repair_stats.get('level2_success', 0)}")
            self.logger.info(f"  Level 3 (Visual Pattern): {repair_stats.get('level3_success', 0)}")
            self.logger.info(f"  Level 4 (LLM): {repair_stats.get('level4_success', 0)}")
            self.logger.info(f"  Failed: {repair_stats.get('failed', 0)}")
        
        # Thread pool cleanup
        if hasattr(self, 'thread_pool'):
            self.thread_pool.shutdown(wait=False)
        
        self.logger.info("=" * 70)