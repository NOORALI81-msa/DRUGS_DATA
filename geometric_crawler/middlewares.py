# geometric_crawler/middlewares.py
"""
Complete protection middleware suite with toggle switches
All techniques are free and open-source
"""
import random
import time
import hashlib
from datetime import datetime
from urllib.parse import urlparse
from scrapy import signals
from .config import Config

# ============================================================
# 1️⃣ USER AGENT ROTATION (Free)
# ============================================================
class UserAgentRotationMiddleware:
    """Rotate user agents from built-in database"""
    
    def __init__(self):
        self.user_agents = Config.USER_AGENTS
        self.current_ua = None
        self.enabled = Config.ENABLE_USER_AGENT_ROTATION
        self.crawler = None
    
    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls()
        middleware.crawler = crawler
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware
    
    def spider_opened(self, spider):
        spider.logger.info(f"🔀 User Agent Rotation: {'ENABLED' if self.enabled else 'DISABLED'}")
    
    def process_request(self, request):
        if not self.enabled:
            return
        
        # Rotate user agent
        ua = random.choice(self.user_agents)
        request.headers['User-Agent'] = ua
        self.current_ua = ua
        
        # Add realistic accept headers
        request.headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7'
        request.headers['Accept-Language'] = random.choice([
            'en-US,en;q=0.9',
            'en-GB,en;q=0.8',
            'en;q=0.7',
            'en-US,en;q=0.9,es;q=0.8'
        ])
        request.headers['Accept-Encoding'] = 'gzip, deflate, br, zstd'
        request.headers['Connection'] = 'keep-alive'
        request.headers['Upgrade-Insecure-Requests'] = '1'
        
        # Add Sec-Fetch headers (modern Chrome sends these)
        request.headers['Sec-Fetch-Dest'] = 'document'
        request.headers['Sec-Fetch-Mode'] = 'navigate'
        request.headers['Sec-Fetch-Site'] = 'none'
        request.headers['Sec-Fetch-User'] = '?1'
        
        # Add Priority header (Chrome 117+)
        request.headers['Priority'] = 'u=0, i'
        
        # Add Referer for non-start requests
        if hasattr(request, 'meta') and request.meta.get('depth', 0) > 0:
            parsed = urlparse(request.url)
            request.headers['Referer'] = f"{parsed.scheme}://{parsed.netloc}/"
            request.headers['Sec-Fetch-Site'] = 'same-origin'


# ============================================================
# 2️⃣ PROXY ROTATION (Free proxies - optional)
# ============================================================
class ProxyRotationMiddleware:
    """Rotate free proxies to avoid IP bans"""
    
    def __init__(self):
        self.proxies = Config.FREE_PROXIES
        self.current_proxy = None
        self.enabled = Config.ENABLE_PROXY_ROTATION and len(self.proxies) > 0
        self.failed_proxies = set()
        self.crawler = None
    
    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls()
        middleware.crawler = crawler
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware
    
    def spider_opened(self, spider):
        if self.enabled:
            spider.logger.info(f"🔄 Proxy Rotation: ENABLED with {len(self.proxies)} proxies")
        else:
            spider.logger.info("🔄 Proxy Rotation: DISABLED (no proxies configured)")
    
    def process_request(self, request):
        if not self.enabled:
            return
        
        # Find a working proxy
        for _ in range(min(3, len(self.proxies))):
            proxy = random.choice(self.proxies)
            if proxy not in self.failed_proxies:
                request.meta['proxy'] = proxy
                self.current_proxy = proxy
                return
    
    def process_response(self, request, response):
        if not self.enabled:
            return response
        
        # Mark proxy as failed on certain status codes
        if response.status in [403, 429, 502, 503, 504]:
            if self.current_proxy:
                self.failed_proxies.add(self.current_proxy)
                if self.crawler and self.crawler.spider:
                    self.crawler.spider.logger.warning(f"⚠️ Proxy failed: {self.current_proxy}")
        
        return response
    
    def process_exception(self, request, exception):
        if not self.enabled:
            return
        
        # Mark proxy as failed on exception
        if self.current_proxy:
            self.failed_proxies.add(self.current_proxy)
            if self.crawler and self.crawler.spider:
                self.crawler.spider.logger.warning(f"⚠️ Proxy error: {self.current_proxy}")
        
        # Retry without proxy
        new_request = request.copy()
        new_request.meta['proxy'] = None
        new_request.dont_filter = True
        return new_request


# ============================================================
# 3️⃣ REQUEST SIGNING (Mimic real browsers)
# ============================================================
class RequestSigningMiddleware:
    """Add browser-like signatures to requests"""
    
    def __init__(self):
        self.enabled = Config.ENABLE_REQUEST_SIGNING
        self.crawler = None
    
    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls()
        middleware.crawler = crawler
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware
    
    def spider_opened(self, spider):
        spider.logger.info(f"✍️ Request Signing: {'ENABLED' if self.enabled else 'DISABLED'}")
    
    def process_request(self, request):
        if not self.enabled:
            return
        
        # Generate browser-like headers
        request.headers['Cache-Control'] = random.choice([
            'max-age=0',
            'no-cache',
            'no-store'
        ])
        
        # Add random headers sometimes
        if random.random() > 0.7:
            request.headers['DNT'] = '1'  # Do Not Track
        
        if random.random() > 0.8:
            request.headers['Save-Data'] = 'on'
        
        # Add sec-ch-ua headers (modern browsers)
        chrome_version = random.randint(110, 120)
        request.headers['Sec-Ch-Ua'] = f'"Chromium";v="{chrome_version}", "Google Chrome";v="{chrome_version}", "Not-A.Brand";v="99"'
        request.headers['Sec-Ch-Ua-Mobile'] = '?0'
        request.headers['Sec-Ch-Ua-Platform'] = random.choice(['"Windows"', '"macOS"', '"Linux"'])


# ============================================================
# 4️⃣ RANDOM DELAY (Human-like behavior)
# ============================================================
class RandomDelayMiddleware:
    """Add random delays between requests"""
    
    def __init__(self):
        self.base_delay = Config.DOWNLOAD_DELAY
        self.enabled = Config.ENABLE_RANDOM_DELAY
        self.crawler = None
    
    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls()
        middleware.crawler = crawler
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware
    
    def spider_opened(self, spider):
        spider.logger.info(f"⏱️ Random Delay: {'ENABLED' if self.enabled else 'DISABLED'} (base: {self.base_delay}s)")
    
    def process_request(self, request):
        if not self.enabled:
            return
        
        # Random delay between 0.5x and 2x base delay
        delay = random.uniform(self.base_delay * 0.5, self.base_delay * 2.0)
        time.sleep(delay)


# ============================================================
# 5️⃣ BROWSER FINGERPRINTING (For Playwright)
# ============================================================
class BrowserFingerprintMiddleware:
    """Add random browser fingerprints to Playwright"""
    
    def __init__(self):
        self.fingerprints = Config.FINGERPRINTS
        self.enabled = Config.ENABLE_FINGERPRINTING
        self.crawler = None
    
    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls()
        middleware.crawler = crawler
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware
    
    def spider_opened(self, spider):
        spider.logger.info(f"🖐️ Browser Fingerprinting: {'ENABLED' if self.enabled else 'DISABLED'}")
    
    def process_request(self, request):
        if not self.enabled:
            return
        
        # Add fingerprint to request meta for Playwright
        fp = random.choice(self.fingerprints)
        request.meta['playwright_context_kwargs'] = {
            'viewport': {
                'width': fp['screen_width'],
                'height': fp['screen_height']
            },
            'locale': fp['language'].split(',')[0],
            'timezone_id': fp['timezone'],
            'color_scheme': random.choice(['light', 'dark']),
        }


# ============================================================
# 6️⃣ COOKIE ROTATION (Fresh cookies per session)
# ============================================================
class CookieRotationMiddleware:
    """Rotate cookies to appear as different users"""
    
    def __init__(self):
        self.enabled = Config.ENABLE_COOKIE_ROTATION
        self.cookie_jar = {}
        self.crawler = None
    
    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls()
        middleware.crawler = crawler
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware
    
    def spider_opened(self, spider):
        spider.logger.info(f"🍪 Cookie Rotation: {'ENABLED' if self.enabled else 'DISABLED'}")
    
    def process_request(self, request):
        if not self.enabled:
            return
        
        # Generate fresh cookies for each domain
        domain = urlparse(request.url).netloc
        
        if domain not in self.cookie_jar:
            # Create new cookie set
            session_id = hashlib.md5(f"{domain}{random.random()}{time.time()}".encode()).hexdigest()[:16]
            timestamp = int(time.time())
            
            cookies = [
                f'_ga=GA1.2.{random.randint(100000000, 999999999)}.{timestamp}',
                f'_gid=GA1.2.{random.randint(100000000, 999999999)}.{timestamp}',
                f'_session_id={session_id}',
                f'visited={random.randint(0, 1)}',
            ]
            
            self.cookie_jar[domain] = '; '.join(cookies)
        
        request.headers['Cookie'] = self.cookie_jar[domain]


# ============================================================
# 7️⃣ SMART RETRY (Exponential backoff + Playwright fallback)
# ============================================================
class SmartRetryMiddleware:
    """Intelligent retry with exponential backoff and Playwright fallback for protected sites"""
    
    def __init__(self):
        self.max_retries = 3
        self.enabled = Config.ENABLE_RETRY_MECHANISM
        self.retry_counts = {}
        self.playwright_fallback_codes = {403, 429}  # Use Playwright for these
        self.crawler = None
    
    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls()
        middleware.crawler = crawler
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware
    
    def spider_opened(self, spider):
        spider.logger.info(f"🔄 Smart Retry: {'ENABLED' if self.enabled else 'DISABLED'}")
    
    def process_response(self, request, response):
        if not self.enabled:
            return response
        
        # Check if we should retry
        if response.status in [403, 429, 500, 502, 503, 504]:
            return self._retry(request, f"HTTP {response.status}", response.status)
        
        # Reset retry count on success
        self.retry_counts.pop(request.url, None)
        return response
    
    def process_exception(self, request, exception):
        if not self.enabled:
            return
        
        return self._retry(request, str(exception), None)
    
    def _retry(self, request, reason, status_code=None):
        """Retry with exponential backoff, fallback to Playwright for 403/429"""
        from scrapy_playwright.page import PageMethod
        
        spider = self.crawler.spider if self.crawler else None
        retry_count = self.retry_counts.get(request.url, 0) + 1
        
        if retry_count <= self.max_retries:
            # Calculate backoff: 2^retry seconds
            backoff = 2 ** retry_count
            
            if spider:
                spider.logger.info(f"🔄 Retry {retry_count}/{self.max_retries} for {request.url} after {backoff}s - {reason}")
            
            self.retry_counts[request.url] = retry_count
            
            # Create new request
            new_request = request.copy()
            new_request.dont_filter = True
            new_request.meta['retry_count'] = retry_count
            new_request.priority = request.priority + 1
            
            # After 1 retry with 403/429, switch to Playwright (bot-protected sites)
            if status_code in self.playwright_fallback_codes and retry_count >= 1:
                if not request.meta.get('playwright'):
                    if spider:
                        spider.logger.info(f"🎭 Switching to Playwright for bot-protected site: {request.url}")
                    new_request.meta['playwright'] = True
                    new_request.meta['playwright_include_page'] = True
                    new_request.meta['playwright_page_goto_kwargs'] = {
                        'wait_until': 'domcontentloaded',
                        'timeout': 60000,
                    }
                    new_request.meta['playwright_page_methods'] = [
                        PageMethod('wait_for_load_state', 'domcontentloaded'),
                        PageMethod('wait_for_timeout', 2000),
                    ]
            
            return new_request
        
        # Max retries reached
        if spider:
            spider.logger.error(f"❌ Max retries reached for {request.url}")
        self.retry_counts.pop(request.url, None)
        # Return empty response instead of None to avoid middleware error
        from scrapy.http import HtmlResponse
        return HtmlResponse(url=request.url, status=status_code if status_code else 403, body=b'')


# ============================================================
# 8️⃣ PARALLEL EXTRACTION MANAGER
# ============================================================
class ParallelExtractionMiddleware:
    """Manage parallel extraction of multiple fields"""
    
    def __init__(self):
        self.enabled = Config.PARALLEL_EXTRACTION
        self.workers = Config.PARALLEL_WORKERS
        self.batch_size = Config.PARALLEL_BATCH_SIZE
        self.crawler = None
    
    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls()
        middleware.crawler = crawler
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware
    
    def spider_opened(self, spider):
        if self.enabled:
            spider.logger.info(f"⚡ Parallel Extraction: ENABLED ({self.workers} workers, batch size: {self.batch_size})")
        else:
            spider.logger.info("⚡ Parallel Extraction: DISABLED")