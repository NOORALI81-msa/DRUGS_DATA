# geometric_crawler/settings.py
import os
import warnings
import logging
from dotenv import load_dotenv

# from geometric_crawler.config import Config
from .config import Config

load_dotenv()

# Suppress asyncio warnings from scrapy-playwright cleanup (harmless on Windows)
warnings.filterwarnings("ignore", message="coroutine .* was never awaited")
warnings.filterwarnings("ignore", category=RuntimeWarning, module="asyncio")
logging.getLogger("asyncio").setLevel(logging.CRITICAL)

BOT_NAME = 'geometric_crawler'
SPIDER_MODULES = ['geometric_crawler.spiders']
NEWSPIDER_MODULE = 'geometric_crawler.spiders'

# ============================================================
# PERFORMANCE (optimized for speed)
# ============================================================
CONCURRENT_REQUESTS = Config.CONCURRENT_REQUESTS
CONCURRENT_REQUESTS_PER_DOMAIN = Config.CONCURRENT_REQUESTS_PER_DOMAIN
DOWNLOAD_DELAY = Config.DOWNLOAD_DELAY
RANDOMIZE_DOWNLOAD_DELAY = True

# AutoThrottle - balanced for speed while avoiding bans
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 0.25
AUTOTHROTTLE_MAX_DELAY = 3.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 8.0

# Connection optimization
DNS_TIMEOUT = 15
DOWNLOAD_TIMEOUT = 30
DOWNLOAD_MAXSIZE = 10485760  # 10MB max
DOWNLOAD_WARNSIZE = 5242880  # Warn at 5MB

# ============================================================
# PLAYWRIGHT
# ============================================================
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
ASYNCIO_EVENT_LOOP = "asyncio.SelectorEventLoop"

DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": True,
    "timeout": 45000,  # 45s - reduced with faster domcontentloaded strategy
    "args": [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-blink-features=AutomationControlled",
    ]
}

PLAYWRIGHT_BROWSER_CREATE_ARGS = {
    "ignore_https_errors": True,
}

# Playwright page timeout settings
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 60000  # 60 seconds

# ============================================================
# COMPLETE MIDDLEWARE STACK (All protections)
# ============================================================
DOWNLOADER_MIDDLEWARES = {
    # Custom middlewares in order
    'geometric_crawler.middlewares.UserAgentRotationMiddleware': 200,
    'geometric_crawler.middlewares.ProxyRotationMiddleware': 250,
    'geometric_crawler.middlewares.RequestSigningMiddleware': 275,
    'geometric_crawler.middlewares.RandomDelayMiddleware': 300,
    'geometric_crawler.middlewares.BrowserFingerprintMiddleware': 325,
    'geometric_crawler.middlewares.CookieRotationMiddleware': 350,
    'geometric_crawler.middlewares.SmartRetryMiddleware': 400,
    'geometric_crawler.middlewares.ParallelExtractionMiddleware': 450,
    
    # Disable default middlewares
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
    'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': None,
}

# ============================================================
# PIPELINES
# ============================================================
ITEM_PIPELINES = {
    'geometric_crawler.pipelines.JsonPipeline': 300,
    'geometric_crawler.pipelines.CsvPipeline': 301,
}

# ============================================================
# LOGGING
# ============================================================
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'

# ============================================================
# RETRY SETTINGS
# ============================================================
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 400, 403, 404, 408, 429]