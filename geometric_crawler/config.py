# geometric_crawler/config.py
"""
Central configuration for all protection techniques and features
Toggle features ON/OFF easily
"""
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # ============================================================
    # 🔧 FEATURE TOGGLES - Turn ON/OFF easily
    # ============================================================
    
    # Parallel Processing (optimized for speed)
    PARALLEL_EXTRACTION = os.getenv('PARALLEL_EXTRACTION', 'true').lower() == 'true'
    PARALLEL_WORKERS = int(os.getenv('PARALLEL_WORKERS', 8))
    PARALLEL_BATCH_SIZE = int(os.getenv('PARALLEL_BATCH_SIZE', 20))
    
    # Protection Techniques
    ENABLE_USER_AGENT_ROTATION = os.getenv('ENABLE_USER_AGENT_ROTATION', 'true').lower() == 'true'
    ENABLE_PROXY_ROTATION = False
    ENABLE_REQUEST_SIGNING = os.getenv('ENABLE_REQUEST_SIGNING', 'true').lower() == 'true'
    ENABLE_RANDOM_DELAY = os.getenv('ENABLE_RANDOM_DELAY', 'false').lower() == 'true'
    ENABLE_FINGERPRINTING = os.getenv('ENABLE_FINGERPRINTING', 'true').lower() == 'true'
    ENABLE_COOKIE_ROTATION = os.getenv('ENABLE_COOKIE_ROTATION', 'true').lower() == 'true'
    ENABLE_RETRY_MECHANISM = os.getenv('ENABLE_RETRY_MECHANISM', 'true').lower() == 'true'
    
    # Resource Saving
    BLOCK_IMAGES = os.getenv('BLOCK_IMAGES', 'true').lower() == 'true'
    BLOCK_FONTS = os.getenv('BLOCK_FONTS', 'true').lower() == 'true'
    BLOCK_CSS = os.getenv('BLOCK_CSS', 'false').lower() == 'true'
    
    # Repair Options
    ENABLE_PARENT_TRAP = os.getenv('ENABLE_PARENT_TRAP', 'true').lower() == 'true'
    ENABLE_KEYWORD_HUNT = os.getenv('ENABLE_KEYWORD_HUNT', 'true').lower() == 'true'
    ENABLE_VISUAL_PATTERN = os.getenv('ENABLE_VISUAL_PATTERN', 'true').lower() == 'true'
    ENABLE_LLM_REPAIR = os.getenv('ENABLE_LLM_REPAIR', 'false').lower() == 'true'  # Off by default
    
    # Performance (optimized for speed)
    CONCURRENT_REQUESTS = int(os.getenv('CONCURRENT_REQUESTS', 16))
    CONCURRENT_REQUESTS_PER_DOMAIN = int(os.getenv('CONCURRENT_REQUESTS_PER_DOMAIN', 8))
    DOWNLOAD_DELAY = float(os.getenv('DOWNLOAD_DELAY', 0.25))
    MAX_PAGES = int(os.getenv('MAX_PAGES', 1000))
    MAX_DEPTH = int(os.getenv('MAX_DEPTH', 3))
    
    # ============================================================
    # 📋 USER AGENTS DATABASE (Free, no API key needed)
    # ============================================================
    USER_AGENTS = [
        # Windows Chrome
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        
        # Windows Firefox
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
        
        # MacOS Safari
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
        
        # Linux
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
        
        # Mobile - iPhone
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.7 Mobile/15E148 Safari/604.1",
        
        # Mobile - Android
        "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    ]
    
    # ============================================================
    # 🌐 FREE PROXY LIST (Test proxies, replace with better ones)
    # ============================================================
    FREE_PROXIES = [
        # Format: "protocol://ip:port"
        "http://51.158.68.68:8811",
        "http://185.61.152.137:8080",
        "http://134.209.29.120:3128",
        "http://103.47.67.134:8080",
        "http://103.47.67.134:8080",
    ]
    
    # ============================================================
    # 🖐️ BROWSER FINGERPRINTS
    # ============================================================
    FINGERPRINTS = [
        {
            "screen_width": 1920,
            "screen_height": 1080,
            "color_depth": 24,
            "pixel_depth": 24,
            "timezone": "America/New_York",
            "language": "en-US,en;q=0.9",
            "platform": "Win32",
            "hardware_concurrency": 8,
            "device_memory": 8
        },
        {
            "screen_width": 1366,
            "screen_height": 768,
            "color_depth": 24,
            "pixel_depth": 24,
            "timezone": "America/Chicago",
            "language": "en-US,en;q=0.9",
            "platform": "Win64",
            "hardware_concurrency": 4,
            "device_memory": 4
        },
        {
            "screen_width": 2560,
            "screen_height": 1440,
            "color_depth": 30,
            "pixel_depth": 30,
            "timezone": "Europe/London",
            "language": "en-GB,en;q=0.8",
            "platform": "MacIntel",
            "hardware_concurrency": 10,
            "device_memory": 16
        }
    ]