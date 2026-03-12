# geometric_crawler/spider_generator.py
"""
LLM-Powered Spider Generator
============================
Creates custom spider configurations for any website using AI analysis.
Now fetches detail page HTML to map sections to CSS selectors.
"""

import re
import os
import json
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime
from urllib.parse import urlparse
from selectolax.parser import HTMLParser

# Try importing requests
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False


@dataclass
class SpiderConfig:
    """Generated spider configuration"""
    name: str
    domain: str
    description: str
    base_url: str
    start_urls: List[str] = field(default_factory=list)
    
    # URL patterns
    follow_patterns: List[str] = field(default_factory=list)
    deny_patterns: List[str] = field(default_factory=list)
    pagination_pattern: Optional[str] = None
    
    # Extraction fields with CSS selectors
    fields: Dict[str, Dict] = field(default_factory=dict)
    
    # Output columns (user-specified order for CSV)
    output_columns: List[str] = field(default_factory=list)
    
    # Spider settings
    max_pages: int = 100
    max_depth: int = 3
    delay: float = 1.0
    concurrent_requests: int = 8
    
    # Advanced
    requires_javascript: bool = False
    custom_headers: Dict[str, str] = field(default_factory=dict)
    cookies: Dict[str, str] = field(default_factory=dict)
    
    # Metadata
    generated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    llm_provider: str = ""
    llm_model: str = ""
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)
    
    def save(self, filepath: str):
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(self.to_json())


class SpiderGenerator:
    """
    LLM-powered spider generator for creating custom web scrapers.
    Fetches detail page HTML to map sections to actual CSS selectors.
    """
    
    LLM_PROVIDERS = {
        "ollama": {"endpoint": "http://localhost:11434/api/generate"},
        "openai": {"endpoint": "https://api.openai.com/v1/chat/completions"},
        "anthropic": {"endpoint": "https://api.anthropic.com/v1/messages"},
        "gemini": {"endpoint_template": "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"},
        "deepseek": {"endpoint": "https://api.deepseek.com/v1/chat/completions"},
    }
    
    def __init__(self, llm_provider: str = "ollama", llm_model: str = None, api_key: str = None):
        self.llm_provider = llm_provider.lower()
        self.llm_model = llm_model or self._default_model()
        self.api_key = api_key or os.environ.get(f"{self.llm_provider.upper()}_API_KEY", "")
        
    def _default_model(self) -> str:
        defaults = {
            "ollama": "llama3",
            "openai": "gpt-4o-mini",
            "anthropic": "claude-3-5-sonnet-20241022",
            "gemini": "gemini-1.5-flash",
            "deepseek": "deepseek-chat",
        }
        return defaults.get(self.llm_provider, "llama3")
    
    def analyze_detail_page(self, detail_url: str) -> Tuple[Dict[str, List[str]], bool]:
        """
        Fetch and analyze detail page to find section headings and their content.
        
        Returns:
            Tuple of (section_to_selectors_map, requires_javascript)
        """
        print(f"🔍 Analyzing detail page: {detail_url}")
        
        section_selectors = {}
        requires_javascript = False
        
        if not REQUESTS_AVAILABLE:
            print("⚠️ requests not available, cannot analyze page")
            return {}, False
        
        try:
            # Try normal HTTP request first
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(detail_url, timeout=10, headers=headers)
            
            if response.status_code != 200:
                print(f"⚠️ HTTP {response.status_code}, page may need JavaScript")
                requires_javascript = True
                # Try with a different user agent
                headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
                response = requests.get(detail_url, timeout=10, headers=headers)
                if response.status_code != 200:
                    return {}, True
            
            html = response.text
            tree = HTMLParser(html)
            
            # Find all headings (h1, h2, h3, h4)
            headings = []
            for tag in ['h1', 'h2', 'h3', 'h4']:
                for node in tree.css(tag):
                    text = node.text(strip=True)
                    if text and len(text) > 2 and len(text) < 200:
                        # Skip navigation/menu headings
                        parent_classes = str(node.parent.attributes.get('class', '') if node.parent else '').lower()
                        if any(skip in parent_classes for skip in ['nav', 'menu', 'sidebar', 'header', 'footer']):
                            continue
                        headings.append({
                            'text': text,
                            'tag': tag,
                            'node': node
                        })
            
            # For each heading, find the content that follows
            for heading in headings:
                section_name = heading['text'].lower().strip()
                # Clean section name for field key
                field_key = re.sub(r'[^a-z0-9]+', '_', section_name).strip('_')[:50]
                
                # Find content selectors
                content_selectors = []
                
                # Strategy 1: Next sibling elements
                current = heading['node'].next
                content_parts = []
                while current and current.tag not in ['h1', 'h2', 'h3', 'h4']:
                    if current.tag in ['p', 'div', 'ul', 'ol', 'table']:
                        # Build a selector for this content element
                        selector = self._build_selector(current)
                        if selector:
                            content_selectors.append(selector)
                    current = current.next
                
                # Strategy 2: Parent container with heading
                parent = heading['node'].parent
                if parent:
                    parent_selector = self._build_selector(parent)
                    if parent_selector:
                        content_selectors.append(f"{parent_selector}")
                
                # Strategy 3: ID-based selectors (common in medical sites)
                heading_id = heading['node'].attributes.get('id', '')
                if heading_id:
                    content_selectors.append(f"#{heading_id} + *")
                    content_selectors.append(f"section[id*='{heading_id}']")
                
                # Strategy 4: Class-based selectors
                heading_classes = heading['node'].attributes.get('class', '')
                if heading_classes:
                    first_class = heading_classes.split()[0]
                    content_selectors.append(f".{first_class} + *")
                    content_selectors.append(f"[class*='{first_class}'] + div")
                
                # Store unique selectors
                if content_selectors:
                    section_selectors[field_key] = list(set(content_selectors))[:5]  # Keep top 5
            
            # Also look for common drug information sections by keyword
            common_sections = {
                'uses': ['uses', 'indications', 'why', 'used for'],
                'dosage': ['dosage', 'dosing', 'how to take', 'administration'],
                'side_effects': ['side effects', 'adverse', 'reactions'],
                'warnings': ['warnings', 'precautions', 'caution'],
                'interactions': ['interactions', 'interact'],
                'mechanism': ['mechanism', 'pharmacology', 'how it works'],
                'pregnancy': ['pregnancy', 'breastfeeding', 'lactation'],
                'overdose': ['overdose', 'over dosage'],
                'storage': ['storage', 'how to store'],
            }
            
            # If we didn't find enough sections, try keyword-based search
            if len(section_selectors) < 3:
                for field_key, keywords in common_sections.items():
                    if field_key not in section_selectors:
                        for keyword in keywords:
                            # Look for elements containing the keyword
                            for elem in tree.css(f'*:contains("{keyword}")'):
                                tag = elem.tag
                                if tag in ['h2', 'h3', 'h4', 'strong', 'b']:
                                    # This might be a section heading
                                    content_selector = self._find_content_for_heading(elem)
                                    if content_selector:
                                        section_selectors[field_key] = [content_selector]
                                        break
                            if field_key in section_selectors:
                                break
            
            print(f"✅ Found {len(section_selectors)} sections with selectors")
            return section_selectors, requires_javascript
            
        except Exception as e:
            print(f"⚠️ Error analyzing detail page: {e}")
            return {}, False
    
    def _build_selector(self, node) -> Optional[str]:
        """Build a CSS selector for a node"""
        if not node or not node.tag:
            return None
        
        attrs = node.attributes or {}
        tag = node.tag
        
        # Prefer ID selector
        if 'id' in attrs and attrs['id']:
            return f"#{attrs['id']}"
        
        # Use class selector
        if 'class' in attrs and attrs['class']:
            classes = attrs['class'].split()
            # Filter out dynamic/hash classes
            stable_classes = [c for c in classes if not re.search(r'[_-][a-f0-9]{6,}', c)]
            if stable_classes:
                return f"{tag}.{'.'.join(stable_classes[:2])}"
        
        # Try data attributes
        for attr in ['data-testid', 'data-section', 'data-component']:
            if attr in attrs and attrs[attr]:
                return f"[{attr}='{attrs[attr]}']"
        
        return None
    
    def _find_content_for_heading(self, heading_node) -> Optional[str]:
        """Find content selector for a heading node"""
        # Look at next siblings
        current = heading_node.next
        while current and current.tag not in ['h1', 'h2', 'h3', 'h4']:
            if current.tag in ['p', 'div', 'ul', 'ol']:
                selector = self._build_selector(current)
                if selector:
                    return selector
            current = current.next
        
        # Look at parent container
        parent = heading_node.parent
        if parent:
            selector = self._build_selector(parent)
            if selector:
                return selector
        
        return None
    
    def generate_from_patterns(self, 
                               master_url: str,
                               list_url: str,
                               detail_url: str,
                               follow_patterns: List[str],
                               deny_patterns: List[str],
                               selected_sections: List[str],
                               pagination_pattern: Optional[str] = None,
                               follow_pagination: bool = False) -> Tuple[SpiderConfig, str]:
        """
        Generate spider config and code from detected URL patterns.
        Fetches detail page to map sections to actual CSS selectors.
        
        Args:
            master_url: Master/listing page URL
            list_url: Product listing/card page URL
            detail_url: Detailed product/drug page URL
            follow_patterns: List of URL patterns to follow
            deny_patterns: List of URL patterns to deny
            selected_sections: List of sections to extract
            pagination_pattern: Pagination URL pattern
            follow_pagination: Whether to follow pagination
        
        Returns:
            Tuple of (SpiderConfig, generated_code_string)
        """
        parsed = urlparse(master_url)
        domain = parsed.netloc
        base_url = f"{parsed.scheme}://{domain}"
        
        # Step 1: Analyze the detail page to map sections to CSS selectors
        print(f"\n🔍 Step 1: Analyzing detail page to map sections...")
        section_selectors, requires_javascript = self.analyze_detail_page(detail_url)
        
        # Step 2: Match selected sections with discovered selectors
        fields = {}
        matched_sections = []
        unmatched_sections = []
        
        for section in selected_sections:
            section_key = re.sub(r'[^a-z0-9]+', '_', section.lower()).strip('_')[:50]
            
            # Try to find matching selectors
            found_selectors = []
            
            # Direct match by key
            if section_key in section_selectors:
                found_selectors = section_selectors[section_key]
                matched_sections.append(section)
            
            # Fuzzy match (section name contains key or vice versa)
            else:
                for discovered_key, selectors in section_selectors.items():
                    if (section_key in discovered_key or discovered_key in section_key or
                        any(word in discovered_key for word in section.lower().split())):
                        found_selectors = selectors
                        matched_sections.append(section)
                        break
            
            # If still no match, use generic selectors
            if not found_selectors:
                unmatched_sections.append(section)
                # Generate generic selectors based on section name
                generic_selectors = [
                    f"h2:contains('{section}') + *",
                    f"h3:contains('{section}') + *",
                    f"*:contains('{section}'):has(p)",
                    f"[class*='{section_key}']",
                    f"[id*='{section_key}']",
                ]
                found_selectors = generic_selectors
            
            # Create field configuration
            fields[section_key] = {
                "selectors": found_selectors[:5],  # Limit to 5 selectors
                "type": "html",
                "required": section in ["Drug Name", "drug_name", "title", "Uses"],
                "description": f"{section} information"
            }
        
        # Step 3: Add drug name field if not present
        if "drug_name" not in fields and "title" not in fields:
            # Try to find drug name selector
            drug_name_selectors = []
            for selector_key in ['drug-name', 'page-title', 'h1']:
                if selector_key in section_selectors:
                    drug_name_selectors = section_selectors[selector_key]
                    break
            
            if not drug_name_selectors:
                drug_name_selectors = [
                    "h1",
                    ".drug-name",
                    "#drug-name",
                    ".page-title",
                    "[itemprop='name']"
                ]
            
            fields["drug_name"] = {
                "selectors": drug_name_selectors,
                "type": "text",
                "required": True,
                "description": "Name of the drug"
            }
        
        # Step 4: Clean follow patterns
        clean_follow_patterns = []
        for pattern in follow_patterns:
            # Clean the pattern to get the base path
            clean = re.sub(r'[a-zA-Z0-9_-]+\.\w+$', '', pattern)  # Remove filename
            clean = re.sub(r'[a-zA-Z0-9_-]+$', '', clean)  # Remove trailing ID
            clean = re.sub(r'\?.*$', '', clean)  # Remove query params
            clean = re.sub(r'\.\.', '', clean)  # Remove relative paths
            
            # Ensure it starts with /
            if not clean.startswith('/'):
                clean = '/' + clean.lstrip('./')
            
            # Escape for regex and add pattern for any ID/filename
            clean_pattern = re.escape(clean) + r'[^/\s]+\.?\w*$'
            clean_follow_patterns.append(clean_pattern)
        
        # If no clean patterns, use originals
        if not clean_follow_patterns:
            clean_follow_patterns = follow_patterns
        
        # Step 5: Build summary
        print(f"\n📊 Section Mapping Summary:")
        print(f"  ✅ Matched: {', '.join(matched_sections)}")
        if unmatched_sections:
            print(f"  ⚠️ Using generic selectors for: {', '.join(unmatched_sections)}")
        
        # Step 6: Generate spider code using LLM or fallback
        spider_code = self._generate_spider_code(
            domain=domain,
            base_url=base_url,
            start_urls=[master_url, list_url, detail_url],
            follow_patterns=clean_follow_patterns,
            deny_patterns=deny_patterns,
            fields=fields,
            pagination_pattern=pagination_pattern if follow_pagination else None,
            requires_javascript=requires_javascript
        )
        
        # Step 7: Create config
        config = SpiderConfig(
            name=f"{domain.replace('.', '_').replace('-', '_')}_spider",
            domain=domain,
            description=f"Auto-generated spider for {domain} extracting: {', '.join(selected_sections[:5])}",
            base_url=base_url,
            start_urls=[master_url, list_url, detail_url],
            follow_patterns=clean_follow_patterns,
            deny_patterns=deny_patterns,
            pagination_pattern=pagination_pattern if follow_pagination else None,
            fields=fields,
            output_columns=["url", "domain", "scraped_at"] + list(fields.keys()),
            max_pages=500,
            max_depth=3,
            delay=1.5,
            concurrent_requests=8,
            requires_javascript=requires_javascript,
            llm_provider=self.llm_provider,
            llm_model=self.llm_model,
        )
        
        return config, spider_code
    
    def _generate_spider_code(self, domain: str, base_url: str, start_urls: List[str],
                               follow_patterns: List[str], deny_patterns: List[str],
                               fields: Dict, pagination_pattern: Optional[str],
                               requires_javascript: bool) -> str:
        """Generate spider code with actual CSS selectors for each field"""
        
        # Build field extraction code
        fields_code = []
        for field_name, field_config in fields.items():
            selectors = field_config.get("selectors", [])
            if selectors:
                selector_lines = []
                for i, selector in enumerate(selectors):
                    indent = "            " if i == 0 else "            "
                    selector_lines.append(f'{indent}"{selector}"')
                selectors_str = ",\n".join(selector_lines)
                
                field_type = field_config.get("type", "html")
                
                if field_type == "text":
                    fields_code.append(f'''
        # Extract {field_name}
        for selector in [{selectors_str}]:
            value = response.css(selector + "::text").get()
            if value and value.strip():
                data["{field_name}"] = value.strip()
                break''')
                else:  # html
                    fields_code.append(f'''
        # Extract {field_name} (HTML)
        for selector in [{selectors_str}]:
            value = response.css(selector).get()
            if value and value.strip():
                data["{field_name}"] = value.strip()
                break''')
        
        # Build follow patterns for rules
        follow_str = ", ".join([f'r"{p}"' for p in follow_patterns])
        deny_str = ", ".join([f'r"{p}"' for p in deny_patterns]) if deny_patterns else ""
        
        rules_code = f'''
    rules = (
        Rule(
            LinkExtractor(
                allow=[{follow_str}],
                deny=[{deny_str}]
            ),
            callback='parse_item',
            follow=True
        ),
    )'''
        
        # Add pagination rule if specified
        if pagination_pattern:
            rules_code += f'''
        Rule(
            LinkExtractor(
                allow=[r"{pagination_pattern}"],
                deny=[{deny_str}]
            ),
            callback='parse_item',
            follow=True
        ),'''
        
        # Build start URLs string
        start_urls_str = ",\n        ".join([f'"{url}"' for url in start_urls])
        
        # Build custom settings
        if requires_javascript:
            custom_settings = '''
    custom_settings = {
        'DOWNLOAD_DELAY': 2.0,
        'CONCURRENT_REQUESTS': 4,
        'ROBOTSTXT_OBEY': True,
        'PLAYWRIGHT': True,
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True,
            'timeout': 30000,
        },
    }'''
        else:
            custom_settings = '''
    custom_settings = {
        'DOWNLOAD_DELAY': 1.5,
        'CONCURRENT_REQUESTS': 8,
        'ROBOTSTXT_OBEY': True,
    }'''
        
        # Generate the complete spider code
        code = f'''"""
Spider: {domain.replace('.', '_')}_spider
Domain: {domain}
Generated: {datetime.now().isoformat()}
Fields: {', '.join(fields.keys())}
"""

import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from datetime import datetime
import re


class {domain.replace('.', '_').title().replace('_', '')}Spider(CrawlSpider):
    """
    Auto-generated spider for {domain}
    Extracts: {', '.join(fields.keys())}
    """
    
    name = "{domain.replace('.', '_')}_spider"
    allowed_domains = ["{domain}"]
    start_urls = [
        {start_urls_str}
    ]
    {custom_settings}
    {rules_code}
    
    def parse_start_url(self, response):
        """Handle start URLs"""
        yield from self.parse_item(response)
    
    def parse_item(self, response):
        """
        Extract structured data from the page using discovered CSS selectors
        """
        data = {{
            'url': response.url,
            'domain': self.allowed_domains[0],
            'scraped_at': datetime.now().isoformat(),
            'status_code': response.status,
            'title': response.css('title::text').get('').strip(),
        }}
        
        # Extract fields with multiple selector fallbacks
        {''.join(fields_code)}
        
        # Remove empty values
        data = {{k: v for k, v in data.items() if v and v.strip()}}
        
        yield data
'''
        
        return code
    
    def clean_spider_code(self, code: str) -> str:
        """Clean LLM-generated code"""
        if not code:
            return ""
        
        # Remove markdown code blocks
        code = re.sub(r'```python\s*', '', code)
        code = re.sub(r'```\s*', '', code)
        code = re.sub(r'```', '', code)
        
        # Remove any HTML tags
        code = re.sub(r'<[^>]+>', '', code)
        
        # Ensure proper imports
        if 'import scrapy' not in code:
            code = "import scrapy\nfrom datetime import datetime\n" + code
        
        return code.strip()