# geometric_crawler/fast_parser.py
"""
UNIVERSAL Fast HTML parser using selectolax (10-20x faster than lxml/parsel)
Dynamically learns patterns from ANY website - no hardcoding.
"""
from selectolax.parser import HTMLParser
import re
import hashlib
import json
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
from urllib.parse import urlparse


class FastParser:
    """
    Universal selectolax-based fast HTML parser.
    Learns extraction patterns dynamically from any website.
    """
    
    def __init__(self):
        # Pattern learning storage - adapts to ANY website
        self.learned_selectors = {}  # domain -> {field: [selectors that worked]}
        self.layout_patterns = {}    # layout_hash -> extraction_config
        self.domain_structures = {}  # domain -> detected structure type
        self.extraction_history = defaultdict(list)  # track what worked
        
        # Universal heading patterns (work on any site)
        self.heading_tags = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']
        self.content_tags = ['p', 'li', 'span', 'div', 'article', 'section']
        
        # Universal field patterns - keywords that indicate field types
        self.field_indicators = {
            'title': ['title', 'name', 'heading', 'product-name', 'drug-name', 'item-name'],
            'price': ['price', 'cost', 'mrp', 'amount', '₹', '$', '€', '£', 'rate'],
            'description': ['description', 'desc', 'about', 'overview', 'summary', 'intro'],
            'rating': ['rating', 'stars', 'score', 'review-score'],
            'brand': ['brand', 'manufacturer', 'company', 'by', 'made-by', 'seller'],
            'category': ['category', 'type', 'genre', 'class'],
            'availability': ['availability', 'stock', 'in-stock', 'out-of-stock'],
            'image': ['image', 'img', 'photo', 'picture', 'thumbnail'],
        }
    
    def parse(self, html: str) -> HTMLParser:
        """Create a fast parser instance"""
        return HTMLParser(html)
    
    def extract_text(self, node) -> str:
        """Extract clean text from a node"""
        if node is None:
            return ""
        text = node.text(separator=' ', strip=True)
        return re.sub(r'\s+', ' ', text).strip() if text else ""
    
    def get_node_signature(self, node) -> str:
        """Generate a signature for a node based on its attributes"""
        if not node:
            return ""
        attrs = node.attributes or {}
        tag = node.tag or ''
        classes = attrs.get('class', '')
        id_attr = attrs.get('id', '')
        return f"{tag}.{classes}#{id_attr}"
    
    # ========================================================================
    # UNIVERSAL EXTRACTION - Works on ANY website
    # ========================================================================
    
    def extract_universal(self, html: str, url: str = None) -> Dict[str, Any]:
        """
        Universal extraction that works on ANY website.
        Combines multiple strategies:
        1. JSON-LD structured data (if available)
        2. Open Graph / Meta tags
        3. Semantic HTML structure
        4. Learned patterns for this domain
        5. Generic heading-based section extraction
        """
        tree = HTMLParser(html)
        data = {}
        domain = urlparse(url).netloc if url else 'unknown'
        
        # Strategy 1: JSON-LD (highest quality structured data)
        json_ld_data = self._extract_json_ld(tree)
        if json_ld_data:
            data.update(json_ld_data)
        
        # Strategy 2: Open Graph & Meta tags
        meta_data = self._extract_meta_tags(tree)
        for key, value in meta_data.items():
            if key not in data or not data[key]:
                data[key] = value
        
        # Strategy 3: Use learned selectors if available for this domain
        if domain in self.learned_selectors:
            learned_data = self._apply_learned_selectors(tree, domain)
            for key, value in learned_data.items():
                if key not in data or not data[key]:
                    data[key] = value
        
        # Strategy 4: Semantic HTML extraction
        semantic_data = self._extract_semantic_html(tree)
        for key, value in semantic_data.items():
            if key not in data or not data[key]:
                data[key] = value
        
        # Strategy 5: Generic section extraction
        sections_data = self._extract_all_sections(tree)
        for key, value in sections_data.items():
            if key not in data or not data[key]:
                data[key] = value
        
        # Learn from this extraction for future pages
        self._learn_patterns(tree, data, domain)
        
        return data
    
    def _extract_json_ld(self, tree: HTMLParser) -> Dict[str, Any]:
        """Extract JSON-LD structured data (universal schema.org)"""
        data = {}
        
        for script in tree.css('script[type="application/ld+json"]'):
            try:
                script_text = script.text()
                if not script_text:
                    continue
                    
                json_data = json.loads(script_text)
                
                # Handle @graph structure
                items = json_data.get('@graph', [json_data]) if isinstance(json_data, dict) else [json_data]
                
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    
                    item_type = str(item.get('@type', '')).lower()
                    
                    # Universal field mappings from schema.org
                    field_mappings = {
                        'name': 'title',
                        'headline': 'title',
                        'description': 'description',
                        'image': 'image',
                        'url': 'url',
                        'brand': 'brand',
                        'manufacturer': 'manufacturer',
                        'author': 'author',
                        'datePublished': 'date_published',
                        'dateModified': 'date_modified',
                        'articleBody': 'full_content',
                        # Product specific
                        'sku': 'sku',
                        'gtin': 'gtin',
                        'mpn': 'mpn',
                        # Drug/Medicine specific
                        'activeIngredient': 'active_ingredient',
                        'dosageForm': 'dosage_form',
                        'prescriptionStatus': 'prescription_status',
                        'sideEffects': 'side_effects',
                        'contraindications': 'contraindications',
                        'warnings': 'warnings',
                        'indication': 'indication',
                        # Restaurant/Food specific
                        'servesCuisine': 'cuisine',
                        'priceRange': 'price_range',
                        'address': 'address',
                        'telephone': 'phone',
                        'openingHours': 'hours',
                        # Review/Rating
                        'aggregateRating': 'rating',
                        'reviewCount': 'review_count',
                    }
                    
                    for json_key, data_key in field_mappings.items():
                        if json_key in item:
                            value = item[json_key]
                            # Handle nested objects
                            if isinstance(value, dict):
                                value = value.get('name', '') or value.get('@value', '') or str(value)
                            elif isinstance(value, list):
                                value = ' | '.join(str(v.get('name', v) if isinstance(v, dict) else v) for v in value)
                            if value and data_key not in data:
                                data[data_key] = str(value)  # Clean key, no prefix
                    
                    # Extract price from offers
                    offers = item.get('offers', {})
                    if isinstance(offers, dict):
                        if 'price' in offers:
                            data['price'] = str(offers.get('price', ''))
                        if 'priceCurrency' in offers:
                            data['currency'] = str(offers.get('priceCurrency', ''))
                    elif isinstance(offers, list) and offers:
                        data['price'] = str(offers[0].get('price', ''))
                        data['currency'] = str(offers[0].get('priceCurrency', ''))
                    
                    # Extract rating
                    rating = item.get('aggregateRating', {})
                    if isinstance(rating, dict):
                        if 'ratingValue' in rating:
                            data['rating'] = str(rating.get('ratingValue', ''))
                        if 'reviewCount' in rating:
                            data['review_count'] = str(rating.get('reviewCount', ''))
                            
            except (json.JSONDecodeError, Exception):
                continue
        
        return data
    
    def _extract_meta_tags(self, tree: HTMLParser) -> Dict[str, Any]:
        """Extract Open Graph and standard meta tags"""
        data = {}
        
        # Open Graph tags - clean keys, pipeline adds data_ prefix
        og_mappings = {
            'og:title': 'title',
            'og:description': 'description',
            'og:image': 'image',
            'og:url': 'url',
            'og:type': 'type',
            'og:site_name': 'site_name',
            'og:price:amount': 'price',
            'og:price:currency': 'currency',
        }
        
        for meta in tree.css('meta[property^="og:"]'):
            prop = meta.attributes.get('property', '')
            content = meta.attributes.get('content', '')
            if prop in og_mappings and content:
                data[og_mappings[prop]] = content
        
        # Twitter cards - clean keys
        twitter_mappings = {
            'twitter:title': 'title',
            'twitter:description': 'description',
            'twitter:image': 'image',
        }
        
        for meta in tree.css('meta[name^="twitter:"]'):
            name = meta.attributes.get('name', '')
            content = meta.attributes.get('content', '')
            if name in twitter_mappings and content and twitter_mappings[name] not in data:
                data[twitter_mappings[name]] = content
        
        # Standard meta tags - clean keys
        std_mappings = {
            'description': 'meta_description',
            'keywords': 'keywords',
            'author': 'author',
        }
        
        for meta in tree.css('meta[name]'):
            name = meta.attributes.get('name', '').lower()
            content = meta.attributes.get('content', '')
            if name in std_mappings and content:
                data[std_mappings[name]] = content
        
        return data
    
    def _extract_semantic_html(self, tree: HTMLParser) -> Dict[str, Any]:
        """Extract data from semantic HTML5 elements"""
        data = {}
        
        # Title from h1
        h1 = tree.css_first('h1')
        if h1 and 'title' not in data:
            text = self.extract_text(h1)
            if text and len(text) > 2:
                data['title'] = text
        
        # Main content area
        main = tree.css_first('main') or tree.css_first('article') or tree.css_first('[role="main"]')
        
        # Look for common patterns by class/id names
        for field, indicators in self.field_indicators.items():
            if field in data:
                continue
                
            for indicator in indicators:
                # Try class selector
                for node in tree.css(f'[class*="{indicator}"]'):
                    text = self.extract_text(node)
                    if text and len(text) > 2 and len(text) < 1000:
                        data[field] = text  # Clean key
                        break
                
                if field in data:
                    break
                    
                # Try id selector
                node = tree.css_first(f'[id*="{indicator}"]')
                if node:
                    text = self.extract_text(node)
                    if text and len(text) > 2 and len(text) < 1000:
                        data[field] = text  # Clean key
                        break
        
        return data
    
    def _extract_all_sections(self, tree: HTMLParser) -> Dict[str, Any]:
        """
        Universal section extraction based on heading structure.
        Works on ANY website by following heading hierarchy.
        """
        data = {}
        sections = {}
        current_heading = 'Overview'
        current_content = []
        seen_texts = set()
        
        # Find main content container
        main = (tree.css_first('main') or tree.css_first('article') or 
                tree.css_first('[role="main"]') or tree.css_first('#content') or
                tree.css_first('#main-content') or tree.css_first('.content') or
                tree.body)
        
        if not main:
            return data
        
        # Collect all headings and paragraphs in order
        for node in main.css('h1, h2, h3, h4, h5, h6, p, li'):
            tag = node.tag.lower() if node.tag else ''
            
            # New section on heading
            if tag in self.heading_tags:
                text = self.extract_text(node)
                if text and 2 < len(text) < 200:
                    # Skip navigation/menu items
                    parent_classes = str(node.parent.attributes.get('class', '') if node.parent else '').lower()
                    if any(skip in parent_classes for skip in ['nav', 'menu', 'sidebar', 'footer', 'header']):
                        continue
                    
                    # Save previous section
                    if current_content:
                        sections[current_heading] = '\n'.join(current_content)
                    
                    current_heading = text
                    current_content = []
            
            # Add content
            elif tag in ['p', 'li']:
                text = self.extract_text(node)
                if text and len(text) > 10:
                    text_hash = hash(text)
                    if text_hash not in seen_texts:
                        seen_texts.add(text_hash)
                        prefix = '• ' if tag == 'li' else ''
                        current_content.append(f"{prefix}{text}")
        
        # Save last section
        if current_content:
            sections[current_heading] = '\n'.join(current_content)
        
        # Convert sections to data fields
        if sections:
            # Create full_content
            full_parts = []
            summary_parts = []
            
            for heading, content in sections.items():
                full_parts.append(f"## {heading}\n\n{content}")
                
                # Summary from first few sections
                if len(summary_parts) < 3:
                    first_line = content.split('\n')[0] if content else ''
                    if first_line and len(first_line) > 20:
                        summary_parts.append(first_line)
                
                # Store individual sections with sanitized names (clean keys)
                safe_heading = re.sub(r'[^a-z0-9]+', '_', heading.lower()).strip('_')[:50]
                if safe_heading:
                    data[safe_heading] = content  # Clean key, pipeline adds data_ prefix
            
            data['full_content'] = '\n\n'.join(full_parts)
            if summary_parts:
                data['summary'] = ' '.join(summary_parts)
        
        return data
    
    def _apply_learned_selectors(self, tree: HTMLParser, domain: str) -> Dict[str, Any]:
        """Apply previously learned selectors for this domain"""
        data = {}
        
        if domain not in self.learned_selectors:
            return data
        
        for field, selectors in self.learned_selectors[domain].items():
            for selector in selectors:
                try:
                    node = tree.css_first(selector)
                    if node:
                        text = self.extract_text(node)
                        if text and len(text) > 2:
                            data[field] = text
                            break
                except:
                    continue
        
        return data
    
    def _learn_patterns(self, tree: HTMLParser, extracted_data: Dict, domain: str):
        """
        Learn which selectors worked for this extraction.
        Stores successful patterns for future use on same domain.
        """
        if domain == 'unknown':
            return
        
        if domain not in self.learned_selectors:
            self.learned_selectors[domain] = defaultdict(list)
        
        # For each extracted field, try to find what selector would match it
        for field, value in extracted_data.items():
            if not isinstance(value, str) or len(value) < 5:
                continue
            
            # Search for elements containing this text
            value_snippet = value[:100]  # First 100 chars for matching
            
            for node in tree.css('h1, h2, h3, p, span, div'):
                node_text = self.extract_text(node)
                if value_snippet in node_text:
                    # Build a selector from this node
                    selector = self._build_selector(node)
                    if selector and selector not in self.learned_selectors[domain][field]:
                        self.learned_selectors[domain][field].append(selector)
                        # Keep only top 3 selectors per field
                        self.learned_selectors[domain][field] = self.learned_selectors[domain][field][:3]
                    break
    
    def _build_selector(self, node) -> Optional[str]:
        """Build a CSS selector from a node"""
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
        
        # Basic tag selector with parent context
        if node.parent:
            parent_tag = node.parent.tag
            parent_attrs = node.parent.attributes or {}
            if 'class' in parent_attrs:
                parent_classes = parent_attrs['class'].split()[:1]
                if parent_classes:
                    return f"{parent_tag}.{parent_classes[0]} {tag}"
        
        return None
    
    # ========================================================================
    # LINK EXTRACTION - Universal
    # ========================================================================
    
    def extract_links_fast(self, html: str, base_url: str) -> List[str]:
        """Fast universal link extraction"""
        from urllib.parse import urljoin
        
        tree = HTMLParser(html)
        links = set()
        
        # All anchor tags
        for a in tree.css('a[href]'):
            href = a.attributes.get('href', '')
            if href and not href.startswith('#') and not href.startswith('javascript:'):
                links.add(urljoin(base_url, href))
        
        # Data attributes (common in SPAs and card layouts)
        for attr in ['data-href', 'data-url', 'data-link', 'data-navigate']:
            for node in tree.css(f'[{attr}]'):
                href = node.attributes.get(attr, '')
                if href:
                    links.add(urljoin(base_url, href))
        
        # Filter out unwanted links
        filtered = []
        for link in links:
            # Skip media files
            if any(ext in link.lower() for ext in ['.pdf', '.jpg', '.png', '.gif', '.mp4', '.mp3', '.zip']):
                continue
            # Skip mailto/tel
            if link.startswith('mailto:') or link.startswith('tel:'):
                continue
            filtered.append(link)
        
        return filtered
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    def learn_pattern(self, domain: str, layout_hash: str, selectors: Dict):
        """Manually add learned patterns for a domain"""
        self.learned_selectors[domain] = selectors
        self.layout_patterns[layout_hash] = selectors
    
    def has_learned(self, domain: str) -> bool:
        """Check if we've learned patterns for this domain"""
        return domain in self.learned_selectors and len(self.learned_selectors[domain]) > 0
    
    def get_learned_domains(self) -> List[str]:
        """Get list of domains we've learned patterns for"""
        return list(self.learned_selectors.keys())
    
    def export_patterns(self) -> Dict:
        """Export learned patterns for persistence"""
        return {
            'learned_selectors': dict(self.learned_selectors),
            'layout_patterns': self.layout_patterns,
        }
    
    def import_patterns(self, patterns: Dict):
        """Import previously learned patterns"""
        if 'learned_selectors' in patterns:
            for domain, selectors in patterns['learned_selectors'].items():
                self.learned_selectors[domain] = defaultdict(list, selectors)
        if 'layout_patterns' in patterns:
            self.layout_patterns.update(patterns['layout_patterns'])


# Singleton instance for reuse across spider
fast_parser = FastParser()
