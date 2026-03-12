# geometric_crawler/geometry.py
"""
Geometric extraction engine using Selectolax for lightning-fast parsing
"""
from selectolax.parser import HTMLParser
import re
import json
from typing import Dict, List, Any, Optional

class GeometricExtractor:
    """
    Extracts elements based on their geometric properties and visual patterns
    Uses Selectolax for fast HTML parsing
    """
    
    def __init__(self):
        self.patterns = {
            'price': {
                'keywords': ['₹', '$', '£', '€', 'price', 'mrp', 'cost'],
                'patterns': [r'[₹$£€]\s*\d+[,.]?\d*', r'\d+[,.]?\d*\s*(rupees|rs\.?)']
            },
            'title': {
                'keywords': ['title', 'name', 'product', 'heading'],
                'patterns': []
            },
            'manufacturer': {
                'keywords': ['manufacturer', 'brand', 'company', 'by', 'mfg'],
                'patterns': []
            },
            'description': {
                'keywords': ['description', 'about', 'details', 'summary'],
                'patterns': []
            }
        }
    
    def extract_all_elements(self, html: str) -> List[Dict]:
        """
        Extract ALL elements with their geometric properties
        Uses Selectolax for ultra-fast parsing
        """
        try:
            tree = HTMLParser(html)
        except Exception as e:
            print(f"❌ Error parsing HTML: {e}")
            return []
            
        elements = []
        
        # Get all text-containing elements
        for node in tree.css('*'):
            try:
                # Skip script and style tags
                if node.tag in ['script', 'style', 'meta', 'link']:
                    continue
                
                # Get text content
                text = node.text(strip=True)
                if not text or len(text) < 2:
                    continue
                
                # 🔧 FIX: Count children safely using iter()
                try:
                    children_count = len(list(node.iter()))
                except:
                    children_count = 0
                
                # 🔧 FIX: Ensure class_names is safely extracted
                try:
                    class_value = node.attributes.get('class', '')
                    class_names = class_value.split() if class_value and isinstance(class_value, str) else []
                except Exception as e:
                    print(f"❌ Error splitting class: {e}, value: {class_value}, type: {type(class_value)}")
                    class_names = []
                
                # Get geometric properties
                element = {
                    'tag': node.tag,
                    'text': text[:200],  # Limit text length
                    'html': node.html[:500] if node.html else '',  # Store snippet for repair
                    'attributes': node.attributes or {},
                    'parent': node.parent.tag if node.parent else None,
                    'children_count': children_count,  # 🔧 FIXED
                    'class_names': class_names,  # 🔧 FIXED
                    'id': node.attributes.get('id', '') if node.attributes else '',
                    'href': node.attributes.get('href', '') if node.attributes else '',
                    'src': node.attributes.get('src', '') if node.attributes else '',
                    'alt': node.attributes.get('alt', '') if node.attributes else '',
                }
                
                elements.append(element)
            except Exception as e:
                print(f"❌ Error processing node: {e}")
                continue
        
        # Group elements into containers based on parent-child relationships
        containers = self.group_into_containers(elements)
        
        return containers
    
    def group_into_containers(self, elements: List[Dict]) -> List[Dict]:
        """
        Group elements into logical containers (cards, sections)
        Based on HTML structure, not positions
        """
        containers = []
        
        # Group by common parent (simplified)
        parent_groups = {}
        for elem in elements:
            parent = elem.get('parent', 'root')
            if parent not in parent_groups:
                parent_groups[parent] = []
            parent_groups[parent].append(elem)
        
        # Convert to container format
        for parent, children in parent_groups.items():
            if len(children) >= 2:  # Only consider containers with multiple elements
                containers.append({
                    'type': 'container',
                    'parent_tag': parent,
                    'elements': children,
                    'element_count': len(children)
                })
        
        return containers
    
    def classify_element(self, element: Dict) -> Dict:
        """
        Determine what type of element this is based on content and attributes
        """
        # 🔧 FIX: Safely handle None values
        text_raw = element.get('text', '')
        text = text_raw.lower() if text_raw and isinstance(text_raw, str) else ''
        
        class_names_raw = element.get('class_names', [])
        class_names_list = class_names_raw if class_names_raw and isinstance(class_names_raw, list) else []
        class_names = ' '.join(class_names_list).lower()
        
        element_id = element.get('id', '')
        element_id = element_id.lower() if element_id and isinstance(element_id, str) else ''
        
        classifications = {}
        
        # Check for price
        price_score = 0
        if any(keyword in text for keyword in self.patterns['price']['keywords']):
            price_score += 0.3
        if any(keyword in class_names for keyword in ['price', 'mrp', 'cost']):
            price_score += 0.4
        if any(keyword in element_id for keyword in ['price', 'mrp', 'cost']):
            price_score += 0.3
        if re.search(r'[₹$£€]\s*\d+', text):
            price_score += 0.5
        
        if price_score > 0.5:
            classifications['price'] = min(price_score, 1.0)
        
        # Check for title
        title_score = 0
        if element.get('tag') in ['h1', 'h2', 'h3']:
            title_score += 0.5
        if any(keyword in class_names for keyword in ['title', 'name', 'heading']):
            title_score += 0.4
        if len(text) > 5 and len(text) < 200 and title_score > 0:
            title_score += 0.2
        
        if title_score > 0.5:
            classifications['title'] = min(title_score, 1.0)
        
        # Check for manufacturer
        mfg_score = 0
        if any(keyword in text for keyword in ['manufacturer', 'brand', 'by', 'mfg']):
            mfg_score += 0.6
        if any(keyword in class_names for keyword in ['manufacturer', 'brand']):
            mfg_score += 0.4
        
        if mfg_score > 0.5:
            classifications['manufacturer'] = min(mfg_score, 1.0)
        
        # Check for image
        if element.get('tag') == 'img' and element.get('src'):
            classifications['image'] = 0.9
        
        # Check for link
        if element.get('href') and element.get('tag') == 'a':
            classifications['link'] = 0.8
        
        # Check for drug listing items (MedlinePlus specific)
        if element.get('tag') == 'li' and element.get('parent') == 'ul':
            text = element.get('text', '').lower()
            # Drug entries often start with drug names followed by "see"
            if '®' in text or '™' in text or ' see ' in text:
                classifications['drug_link'] = 0.8
            
        return classifications
    
    def extract_container_data(self, container: Dict) -> Dict:
        """
        Extract structured data from a container (product card, article, etc.)
        """
        # 🔧 FIX: Handle None or invalid containers
        if container is None:
            return {
                'type': 'unknown',
                'elements': {},
                'links': [],
                'confidence': 0.0
            }
        
        result = {
            'type': 'unknown',
            'elements': {},
            'links': [],
            'confidence': 0.0
        }
        
        # Classify each element in the container
        for elem in (container.get('elements', []) or []):
            if elem is None or not isinstance(elem, dict):
                continue
            
            try:
                classifications = self.classify_element(elem)
            except Exception as e:
                print(f"❌ Error classifying element: {e}")
                continue
            
            # Check if this is a drug listing container
            drug_links_count = sum(1 for c in classifications if c == 'drug_link')
            if drug_links_count > 5:
                result['type'] = 'drug_listing'
                result['confidence'] = 0.8
                
            for field, score in classifications.items():
                if field not in result['elements'] or score > result['elements'][field].get('confidence', 0):
                    result['elements'][field] = {
                        'value': elem.get('text', ''),
                        'confidence': score,
                        'element': elem
                    }
            
            # Collect links
            if elem.get('href'):
                result['links'].append({
                    'url': elem['href'],
                    'text': elem.get('text', ''),
                    'confidence': 0.7
                })
        
        # Determine container type based on elements found
        if 'title' in result['elements'] and 'price' in result['elements']:
            result['type'] = 'product_card'
            result['confidence'] = (result['elements']['title']['confidence'] + 
                                   result['elements']['price']['confidence']) / 2
        elif 'title' in result['elements'] and 'image' in result['elements']:
            result['type'] = 'article_card'
            result['confidence'] = result['elements']['title']['confidence']
        else:
            result['type'] = 'generic_container'
            result['confidence'] = 0.5
        
        return result