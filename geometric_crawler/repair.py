# geometric_crawler/repair.py
"""
Self-healing repair logic for failed extractions
Uses multiple strategies from simple to complex
Supports multiple LLM providers: Ollama, OpenAI, Anthropic, Gemini, DeepSeek
"""
import re
import os
from typing import Dict, List, Optional
from selectolax.parser import HTMLParser

# Try importing requests for API calls
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False


class RepairEngine:
    """
    4-level repair system that tries cheapest solutions first
    Supports multiple LLM providers for Level 4 repair
    """
    
    # LLM Provider configurations
    LLM_PROVIDERS = {
        "ollama": {
            "endpoint": "http://localhost:11434/api/generate",
            "default_model": "llama3",
        },
        "openai": {
            "endpoint": "https://api.openai.com/v1/chat/completions",
            "default_model": "gpt-3.5-turbo",
        },
        "anthropic": {
            "endpoint": "https://api.anthropic.com/v1/messages",
            "default_model": "claude-3-5-sonnet-20241022",
        },
        "gemini": {
            "endpoint_template": "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
            "default_model": "gemini-1.5-flash",
        },
        "deepseek": {
            "endpoint": "https://api.deepseek.com/v1/chat/completions",
            "default_model": "deepseek-chat",
        },
    }
    
    def __init__(self, use_llm: bool = False, llm_provider: str = "ollama", 
                 llm_model: str = None, api_key: str = None):
        self.use_llm = use_llm
        self.llm_provider = llm_provider.lower() if llm_provider else "ollama"
        self.llm_model = llm_model
        self.api_key = api_key or os.environ.get(f"{self.llm_provider.upper()}_API_KEY", "")
        
        # Set default model if not provided
        if not self.llm_model and self.llm_provider in self.LLM_PROVIDERS:
            self.llm_model = self.LLM_PROVIDERS[self.llm_provider]["default_model"]
        
        self.repair_stats = {
            'level1_success': 0,
            'level2_success': 0,
            'level3_success': 0,
            'level4_success': 0,
            'failed': 0
        }
    
    def repair_field(self, html: str, field_name: str, field_config: Dict, context: Dict = None) -> Dict:
        """
        Main repair entry point - tries strategies in order
        """
        # Level 1: Parent Trap (cheapest)
        result = self._parent_trap_repair(html, field_name, field_config, context)
        if result['success']:
            self.repair_stats['level1_success'] += 1
            return result
        
        # Level 2: Keyword Hunt
        result = self._keyword_hunt_repair(html, field_name, field_config, context)
        if result['success']:
            self.repair_stats['level2_success'] += 1
            return result
        
        # Level 3: Visual Pattern Matching
        result = self._visual_pattern_repair(html, field_name, field_config, context)
        if result['success']:
            self.repair_stats['level3_success'] += 1
            return result
        
        # Level 4: LLM Repair (if enabled and available)
        if self.use_llm:
            result = self._llm_repair(html, field_name, field_config, context)
            if result['success']:
                self.repair_stats['level4_success'] += 1
                return result
        
        # All repairs failed
        self.repair_stats['failed'] += 1
        return {
            'success': False,
            'value': None,
            'method': 'none',
            'confidence': 0
        }
    
    def _parent_trap_repair(self, html: str, field_name: str, field_config: Dict, context: Dict = None) -> Dict:
        """
        Level 1: Find parent container and look for element in expected position
        """
        tree = HTMLParser(html)
        
        # Get the failed selector from context
        failed_selector = context.get('failed_selector', '') if context else ''
        
        # Try to find parent by stripping last part of selector
        if failed_selector:
            parts = failed_selector.split(' > ')
            if len(parts) > 1:
                parent_selector = ' > '.join(parts[:-1])
                parent = tree.css_first(parent_selector)
                
                if parent:
                    # Look for elements in parent that might contain our field
                    keywords = field_config.get('keywords', [field_name])
                    for child in parent.css('*'):
                        text = child.text(strip=True)
                        if text and any(kw.lower() in text.lower() for kw in keywords):
                            return {
                                'success': True,
                                'value': text,
                                'method': 'parent_trap',
                                'confidence': 0.8,
                                'new_selector': f"{parent_selector} > {child.tag}"
                            }
        
        return {'success': False}
    
    def _keyword_hunt_repair(self, html: str, field_name: str, field_config: Dict, context: Dict = None) -> Dict:
        """
        Level 2: Search for keywords in nearby elements
        """
        tree = HTMLParser(html)
        keywords = field_config.get('keywords', [field_name])
        
        # Search in all elements
        best_match = None
        best_score = 0
        
        for elem in tree.css('div, span, p, h1, h2, h3, td, li'):
            text = elem.text(strip=True)
            if not text or len(text) > 500:  # Skip huge blocks
                continue
            
            # Calculate keyword match score
            score = 0
            text_lower = text.lower()
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    score += 0.3
            
            # Check if it contains field indicators
            if field_name in elem.attributes.get('class', '') or field_name in elem.attributes.get('id', ''):
                score += 0.5
            
            if score > best_score:
                best_score = score
                best_match = text
        
        if best_match and best_score > 0.5:
            return {
                'success': True,
                'value': best_match,
                'method': 'keyword_hunt',
                'confidence': best_score
            }
        
        return {'success': False}
    
    def _visual_pattern_repair(self, html: str, field_name: str, field_config: Dict, context: Dict = None) -> Dict:
        """
        Level 3: Use visual patterns (font size, color, position indicators)
        """
        tree = HTMLParser(html)
        
        # For now, use tag-based visual hints
        if field_name == 'title':
            # Titles are often in heading tags
            for tag in ['h1', 'h2', 'h3']:
                elem = tree.css_first(tag)
                if elem:
                    text = elem.text(strip=True)
                    if text:
                        return {
                            'success': True,
                            'value': text,
                            'method': 'visual_pattern',
                            'confidence': 0.7
                        }
        
        elif field_name == 'price':
            # Prices often have currency symbols
            for elem in tree.css('*'):
                text = elem.text(strip=True)
                if text and re.search(r'[₹$£€]\s*\d+', text):
                    return {
                        'success': True,
                        'value': text,
                        'method': 'visual_pattern',
                        'confidence': 0.8
                    }
        
        return {'success': False}
    
    def _llm_repair(self, html: str, field_name: str, field_config: Dict, context: Dict = None) -> Dict:
        """
        Level 4: LLM repair (last resort)
        Supports multiple providers: Ollama, OpenAI, Anthropic, Gemini, DeepSeek
        """
        if not self.use_llm:
            return {'success': False}
        
        if not REQUESTS_AVAILABLE:
            print("⚠️ requests library not available. LLM repair disabled.")
            return {'success': False}
        
        # Prepare HTML snippet
        cropped = html[:2000]
        cropped = re.sub(r'<script.*?>.*?</script>', '', cropped, flags=re.DOTALL)
        cropped = re.sub(r'<style.*?>.*?</style>', '', cropped, flags=re.DOTALL)
        
        prompt = f"""Extract the {field_name} ({field_config.get('description', '')}) from this HTML.
Keywords: {', '.join(field_config.get('keywords', []))}

HTML:
{cropped}

Return ONLY the extracted value as plain text, nothing else.
Do not include any HTML tags, JavaScript code, or explanations."""
        
        try:
            value = self._call_llm(prompt)
            
            if value:
                # Clean the response
                value = re.sub(r'<[^>]+>', '', value)
                value = re.sub(r'\{.*?\}', '', value)
                value = value.strip()
                
                if value and len(value) < 500:
                    return {
                        'success': True,
                        'value': value,
                        'method': f'llm_{self.llm_provider}',
                        'confidence': 0.9
                    }
        except Exception as e:
            print(f"LLM repair failed ({self.llm_provider}): {e}")
        
        return {'success': False}
    
    def _call_llm(self, prompt: str) -> Optional[str]:
        """
        Call LLM based on configured provider
        """
        provider = self.llm_provider
        model = self.llm_model
        
        if provider == "ollama":
            return self._call_ollama(prompt, model)
        elif provider == "openai":
            return self._call_openai(prompt, model)
        elif provider == "anthropic":
            return self._call_anthropic(prompt, model)
        elif provider == "gemini":
            return self._call_gemini(prompt, model)
        elif provider == "deepseek":
            return self._call_deepseek(prompt, model)
        else:
            print(f"⚠️ Unknown LLM provider: {provider}")
            return None
    
    def _call_ollama(self, prompt: str, model: str) -> Optional[str]:
        """Call Ollama local server"""
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('localhost', 11434))
        sock.close()
        
        if result != 0:
            print("⚠️ Ollama not running on port 11434")
            return None
        
        response = requests.post(
            'http://localhost:11434/api/generate',
            json={
                'model': model or 'llama3',
                'prompt': prompt,
                'stream': False,
                'options': {'temperature': 0.1, 'num_predict': 100}
            },
            timeout=30
        )
        
        if response.status_code == 200:
            return response.json().get('response', '').strip()
        return None
    
    def _call_openai(self, prompt: str, model: str) -> Optional[str]:
        """Call OpenAI API"""
        if not self.api_key:
            print("⚠️ OpenAI API key not configured")
            return None
        
        response = requests.post(
            'https://api.openai.com/v1/chat/completions',
            headers={
                'Authorization': f'Bearer {self.api_key}',
                'Content-Type': 'application/json'
            },
            json={
                'model': model or 'gpt-3.5-turbo',
                'messages': [{'role': 'user', 'content': prompt}],
                'temperature': 0.1,
                'max_tokens': 100
            },
            timeout=30
        )
        
        if response.status_code == 200:
            return response.json()['choices'][0]['message']['content'].strip()
        print(f"⚠️ OpenAI API error: {response.status_code} - {response.text[:200]}")
        return None
    
    def _call_anthropic(self, prompt: str, model: str) -> Optional[str]:
        """Call Anthropic API"""
        if not self.api_key:
            print("⚠️ Anthropic API key not configured")
            return None
        
        response = requests.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'x-api-key': self.api_key,
                'anthropic-version': '2023-06-01',
                'Content-Type': 'application/json'
            },
            json={
                'model': model or 'claude-3-5-sonnet-20241022',
                'max_tokens': 100,
                'messages': [{'role': 'user', 'content': prompt}]
            },
            timeout=30
        )
        
        if response.status_code == 200:
            content = response.json().get('content', [])
            if content and len(content) > 0:
                return content[0].get('text', '').strip()
        print(f"⚠️ Anthropic API error: {response.status_code} - {response.text[:200]}")
        return None
    
    def _call_gemini(self, prompt: str, model: str) -> Optional[str]:
        """Call Google Gemini API"""
        if not self.api_key:
            print("⚠️ Gemini API key not configured")
            return None
        
        model = model or 'gemini-1.5-flash'
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={self.api_key}"
        
        response = requests.post(
            url,
            headers={'Content-Type': 'application/json'},
            json={
                'contents': [{'parts': [{'text': prompt}]}],
                'generationConfig': {'temperature': 0.1, 'maxOutputTokens': 100}
            },
            timeout=30
        )
        
        if response.status_code == 200:
            candidates = response.json().get('candidates', [])
            if candidates:
                content = candidates[0].get('content', {})
                parts = content.get('parts', [])
                if parts:
                    return parts[0].get('text', '').strip()
        print(f"⚠️ Gemini API error: {response.status_code} - {response.text[:200]}")
        return None
    
    def _call_deepseek(self, prompt: str, model: str) -> Optional[str]:
        """Call DeepSeek API"""
        if not self.api_key:
            print("⚠️ DeepSeek API key not configured")
            return None
        
        response = requests.post(
            'https://api.deepseek.com/v1/chat/completions',
            headers={
                'Authorization': f'Bearer {self.api_key}',
                'Content-Type': 'application/json'
            },
            json={
                'model': model or 'deepseek-chat',
                'messages': [{'role': 'user', 'content': prompt}],
                'temperature': 0.1,
                'max_tokens': 100
            },
            timeout=30
        )
        
        if response.status_code == 200:
            return response.json()['choices'][0]['message']['content'].strip()
        print(f"⚠️ DeepSeek API error: {response.status_code} - {response.text[:200]}")
        return None
        
    def get_stats(self) -> Dict:
        """Get repair statistics"""
        return self.repair_stats