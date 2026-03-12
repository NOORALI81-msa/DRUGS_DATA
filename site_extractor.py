# LLM-based spider config generation
def generate_spider_config_llm(master_url, list_url, detail_url, follow_patterns, selected_sections, pagination_pattern, follow_pagination, llm_model="llama2", timeout=60):
    """Call LLM to generate universal Scrapy spider config for the site."""
    prompt = f"""
You are an expert Scrapy spider generator.
Given the following:
- Start/listing page URL: {master_url}
- Example product/list page URL: {list_url}
- Example detail page URL: {detail_url}
- Unique URL patterns for detail pages: {follow_patterns}
- Sections to extract from each detail page: {selected_sections}
- Pagination pattern: {pagination_pattern if follow_pagination else None}

Your task:
1. Write a complete Scrapy spider (Python code) that:
   - Starts from the listing page.
   - Follows all links matching the unique detail page patterns (generalize from the examples).
   - Handles pagination if a pattern is provided.
   - Extracts all requested sections from each detail page.
2. Name the spider based on the domain (e.g., "one_mg_drug").
3. Output only valid Python code for the Scrapy spider (no explanations, no JSON, no markdown).

Example output:
<python code for a Scrapy spider>
"""
    llm_api_url = "http://localhost:11434/api/generate"
    payload = {
        "model": llm_model,
        "prompt": prompt,
        "stream": False
    }
    try:
        response = requests.post(llm_api_url, json=payload, timeout=timeout)
        response.raise_for_status()
        llm_result = response.json()
        llm_output = llm_result.get("response", "")
        config_json = None
        try:
            config_json = json.loads(llm_output)
        except Exception:
            import re
            match = re.search(r"\{.*\}", llm_output, re.DOTALL)
            if match:
                config_json = json.loads(match.group(0))
        return config_json, llm_output
    except Exception as e:
        return None, f"LLM call failed: {e}"
import streamlit as st
import requests
from selectolax.parser import HTMLParser
import json
import time
from datetime import datetime
import re
from urllib.parse import urljoin, urlparse
from pathlib import Path
import hashlib
import pandas as pd
import io
import zipfile



class SingleFileSaver:
    def __init__(self, output_dir="extractions"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.current_file = None
    
    def create_file(self, start_url):
        """Create ONE file for all extractions"""
        # Extract domain from URL
        domain = urlparse(start_url).netloc.replace('www.', '').split('.')[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{domain}_drugs_{timestamp}.md"
        filepath = self.output_dir / filename
        
        # Create file with header
        md = f"""# Drug Extractions from {start_url}

**Started:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Total Drugs:** Will be updated as extraction progresses

---
"""
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(md)
        
        self.current_file = filepath
        return filepath
    
    def append_drug(self, drug_name, url, content, metadata=None):
        """Append a single drug to the main file"""
        if not self.current_file or not self.current_file.exists():
            return None
        
        # Read current file
        with open(self.current_file, 'r', encoding='utf-8') as f:
            existing = f.read()
        
        # Prepare drug section
        drug_section = f"""
## {drug_name}

**URL:** {url}
**Extracted:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Time:** {metadata.get('time', 0):.1f}s
**Confidence:** {metadata.get('confidence', 0):.0%}

{content}

---
"""
        
        # Update drug count in header
        drug_count = existing.count('## ')
        updated_header = existing.split('---')[0]
        new_header = f"""# Drug Extractions from {url}

**Started:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Total Drugs:** {drug_count + 1} (updating...)

---
"""
        
        # Rebuild file
        remaining = existing.split('---', 1)[1] if '---' in existing else ''
        new_content = new_header + drug_section + remaining
        
        with open(self.current_file, 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        return self.current_file
    
    def mark_failed(self, drug_name, url, error):
        """Append failed drug to the main file"""
        if not self.current_file or not self.current_file.exists():
            return None
        
        with open(self.current_file, 'r', encoding='utf-8') as f:
            existing = f.read()
        
        failed_section = f"""
## ❌ {drug_name} (FAILED)

**URL:** {url}
**Error:** {error}
**Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---
"""
        
        new_content = existing + failed_section
        
        with open(self.current_file, 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        return self.current_file
    
    def finalize(self, total_drugs, successful):
        """Update final counts when done"""
        if not self.current_file or not self.current_file.exists():
            return
        
        with open(self.current_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Update header with final counts
        lines = content.split('\n')
        for i, line in enumerate(lines):
            if line.startswith('**Total Drugs:**'):
                lines[i] = f"**Total Drugs:** {total_drugs} (Successful: {successful})"
                break
        
        with open(self.current_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))

# ============================================================================
# CSV EXPORTER
# ============================================================================

class CSVExporter:
    def export_summary(self, results):
        """Export extraction summary to CSV"""
        data = []
        for r in results:
            data.append({
                'Drug Name': r.get('drug_name', 'Unknown'),
                'URL': r.get('url', ''),
                'Confidence': f"{r.get('confidence', 0):.0%}",
                'Issues': len(r.get('issues', [])),
                'Time (s)': round(r.get('time', 0), 1),
                'File': Path(r.get('saved_to', '')).name
            })
        return pd.DataFrame(data)

# ============================================================================
# HALLUCINATION CHECKER
# ============================================================================

class HallucinationChecker:
    def check(self, source, output):
        issues = []
        
        # Check emergency numbers
        emergency_pattern = r'1-?800-?\d{3}-?\d{4}|911'
        source_emergency = re.findall(emergency_pattern, source, re.I)
        output_emergency = re.findall(emergency_pattern, output, re.I)
        
        if source_emergency and not output_emergency:
            issues.append("Missing emergency numbers")
        
        # Check warnings
        if 'warning' in source.lower() and 'warning' not in output.lower():
            issues.append("Missing warnings")
        
        # Check overdose
        if 'overdose' in source.lower() and 'overdose' not in output.lower():
            issues.append("Missing overdose info")
        
        confidence = max(0, 1.0 - len(issues) * 0.25)
        
        return {
            'confidence': confidence,
            'issues': issues,
            'safe': confidence > 0.7
        }

# ============================================================================
# CONTENT EXTRACTOR
# ============================================================================

class ContentExtractor:
    def get_content(self, url):
        """Fetch and clean page content"""
        try:
            r = requests.get(url, timeout=10, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            })
            tree = HTMLParser(r.text)
            
            # Remove junk
            for e in tree.css('script, style, nav, footer, header, .ad, .menu'):
                e.decompose()
            
            # Get title
            title = tree.css_first('h1')
            title_text = title.text(strip=True) if title else url.split('/')[-1].replace('.html', '')
            
            # Get content (limited to 4000 chars for speed)
            content_parts = []
            char_count = 0
            for elem in tree.css('h1, h2, h3, p, li'):
                text = elem.text(strip=True)
                if text and len(text) > 15:
                    if char_count + len(text) > 4000:
                        content_parts.append("...[truncated]...")
                        break
                    content_parts.append(text)
                    char_count += len(text)
            
            return {
                'success': True,
                'title': title_text,
                'content': '\n'.join(content_parts),
                'url': url,
                'tree': tree
            }
        except Exception as e:
            return {'success': False, 'error': str(e), 'url': url}

# ============================================================================
# URL DISCOVERY
# ============================================================================

class URLDiscoverer:
    def __init__(self):
        self.drug_patterns = [
            r'/druginfo/meds/', r'-drug\.htm', r'/drugs/',
            r'/medication/', r'/prescription-drugs/', r'/medicine/'
        ]
    
    def extract_drug_links(self, page_url, html_tree):
        """Extract drug links from a page"""
        links = []
        
        if html_tree is None:
            return links
        
        base_domain = urlparse(page_url).netloc
        
        for a in html_tree.css('a[href]'):
            href = a.attributes.get('href', '')
            text = a.text(strip=True)
            
            if not href or href.startswith('#') or 'javascript:' in href:
                continue
            
            full_url = urljoin(page_url, href)
            
            # Stay in same domain
            if urlparse(full_url).netloc != base_domain:
                continue
            
            # Check if it's a drug page
            if any(re.search(p, full_url, re.I) for p in self.drug_patterns):
                links.append({
                    'url': full_url,
                    'text': text or full_url.split('/')[-1].replace('.html', '')
                })
        
        return links
    
    def find_next_page(self, page_url, html_tree):
        """Find next page link"""
        if html_tree is None:
            return None
        for a in html_tree.css('a[href]'):
            href = a.attributes.get('href', '')
            text = a.text(strip=True).lower()
            
            if 'next' in text or '>' in text or 'more' in text:
                return urljoin(page_url, href)
            if 'page=' in href or 'start=' in href or 'offset=' in href:
                return urljoin(page_url, href)
        
        return None

# ============================================================================
# LLM EXTRACTOR (Uses app.py config)
# ============================================================================

class LLMExtractor:
    def extract(self, content, prompt):
        """Use LLM config from app.py sidebar"""
        
        # Get LLM settings from session state
        llm_enabled = st.session_state.get('llm_enabled', False)
        provider = st.session_state.get('llm_provider', 'ollama')
        model = st.session_state.get('llm_model', 'llama3')
        api_key = st.session_state.api_keys.get(provider, '') if provider != 'ollama' else ''
        
        if not llm_enabled:
            return {'success': False, 'error': 'LLM not enabled in sidebar'}
        
        # Prepare prompt
        llm_prompt = f"""Extract the following from this drug page. Be concise.

REQUEST: {prompt}

CONTENT:
{content[:4000]}

Extract ONLY what's requested:"""
        
        try:
            if provider == "ollama":
                return self._call_ollama(model, llm_prompt)
            elif provider == "openai":
                return self._call_openai(model, llm_prompt, api_key)
            elif provider == "anthropic":
                return self._call_anthropic(model, llm_prompt, api_key)
            elif provider == "gemini":
                return self._call_gemini(model, llm_prompt, api_key)
            elif provider == "deepseek":
                return self._call_deepseek(model, llm_prompt, api_key)
            else:
                return {'success': False, 'error': f'Unknown provider: {provider}'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _call_ollama(self, model, prompt):
        r = requests.post("http://localhost:11434/api/generate", json={
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.1, "num_predict": 1000}
        }, timeout=60)
        if r.ok:
            return {'success': True, 'content': r.json().get('response', '')}
        return {'success': False, 'error': f"Ollama error: {r.status_code}"}
    
    def _call_openai(self, model, prompt, api_key):
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": 3000
        }
        r = requests.post("https://api.openai.com/v1/chat/completions", 
                         json=payload, headers=headers, timeout=30)
        if r.ok:
            return {'success': True, 'content': r.json()['choices'][0]['message']['content']}
        return {'success': False, 'error': f"OpenAI error: {r.text}"}
    
    def _call_anthropic(self, model, prompt, api_key):
        headers = {"x-api-key": api_key, "anthropic-version": "2023-06-01", 
                  "Content-Type": "application/json"}
        payload = {
            "model": model,
            "max_tokens": 3000,
            "temperature": 0.1,
            "messages": [{"role": "user", "content": prompt}]
        }
        r = requests.post("https://api.anthropic.com/v1/messages", 
                         json=payload, headers=headers, timeout=30)
        if r.ok:
            return {'success': True, 'content': r.json()['content'][0]['text']}
        return {'success': False, 'error': f"Anthropic error: {r.text}"}
    
    def _call_gemini(self, model, prompt, api_key):
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
        payload = {"contents": [{"parts": [{"text": prompt}]}]}
        r = requests.post(url, json=payload, timeout=30)
        if r.ok:
            return {'success': True, 'content': r.json()['candidates'][0]['content']['parts'][0]['text']}
        return {'success': False, 'error': f"Gemini error: {r.text}"}
    
    def _call_deepseek(self, model, prompt, api_key):
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": 3000
        }
        r = requests.post("https://api.deepseek.com/v1/chat/completions", 
                         json=payload, headers=headers, timeout=30)
        if r.ok:
            return {'success': True, 'content': r.json()['choices'][0]['message']['content']}
        return {'success': False, 'error': f"DeepSeek error: {r.text}"}

# ============================================================================
# MAIN EXTRACTOR
# ============================================================================

class DrugExtractor:
    def __init__(self):
        self.discoverer = URLDiscoverer()
        self.content = ContentExtractor()
        self.llm = LLMExtractor()
        self.checker = HallucinationChecker()
        self.saver = SingleFileSaver()
        self.csv = CSVExporter()
        
        self.stats = {
            'drugs_found': 0,
            'drugs_extracted': 0,
            'failed': 0,
            'times': []
        }
    
    def discover_drugs(self, start_url, max_drugs=30, max_pages=3, follow_pagination=True):
        """Discover drug URLs"""
        discovered = []
        to_visit = [start_url]
        visited = set()
        pages = 0
        
        progress = st.progress(0)
        status = st.empty()
        
        while to_visit and len(discovered) < max_drugs and pages < max_pages:
            url = to_visit.pop(0)
            if url in visited:
                continue
            visited.add(url)
            pages += 1
            
            status.text(f"Scanning page {pages}: {url}")
            progress.progress(pages / max_pages)
            
            page = self.content.get_content(url)
            if not page['success']:
                continue
            
            # Extract links
            links = self.discoverer.extract_drug_links(url, page.get('tree', None))
            for link in links:
                if link['url'] not in [d['url'] for d in discovered]:
                    discovered.append(link)
            
            # Find next page
            if follow_pagination and len(discovered) < max_drugs:
                next_page = self.discoverer.find_next_page(url, page.get('tree', None))
                if next_page and next_page not in visited:
                    to_visit.append(next_page)
            
            time.sleep(0.5)
        
        self.stats['drugs_found'] = len(discovered)
        return discovered[:max_drugs]
    
    def extract_drug(self, drug_info, prompt):
        """Extract single drug"""
        start = time.time()
        
        # Get content
        page = self.content.get_content(drug_info['url'])
        if not page['success']:
            return {'success': False, 'error': page.get('error')}
        
        # Extract with LLM
        result = self.llm.extract(page['content'], prompt)
        if not result['success']:
            return {'success': False, 'error': result.get('error')}
        
        # Validate
        validation = self.checker.check(page['content'], result['content'])
        
        extract_time = time.time() - start
        self.stats['times'].append(extract_time)
        
        return {
            'success': True,
            'drug_name': page['title'],
            'url': drug_info['url'],
            'extraction': result['content'],
            'confidence': validation['confidence'],
            'issues': validation['issues'],
            'safe': validation['safe'],
            'time': extract_time
        }

# ============================================================================
# UI FUNCTION
# ============================================================================

def render_extractor_panel():
    """Main UI - uses LLM config from app.py sidebar"""
    
    st.markdown("# 💊 Drug Information Extractor")
    
    # Show current LLM config from sidebar
    if st.session_state.get('llm_enabled', False):
        st.success(f"✅ Using: {st.session_state.llm_provider} / {st.session_state.llm_model}")
    else:
        st.error("❌ LLM not enabled. Enable in sidebar first.")
        return
    
    # Initialize
    if 'extractor' not in st.session_state:
        st.session_state.extractor = DrugExtractor()
    ext = st.session_state.extractor
    
    # URL input
    url = st.text_input(
        "Start URL",
        value="https://medlineplus.gov/druginfo/drug_Aa.html",
        help="Drug index page or single drug URL"
    )
    
    # Options
    col1, col2, col3 = st.columns(3)
    with col1:
        max_drugs = st.number_input("Max drugs", 1, 100, 20)
    with col2:
        max_pages = st.number_input("Max pages", 1, 10, 3)
    with col3:
        delay = st.number_input("Delay (s)", 0.5, 3.0, 1.0)
    
    follow_pagination = st.checkbox("Follow pagination", True)
    
    # Extraction prompt
    prompt = st.text_area(
        "What to extract?",
        value="Extract: drug name, uses, side effects, warnings, overdose information",
        height=100
    )
    
    if st.button("🔍 Start Extraction", type="primary"):
        
        # PHASE 1: DISCOVER
        st.markdown("---")
        st.markdown("### 🔍 Phase 1: Discovering Drugs")

        with st.spinner("Scanning for drug links..."):
            drugs = ext.discover_drugs(url, max_drugs, max_pages, follow_pagination)

        if not drugs:
            drugs = [{'url': url, 'text': url.split('/')[-1]}]

        st.success(f"✅ Found {len(drugs)} drugs")

        # Create ONE file for all drugs
        st.markdown("### 📁 Creating Output File")
        main_file = ext.saver.create_file(url)
        st.success(f"📄 All drugs will be saved to: {Path(main_file).name}")

        # PHASE 2: EXTRACT
        st.markdown("---")
        st.markdown("### ⚙️ Phase 2: Extracting")

        progress = st.progress(0)
        status = st.empty()
        results = []
        successful = 0

        for idx, drug in enumerate(drugs):
            progress.progress(idx / len(drugs))
            status.text(f"[{idx+1}/{len(drugs)}] Extracting: {drug.get('text', 'Unknown')}")
            
            # Extract
            result = ext.extract_drug(drug, prompt)
            
            if result['success']:
                # Append to main file
                ext.saver.append_drug(
                    result['drug_name'],
                    result['url'],
                    result['extraction'],
                    {'time': result['time'], 'confidence': result['confidence']}
                )
                
                results.append(result)
                successful += 1
                icon = "✅" if result['safe'] else "⚠️"
                st.success(f"{icon} {result['drug_name']} - {result['time']:.1f}s")
            else:
                # Append failed to main file
                ext.saver.mark_failed(
                    drug.get('text', 'Unknown'),
                    drug['url'],
                    result.get('error', 'Unknown error')
                )
                st.error(f"❌ Failed: {drug.get('text', 'Unknown')}")
            
            time.sleep(delay)

        # Finalize file with counts
        ext.saver.finalize(len(drugs), successful)

        st.success(f"✅ All extractions saved to: {ext.saver.current_file}")
        
        # RESULTS
        st.markdown("---")
        st.markdown("## 📊 Complete!")
        
        if results:
            # Summary
            avg_time = sum(ext.stats['times']) / len(ext.stats['times'])
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Success", len(results))
            with col2:
                st.metric("Failed", len(drugs) - len(results))
            with col3:
                st.metric("Avg Time", f"{avg_time:.1f}s")
            
            # CSV Export
            df = ext.csv.export_summary(results)
            csv_data = df.to_csv(index=False)
            
            st.download_button(
                "📥 Download CSV Summary",
                data=csv_data,
                file_name=f"extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )

# Export
__all__ = ['render_extractor_panel']