#!/usr/bin/env python
# run.py - UPDATED VERSION WITH PATH FIX
"""
Geometric Self-Healing Crawler Runner with ALL options
"""
import os
import sys
import argparse
import re
from urllib.parse import urlparse

# 🔧 FIX: Add current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def main():
    parser = argparse.ArgumentParser(description='Geometric Self-Healing Crawler')

    # Spider selection
    parser.add_argument('--spider', choices=['geometric', 'medlineplus', 'drug'], default='geometric',
                       help='Spider to run (drug=depth-first drug extraction)')
    
    # URL options
    parser.add_argument('--url', help='Single URL to crawl')
    parser.add_argument('--urls', help='Comma-separated list of URLs')
    parser.add_argument('--follow-patterns',
                       help='Comma-separated URL patterns to follow (regex or substring)')
    
    # Drug spider specific options
    parser.add_argument('--max-drugs', type=int, default=0,
                       help='Max drugs to extract (drug spider only, 0 = unlimited)')
    parser.add_argument('--follow-fda', action='store_true', default=True,
                       help='Follow FDA info links (drug spider)')
    parser.add_argument('--max-subpage-depth', type=int, default=3,
                       help='Max depth for sub-pages per drug (prevents infinite loops)')
    parser.add_argument('--max-subpages', type=int, default=20,
                       help='Max sub-pages to visit per drug (prevents infinite loops)')
    
    # Performance options
    parser.add_argument('--pages', type=int, default=100, help='Max pages to crawl')
    parser.add_argument('--depth', type=int, default=3, help='Max crawl depth')
    parser.add_argument('--threads', type=int, default=8, help='Concurrent requests')
    parser.add_argument('--delay', type=float, default=1.0, help='Download delay')
    parser.add_argument('--playwright-always', action='store_true',
                       help='Use Playwright for every page (no HTTP fast-path)')
    parser.add_argument('--http-only', action='store_true',
                       help='Use HTTP only, skip Playwright completely (for heavy sites)')
    parser.add_argument('--http-after-first', action='store_true', default=True,
                       help='Use HTTP after first layout (Playwright for first page)')
    parser.add_argument('--no-http-after-first', action='store_false', dest='http_after_first',
                       help='Disable HTTP fast-path')
    
    # Parallel options
    parser.add_argument('--parallel', action='store_true', default=True, help='Enable parallel extraction')
    parser.add_argument('--no-parallel', action='store_false', dest='parallel', help='Disable parallel extraction')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers')
    
    # Protection options
    parser.add_argument('--protections', action='store_true', default=True, help='Enable all protections')
    parser.add_argument('--no-protections', action='store_false', dest='protections', help='Disable all protections')
    parser.add_argument('--rotate-ua', action='store_true', default=True, help='Rotate user agents')
    parser.add_argument('--no-rotate-ua', action='store_false', dest='rotate_ua', help='Disable user agent rotation')
    parser.add_argument('--random-delay', action='store_true', default=True, help='Add random delays')
    parser.add_argument('--no-random-delay', action='store_false', dest='random_delay', help='Disable random delays')
    
    # Resource saving
    parser.add_argument('--block-images', action='store_true', default=True, help='Block images (save bandwidth)')
    parser.add_argument('--no-block-images', action='store_false', dest='block_images', help='Allow images')
    
    # Repair options
    parser.add_argument('--use-llm', action='store_true', help='Enable LLM repair (requires ollama)')
    parser.add_argument('--repair-level', type=int, choices=[1,2,3,4], default=3, 
                       help='Max repair level (1=parent trap, 2=keyword, 3=visual, 4=llm)')
    
    # LLM options
    parser.add_argument('--llm-model', default='llama3', help='LLM model to use')
    parser.add_argument('--llm-provider', default='ollama', choices=['ollama', 'openai'], help='LLM provider')
    parser.add_argument('--llm-temperature', type=float, default=0.1, help='LLM temperature')
    
    # Output
    parser.add_argument('--output', help='Output filename prefix')
    parser.add_argument('--new-file', action='store_true',
                       help='Force a new output file for this run')
    parser.add_argument('--use-existing-file', action='store_true',
                       help='Append to an existing output file instead of creating a new one')
    parser.add_argument('--existing-file',
                       help='Path to existing CSV file (used with --use-existing-file)')
    parser.add_argument('--detail-only', action='store_true',
                       help='Write only page_detail items to output')
    
    args = parser.parse_args()
    
    # Validate URL
    if not args.url and not args.urls:
        print(" Error: Please provide --url or --urls")
        sys.exit(1)
    
    # 🔧 FIX: Set environment variables
    os.environ['SCRAPY_SETTINGS_MODULE'] = 'geometric_crawler.settings'
    
    # Override config with command line args
    os.environ['PARALLEL_EXTRACTION'] = str(args.parallel).lower()
    os.environ['PARALLEL_WORKERS'] = str(args.workers)
    os.environ['CONCURRENT_REQUESTS'] = str(args.threads)
    os.environ['DOWNLOAD_DELAY'] = str(args.delay)
    os.environ['MAX_PAGES'] = str(args.pages)
    os.environ['MAX_DEPTH'] = str(args.depth)
    os.environ['ENABLE_USER_AGENT_ROTATION'] = str(args.rotate_ua and args.protections).lower()
    os.environ['ENABLE_RANDOM_DELAY'] = str(args.random_delay and args.protections).lower()
    os.environ['BLOCK_IMAGES'] = str(args.block_images).lower()
    os.environ['ENABLE_LLM_REPAIR'] = str(args.use_llm).lower()
    os.environ['USE_EXISTING_OUTPUT'] = str(args.use_existing_file).lower()
    os.environ['FORCE_NEW_OUTPUT'] = str(args.new_file).lower()
    os.environ['DETAIL_ONLY'] = str(args.detail_only).lower()
    if args.output:
        os.environ['OUTPUT_PREFIX'] = args.output
    elif 'OUTPUT_PREFIX' in os.environ:
        del os.environ['OUTPUT_PREFIX']
    if args.existing_file:
        os.environ['EXISTING_OUTPUT_FILE'] = args.existing_file
    elif 'EXISTING_OUTPUT_FILE' in os.environ:
        del os.environ['EXISTING_OUTPUT_FILE']
    
    # LLM environment variables
    if args.llm_model:
        os.environ['OLLAMA_MODEL'] = args.llm_model
    if args.llm_provider:
        os.environ['LLM_PROVIDER'] = args.llm_provider
    if args.llm_temperature:
        os.environ['LLM_TEMPERATURE'] = str(args.llm_temperature)
    
    # Build command
    cmd = ['scrapy', 'crawl', args.spider]
    
    if args.url:
        cmd.extend(['-a', f'urls={args.url}'])
    else:
        cmd.extend(['-a', f'urls={args.urls}'])
    
    cmd.extend(['-a', f'max_pages={args.pages}'])
    cmd.extend(['-a', f'max_depth={args.depth}'])
    cmd.extend(['-a', f'use_llm={str(args.use_llm).lower()}'])
    cmd.extend(['-a', f'use_existing_file={str(args.use_existing_file).lower()}'])
    
    # Drug spider specific args
    if args.spider == 'drug':
        cmd.extend(['-a', f'max_drugs={args.max_drugs}'])
        cmd.extend(['-a', f'follow_fda={str(args.follow_fda).lower()}'])
        cmd.extend(['-a', f'max_subpage_depth={args.max_subpage_depth}'])
        cmd.extend(['-a', f'max_subpages={args.max_subpages}'])
    
    use_http_after_first = args.http_after_first and not args.playwright_always
    cmd.extend(['-a', f'use_http_after_first={str(use_http_after_first).lower()}'])
    cmd.extend(['-a', f'http_only={str(args.http_only).lower()}'])
    if args.existing_file:
        cmd.extend(['-a', f'resume_file={args.existing_file}'])
    if args.follow_patterns:
        cmd.extend(['-a', f'follow_patterns={args.follow_patterns}'])

    # Per-run state folder: spider + domain + incrementing run number.
    seed_urls = args.url or args.urls or ''
    first_url = (seed_urls.split(',')[0] if seed_urls else '').strip()
    parsed = urlparse(first_url) if first_url else None
    domain = (parsed.netloc if parsed and parsed.netloc else 'no_domain').lower()
    domain_key = re.sub(r'[^a-z0-9]+', '_', domain).strip('_') or 'no_domain'

    crawl_state_root = '.crawl_state'
    os.makedirs(crawl_state_root, exist_ok=True)
    prefix = f"{args.spider}_{domain_key}_"
    max_run_no = 0
    for name in os.listdir(crawl_state_root):
        full = os.path.join(crawl_state_root, name)
        if not os.path.isdir(full):
            continue
        if not name.startswith(prefix):
            continue
        suffix = name[len(prefix):]
        if suffix.isdigit():
            max_run_no = max(max_run_no, int(suffix))

    run_no = max_run_no + 1
    jobdir = os.path.join(crawl_state_root, f"{prefix}{run_no:03d}")
    cmd.extend(['-s', f'JOBDIR={jobdir}'])

    # Expose metadata for pipelines (e.g., MongoDB)
    os.environ['RUN_NUMBER'] = str(run_no)
    os.environ['SITE_DOMAIN'] = domain
    
    # Print configuration
    print("=" * 80)
    print("🌐 GEOMETRIC SELF-HEALING CRAWLER")
    print("=" * 80)
    print(f"Spider: {args.spider}")
    print(f"Target: {args.url or args.urls}")
    print(f"Max pages: {args.pages}")
    print(f"Max depth: {args.depth}")
    print(f"Threads: {args.threads}")
    print(f"Delay: {args.delay}s")
    print(f"Parallel extraction: {'' if args.parallel else ''} ({args.workers} workers)")
    print(f"LLM Repair: {'' if args.use_llm else ''} (model: {args.llm_model})")
    print(f"Use existing file: {'' if args.use_existing_file else ''}")
    print(f"Detail only: {'' if args.detail_only else ''}")
    print(f"Playwright always: {'' if args.playwright_always else ''}")
    print(f"HTTP only: {'' if args.http_only else ''}")
    print(f"HTTP after first: {'' if use_http_after_first else ''}")
    if args.new_file:
        print("Force new file: ")
    if args.output:
        print(f"Output prefix: {args.output}")
    if args.existing_file:
        print(f"Existing file: {args.existing_file}")
    if args.follow_patterns:
        print(f"Follow patterns: {args.follow_patterns}")
    print("\n🛡️ PROTECTIONS:")
    print(f"  User Agent Rotation: {'' if args.rotate_ua and args.protections else ''}")
    print(f"  Random Delay: {'' if args.random_delay and args.protections else ''}")
    print(f"  Block Images: {'' if args.block_images else ''}")
    print("=" * 80)
    print("\n🚀 Starting crawl...\n")
    
    # Run spider
    from scrapy.cmdline import execute
    execute(cmd)

if __name__ == '__main__':
    main()