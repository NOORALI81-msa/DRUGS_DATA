#!/usr/bin/env python
# run_parallel.py
"""
Multi-Site Parallel Crawler
Crawl multiple websites simultaneously using multiprocessing.
"""
import os
import sys
import argparse
import multiprocessing
from multiprocessing import Pool, Process, Queue
from datetime import datetime
import time

# Add current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def run_single_crawl(args_tuple):
    """Run a single crawl in a separate process"""
    url, max_pages, max_depth, follow_patterns, output_prefix = args_tuple
    
    from urllib.parse import urlparse
    domain = urlparse(url).netloc.replace('www.', '')
    
    # Set environment variables for this process
    os.environ['SCRAPY_SETTINGS_MODULE'] = 'geometric_crawler.settings'
    os.environ['MAX_PAGES'] = str(max_pages)
    os.environ['MAX_DEPTH'] = str(max_depth)
    os.environ['FORCE_NEW_OUTPUT'] = 'true'
    os.environ['DETAIL_ONLY'] = 'true'
    
    # Build command
    cmd = [
        'scrapy', 'crawl', 'geometric',
        '-a', f'urls={url}',
        '-a', f'max_pages={max_pages}',
        '-a', f'max_depth={max_depth}',
    ]
    
    if follow_patterns:
        cmd.extend(['-a', f'follow_patterns={follow_patterns}'])
    
    print(f"\n🚀 Starting crawl: {domain}")
    print(f"   URL: {url}")
    print(f"   Pages: {max_pages}, Depth: {max_depth}")
    if follow_patterns:
        print(f"   Follow patterns: {follow_patterns}")
    
    # Run spider
    try:
        from scrapy.cmdline import execute
        execute(cmd)
        return {'domain': domain, 'status': 'success'}
    except Exception as e:
        print(f"❌ Error crawling {domain}: {e}")
        return {'domain': domain, 'status': 'error', 'error': str(e)}


def run_parallel_crawls(sites, max_pages, max_depth, follow_patterns, cores):
    """Run multiple crawls in parallel using multiprocessing"""
    
    # Prepare arguments for each site
    crawl_args = []
    for site in sites:
        url = site['url']
        patterns = site.get('follow_patterns') or follow_patterns
        crawl_args.append((url, max_pages, max_depth, patterns, None))
    
    print(f"\n{'='*80}")
    print(f"🌐 PARALLEL MULTI-SITE CRAWLER")
    print(f"{'='*80}")
    print(f"Sites to crawl: {len(sites)}")
    print(f"Cores to use: {cores}")
    print(f"Max pages per site: {max_pages}")
    print(f"Max depth: {max_depth}")
    print(f"{'='*80}\n")
    
    start_time = time.time()
    
    # Use multiprocessing Pool to run crawls in parallel
    # Note: Due to Scrapy's architecture, we run in sequence per process
    # but multiple processes can run different sites
    with Pool(processes=cores) as pool:
        results = pool.map(run_single_crawl, crawl_args)
    
    elapsed = time.time() - start_time
    
    # Summary
    print(f"\n{'='*80}")
    print(f"📊 PARALLEL CRAWL SUMMARY")
    print(f"{'='*80}")
    print(f"Total time: {elapsed:.1f} seconds")
    success = sum(1 for r in results if r['status'] == 'success')
    print(f"Successful: {success}/{len(sites)}")
    
    for result in results:
        status_icon = '✅' if result['status'] == 'success' else '❌'
        print(f"  {status_icon} {result['domain']}: {result['status']}")
    
    print(f"{'='*80}\n")
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description='Multi-Site Parallel Crawler - Crawl multiple websites simultaneously'
    )
    
    # Site options
    parser.add_argument('--urls', nargs='+', help='List of URLs to crawl')
    parser.add_argument('--preset', choices=['drugs', 'food', 'ecommerce', 'news', 'mixed'],
                       help='Use preset list of sites')
    
    # Performance options
    parser.add_argument('--cores', type=int, default=4, help='Number of CPU cores to use')
    parser.add_argument('--pages', type=int, default=50, help='Max pages per site')
    parser.add_argument('--depth', type=int, default=2, help='Max crawl depth')
    parser.add_argument('--follow-patterns', help='URL patterns to follow (regex)')
    
    args = parser.parse_args()
    
    # Define preset site lists
    presets = {
        'drugs': [
            {'url': 'https://www.1mg.com/drugs-all-medicines', 'follow_patterns': '/drugs/'},
            {'url': 'https://medlineplus.gov/druginformation.html', 'follow_patterns': '/druginfo/'},
            {'url': 'https://www.rxlist.com/drugs/alpha_a.htm', 'follow_patterns': '/'},
        ],
        'food': [
            {'url': 'https://www.zomato.com/ncr/restaurants', 'follow_patterns': '/restaurant/'},
            {'url': 'https://www.swiggy.com/restaurants', 'follow_patterns': '/restaurants/'},
        ],
        'ecommerce': [
            {'url': 'https://www.amazon.in/bestsellers', 'follow_patterns': '/dp/'},
            {'url': 'https://www.flipkart.com', 'follow_patterns': '/p/'},
        ],
        'news': [
            {'url': 'https://news.ycombinator.com', 'follow_patterns': '/item'},
            {'url': 'https://www.reddit.com/r/programming', 'follow_patterns': '/comments/'},
        ],
        'mixed': [
            {'url': 'https://www.1mg.com/drugs-all-medicines', 'follow_patterns': '/drugs/'},
            {'url': 'https://medlineplus.gov/druginformation.html', 'follow_patterns': '/druginfo/'},
            {'url': 'https://www.amazon.in/bestsellers', 'follow_patterns': '/dp/'},
        ],
    }
    
    # Build site list
    sites = []
    
    if args.preset:
        sites = presets.get(args.preset, [])
        print(f"📚 Using preset: {args.preset} ({len(sites)} sites)")
    elif args.urls:
        for url in args.urls:
            sites.append({'url': url, 'follow_patterns': args.follow_patterns})
    else:
        print("❌ Error: Please provide --urls or --preset")
        print("\nExamples:")
        print("  python run_parallel.py --preset drugs --cores 4 --pages 50")
        print("  python run_parallel.py --urls https://site1.com https://site2.com --cores 2")
        sys.exit(1)
    
    if not sites:
        print("❌ Error: No sites to crawl")
        sys.exit(1)
    
    # Run parallel crawls
    results = run_parallel_crawls(
        sites=sites,
        max_pages=args.pages,
        max_depth=args.depth,
        follow_patterns=args.follow_patterns,
        cores=min(args.cores, len(sites))  # Don't use more cores than sites
    )
    
    return 0 if all(r['status'] == 'success' for r in results) else 1


if __name__ == '__main__':
    # Required for Windows multiprocessing
    multiprocessing.freeze_support()
    sys.exit(main())
