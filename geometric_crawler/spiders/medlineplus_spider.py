"""
MedlinePlus Drug Spider

"""

import scrapy
import re
import hashlib
import csv
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from ..items import ScrapedItem


# Standard section name mapping for clean CSV columns
# Maps MedlinePlus headings to clean column names
SECTION_NAME_MAP = {
    # Uses/Indications
    "why is this medication prescribed?": "uses",
    "why is this medicine prescribed?": "uses",
    "what is this medication used for?": "uses",
    "indications": "uses",
    "uses": "uses",
    
    # Dosage/Administration
    "how should this medicine be used?": "dosage",
    "how should i take this medicine?": "dosage",
    "how should this medication be taken?": "dosage",
    "dosage and administration": "dosage",
    "dosage": "dosage",
    "directions": "dosage",
    
    # Side Effects
    "what side effects can this medication cause?": "side_effects",
    "what are the side effects?": "side_effects",
    "side effects": "side_effects",
    "adverse reactions": "side_effects",
    
    # Precautions/Warnings
    "what special precautions should i follow?": "precautions",
    "precautions": "precautions",
    "warnings": "warnings",
    "warnings and precautions": "warnings",
    
    # Storage
    "what storage conditions are needed for this medicine?": "storage",
    "how should i store this medication?": "storage",
    "storage": "storage",
    
    # Overdose/Emergency
    "in case of emergency/overdose": "overdose",
    "what should i do in case of overdose?": "overdose",
    "overdose": "overdose",
    "emergency": "overdose",
    
    # Missed Dose
    "what should i do if i forget a dose?": "missed_dose",
    "missed dose": "missed_dose",
    
    # Diet
    "what special dietary instructions should i follow?": "dietary_instructions",
    "dietary instructions": "dietary_instructions",
    "diet": "dietary_instructions",
    
    # Other Uses
    "other uses for this medicine": "other_uses",
    "off-label uses": "other_uses",
    
    # Other Information
    "what other information should i know?": "other_info",
    "other information": "other_info",
    
    # Brand Names
    "brand names": "brand_names",
    "brand name": "brand_names",
    "other names": "other_names",
    
    # Overview/Description
    "overview": "overview",
    "description": "description",
    "about this drug": "description",
}


def sanitize_section_name(heading: str) -> str:
    """
    Convert section heading to clean column name.
    Uses mapping for common headings, falls back to sanitization.
    """
    if not heading:
        return "overview"
    
    heading_lower = heading.lower().strip()
    
    # Check mapping first
    if heading_lower in SECTION_NAME_MAP:
        return SECTION_NAME_MAP[heading_lower]
    
    # Fallback: sanitize the heading
    # Remove special characters, convert spaces to underscores
    sanitized = re.sub(r'[^a-z0-9\s]', '', heading_lower)
    sanitized = re.sub(r'\s+', '_', sanitized.strip())
    sanitized = re.sub(r'_+', '_', sanitized)  # Remove duplicate underscores
    sanitized = sanitized.strip('_')
    
    # Truncate very long names
    if len(sanitized) > 40:
        sanitized = sanitized[:40].rstrip('_')
    
    return sanitized or "content"


class MedlinePlusSpider(scrapy.Spider):
    name = "medlineplus"
    allowed_domains = ["medlineplus.gov"]
    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
            "https": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
        }
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Parallel extraction settings
        self.num_cores = int(kwargs.get('cores', 4))
        self.thread_pool = ThreadPoolExecutor(max_workers=self.num_cores)

        # Start page (A-Z listing)
        self.start_urls = [
            "https://medlineplus.gov/druginfo/drug_Aa.html",
            "https://medlineplus.gov/druginfo/drug_Bb.html",
            "https://medlineplus.gov/druginfo/drug_Cc.html",
            "https://medlineplus.gov/druginfo/drug_Dd.html",
            "https://medlineplus.gov/druginfo/drug_Ee.html",
        ]

        # Only accept URLs that are on medlineplus.gov
        if "urls" in kwargs:
            custom_urls = kwargs.get("urls", "").split(",")
            valid_urls = [u.strip() for u in custom_urls if "medlineplus.gov" in u and u.strip()]
            if valid_urls:
                self.start_urls = valid_urls
            else:
                self.logger.warning(" Custom URLs ignored - MedlinePlus spider only accepts medlineplus.gov URLs")

        self.max_depth = int(kwargs.get("max_depth", 2))
        self.max_pages = int(kwargs.get("max_pages", 100))
        self.visited_urls = set()
        self.visited_master_links = set()  # Track visited master (A-Z) links
        self.domain = "medlineplus.gov"
        self.use_existing_file = kwargs.get("use_existing_file", "false").lower() == "true"
        self.resume_file = kwargs.get("resume_file", "")
        self.resume_urls = set()
        self.follow_related = kwargs.get("follow_related", "false").lower() == "true"

        if self.use_existing_file:
            self._load_resume_state()

        self.stats = {
            "drug_pages": 0,
            "listing_pages": 0,
            "spanish_skipped": 0,
            "errors": 0,
        }

    def _resolve_resume_file(self):
        if self.resume_file:
            candidate = Path(self.resume_file)
            if not candidate.is_absolute():
                candidate = Path(__file__).resolve().parents[2] / candidate
            # Make sure it's a file, not a directory
            if candidate.exists() and candidate.is_file():
                return candidate
            return None

        base_dir = Path(__file__).resolve().parents[2]
        csv_dir = base_dir / "outputs" / self.domain / "csv"
        
        # Check if directory exists before globbing
        if not csv_dir.exists() or not csv_dir.is_dir():
            return None
            
        files = sorted(csv_dir.glob(f"{self.domain}_*.csv"), key=lambda p: p.stat().st_mtime)
        return files[-1] if files else None

    def _load_resume_state(self):
        resume_path = self._resolve_resume_file()
        if not resume_path or not resume_path.exists():
            self.logger.info(" Resume mode enabled but no existing CSV found; starting fresh")
            return

        last_url = None
        loaded = 0
        with open(resume_path, "r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                url = (row.get("url") or "").strip()
                if not url:
                    continue
                self.resume_urls.add(url)
                last_url = url
                loaded += 1

        self.resume_last_url = last_url

        if self.resume_last_url:
            self.logger.info(f" Resume mode loaded {loaded} URLs, last URL: {self.resume_last_url}")
        else:
            self.logger.info(" Resume mode found CSV without URLs; starting fresh")

    # ======================================================
    # START REQUESTS (Scrapy 2.13+ compatible)
    # ======================================================
    async def start(self):
        """Async start method for Scrapy 2.13+"""
        for url in self.start_urls:
            callback = self.parse_drug if re.search(r"/meds/[a-z]\d+\.html$", url) else self.parse_listing
            if callback == self.parse_drug and url in self.resume_urls:
                continue
            yield scrapy.Request(
                url=url,
                callback=callback,
                meta={"depth": 0},
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept-Language": "en-US,en;q=0.9",
                },
            )
    
    def start_requests(self):
        """Fallback for Scrapy < 2.13 compatibility"""
        for url in self.start_urls:
            callback = self.parse_drug if re.search(r"/meds/[a-z]\d+\.html$", url) else self.parse_listing
            if callback == self.parse_drug and url in self.resume_urls:
                continue
            yield scrapy.Request(
                url=url,
                callback=callback,
                meta={"depth": 0},
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept-Language": "en-US,en;q=0.9",
                },
            )

    # ======================================================
    # LISTING PAGE (A-Z)
    # ======================================================

    def parse_listing(self, response):
        depth = response.meta.get("depth", 0)
        self.stats["listing_pages"] += 1
        self.logger.info(f" Listing page (depth={depth}): {response.url}")

        # Only process drug links on the given start URL (e.g., Aa page). No master/A-Z crawling.

        # Only process drug links if this is a revisit with process_drugs flag or after all master links are queued
        # Find and queue all drug links on the current listing page
        drug_links = response.xpath('//a[contains(@href, "/meds/") and not(contains(@href, "/spanish/"))]/@href').getall()
        self.logger.info(f" Found {len(drug_links)} drug links on {response.url}")
        for href in drug_links:
            url = response.urljoin(href)
            if "/spanish/" in url:
                self.stats["spanish_skipped"] += 1
                continue
            if url not in self.visited_urls:
                self.visited_urls.add(url)
                self.logger.info(f" Queuing drug page: {url}")
                yield scrapy.Request(url, callback=self.parse_drug, meta={"depth": depth + 1})

        # If no drug links found, treat this page as a drug page and try to extract data
        if not drug_links:
            self.logger.warning(f" No drug links found on {response.url}, attempting to extract as drug page.")
            # Avoid infinite recursion: only do this if not already in parse_drug
            if not response.meta.get("_forced_drug_extract"):
                yield scrapy.Request(
                    response.url,
                    callback=self.parse_drug,
                    meta={"depth": depth + 1, "_forced_drug_extract": True},
                    dont_filter=True
                )

    # ======================================================
    # DRUG PAGE
    # ======================================================
    def parse_drug(self, response):
        depth = response.meta.get("depth", 0)
        url = response.url

        if "/spanish/" in url:
            self.stats["spanish_skipped"] += 1
            return

        if url in self.resume_urls:
            return

        if self.stats["drug_pages"] >= self.max_pages:
            self.crawler.engine.close_spider(self, "max_pages_reached")
            return

        self.stats["drug_pages"] += 1
        self.logger.info(f"Extracting drug page: {url}")

        drug_data = {}

        # ---------------- TITLE ----------------
        title = response.xpath("string(//h1)").get()
        drug_data["title"] = title.strip() if title else url.split("/")[-1]

        # ---------------- DRUG ID ----------------
        m = re.search(r"/([a-z]\d+\.html)", url)
        if m:
            drug_data["drug_id"] = m.group(1)

        # ==================================================
        # MAIN DRUG CONTENT
        # MedlinePlus currently uses #mplus-content.
        # Keep fallback selectors for layout changes.
        # ==================================================
        content_selectors = [
            '//*[@id="mplus-content"]//article',
            '//*[@id="mplus-content"]',
            '//*[@id="drug-content"]',
            '//*[@id="main-content"]',
        ]

        main = None
        for selector in content_selectors:
            candidate = response.xpath(selector)
            if candidate:
                main = candidate
                break

        if not main:
            self.logger.warning("⚠️ No known content container found")
        else:
            sections = []
            current_section = {
                "heading": "Overview",
                "content": []
            }

            for node in main.xpath('.//*[self::h2 or self::p or self::ul or self::ol]'):
                tag = node.root.tag.lower()

                if tag == "h2":
                    heading = node.xpath("string()").get()
                    if heading and heading.strip():
                        if current_section["content"]:
                            sections.append(current_section)
                        current_section = {
                            "heading": heading.strip(),
                            "content": []
                        }
                    continue

                if tag == "p":
                    text = node.xpath("string()").get()
                    if text and text.strip():
                        current_section["content"].append({
                            "type": "paragraph",
                            "text": text.strip()
                        })
                elif tag in ["ul", "ol"]:
                    items = [
                        li.xpath("string()").get().strip()
                        for li in node.xpath("./li")
                        if li.xpath("string()").get()
                    ]
                    if items:
                        current_section["content"].append({
                            "type": "list",
                            "items": items
                        })

            if current_section["content"]:
                sections.append(current_section)

            # Store sections in drug_data for logging and output
            drug_data["sections"] = sections

            if sections:
                # Process sections in parallel using thread pool
                full_content_blocks = []
                summary_paragraphs = []

                def _process_section(section):
                    """Process a single section - can run in parallel"""
                    heading = (section.get("heading") or "").strip()
                    section_key = sanitize_section_name(heading)
                    heading_block = f"## {heading}" if heading else None
                    
                    content_parts = []
                    content_blocks = []
                    for part in section.get("content", []):
                        if part.get("type") == "paragraph":
                            text = part.get("text", "").strip()
                            if text:
                                content_parts.append(text)
                                content_blocks.append(text)
                        elif part.get("type") == "text":
                            text = part.get("text", "").strip()
                            if text:
                                content_parts.append(text)
                                content_blocks.append(text)
                        elif part.get("type") == "list":
                            for item_text in part.get("items", []):
                                item_text = str(item_text).strip()
                                if item_text:
                                    line = f"• {item_text}"
                                    content_parts.append(line)
                                    content_blocks.append(line)
                    
                    content_text = "\n".join(content_parts)
                    return {
                        "section_key": section_key,
                        "content_text": content_text,
                        "heading_block": heading_block,
                        "content_blocks": content_blocks,
                        "first_paragraphs": [p for p in content_parts[:2] if not p.startswith("•")],
                    }

                # Run section processing in parallel
                results = list(self.thread_pool.map(_process_section, sections))
                
                for result in results:
                    if result["heading_block"]:
                        full_content_blocks.append(result["heading_block"])
                    full_content_blocks.extend(result["content_blocks"])
                    
                    if result["content_text"] and result["section_key"]:
                        drug_data[result["section_key"]] = result["content_text"]
                    
                    if len(summary_paragraphs) < 3:
                        for p in result["first_paragraphs"]:
                            if len(summary_paragraphs) < 3:
                                summary_paragraphs.append(p)
                
                # Store full_content with all headings and content
                if full_content_blocks:
                    drug_data["full_content"] = "\n\n".join(full_content_blocks)
                if summary_paragraphs:
                    drug_data["summary"] = " ".join(summary_paragraphs)

        # ==================================================
        # FALLBACK FULL TEXT CONTENT (if no sections extracted)
        # ==================================================
        if not drug_data.get("full_content") and main:
            paragraphs = [
                p.xpath("string()").get().strip()
                for p in main.xpath(".//p")
                if p.xpath("string()").get()
            ]
            full_content_blocks = paragraphs
            summary_paragraphs = paragraphs[:3]

        if full_content_blocks:
            drug_data["full_content"] = "\n\n".join(full_content_blocks)
        if summary_paragraphs:
            drug_data["summary"] = " ".join(summary_paragraphs)

        # ==================================================
        # RELATED DRUGS
        # ==================================================
        related = []
        for a in response.xpath('//a[contains(@href,"/meds/")]'):
            href = a.xpath("@href").get()
            text = a.xpath("string()").get()

            if href and text and "/spanish/" not in href:
                related.append({
                    "name": text.strip(),
                    "url": response.urljoin(href)
                })

        if related:
            drug_data["related_drugs"] = related[:10]

        # ==================================================
        # IMAGE AND VIDEO EXTRACTION
        # ==================================================
        # Extract image URLs
        image_urls = []
        for img in response.css("img[src]"):
            src = img.attrib.get("src", "") or img.attrib.get("data-src", "")
            if src and not src.startswith("data:"):
                if src.startswith("//"):
                    src = "https:" + src
                elif src.startswith("/"):
                    src = response.urljoin(src)
                image_urls.append(src)
        if image_urls:
            drug_data["image_urls"] = ", ".join(image_urls[:20])

        # Extract video URLs
        video_urls = []
        for video in response.css("video source[src], video[src]"):
            src = video.attrib.get("src", "")
            if src:
                video_urls.append(response.urljoin(src) if not src.startswith("http") else src)
        for iframe in response.css("iframe[src]"):
            src = iframe.attrib.get("src", "")
            if any(p in src for p in ["youtube", "vimeo", "dailymotion", "video"]):
                video_urls.append(src)
        if video_urls:
            drug_data["video_urls"] = ", ".join(video_urls[:10])

        # follow related (optional)
        if self.follow_related and depth < self.max_depth:
            for r in related[:10]:
                link = r["url"]
                if link not in self.visited_urls:
                    self.visited_urls.add(link)
                    yield scrapy.Request(link, callback=self.parse_drug, meta={"depth": depth + 1})

        # ==================================================
        # CREATE ITEM
        # ==================================================
        item = ScrapedItem()
        item["url"] = url
        item["domain"] = self.domain
        item["container_type"] = "drug_detail"
        item["data"] = drug_data
        item["confidence"] = 1.0
        item["layout_hash"] = hashlib.md5(response.text[:1000].encode()).hexdigest()

        self.logger.info(f" Extracted sections: {len(drug_data.get('sections', []))}")
        yield item

    # ======================================================
    # CLOSE
    # ======================================================
    def closed(self, reason):
        if hasattr(self, 'thread_pool'):
            self.thread_pool.shutdown(wait=False)
        self.logger.info("=" * 60)
        self.logger.info("MEDLINE SPIDER STATS")
        self.logger.info(f"Drug pages: {self.stats['drug_pages']}")
        self.logger.info(f"Listing pages: {self.stats['listing_pages']}")
        self.logger.info(f"Spanish skipped: {self.stats['spanish_skipped']}")
        self.logger.info(f"Errors: {self.stats['errors']}")
        self.logger.info(f"Parallel cores: {self.num_cores}")
        self.logger.info("=" * 60)