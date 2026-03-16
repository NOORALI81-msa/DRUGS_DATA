"""
Domain-based output pipelines

Creates structure:

outputs/
   domain/
      csv/
      json/

Example:
outputs/medlineplus.gov/csv/medlineplus.gov_20260224_1.csv
outputs/medlineplus.gov/json/medlineplus.gov_20260224_1.jsonl

File naming: domain_YYYYMMDD_N.ext (N = sequential number 1, 2, 3...)
"""

import json
import csv
import os
import sys
import atexit
import signal
from datetime import datetime
from pathlib import Path
from scrapy.exceptions import DropItem

# CSV field size limit: 256KB (enough for most content, prevents memory issues)
# Default 128KB is too small for medical content, but we don't want unlimited
MAX_FIELD_SIZE = 262144  # 256KB per field
csv.field_size_limit(MAX_FIELD_SIZE)

# Maximum characters per field when writing (truncate to save storage)
MAX_FIELD_CHARS = 50000  # ~50KB text limit per field


def truncate_large_fields(data: dict, max_chars: int = MAX_FIELD_CHARS) -> dict:
    """Truncate string fields that exceed max_chars to save RAM/storage."""
    result = {}
    for key, value in data.items():
        if isinstance(value, str) and len(value) > max_chars:
            result[key] = value[:max_chars] + f"... [TRUNCATED - was {len(value)} chars]"
        elif isinstance(value, dict):
            result[key] = truncate_large_fields(value, max_chars)
        elif isinstance(value, list):
            result[key] = [
                truncate_large_fields(v, max_chars) if isinstance(v, dict) 
                else (v[:max_chars] + "... [TRUNCATED]" if isinstance(v, str) and len(v) > max_chars else v)
                for v in value
            ]
        else:
            result[key] = value
    return result


# ============================================================
# BASE PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / "outputs"


def get_next_sequence_number(directory: Path, domain: str, date: str, extension: str) -> int:
    """
    Get the next sequential number for output files.
    Scans existing files like domain_date_1.ext, domain_date_2.ext and returns next number.
    """
    if not directory.exists():
        return 1
    
    # Pattern: domain_date_N.ext
    import re
    pattern = re.compile(rf"^{re.escape(domain)}_{date}_(\d+)\.{extension}$")
    
    max_num = 0
    for f in directory.iterdir():
        match = pattern.match(f.name)
        if match:
            num = int(match.group(1))
            if num > max_num:
                max_num = num
    
    return max_num + 1


def print_output_summary(filename: Path, count: int, format_type: str):
    """Print prominent output file summary - visible even on keyboard interrupt"""
    print("\n" + "=" * 70)
    print(f"📄 OUTPUT FILE CREATED: {filename}")
    print(f"📊 Items saved: {count}")
    print(f"📁 Format: {format_type.upper()}")
    print("=" * 70 + "\n")


# Global tracking of active output files for interrupt handling
_active_output_files = {}


def register_output_file(pipeline_name: str, filename: Path, count_func):
    """Register an active output file for interrupt handling"""
    _active_output_files[pipeline_name] = (filename, count_func)


def unregister_output_file(pipeline_name: str):
    """Unregister an output file after normal completion"""
    _active_output_files.pop(pipeline_name, None)


def _on_exit():
    """Print any active output files on exit (including Ctrl+C)"""
    for name, (filename, count_func) in _active_output_files.items():
        if filename and filename.exists():
            try:
                count = count_func() if callable(count_func) else 0
                print(f"\n⚠️ Spider interrupted - partial output saved:")
                print_output_summary(filename, count, filename.suffix.lstrip('.'))
            except:
                print(f"\n⚠️ Output file: {filename}")


# Register exit handler
atexit.register(_on_exit)


# ============================================================
# JSON PIPELINE
# ============================================================

class JsonPipeline:

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        pipeline.crawler = crawler
        return pipeline

    def open_spider(self, spider):
        # Check if this pipeline should be active based on output format
        output_format = getattr(spider, 'output_format', os.getenv('OUTPUT_FORMAT', 'json')).lower()
        self.enabled = output_format in ('json', 'jsonl', 'both')
        
        if not self.enabled:
            self.file = None
            self.count = 0
            self.filename = None
            return
        
        now = datetime.now()
        date = now.strftime("%Y%m%d")
        timestamp = now.strftime("%H%M%S")

        # domain from spider (handle None/missing domain)
        domain = getattr(spider, "domain", None) or spider.name or "unknown"
        if domain.startswith('www.'):
            domain = domain[4:]

        # folder structure
        domain_dir = OUTPUT_DIR / domain / "json"
        domain_dir.mkdir(parents=True, exist_ok=True)

        self.filename = domain_dir / f"{domain}_{date}_{timestamp}.jsonl"
        
        self.file = open(self.filename, "w", encoding="utf-8")
        self.count = 0
        self.drug_complete_count = 0
        
        # Register for interrupt handling
        register_output_file("json", self.filename, lambda: self.count)

        spider.logger.info(f" JSON output: {self.filename}")

    def process_item(self, item, spider):
        # Skip if pipeline is disabled
        if not self.enabled:
            return item
        
        # 🔧 FIX: Properly filter non-detail items when DETAIL_ONLY is enabled
        if os.getenv("DETAIL_ONLY", "false").lower() == "true":
            if item.get("container_type") != "page_detail":
                spider.logger.debug(f"  Skipping non-detail JSON item: {item.get('container_type')}")
                raise DropItem(f"Not a page_detail: {item.get('container_type')}")
        
        # Truncate large fields to save RAM/storage
        truncated_item = truncate_large_fields(dict(item))
        
        # MANDATORY: Always set timestamp for each row
        if not truncated_item.get("scraped_at"):
            truncated_item["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Add explicit numbering so users can track progress directly from JSON.
        row_no = self.count + 1
        truncated_item["row_no"] = row_no

        if truncated_item.get("container_type") == "drug_complete":
            self.drug_complete_count += 1
            truncated_item["drug_no"] = self.drug_complete_count
            if isinstance(truncated_item.get("data"), dict):
                truncated_item["data"]["drug_no"] = self.drug_complete_count
        
        json.dump(truncated_item, self.file, ensure_ascii=False, indent=2)
        self.file.write("\n")
        self.file.flush()

        self.count += 1
        spider.logger.info(f"JSON saved item {self.count}")
        return item

    def close_spider(self, spider):
        if self.file:
            self.file.close()
        spider.logger.info(f" JSON done: {self.count}")
        
        # Unregister from interrupt handler (completed normally)
        unregister_output_file("json")
        
        # Print prominent output summary (visible even on Ctrl+C)
        if self.enabled and self.filename:
            print_output_summary(self.filename, self.count, "jsonl")


# ============================================================
# CSV PIPELINE
# ============================================================

class CsvPipeline:

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        pipeline.crawler = crawler
        return pipeline

    def open_spider(self, spider):
        # Check if this pipeline should be active based on output format
        output_format = getattr(spider, 'output_format', os.getenv('OUTPUT_FORMAT', 'json')).lower()
        self.enabled = output_format in ('csv', 'both')
        
        if not self.enabled:
            self.file = None
            self.writer = None
            self.count = 0
            self.filename = None
            return
        
        now = datetime.now()
        date = now.strftime("%Y%m%d")
        timestamp = now.strftime("%H%M%S")

        # Handle None/missing domain gracefully
        domain = getattr(spider, "domain", None) or spider.name or "unknown"
        if domain.startswith('www.'):
            domain = domain[4:]

        domain_dir = OUTPUT_DIR / domain / "csv"
        domain_dir.mkdir(parents=True, exist_ok=True)

        use_existing = os.getenv("USE_EXISTING_OUTPUT", "false").lower() == "true"
        existing_output_file = os.getenv("EXISTING_OUTPUT_FILE", "").strip()
        output_prefix = os.getenv("OUTPUT_PREFIX", "").strip()
        force_new = os.getenv("FORCE_NEW_OUTPUT", "false").lower() == "true"

        self.append_mode = False
        if use_existing:
            if existing_output_file:
                self.filename = Path(existing_output_file)
                if not self.filename.is_absolute():
                    self.filename = BASE_DIR / self.filename
            else:
                existing_files = sorted(domain_dir.glob(f"{domain}_*.csv"), key=lambda p: p.stat().st_mtime)
                if existing_files:
                    self.filename = existing_files[-1]
                else:
                    self.filename = domain_dir / f"{domain}_{date}_{timestamp}.csv"
            self.append_mode = self.filename.exists()
        else:
            if output_prefix:
                self.filename = domain_dir / f"{output_prefix}.csv"
            elif force_new:
                self.filename = domain_dir / f"{domain}_{date}_{timestamp}.csv"
            else:
                # Default: use actual timestamp
                self.filename = domain_dir / f"{domain}_{date}_{timestamp}.csv"

        self.filename.parent.mkdir(parents=True, exist_ok=True)
        mode = "a" if self.append_mode else "w"
        self.file = open(self.filename, mode, newline="", encoding="utf-8-sig")

        self.writer = None
        self.count = 0
        self.default_fieldnames = None
        
        # Register for interrupt handling
        register_output_file("csv", self.filename, lambda: self.count)

        mode_label = "append" if self.append_mode else "new"
        spider.logger.info(f" CSV output ({mode_label}): {self.filename}")

    # --------------------------------------------------------
    # Flatten nested item.data with clean structure
    # --------------------------------------------------------
    def sanitize_column_name(self, name: str) -> str:
        """
        Clean up column name for CSV export.
        Removes special characters, standardizes format.
        Preserves underscores for readability.
        """
        import re
        if not name:
            return "field"
        
        # Remove common prefixes
        for prefix in ['data_', 'section_', 'field_']:
            if name.startswith(prefix):
                name = name[len(prefix):]
        
        # Convert to lowercase
        name = name.lower()
        
        # Remove special characters but keep underscores and spaces
        name = re.sub(r'[^a-z0-9\s_]', '', name)
        
        # Convert spaces to underscores
        name = re.sub(r'\s+', '_', name.strip())
        
        # Remove duplicate underscores
        name = re.sub(r'_+', '_', name).strip('_')
        
        # Truncate very long names
        if len(name) > 50:
            name = name[:50].rstrip('_')
        
        return name or "field"
    
    def flatten_item(self, item):
        # Truncate large fields first to save RAM/storage
        item = truncate_large_fields(dict(item))
        flat = {}

        # Core metadata first
        flat["url"] = item.get("url", "")
        flat["domain"] = item.get("domain", "")
        
        # MANDATORY: Always set timestamp for each row
        scraped_at = item.get("scraped_at", "")
        if not scraped_at:
            scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        flat["scraped_at"] = scraped_at

        # Check if item has "data" key (ScrapedItem from built-in spiders)
        # or is a plain dict (from generated spiders)
        data = item.get("data", {})
        
        # If no "data" key, treat the item itself as data (generated spider output)
        if not data and item:
            # Use all keys except metadata keys
            metadata_keys = {"url", "domain", "scraped_at", "container_type", "confidence", "layout_hash"}
            data = {k: v for k, v in item.items() if k not in metadata_keys}

        if isinstance(data, dict):
            for key, value in data.items():
                # Skip the raw 'sections' list - we want flattened section columns
                if key == "sections":
                    continue
                
                # Clean the column name
                field = self.sanitize_column_name(key)

                if isinstance(value, str):
                    flat[field] = value.strip()

                elif isinstance(value, list):
                    if value and isinstance(value[0], dict):
                        flat[field] = json.dumps(value, ensure_ascii=False)
                    else:
                        flat[field] = "\n".join(str(v).strip() for v in value if v)

                elif isinstance(value, dict):
                    flat[field] = json.dumps(value, ensure_ascii=False)

                else:
                    flat[field] = str(value)

        return flat

    # --------------------------------------------------------
    def _rewrite_with_new_fields(self, new_fields, flat, spider):
        """Rewrite CSV with expanded headers to include new fields."""
        existing_rows = []
        existing_fields = list(self.writer.fieldnames) if self.writer else []
        updated_fields = sorted(set(existing_fields) | set(new_fields) | set(flat.keys()))

        # Read existing rows
        try:
            self.file.close()
            with open(self.filename, "r", newline="", encoding="utf-8-sig") as read_file:
                reader = csv.DictReader(read_file)
                existing_rows = list(reader)
        except FileNotFoundError:
            existing_rows = []

        # Rewrite file with expanded header
        self.file = open(self.filename, "w", newline="", encoding="utf-8-sig")
        self.writer = csv.DictWriter(self.file, fieldnames=updated_fields)
        self.writer.writeheader()
        for row in existing_rows:
            self.writer.writerow(row)
        self.file.flush()

        spider.logger.info(
            f"🔁 CSV header updated with new fields: {', '.join(sorted(new_fields))}"
        )

    def process_item(self, item, spider):
        # Skip if pipeline is disabled
        if not self.enabled:
            return item
        
        # 🔧 FIX: Properly filter non-detail items when DETAIL_ONLY is enabled
        if os.getenv("DETAIL_ONLY", "false").lower() == "true":
            if item.get("container_type") != "page_detail":
                spider.logger.debug(f"  Skipping non-detail item: {item.get('container_type')}")
                raise DropItem(f"Not a page_detail: {item.get('container_type')}")

        flat = self.flatten_item(item)
        
        # Filter to user-specified columns if CSV_OUTPUT_COLUMNS is set
        csv_columns = os.getenv("CSV_OUTPUT_COLUMNS", "").strip()
        if csv_columns:
            selected_columns = [c.strip() for c in csv_columns.split(",") if c.strip()]
            # Filter flat dict to only include selected columns (preserve order)
            filtered_flat = {}
            for col in selected_columns:
                if col in flat:
                    filtered_flat[col] = flat[col]
                else:
                    filtered_flat[col] = ""  # Empty value for missing columns
            flat = filtered_flat

        if self.writer is None:
            # Use selected columns order if specified, otherwise sort
            csv_columns_env = os.getenv("CSV_OUTPUT_COLUMNS", "").strip()
            if csv_columns_env:
                fieldnames = [c.strip() for c in csv_columns_env.split(",") if c.strip()]
            else:
                fieldnames = sorted(flat.keys())
            self.writer = csv.DictWriter(self.file, fieldnames=fieldnames)
            need_header = True
            if self.append_mode:
                try:
                    need_header = self.filename.stat().st_size == 0
                except FileNotFoundError:
                    need_header = True
            if need_header:
                self.writer.writeheader()
            # Flush headers immediately so file exists on disk
            self.file.flush()
            if hasattr(os, 'fsync'):
                try:
                    os.fsync(self.file.fileno())
                except:
                    pass
        else:
            new_fields = [key for key in flat.keys() if key not in self.writer.fieldnames]
            if new_fields:
                self._rewrite_with_new_fields(new_fields, flat, spider)

        self.writer.writerow(flat)
        self.file.flush()
        
        # Force sync to disk after each write
        if hasattr(os, 'fsync'):
            try:
                os.fsync(self.file.fileno())
            except:
                pass

        self.count += 1
        spider.logger.info(f"CSV saved item {self.count}")
        return item

    def close_spider(self, spider):
        # Skip if pipeline is disabled
        if not self.enabled or not self.file:
            return
        
        # If no items were written, write headers with default fields so file isn't empty
        if self.writer is None and self.default_fieldnames is None:
            default_fields = ['url', 'domain', 'container_type', 'confidence', 'scraped_at', 'layout_hash']
            self.writer = csv.DictWriter(self.file, fieldnames=default_fields)
            self.writer.writeheader()
            self.file.flush()
            if hasattr(os, 'fsync'):
                try:
                    os.fsync(self.file.fileno())
                except:
                    pass
        
        self.file.flush()
        # Final sync before close
        if hasattr(os, 'fsync'):
            try:
                os.fsync(self.file.fileno())
            except:
                pass
        self.file.close()
        spider.logger.info(f"CSV done: {self.count}")
        
        # Unregister from interrupt handler (completed normally)
        unregister_output_file("csv")
        
        # Print prominent output summary (visible even on Ctrl+C)
        if self.filename:
            print_output_summary(self.filename, self.count, "csv")
