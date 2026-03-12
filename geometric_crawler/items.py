# geometric_crawler/items.py
import scrapy
from datetime import datetime

class ScrapedItem(scrapy.Item):
    """Universal scraped item"""
    url = scrapy.Field()
    domain = scrapy.Field()
    container_type = scrapy.Field()
    data = scrapy.Field()  # Dictionary of extracted fields
    confidence = scrapy.Field()
    layout_hash = scrapy.Field()
    repair_count = scrapy.Field()
    scraped_at = scrapy.Field()
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self['scraped_at'] = datetime.now().isoformat()