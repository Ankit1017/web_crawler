import os
from dataclasses import dataclass
from typing import List, Dict


@dataclass
class CrawlerConfig:
    """Configuration settings for the web crawler"""

    # Crawling settings
    MAX_PAGES: int = 1000
    DELAY_BETWEEN_REQUESTS: float = 1.0
    REQUEST_TIMEOUT: int = 30
    MAX_RETRIES: int = 3

    # Content filtering
    USEFUL_URL_PATTERNS: List[str] = None
    EXCLUDED_EXTENSIONS: List[str] = None
    MIN_CONTENT_LENGTH: int = 100

    # Database settings
    DATABASE_URL: str = "sqlite:///webcrawler.db"
    REDIS_URL: str = "redis://localhost:6379/0"

    # Search settings
    ELASTICSEARCH_URL: str = "http://localhost:9200"

    # Feed settings
    FEED_TITLE: str = "Web Crawler Feed"
    FEED_DESCRIPTION: str = "Curated content from web crawling"
    MAX_FEED_ITEMS: int = 50

    def __post_init__(self):
        if self.USEFUL_URL_PATTERNS is None:
            self.USEFUL_URL_PATTERNS = [
                r'/article/', r'/blog/', r'/news/', r'/post/',
                r'/story/', r'/content/', r'/page/'
            ]

        if self.EXCLUDED_EXTENSIONS is None:
            self.EXCLUDED_EXTENSIONS = [
                '.pdf', '.jpg', '.jpeg', '.png', '.gif',
                '.mp4', '.avi', '.zip', '.exe', '.css', '.js'
            ]


# Global configuration instance
config = CrawlerConfig()
