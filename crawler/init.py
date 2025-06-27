"""Web crawler module for content discovery and extraction"""

from .core_crawler import WebCrawler
from .url_manager import URLManager
from .content_processor import ContentProcessor
from .feed_generator import FeedGenerator

__version__ = "1.0.0"

__all__ = [
    'WebCrawler',
    'URLManager',
    'ContentProcessor',
    'FeedGenerator'
]
