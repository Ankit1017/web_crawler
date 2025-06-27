import asyncio
import aiohttp
import time
import logging
from urllib.parse import urljoin, urlparse
from typing import Set, List, Optional
from bs4 import BeautifulSoup
import re

from crawler.url_manager import URLManager
from crawler.content_processor import ContentProcessor
from crawler.feed_generator import FeedGenerator
from config import config
from utils.database import DatabaseManager
from utils.helpers import is_valid_url, normalize_url

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WebCrawler:
    """Main web crawler class that orchestrates the crawling process"""

    def __init__(self, seed_urls: List[str]):
        self.seed_urls = seed_urls
        self.url_manager = URLManager()
        self.content_processor = ContentProcessor()
        self.feed_generator = FeedGenerator()
        self.db_manager = DatabaseManager()
        self.session: Optional[aiohttp.ClientSession] = None

        # Statistics
        self.crawled_count = 0
        self.total_pages = 0

    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT)

        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'WebCrawler/1.0 (+https://example.com/bot)'
            }
        )

        # Initialize seed URLs
        for url in self.seed_urls:
            await self.url_manager.add_url(url, priority=10)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()

    async def crawl(self) -> None:
        """Main crawling method"""
        logger.info(f"Starting crawl with {len(self.seed_urls)} seed URLs")

        while self.crawled_count < config.MAX_PAGES:
            url = await self.url_manager.get_next_url()
            if not url:
                logger.info("No more URLs to crawl")
                break

            try:
                await self._crawl_page(url)
                self.crawled_count += 1

                if self.crawled_count % 10 == 0:
                    logger.info(f"Crawled {self.crawled_count} pages")

            except Exception as e:
                logger.error(f"Error crawling {url}: {str(e)}")

            # Respect politeness delay
            await asyncio.sleep(config.DELAY_BETWEEN_REQUESTS)

    async def _crawl_page(self, url: str) -> None:
        """Crawl a single page and extract content"""

        # Check if already crawled
        if await self.url_manager.is_crawled(url):
            return

        try:
            # Fetch the page
            html_content = await self._fetch_page(url)
            if not html_content:
                return

            # Parse and extract content
            soup = BeautifulSoup(html_content, 'html.parser')

            # Extract useful content
            content_data = await self.content_processor.extract_content(
                url, soup, html_content
            )

            if content_data and self._is_useful_content(content_data):
                # Save to database
                await self.db_manager.save_content(content_data)

                # Update feed
                await self.feed_generator.add_content(content_data)

                logger.info(f"Extracted content from: {url}")

            # Extract and queue new URLs
            new_urls = await self._extract_urls(url, soup)
            for new_url in new_urls:
                await self.url_manager.add_url(new_url)

            # Mark as crawled
            await self.url_manager.mark_crawled(url)

        except Exception as e:
            logger.error(f"Error processing {url}: {str(e)}")

    async def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch HTML content from URL"""
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    content_type = response.headers.get('content-type', '')
                    if 'text/html' in content_type:
                        return await response.text()
                else:
                    logger.warning(f"HTTP {response.status} for {url}")

        except asyncio.TimeoutError:
            logger.warning(f"Timeout fetching {url}")
        except Exception as e:
            logger.error(f"Error fetching {url}: {str(e)}")

        return None

    async def _extract_urls(self, base_url: str, soup: BeautifulSoup) -> List[str]:
        """Extract and filter URLs from page"""
        urls = []

        for link in soup.find_all('a', href=True):
            href = link['href']
            full_url = urljoin(base_url, href)

            if self._is_valid_crawl_url(full_url):
                normalized_url = normalize_url(full_url)
                urls.append(normalized_url)

        return urls

    def _is_valid_crawl_url(self, url: str) -> bool:
        """Check if URL should be crawled"""
        if not is_valid_url(url):
            return False

        parsed = urlparse(url)

        # Check for excluded file extensions
        for ext in config.EXCLUDED_EXTENSIONS:
            if parsed.path.lower().endswith(ext):
                return False

        # Check for useful URL patterns
        for pattern in config.USEFUL_URL_PATTERNS:
            if re.search(pattern, url, re.IGNORECASE):
                return True

        return False

    def _is_useful_content(self, content_data: dict) -> bool:
        """Determine if extracted content is useful"""
        if not content_data.get('content'):
            return False

        content_length = len(content_data['content'])
        return content_length >= config.MIN_CONTENT_LENGTH
