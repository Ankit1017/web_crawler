import asyncio
import redis.asyncio as redis
from urllib.parse import urlparse
from typing import Optional, Set
import json
import hashlib

from config import config


class URLManager:
    """Manages URL queues and tracking"""

    def __init__(self):
        self.redis_client = None
        self.crawled_urls: Set[str] = set()

    async def _get_redis(self):
        """Get Redis connection"""
        if not self.redis_client:
            self.redis_client = redis.from_url(config.REDIS_URL)
        return self.redis_client

    async def add_url(self, url: str, priority: int = 1) -> None:
        """Add URL to crawling queue"""
        if await self.is_crawled(url):
            return

        redis_client = await self._get_redis()
        url_hash = self._hash_url(url)

        # Add to priority queue
        await redis_client.zadd(
            "crawl_queue",
            {url: priority}
        )

        # Store URL details
        url_data = {
            'url': url,
            'domain': urlparse(url).netloc,
            'added_timestamp': asyncio.get_event_loop().time()
        }

        await redis_client.hset(
            f"url_data:{url_hash}",
            mapping=url_data
        )

    async def get_next_url(self) -> Optional[str]:
        """Get next URL to crawl from queue"""
        redis_client = await self._get_redis()

        # Get highest priority URL
        result = await redis_client.zpopmax("crawl_queue")

        if result:
            url, priority = result[0]
            return url.decode() if isinstance(url, bytes) else url

        return None

    async def mark_crawled(self, url: str) -> None:
        """Mark URL as crawled"""
        redis_client = await self._get_redis()
        url_hash = self._hash_url(url)

        await redis_client.sadd("crawled_urls", url_hash)
        self.crawled_urls.add(url_hash)

    async def is_crawled(self, url: str) -> bool:
        """Check if URL has been crawled"""
        url_hash = self._hash_url(url)

        if url_hash in self.crawled_urls:
            return True

        redis_client = await self._get_redis()
        result = await redis_client.sismember("crawled_urls", url_hash)

        if result:
            self.crawled_urls.add(url_hash)

        return bool(result)

    def _hash_url(self, url: str) -> str:
        """Generate hash for URL"""
        return hashlib.md5(url.encode()).hexdigest()
