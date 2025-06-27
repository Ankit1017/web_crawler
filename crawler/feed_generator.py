import json
from datetime import datetime
from typing import Dict, List
import xml.etree.ElementTree as ET
from xml.dom import minidom

from config import config
from utils.database import DatabaseManager


class FeedGenerator:
    """Generates RSS and JSON feeds from crawled content"""

    def __init__(self):
        self.db_manager = DatabaseManager()

    async def add_content(self, content_data: Dict) -> None:
        """Add content to feed"""
        # Content is automatically added to feed when saved to database
        pass

    async def generate_rss_feed(self, max_items: int = None) -> str:
        """Generate RSS 2.0 feed"""
        if max_items is None:
            max_items = config.MAX_FEED_ITEMS

        # Get recent content
        content_items = await self.db_manager.get_recent_content(max_items)

        # Create RSS XML
        rss = ET.Element('rss', version='2.0')
        channel = ET.SubElement(rss, 'channel')

        # Channel metadata
        ET.SubElement(channel, 'title').text = config.FEED_TITLE
        ET.SubElement(channel, 'description').text = config.FEED_DESCRIPTION
        ET.SubElement(channel, 'link').text = 'https://example.com'
        ET.SubElement(channel, 'lastBuildDate').text = datetime.utcnow().strftime(
            '%a, %d %b %Y %H:%M:%S GMT'
        )

        # Add items
        for content in content_items:
            item = ET.SubElement(channel, 'item')

            ET.SubElement(item, 'title').text = content.get('title', 'Untitled')
            ET.SubElement(item, 'description').text = content.get('description', '')
            ET.SubElement(item, 'link').text = content['url']
            ET.SubElement(item, 'guid').text = content['url']

            if content.get('publish_date'):
                ET.SubElement(item, 'pubDate').text = self._format_rss_date(
                    content['publish_date']
                )

            if content.get('author'):
                ET.SubElement(item, 'author').text = content['author']

            # Add categories (tags)
            for tag in content.get('tags', []):
                ET.SubElement(item, 'category').text = tag

        # Format XML
        rough_string = ET.tostring(rss, 'unicode')
        reparsed = minidom.parseString(rough_string)
        return reparsed.toprettyxml(indent="  ")

    async def generate_json_feed(self, max_items: int = None) -> str:
        """Generate JSON Feed 1.1"""
        if max_items is None:
            max_items = config.MAX_FEED_ITEMS

        content_items = await self.db_manager.get_recent_content(max_items)

        feed = {
            "version": "https://jsonfeed.org/version/1.1",
            "title": config.FEED_TITLE,
            "description": config.FEED_DESCRIPTION,
            "home_page_url": "https://example.com",
            "feed_url": "https://example.com/feed.json",
            "items": []
        }

        for content in content_items:
            item = {
                "id": content['url'],
                "url": content['url'],
                "title": content.get('title', 'Untitled'),
                "content_text": content.get('content', ''),
                "summary": content.get('description', ''),
                "date_published": content.get('publish_date'),
                "tags": content.get('tags', [])
            }

            if content.get('author'):
                item['authors'] = [{"name": content['author']}]

            feed['items'].append(item)

        return json.dumps(feed, indent=2, ensure_ascii=False)

    async def generate_topic_feed(self, topic: str, format_type: str = 'rss') -> str:
        """Generate topic-specific feed"""
        content_items = await self.db_manager.get_content_by_topic(
            topic, config.MAX_FEED_ITEMS
        )

        if format_type == 'json':
            return await self._generate_topic_json_feed(topic, content_items)
        else:
            return await self._generate_topic_rss_feed(topic, content_items)

    def _format_rss_date(self, date_str: str) -> str:
        """Format date for RSS"""
        try:
            from dateutil import parser
            dt = parser.parse(date_str)
            return dt.strftime('%a, %d %b %Y %H:%M:%S GMT')
        except:
            return datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
