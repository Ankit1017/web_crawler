from bs4 import BeautifulSoup, Comment
import re
from datetime import datetime
from typing import Dict, Optional, List
import nltk
from textstat import flesch_reading_ease

# Download required NLTK data
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')


class ContentProcessor:
    """Processes and extracts useful content from web pages"""

    def __init__(self):
        self.content_selectors = [
            'article', '[role="main"]', '.content', '#content',
            '.post-content', '.entry-content', '.article-body'
        ]

        self.title_selectors = [
            'h1', '.title', '.post-title', '.article-title',
            '.entry-title', '[property="og:title"]'
        ]

    async def extract_content(self, url: str, soup: BeautifulSoup,
                              html_content: str) -> Optional[Dict]:
        """Extract structured content from HTML"""

        # Clean HTML
        self._clean_html(soup)

        # Extract main content
        content = self._extract_main_content(soup)
        if not content:
            return None

        # Extract metadata
        title = self._extract_title(soup)
        description = self._extract_description(soup)
        author = self._extract_author(soup)
        publish_date = self._extract_publish_date(soup)
        tags = self._extract_tags(soup)

        # Content analysis
        word_count = len(content.split())
        reading_time = max(1, word_count // 200)  # Average reading speed
        readability_score = flesch_reading_ease(content)

        return {
            'url': url,
            'title': title,
            'description': description,
            'content': content,
            'author': author,
            'publish_date': publish_date,
            'tags': tags,
            'word_count': word_count,
            'reading_time': reading_time,
            'readability_score': readability_score,
            'extracted_at': datetime.utcnow().isoformat(),
            'content_hash': self._hash_content(content)
        }

    def _clean_html(self, soup: BeautifulSoup) -> None:
        """Remove unwanted elements from HTML"""

        # Remove scripts, styles, and comments
        for element in soup(['script', 'style', 'nav', 'header', 'footer']):
            element.decompose()

        # Remove comments
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()

        # Remove ads and social media widgets
        ad_classes = ['ad', 'advertisement', 'social-share', 'related-posts']
        for class_name in ad_classes:
            for element in soup.find_all(class_=re.compile(class_name, re.I)):
                element.decompose()

    def _extract_main_content(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract main content using multiple strategies"""

        # Strategy 1: Use content selectors
        for selector in self.content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                content = content_elem.get_text(strip=True)
                if len(content) > 200:  # Minimum content length
                    return content

        # Strategy 2: Find largest text block
        all_paragraphs = soup.find_all('p')
        if all_paragraphs:
            content_blocks = []
            for p in all_paragraphs:
                text = p.get_text(strip=True)
                if len(text) > 50:  # Filter out short paragraphs
                    content_blocks.append(text)

            if content_blocks:
                return ' '.join(content_blocks)

        # Strategy 3: Fallback to body text
        body = soup.find('body')
        if body:
            content = body.get_text(strip=True)
            if len(content) > 200:
                return content

        return None

    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract page title"""

        # Try specific selectors first
        for selector in self.title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                title = title_elem.get_text(strip=True)
                if title:
                    return title

        # Fallback to HTML title
        title_tag = soup.find('title')
        if title_tag:
            return title_tag.get_text(strip=True)

        return None

    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract page description"""

        # Try meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            return meta_desc['content'].strip()

        # Try Open Graph description
        og_desc = soup.find('meta', attrs={'property': 'og:description'})
        if og_desc and og_desc.get('content'):
            return og_desc['content'].strip()

        return None

    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract author information"""

        # Try various author selectors
        author_selectors = [
            '[rel="author"]', '.author', '.byline',
            '[property="article:author"]', '.post-author'
        ]

        for selector in author_selectors:
            author_elem = soup.select_one(selector)
            if author_elem:
                author = author_elem.get_text(strip=True)
                if author:
                    return author

        return None

    def _extract_publish_date(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract publish date"""

        # Try various date selectors
        date_selectors = [
            '[property="article:published_time"]',
            '[datetime]', '.date', '.publish-date',
            'time'
        ]

        for selector in date_selectors:
            date_elem = soup.select_one(selector)
            if date_elem:
                # Try datetime attribute first
                date_str = date_elem.get('datetime') or date_elem.get('content')
                if not date_str:
                    date_str = date_elem.get_text(strip=True)

                if date_str:
                    return self._parse_date(date_str)

        return None

    def _extract_tags(self, soup: BeautifulSoup) -> List[str]:
        """Extract tags/categories"""
        tags = []

        # Try various tag selectors
        tag_selectors = [
            '.tags a', '.categories a', '.tag',
            '[property="article:tag"]'
        ]

        for selector in tag_selectors:
            tag_elems = soup.select(selector)
            for elem in tag_elems:
                tag_text = elem.get_text(strip=True)
                if tag_text and tag_text not in tags:
                    tags.append(tag_text)

        return tags[:10]  # Limit to 10 tags

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to ISO format"""
        try:
            # Try common date formats
            from dateutil import parser
            parsed_date = parser.parse(date_str)
            return parsed_date.isoformat()
        except:
            return None

    def _hash_content(self, content: str) -> str:
        """Generate hash for content deduplication"""
        import hashlib
        return hashlib.md5(content.encode()).hexdigest()
