import re
import hashlib
import time
from urllib.parse import urlparse, urlunparse, urljoin
from typing import Optional, List, Set
import tldextract
import logging

logger = logging.getLogger(__name__)


def is_valid_url(url: str) -> bool:
    """Validate URL format and check if it's accessible"""
    if not url or not isinstance(url, str):
        return False

    # Basic URL pattern validation
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?'  # domain
        r'|localhost|'  # localhost
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # IP
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    if not url_pattern.match(url):
        return False

    # Additional checks
    parsed = urlparse(url)

    # Check for valid scheme
    if parsed.scheme not in ['http', 'https']:
        return False

    # Check for valid netloc
    if not parsed.netloc:
        return False

    return True


def normalize_url(url: str) -> str:
    """Normalize URL by removing fragments and unnecessary parameters"""
    try:
        parsed = urlparse(url)

        # Remove fragment and common tracking parameters
        query_params = []
        if parsed.query:
            params = parsed.query.split('&')
            for param in params:
                if '=' in param:
                    key, value = param.split('=', 1)
                    # Skip common tracking parameters
                    if key.lower() not in ['utm_source', 'utm_medium', 'utm_campaign',
                                           'utm_term', 'utm_content', 'ref', 'source']:
                        query_params.append(param)

        # Reconstruct URL
        normalized = parsed._replace(
            fragment='',
            query='&'.join(query_params) if query_params else ''
        )

        return urlunparse(normalized).rstrip('/')

    except Exception as e:
        logger.warning(f"Error normalizing URL {url}: {str(e)}")
        return url


def extract_domain(url: str) -> Optional[str]:
    """Extract domain from URL"""
    try:
        parsed = urlparse(url)
        return parsed.netloc.lower()
    except Exception:
        return None


def get_domain_info(url: str) -> dict:
    """Get detailed domain information"""
    try:
        extracted = tldextract.extract(url)
        return {
            'domain': extracted.domain,
            'subdomain': extracted.subdomain,
            'suffix': extracted.suffix,
            'registered_domain': extracted.registered_domain
        }
    except Exception:
        parsed = urlparse(url)
        return {
            'domain': parsed.netloc,
            'subdomain': '',
            'suffix': '',
            'registered_domain': parsed.netloc
        }


def is_same_domain(url1: str, url2: str) -> bool:
    """Check if two URLs belong to the same domain"""
    domain1 = extract_domain(url1)
    domain2 = extract_domain(url2)
    return domain1 == domain2 if domain1 and domain2 else False


def should_crawl_url(url: str, seed_domains: Set[str] = None,
                     allowed_patterns: List[str] = None,
                     blocked_patterns: List[str] = None) -> bool:
    """Determine if URL should be crawled based on rules"""

    if not is_valid_url(url):
        return False

    # Check domain restrictions
    if seed_domains:
        domain = extract_domain(url)
        if domain not in seed_domains:
            return False

    # Check blocked patterns
    if blocked_patterns:
        for pattern in blocked_patterns:
            if re.search(pattern, url, re.IGNORECASE):
                return False

    # Check allowed patterns
    if allowed_patterns:
        for pattern in allowed_patterns:
            if re.search(pattern, url, re.IGNORECASE):
                return True
        return False  # If patterns specified but none match

    return True


def clean_text(text: str) -> str:
    """Clean and normalize text content"""
    if not text:
        return ""

    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)

    # Remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s.,!?;:()\-"]', '', text)

    return text.strip()


def extract_keywords(text: str, max_keywords: int = 10) -> List[str]:
    """Extract keywords from text using simple frequency analysis"""
    if not text:
        return []

    # Simple tokenization and filtering
    words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())

    # Common stop words to filter out
    stop_words = {
        'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with',
        'by', 'from', 'up', 'about', 'into', 'through', 'during', 'before',
        'after', 'above', 'below', 'between', 'among', 'this', 'that', 'these',
        'those', 'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves',
        'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his',
        'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself',
        'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which',
        'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are',
        'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having',
        'do', 'does', 'did', 'doing', 'a', 'an', 'will', 'would', 'should',
        'could', 'can', 'may', 'might', 'must', 'shall'
    }

    # Filter words and count frequency
    word_freq = {}
    for word in words:
        if word not in stop_words and len(word) > 3:
            word_freq[word] = word_freq.get(word, 0) + 1

    # Sort by frequency and return top keywords
    sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
    return [word for word, freq in sorted_words[:max_keywords]]


def generate_content_hash(content: str) -> str:
    """Generate unique hash for content deduplication"""
    if not content:
        return ""

    # Normalize content for hashing
    normalized = clean_text(content).lower()
    return hashlib.md5(normalized.encode('utf-8')).hexdigest()


def estimate_reading_time(text: str, words_per_minute: int = 200) -> int:
    """Estimate reading time in minutes"""
    if not text:
        return 0

    word_count = len(text.split())
    reading_time = max(1, round(word_count / words_per_minute))
    return reading_time


def truncate_text(text: str, max_length: int = 200, suffix: str = "...") -> str:
    """Truncate text to specified length"""
    if not text or len(text) <= max_length:
        return text

    # Try to truncate at word boundary
    truncated = text[:max_length]
    last_space = truncated.rfind(' ')

    if last_space > 0:
        truncated = truncated[:last_space]

    return truncated + suffix


def is_content_duplicate(content1: str, content2: str, threshold: float = 0.8) -> bool:
    """Check if two content pieces are duplicates using simple similarity"""
    if not content1 or not content2:
        return False

    # Simple similarity check based on common words
    words1 = set(clean_text(content1).lower().split())
    words2 = set(clean_text(content2).lower().split())

    if len(words1) == 0 or len(words2) == 0:
        return False

    intersection = len(words1 & words2)
    union = len(words1 | words2)

    similarity = intersection / union if union > 0 else 0
    return similarity >= threshold


def rate_limit_delay(domain: str, default_delay: float = 1.0) -> float:
    """Calculate appropriate delay for domain based on politeness rules"""

    # Domain-specific delays (can be configured)
    domain_delays = {
        'wikipedia.org': 0.5,
        'github.com': 1.0,
        'stackoverflow.com': 2.0,
        'reddit.com': 3.0,
        'twitter.com': 5.0,
        'facebook.com': 5.0
    }

    return domain_delays.get(domain, default_delay)


def create_robots_txt_url(base_url: str) -> str:
    """Create robots.txt URL for given base URL"""
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    return robots_url


def parse_robots_txt(robots_content: str, user_agent: str = '*') -> dict:
    """Parse robots.txt content and extract rules"""
    rules = {
        'allowed': [],
        'disallowed': [],
        'crawl_delay': None,
        'sitemap': []
    }

    if not robots_content:
        return rules

    current_user_agent = None
    lines = robots_content.split('\n')

    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue

        if line.lower().startswith('user-agent:'):
            current_user_agent = line.split(':', 1)[1].strip()
        elif current_user_agent in [user_agent, '*']:
            if line.lower().startswith('disallow:'):
                path = line.split(':', 1)[1].strip()
                if path:
                    rules['disallowed'].append(path)
            elif line.lower().startswith('allow:'):
                path = line.split(':', 1)[1].strip()
                if path:
                    rules['allowed'].append(path)
            elif line.lower().startswith('crawl-delay:'):
                try:
                    delay = float(line.split(':', 1)[1].strip())
                    rules['crawl_delay'] = delay
                except ValueError:
                    pass
        elif line.lower().startswith('sitemap:'):
            sitemap_url = line.split(':', 1)[1].strip()
            if sitemap_url:
                rules['sitemap'].append(sitemap_url)

    return rules
