import asyncio
import sqlite3
import aiosqlite
import json
from datetime import datetime
from typing import Dict, List, Optional
import logging

from config import config

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Handles database operations for crawled content"""

    def __init__(self):
        self.db_path = config.DATABASE_URL.replace('sqlite:///', '') if config.DATABASE_URL.startswith(
            'sqlite://') else 'webcrawler.db'
        self.initialized = False

    async def init_db(self):
        """Initialize database tables"""
        if self.initialized:
            return

        async with aiosqlite.connect(self.db_path) as db:
            # Create content table
            await db.execute('''
                CREATE TABLE IF NOT EXISTS content (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    title TEXT,
                    description TEXT,
                    content TEXT,
                    author TEXT,
                    publish_date TEXT,
                    tags TEXT,
                    word_count INTEGER,
                    reading_time INTEGER,
                    readability_score REAL,
                    extracted_at TEXT,
                    content_hash TEXT UNIQUE
                )
            ''')

            # Create indexes for better performance
            await db.execute('CREATE INDEX IF NOT EXISTS idx_url ON content(url)')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_content_hash ON content(content_hash)')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_extracted_at ON content(extracted_at)')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_tags ON content(tags)')

            await db.commit()

        self.initialized = True
        logger.info("Database initialized successfully")

    async def save_content(self, content_data: Dict) -> bool:
        """Save content to database"""
        await self.init_db()

        try:
            async with aiosqlite.connect(self.db_path) as db:
                # Check if content already exists
                cursor = await db.execute(
                    'SELECT id FROM content WHERE content_hash = ?',
                    (content_data.get('content_hash'),)
                )
                existing = await cursor.fetchone()

                if existing:
                    logger.debug(f"Content already exists for URL: {content_data['url']}")
                    return False

                # Convert tags list to JSON string
                tags_json = json.dumps(content_data.get('tags', []))

                # Insert new content
                await db.execute('''
                    INSERT INTO content (
                        url, title, description, content, author, publish_date,
                        tags, word_count, reading_time, readability_score,
                        extracted_at, content_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    content_data['url'],
                    content_data.get('title'),
                    content_data.get('description'),
                    content_data.get('content'),
                    content_data.get('author'),
                    content_data.get('publish_date'),
                    tags_json,
                    content_data.get('word_count'),
                    content_data.get('reading_time'),
                    content_data.get('readability_score'),
                    content_data.get('extracted_at'),
                    content_data.get('content_hash')
                ))

                await db.commit()
                logger.info(f"Saved content: {content_data['url']}")
                return True

        except Exception as e:
            logger.error(f"Error saving content {content_data['url']}: {str(e)}")
            return False

    async def get_recent_content(self, limit: int = 50) -> List[Dict]:
        """Get recent content from database"""
        await self.init_db()

        try:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute('''
                    SELECT url, title, description, content, author, publish_date,
                           tags, word_count, reading_time, readability_score, extracted_at
                    FROM content 
                    ORDER BY extracted_at DESC 
                    LIMIT ?
                ''', (limit,))

                rows = await cursor.fetchall()
                content_list = []

                for row in rows:
                    try:
                        tags = json.loads(row[6]) if row[6] else []
                    except json.JSONDecodeError:
                        tags = []

                    content_list.append({
                        'url': row[0],
                        'title': row[1],
                        'description': row[2],
                        'content': row[3],
                        'author': row[4],
                        'publish_date': row[5],
                        'tags': tags,
                        'word_count': row[7],
                        'reading_time': row[8],
                        'readability_score': row[9],
                        'extracted_at': row[10]
                    })

                return content_list

        except Exception as e:
            logger.error(f"Error retrieving recent content: {str(e)}")
            return []

    async def get_content_by_topic(self, topic: str, limit: int = 50) -> List[Dict]:
        """Get content filtered by topic/tag"""
        await self.init_db()

        try:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute('''
                    SELECT url, title, description, content, author, publish_date,
                           tags, word_count, reading_time, readability_score, extracted_at
                    FROM content 
                    WHERE tags LIKE ? OR title LIKE ? OR content LIKE ?
                    ORDER BY extracted_at DESC 
                    LIMIT ?
                ''', (f'%{topic}%', f'%{topic}%', f'%{topic}%', limit))

                rows = await cursor.fetchall()
                content_list = []

                for row in rows:
                    try:
                        tags = json.loads(row[6]) if row[6] else []
                    except json.JSONDecodeError:
                        tags = []

                    content_list.append({
                        'url': row[0],
                        'title': row[1],
                        'description': row[2],
                        'content': row[3],
                        'author': row[4],
                        'publish_date': row[5],
                        'tags': tags,
                        'word_count': row[7],
                        'reading_time': row[8],
                        'readability_score': row[9],
                        'extracted_at': row[10]
                    })

                return content_list

        except Exception as e:
            logger.error(f"Error retrieving content by topic {topic}: {str(e)}")
            return []

    async def search_content(self, query: str, limit: int = 20) -> List[Dict]:
        """Simple text search in content"""
        await self.init_db()

        try:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute('''
                    SELECT url, title, description, content, author, publish_date,
                           tags, word_count, reading_time, readability_score, extracted_at
                    FROM content 
                    WHERE title LIKE ? OR description LIKE ? OR content LIKE ?
                    ORDER BY 
                        CASE 
                            WHEN title LIKE ? THEN 1
                            WHEN description LIKE ? THEN 2
                            ELSE 3
                        END,
                        extracted_at DESC
                    LIMIT ?
                ''', (f'%{query}%', f'%{query}%', f'%{query}%',
                      f'%{query}%', f'%{query}%', limit))

                rows = await cursor.fetchall()
                content_list = []

                for row in rows:
                    try:
                        tags = json.loads(row[6]) if row[6] else []
                    except json.JSONDecodeError:
                        tags = []

                    content_list.append({
                        'url': row[0],
                        'title': row[1],
                        'description': row[2],
                        'content': row[3],
                        'author': row[4],
                        'publish_date': row[5],
                        'tags': tags,
                        'word_count': row[7],
                        'reading_time': row[8],
                        'readability_score': row[9],
                        'extracted_at': row[10]
                    })

                return content_list

        except Exception as e:
            logger.error(f"Error searching content: {str(e)}")
            return []

    async def get_stats(self) -> Dict:
        """Get database statistics"""
        await self.init_db()

        try:
            async with aiosqlite.connect(self.db_path) as db:
                # Total content count
                cursor = await db.execute('SELECT COUNT(*) FROM content')
                total_count = (await cursor.fetchone())[0]

                # Content added today
                today = datetime.utcnow().date().isoformat()
                cursor = await db.execute(
                    'SELECT COUNT(*) FROM content WHERE DATE(extracted_at) = ?',
                    (today,)
                )
                today_count = (await cursor.fetchone())[0]

                # Top tags
                cursor = await db.execute('''
                    SELECT tags, COUNT(*) as count 
                    FROM content 
                    WHERE tags != '[]' AND tags IS NOT NULL
                    GROUP BY tags 
                    ORDER BY count DESC 
                    LIMIT 10
                ''')
                tag_rows = await cursor.fetchall()

                top_tags = []
                for tag_row in tag_rows:
                    try:
                        tags = json.loads(tag_row[0])
                        for tag in tags:
                            top_tags.append({'tag': tag, 'count': tag_row[1]})
                    except json.JSONDecodeError:
                        continue

                return {
                    'total_content': total_count,
                    'content_today': today_count,
                    'top_tags': top_tags[:10]
                }

        except Exception as e:
            logger.error(f"Error getting stats: {str(e)}")
            return {
                'total_content': 0,
                'content_today': 0,
                'top_tags': []
            }
