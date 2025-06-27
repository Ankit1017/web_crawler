import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from elasticsearch import AsyncElasticsearch, exceptions
import hashlib

from config import config
from utils.database import DatabaseManager
from utils.helpers import extract_keywords, clean_text

logger = logging.getLogger(__name__)


class ContentIndexer:
    """Handles content indexing for search functionality"""

    def __init__(self):
        self.es_client: Optional[AsyncElasticsearch] = None
        self.db_manager = DatabaseManager()
        self.index_name = 'web_content'

    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()

    async def connect(self):
        """Connect to Elasticsearch"""
        try:
            self.es_client = AsyncElasticsearch(
                [config.ELASTICSEARCH_URL],
                timeout=30,
                max_retries=3,
                retry_on_timeout=True
            )

            # Test connection
            await self.es_client.info()
            logger.info("Connected to Elasticsearch")

            # Create index if it doesn't exist
            await self.create_index()

        except Exception as e:
            logger.error(f"Failed to connect to Elasticsearch: {str(e)}")
            # Fallback to database-only search
            self.es_client = None

    async def close(self):
        """Close Elasticsearch connection"""
        if self.es_client:
            await self.es_client.close()
            self.es_client = None

    async def create_index(self):
        """Create Elasticsearch index with proper mapping"""
        if not self.es_client:
            return

        mapping = {
            "mappings": {
                "properties": {
                    "url": {
                        "type": "keyword"
                    },
                    "title": {
                        "type": "text",
                        "analyzer": "standard",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "content": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "description": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "author": {
                        "type": "keyword"
                    },
                    "tags": {
                        "type": "keyword"
                    },
                    "keywords": {
                        "type": "keyword"
                    },
                    "publish_date": {
                        "type": "date",
                        "format": "strict_date_optional_time||epoch_millis"
                    },
                    "indexed_at": {
                        "type": "date",
                        "format": "strict_date_optional_time||epoch_millis"
                    },
                    "word_count": {
                        "type": "integer"
                    },
                    "reading_time": {
                        "type": "integer"
                    },
                    "readability_score": {
                        "type": "float"
                    },
                    "domain": {
                        "type": "keyword"
                    },
                    "content_hash": {
                        "type": "keyword"
                    }
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "content_analyzer": {
                            "type": "standard",
                            "stopwords": "_english_"
                        }
                    }
                }
            }
        }

        try:
            # Check if index exists
            exists = await self.es_client.indices.exists(index=self.index_name)

            if not exists:
                await self.es_client.indices.create(
                    index=self.index_name,
                    body=mapping
                )
                logger.info(f"Created Elasticsearch index: {self.index_name}")
            else:
                logger.info(f"Elasticsearch index already exists: {self.index_name}")

        except Exception as e:
            logger.error(f"Error creating Elasticsearch index: {str(e)}")

    async def index_content(self, content_data: Dict) -> bool:
        """Index content for search"""
        try:
            # Prepare document for indexing
            doc = await self._prepare_document(content_data)

            # Index to Elasticsearch if available
            if self.es_client:
                await self._index_to_elasticsearch(doc)

            # Always save to database as fallback
            await self.db_manager.save_content(content_data)

            logger.info(f"Indexed content: {content_data['url']}")
            return True

        except Exception as e:
            logger.error(f"Error indexing content {content_data['url']}: {str(e)}")
            return False

    async def _prepare_document(self, content_data: Dict) -> Dict:
        """Prepare document for indexing"""
        from urllib.parse import urlparse

        # Extract domain
        domain = urlparse(content_data['url']).netloc

        # Extract keywords from content
        content_text = content_data.get('content', '')
        title_text = content_data.get('title', '')
        combined_text = f"{title_text} {content_text}"

        keywords = extract_keywords(combined_text, max_keywords=20)

        # Clean and prepare content
        cleaned_content = clean_text(content_text)
        cleaned_title = clean_text(title_text)

        doc = {
            'url': content_data['url'],
            'title': cleaned_title,
            'content': cleaned_content,
            'description': content_data.get('description', ''),
            'author': content_data.get('author'),
            'tags': content_data.get('tags', []),
            'keywords': keywords,
            'publish_date': content_data.get('publish_date'),
            'indexed_at': datetime.utcnow().isoformat(),
            'word_count': content_data.get('word_count', 0),
            'reading_time': content_data.get('reading_time', 0),
            'readability_score': content_data.get('readability_score'),
            'domain': domain,
            'content_hash': content_data.get('content_hash')
        }

        return doc

    async def _index_to_elasticsearch(self, doc: Dict) -> None:
        """Index document to Elasticsearch"""
        if not self.es_client:
            return

        try:
            # Use content hash as document ID to prevent duplicates
            doc_id = doc['content_hash']

            await self.es_client.index(
                index=self.index_name,
                id=doc_id,
                body=doc
            )

        except exceptions.RequestError as e:
            logger.error(f"Elasticsearch indexing error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error during Elasticsearch indexing: {str(e)}")

    async def bulk_index(self, content_list: List[Dict]) -> int:
        """Bulk index multiple content items"""
        if not content_list:
            return 0

        indexed_count = 0

        if self.es_client:
            try:
                # Prepare bulk operations
                bulk_body = []

                for content_data in content_list:
                    doc = await self._prepare_document(content_data)
                    doc_id = doc['content_hash']

                    # Add index operation
                    bulk_body.append({
                        "index": {
                            "_index": self.index_name,
                            "_id": doc_id
                        }
                    })
                    bulk_body.append(doc)

                # Execute bulk operation
                if bulk_body:
                    response = await self.es_client.bulk(body=bulk_body)

                    # Count successful operations
                    for item in response['items']:
                        if 'index' in item and item['index']['status'] in [200, 201]:
                            indexed_count += 1
                        elif 'index' in item:
                            logger.warning(f"Failed to index document: {item['index']}")

                    logger.info(f"Bulk indexed {indexed_count}/{len(content_list)} documents")

            except Exception as e:
                logger.error(f"Bulk indexing error: {str(e)}")

        # Fallback: index to database
        for content_data in content_list:
            try:
                await self.db_manager.save_content(content_data)
            except Exception as e:
                logger.error(f"Error saving to database: {str(e)}")

        return indexed_count

    async def reindex_all(self) -> int:
        """Reindex all content from database"""
        logger.info("Starting full reindex...")

        try:
            # Get all content from database
            content_list = await self.db_manager.get_recent_content(limit=10000)  # Adjust limit as needed

            if not content_list:
                logger.info("No content found to reindex")
                return 0

            # Clear existing index
            if self.es_client:
                try:
                    await self.es_client.indices.delete(index=self.index_name)
                    await self.create_index()
                    logger.info("Recreated Elasticsearch index")
                except exceptions.NotFoundError:
                    await self.create_index()

            # Bulk index content
            indexed_count = await self.bulk_index(content_list)

            logger.info(f"Reindexing completed: {indexed_count} documents indexed")
            return indexed_count

        except Exception as e:
            logger.error(f"Error during reindexing: {str(e)}")
            return 0

    async def delete_content(self, content_hash: str) -> bool:
        """Delete content from index"""
        try:
            if self.es_client:
                await self.es_client.delete(
                    index=self.index_name,
                    id=content_hash,
                    ignore=[404]  # Ignore if document doesn't exist
                )

            return True

        except Exception as e:
            logger.error(f"Error deleting content {content_hash}: {str(e)}")
            return False

    async def get_index_stats(self) -> Dict:
        """Get indexing statistics"""
        stats = {
            'total_documents': 0,
            'index_size': 0,
            'elasticsearch_available': bool(self.es_client)
        }

        try:
            if self.es_client:
                # Get Elasticsearch stats
                es_stats = await self.es_client.indices.stats(index=self.index_name)

                if self.index_name in es_stats['indices']:
                    index_stats = es_stats['indices'][self.index_name]
                    stats['total_documents'] = index_stats['total']['docs']['count']
                    stats['index_size'] = index_stats['total']['store']['size_in_bytes']

            # Get database stats as fallback
            db_stats = await self.db_manager.get_stats()
            if not stats['total_documents']:
                stats['total_documents'] = db_stats.get('total_content', 0)

        except Exception as e:
            logger.error(f"Error getting index stats: {str(e)}")

        return stats

    async def health_check(self) -> Dict:
        """Check health of indexing system"""
        health = {
            'elasticsearch': False,
            'database': False,
            'overall': False
        }

        # Check Elasticsearch
        if self.es_client:
            try:
                await self.es_client.cluster.health()
                health['elasticsearch'] = True
            except Exception as e:
                logger.error(f"Elasticsearch health check failed: {str(e)}")

        # Check database
        try:
            await self.db_manager.get_stats()
            health['database'] = True
        except Exception as e:
            logger.error(f"Database health check failed: {str(e)}")

        # Overall health
        health['overall'] = health['database']  # At minimum, database should work

        return health
