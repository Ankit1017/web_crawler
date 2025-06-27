from typing import List, Dict, Optional
import re
from elasticsearch import AsyncElasticsearch
from datetime import datetime

from config import config
from utils.database import DatabaseManager


class SearchEngine:
    """Handles search functionality for crawled content"""

    def __init__(self):
        self.es_client = None
        self.db_manager = DatabaseManager()

    async def _get_elasticsearch(self):
        """Get Elasticsearch connection"""
        if not self.es_client:
            self.es_client = AsyncElasticsearch([config.ELASTICSEARCH_URL])
        return self.es_client

    async def index_content(self, content_data: Dict) -> None:
        """Index content for search"""
        es = await self._get_elasticsearch()

        doc = {
            'url': content_data['url'],
            'title': content_data.get('title', ''),
            'content': content_data.get('content', ''),
            'description': content_data.get('description', ''),
            'author': content_data.get('author', ''),
            'tags': content_data.get('tags', []),
            'publish_date': content_data.get('publish_date'),
            'word_count': content_data.get('word_count', 0),
            'indexed_at': datetime.utcnow().isoformat()
        }

        await es.index(
            index='web_content',
            id=content_data['content_hash'],
            body=doc
        )

    async def search(self, query: str, filters: Dict = None,
                     size: int = 20, offset: int = 0) -> Dict:
        """Search content with filters"""
        es = await self._get_elasticsearch()

        # Build Elasticsearch query
        search_body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": ["title^3", "content^2", "description"],
                                "type": "best_fields"
                            }
                        }
                    ],
                    "filter": []
                }
            },
            "highlight": {
                "fields": {
                    "title": {},
                    "content": {"fragment_size": 150, "number_of_fragments": 3}
                }
            },
            "sort": [
                {"_score": {"order": "desc"}},
                {"publish_date": {"order": "desc"}}
            ]
        }

        # Apply filters
        if filters:
            if filters.get('author'):
                search_body["query"]["bool"]["filter"].append({
                    "term": {"author.keyword": filters['author']}
                })

            if filters.get('tags'):
                search_body["query"]["bool"]["filter"].append({
                    "terms": {"tags.keyword": filters['tags']}
                })

            if filters.get('date_from') or filters.get('date_to'):
                date_range = {}
                if filters.get('date_from'):
                    date_range['gte'] = filters['date_from']
                if filters.get('date_to'):
                    date_range['lte'] = filters['date_to']

                search_body["query"]["bool"]["filter"].append({
                    "range": {"publish_date": date_range}
                })

        # Execute search
        result = await es.search(
            index='web_content',
            body=search_body,
            size=size,
            from_=offset
        )

        # Format results
        hits = result['hits']
        search_results = {
            'total': hits['total']['value'],
            'results': [],
            'took': result['took']
        }

        for hit in hits['hits']:
            source = hit['_source']
            result_item = {
                'url': source['url'],
                'title': source['title'],
                'description': source['description'],
                'author': source.get('author'),
                'tags': source.get('tags', []),
                'publish_date': source.get('publish_date'),
                'score': hit['_score'],
                'highlights': hit.get('highlight', {})
            }
            search_results['results'].append(result_item)

        return search_results

    async def get_trending_topics(self, days: int = 7) -> List[Dict]:
        """Get trending topics based on recent content"""
        es = await self._get_elasticsearch()

        search_body = {
            "query": {
                "range": {
                    "indexed_at": {
                        "gte": f"now-{days}d"
                    }
                }
            },
            "aggs": {
                "trending_tags": {
                    "terms": {
                        "field": "tags.keyword",
                        "size": 20
                    }
                }
            },
            "size": 0
        }

        result = await es.search(index='web_content', body=search_body)

        trending = []
        for bucket in result['aggregations']['trending_tags']['buckets']:
            trending.append({
                'topic': bucket['key'],
                'count': bucket['doc_count']
            })

        return trending
