import asyncio
import argparse
from crawler.core_crawler import WebCrawler
from search.search_engine import SearchEngine
from search.indexer import ContentIndexer
from crawler.feed_generator import FeedGenerator
from utils.database import DatabaseManager


async def main():
    parser = argparse.ArgumentParser(description='Web Crawler')
    parser.add_argument('command', choices=['crawl', 'search', 'feed', 'index', 'reindex', 'stats'])
    parser.add_argument('--urls', nargs='+', help='Seed URLs for crawling')
    parser.add_argument('--query', help='Search query')
    parser.add_argument('--format', choices=['rss', 'json'], default='rss')
    parser.add_argument('--output', help='Output file')
    parser.add_argument('--topic', help='Topic for topic-specific feed')
    parser.add_argument('--limit', type=int, default=20, help='Number of results')

    args = parser.parse_args()

    if args.command == 'crawl':
        if not args.urls:
            print("Please provide seed URLs with --urls")
            return

        async with WebCrawler(args.urls) as crawler:
            await crawler.crawl()

    elif args.command == 'search':
        if not args.query:
            print("Please provide search query with --query")
            return

        search_engine = SearchEngine()
        results = await search_engine.search(args.query, size=args.limit)

        print(f"Found {results['total']} results:")
        for result in results['results']:
            print(f"- {result['title']}")
            print(f"  {result['url']}")
            if result.get('highlights'):
                for field, highlights in result['highlights'].items():
                    print(f"  {field}: ...{highlights[0]}...")
            print()

    elif args.command == 'feed':
        feed_generator = FeedGenerator()

        if args.topic:
            if args.format == 'json':
                feed_content = await feed_generator.generate_topic_feed(args.topic, 'json')
            else:
                feed_content = await feed_generator.generate_topic_feed(args.topic, 'rss')
        else:
            if args.format == 'json':
                feed_content = await feed_generator.generate_json_feed()
            else:
                feed_content = await feed_generator.generate_rss_feed()

        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(feed_content)
            print(f"Feed saved to {args.output}")
        else:
            print(feed_content)

    elif args.command == 'index':
        async with ContentIndexer() as indexer:
            health = await indexer.health_check()
            print("Indexer Health Check:")
            print(f"  Elasticsearch: {'✓' if health['elasticsearch'] else '✗'}")
            print(f"  Database: {'✓' if health['database'] else '✗'}")
            print(f"  Overall: {'✓' if health['overall'] else '✗'}")

    elif args.command == 'reindex':
        async with ContentIndexer() as indexer:
            print("Starting full reindex...")
            count = await indexer.reindex_all()
            print(f"Reindexed {count} documents")

    elif args.command == 'stats':
        async with ContentIndexer() as indexer:
            stats = await indexer.get_index_stats()
            db_stats = await DatabaseManager().get_stats()

            print("System Statistics:")
            print(f"  Total Documents: {stats['total_documents']}")
            print(f"  Index Size: {stats['index_size']} bytes")
            print(f"  Elasticsearch Available: {'Yes' if stats['elasticsearch_available'] else 'No'}")
            print(f"  Content Added Today: {db_stats['content_today']}")
            print(f"  Top Tags: {', '.join([tag['tag'] for tag in db_stats['top_tags'][:5]])}")


if __name__ == "__main__":
    asyncio.run(main())
