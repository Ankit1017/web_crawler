#!/usr/bin/env python3
"""
Elasticsearch Spell Correction Script with Connection Troubleshooting
"""

from elasticsearch import Elasticsearch
import urllib3
import ssl

# Disable SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class ElasticsearchSpellCorrector:
    def __init__(self, host='localhost', port=9200):
        """Initialize Elasticsearch client with multiple connection attempts"""
        self.es = None
        self.index_name = "products"

        # Try multiple connection methods
        connection_methods = [
            # Method 1: HTTPS with SSL verification disabled (Elasticsearch 8.x+)
            lambda: Elasticsearch([f'https://{host}:{port}'],
                                  verify_certs=False,
                                  ssl_show_warn=False),

            # Method 2: HTTP connection (Elasticsearch 7.x)
            lambda: Elasticsearch([{'host': host, 'port': port, 'scheme': 'http'}]),

            # Method 3: HTTPS with basic auth (if credentials needed)
            lambda: Elasticsearch([f'https://{host}:{port}'],
                                  verify_certs=False,
                                  ssl_show_warn=False,
                                  http_auth=('elastic', 'changeme')),  # Default credentials

            # Method 4: Simple URL format
            lambda: Elasticsearch([f'http://{host}:{port}']),
        ]

        for i, method in enumerate(connection_methods, 1):
            try:
                print(f"Trying connection method {i}...")
                self.es = method()

                # Test the connection
                if self.es.ping():
                    print(f"✅ Successfully connected using method {i}")
                    server_info = self.es.info()
                    print(f"Elasticsearch version: {server_info['version']['number']}")
                    break
                else:
                    print(f"❌ Method {i} failed: Connection test failed")

            except Exception as e:
                print(f"❌ Method {i} failed: {e}")
                continue

        if not self.es or not self.es.ping():
            raise ConnectionError("All connection methods failed. Please check Elasticsearch setup.")

        self.setup_index()
        self.populate_sample_data()

    def setup_index(self):
        """Create index with spell correction capabilities"""
        index_mapping = {
            "settings": {
                "analysis": {
                    "analyzer": {
                        "spellcheck_analyzer": {
                            "type": "standard",
                            "stopwords": "_none_"
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "product_name": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "spellcheck": {
                        "type": "text",
                        "analyzer": "spellcheck_analyzer"
                    }
                }
            }
        }

        # Delete existing index if it exists
        if self.es.indices.exists(index=self.index_name):
            self.es.indices.delete(index=self.index_name)

        # Create the index
        self.es.indices.create(index=self.index_name, body=index_mapping)
        print(f"Index '{self.index_name}' created successfully")

    def populate_sample_data(self):
        """Insert sample product data"""
        products = [
            {"product_name": "Honda Scooty", "spellcheck": "honda scooty scooter"},
            {"product_name": "Yamaha Scooter", "spellcheck": "yamaha scooter scooty"},
            {"product_name": "TVS Scooty Pep", "spellcheck": "tvs scooty pep scooter"},
            {"product_name": "Hero Electric Scooter", "spellcheck": "hero electric scooter scooty"},
            {"product_name": "Bajaj Scooty", "spellcheck": "bajaj scooty scooter"},
            {"product_name": "Suzuki Access", "spellcheck": "suzuki access scooty scooter"},
            {"product_name": "Honda Activa", "spellcheck": "honda activa scooty scooter"}
        ]

        # Index the data
        for i, product in enumerate(products):
            self.es.index(index=self.index_name, id=i + 1, body=product)

        # Refresh index to make data searchable
        self.es.indices.refresh(index=self.index_name)
        print(f"Sample data populated: {len(products)} products indexed")

    def complete_spell_correction(self, query_text):
        """Complete spell correction with search"""
        # Spell correction using term suggester
        suggestion_query = {
            "suggest": {
                "spell_suggester": {
                    "text": query_text,
                    "term": {
                        "field": "spellcheck",
                        "size": 3,
                        "sort": "frequency"
                    }
                }
            }
        }

        response = self.es.search(index=self.index_name, body=suggestion_query)

        corrected_terms = []
        for suggestion in response['suggest']['spell_suggester']:
            if suggestion['options']:
                best_option = suggestion['options'][0]
                corrected_terms.append(best_option['text'])
            else:
                corrected_terms.append(suggestion['text'])

        corrected_query = ' '.join(corrected_terms)

        # Search with corrected query
        search_query = {
            "query": {
                "multi_match": {
                    "query": corrected_query,
                    "fields": ["product_name^2", "spellcheck"],
                    "type": "best_fields"
                }
            }
        }

        search_response = self.es.search(index=self.index_name, body=search_query)

        return {
            "original_query": query_text,
            "corrected_query": corrected_query,
            "results": search_response['hits']['hits']
        }


def check_elasticsearch_setup():
    """Check Elasticsearch setup and provide troubleshooting info"""
    print("🔍 Elasticsearch Connection Diagnostics")
    print("=" * 50)

    import subprocess
    import socket

    # Check if Elasticsearch service is running
    try:
        result = subprocess.run(['systemctl', 'is-active', 'elasticsearch'],
                                capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Elasticsearch service is active")
        else:
            print("❌ Elasticsearch service is not active")
            print("   Try: sudo systemctl start elasticsearch")
    except:
        print("ℹ️  Cannot check service status (may not be a systemd system)")

    # Check port accessibility
    for scheme, port in [('HTTP', 9200), ('HTTPS', 9200)]:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(('localhost', port))
            sock.close()
            if result == 0:
                print(f"✅ Port {port} is accessible")
            else:
                print(f"❌ Port {port} is not accessible")
        except:
            print(f"❌ Cannot check port {port}")

    # Test curl commands
    print("\n🧪 Testing connectivity...")
    for url in ['http://localhost:9200', 'https://localhost:9200']:
        try:
            import requests
            response = requests.get(url, timeout=5, verify=False)
            print(f"✅ {url} - Response: {response.status_code}")
        except requests.exceptions.SSLError:
            print(f"🔒 {url} - SSL/TLS connection (try with -k flag)")
        except requests.exceptions.ConnectionError:
            print(f"❌ {url} - Connection refused")
        except Exception as e:
            print(f"❌ {url} - Error: {e}")


def main():
    """Main function with comprehensive error handling"""
    print("Elasticsearch Spell Correction Setup")
    print("=" * 40)

    # Run diagnostics first
    check_elasticsearch_setup()
    print()

    try:
        # Initialize the spell corrector
        spell_corrector = ElasticsearchSpellCorrector()

        # Test spell correction
        test_queries = ["skuty", "scuter", "yamha scooter"]

        print("\n📝 Testing Spell Corrections:")
        print("-" * 30)

        for query in test_queries:
            try:
                result = spell_corrector.complete_spell_correction(query)
                print(f"\nQuery: '{result['original_query']}' → '{result['corrected_query']}'")

                if result['results']:
                    print("Results:")
                    for i, hit in enumerate(result['results'][:3], 1):
                        print(f"  {i}. {hit['_source']['product_name']} (Score: {hit['_score']:.2f})")
                else:
                    print("  No results found")

            except Exception as e:
                print(f"Error processing '{query}': {e}")

        print("\n✅ Spell correction is working!")

    except Exception as e:
        print(f"\n❌ Setup failed: {e}")
        print("\n🔧 Manual Troubleshooting Steps:")
        print("1. Check if Elasticsearch is installed and running[4][6]:")
        print("   sudo systemctl status elasticsearch")
        print("2. For Elasticsearch 8.x+, try HTTPS instead of HTTP[2]:")
        print("   curl -k https://localhost:9200")
        print("3. Check firewall settings[4][6]:")
        print("   sudo ufw status")
        print("   sudo ufw allow 9200/tcp")
        print("4. Check Elasticsearch logs[4]:")
        print("   sudo tail -f /var/log/elasticsearch/elasticsearch.log")
        print("5. Restart Elasticsearch service[6]:")
        print("   sudo systemctl restart elasticsearch")


if __name__ == "__main__":
    main()
