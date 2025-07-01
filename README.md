# 🕷️ Web Crawler

A lightweight Python-based web crawler designed to crawl websites, extract URLs, and collect structured page content efficiently. This project was built for learning and exploring how basic crawling, content parsing, and recursive link exploration works.

---

## 🚀 Features

- Crawl and extract links from a target domain
- Parse page content using BeautifulSoup
- Avoids duplicate URL visits using a set
- Recursively crawls discovered links within the same domain
- Customizable depth level for crawling
- CLI-friendly — easily run from the terminal

---

## 🛠️ Tech Stack

- **Python 3**
- **Requests** – For making HTTP requests
- **BeautifulSoup4** – For HTML parsing
- **re (Regex)** – For URL pattern matching and cleanup

---

## 📁 Project Structure


web_crawler/
│
├── main.py # Entry point for crawling
├── crawler # Core crawling logic
├── utils # Helper functions
├── requirements.txt # Dependencies
└── README.md # Project documentation

---

## ⚙️ How It Works

1. Starts crawling from a given root URL.
2. Extracts all internal links using anchor tags (`<a href="">`).
3. Normalizes and filters links to avoid external sites.
4. Avoids revisiting the same URLs.
5. Recursively crawls links up to a specified depth.

---

## ✅ Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/Ankit1017/web_crawler.git
cd web_crawler

📚 Use Cases
SEO and internal link analysis

Learning web scraping and crawling

Research and content discovery

Custom website mapping tools

🧠 Future Enhancements
Add asyncio or multithreading

Support robots.txt and rate limiting

Export results to CSV/JSON

Extract content like headings, metadata, or paragraphs

🙋‍♂️ Author
Ankit Bansal
GitHub: @Ankit1017

📄 License
This project is licensed under the MIT License.
