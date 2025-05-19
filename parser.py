import requests
from requests import Response
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from typing import Generator, Tuple
import re
from base import DataBase
import argparse

class WebCrawler:
    def __init__(self, start_url: str, max_pages: int = 10):
        self.start_url = start_url
        self.max_pages = max_pages
        self.visited_urls = set()
        self.domain = urlparse(start_url).netloc

    def is_valid_url(self, url: str) -> bool:
        parsed = urlparse(url)
        if not (bool(parsed.netloc) and parsed.netloc == self.domain
                and parsed.scheme in {'http', 'https'}):
            return False

        path = parsed.path.lower()
        invalid_extension_re = re.compile(r'^.*\.(?!html$)[a-z0-9]{2,4}(?=$|\?|#|/)')

        if invalid_extension_re.match(path):
            return False
        return True

    def extract_links(self, response: Response) -> set:
        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            links = set()

            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                full_url = urljoin(response.url, href)
                if self.is_valid_url(full_url):
                    links.add(full_url)

            return links
        except:
            return set()

    def get_page_text(self, response: Response) -> str:
        try:
            soup = BeautifulSoup(response.text, 'html.parser')

            for script in soup(['script', 'style']):
                script.decompose()

            return ' '.join(soup.stripped_strings)
        except:
            return ""

    def crawl(self) -> Generator[Tuple[str, str], None, None]:
        queue = [self.start_url]
        self.visited_urls.add(self.start_url)
        pages_crawled = 0

        while queue and pages_crawled < self.max_pages:
            current_url = queue.pop(0)
            response = requests.get(current_url, timeout=5)
            text_data = self.get_page_text(response)

            yield (current_url, text_data)
            pages_crawled += 1

            # Добавляем новые ссылки в очередь
            for link in self.extract_links(response):
                if link not in self.visited_urls:
                    self.visited_urls.add(link)
                    queue.append(link)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Web crawler with database storage')
    parser.add_argument('--db', type=str, default='base.db',
                       help='Database filename (default: base.db)')
    parser.add_argument('--start-url', type=str, default='https://spbu.ru/',
                       help='Starting URL for crawling (default: https://spbu.ru/)')
    parser.add_argument('--max-pages', type=int, default=50000,
                       help='Maximum number of pages to crawl (default: 50000)')
    args = parser.parse_args()

    db = DataBase(args.db)
    crawler = WebCrawler(start_url=args.start_url, max_pages=args.max_pages)

    for url, text in crawler.crawl():
        print(f"URL: {url}")
        print(f"Text data (first 100 chars): {text[:100]}...")
        print("-" * 80)
        db.insert_page(url, text)
    db.close()