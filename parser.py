import requests
from requests import Response
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from typing import Generator, Tuple


class WebCrawler:
    def __init__(self, start_url: str, max_pages: int = 10):
        self.start_url = start_url
        self.max_pages = max_pages
        self.visited_urls = set()
        self.domain = urlparse(start_url).netloc

    def is_valid_url(self, url: str) -> bool:
        parsed = urlparse(url)
        return bool(parsed.netloc) and parsed.netloc == self.domain and parsed.scheme in {'http', 'https'}

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
    crawler = WebCrawler(start_url="https://spbu.ru/", max_pages=5)

    for url, text in crawler.crawl():
        print(f"URL: {url}")
        print(f"Text data (first 100 chars): {text[:100]}...")
        print("-" * 80)