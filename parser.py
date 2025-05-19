import requests
from requests import Response
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from typing import Generator, Tuple, Set, Optional, Pattern
from base import DataBase
import argparse
import re


class WebCrawler:
    def __init__(self, start_url: str, max_pages: int = 10) -> None:
        """Инициализация веб-краулера.

        Args:
            start_url: Начальный URL для обхода.
            max_pages: Максимальное количество страниц для сканирования (по умолчанию 10).
        """
        self.start_url: str = start_url
        self.max_pages: int = max_pages
        self.visited_urls: Set[str] = set()
        self.domain: str = urlparse(start_url).netloc
        self._invalid_extension_re: Pattern[str] = re.compile(
            r'^.*\.(?!html$)[a-z0-9]{2,4}(?=$|\?|#|/)'
        )

    def is_valid_url(self, url: str) -> bool:
        """Проверяет, является ли URL допустимым для сканирования.

        Args:
            url: URL для проверки.

        Returns:
            True если URL допустим, иначе False.
        """
        parsed = urlparse(url)
        if not (bool(parsed.netloc) and parsed.netloc == self.domain
                and parsed.scheme in {'http', 'https'}):
            return False

        path = parsed.path.lower()
        if self._invalid_extension_re.match(path):
            return False
        return True

    def extract_links(self, response: Response) -> Set[str]:
        """Извлекает все допустимые ссылки со страницы.

        Args:
            response: Ответ HTTP-запроса.

        Returns:
            Множество найденных допустимых URL.
        """
        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            links: Set[str] = set()

            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                full_url = urljoin(response.url, href)
                if self.is_valid_url(full_url):
                    links.add(full_url)

            return links
        except Exception:
            return set()

    def get_page_text(self, response: Response) -> str:
        """Извлекает текстовое содержимое страницы.

        Args:
            response: Ответ HTTP-запроса.

        Returns:
            Очищенный текст страницы или пустая строка в случае ошибки.
        """
        try:
            soup = BeautifulSoup(response.text, 'html.parser')

            for script in soup(['script', 'style']):
                script.decompose()

            return ' '.join(soup.stripped_strings)
        except Exception:
            return ""

    def crawl(self) -> Generator[Tuple[str, str], None, None]:
        """Основной метод сканирования веб-страниц.

        Yields:
            Кортеж (URL, текст) для каждой посещенной страницы.
        """
        queue: List[str] = [self.start_url]
        self.visited_urls.add(self.start_url)
        pages_crawled: int = 0

        while queue and pages_crawled < self.max_pages:
            current_url: str = queue.pop(0)
            try:
                response: Response = requests.get(current_url, timeout=5)
                text_data: str = self.get_page_text(response)

                yield (current_url, text_data)
                pages_crawled += 1

                for link in self.extract_links(response):
                    if link not in self.visited_urls:
                        self.visited_urls.add(link)
                        queue.append(link)
            except requests.RequestException:
                continue


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Web crawler with database storage')
    parser.add_argument('--db', type=str, default='base.db',
                        help='Database filename (default: base.db)')
    parser.add_argument('--start-url', type=str, default='https://spbu.ru/',
                        help='Starting URL for crawling (default: https://spbu.ru/)')
    parser.add_argument('--max-pages', type=int, default=50000,
                        help='Maximum number of pages to crawl (default: 50000)')
    args: argparse.Namespace = parser.parse_args()

    db: DataBase = DataBase(args.db)
    crawler: WebCrawler = WebCrawler(start_url=args.start_url, max_pages=args.max_pages)

    for url, text in crawler.crawl():
        print(f"URL: {url}")
        print(f"Text data (first 100 chars): {text[:100]}...")
        print("-" * 80)
        db.insert_page(url, text)
    db.close()