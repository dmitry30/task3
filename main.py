from base import DataBase
from parser import WebCrawler
from indexer import Indexer

if __name__ == '__main__':
    db = DataBase("base.db")

    # # Загрузка сайтов
    # crawler = WebCrawler(start_url="https://spbu.ru/", max_pages=5000000)
    # for url, data in crawler.crawl():
    #     print(f"URL: {url}")
    #     print(f"Text data (first 100 chars): {data[:100]}...")
    #     print("-" * 80)
    #     db.insert_page(url, data)

    cursor = db.conn.cursor()
    cursor.execute('''SELECT count(*) FROM pages ''')
    print("Всего сайтов:", cursor.fetchall()[0][0])
    def docs():
        i = 0
        cursor = db.conn.cursor()
        while True:
            cursor.execute('''SELECT * FROM pages limit 100 offset ?''', (i,))
            pages = cursor.fetchall()
            i += 100
            if len(pages) == 0:
                return
            for p in pages:
                yield p[0], p[2]

    indexer = Indexer()
    indexer.process(docs())
    print(indexer.search("ПМПУ"))
    while True:
        print(indexer.search(input()))
    db.close()