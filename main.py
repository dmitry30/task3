from base import DataBase
from indexer import Indexer
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Поисковый индексер для базы данных страниц')
    parser.add_argument('--db', type=str, default='base.db',
                       help='Имя файла базы данных (по умолчанию: base.db)')
    parser.add_argument('--compression', type=bool, default=True,
                       help='Использовать сжатие индекса (по умолчанию: True)')
    parser.add_argument('--search', type=str, default='Ректор СПбГУ',
                       help='Поисковый запрос (по умолчанию: "Ректор СПбГУ")')
    args = parser.parse_args()

    db = DataBase(args.db)

    cursor = db.conn.cursor()
    cursor.execute('''SELECT count(*) FROM pages ''')
    print("Всего сайтов:", cursor.fetchall()[0][0])
    def docs():
        i = 0
        cursor = db.conn.cursor()
        while True:
            cursor.execute('''SELECT * FROM pages limit 1000 offset ?''', (i,))
            pages = cursor.fetchall()
            i += 1000
            if len(pages) == 0:
                return
            for p in pages:
                yield p[0], p[2]

    indexer = Indexer(use_compression=args.compression)
    indexer.process(docs())
    res = indexer.search(args.search)
    if len(res) == 0:
        print("No results")
    else:
        print('\n'.join([_[0] for _ in db.get_urls_by_ids(res)]))
        print(len(res), "results")
    db.close()