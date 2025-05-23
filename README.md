## Работа с инвертированным индексом 
Основной файл main.py

```bash
usage: main.py [-h] [--db DB] [--compression COMPRESSION] [--search SEARCH]
Поисковый индексер для базы данных страниц
options:
  -h, --help            show this help message and exit
  --db DB               Имя файла базы данных (по умолчанию: base.db)
  --compression COMPRESSION
                        Использовать сжатие индекса (по умолчанию: True)
  --search SEARCH       Поисковый запрос (по умолчанию: "Ректор СПбГУ")
```

В результате вернет ссылки в которых есть поисковый запрос


В базе данных таблица pages(id int, url text, data text)

Если базы нет ее можно создать при помощи parser.py <br>

```bash
usage: parser.py [-h] [--db DB] [--start-url START_URL] [--max-pages MAX_PAGES]

Web crawler with database storage

options:
  -h, --help            show this help message and exit
  --db DB               Имя файла базы данных(по умолчанию: base.db)
  --start-url START_URL
                        Начальная страница для прохода (по умолчанию: https://spbu.ru/)
  --max-pages MAX_PAGES
                        Максимальное количество страниц для прохода (по умолчанию: 50000)
```


Результаты проверки для сайта spbu для 40000 страниц
1) Время создания индексов 60 секунд
2) Время построение сжатых индексов 14 секунд
3) Время поиска 0.09 секунд для сжатых индексов, и 0.005 для несжатых 
4) Ректор СПбГУ встречается на каждой странице, так как он есть в выпадающем меню заголовка 