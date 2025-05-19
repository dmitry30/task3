import sqlite3

class DataBase():

    def __init__(self, filename):
        self.conn = sqlite3.connect(filename)
        cursor = self.conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS pages
                          (id INTEGER PRIMARY KEY, url TEXT UNIQUE, data TEXT)''')
        self.conn.commit()
        cursor.close()

    def close(self):
        self.conn.close()

    def insert_page(self, url, data):
        try:
            cursor = self.conn.cursor()
            cursor.execute('''INSERT INTO pages (url, data) VALUES (?, ?)''', (url, data))
            self.conn.commit()
            cursor.close()
        except sqlite3.IntegrityError:
            pass

    def get_urls_by_ids(self, ids):
        try:
            cursor = self.conn.cursor()
            placeholders = ','.join('?' for _ in ids)
            cursor.execute(f'SELECT url FROM pages WHERE id IN ({placeholders})', ids)
            urls = cursor.fetchall()
            cursor.close()
            return urls
        except sqlite3.IntegrityError:
            pass