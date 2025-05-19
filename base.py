import sqlite3
from typing import Optional, List, Tuple, Any, Union


class DataBase:
    def __init__(self, filename: str) -> None:
        """Инициализация базы данных. Создает таблицу pages, если она не существует.

        Args:
            filename: Путь к файлу базы данных SQLite.
        """
        self.conn: sqlite3.Connection = sqlite3.connect(filename)
        cursor: sqlite3.Cursor = self.conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS pages
                          (id INTEGER PRIMARY KEY, url TEXT UNIQUE, data TEXT)''')
        self.conn.commit()
        cursor.close()

    def close(self) -> None:
        """Закрывает соединение с базой данных."""
        self.conn.close()

    def insert_page(self, url: str, data: str) -> None:
        """Добавляет новую запись в таблицу pages.

        Args:
            url: URL страницы (уникальный ключ).
            data: Данные страницы.

        Raises:
            sqlite3.IntegrityError: Если URL уже существует в базе.
        """
        try:
            cursor: sqlite3.Cursor = self.conn.cursor()
            cursor.execute('''INSERT INTO pages (url, data) VALUES (?, ?)''', (url, data))
            self.conn.commit()
            cursor.close()
        except sqlite3.IntegrityError:
            pass  # Игнорируем дубликаты URL

    def get_urls_by_ids(self, ids: List[int]) -> Optional[List[Tuple[str]]]:
        """Возвращает URL-адреса по заданным ID.

        Args:
            ids: Список идентификаторов записей.

        Returns:
            Список кортежей с URL-адресами или None в случае ошибки.
        """
        try:
            cursor: sqlite3.Cursor = self.conn.cursor()
            placeholders: str = ','.join('?' for _ in ids)
            cursor.execute(f'SELECT url FROM pages WHERE id IN ({placeholders})', ids)
            urls: List[Tuple[str]] = cursor.fetchall()
            cursor.close()
            return urls
        except sqlite3.IntegrityError:
            return None