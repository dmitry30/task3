import time
from collections import defaultdict
from typing import List, Dict, Tuple, Set, Iterator, Any, Union, BinaryIO
import pickle
import re
from pympler import asizeof


class InvertedIndex:
    def __init__(self, use_compression: bool = True) -> None:
        """Инициализация инвертированного индекса.

        Args:
            use_compression: Использовать ли сжатие индекса (по умолчанию True).
        """
        self.index: Union[Dict[str, Set[int]], Dict[str, bytes]] = defaultdict(set)
        self.doc_ids: Dict[str, int] = {}
        self.doc_counter: int = 1
        self.use_compression: bool = use_compression

    def add_document(self, doc_id: str, text: str) -> None:
        """Добавляет документ в индекс.

        Args:
            doc_id: Уникальный идентификатор документа.
            text: Текст документа для индексации.
        """
        if doc_id in self.doc_ids:
            return

        self.doc_ids[doc_id] = self.doc_counter
        self.doc_counter += 1

        words = self._tokenize(text)
        for word in words:
            self.index[word].add(self.doc_ids[doc_id])

    def _tokenize(self, text: str) -> List[str]:
        """Токенизирует текст на слова.

        Args:
            text: Входной текст для токенизации.

        Returns:
            Список токенов (слов).
        """
        return re.findall(r'[-+]?\d*\.?\d+|\b[\w+]+\b', text)

    def compress_index(self) -> None:
        """Сжимает индекс используя гамма-кодирование."""
        if not self.use_compression:
            return

        compressed_index: Dict[str, bytes] = {}
        for term, docs in self.index.items():
            sdocs = sorted(docs)
            shift = [sdocs[0]]
            shift.extend([sdocs[i + 1] - sdocs[i] for i in range(len(sdocs) - 1)])
            compressed_index[term] = b''.join([self._gamma_encode(_) for _ in shift])
        self.index = compressed_index

    def _gamma_encode(self, num: int) -> bytes:
        """Кодирует число используя гамма-кодирование Элиаса.

        Args:
            num: Число для кодирования.

        Returns:
            Закодированное число в виде bytes.

        Raises:
            ValueError: Если num == 0.
        """
        if num == 0:
            raise ValueError('Недопустимое значение')

        binary = bin(num)[3:]
        return ('0' * len(binary) + '1' + binary).encode('utf-8')

    def _gamma_decode(self, encoded: bytes) -> Iterator[int]:
        """Декодирует поток чисел, закодированных гамма-кодированием.

        Args:
            encoded: Закодированные данные.

        Yields:
            Очередное декодированное число.
        """
        if not encoded:
            return

        pos_stream = encoded.decode('utf-8')
        pos_start = 0
        prev_pos = 0
        while pos_start < len(pos_stream):
            unary_end = pos_stream[pos_start:].find('1')
            if unary_end == -1:
                break
            unary_end += pos_start
            length = unary_end - pos_start
            binary = '1' + pos_stream[unary_end + 1:unary_end + 1 + length]
            delta_pos = int(binary, 2)
            pos = prev_pos + delta_pos
            prev_pos = pos
            yield pos
            pos_start = unary_end + 1 + length

    def search(self, query: str) -> List[str]:
        """Выполняет поиск по индексу.

        Args:
            query: Поисковый запрос.

        Returns:
            Список идентификаторов документов, содержащих все термины запроса.
        """
        terms = self._tokenize(query)
        if not terms:
            return []

        postings_lists: List[List[int]] = []
        for term in terms:
            if term in self.index:
                if self.use_compression:
                    postings_lists.append([_ for _ in self._gamma_decode(self.index[term])])
                else:
                    postings_lists.append(list(self.index[term]))
            else:
                return []

        result = set(postings_lists[0])
        for postings in postings_lists[1:]:
            result.intersection_update(postings)
            if not result:
                break

        return [doc_id for doc_id, idx in self.doc_ids.items() if idx in result]

    def save_to_file(self, filename: str) -> None:
        """Сохраняет индекс в файл.

        Args:
            filename: Имя файла для сохранения.
        """
        with open(filename, 'wb') as f:
            pickle.dump({
                'index': self.index,
                'doc_ids': self.doc_ids,
                'doc_counter': self.doc_counter,
                'use_compression': self.use_compression
            }, f)

    def load_from_file(self, filename: str) -> None:
        """Загружает индекс из файла.

        Args:
            filename: Имя файла для загрузки.
        """
        with open(filename, 'rb') as f:
            data: Dict[str, Any] = pickle.load(f)
            self.index = data['index']
            self.doc_ids = data['doc_ids']
            self.doc_counter = data['doc_counter']
            self.use_compression = data['use_compression']


class Indexer:
    def __init__(self, use_compression: bool = True) -> None:
        """Инициализация индексатора.

        Args:
            use_compression: Использовать ли сжатие индекса.
        """
        self.index: InvertedIndex = InvertedIndex(use_compression)

    def process(self, docs: List[Tuple[str, str]]) -> float:
        """Обрабатывает коллекцию документов.

        Args:
            docs: Список кортежей (doc_id, text).

        Returns:
            Время обработки в секундах.
        """
        start_time: float = time.time()
        count: int = 0

        for doc_id, text in docs:
            self.index.add_document(doc_id, text)
            count += 1
            if count % 100 == 0:
                print(f'Processed {count} documents')

        if self.index.use_compression:
            print('Compressing index...')
            compress_start: float = time.time()
            self.index.compress_index()
            compress_time: float = time.time() - compress_start
            print(f'Index compressed in {compress_time:.2f} seconds')

        total_time: float = time.time() - start_time
        print(f'Indexing completed. Processed {count} documents in {total_time:.2f} seconds')
        return total_time

    def save_index(self, filename: str) -> None:
        """Сохраняет индекс в файл.

        Args:
            filename: Имя файла для сохранения.
        """
        self.index.save_to_file(filename)

    def load_index(self, filename: str) -> None:
        """Загружает индекс из файла.

        Args:
            filename: Имя файла для загрузки.
        """
        self.index.load_from_file(filename)

    def search(self, query: str) -> List[str]:
        """Выполняет поиск по индексу.

        Args:
            query: Поисковый запрос.

        Returns:
            Список идентификаторов документов, содержащих все термины запроса.
        """
        return self.index.search(query)

    def get_index_size(self) -> Tuple[int, int]:
        """Возвращает размер индекса в байтах.

        Returns:
            Кортеж с размером индекса в байтах.
        """
        return asizeof.asizeof(self.index.index)