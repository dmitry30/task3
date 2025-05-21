import os
import tempfile
import timeit
import unittest

from base import DataBase
from indexer import InvertedIndex, Indexer


class TestInvertedIndex(unittest.TestCase):
    def setUp(self):
        self.test_docs = [
            ("doc1", "Ректор СПбГУ объявил о новых правилах"),
            ("doc2", "В МГУ прошло собрание ректора"),
            ("doc3", "СПбГУ и МГУ сотрудничают в области науки"),
            ("doc4", "Ректор СПбГУ встретился с ректором МГУ")
        ]

    def test_add_document(self):
        '''Тест на добавление нового документа'''
        index = InvertedIndex(use_compression=False)
        for doc_id, text in self.test_docs:
            index.add_document(doc_id, text)

        self.assertEqual(len(index.doc_ids), 4)
        self.assertIn("спбгу", index.index.keys())
        self.assertEqual(len(index.index["спбгу"]), 3)

    def test_empty_search(self):
        '''Тест пустого поискового запроса'''
        index = InvertedIndex()
        index.add_document("doc1", "Содержимое документа")
        results = index.search("")
        self.assertEqual(len(results), 0)

    def test_tokenize(self):
        '''Тест на токенизацию текста'''
        index = InvertedIndex()
        tokens = index._tokenize("Ректор СПбГУ объявил о новых правилах")
        expected = ["ректор", "спбгу", "объявил", "о", "новых", "правилах"]
        self.assertEqual(tokens, expected)

    def test_search_without_compression(self):
        '''Тест на поиск текста без сжатия'''
        index = InvertedIndex(use_compression=False)
        for doc_id, text in self.test_docs:
            index.add_document(doc_id, text)

        results = index.search("Ректор СПбГУ")
        self.assertEqual(len(results), 2)
        self.assertIn("doc1", results)
        self.assertIn("doc4", results)

    def test_search_with_compression(self):
        '''Тест на поиск текста со сжатием'''
        index = InvertedIndex(use_compression=True)
        for doc_id, text in self.test_docs:
            index.add_document(doc_id, text)
        index.compress_index()

        results = index.search("Ректор СПбГУ")
        self.assertEqual(len(results), 2)
        self.assertIn("doc1", results)
        self.assertIn("doc4", results)

    def test_gamma_encoding(self):
        '''Тест на корректное кодирование - декодирование'''
        index = InvertedIndex()
        test_numbers = [1, 2, 3, 4, 5, 10, 100]
        for num in test_numbers:
            encoded = index._gamma_encode(num)
            decoded = next(index._gamma_decode(encoded))
            self.assertEqual(num, decoded)

    def test_compression_ratio(self):
        '''Тест на размер сжатия'''
        large_docs = []
        for i in range(1000):
            large_docs.append((f"doc{i}", f"Документ номер {i} содержит тестовые данные о ректоре университета"))

        # Индекс без сжатия
        index_uncompressed = InvertedIndex(use_compression=False)
        for doc_id, text in large_docs:
            index_uncompressed.add_document(doc_id, text)

        # Индекс со сжатием
        index_compressed = InvertedIndex(use_compression=True)
        for doc_id, text in large_docs:
            index_compressed.add_document(doc_id, text)
        index_compressed.compress_index()

        import pickle
        size_uncompressed = len(pickle.dumps(index_uncompressed.index))
        size_compressed = len(pickle.dumps(index_compressed.index))

        print(f"\nUncompressed size: {size_uncompressed} bytes")
        print(f"Compressed size: {size_compressed} bytes")
        print(f"Compression ratio: {size_uncompressed / size_compressed:.2f}x")

        self.assertLess(size_compressed, size_uncompressed)

    def test_save_load_index(self):
        '''Тест на сохранение и загрузку индекса'''
        index = InvertedIndex(use_compression=True)
        for doc_id, text in self.test_docs:
            index.add_document(doc_id, text)
        index.compress_index()

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                index.save_to_file(tmp.name)

                new_index = InvertedIndex()
                new_index.load_from_file(tmp.name)

                self.assertEqual(len(new_index.doc_ids), 4)
                results = new_index.search("СПбГУ")
                self.assertEqual(len(results), 3)
            finally:
                os.unlink(tmp.name)

    def test_duplicate_documents(self):
        '''Тест обработки дубликатов документов'''
        index = InvertedIndex()
        index.add_document("doc1", "Тестовый документ")
        index.add_document("doc1", "Тестовый документ")
        self.assertEqual(len(index.doc_ids), 1)
        self.assertEqual(len(index.index["тестовый"]), 1)

    def test_search_with_stop_words(self):
        '''Тест поиска со стоп-словами'''
        indexer = Indexer()
        indexer.process([
            ("doc1", "Это тестовый документ"),
            ("doc2", "Другой документ")
        ])

        results = indexer.search("Это документ")
        self.assertEqual(len(results), 1)
        self.assertIn("doc1", results)

    def test_case_sensitivity(self):
        '''Тест на чувствительность к регистру'''
        index = InvertedIndex()
        index.add_document("doc1", "Тест тест ТЕСТ")
        self.assertEqual(len(index.index), 1)
        self.assertIn("тест", index.index)


class TestIndexer(unittest.TestCase):
    def setUp(self):
        self.test_db_path = tempfile.mktemp()
        self.db = DataBase(self.test_db_path)
        for doc_id, text in [
            ("doc1", "Ректор СПбГУ объявил о новых правилах"),
            ("doc2", "В МГУ прошло собрание ректора"),
            ("doc3", "СПбГУ и МГУ сотрудничают в области науки"),
            ("doc4", "Ректор СПбГУ встретился с ректором МГУ")
        ]:
            self.db.insert_page(doc_id, text)

    def tearDown(self):
        self.db.close()
        if os.path.exists(self.test_db_path):
            os.unlink(self.test_db_path)

    def test_indexer_process(self):
        '''Тест индексера на чтение из базы данных'''

        def docs():
            cursor = self.db.conn.cursor()
            cursor.execute('''SELECT * FROM pages''')
            for row in cursor.fetchall():
                yield row[0], row[2]

        indexer = Indexer(use_compression=True)
        processing_time = indexer.process(docs())

        self.assertGreater(processing_time, 0)
        self.assertEqual(len(indexer.index.doc_ids), 4)

    def test_indexer_search(self):
        '''Тест индексера на поиск'''
        indexer = Indexer(use_compression=True)
        for doc_id, text in [
            ("doc1", "Ректор СПбГУ объявил о новых правилах"),
            ("doc2", "В МГУ прошло собрание ректора"),
            ("doc3", "СПбГУ и МГУ сотрудничают в области науки"),
            ("doc4", "Ректор СПбГУ встретился с ректором МГУ")
        ]:
            indexer.index.add_document(doc_id, text)
        indexer.index.compress_index()

        results = indexer.search("Ректор СПбГУ")
        self.assertEqual(len(results), 2)
        self.assertIn("doc1", results)
        self.assertIn("doc4", results)

    def test_index_size_comparison(self):
        '''Тест индексера на сжатие'''
        large_db_path = tempfile.mktemp()
        large_db = DataBase(large_db_path)
        try:
            for i in range(1000):
                text = f"Документ номер {i} содержит информацию о ректоре университета {i % 10}"
                large_db.insert_page(f"doc{i}", text)

            def get_docs():
                cursor = large_db.conn.cursor()
                cursor.execute('''SELECT * FROM pages''')
                for row in cursor.fetchall():
                    yield row[0], row[2]

            # Индекс без сжатия
            indexer_uncompressed = Indexer(use_compression=False)
            indexer_uncompressed.process(get_docs())
            uncompressed_size = indexer_uncompressed.get_index_size()

            # Индекс со сжатием
            indexer_compressed = Indexer(use_compression=True)
            indexer_compressed.process(get_docs())
            compressed_size = indexer_compressed.get_index_size()

            # Проверяем что сжатый индекс значительно меньше
            self.assertLess(compressed_size, uncompressed_size)
            compression_ratio = uncompressed_size / compressed_size
            print(f"\nCompression ratio: {compression_ratio:.2f}x")

        finally:
            large_db.close()
            if os.path.exists(large_db_path):
                os.unlink(large_db_path)


class PerformanceTests(unittest.TestCase):
    def test_indexing_performance(self):
        """Тест производительности индексации"""
        docs = [(f"doc{i}", "СПбГУ " * 100) for i in range(1000)]

        def test_uncompressed():
            indexer = Indexer(use_compression=False)
            indexer.process(docs)

        def test_compressed():
            indexer = Indexer(use_compression=True)
            indexer.process(docs)

        uncompressed_time = timeit.timeit(test_uncompressed, number=1)
        compressed_time = timeit.timeit(test_compressed, number=1)

        print(f"\nUncompressed indexing time: {uncompressed_time:.2f}s")
        print(f"Compressed indexing time: {compressed_time:.2f}s")

        self.assertLess(compressed_time, uncompressed_time * 1.5)


if __name__ == '__main__':
    unittest.main(verbosity=2)
