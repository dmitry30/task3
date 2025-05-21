import unittest
import os
import tempfile
from indexer import InvertedIndex, Indexer
from base import DataBase


class TestInvertedIndex(unittest.TestCase):
    def setUp(self):
        self.test_docs = [
            ("doc1", "Ректор СПбГУ объявил о новых правилах"),
            ("doc2", "В МГУ прошло собрание ректора"),
            ("doc3", "СПбГУ и МГУ сотрудничают в области науки"),
            ("doc4", "Ректор СПбГУ встретился с ректором МГУ")
        ]

    def test_add_document(self):
        index = InvertedIndex(use_compression=False)
        for doc_id, text in self.test_docs:
            index.add_document(doc_id, text)

        self.assertEqual(len(index.doc_ids), 4)
        self.assertIn("СПбГУ", index.index.keys())
        self.assertEqual(len(index.index["СПбГУ"]), 3)

    def test_tokenize(self):
        index = InvertedIndex()
        tokens = index._tokenize("Ректор СПбГУ объявил о новых правилах")
        expected = ["Ректор", "СПбГУ", "объявил", "о", "новых", "правилах"]
        self.assertEqual(tokens, expected)

    def test_search_without_compression(self):
        index = InvertedIndex(use_compression=False)
        for doc_id, text in self.test_docs:
            index.add_document(doc_id, text)

        results = index.search("Ректор СПбГУ")
        self.assertEqual(len(results), 2)
        self.assertIn("doc1", results)
        self.assertIn("doc4", results)

    def test_search_with_compression(self):
        index = InvertedIndex(use_compression=True)
        for doc_id, text in self.test_docs:
            index.add_document(doc_id, text)
        index.compress_index()

        results = index.search("Ректор СПбГУ")
        self.assertEqual(len(results), 2)
        self.assertIn("doc1", results)
        self.assertIn("doc4", results)

    def test_gamma_encoding(self):
        index = InvertedIndex()
        test_numbers = [1, 2, 3, 4, 5, 10, 100]
        for num in test_numbers:
            encoded = index._gamma_encode(num)
            decoded = next(index._gamma_decode(encoded))
            self.assertEqual(num, decoded)

    def test_compression_ratio(self):
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


if __name__ == '__main__':
    unittest.main(verbosity=2)