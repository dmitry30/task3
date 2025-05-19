import os
import json
import math
import time
from collections import defaultdict
from typing import List, Dict, Tuple
from bs4 import BeautifulSoup
import pickle


class InvertedIndex:
    def __init__(self, use_compression=True):
        self.index = defaultdict(list)
        self.doc_ids = {}
        self.doc_counter = 0
        self.use_compression = use_compression

    def add_document(self, doc_id: str, text: str):
        if doc_id in self.doc_ids:
            return

        self.doc_ids[doc_id] = self.doc_counter
        self.doc_counter += 1

        words = self._tokenize(text)
        for pos, word in enumerate(words):
            if self.use_compression:
                # Для сжатого индекса храним (doc_id, позиции)
                if word in self.index:
                    last_doc_id, positions = self.index[word][-1]
                    if last_doc_id == self.doc_ids[doc_id]:
                        positions.append(pos)
                    else:
                        self.index[word].append((self.doc_ids[doc_id], [pos]))
                else:
                    self.index[word].append((self.doc_ids[doc_id], [pos]))
            else:
                # Для несжатого индекса просто добавляем
                self.index[word].append((self.doc_ids[doc_id], pos))

    def _tokenize(self, text: str) -> List[str]:
        return text.lower().split()

    def compress_index(self):
        if not self.use_compression:
            return

        compressed_index = {}
        for term, postings in self.index.items():
            postings.sort(key=lambda x: x[0])

            compressed_postings = []
            prev_doc_id = 0
            for doc_id, positions in postings:
                delta_doc = doc_id - prev_doc_id
                prev_doc_id = doc_id

                if len(positions) > 1:
                    positions.sort()
                    compressed_positions = []
                    prev_pos = 0
                    for pos in positions:
                        delta_pos = pos - prev_pos
                        prev_pos = pos
                        compressed_positions.append(self._gamma_encode(delta_pos))
                    compressed_positions = b''.join(compressed_positions)
                else:
                    compressed_positions = self._gamma_encode(positions[0])

                compressed_postings.append((self._gamma_encode(delta_doc), compressed_positions))

            compressed_index[term] = compressed_postings

        self.index = compressed_index

    def _gamma_encode(self, num: int) -> bytes:
        if num == 0:
            return b''

        binary = bin(num)[3:]
        length = len(binary)
        unary = '1' * length + '0'
        return (unary + binary).encode('utf-8')

    def _gamma_decode(self, encoded: bytes) -> int:
        if not encoded:
            return 0

        s = encoded.decode('utf-8')
        unary_end = s.find('0')
        length = unary_end
        binary = '1' + s[unary_end + 1:unary_end + 1 + length]
        return int(binary, 2)

    def search(self, query: str) -> List[str]:
        terms = self._tokenize(query)
        if not terms:
            return []

        postings_lists = []
        for term in terms:
            if term in self.index:
                if self.use_compression:
                    decompressed = []
                    prev_doc_id = 0
                    for encoded_doc, encoded_positions in self.index[term]:
                        delta_doc = self._gamma_decode(encoded_doc)
                        doc_id = prev_doc_id + delta_doc
                        prev_doc_id = doc_id

                        positions = []
                        if isinstance(encoded_positions, bytes):
                            pos_stream = encoded_positions.decode('utf-8')
                            pos_start = 0
                            prev_pos = 0
                            while pos_start < len(pos_stream):
                                unary_end = pos_stream[pos_start:].find('0')
                                if unary_end == -1:
                                    break
                                unary_end += pos_start
                                length = unary_end - pos_start
                                binary = '1' + pos_stream[unary_end + 1:unary_end + 1 + length]
                                delta_pos = int(binary, 2)
                                pos = prev_pos + delta_pos
                                positions.append(pos)
                                prev_pos = pos
                                pos_start = unary_end + 1 + length

                        decompressed.append((doc_id, positions))
                    postings_lists.append(decompressed)
                else:
                    postings_lists.append(self.index[term])
            else:
                return []

        result = set(self._get_doc_ids(postings_lists[0]))
        for postings in postings_lists[1:]:
            result.intersection_update(self._get_doc_ids(postings))
            if not result:
                break

        return [doc_id for doc_id, idx in self.doc_ids.items() if idx in result]

    def _get_doc_ids(self, postings):
        return [doc_id for doc_id, _ in postings]

    def save_to_file(self, filename: str):
        with open(filename, 'wb') as f:
            pickle.dump({
                'index': self.index,
                'doc_ids': self.doc_ids,
                'doc_counter': self.doc_counter,
                'use_compression': self.use_compression
            }, f)

    def load_from_file(self, filename: str):
        with open(filename, 'rb') as f:
            data = pickle.load(f)
            self.index = data['index']
            self.doc_ids = data['doc_ids']
            self.doc_counter = data['doc_counter']
            self.use_compression = data['use_compression']


class Indexer:
    def __init__(self, use_compression=True):
        self.index = InvertedIndex(use_compression)

    def process(self, docs):
        start_time = time.time()
        count = 0

        for doc_id, text in docs:
            self.index.add_document(doc_id, text)
            count += 1
            if count % 100 == 0:
                print(f'Processed {count} documents')

        if self.index.use_compression:
            print('Compressing index...')
            compress_start = time.time()
            self.index.compress_index()
            compress_time = time.time() - compress_start
            print(f'Index compressed in {compress_time:.2f} seconds')

        total_time = time.time() - start_time
        print(f'Indexing completed. Processed {count} documents in {total_time:.2f} seconds')
        return total_time

    def save_index(self, filename: str):
        self.index.save_to_file(filename)

    def load_index(self, filename: str):
        self.index.load_from_file(filename)

    def search(self, query: str) -> List[str]:
        return self.index.search(query)

    def get_index_size(self) -> Tuple[int, int]:
        uncompressed_size = len(pickle.dumps({
            'index': self.index.index,
            'doc_ids': self.index.doc_ids,
            'doc_counter': self.index.doc_counter,
            'use_compression': False
        }))

        compressed_size = len(pickle.dumps({
            'index': self.index.index,
            'doc_ids': self.index.doc_ids,
            'doc_counter': self.index.doc_counter,
            'use_compression': True
        }))

        return uncompressed_size, compressed_size