"""Directory cleanup regression tests, isolated from DB/RPC bootstrap.

Run: python3 -m unittest discover -s tests/seasearch -p test_directory_cleanup.py -v
"""
import hashlib
import unittest
from unittest.mock import Mock

from test_realtime_index import load_class


class DirectoryCleanupTests(unittest.TestCase):
    def make_index(self, paths):
        md5 = lambda path: hashlib.md5(path.encode()).hexdigest()
        cls = load_class('seasearch/index_store/repo_file_index.py', 'RepoFileIndex', {
            'md5': md5,
            'is_sys_dir_or_file': lambda path: False,
            'SEASEARCH_BULK_OPETATE_LIMIT': 100,
        })
        index = cls.__new__(cls)
        documents = {md5(path): path for path in paths}

        def search(index_name, dsl):
            prefix = dsl['query']['bool']['filter'][0]['prefix']['path']
            hits = [{'_id': doc_id, '_source': {'path': path}}
                    for doc_id, path in documents.items() if path.startswith(prefix)]
            return {'hits': {'hits': hits, 'total': {'value': len(hits)}}}

        def bulk(index_name, operations):
            for operation in operations:
                documents.pop(operation['delete']['_id'], None)

        index.seasearch_api = Mock()
        index.seasearch_api.normal_search.side_effect = search
        index.seasearch_api.bulk.side_effect = bulk
        return index, documents

    def test_deleted_directory_does_not_delete_prefix_siblings(self):
        deleted = ['/Customer/', '/Customer/old.txt', '/Customer/nested/',
                   '/Customer/nested/old.txt']
        retained = ['/Customer Inc./keep.txt', '/Customer2/keep.txt', '/Customer.txt']
        for directory in ('/Customer', '/Customer/'):
            with self.subTest(directory=directory):
                index, documents = self.make_index(deleted + retained)
                index.delete_files_by_deleted_dirs('file_repo', [directory])
                self.assertEqual(set(documents.values()), set(retained))

    def test_root_cleanup_still_matches_all_paths(self):
        index, documents = self.make_index(['/', '/a.txt', '/Customer/nested/b.txt'])
        index.delete_files_by_deleted_dirs('file_repo', ['/'])
        self.assertFalse(documents)


if __name__ == '__main__':
    unittest.main()
