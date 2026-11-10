"""Isolated DOCX extraction tests without DB/storage initialization."""

import ast
from io import BytesIO
from pathlib import Path
import re
import unittest
from unittest.mock import Mock, patch
from zipfile import ZipFile, ZIP_DEFLATED


SOURCE = Path(__file__).resolve().parents[2] / 'seasearch/utils/extract.py'


class DocxExtractionTests(unittest.TestCase):
    def setUp(self):
        tree = ast.parse(SOURCE.read_text())
        names = {'ZipString', 'extract_docx_text', 'Extractor'}
        self.namespace = {
            'ZipFile': ZipFile, 'BytesIO': BytesIO, 're': re, 'logger': Mock(),
            'DOCX_MAX_MEMBER_SIZE': 64, 'DOCX_MAX_UNCOMPRESSED_SIZE': 128,
            'ZERO_OBJ_ID': '0' * 40,
        }
        nodes = [node for node in tree.body
                 if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in names]
        exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), 'exec'), self.namespace)

    def archive(self, members):
        output = BytesIO()
        with ZipFile(output, 'w', compression=ZIP_DEFLATED) as archive:
            for name, content in members.items():
                archive.writestr(name, content)
        return output.getvalue()

    def test_normal_text_extracted(self):
        content = self.archive({'word/document.xml': b'<w:p>Hello</w:p>\n<w:p>world</w:p>'})
        result = self.namespace['extract_docx_text'](content)
        self.assertEqual(result, b' Hello \n world ')

    def test_oversized_member_rejected_before_decompression(self):
        content = self.archive({'word/document.xml': b' ' * 1024})
        with patch.object(ZipFile, 'open', side_effect=AssertionError('Must not decompress')):
            with self.assertRaisesRegex(ValueError, 'indexing size limit'):
                self.namespace['extract_docx_text'](content)

    def test_total_limit_rejected_before_decompression(self):
        content = self.archive({'word/document.xml': b' ' * 64, 'a': b' ' * 64, 'b': b'x'})
        with patch.object(ZipFile, 'open', side_effect=AssertionError('Must not decompress')):
            with self.assertRaises(ValueError):
                self.namespace['extract_docx_text'](content)

    def test_oversized_file_skips_content_without_raising_from_extractor(self):
        content = self.archive({'word/document.xml': b' ' * 1024})
        storage = Mock()
        storage.load_seafile.return_value.get_content.return_value = content
        self.namespace['fs_mgr'] = storage
        extractor = self.namespace['Extractor'](self.namespace['extract_docx_text'])
        self.assertIsNone(extractor.extract('repo', 1, 'file-id', '/large.docx'))
        self.namespace['logger'].warning.assert_called_once()


if __name__ == '__main__':
    unittest.main()
