import unittest

from gnmutils.parser.dataparser import DataParser
from gnmutils.sources.filedatasource import FileDataSource
from gnmutils.exceptions import ParserNotInitializedException, FilePathException


class TestDataParser(unittest.TestCase):
    def test_creation(self):
        parser = DataParser()
        self.assertIsNotNone(parser)
        self.assertIsNone(parser.data_source)
        self.assertIsNone(parser.data_reader)

    def test_data_id(self):
        parser = DataParser()
        self.assertRaises(NotImplementedError, parser.data_id, 1)

    def test_parsed_data(self):
        parser = DataParser()
        self.assertEqual(len(parser.parsed_data), 0)

    def test_data(self):
        parser = DataParser()
        self.assertIsNone(parser.data)

    def test_check_caches(self):
        parser = DataParser()
        self.assertRaises(NotImplementedError, parser.check_caches)

    def test_clear_caches(self):
        parser = DataParser()
        self.assertRaises(NotImplementedError, parser.clear_caches)

    def test_piece(self):
        parser = DataParser()
        self.assertRaises(ParserNotInitializedException, parser.add_piece, piece=None)
        self.assertRaises(NotImplementedError, parser._piece_from_dict, piece=None)

    def test_archiving(self):
        parser = DataParser()
        self.assertRaises(FilePathException, parser.load_archive_state, path=None)
        # TODO: add writing of archive

    def test_pop(self):
        parser = DataParser()
        self.assertRaises(NotImplementedError, parser.pop_data)

    def test_parsing_finished(self):
        parser = DataParser()
        self.assertIsNone(parser._parsing_finished().next())
