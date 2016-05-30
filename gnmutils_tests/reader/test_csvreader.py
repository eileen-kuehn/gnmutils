import unittest
import os
import gnmutils_tests

from gnmutils.reader.csvreader import CSVReader
from gnmutils.parser.jobparser import JobParser


class TestCSVReader(unittest.TestCase):
    def test_correct_order(self):
        # TODO: datasource should default to FileDatasource
        parser = JobParser()  # TODO: I guess I still need to set the correct data source
        reader = CSVReader()
        reader.parser = parser
        for index, data in enumerate(parser.parse(path=self._file_path())):
            self.assertIsNotNone(data)
        self.assertEqual(index, 0)

        # TODO: enumerate job to check correct order of processes
        tree = data.tree
        self.assertIsNotNone(tree)
        self.assertEqual(tree.getVertexCount(), 9109)

        for node, depth in tree.walkDFS():
            # check children
            last_child = None
            if len(node.children) > 0:
                for child in node.children:
                    if last_child:
                        self.assertTrue(last_child.value.tme <= child.value.tme,
                                        "TME (%d) is not smaller than previous TME (%d)" %
                                        (child.value.tme, last_child.value.tme))
                    else:
                        self.assertTrue(node.value.tme <= child.value.tme,
                                        "TME of parent (%d) is not smaller than this of child (%d)" %
                                        (node.value.tme, child.value.tme))
                        last_child = child

    def _file_path(self):
        return os.path.join(
            os.path.dirname(gnmutils_tests.__file__),
            "data/c00-001-001/1/1-process.csv"
        )
