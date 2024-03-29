import unittest

from gnmutils.parser.jobparser import JobParser
from gnmutils.objects.process import Process
from gnmutils.exceptions import *


class TestJobParserFunctions(unittest.TestCase):
    def setUp(self):
        self.jobParser = JobParser()

    def test_setUp(self):
        self.assertFalse(self.jobParser.data.is_valid(), "JobParser should return False")
        self.assertEqual(0, self.jobParser.data.process_count(), 'Job should contain 0 processes')
        self.assertEqual(None, self.jobParser.data.tree, "Tree should be None")
        
    def test_oneNodeWithoutRoot(self):
        process = Process(tme=1, pid=2, ppid=1, valid=1)
        self.jobParser.add_piece(piece=process)
        self.assertFalse(self.jobParser.data.is_valid(), "JobParser should return False")
        self.assertEqual(None, self.jobParser.data.tree, "Tree should be None")
        
    def test_oneNodeWithRoot(self):
        process = Process(tme=1, pid=2, ppid=1, name="sge_shepherd", cmd="sge_shepherd", valid=1)
        self.jobParser.add_piece(piece=process)
        self.assertTrue(self.jobParser.data.is_valid(), "JobParser should return True")
        self.assertIsNotNone(self.jobParser.data.tree, "Tree should not be None")
        
    def test_twoRootedTrees(self):
        tree1_process1 = Process(tme=1, pid=1, ppid=0, name="sge_shepherd", cmd="sge_shepherd", valid=1)
        tree1_process2 = Process(tme=1, pid=2, ppid=1, valid=1)
        tree1_process3 = Process(tme=1, pid=3, ppid=1, valid=1)
        tree1_process4 = Process(tme=1, pid=4, ppid=2, valid=1)
        
        tree2_process1 = Process(tme=1, pid=10, ppid=0, name="sge_shepherd", cmd="sge_shepherd", valid=1)
        tree2_process2 = Process(tme=1, pid=11, ppid=10, valid=1)
        tree2_process3 = Process(tme=1, pid=12, ppid=10, valid=1)
        
        self.jobParser.add_piece(piece=tree1_process1)
        self.jobParser.add_piece(piece=tree1_process2)
        self.jobParser.add_piece(piece=tree1_process3)
        self.jobParser.add_piece(piece=tree1_process4)
        
        self.assertRaises(NonUniqueRootException, lambda : self.jobParser.add_piece(piece=tree2_process1))
        self.jobParser.add_piece(piece=tree2_process2)
        self.jobParser.add_piece(piece=tree2_process3)
        
        self.assertIsNone(self.jobParser.data.tree, "JobParser should not return tree")
        
    def test_twoTrees(self):
        tree1_process1 = Process(tme=1, pid=1, ppid=0, name="sge_shepherd", cmd="sge_shepherd", valid=1)
        tree1_process2 = Process(tme=1, pid=2, ppid=1, valid=1)
        tree1_process3 = Process(tme=1, pid=3, ppid=1, valid=1)
        tree1_process4 = Process(tme=1, pid=4, ppid=2, valid=1)
        
        tree2_process1 = Process(tme=1, pid=10, ppid=0, valid=1)
        tree2_process2 = Process(tme=1, pid=11, ppid=10, valid=1)
        tree2_process3 = Process(tme=1, pid=12, ppid=10, valid=1)
        
        self.jobParser.add_piece(piece=tree1_process1)
        self.jobParser.add_piece(piece=tree1_process2)
        self.jobParser.add_piece(piece=tree1_process3)
        self.jobParser.add_piece(piece=tree1_process4)
        
        self.jobParser.add_piece(piece=tree2_process1)
        self.jobParser.add_piece(piece=tree2_process2)
        self.jobParser.add_piece(piece=tree2_process3)
        
        self.assertIsNone(self.jobParser.data.tree, "JobParser should not return tree")

if __name__ == '__main__':
    unittest.main()
