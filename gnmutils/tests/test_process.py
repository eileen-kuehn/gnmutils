import unittest

from gnmutils.process import Process


class TestProcessFunctions(unittest.TestCase):
    def setUp(self):
        self.process = Process()
        self.header = {"tme": 0,
                       "pid": 1,
                       "ppid": 2,
                       "uid": 3,
                       "name": 4,
                       "cmd": 5,
                       "exit_code": 6,
                       "state": 7,
                       "gpid": 8}

    def test_setUp(self):
        self.assertEqual(False, self.process.valid)

    def test_process_events(self):
        row_start = ["1406555483","9939","9881","0","(sge_shepherd)","sge_shepherd-5073566","0",".","9939"]
        row_valid_start = ["1406555483","9939","9881","0","(sge_shepherd)","sge_shepherd-5073566","0","fork","9939"]
        row_end = ["1406556210","9939","9881","0","(sge_shepherd)","sge_shepherd-5073566","0","exit","9939"]
        data_start_dict = {}
        data_valid_start_dict = {}
        data_end_dict = {}
        for key in self.header:
            data_start_dict[key] = row_start[self.header[key]]
            data_valid_start_dict[key] = row_valid_start[self.header[key]]
            data_end_dict[key] = row_end[self.header[key]]

        self.process.addProcessEvent(**data_start_dict)
        self.process.addProcessEvent(**data_end_dict)
        self.assertEqual(False, self.process.valid)

        process = Process()
        process.addProcessEvent(**data_end_dict)
        process.addProcessEvent(**data_start_dict)
        self.assertEqual(False, process.valid)

        valid_process = Process()
        valid_process.addProcessEvent(**data_valid_start_dict)
        valid_process.addProcessEvent(**data_end_dict)
        self.assertEqual(True, valid_process.valid)

        valid_process2 = Process()
        valid_process2.addProcessEvent(**data_end_dict)
        valid_process2.addProcessEvent(**data_valid_start_dict)
        self.assertEqual(True, valid_process2.valid)

