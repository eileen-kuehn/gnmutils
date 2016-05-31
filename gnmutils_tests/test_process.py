import unittest

from gnmutils.objects.process import Process


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
        row_start = ["1406555483", "9939", "9881", "0", "(sge_shepherd)", "sge_shepherd-5073566", "0", ".", "9939"]
        row_valid_start = ["1406555483", "9939", "9881", "0", "(sge_shepherd)", "sge_shepherd-5073566", "0", "fork", "9939"]
        row_end = ["1406556210", "9939", "9881", "0", "(sge_shepherd)", "sge_shepherd-5073566", "0", "exit", "9939"]
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

    def test_process_from_row(self):
        row_string = "1405011331,1405065581,30726,7733,30726,0,(sge_shepherd)," \
                     "sge_shepherd-4165419,0,0,1,,,,,0,,,exit"
        row_header = "tme,exit_tme,pid,ppid,gpid,uid,name,cmd,error_code,signal,valid," \
                     "int_in_volume,int_out_volume,ext_in_volume,ext_out_volume,tree_depth," \
                     "process_type,color,state"
        process = Process.process_from_row(dict(zip(row_header.split(","), row_string.split(","))))
        self.assertIsNotNone(process)
        self.assertEqual(process.getDuration(), 54250)
        self.assertEqual(process.getHeader(), row_header)
        self.assertEqual(process.getRow(), row_string)

        row_string = "1405011331,,30726,7733,30726,0,(sge_shepherd)," \
                     "sge_shepherd-4165419,0,0,1,,,,,0,,,exit"
        process = Process.process_from_row(dict(zip(row_header.split(","), row_string.split(","))))
        self.assertIsNotNone(process)
        self.assertEqual(process.getDuration(), 0)
        self.assertEqual(process.getHeader(), row_header)
        self.assertEqual(process.getRow(), '1405011331,0,30726,7733,30726,0,(sge_shepherd),sge_shepherd-4165419,0,0,1,,,,,0,,,exit')
