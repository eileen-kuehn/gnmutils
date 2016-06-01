import unittest

from gnmutils.objects.process import Process
from gnmutils.exceptions import ArgumentNotDefinedException, ProcessMismatchException


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
        self.assertEqual(self.process.pid, 0)
        self.assertEqual(self.process.ppid, 0)
        self.assertEqual(self.process.gpid, 0)
        self.assertEqual(self.process.uid, 0)
        self.assertEqual(self.process.tme, 0)
        self.assertEqual(self.process.traffic, [])

    def test_exit_code(self):
        process = Process(exit_code=1)
        self.assertEqual(process.exit_code, 0)  # TODO: why is here an output at all?
        self.assertEqual(process.error_code, 0)
        self.assertEqual(process.signal, 1)
        process = Process(exit_code="256")
        self.assertEqual(process.error_code, 1)
        self.assertEqual(process.signal, 0)
        process = Process(exit_code=257)
        self.assertEqual(process.error_code, 1)
        self.assertEqual(process.signal, 1)

    def test_process_events(self):
        row_start = ["1406555483", "9939", "9881", "0", "(sge_shepherd)", "sge_shepherd-5073566", "0", ".", "9939"]
        row_valid_start = ["1406555483", "9939", "9881", "0", "(sge_shepherd)", "sge_shepherd-5073566", "0", "fork", "9939"]
        row_end = ["1406556210", "9940", "9881", "0", "(sge_shephrd)", "sge_shepherd-5073567", "0", "exit", "9939"]
        row_valid_end = ["1406556210", "9939", "9881", "0", "(sge_shepherd)", "sge_shepherd-5073566", "0", "exit", "9939"]
        data_start_dict = {}
        data_valid_start_dict = {}
        data_end_dict = {}
        data_valid_end_dict = {}
        for key in self.header:
            data_start_dict[key] = row_start[self.header[key]]
            data_valid_start_dict[key] = row_valid_start[self.header[key]]
            data_end_dict[key] = row_end[self.header[key]]
            data_valid_end_dict[key] = row_valid_end[self.header[key]]

        self.process.addProcessEvent(**data_start_dict)
        self.process.addProcessEvent(**data_valid_end_dict)
        self.assertEqual(False, self.process.valid)

        process = Process()
        process.addProcessEvent(**data_valid_end_dict)
        self.assertEqual(process.name, data_valid_end_dict["name"])
        process.addProcessEvent(**data_start_dict)
        self.assertEqual(False, process.valid)

        valid_process = Process()
        valid_process.addProcessEvent(**data_valid_start_dict)
        valid_process.addProcessEvent(**data_valid_end_dict)
        self.assertEqual(True, valid_process.valid)

        valid_process2 = Process()
        valid_process2.addProcessEvent(**data_valid_end_dict)
        valid_process2.addProcessEvent(**data_valid_start_dict)
        self.assertEqual(True, valid_process2.valid)

        process = Process()
        process.addProcessEvent(**data_valid_start_dict)
        self.assertRaises(ProcessMismatchException, process.addProcessEvent, **data_end_dict)

    def test_event_conversion(self):
        row_string = "1405011331,1405065581,30726,7733,30726,0,(sge_shepherd)," \
                     "sge_shepherd-4165419,0,0,1,,,,,0,,,exit"
        row_header = "tme,exit_tme,pid,ppid,gpid,uid,name,cmd,error_code,signal,valid," \
                     "int_in_volume,int_out_volume,ext_in_volume,ext_out_volume,tree_depth," \
                     "process_type,color,state"
        process = Process(**dict(zip(row_header.split(","), row_string.split(","))))
        self.assertEqual(process.toProcessEvent(),
                         {
                             "tme": 1405065581,
                             "name": "(sge_shepherd)",
                             "cmd": "sge_shepherd-4165419",
                             "pid": 30726,
                             "ppid": 7733,
                             "uid": 0,
                             "gpid": 30726,
                             "state": "exit",
                             "exit_code": 0
                         })

        row_string = "1405011331,,30726,7733,30726,0,(sge_shepherd)," \
                     "sge_shepherd-4165419,0,0,1,,,,,0,,,."
        process = Process(**dict(zip(row_header.split(","), row_string.split(","))))
        self.assertEqual(process.toProcessEvent(),
                         {
                             "tme": 1405011331,
                             "name": "(sge_shepherd)",
                             "cmd": "sge_shepherd-4165419",
                             "pid": 30726,
                             "ppid": 7733,
                             "uid": 0,
                             "gpid": 30726,
                             "state": ".",
                         })

    def test_process_from_row(self):
        row_string = "1405011331,1405065581,30726,7733,30726,0,(sge_shepherd)," \
                     "sge_shepherd-4165419,1,1,1,,,,,0,,,exit"
        row_header = "tme,exit_tme,pid,ppid,gpid,uid,name,cmd,error_code,signal,valid," \
                     "int_in_volume,int_out_volume,ext_in_volume,ext_out_volume,tree_depth," \
                     "process_type,color,state"
        process = Process.from_dict(dict(zip(row_header.split(","), row_string.split(","))))
        self.assertIsNotNone(process)
        self.assertEqual(process.getDuration(), 54250)
        self.assertEqual(process.getHeader(), row_header)
        self.assertEqual(process.getRow(), row_string)
        self.assertEqual(process.error_code, 1)
        self.assertEqual(process.signal, 1)
        self.assertEqual(process.exit_code, 257)

        row_string = "1405011331,,30726,7733,30726,0,(sge_shepherd)," \
                     "sge_shepherd-4165419,0,0,1,,,,,0,,,exit"
        process = Process.from_dict(dict(zip(row_header.split(","), row_string.split(","))))
        self.assertIsNotNone(process)
        self.assertEqual(process.getDuration(), 0)
        self.assertEqual(process.getHeader(), row_header)
        self.assertEqual(process.getRow(), '1405011331,0,30726,7733,30726,0,(sge_shepherd),sge_shepherd-4165419,0,0,1,,,,,0,,,exit')

        row_string = "1405011331,1405065581,30726,7733,30726,0,(sge_shepherd)," \
                     "sge_shepherd-4165419,,,1,,,,,0,,,exit"
        process = Process.from_dict(dict(zip(row_header.split(","), row_string.split(","))))
        self.assertEqual(process.signal, 0)
        self.assertEqual(process.error_code, 0)

        row_string = "1405011331,1405065581,30726,7733,30726,0,(sge_shepherd)," \
                     "sge_shepherd-4165419,sth,,1,,,,,0,,,exit"
        self.assertRaises(ValueError, Process.from_dict, dict(zip(row_header.split(","), row_string.split(","))))

        row_string = "1405011331,1405065581,30726,7733,30726,0,(sge_shepherd)," \
                     "sge_shepherd-4165419,,,1,,,,,0,,,exit"
        row_header = "tme,exit_tme,pid,ppid,gpid,uid,name,cmd,error_code,signal,valid," \
                     "int_in_volume,int_out_volume,ext_in_volume,ext_out_volume,tree_depth," \
                     "process_type,color,states"
        self.assertRaises(ArgumentNotDefinedException, Process.from_dict, dict(zip(row_header.split(","), row_string.split(","))))

    def test_traffic(self):
        # TODO: test traffic stuff
        pass

    def test_batchsystem_id(self):
        row_string = "1405011331,,30726,7733,30726,0,(sge_shepherd)," \
                     "sge_shepherd-4165419,0,0,1,,,,,0,,,exit"
        row_header = "tme,exit_tme,pid,ppid,gpid,uid,name,cmd,error_code,signal,valid," \
                     "int_in_volume,int_out_volume,ext_in_volume,ext_out_volume,tree_depth," \
                     "process_type,color,state"
        process = Process(**dict(zip(row_header.split(","), row_string.split(","))))
        self.assertEqual("sge_shepherd-4165419", process.cmd)
        self.assertEqual(4165419, process.batchsystemId)

        row_string = "1405011331,,30726,7733,30726,0,(sge_shepherd)," \
                     ",0,0,1,,,,,0,,,exit"
        process = Process(**dict(zip(row_header.split(","), row_string.split(","))))
        self.assertIsNone(process.batchsystemId)

    def test_repr(self):
        row_string = "1405011331,,30726,7733,30726,0,(sge_shepherd)," \
                     "sge_shepherd-4165419,0,0,1,,,,,0,,,exit"
        row_header = "tme,exit_tme,pid,ppid,gpid,uid,name,cmd,error_code,signal,valid," \
                     "int_in_volume,int_out_volume,ext_in_volume,ext_out_volume,tree_depth," \
                     "process_type,color,state"
        process = Process(**dict(zip(row_header.split(","), row_string.split(","))))
        self.assertEqual("Process: name ((sge_shepherd)), cmd (sge_shepherd-4165419), pid (30726), "
                         "ppid (7733), uid (0), gpid (30726), valid (1), tme (1405011331), "
                         "exit_tme (0), state (exit), error_code (0), signal (0), job_id (None), "
                         "tree_depth (0), process_type (), color (), int_in_volume (), "
                         "int_out_volume (), ext_in_volume (), ext_out_volume ()",
                         process.__repr__())

