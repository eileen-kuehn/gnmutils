import unittest
import os
import gnmutils_tests

from gnmutils.objects.job import Job
from gnmutils.objects.process import Process
from gnmutils.exceptions import NoDataSourceException, FilePathException
from gnmutils.sources.filedatasource import FileDataSource
from gnmutils.parser.jobparser import JobParser
from gnmutils.reader.csvreader import CSVReader
from gnmutils.monitoringconfiguration import MonitoringConfiguration


class TestJobFunctions(unittest.TestCase):
    def setUp(self):
        self.empty_job = Job()

    def test_setUp(self):
        self.assertEqual(False, self.empty_job.is_complete())

    def test_complete_job(self):
        process = Process(name="sge_shepherd", cmd="sge_shepherd", pid="1", ppid="0", gpid="1")
        process2 = Process(name="test", pid="2", ppid="1", gpid="1")

        self.empty_job.add_process(process)
        self.assertEqual(True, self.empty_job.is_complete())

        job2 = Job()
        job2.add_process(process2)
        job2.add_process(process)
        self.assertEqual(True, job2.is_complete())

    def test_incomplete_job(self):
        process_root = Process(name="sge_shepherd", cmd="sge_shepherd", pid="3", ppid="0", gpid=3)
        process = Process(name="test", pid="1", ppid="0", gpid="3")
        process2 = Process(name="test2", pid="2", ppid="1", gpid="3")

        self.empty_job.add_process(process=process)
        self.empty_job.add_process(process=process2)
        self.assertEqual(False, self.empty_job.is_complete())

        job2 = Job()
        job2.add_process(process=process2)
        job2.add_process(process=process_root)
        self.assertEqual(False, job2.is_complete())

    def test_cache_deletion(self):
        job = Job()
        process_root = Process(name="sge_shepherd", cmd="sge_shepherd", pid="3", ppid="0", gpid=3)
        job.add_process(process=process_root)
        self.assertEqual(1, job.process_count())
        job.clear_caches()
        self.assertEqual(0, job.process_count())

    def test_prepare_traffic(self):
        job = Job()
        self.assertRaises(NoDataSourceException, job.prepare_traffic)

        job = Job(data_source=FileDataSource())
        self.assertRaises(FilePathException, job.prepare_traffic)

        data_source = FileDataSource()
        parser = JobParser(data_source=data_source)
        reader = CSVReader()
        reader.parser = parser
        for job in parser.parse(path=os.path.join(
                os.path.dirname(gnmutils_tests.__file__),
                "data/c00-001-001/1/1-process.csv")):
            job.prepare_traffic()
        count = 0
        for process in job.processes():
            count += len(process.traffic)
        self.assertEqual(count, 3155)
        self.assertEqual(job.db_id, "1")
        self.assertEqual(job.job_id, 4165419)
        self.assertEqual(job.gpid, 30726)
        self.assertEqual(job.uid, 14808)
        self.assertEqual(job.tme, 1405011331)
        self.assertEqual(job.exit_tme, 1405065581)
        self.assertEqual(job.exit_code, 0)
        self.assertEqual(len(job.faulty_nodes), 1)
        job.regenerate_tree()

    def test_last_tme(self):
        self.assertIsNone(self.empty_job.last_tme)
        self.empty_job.add_process(process=Process(tme=1, exit_tme=3, name="sge_shepherd", cmd="sge_shepherd", pid="3", ppid="0", gpid=3))
        self.assertEqual(self.empty_job.last_tme, 3)
        self.empty_job.add_process(process=Process(name="test", pid="1", ppid="0", gpid="3", tme=2, exit_tme=4))
        self.assertEqual(self.empty_job.last_tme, 4)

    def test_configuration(self):
        self.assertEqual(self.empty_job.configuration, MonitoringConfiguration(version="alpha", level="treeconnection"))

        job = Job(configuration=MonitoringConfiguration(version="beta"))
        self.assertNotEqual(job.configuration, self.empty_job.configuration)

        job.job_id = "1"
        self.assertEqual(job.job_id, "1")

    def test_header(self):
        self.assertEqual(Job.default_header(), {'int_out_volume': 12,
                                                'ext_out_volume': 14,
                                                'uid': 5,
                                                'pid': 2,
                                                'int_in_volume': 11,
                                                'gpid': 4,
                                                'exit_tme': 1,
                                                'name': 6,
                                                'ext_in_volume': 13,
                                                'signal': 9,
                                                'cmd': 7,
                                                'valid': 10,
                                                'tme': 0,
                                                'ppid': 3,
                                                'error_code': 8})
        self.assertEqual(Job.default_header(length=9), {"tme": 0,
                                                        "pid": 1,
                                                        "ppid": 2,
                                                        "uid": 3,
                                                        "name": 4,
                                                        "cmd": 5,
                                                        "exit_code": 6,
                                                        "state": 7,
                                                        "gpid": 8})

    def test_processes(self):
        data_source = FileDataSource()
        for job in data_source.jobs(path=self._file_path()):
            for element in job:
                tree = element.tree
                count = 0
                for node, depth in tree.walkDFS():
                    count += 1
                    # check pid order of children
                    initial = 0
                    last_tme = 0
                    for process in node.children:
                        self.assertTrue(
                            process.value.pid >= initial or (process.value.pid < initial and last_tme < process.value.tme), "%d: initial %d differs %d (%s)" %(count, initial, process.value.pid, [(child.value.pid, child.value.tme) for child in node.children]))
                        initial = process.value.pid
                        last_tme = process.value.tme

    def test_processes_in_order(self):
        data_source = FileDataSource()
        for job in data_source.jobs(path=self._file_path()):
            last_tme = 0
            last_pid = 0
            for process in job.processes_in_order():
                self.assertTrue(process.tme >= last_tme)
                if last_tme == process.tme:
                    # also check for pid
                    self.assertTrue(process.pid > last_pid or ((last_pid + process.pid) % 32768 < 500), "%d vs %d" % (last_pid, process.pid))
                    last_pid = process.pid
                else:
                    last_pid = 0
                last_tme = process.tme

    def _file_path(self):
        return os.path.join(
            os.path.dirname(gnmutils_tests.__file__),
            "data/c00-001-001/1/1-process.csv"
        )
