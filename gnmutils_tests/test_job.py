import unittest

from gnmutils.job import Job
from gnmutils.objects.process import Process


class TestJobFunctions(unittest.TestCase):
    def setUp(self):
        self.job = Job()

    def test_setUp(self):
        self.assertEqual(False, self.job.is_complete())

    def test_complete_job(self):
        process = Process(name="sge_shepherd", cmd="sge_shepherd", pid="1", ppid="0", gpid="1")
        process2 = Process(name="test", pid="2", ppid="1", gpid="1")

        self.job.add_process(process)
        self.assertEqual(True, self.job.is_complete())

        job2 = Job()
        job2.add_process(process2)
        job2.add_process(process)
        self.assertEqual(True, job2.is_complete())

    def test_incomplete_job(self):
        process_root = Process(name="sge_shepherd", cmd="sge_shepherd", pid="3", ppid="0", gpid=3)
        process = Process(name="test", pid="1", ppid="0", gpid="3")
        process2 = Process(name="test2", pid="2", ppid="1", gpid="3")

        self.job.add_process(process=process)
        self.job.add_process(process=process2)
        self.assertEqual(False, self.job.is_complete())

        job2 = Job()
        job2.add_process(process=process2)
        job2.add_process(process=process_root)
        self.assertEqual(False, job2.is_complete())
