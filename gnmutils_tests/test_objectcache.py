import unittest

from gnmutils.objectcache import ObjectCache
from gnmutils.process import Process


class TestObjectCacheFunctions(unittest.TestCase):
    def setUp(self):
        self.objectCache = ObjectCache()

    def test_setUp(self):
        self.assertEqual(len(self.objectCache.objectCache), 0, "object cache not empty")
        self.assertEqual(len(self.objectCache.faultyNodes), 0, "object cache not empty")
        self.assertEqual(len(self.objectCache.unfound), 0, "object cache not empty")

    def test_insertRemove(self):
        process = Process(tme=1, pid=2)
        process2 = Process(tme=2, pid=2)
        process3 = Process(tme=0, pid=2)
        process4 = Process(tme=0, pid=3)

        self.assertEqual(len(self.objectCache.objectCache), 0, "object cache not empty")
        self.objectCache.addObject(process)
        self.assertEqual(len(self.objectCache.objectCache), 1,
                         "object cache should contain one process")

        loadedProcess = self.objectCache.getObject(tme=process.tme, pid=process.pid)
        self.assertIsNotNone(loadedProcess, "No object loaded from cache")
        self.assertEqual(process, loadedProcess, "objects should be identical")
        self.objectCache.removeObject(object=process)
        self.assertEqual(len(self.objectCache.objectCache), 0, "object cache not empty")

        self.objectCache.addObject(process)
        self.objectCache.addObject(process2)
        self.objectCache.addObject(process3)
        self.objectCache.addObject(process4)
        self.assertEqual(len(self.objectCache.objectCache), 2,
                         "object cache should contain two different categories")
        loadedProcess = self.objectCache.getObject(tme=process2.tme, pid=process2.pid)
        self.assertEqual(process2, loadedProcess, "objects should be identical")
        loadedProcess = self.objectCache.getObject(tme=process3.tme, pid=process3.pid)
        self.assertEqual(process3, loadedProcess, "objects should be identical")
        loadedProcess = self.objectCache.getObject(tme=process.tme, pid=process.pid)
        self.assertEqual(process, loadedProcess, "objects should be identical")
        loadedProcess = self.objectCache.getObject(tme=process4.tme, pid=process4.pid)
        self.assertEqual(process4, loadedProcess, "objects should be identical")

    def test_removeObject(self):
        process = Process(tme=1, pid=2)
        process2 = Process(tme=2, pid=2)
        process3 = Process(tme=0, pid=2)
        process4 = Process(tme=0, pid=3)

        self.objectCache.addObject(process)

        self.assertEqual(len(self.objectCache.objectCache), 1, "object cache should not be empty")
        self.objectCache.removeObject(object=process)
        self.assertEqual(len(self.objectCache.objectCache), 0, "object cache should be empty")

        self.objectCache.addObject(process2)

    def test_clear(self):
        process = Process(tme=1, pid=2)
        process2 = Process(tme=2, pid=2)
        process3 = Process(tme=0, pid=2)
        process4 = Process(tme=0, pid=3)

        self.objectCache.addObject(process)
        self.objectCache.addObject(process2)
        self.objectCache.addObject(process3)
        self.objectCache.addObject(process4)

        self.assertEqual(len(self.objectCache.objectCache), 2,
                         "object cache should contain two different categories")
        self.assertEqual(len(self.objectCache.faultyNodes), 0,
                         "object cache should not have faulty nodes")
        self.assertEqual(len(self.objectCache.unfound), 0,
                         "object cache should not have unfound nodes")

        self.objectCache.unfound.add(process)
        self.objectCache.clear()

        self.assertEqual(len(self.objectCache.objectCache), 0, "object cache should be empty")
        self.assertEqual(len(self.objectCache.faultyNodes), 0, "faulty nodes should be empty")
        self.assertEqual(len(self.objectCache.unfound), 0, "unfound should be empty")

    def test_update(self):
        process = Process(tme=1, pid=2)
        self.objectCache.addObject(process)

        theProcess = self.objectCache.getObject(tme=process.tme, pid=process.pid)
        theProcess.name = "test"
        newProcess = self.objectCache.getObject(tme=process.tme, pid=process.pid)
        self.assertEqual("test", newProcess.name, "name is not identical")

    def test_updateIndex(self):
        process = Process(tme=1, pid=2, name="old")
        process2 = Process(tme=1, pid=2, name="new")

        self.objectCache.addObject(object=process)

        index = self.objectCache.getObjectIndex(tme=process.tme, pid=process.pid)
        self.objectCache.objectCache[process.pid][index] = process2

        newProcess = self.objectCache.getObject(tme=process.tme, pid=process.pid)
        self.assertEqual(process2.name, newProcess.name)

    def test_getNullObject(self):
        object = self.objectCache.getObject(tme=1, pid=1)
        self.assertIsNone(object, "object cache did not return None")


if __name__ == '__main__':
    unittest.main()
