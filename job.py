from gnmutils.objectcache import ObjectCache
from gnmutils.monitoringconfiguration import MonitoringConfiguration
from evenmoreutils.tree import Tree, Node

from exceptions import *


class Job(object):
    """
    This class acts as a wrapper for the different processes forming a batch system job.
    It allows access to the job tree.
    """
    def __init__(self, db_id=None, job_id=None, workernode=None, run=None, tme=None, gpid=None, configuration=None,
                 last_tme=None):
        self._db_id = db_id
        self._job_id = job_id
        self._workernode = workernode
        self._run = run
        self._tme = (tme or 0)
        self._gpid = (gpid or 0)
        self._root = None
        self._process_cache = ObjectCache()
        self._tree_initialized = False
        self._configuration = configuration
        self._last_tme = last_tme
        self._tree = None

    def clear_caches(self):
        self._root = None
        self._process_cache.clear()
        self._tree_initialized = False

    @property
    def last_tme(self):
        return self._last_tme or self.exit_tme

    @property
    def configuration(self):
        if self._configuration is None:
            return MonitoringConfiguration(version="alpha", level="treeconnection")
        return self._configuration

    @configuration.setter
    def configuration(self, value=None):
        self._configuration = value

    @property
    def workernode(self):
        return self._workernode

    @workernode.setter
    def workernode(self, workernode=None):
        self._workernode = workernode

    @property
    def run(self):
        return self._run

    @run.setter
    def run(self, run=None):
        self._run = run

    @property
    def db_id(self):
        return self._db_id or self.job_id

    @db_id.setter
    def db_id(self, value=None):
        self._db_id = value

    @property
    def job_id(self):
        try:
            return self._root.value.batchsystemId
        except:
            return self._job_id

    @job_id.setter
    def job_id(self, job_id=None):
        self._job_id = job_id

    @property
    def gpid(self):
        try:
            return self._root.value.gpid
        except:
            return self._gpid

    @property
    def uid(self):
        process_cache = self._process_cache.objectCache
        for pid in process_cache:
            for node in process_cache[pid]:
                if node.value.uid > 0:
                    return node.value.uid
        return 0

    @property
    def tme(self):
        try:
            return self._root.value.tme
        except:
            return self._tme

    @property
    def exit_tme(self):
        try:
            return self._root.value.exit_tme
        except AttributeError:
            return None

    @property
    def exit_code(self):
        return self._root.value.exit_code

    @property
    def tree(self):
        return self._get_tree()

    @property
    def process_cache(self):
        return self._process_cache.objectCache

    @property
    def faulty_nodes(self):
        return self._process_cache.faultyNodes

    def regenerate_tree(self):
        return self._get_tree(reinitialize=True)

    def add_node_object(self, node=None, is_root=False):
        self._add(node=node, is_root=is_root)

    def add_process(self, process=None, is_root=False):
        node = Node(value=process)
        self._add(node=node, is_root=is_root)

    def is_valid(self):
        if len(self._process_cache.faultyNodes) > 1 or self._root is None:
            return False
        process_cache = self._process_cache.objectCache
        for pid in process_cache:
            for node in process_cache[pid]:
                if not node.value.valid:
                    return False
        return True

    def is_complete(self):
        tree = self.tree
        return tree is not None

    def processes(self):
        tree = self.tree
        if tree is not None:
            for node, depth in tree.walkDFS():
                yield node.value
        else:
            logging.getLogger(self.__class__.__name__).warning("There is no tree for current job")
            process_cache = self._process_cache.objectCache
            for pid in process_cache:
                for node in process_cache[pid]:
                    yield node.value

    def process_count(self):
        count = 0
        process_cache = self._process_cache.objectCache
        for pid in process_cache:
            count += len(process_cache[pid])
        return count

    @staticmethod
    def default_header(length=15):
        """
        Returns the header for CSV output in dictionary format.

        :param length: Number of elements being expected
        :return: Dictionary of keys and their positions
        """
        return {"tme": 0, "exit_tme": 1, "pid": 2, "ppid": 3, "gpid": 4,
                "uid": 5, "name": 6, "cmd": 7, "error_code": 8, "signal": 9,
                "valid": 10, "int_in_volume": 11, "int_out_volume": 12,
                "ext_in_volume": 13, "ext_out_volume": 14}

    def _add(self, node=None, is_root=False):
        if "sge_shepherd" in node.value.cmd or is_root:
            if self._root is not None:
                raise NonUniqueRootException
            self._root = node
        if node.value.exit_tme > self._last_tme:
            self._last_tme = node.value.exit_tme
        self._process_cache.addNodeObject(node)

    def _get_tree(self, reinitialize=False):
        if reinitialize or not self._tree_initialized:
            if reinitialize:
                self._tree = None
                self._process_cache.faultyNodes = set()
                for pid in self._process_cache.objectCache:
                    for node in self._process_cache.objectCache[pid]:
                        node.children = []
            self._initialize_tree()
            self._tree_initialized = True
        if self._tree is None:
            if (len(self._process_cache.faultyNodes) <= 1 and self._root and
                    (Tree(self._root).getVertexCount() == self.process_count())):
                self._tree = Tree(self._root)
        logging.getLogger(self.__class__.__name__).info("faulty nodes: %s" % self._process_cache.faultyNodes)
        return self._tree

    def _initialize_tree(self):
        logging.getLogger(self.__class__.__name__).info("Initializing tree structure")
        process_cache = self._process_cache.objectCache
        # sort the keys first to get the correct ordering in the final tree
        for pid in sorted(process_cache.keys(), key=lambda item: int(item)):
            for node in process_cache[pid]:
                parent = self._process_cache.getNodeObject(tme=node.value.tme,
                                                           pid=node.value.ppid,
                                                           rememberError=True)
                if parent:
                    parent.add(node)
        logging.getLogger(self.__class__.__name__).info("no parents found for %d nodes" % (
            len(self._process_cache.faultyNodes)
        ))

        if len(self._process_cache.faultyNodes) <= 1 and self._root:
            # set depth
            for node, depth in Tree(self._root).walkDFS():
                node.value.tree_depth = depth

    def __repr__(self):
        return "%s: db_id (%s), job_id (%s), gpid (%d), workernode (%s), configuration (%s), run (%s), tme (%d), root (%s), " \
               "process_cache (%s), tree_initialized (%s)" % (
            self.__class__.__name__, self.db_id, self.job_id, self.gpid, self.workernode, self.configuration, self.run,
            self.tme, (self._root and self._root.value), self._process_cache, self._tree_initialized
        )