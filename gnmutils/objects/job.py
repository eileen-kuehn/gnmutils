import bisect
import logging

from gnmutils.objectcache import ObjectCache
from gnmutils.monitoringconfiguration import MonitoringConfiguration
from gnmutils.exceptions import NonUniqueRootException, DataNotInCacheException, \
    NoDataSourceException, FilePathException

from evenmoreutils.tree import Tree, Node


class Job(object):
    """
    This class acts as a wrapper for the different processes forming a batch system job.
    It allows access to the job tree.
    """
    def __init__(self, db_id=None, job_id=None, workernode=None, run=None, tme=None, gpid=None,
                 configuration=None, last_tme=None, **kwargs):
        self._db_id = db_id
        self._job_id = job_id
        self.workernode = workernode
        self.run = run
        self._tme = int(tme) if tme is not None else 0
        self._gpid = int(gpid) if gpid is not None else 0
        self._root = None
        self._process_cache = ObjectCache()
        self._tree_initialized = False
        self._configuration = configuration
        self._last_tme = last_tme
        self._tree = None
        # for lazy loading of traffic
        self.data_source = kwargs.get("data_source", None)
        self.path = kwargs.get("path", None)

    def clear_caches(self):
        self._root = None
        self._process_cache.clear()
        self._tree_initialized = False

    def prepare_traffic(self):
        try:
            traffic = self.data_source.read_traffic(path=self.path, name=self.db_id)
            for traffics in traffic:
                for element in traffics:
                    self.add_traffic(element)
        except AttributeError:
            raise NoDataSourceException
        except FilePathException:
            raise

    @property
    def last_tme(self):
        """
        Method to return the last known tme within the job.

        :return: last known tme
        """
        return self._last_tme or self.exit_tme

    @last_tme.setter
    def last_tme(self, value):
        """
        Method to set the last known tme.

        :param value: the last tme to set
        """
        self._last_tme = value

    @property
    def configuration(self):
        """
        Method to return the configuration the job was created/recorded with.

        :return: extracted configuration
        """
        if self._configuration is None:
            return MonitoringConfiguration(version="alpha", level="treeconnection")
        return self._configuration

    @configuration.setter
    def configuration(self, value=None):
        """
        Method to set the configuration the job was created/recorded with.

        :param value: the configuration to set
        """
        self._configuration = value

    @property
    def db_id(self):
        """
        Method to return the associated id of the job within the database.

        :return: job identifier in database
        """
        return self._db_id or self.job_id

    @db_id.setter
    def db_id(self, value=None):
        """
        Method to set the associated id of the job within the database.

        :param value: job identifier in database to set
        """
        self._db_id = value

    @property
    def job_id(self):
        """
        Method returns the job id. If a valid id from the batchsystem can be extracted, it is
        returned instead of a self-assigned one.

        :return: batchsystem job id or self-assigned
        """
        try:
            batchsystem_id = self._root.value.batchsystemId
            if batchsystem_id is not None:
                return batchsystem_id
            else:
                return self._job_id
        except:
            return self._job_id

    @job_id.setter
    def job_id(self, job_id=None):
        """
        Method to set a self-assigned job identifier.

        :param job_id: job identifier to be set
        """
        self._job_id = job_id

    @property
    def gpid(self):
        """
        Method that returns the associated group identifier of the job.

        :return: group identifier
        """
        try:
            return self._root.value.gpid
        except:
            return self._gpid

    @property
    def uid(self):
        """
        Method that returns the associated user identifier of the job. As the root process
        has uid 0, the first valid uid that is found in the process hierarchy is used.

        :return: first valid uid for job
        """
        process_cache = self._process_cache.object_cache
        for pid in process_cache:
            for node in process_cache[pid]:
                if node.value.uid > 0:
                    return node.value.uid
        return 0

    @property
    def tme(self):
        """
        Method that returns the tme when the job was started.

        :return: tme of the job
        """
        try:
            return self._root.value.tme
        except:
            return self._tme

    @property
    def exit_tme(self):
        """
        Method that returns the exit_tme when the job was finished.
        Attention: this does not necessarily have to be the last known tme!

        This method returns None if the job has not been finished so far.

        :return: Exit_tme or None if unfinished
        """
        try:
            return self._root.value.exit_tme
        except AttributeError:
            return None

    @property
    def exit_code(self):
        """
        Method returns the exit_code of the job.

        :return: exit_code of job
        """
        return self._root.value.exit_code

    @property
    def tree(self):
        """
        Method returns the assigned tree of the job.

        :return: process tree of job
        """
        return self._get_tree()

    @property
    def process_cache(self):
        """
        This method gives access to the actual process cache used for building the job.

        :return: process_cache
        """
        return self._process_cache.object_cache

    @property
    def faulty_nodes(self):
        """
        This method gives access to faulty nodes that have not correctly been assigned to the job.

        :return: faulty_nodes
        """
        return self._process_cache.faulty_nodes

    def regenerate_tree(self):
        """
        Method to re-generate the assigned tree. The actual tree is cached. If a change has been
        applied to the job after the tree was generated, it should be regenerated because it is
        internally cached.

        :return: re-generated process tree of job
        """
        return self._get_tree(reinitialize=True)

    def add_node_object(self, node=None, is_root=False):
        """
        Method adds a process that is already encapsulated into a node to the current job.

        :param node: node to be added
        :param is_root: is the process root of the tree
        """
        self._add(node=node, is_root=is_root)

    def add_process(self, process=None, is_root=False):
        """
        Method that adds a process to the current job.

        :param process: process to be added
        :param is_root: is the process root of the tree
        """
        node = Node(value=process)
        self._add(node=node, is_root=is_root)

    def add_traffic(self, traffic=None):
        """
        Method to add traffic to the current job.

        :param traffic: traffic to be added
        """
        process_node = self._process_cache.get_data(value=traffic.tme, key=traffic.pid)
        process_node.value.traffic.append(traffic)

    def is_valid(self):
        """
        Method that checks if the current job is valid. Therefore it validates that only for one
        process no parent was found (root process). It also checks that a root is existent at all.
        Method also recursively checks if all processes are valid. This means, each process needs
        a start and exit event.

        :return: true if job seems to be valid, false otherwise
        """
        if len(self._process_cache.faulty_nodes) > 1 or self._root is None:
            return False
        process_cache = self.process_cache
        for pid in process_cache:
            for node in process_cache[pid]:
                if not node.value.valid:
                    return False
        return True

    def is_complete(self):
        """
        Method that tells if the job is completed by checking if the actual process tree can be
        generated.
        Attention: true does not mean it is complete. There might still be some processes missing.

        :return: true if tree can be generated, false otherwise
        """
        tree = self.tree
        return tree is not None

    def processes(self):
        """
        Generator that returns processes of the job in depth first order.

        :return: process generator of the job
        """
        tree = self.tree
        if tree is not None:
            for node, _ in tree.walkDFS():
                yield node.value
        else:
            logging.getLogger(self.__class__.__name__).warning("There is no tree for current job")
            process_cache = self.process_cache
            for pid in process_cache:
                for node in process_cache[pid]:
                    yield node.value

    def processes_in_order(self):
        """
        Method that returns processes in order depending on tme and their pid. This is especially
        useful when replaying a file as a stream.

        :return: process generator of the job
        """
        tree = self.tree
        processes = []
        processes_in_order = []
        # create the actual array
        for node, _ in tree.walkDFS():
            processes.append(node.value)
        processes.sort(key=lambda x: x.tme)
        current_tme = processes[0].tme
        current_processes = []
        current_pid = processes[0].gpid - 1  # to also include first pid in correct order
        while processes:
            if processes[0].tme == current_tme:
                current_processes.append(processes.pop(0))
            else:
                # do sorting
                ordered = self._create_order(current_processes, current_pid)
                # reset values
                current_tme = processes[0].tme
                current_pid = ordered[-1].pid
                processes_in_order.extend(ordered)
                current_processes = []
        for process in processes_in_order:
            yield process

    def _create_order(self, elements, start_pid):
        elements_in_order = []
        elements.sort(key=lambda x: x.pid)
        bigger = [process for process in elements if process.pid > start_pid]
        elements_in_order.extend(bigger)
        smaller = [process for process in elements if process.pid <= start_pid]
        elements_in_order.extend(smaller)
        return elements_in_order

    def process_count(self):
        """
        Method that returns the count of the processes inside the job.

        :return: process count
        """
        count = 0
        process_cache = self.process_cache
        for pid in process_cache:
            count += len(process_cache[pid])
        return count

    @staticmethod
    def default_header(**kwargs):
        """
        Returns the header for CSV output in dictionary format.

        :param length: Number of elements being expected
        :return: Dictionary of keys and their positions
        """
        length = kwargs.get("length", 15)
        if length == 9:
            return {"tme": 0, "pid": 1, "ppid": 2, "uid": 3, "name": 4, "cmd": 5, "exit_code": 6,
                    "state": 7, "gpid": 8}
        return {"tme": 0, "exit_tme": 1, "pid": 2, "ppid": 3, "gpid": 4, "uid": 5, "name": 6,
                "cmd": 7, "error_code": 8, "signal": 9, "valid": 10, "int_in_volume": 11,
                "int_out_volume": 12, "ext_in_volume": 13, "ext_out_volume": 14}

    def _add(self, node=None, is_root=False):
        if "sge_shepherd" in node.value.cmd or is_root:
            if self._root is not None:
                raise NonUniqueRootException
            self._root = node
        if node.value.exit_tme > self._last_tme:
            self._last_tme = node.value.exit_tme
        self._process_cache.add_data(data=node, key=node.value.pid, value=node.value.tme)

    def _get_tree(self, reinitialize=False):
        if reinitialize or not self._tree_initialized:
            if reinitialize:
                self._tree = None
                self._process_cache.faulty_nodes = set()
                for pid in self.process_cache:
                    for node in self.process_cache[pid]:
                        node.children = []
            self._initialize_tree()
            self._tree_initialized = True
        if self._tree is None:
            if (len(self._process_cache.faulty_nodes) <= 1 and self._root and
                    (Tree(self._root).getVertexCount() == self.process_count())):
                self._tree = Tree(self._root)
        logging.getLogger(self.__class__.__name__).info(
            "faulty nodes: %s", self._process_cache.faulty_nodes
        )
        return self._tree

    @staticmethod
    def _add_function(child, children):
        tmes = [node.value.tme for node in children]
        tme_index = bisect.bisect_left(tmes, child.value.tme)
        # check for equality of following element
        if tmes[tme_index] == child.value.tme or tmes[tme_index + 1] == child.value.tme \
                if len(children) > tme_index + 1 else True:
            # I also need to do a sorting regarding pid
            # so first filter relevant elements with same tme
            pids = [node.value.pid for node in children if node.value.tme == child.value.tme]
            pid_index = bisect.bisect_left(pids, child.value.pid)
            return tme_index + pid_index
        return tme_index

    def _initialize_tree(self):
        logging.getLogger(self.__class__.__name__).info("Initializing tree structure")
        process_cache = self.process_cache
        # sort the keys first to get the correct ordering in the final tree
        for pid in sorted(process_cache.keys(), key=lambda item: int(item)):
            for node in process_cache[pid]:
                try:
                    parent = self._process_cache.get_data(value=node.value.tme,
                                                          key=node.value.ppid,
                                                          remember_error=True)
                except DataNotInCacheException:
                    pass
                else:
                    if parent:
                        parent.add(node, orderPosition=self._add_function)
        logging.getLogger(self.__class__.__name__).info(
            "no parents found for %d nodes", len(self._process_cache.faulty_nodes)
        )

        if len(self._process_cache.faulty_nodes) <= 1 and self._root:
            # set depth
            for node, depth in Tree(self._root).walkDFS():
                node.value.tree_depth = depth

    def __repr__(self):
        return "%s: db_id (%s), job_id (%s), gpid (%d), workernode (%s), configuration (%s), " \
               "run (%s), tme (%d), root (%s), process_cache (%s), tree_initialized (%s)" % \
               (self.__class__.__name__, self.db_id, self.job_id, self.gpid, self.workernode,
                self.configuration, self.run, self.tme, (self._root and self._root.value),
                self._process_cache, self._tree_initialized)
