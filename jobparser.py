import bisect
import logging

from evenmoreutils.tree import Node
from evenmoreutils.tree import Tree

from process import *
from objectcache import ObjectCache

class JobParser(object):
    def __init__(self):
        self._root = None
        self._processCache = ObjectCache()
        self._treeInitialized = False
        
    def defaultHeader(self):
        return {"tme":0, "exit_tme": 1 ,"pid": 2, "ppid": 3, "gpid": 4,
            "uid": 5, "name": 6, "cmd": 7, "error_code": 8, "signal": 9,
            "valid": 10, "int_in_volume": 11, "int_out_volume": 12,
            "ext_in_volume": 13, "ext_out_volume":14}

    def clearCaches(self):
        logging.info("clearing caches of process parser")
        del self._root
        self._root = None
        self._processCache.clear()
        self._treeInitialized = False

    def parseRow(self, row=None, headerCache=None, tme=None):
        if not headerCache: headerCache=self.defaultHeader()
        dataDict = {}
        for key in headerCache:
            dataDict[key] = row[headerCache[key]]
        
        process = Process(**dataDict)
        
        newNode = Node(value=process)
        if "sge_shepherd" in row[headerCache['name']]: self._root = newNode
        self._processCache.addNodeObject(newNode)
        
    def addProcess(self, process=None):
        newNode = Node(value=process)
        if "sge_shepherd" == process.name: self._root = newNode
        self._processCache.addNodeObject(newNode)

    def isValid(self):
        if len(self._processCache.faultyNodes > 1):
            return false
        for process in self._processCache:
            if not process.valid:
                return false
        return true

    # access to tree
    @property
    def tree(self):
        if not self._treeInitialized:
            self._initializeTree()
            self._treeInitialized = True
        if len(self._processCache.faultyNodes) <= 1:
            return Tree(self._root)
        logging.info("faulty nodes: %s" %self._processCache.faultyNodes)
        return None

    def _initializeTree(self):
        logging.info("Initializing tree structure")
        # sort the keys first to get the correct ordering in the final tree
        processCache = self._processCache.objectCache
        for pid in sorted(processCache.keys(), key=lambda item: int(item)):
            for node in processCache[pid]:
                parent = self._processCache.getNodeObjectForTME(tme=node.value.tme, pid=node.value.ppid)
                if parent:
                    parent.add(node)
                else:
                    logging.error("no parent was found for node %s (%s)" 
                        %(node.value.pid, node.value.tme))
                    
        if len(self._processCache.faultyNodes) <= 1:
            # set depth
            for node, depth in Tree(self._root).walkDFS():
                node.value.tree_depth = depth
