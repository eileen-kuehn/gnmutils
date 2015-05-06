import bisect
import logging

from evenmoreutils.tree import Node
from evenmoreutils.tree import Tree

from process import *

class ProcessParser(object):
    def __init__(self):
        self._root = None
        self._processCache = {}
        self._faultyNodes = set()
        self._treeInitialized = False

    def clearCaches(self):
        logging.info("clearing caches of process parser")
        del self._root
        self._root = None
        del self._faultyNodes
        self._faultyNodes = set()
        del self._processCache
        self._processCache = {}
        self._treeInitialized = False

    def parseRow(self, row=None, headerCache=None, tme=None):
        dataDict = {}
        for key in headerCache:
            dataDict[key] = row[headerCache[key]]
        
        process = Process(**dataDict)
        self._addProcess(process, "sge_shepherd" in row[headerCache['name']])

    # access to tree
    @property
    def tree(self):
        if not self._treeInitialized:
            self._initializeTree()
            self._treeInitialized = True
        if len(self._faultyNodes) <= 1:
            return Tree(self._root)
        logging.info("faulty nodes: %s" %self._faultyNodes)
        return None
        
    def _addProcess(self, processObject, isRoot):
        newNode = Node(value=processObject)
        if isRoot: self._root = newNode
        
        try:
            tmeList = [int(process.value.tme) for process in self._processCache[processObject.pid]]
            index = bisect.bisect_left(tmeList, int(processObject.tme))
            self._processCache[processObject.pid].insert(index, newNode)
        except KeyError:
            self._processCache[processObject.pid] = [newNode]
        
    def _getNodeForTME(self, tme, pid):
        tme = int(tme)
        try:
            processArray = self._processCache[pid]
            tmeArray = [int(node.value.tme) for node in processArray]
            index = bisect.bisect_right(tmeArray, tme) - 1
            return processArray[index]
        except KeyError:
            self._faultyNodes.add(pid)
            logging.error("error for %s (%d)" %(pid, tme))

    def _initializeTree(self):
        logging.info("Initializing tree structure")
        # sort the keys first to get the correct ordering in the final tree
        for pid in sorted(self._processCache.keys(), key=lambda item: int(item)):
            for node in self._processCache[pid]:
                parent = self._getNodeForTME(tme=int(node.value.tme), pid=node.value.ppid)
                if parent:
                    parent.add(node)
                else:
                    logging.error("no parent was found for node %s (%s)" %(node.value.pid, node.value.tme))
                    
        if len(self._faultyNodes) <= 1:
            # set depth
            for node, depth in Tree(self._root).walkDFS():
                node.value.tree_depth = depth
