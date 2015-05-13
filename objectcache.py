import bisect
import logging

class ObjectCache(object):
    def __init__(self):
        self._objectCache = {}
        self._faultyNodes = set()
        self._unfound = set()
        
    def addObject(self, object=None, pid=None, tme=None):
        if not pid: pid=object.pid
        if not tme: tme=object.tme
        try:
            tmeList = [process.tme for process in self._objectCache[pid]]
            index = bisect.bisect_left(tmeList, int(tme))
            self._objectCache[pid].insert(index, object)
        except KeyError:
            self._objectCache[pid] = [object]
        
    def getObject(self, tme, pid):
        index = self.getObjectIndex(tme, pid)
        if index is not None: return self._objectCache[pid][index]
        
    def removeObject(self, object=None, pid=None):
        if not pid: pid = object.pid
        try:
            objectArray = self._objectCache[pid]
            objectArray.remove(object)
            if len(self._objectCache[pid]) == 0:
                del self._objectCache[pid]
            return True
        except KeyError:
            pass
        return False
            
    def getObjectIndex(self, tme, pid):
        try:
            processArray = self._objectCache[pid]
            tmeArray = [node.tme for node in processArray]
            index = bisect.bisect_right(tmeArray, tme) - 1
            return index
        except KeyError:
            self._faultyNodes.add(pid)
            logging.error("error for %s (%d)" %(pid, tme))
            return None
        
    def addNodeObject(self, nodeObject):
        try:
            tmeList = [process.value.tme for process in self._objectCache[nodeObject.value.pid]]
            index = bisect.bisect_left(tmeList, int(nodeObject.value.tme))
            self._objectCache[nodeObject.value.pid].insert(index, nodeObject)
        except KeyError:
            self._objectCache[nodeObject.value.pid] = [nodeObject]
            
    def getNodeObjectForTME(self, tme, pid):
        try:
            processArray = self._objectCache[pid]
            tmeArray = [node.value.tme for node in processArray]
            index = bisect.bisect_right(tmeArray, tme) - 1
            return processArray[index]
        except KeyError:
            self._faultyNodes.add(pid)
            logging.error("error for %s (%d)" %(pid, tme))
            
    def clear(self):
        del self._objectCache
        self._objectCache = {}
        del self._faultyNodes
        self._faultyNodes = set()
        del self._unfound
        self._unfound = set()
            
    @property
    def unfound(self):
        return self._unfound
            
    @property
    def faultyNodes(self):
        return self._faultyNodes
        
    @property
    def objectCache(self):
        return self._objectCache