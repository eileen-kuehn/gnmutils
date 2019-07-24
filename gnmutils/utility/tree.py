class Node(object):
    def __init__(self, value=None, children=False, parent=None):
        self.value = value
        self.children = children or []
        # if len(self.children) > 0:
        #     self._tmes = [child.value.tme for child in children]
        #     self._pids = [child.value.pid for child in children]
        # else:
        #     self._tmes = []
        #     self._pids = []
        self.parent = parent

    def __getstate__(self):
        return self.value

    def __setstate__(self, state):
        self.value = state

    # def add(self, child, orderPosition = lambda child, children, tmes, pids: len(children)):
    def add(self, child):
        child.parent = self
        # self._tmes.append(child.value.tme)
        # self._pids.append(child.value.pid)
        self.children.append(child)
        # self._tmes.insert(order_position, child.value.tme)
        # self._pids.insert(order_position, child.value.pid)
        # self.children.insert(order_position, child)


class Tree(object):
    def __init__(self, root=None):
        self._root = root

    @property
    def root(self):
        return self._root

    @root.setter
    def root(self, value=None):
        self._root = value

    def getVertexCount(self, node=None):
        vertex_count = 0
        for _, _ in self.walkDFS(node=node):
            vertex_count += 1
        return vertex_count

    def walkBFS(self, node=None, stopCriteria=lambda n, d: False):
        if node is None: node = self.root
        toVisit = [node]
        while len(toVisit) > 0:
            node = toVisit.pop(0)
            yield node
            for child in node.children:
                toVisit.append(child)

    def walkDFS(self, node=None, depth=0, stopCriteria=lambda n, d: False):
        if node is None: node = self.root
        if stopCriteria(node, depth): return
        # iterate tree in pre-order depth first
        yield node, depth
        try:
            for child in node.children[:]:
                for n, d in self.walkDFS(child, depth=depth + 1,
                                         stopCriteria=stopCriteria):
                    yield n, d
        except TypeError:
            pass

    def walkToRoot(self, node):
        yield node
        while node.parent is not None:
            node = node.parent
            yield node

    def getDepth(self, node):
        depth = 0
        while node.parent is not None:
            depth += 1
            node = node.parent
        return depth

    def printTree(self, stopCriteria=lambda n, d: False):
        for node, depth in self.walkDFS(stopCriteria=stopCriteria):
            print("at layer %d for node %s" % (depth, node.value.name))
            for child in node.children:
                if not stopCriteria(child, 0):
                    print("Tme %s for pid %s (%s)" % (
                    child.value.tme, child.value.pid, child.value.name))
