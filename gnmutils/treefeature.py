from gnmutils.feature import Feature


class TreeFeature(Feature):
    def __init__(self, id=None, degrees=None, height=None, count=None,
            leaves=None, uid=None):
        self.id = id
        self.degrees = degrees
        self.height = height
        self.count = count
        self.leaves = leaves
        self.uid = uid

    def _getDegrees(self):
        return "c(%s)" %(str.join(",", [str(obj) for obj in self.degrees]))

    def getRow(self):
        return ("%d\t%s\t%d\t%d\t%d\t%s" %(self.id, self._getDegrees(),
            self.height, self.count, self.leaves, self.uid))

    def getHeader(self):
        return "id\tdegrees\theight\tcount\tleaves\tuid"

    class Factory:
        def create(self, **kwargs): return TreeFeature(**kwargs)

