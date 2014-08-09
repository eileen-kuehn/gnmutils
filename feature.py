class Feature(object):
    def __init__(self, degrees=None, height=None, count=None, leaves=None, uid=None):
        self.degrees = degrees
        self.height = height
        self.count = count
        self.leaves = leaves
        self.uid = uid

    def _getDegrees(self):
        return "c(%s)" %(str.join(",", [str(obj) for obj in degrees]))

    def getRow(self):
        return ("%d,%s,%d,%d,%d,%d" %(self.id, self._getDegrees(),
            self.height, self.count, self.leaves, self.uid))

    def getHeader(self):
        return "id,degrees,height,count,leaves,uid"

