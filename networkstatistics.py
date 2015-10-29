class NetworkStatistics(object):
    def __init__(self, tme=None, event_count=0, data_size=0, workernode=None, run=None):
        self._tme = tme
        self._event_count = event_count
        self._data_size = data_size
        self._workernode = workernode
        self._run = run

    @property
    def workernode(self):
        return self._workernode

    @property
    def run(self):
        return self._run

    @property
    def event_count(self):
        return self._event_count

    @property
    def data_size(self):
        return self._data_size

    @property
    def tme(self):
        return self._tme

    def add(self, data_dict=None):
        # TODO: take care on different data types
        if self._tme is None:
            self._tme = data_dict.get("tme", 0)
        for key in data_dict.keys():
            self._data_size += len(str(data_dict[key]))
        self._event_count += 1

    def getRow(self):
        return "%s,%d,%d" % (str(self._tme), self._event_count, self._data_size)

    def getHeader(self):
        return "tme,event_count,data_size"
