from gnmutils.parser.dataparser import DataParser
from gnmutils.traffic import Traffic


class TrafficParser(DataParser):
    def __init__(self):
        DataParser.__init__(self)
        self._trafficCache = []

    def defaultHeader(self, length=10):
        if length > 10:
            return {"tme": 0, "pid": 1, "ppid": 2, "uid": 3, "int_in_rate": 4, "ext_in_rate": 5, "int_out_rate": 6, "ext_out_rate": 7, "int_in_cnt": 8, "ext_in_cnt": 9, "int_out_cnt": 10, "ext_out_cnt": 11, "conn": 12, "gpid": 13}
        return {"tme": 0, "pid": 1, "ppid": 2, "uid": 3, "in_rate": 4, "out_rate": 5, "in_cnt": 6, "out_cnt": 7, "conn": 8, "gpid": 9}

    # TODO: use information from parseRow
    def parse(self, *args, **kwargs):
        pass

    def parseRow(self, row=None, headerCache=None, tme=None):
        # ensure right types
        row[headerCache['gpid']] = int(row[headerCache['gpid']])
        row[headerCache['tme']] = tme
        trafficDict = {}
        for key in headerCache:
          trafficDict[key] = row[headerCache[key]]
        trafficDict["workernode"] = self._converter.getWorkernodeObject().name
        traffic = Traffic(**trafficDict)
        try:
            self._matchAndWriteTraffic(traffic)
        except Exception:
            self._trafficCache.append(traffic)

    def clearCaches(self):
        for traffic in self._trafficCache:
            try:
                self._matchAndWriteTraffic(traffic)
            except Exception:
                self._converter.dumpIncompletes(typename="traffic", data=traffic)
        del self._trafficCache
        self._trafficCache = []

    def _matchAndWriteTraffic(self, trafficObject):
        if trafficObject.gpid == 0:
            self._converter.dumpData(
                    typename="traffic",
                    data=trafficObject)
        else:
            job = self._operator.getJob(
                    tme=trafficObject.tme+20,
                    last_tme=trafficObject.tme-20,
                    gpid=trafficObject.gpid)
            self._converter.dumpData(
                    typename="traffic",
                    data=trafficObject,
                    jobId=job.id_value)

