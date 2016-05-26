from gnmutils.traffic import Traffic


class NetworkStatistics(object):
    def __init__(self, tme=None, event_count=0, data_size=0, traffic_in_size=.0,
                 traffic_out_size=.0, background_traffic_in_size=.0, background_traffic_out_size=.0,
                 workernode=None, run=None, interval=20):
        self._tme = tme
        self._event_count = event_count
        self._data_size = data_size
        self._traffic_in_size = traffic_in_size
        self._traffic_out_size = traffic_out_size
        self._background_traffic_in_size = background_traffic_in_size
        self._background_traffic_out_size = background_traffic_out_size
        self._workernode = workernode
        self._run = run
        self._interval = interval

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
    def traffic_in_size(self):
        return self._traffic_in_size
    
    @property
    def traffic_out_size(self):
        return self._traffic_out_size

    @property
    def background_traffic_in_size(self):
        return self._background_traffic_in_size
    
    @property
    def background_traffic_out_size(self):
        return self._background_traffic_out_size

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

        # check for traffic data on workernode itself
        if Traffic.is_conform(**data_dict):
            traffic = Traffic(**data_dict)
            if traffic.gpid > 0:
                self._traffic_in_size += float(traffic.in_rate) * self._interval
                self._traffic_out_size += float(traffic.out_rate) * self._interval
            else:
                self._background_traffic_in_size += float(traffic.in_rate) * self._interval
                self._background_traffic_out_size += float(traffic.out_rate) * self._interval

    def getRow(self):
        return "%s,%d,%.2f,%.2f,%.2f,%.2f,%.2f" % (
            str(self._tme), self._event_count, self._data_size/1024.0, self._traffic_in_size,
            self._traffic_out_size, self._background_traffic_in_size,
            self._background_traffic_out_size
        )

    def getHeader(self):
        return "tme,event_count,data_size,traffic_in_size,traffic_out_size," \
               "background_traffic_in_size,background_traffic_out_size"
