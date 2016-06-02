from gnmutils.objects.traffic import Traffic
from gnmutils.objects.process import Process
from gnmutils.exceptions import ArgumentNotDefinedException


class NetworkStatistics(object):
    def __init__(self, tme=None, event_count=0, data_size=0, traffic_in_size=.0,
                 traffic_out_size=.0, background_traffic_in_size=.0, background_traffic_out_size=.0,
                 workernode=None, run=None, interval=20):
        self.tme = tme
        self.event_count = event_count
        self.data_size = data_size
        self.traffic_in_size = traffic_in_size
        self.traffic_out_size = traffic_out_size
        self.background_traffic_in_size = background_traffic_in_size
        self.background_traffic_out_size = background_traffic_out_size
        self.workernode = workernode
        self.run = run
        self.interval = interval

    def add(self, data_dict=None):
        # TODO: take care on different data types
        if self.tme is None:
            self.tme = data_dict.get("tme", 0)
        for key in data_dict.keys():
            self.data_size += len(str(data_dict[key]))
        self.event_count += 1

        # TODO: what about process data?!
        # check for traffic data on workernode itself
        try:
            traffic = Traffic.from_dict(data_dict)
            if traffic is not None:
                if traffic.gpid > 0:
                    self.traffic_in_size += float(traffic.in_rate) * self.interval
                    self.traffic_out_size += float(traffic.out_rate) * self.interval
                else:
                    self.background_traffic_in_size += float(traffic.in_rate) * self.interval
                    self.background_traffic_out_size += float(traffic.out_rate) * self.interval
        except ArgumentNotDefinedException:
            pass

    def getRow(self):
        return "%s,%d,%.2f,%.2f,%.2f,%.2f,%.2f" % (
            str(self.tme), self.event_count, self.data_size/1024.0, self.traffic_in_size,
            self.traffic_out_size, self.background_traffic_in_size,
            self.background_traffic_out_size
        )

    def getHeader(self):
        return "tme,event_count,data_size,traffic_in_size,traffic_out_size," \
               "background_traffic_in_size,background_traffic_out_size"
