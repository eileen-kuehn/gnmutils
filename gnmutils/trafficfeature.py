from feature import *

class TrafficFeature(Feature):
    def __init__(self, uid=None, id=None, exit_tme=None, tme=None,
            int_in_volume=0, int_in_rate=0, int_out_volume=0, int_out_rate=0,
            ext_in_volume=0, ext_in_rate=0, ext_out_volume=0, ext_out_rate=0):
        self.id = id
        self.uid = uid
        self.tme = tme
        self.exit_tme = exit_tme
        self.int_in_volume = int_in_volume
        self.int_in_rate = int_in_rate
        self.int_out_volume = int_out_volume
        self.int_out_rate = int_out_rate
        self.ext_in_volume = ext_in_volume
        self.ext_in_rate = ext_in_rate
        self.ext_out_volume = ext_out_volume
        self.ext_out_rate = ext_out_rate

    def addTraffic(self, traffic):
        if "int" in traffic.conn_cat:
            # internal interface
            self.int_in_volume += float(traffic.in_rate) * int(traffic.interval)
            self.int_out_volume += float(traffic.out_rate) * int(traffic.interval)
        elif "ext" in traffic.conn_cat:
            # external interface
            self.ext_in_volume += float(traffic.in_rate) * int(traffic.interval)
            self.ext_out_volume += float(traffic.out_rate) * int(traffic.interval)

    def _getDuration(self):
        return self.exit_tme and self.tme and (self.exit_tme - self.tme) or 0

    def _getIntInRate(self):
        try:
            return self.int_in_volume / self._getDuration()
        except ZeroDivisionError:
            return self.int_in_rate

    def _getIntOutRate(self):
        try:
            return self.int_out_volume / self._getDuration()
        except ZeroDivisionError:
            return self.int_out_rate

    def _getExtInRate(self):
        try:
            return self.ext_in_volume / self._getDuration()
        except ZeroDivisionError:
            return self.ext_in_rate

    def _getExtOutRate(self):
        try:
            return self.ext_out_volume / self._getDuration()
        except ZeroDivisionError:
            return self.ext_out_rate

    def getRow(self):
        return ("%d\t%s\t%s\t%s\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f"
                %(self.id, self.tme, self.exit_tme, self.uid,
                    self.int_in_volume, self._getIntInRate(),
                    self.int_out_volume, self._getIntOutRate(),
                    self.ext_in_volume, self._getExtInRate(),
                    self.ext_out_volume, self._getExtOutRate()))

    def getHeader(self):
        return ("id\ttme\texit_tme\tuid\tint_in_volume\tint_in_rate\t"
                "int_out_volume\tint_out_rate\text_in_volume\t"
                "ext_in_rate\text_out_volume\text_out_rate")

    class Factory:
        def create(self, **kwargs): return TrafficFeature(**kwargs)

