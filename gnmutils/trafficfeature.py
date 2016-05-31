from feature import Feature

from gnmutils.objects.gnm_object import check_id, check_tme


class TrafficFeature(Feature):
    default_key_type = {
        'uid': check_id,
        'id': check_id,
        'exit_tme': check_tme,
        'tme': check_tme,
        'int_in_volume': float,
        'int_in_rate': float,
        'int_out_volume': float,
        'int_out_rate': float,
        'ext_in_volume': float,
        'ext_in_rate': float,
        'ext_out_volume': float,
        'ext_out_rate': float,
    }

    def __init__(self, uid=None, id=None, exit_tme=None, tme=None,
            int_in_volume=0, int_in_rate=0, int_out_volume=0, int_out_rate=0,
            ext_in_volume=0, ext_in_rate=0, ext_out_volume=0, ext_out_rate=0):
        Feature.__init__(uid=uid, tme=tme)
        self.id = self._convert_to_default_type("id", id)
        self.exit_tme = self._convert_to_default_type("exit_tme", exit_tme)
        self.int_in_volume = self._convert_to_default_type("int_in_volume", int_in_volume)
        self.int_in_rate = self._convert_to_default_type("int_in_rate", int_in_rate)
        self.int_out_volume = self._convert_to_default_type("int_out_volume", int_out_volume)
        self.int_out_rate = self._convert_to_default_type("int_out_rate", int_out_rate)
        self.ext_in_volume = self._convert_to_default_type("ext_in_volume", ext_in_volume)
        self.ext_in_rate = self._convert_to_default_type("ext_in_rate", ext_in_rate)
        self.ext_out_volume = self._convert_to_default_type("ext_out_volume", ext_out_volume)
        self.ext_out_rate = self._convert_to_default_type("ext_out_rate", ext_out_rate)

    def addTraffic(self, traffic):
        """
        Adds traffic to the current accumulation of data.

        :param :py:class:`Traffic` traffic: traffic to be added
        """
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

    def default_header(length, **kwargs):
        return {"id": 0, "tme": 1, "exit_tme": 2, "uid": 3, "int_in_volume": 4, "int_in_rate": 5,
                "int_out_volume": 6, "int_out_rate": 7, "ext_in_volume": 8, "ext_in_rate": 9,
                "ext_out_volume": 10, "ext_out_rate": 11}

    class Factory:
        def create(self, **kwargs): return TrafficFeature(**kwargs)

