class MonitoringConfiguration(object):
    def __init__(self, version=None, interval=20, level=None, grouping=None, skip_other_pids=False,
                 **kwargs):
        self._version = version
        self._interval = interval
        self._level = level
        self._grouping = grouping
        self._skip_other_pids = skip_other_pids or kwargs.get("skipOtherPids", False)

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, value=None):
        self._version = value

    @property
    def interval(self):
        return self._interval

    @interval.setter
    def interval(self, value=None):
        self._interval = value

    @property
    def level(self):
        return self._level

    @level.setter
    def level(self, value=None):
        self._level = value

    @property
    def grouping(self):
        return self._grouping

    @grouping.setter
    def grouping(self, value=None):
        self._grouping = value

    @property
    def skip_other_pids(self):
        return self._skip_other_pids

    @skip_other_pids.setter
    def skip_other_pids(self, value=None):
        self._skip_other_pids = value

    def getRow(self):
        return "# version: %s, interval: %d, level: %s, grouping: %s, skipOtherPids: %s" % (
            self.version, self.interval, self.level, self.grouping, self.skip_other_pids
        )

    def __repr__(self):
        return "%s: version (%s), interval (%d), level (%s), grouping (%s), skipOtherPids (%s)" % (
            self.__class__.__name__, self.version, self.interval, self.level, self.grouping,
            self.skip_other_pids
        )
