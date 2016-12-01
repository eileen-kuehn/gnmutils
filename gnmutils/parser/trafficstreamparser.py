"""
This module implements a :py:class:`DataParser` that creates a single CSV file of traffic data
from the stream received from the GNM tool.
"""
import logging

from gnmutils.exceptions import DataNotInCacheException
from gnmutils.parser.dataparser import DataParser
from gnmutils.objectcache import ObjectCache
from gnmutils.objects.traffic import Traffic
from gnmutils.objects.job import Job


class TrafficWrapper(object):
    def __init__(self, job=None, configuration=None):
        self._traffic = {}
        self.pid = None
        self.tme = None
        self.exit_tme = None
        if job is not None:
            self.pid = job.gpid
            self.tme = job.tme
            self.exit_tme = job.last_tme
            self._traffic["workernode"] = job.workernode
            self._traffic["run"] = job.run
            self._traffic["id"] = job.db_id
        self._traffic["data"] = []
        self._traffic["configuration"] = configuration

    @property
    def traffic(self):
        """
        Enables access to the underlying dictionary that contains relevant information about
        the traffic.

        :return: dictionary containing relevant traffic information
        :rtype: dict
        """
        return self._traffic

    @property
    def data(self):
        """
        Returns the array that contains the traffic entries.

        :return: traffic data
        :rtype: list
        """
        return self._traffic["data"]


class TrafficStreamParser(DataParser):
    """
    First the :py:class:`TrafficStreamParser` follows a very simple approach. When a traffic entry
    is going to be parsed, it checks if it already knows the :py:class:`Job` it belongs to.
    If no :py:class:`Job` can be found, a streamlined version (just identified by unique constraint
    values) is loaded and put into the cache. This cache is used to attach the single
    :py:class:`Traffic` objects in form of an array.

    At the time when splitting the stream, it does not matter, if the single traffic entries are
    matched to existing :py:class:`Process`es. They just need to appear in csv files.
    """
    def __init__(self, data_source=None, data_reader=None, workernode=None, run=None, **kwargs):
        DataParser.__init__(self, data_source, data_reader, **kwargs)
        if self.data_source is not None:
            self._data = next(self.data_source.object_data(
                pattern="traffic_data.pkl",
                path=kwargs.get("path", None)
            ), ObjectCache())
            self._parsed_data = next(self.data_source.object_data(
                pattern="traffic_parsed_data.pkl",
                path=kwargs.get("path", None)
            ), set())
        else:
            self._data = ObjectCache()
        self.workernode = workernode
        self.run = run
        self._last_tme = None

    def pop_data(self):
        for key in self._data.object_cache.keys():
            while self._data.object_cache[key]:
                wrapper = self._data.object_cache[key].pop()
                yield wrapper.traffic

    def check_caches(self, **kwargs):
        if not self._changed:
            return
        for traffic in self._data.unfound.copy():
            _, appended, _ = self._match_traffic(traffic=traffic)
            if appended:
                self._data.unfound.discard(traffic)

    def clear_caches(self):
        self._data.clear()

    # TODO: fix naming of method
    def defaultHeader(self, **kwargs):
        return Traffic.default_header(**kwargs)

    def archive_state(self, **kwargs):
        if self.data_source is not None:
            self.data_source.write_object_data(
                data=self._data,
                name="traffic_data",
                **kwargs
            )
            self.data_source.write_object_data(
                data=self.configuration,
                name="configuration",
                **kwargs
            )
            self.data_source.write_object_data(
                data=self._parsed_data,
                name="traffic_parsed_data",
                **kwargs
            )
        else:
            logging.getLogger(self.__class__.__name__).warning(
                "Archiving not done because of missing data_source"
            )

    def _add_piece(self, piece=None):
        self._changed = True
        # look for matching job
        finished, _, matching_wrapper = self._match_traffic(traffic=piece)
        if finished and object is not None:
            self._data.remove_data(data=matching_wrapper)
            return matching_wrapper.traffic

        # check for other finished jobs
        self._last_tme = piece.tme - self._interval()
        return next(self._check_data(), None)

    def _match_traffic(self, traffic=None):
        # load job object from cache
        matching_traffic_wrapper = None
        finished = False
        try:
            object_index = self._data.data_index(value=traffic.tme, key=traffic.gpid)
        except DataNotInCacheException:
            appended = self._match_with_new_wrapper(traffic=traffic)
        else:
            try:
                matching_traffic_wrapper = self._data.object_cache[traffic.gpid][object_index]
                if traffic.tme - self._interval() <= matching_traffic_wrapper.exit_tme:
                    matching_traffic_wrapper.data.append(traffic)
                    appended = True
                else:
                    # remember and remove old wrapper
                    finished = True
                    appended = self._match_with_new_wrapper(traffic=traffic)
            except IndexError:
                # no wrapper is known
                appended = self._match_with_new_wrapper(traffic=traffic)
            except KeyError:
                # no wrapper is known
                appended = self._match_with_new_wrapper(traffic=traffic)
        return finished, appended, matching_traffic_wrapper

    def _piece_from_dict(self, data_dict=None):
        return Traffic(**data_dict)

    def _match_with_new_wrapper(self, traffic=None):
        if traffic.gpid == 0:
            wrapper = TrafficWrapper(Job(
                gpid=0,
                tme=0,
                last_tme=5000000000,
                workernode=self.workernode,
                run=self.run))
        else:
            wrapper = self._load_traffic_wrapper(traffic=traffic)
        if wrapper is not None:
            wrapper.data.append(traffic)
            self._data.add_data(data=wrapper)
            return True
        else:
            logging.getLogger(self.__class__.__name__).warning(
                "was not able to get job for traffic (gpid: %s, tme: %s, workernode: %s, run: %s)",
                traffic.gpid, traffic.tme, self.workernode, self.run
            )
            self._data.unfound.add(traffic)
        return False

    def _load_traffic_wrapper(self, traffic=None):
        job = Job(workernode=self.workernode,
                  run=self.run,
                  tme=traffic.tme + self._interval(),
                  gpid=traffic.gpid)
        job = self.data_source.job_description(data=job)
        if job is not None and job.last_tme > 0:
            wrapper = TrafficWrapper(
                job=job,
                configuration=self.configuration
            )
            return wrapper
        return None

    def _interval(self):
        if self.configuration is not None:
            return self.configuration.interval
        return 20

    def _parsing_finished(self):
        for data in self._check_data():
            yield data

    def _check_data(self):
        for key in self._data.object_cache.keys():
            for wrapper in self._data.object_cache[key][:]:
                if wrapper.exit_tme < self._last_tme:
                    self._data.object_cache[key].remove(wrapper)
                    yield wrapper.traffic

