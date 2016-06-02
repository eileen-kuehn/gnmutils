"""
The module implements the parsing of original output files from GNM tool.
"""
import logging

from gnmutils.parser.dataparser import DataParser
from gnmutils.objectcache import ObjectCache
from gnmutils.objects.process import Process
from gnmutils.objects.job import Job
from gnmutils.exceptions import ProcessMismatchException


class ProcessStreamParser(DataParser):
    """
    The :py:class:`ProcessStreamParser` works on the log files produced by the GNM monitoring tool.
    One after the other it parses the different lines belonging to one specific run on one specific
    workernode. It takes care in splitting the data belonging to different :py:class:`job`s.
    As soon as one object has been finished it is given to the :py:attr:`DataSource` for further
    handling/storage.

    As the completeness regarding the traffic cannot be determined automatically, it is ... what?!
    """
    job_root_name = "sge_shepherd"

    def __init__(self, data_source=None, data_reader=None, workernode=None, run=None, **kwargs):
        self._process_cache = ObjectCache()
        DataParser.__init__(self, data_source, data_reader, **kwargs)
        self._data = self._data or ObjectCache()
        self.workernode = workernode
        self.run = run

    def load_archive_state(self, path=None):
        if self.data_source is not None:
            self._process_cache = next(self.data_source.object_data(
                pattern="process_cache.pkl",
                path=path
            ), ObjectCache())

    @staticmethod
    def defaultHeader(**kwargs):
        return Job.default_header(**kwargs)

    def archive_state(self, **kwargs):
        if self.data_source is not None:
            self.data_source.write_object_data(
                data=self._data,
                name="data",
                **kwargs
            )
            self.data_source.write_object_data(
                data=self._process_cache,
                name="process_cache",
                **kwargs
            )
            self.data_source.write_object_data(
                data=self._configuration,
                name="configuration",
                **kwargs
            )
            self.data_source.write_object_data(
                data=self._parsed_data,
                name="parsed_data",
                **kwargs
            )
        else:
            logging.getLogger(self.__class__.__name__).warning(
                "Archiving not done because of missing data_source"
            )

    def pop_data(self):
        for key in self._data.object_cache.keys():
            while self._data.object_cache[key]:
                yield self._data.object_cache[key].pop()

    def check_caches(self, **kwargs):
        if not self._changed:
            return
        for process in self._process_cache.unfound.copy():
            is_finished, job = self._finish_process(process)
            if is_finished:
                self._process_cache.unfound.discard(process)
            else:
                # try to load job from data_source
                job_object = Job(
                    workernode=self.workernode,
                    run=self.run,
                    tme=process.tme,
                    gpid=process.gpid,
                    configuration=self.configuration,
                    data_source=self.data_source)
                job_reader = self.data_source.read_job(
                    data=job_object,
                    path=kwargs.get("path", None)
                )
                if job_reader is not None:
                    for job in job_reader:
                        if job is not None and job.job_id:
                            job_object = job
                            job_object.add_process(process=process)
                            self._process_cache.unfound.discard(process)

    def clear_caches(self):
        self._data.clear()
        self._process_cache.clear()

    def _piece_from_dict(self, piece=None):
        process = Process()
        process.addProcessEvent(**piece)
        return process

    def _add_piece(self, process=None):
        self._changed = True
        if process.gpid > 0:
            if "exit" in process.state:
                # look for matching piece
                object_index = self._process_cache.data_index(
                    value=process.exit_tme,
                    key=process.pid
                )
                # load process object from cache
                try:
                    matching_process = self._process_cache.object_cache[process.pid][object_index]
                    process.addProcessEvent(**matching_process.toProcessEvent())
                except KeyError as exception:
                    # exit state received first
                    logging.getLogger(self.__class__.__name__).warning(
                        "received exit event of process before actual start event: %s", exception
                    )
                    self._process_cache.add_data(data=process)
                except ProcessMismatchException as exception:
                    logging.getLogger(self.__class__.__name__).warning(exception)
                    self._process_cache.add_data(data=process)
                else:
                    self._process_cache.remove_data(data=matching_process, key=matching_process.pid)
                    is_finished, job = self._finish_process(process=process)
                    if not is_finished and job is None:
                        self._process_cache.unfound.add(process)
                    elif is_finished:
                        self._data.remove_data(data=job, key=job.gpid)
                        return job
            else:
                if self.job_root_name in process.name:
                    # create new dummy job
                    self._data.add_data(
                        data=Job(workernode=self.workernode,
                                 run=self.run,
                                 tme=process.tme,
                                 gpid=process.gpid,
                                 job_id=process.batchsystemId,
                                 configuration=self.configuration,
                                 data_source=self.data_source),
                        key=process.gpid,
                        value=process.tme)
                self._process_cache.add_data(data=process)

    def _finish_process(self, process=None):
        object_index = self._data.data_index(value=process.tme, key=process.gpid)
        try:
            matching_job = self._data.object_cache[process.gpid][object_index]
        except KeyError:
            logging.getLogger(self.__class__.__name__).debug(
                "no matching job has been found %s", process
            )
        else:
            matching_job.add_process(process=process,
                                     is_root=(self.job_root_name in process.name))
            if self.job_root_name in process.name:
                return True, matching_job
            return False, matching_job
        return False, None
