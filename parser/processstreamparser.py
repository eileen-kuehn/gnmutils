from gnmutils.parser.dataparser import DataParser
from gnmutils.objectcache import ObjectCache
from gnmutils.process import Process
from gnmutils.job import Job
from gnmutils.exceptions import ProcessMismatchException

from utility.exceptions import *


class ProcessStreamParser(DataParser):
    job_root_name = "sge_shepherd"

    """
    The :py:class:`ProcessStreamParser` works on the log files produced by the GNM monitoring tool. One after the other
    it parses the different lines belonging to one specific run on one specific workernode. It takes care in splitting
    the data belonging to different :py:class:`job`s. As soon as one object has been finished it is given to the
    :py:attrib:`DataSource` for further handling/storage.

    As the completeness regarding the traffic cannot be determined automatically, it is ... what?!
    """
    def __init__(self, workernode=None, run=None, **kwargs):
        self._process_cache = ObjectCache()
        DataParser.__init__(self, **kwargs)
        self._data = self._data or ObjectCache()
        self._workernode = workernode
        self._run = run

    def load_archive_state(self, path=None):
        if self._data_source is not None:
            self._process_cache = next(self._data_source.object_data(
                pattern="process_cache.pkl",
                path=path
            ), ObjectCache())

    # TODO: fix naming of method
    def defaultHeader(self, **kwargs):
        return Job.default_header(**kwargs)

    @property
    def workernode(self):
        return self._workernode

    @workernode.setter
    def workernode(self, value=None):
        self._workernode = value

    @property
    def run(self):
        return self._run

    @run.setter
    def run(self, value=None):
        self._run = value

    def archive_state(self, **kwargs):
        if self._data_source is not None:
            self._data_source.write_object_data(data=self._data, name="data", **kwargs)
            self._data_source.write_object_data(data=self._process_cache, name="process_cache", **kwargs)
            self._data_source.write_object_data(data=self._configuration, name="configuration", **kwargs)
            self._data_source.write_object_data(data=self._parsed_data, name="parsed_data", **kwargs)
        else:
            logging.getLogger(self.__class__.__name__).warning("Archiving not done because of missing data_source")

    def pop_data(self):
        for key in self._data.objectCache.keys():
            while self._data.objectCache[key]:
                yield self._data.objectCache[key].pop()

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
                    data_source=self._data_source)
                for job in self._data_source.read_job(
                    data=job_object,
                    path=kwargs.get("path", None)
                ):
                    if job.job_id:
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
                object_index = self._process_cache.getObjectIndex(tme=process.exit_tme, pid=process.pid)
                # load process object from cache
                try:
                    matching_process = self._process_cache.objectCache[process.pid][object_index]
                    process.addProcessEvent(**matching_process.toProcessEvent())
                except KeyError as e:
                    # exit state received first
                    logging.getLogger(self.__class__.__name__).warning(
                        "received exit event of process before actual start event: %s" % e)
                    self._process_cache.addObject(process)
                except ProcessMismatchException as e:
                    logging.getLogger(self.__class__.__name__).warning(e)
                    self._process_cache.addObject(process)
                else:
                    self._process_cache.removeObject(matching_process, pid=matching_process.pid)
                    is_finished, job = self._finish_process(process=process)
                    if not is_finished and job is None:
                        self._process_cache.unfound.add(process)
                    elif is_finished:
                        self._data.removeObject(job, pid=job.gpid)
                        return job
            else:
                if self.job_root_name in process.name:
                    # create new dummy job
                    self._data.addObject(
                        object=Job(
                            workernode=self.workernode,
                            run=self.run,
                            tme=process.tme,
                            gpid=process.gpid,
                            job_id=process.batchsystemId,
                            configuration=self.configuration,
                            data_source=self._data_source),
                        pid=process.gpid,
                        tme=process.tme)
                self._process_cache.addObject(process)

    def _finish_process(self, process=None):
        object_index = self._data.getObjectIndex(tme=process.tme, pid=process.gpid)
        try:
            matching_job = self._data.objectCache[process.gpid][object_index]
        except KeyError as e:
            logging.getLogger(self.__class__.__name__).debug("no matching job has been found %s" % process)
        else:
            matching_job.add_process(process=process,
                                     is_root=(self.job_root_name in process.name))
            if self.job_root_name in process.name:
                return True, matching_job
            return False, matching_job
        return False, None
