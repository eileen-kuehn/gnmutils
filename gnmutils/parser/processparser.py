"""
The module contains the class :py:class:`ProcessParser` that accumulates different process events
to complete processes that can be stored as CSV.
"""
import time
import cPickle as pickle
import logging

from gnmutils.parser.dataparser import DataParser
from gnmutils.objects.process import Process
from gnmutils.objectcache import ObjectCache
from gnmutils.parser.jobparser import JobParser
from gnmutils.exceptions import NonUniqueRootException
from utility.exceptions import BasicException


class ProcessParser(DataParser):
    """
    The :py:class:`ProcessParser` takes process events and accumulates these to a complete process.
    Complete processes belonging to the same job can then be joined by using a
    :py:class:`JobParser`.
    """
    def __init__(self, data_source=None, data_reader=None, converter=None, operator=None,
                 processCache=None, jobCache=None):
        DataParser.__init__(self, data_source=data_source, data_reader=data_reader)
        self._converter = converter
        self._operator = operator

        try:
            self._process_cache = pickle.load(open(processCache, "rb"))
        except Exception as exception:
            self._process_cache = ObjectCache()
            logging.getLogger(self.__class__.__name__).warn(
                "%s: did not load pickled ProcessCache", exception
            )
        else:
            logging.getLogger(self.__class__.__name__).info(
                "Initialized with pickled ProcessCache"
            )
        try:
            self._job_cache = pickle.load(open(jobCache, "rb"))
        except Exception as exception:
            self._job_cache = ObjectCache()
            logging.getLogger(self.__class__.__name__).warn(
                "%s: did not load pickled JobCache", exception
            )
        else:
            logging.getLogger(self.__class__.__name__).info(
                "Initialized with pickled JobCache"
            )

    @staticmethod
    def defaultHeader(length=9):
        """
        Returns a dictionary of fields and positions in default configuration of header.

        :param int length: Length of expected header
        :return: Dictionary with keys describing the attributes and values giving the position
        :rtype: dict
        """
        return {"tme": 0, "pid": 1, "ppid": 2, "uid": 3, "name": 4, "cmd": 5, "exit_code": 6,
                "state": 7, "gpid": 8}

    def _finish_process(self, job=None, process=None):
        if not job.last_tme:
            job.last_tme = process.exit_tme
        if process.exit_tme > job.last_tme:
            job.last_tme = process.exit_tme
            self._operator.updateJob(job)
        self._move_process(process=process, job_id=(job.id_value or 0))

        # job is complete so remember and save it
        if "sge_shepherd" in process.cmd:
            job.exit_tme = process.exit_tme

            job_parser = self._job_cache.get_data(key=job.id_value, value=0)
            if not self._save_and_delete_job(job_parser=job_parser, job_id=job.id_value, job=job):
                logging.info("waiting for more processes to complete job...")

                # remove dbJob from Cache
                job.valid = job_parser.isValid()
                job.completed = False
                self._operator.saveAndDeleteJob(job)

    def parseRow(self, row=None, headerCache=None, tme=None):
        if "state" in headerCache:
            if "exit" in row[headerCache['state']]:
                # load process and set exit arguments,
                # afterwards remove it from cache
                pid = int(row[headerCache['pid']])
                process_index = self._process_cache.data_index(value=tme, key=pid)
                try:
                    process = self._process_cache.object_cache[pid][process_index]
                    if (row[headerCache['name']] not in process.name and
                                row[headerCache['cmd']] not in process.cmd):
                        # wrong process selected!
                        logging.getLogger(self.__class__.__name__).warning(
                            "process %s has not been logged", row
                        )
                        process = Process(name=row[headerCache['name']],
                                          cmd=row[headerCache['cmd']],
                                          pid=row[headerCache['pid']],
                                          ppid=row[headerCache['ppid']],
                                          uid=row[headerCache['uid']])
                        self._process_cache.add_data(data=process)
                        process_index = self._process_cache.data_index(value=tme, key=pid)
                except KeyError:
                    # exit event received firsts
                    process = Process()
                    process.addProcessEvent(name=row[headerCache['name']],
                                            cmd=row[headerCache['cmd']],
                                            pid=row[headerCache['pid']],
                                            ppid=row[headerCache['ppid']],
                                            uid=row[headerCache['uid']],
                                            tme=row[headerCache['tme']],
                                            exit_code=row[headerCache['exit_code']],
                                            gpid=row[headerCache['gpid']],
                                            state=row[headerCache['state']])
                    self._process_cache.add_data(data=process)
                    process_index = self._process_cache.data_index(value=tme, key=pid)
                else:
                    process.addProcessEvent(tme=row[headerCache['tme']],
                                            exit_code=row[headerCache['exit_code']],
                                            state=row[headerCache['state']])
                try:
                    job = self._operator.getJob(tme=tme,
                                                gpid=int(row[headerCache['gpid']]))
                except BasicException:
                    # the job is currently not known so remember as unknown
                    self._process_cache.unfound.add(process)
                    self._process_cache.remove_data(data=process, key=pid)
                except Exception:
                    # the job is currently not known so remember as unknown
                    self._process_cache.unfound.add(process)
                    self._process_cache.remove_data(data=process, key=pid)
                else:
                    # job has been found, so save current data
                    self._finish_process(job=job, process=process)
            else:
                # a new process is getting to know
                # process has been started, so create and remember
                process = self._create_process(row=row, header_cache=headerCache)
                if "sge_shepherd" in process.cmd:
                    # new pilot is starting
                    try:
                        job = self._operator.getJob(tme=tme,
                                                    gpid=int(row[headerCache['gpid']]),
                                                    batchsystemId=process.batchsystemId)
                        if job.exit_tme and (int(job.exit_tme) < int(tme)):
                            self._operator.createJob(tme=tme,
                                                     gpid=int(row[headerCache['gpid']]),
                                                     batchsystemId=process.batchsystemId)
                        else:
                            logging.getLogger(self.__class__.__name__).error(
                                "ATTENTION: job was not created as it already seems to be "
                                "existent - job_id from DB %d vs CSV %d", job.job_id,
                                process.batchsystemId
                            )
                    except Exception:
                        self._operator.createJob(tme=tme,
                                                 gpid=int(row[headerCache['gpid']]),
                                                 batchsystemId=process.batchsystemId)
        else:
            # load object
            self._create_process(row=row, header_cache=headerCache)

    def check_caches(self, tme=None):
        logging.debug("checking caches")

        # check unfound nodes if a job already exists
        for process in self._process_cache.unfound.copy():
            try:
                job = self._operator.getJob(tme=tme,
                                            gpid=process.gpid)
            except Exception:
                # check if process is already too old and remove it
                if tme - process.exit_tme >= 86400:
                    self._operator.dumpErrors(typename="process", data=process)
                    self._process_cache.unfound.discard(process)
            else:
                logging.info("removed unfound node")
                # job has been found, so save current data
                self._finish_process(job=job, process=process)
                self._process_cache.unfound.discard(process)

        for jid in self._job_cache.object_cache.keys():
            for job_parser in self._job_cache.object_cache[jid][:]:
                if jid == 0:
                    self._save_raw_processes(job_parser=job_parser, job_id=jid)
                else:
                    result = self._save_and_delete_job(job_parser=job_parser, job_id=jid)
                    if not result:
                        # check if job is already too old and remove it
                        job = self._operator.getJobById(jobId=jid)
                        if job.last_tme and (tme - job.last_tme >= 86400):
                            self._save_raw_processes(job_parser=job_parser, job_id=jid)

    def clear_caches(self, **kwargs):
        """
        Method clears the current caches and takes care to pickle the uncompleted processes
        and jobs.
        """
        logging.debug("clearing caches")

        pickle.dump(self._process_cache, open(
            self._operator.getPicklePath(typename="process"),
            "wb"
        ), -1)
        self._process_cache.clear()

        pickle.dump(self._job_cache, open(self._operator.getPicklePath(typename="job"), "wb"), -1)
        self._job_cache.clear()

    def _create_process(self, row=None, header_cache=None):
        process = Process.from_dict(row=dict(zip(header_cache, row)))
        self._process_cache.add_data(data=process)
        return process

    def _move_process(self, process=None, job_id=None):
        if job_id == 0:
            logging.info("received jobId 0")
        job_parser = self._job_cache.get_data(value=0, key=job_id)
        try:
            job_parser.addProcess(process=process)
        except NonUniqueRootException as exception:
            logging.getLogger(self.__class__.__name__).error(
                "%s: added second root node to tree with id %d - batchsystemId: %d", exception,
                job_id, process.batchsystemId
            )
        except Exception:
            job_parser = JobParser()
            job_parser.add_piece(piece=process)
            # add tme field for ObjectCache
            job_parser.tme = 0
            self._job_cache.add_data(data=job_parser, key=job_id)
        self._process_cache.remove_data(data=process)

    def _save_and_delete_job(self, job_parser=None, job_id=None, job=None):
        tree = job_parser.regenerateTree()
        if tree is not None:
            path = self._operator.getPath(typename="process", jobId=job_id)
            with open(path, "w+") as csvfile:
                csvfile.write("# Created by %s on %s\n" %
                              ("processparser.py", time.strftime("%Y%m%d")))
                csvfile.write("# Input Data: Raw Stream Data\n")
                csvfile.write("# Output Data: Combined Process Events\n")
                csvfile.write("%s\n" %(tree.root.value.getHeader()))
                for node in tree.walkBFS():
                    csvfile.write("%s\n" %(node.value.getRow()))

            if job is None:
                job = self._operator.getJobById(job_id)
            if job is not None:
                job.valid = job_parser.isValid()
                job.completed = True
                job.uid = job_parser.uid
                self._operator.saveAndDeleteJob(job)
            self._job_cache.remove_data(data=job_parser, key=job_id)
            return True
        # otherwise keep job in cache and wait for more...
        return False

    def _save_raw_processes(self, job_parser=None, job_id=None):
        process_cache = job_parser.processCache
        if job_id > 0:
            logging.getLogger(self.__class__.__name__).error(
                "have not been able to write tree with %d processes for job %d, writing without "
                "tree", job_parser.processCount(), job_id
            )
            for pid in process_cache:
                for node in process_cache[pid]:
                    self._operator.dumpData(typename="process", data=node.value, jobId=job_id)
        else:
            logging.getLogger(self.__class__.__name__).error(
                "have not been able to write tree with %d processes, writing without tree",
                job_parser.processCount()
            )
            for pid in process_cache:
                for node in process_cache[pid]:
                    self._operator.dumpIncompletes(typename="process", data=node.value)
        self._job_cache.remove_data(data=job_parser, key=job_id)

