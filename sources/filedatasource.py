import os
import re
import pickle

from gnmutils.sources.datasource import DataSource
from gnmutils.reader.csvreader import CSVReader
from gnmutils.parser.jobparser import JobParser
from gnmutils.parser.streamparser import StreamParser

from utility.exceptions import *
from evenmoreutils import path as pathutils


class FileDataSource(DataSource):
    default_path = "/Users/eileen/projects/Dissertation/Development/data/raw"

    def __init__(self):
        pass

    def is_available(self):
        return True

    def object_data(self, **kwargs):
        for dir_entry in sorted(os.listdir(kwargs.get("path",
                                                      self.default_path))):
            m = re.search(kwargs.get("pattern", ".pkl"), dir_entry)
            if m:
                file_path = os.path.join(kwargs.get("path", self.default_path), dir_entry)
                logging.getLogger(self.__class__.__name__).debug("reading %s for object data" % file_path)
                data = pickle.load(open(file_path, "rb"))
                yield data
        yield None

    def write_object_data(self, **kwargs):
        object_data = kwargs["data"]
        path = pathutils.ensureDirectory(kwargs.get("path", self.default_path))
        with open("%s/%s.pkl" % (path,
                                 kwargs.get("name", "object_data")), "w+") as data_file:
            pickle.dump(object_data, data_file)

    def jobs(self, **kwargs):
        path = kwargs.get("path", self.default_path)
        # TODO: fix static path configuration
        if "processed" in kwargs.get("source", "processed"):
            converter = CSVReader()
            # convert processed jobs
            for dir_entry in sorted(os.listdir(path)):
                m = re.match(kwargs.get("pattern", "(\d*)-process.csv"), dir_entry)
                if m:
                    yield self._read_job(
                        path=os.path.join(path, dir_entry),
                        converter=converter
                    )
        else:
            # convert raw data
            workernodeSubdirs = pathutils.getImmediateSubdirectories(path)
            # TODO: better determination if it is actually a workernode subdir
            if workernodeSubdirs and "processed" not in workernodeSubdirs:
                # it might be that we are in workernode level, so check for run
                for workernodeSubdir in workernodeSubdirs:
                    currentPath = os.path.join(path, workernodeSubdir)
                    runSubdirs = pathutils.getImmediateSubdirectories(currentPath)
                    if runSubdirs and "processed" not in runSubdirs:
                        # we seem to be at run level
                        for runSubdir in runSubdirs:
                            if "unzipped" not in runSubdir:
                                for job in self._read_stream(
                                        path=os.path.join(currentPath, runSubdir),
                                        workernode=workernodeSubdir,
                                        run=runSubdir,
                                        archive=kwargs.get("archive", False)
                                ):
                                    yield job
                    else:
                        # we have already been at run level, so identify workernode
                        runSubdir = workernodeSubdir
                        if "unzipped" not in runSubdir:
                            workernodePaths = os.path.split(path)
                            for job in self._read_stream(
                                    path=currentPath,
                                    workernode=workernodePaths[1],
                                    run=runSubdir,
                                    archive=kwargs.get("archive", False)
                            ):
                                yield job
            else:
                # we have already been at run level, so identify run and workernode
                runPaths = os.path.split(path)
                workernodePaths = os.path.split(runPaths[0])
                for job in self._read_stream(
                        path=path,
                        workernode=workernodePaths[1],
                        run=runPaths[1],
                        archive=kwargs.get("archive", False)
                ):
                    yield job

    def write_job(self, **kwargs):
        job = kwargs["data"]
        path = pathutils.ensureDirectory(kwargs.get("path", self.default_path))
        with open("%s/%s-process.csv" % (path,
                                         job.db_id), "w+") as job_file:
                # TODO: write something about creation
                header_initialized = False
                for process in job.processes():
                    if not header_initialized:
                        # write header
                        job_file.write("%s\n" % job.configuration.getRow())
                        job_file.write("%s\n" % process.getHeader())
                        header_initialized = True
                    job_file.write("%s\n" % process.getRow())

    def write_payload(self, **kwargs):
        self._write_payload(kwargs)

    def write_payload_result(self, **kwargs):
        logging.getLogger(self.__class__.__name__).warn("writing of payload results to filesystem is not supported")

    def _read_stream(self, path=None, workernode=None, run=None, converter=CSVReader(), archive=False):
        converter.parser = StreamParser(workernode=workernode, run=run, data_source=self, path=path)
        for dir_entry in sorted(os.listdir(path)):
            if re.match("^[0-9]{10}-process.log-[0-9]{8}", dir_entry):
                for job in converter.read(filename=os.path.join(path, dir_entry)):
                    yield job
        converter.parser.check_caches()
        for jid in converter.parser.data.objectCache.keys():
            while converter.parser.data.objectCache[jid]:
                yield converter.parser.data.objectCache[jid].pop()
        if archive:
            converter.parser.archive_state(path=path)

    def _read_job(self, path=None, converter=CSVReader()):
        converter.parser = JobParser()
        converter.read(filename=path)
        return converter.parser.data

    def _write_payload(self, **kwargs):
        payload = kwargs["data"]
        # TODO: some trouble with job_id here
        path = pathutils.ensureDirectory(kwargs.get("path", self.default_path))
        with open("%s/%s.csv" % (path,
                                 payload.job_id), "w+") as payload_file:
                # TODO: write something about creation
                header_initialized = False
                for process in payload.processes():
                    if not header_initialized:
                        # write header
                        payload_file.write("%s\n" % process.getHeader())
                        header_initialized = True
                    payload_file.write("%s\n" % process.getRow())
