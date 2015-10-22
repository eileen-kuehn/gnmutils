import os
import re
import pickle
import zipfile

from gnmutils.sources.datasource import DataSource
from gnmutils.reader.csvreader import CSVReader
from gnmutils.parser.jobparser import JobParser
from gnmutils.parser.processstreamparser import ProcessStreamParser
from gnmutils.parser.trafficstreamparser import TrafficStreamParser
from gnmutils.utils import *

from utility.exceptions import *
from evenmoreutils import path as pathutils


class FileDataSource(DataSource):
    default_path = "/Users/eileen/projects/Dissertation/Development/data/raw"

    def __init__(self):
        pass

    def is_available(self):
        return True

    def object_data(self, **kwargs):
        """
        :param path:
        :param pattern:
        :return:
        """
        for dir_entry in sorted(os.listdir(kwargs.get("path",
                                                      self.default_path))):
            if re.search(kwargs.get("pattern", ".pkl"), dir_entry):
                file_path = os.path.join(kwargs.get("path", self.default_path), dir_entry)
                logging.getLogger(self.__class__.__name__).debug("reading %s for object data" % file_path)
                data = pickle.load(open(file_path, "rb"))
                yield data

    def write_object_data(self, **kwargs):
        """
        :param path:
        :param data:
        :param name:
        :return:
        """
        object_data = kwargs.get("data", None)
        path = pathutils.ensureDirectory(kwargs.get("path", self.default_path))
        with open("%s/%s.pkl" % (path,
                                 kwargs.get("name", "object_data")), "w+") as data_file:
            pickle.dump(object_data, data_file)

    def traffics(self, **kwargs):
        """
        :param path:
        :param data_path:
        :param source:
        :param stateful:
        :return:
        """
        path = kwargs.get("path", self.default_path)
        if "processed" in kwargs.get("source", "processed"):
            pass
        else:
            # convert raw data
            for base_path, workernode, run in relevant_directories(path=path):
                current_path = os.path.join(os.path.join(base_path, workernode), run)
                converter = CSVReader()
                parser = TrafficStreamParser(
                    workernode=workernode,
                    run=run,
                    data_source=self,
                    path=current_path,
                    data_reader=converter)
                converter.parser = parser
                for traffic in self._read_stream(
                    path=current_path,
                    data_path=os.path.join(os.path.join(
                        kwargs.get("data_path", self.default_path), workernode), run),
                    workernode=workernode,
                    run=run,
                    stateful=kwargs.get("stateful", False),
                    pattern="^[0-9]{10}-traffic.log-[0-9]{8}",
                    converter=converter
                ):
                    yield traffic

    def jobs(self, **kwargs):
        """
        :param path:
        :param source:
        :param pattern:
        :param stateful:
        :return:
        """
        path = kwargs.get("path", self.default_path)
        if "processed" in kwargs.get("source", "processed"):
            converter = CSVReader()
            for base_path, workernode, run in relevant_directories(path=path):
                current_path = os.path.join(os.path.join(base_path, workernode), run)
                for dir_entry in sorted(os.listdir(current_path)):
                    m = re.match(kwargs.get("pattern", "(\d*)-process.csv"), dir_entry)
                    if m:
                        yield self.read_job(
                            path=current_path,
                            name=m.group(1),
                            converter=converter
                        )
        else:
            # convert raw data
            for base_path, workernode, run in relevant_directories(path=path):
                current_path = os.path.join(os.path.join(base_path, workernode), run)
                converter = CSVReader()
                parser = ProcessStreamParser(
                    workernode=workernode,
                    run=run,
                    data_source=self,
                    path=current_path,
                    data_reader=converter)
                converter.parser = parser
                for job in self._read_stream(
                        path=os.path.join(os.path.join(base_path, workernode), run),
                        data_path=os.path.join(os.path.join(
                            kwargs.get("data_path", self.default_path), workernode), run),
                        workernode=workernode,
                        run=run,
                        stateful=kwargs.get("stateful", False),
                        pattern="^[0-9]{10}-process.log-[0-9]{8}",
                        converter=converter
                ):
                    yield job

    def write_traffic(self, **kwargs):
        """
        :param path:
        :param data:
        :return:
        """
        traffic = kwargs.get("data", None)
        path = pathutils.ensureDirectory(kwargs.get("path", self.default_path))
        base_path = os.path.join(os.path.join(path, traffic["workernode"]), traffic["run"])
        pathutils.ensureDirectory(base_path)
        with open(os.path.join(
            base_path, "%s-traffic.csv" % traffic["id"]
        ), "a") as traffic_file:
            # TODO: write something about creation
            header_initialized = False
            print(traffic["data"])
            for traffic_data in traffic["data"]:
                if not header_initialized:
                    # write header
                    if traffic["configuration"] is not None:
                        traffic_file.write("%s\n" % traffic["configuration"].getRow())
                    traffic_file.write("%s\n" % traffic_data.getHeader())
                    header_initialized = True
                traffic_file.write("%s\n" % traffic_data.getRow())

    def write_job(self, **kwargs):
        """
        :param path:
        :param data:
        :return:
        """
        job = kwargs.get("data", None)
        # TODO: ensure that this is a base path!
        path = pathutils.ensureDirectory(kwargs.get("path", self.default_path))
        base_path = os.path.join(os.path.join(path, job.workernode), job.run)
        pathutils.ensureDirectory(base_path)
        with open(os.path.join(
                base_path, "%s-process.csv" %job.db_id
        ), "w") as job_file:
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

    def _read_stream(self, path=None, data_path=None, workernode=None, run=None, converter=CSVReader(), stateful=False,
                     pattern=None):
        """
        :param path:
        :param data_path:
        :param workernode:
        :param run:
        :param converter:
        :param stateful:
        :param pattern:
        :return:
        """
        for dir_entry in sorted(os.listdir(path)):
            if re.match(pattern, dir_entry):
                for job in converter.parser.parse(path=os.path.join(path, dir_entry)):
                    yield job
        converter.parser.check_caches(path=data_path)
        for data in converter.parser.pop_data():
            yield data
        if stateful:
            converter.parser.archive_state(path=path)

    def read_job(self, path=None, name=None, converter=CSVReader()):
        """
        :param path:
        :param name:
        :param converter:
        :return:
        """
        parser = JobParser(data_reader=converter)
        converter.parser = parser
        return parser.parse(path=os.path.join(path, "%s-process.csv" % name))

    def _write_payload(self, **kwargs):
        """
        :param path:
        :param data:
        :return:
        """
        payload = kwargs.get("data", None)
        # TODO: some trouble with job_id here
        path = pathutils.ensureDirectory(kwargs.get("path", self.default_path))
        with open("%s/%s/%s/%s-process.csv" % (
                path, payload.workernode, payload.run, payload.job_id
        ), "w+") as payload_file:
                # TODO: write something about creation
                header_initialized = False
                for process in payload.processes():
                    if not header_initialized:
                        # write header
                        payload_file.write("%s\n" % process.getHeader())
                        header_initialized = True
                    payload_file.write("%s\n" % process.getRow())

    def archive(self, **kwargs):
        """
        :param path:
        :param data:
        :param name:
        """
        job = kwargs.get("data", None)
        path = kwargs.get("path", None)
        current_path = os.path.join(os.path.join(path, job.workernode), job.run)
        name = "%s.zip" % kwargs.get("name", "jobarchive")
        archive_path = os.path.join(current_path, name)
        try:
            with zipfile.ZipFile(archive_path, mode="a", allowZip64=True) as zf:
                process_source = os.path.join(current_path, "%s-process.csv" % job.db_id)
                traffic_source = os.path.join(current_path, "%s-traffic.csv" % job.db_id)
                if os.path.isfile(process_source) and \
                        os.path.isfile(traffic_source):
                    zf.write(process_source, os.path.basename(process_source))
                    zf.write(traffic_source, os.path.basename(traffic_source))
                if zf.testzip() is None:
                    os.remove(process_source)
                    os.remove(traffic_source)
                else:
                    logging.getLogger(self.__class__.__name__).critical(
                        "something is wrong with zipfile %s for file %s" %
                        (archive_path, zf.testzip())
                    )
        except zipfile.BadZipfile as e:
            logging.getLogger(self.__class__.__name__).critical(
                "%s: Received bad zipfile error for zipfile %s" % (e, archive_path))
            sys.exit(1)
        except zipfile.LargeZipFile as e:
            logging.getLogger(self.__class__.__name__).critical(
                "%s: Received large zipfile error for zipfile %s" % (e, archive_path))
            sys.exit(1)
