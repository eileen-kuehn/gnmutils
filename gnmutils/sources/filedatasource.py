"""
This module implements a data source that solely works based on the file system.
"""
import time
import inspect
import pickle
import zipfile
import os
import re
import logging
import sys

from gnmutils.sources.datasource import DataSource
from gnmutils.reader.csvreader import CSVReader
from gnmutils.parser.jobparser import JobParser
from gnmutils.parser.processstreamparser import ProcessStreamParser
from gnmutils.parser.trafficstreamparser import TrafficStreamParser
from gnmutils.parser.trafficparser import TrafficParser
from gnmutils.parser.networkstatisticsparser import NetworkStatisticsParser
from gnmutils.utils import relevant_directories
from gnmutils.exceptions import FilePathException

from evenmoreutils import path as pathutils
from evenmoreutils import csv as csvutils


class FileDataSource(DataSource):
    """
    The class FileDataSource works based on the filesystem and does not depend on a database.
    Attention: Currently it is not possible to get files that are only managed on the filesystem
    into the database by using implemented methods/classes.
    """
    default_path = None

    def is_available(self):
        return True

    def object_data(self, path, **kwargs):
        """
        :param path:
        :param pattern:
        :return:
        """
        for dir_entry in sorted(os.listdir(path)):
            if re.search(kwargs.get("pattern", ".pkl"), dir_entry):
                file_path = os.path.join(kwargs.get("path", self.default_path), dir_entry)
                logging.getLogger(self.__class__.__name__).debug(
                    "reading %s for object data", file_path
                )
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

    def traffic(self, **kwargs):
        """
        :param path:
        :param name:
        :return:
        """
        path = kwargs.get("path", self.default_path)
        name = kwargs.get("name", None)
        if name is not None:
            return self.read_traffic(
                path=path,
                name=name
            )

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
            for base_path, workernode, run, _ in relevant_directories(path=path):
                current_path = os.path.join(os.path.join(base_path, workernode), run)
                converter = CSVReader()
                parser = TrafficStreamParser(
                    workernode=workernode,
                    run=run,
                    data_source=self,
                    path=current_path,
                    data_reader=converter
                )
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
            for base_path, workernode, run, filename in relevant_directories(path=path):
                current_path = os.path.join(os.path.join(base_path, workernode), run)
                if filename:
                    for job in self.read_job(
                        path=current_path,
                        name=filename,
                        converter=converter
                    ):
                        yield job
                else:
                    for dir_entry in sorted(os.listdir(current_path)):
                        matches = re.match(kwargs.get("pattern", "(\d*)-process.csv"), dir_entry)
                        if matches:
                            for job in self.read_job(
                                path=current_path,
                                name=matches.group(1),
                                converter=converter
                            ):
                                yield job
        else:
            # convert raw data
            for base_path, workernode, run, _ in relevant_directories(path=path):
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
                        path=current_path,
                        data_path=os.path.join(os.path.join(
                            kwargs.get("data_path", self.default_path), workernode), run),
                        workernode=workernode,
                        run=run,
                        stateful=kwargs.get("stateful", False),
                        pattern="^[0-9]{10}-process.log-[0-9]{8}",
                        converter=converter
                ):
                    yield job

    def network_statistics(self, **kwargs):
        """
        :param path:
        :param stateful:
        :return:
        """
        path = kwargs.get("path", self.default_path)
        for base_path, workernode, run, _ in relevant_directories(path=path):
            current_path = os.path.join(os.path.join(base_path, workernode), run)
            converter = CSVReader()
            parser = NetworkStatisticsParser(
                workernode=workernode,
                run=run,
                data_source=self,
                path=current_path,
                data_reader=converter
            )
            converter.parser = parser
            for statistics in self._read_stream(
                    path=current_path,
                    workernode=workernode,
                    run=run,
                    stateful=kwargs.get("stateful", False),
                    pattern="^[0-9]{10}-(process|traffic).log-[0-9]{8}",
                    converter=converter
            ):
                yield statistics

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
            comment_string = "# Created by %s (%s) on %s" % (
                self.__class__.__name__,
                inspect.currentframe().f_code.co_name,
                time.strftime("%Y%m%d")
            )
            if traffic["configuration"] is not None:
                header_data = "%s\n%s\n%s" % (
                    comment_string,
                    traffic["configuration"].getRow(),
                    traffic["data"][0].getHeader()
                )
            else:
                header_data = "%s\n%s" % (
                    comment_string,
                    traffic["data"][0].getHeader()
                )
            for traffic_data in traffic["data"]:
                csvutils.dumpToFile(
                    file=traffic_file,
                    data="%s" % traffic_data.getRow(),
                    header="%s" % header_data
                )

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
        with open(os.path.join(base_path, "%s-process.csv" % job.db_id), "w") as job_file:
            header_initialized = False
            for process in job.processes():
                if not header_initialized:
                    # write header
                    comment_string = "# Created by %s (%s) on %s" % (
                        self.__class__.__name__,
                        inspect.currentframe().f_code.co_name,
                        time.strftime("%Y%m%d")
                    )
                    job_file.write("%s\n" % comment_string)
                    job_file.write("%s\n" % job.configuration.getRow())
                    job_file.write("%s\n" % process.getHeader())
                    header_initialized = True
                job_file.write("%s\n" % process.getRow())

    def write_payload(self, **kwargs):
        self._write_payload(**kwargs)

    def write_payload_result(self, **kwargs):
        logging.getLogger(self.__class__.__name__).warn(
            "writing of payload results to filesystem is not supported"
        )

    def _read_stream(self, path=None, data_path=None, workernode=None, run=None,
                     converter=CSVReader(), stateful=False, pattern=None):
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
                for data_object in converter.parser.parse(path=os.path.join(path, dir_entry)):
                    yield data_object
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
        parser = JobParser(
            data_source=self,
            data_reader=converter,
            path=path,
            name=name)
        converter.parser = parser
        if ".csv" in name:
            return parser.parse(path=os.path.join(path, name))
        return parser.parse(path=os.path.join(path, "%s-process.csv" % name))

    def read_traffic(self, path, name, converter=CSVReader()):
        """
        :param path:
        :param name:
        :param converter:
        :return:
        """
        parser = TrafficParser(data_reader=converter)
        converter.parser = parser
        try:
            file_path = os.path.join(path, "%s-traffic.csv" % name)
        except AttributeError:
            raise FilePathException(value="path=%s, name=%s" % (path, name))
        return parser.parse(path=file_path)

    def _write_payload(self, **kwargs):
        """
        Attention: Currently the patterns %s-process.csv and %s-traffic.csv are statically assumed.

        :param path:
        :param data:
        :return:
        """
        payload = kwargs.get("data", None)
        # TODO: some trouble with job_id here
        path = pathutils.ensureDirectory(kwargs.get("path", self.default_path))
        current_path = pathutils.ensureDirectory(
            os.path.join(os.path.join(path, payload.workernode), payload.run)
        )
        with open(os.path.join(
                current_path, "%s-process.csv" % payload.db_id
        ), "w") as process_file, open(os.path.join(
            current_path, "%s-traffic.csv" % payload.db_id
        ), "w") as traffic_file:
            for process in payload.processes():
                # write process information
                if process_file.tell() == 0:
                    comment_string = "# Created by %s (%s) on %s" % (
                        self.__class__.__name__,
                        inspect.currentframe().f_code.co_name,
                        time.strftime("%Y%m%d")
                    )
                    process_file.write("%s\n" % comment_string)
                    # write header
                    process_file.write("%s\n" % process.getHeader())
                process_file.write("%s\n" % process.getRow())
                # write traffic information
                for traffic in process.traffic:
                    if traffic_file.tell() == 0:
                        comment_string = "# Created by %s (%s) on %s" % (
                            self.__class__.__name__,
                            inspect.currentframe().f_code.co_name,
                            time.strftime("%Y%m%d")
                        )
                        # write header
                        traffic_file.write("%s\n" % comment_string)
                        traffic_file.write("%s\n" % traffic.getHeader())
                    traffic_file.write("%s\n" % traffic.getRow())

    def write_network_statistics(self, **kwargs):
        network_statistics = kwargs.get("data", None)
        if len(network_statistics) > 0:
            stat = network_statistics.values()[0]
            path = pathutils.ensureDirectory(kwargs.get("path", self.default_path))
            current_path = pathutils.ensureDirectory(
                os.path.join(os.path.join(path, stat.workernode), stat.run)
            )
            with open(os.path.join(current_path, "network_statistics.csv"), "w") as statistics_file:
                for tme in sorted(network_statistics.keys()):
                    # write header
                    if statistics_file.tell() == 0:
                        comment_string = "# Created by %s (%s) on %s" % (
                            self.__class__.__name__,
                            inspect.currentframe().f_code.co_name,
                            time.strftime("%Y%m%d")
                        )
                        statistics_file.write("%s\n" % comment_string)
                        statistics_file.write("%s\n" % network_statistics[tme].getHeader())
                    statistics_file.write("%s\n" % network_statistics[tme].getRow())

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
            with zipfile.ZipFile(archive_path, mode="a", allowZip64=True) as zip_file:
                process_source = os.path.join(current_path, "%s-process.csv" % job.db_id)
                traffic_source = os.path.join(current_path, "%s-traffic.csv" % job.db_id)
                if os.path.isfile(process_source) and \
                        os.path.isfile(traffic_source):
                    zip_file.write(process_source, os.path.basename(process_source))
                    zip_file.write(traffic_source, os.path.basename(traffic_source))
                if zip_file.testzip() is None:
                    os.remove(process_source)
                    os.remove(traffic_source)
                else:
                    logging.getLogger(self.__class__.__name__).critical(
                        "something is wrong with zipfile %s for file %s",
                        archive_path, zip_file.testzip()
                    )
        except zipfile.BadZipfile as e:
            logging.getLogger(self.__class__.__name__).critical(
                "%s: Received bad zipfile error for zipfile %s", e, archive_path)
            sys.exit(1)
        except zipfile.LargeZipFile as e:
            logging.getLogger(self.__class__.__name__).critical(
                "%s: Received large zipfile error for zipfile %s", e, archive_path)
            sys.exit(1)
