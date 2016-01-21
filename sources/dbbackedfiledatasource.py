import os

from gnmutils.sources.filedatasource import FileDataSource
from gnmutils.db.dbobjects import DBJobObject, DBPayloadObject, DBPayloadResultObject, DBWorkernodeObject
from gnmutils.db.dboperator import DBOperator
from gnmutils.utils import *

from dbutils.datasource import DataSource as DBDataSource
from dbutils.sqlcommand import SQLCommand
from utility.exceptions import *


class DBBackedFileDataSource(FileDataSource):
    def __init__(self):
        FileDataSource.__init__(self)
        self._db_data_source = DBDataSource(connectionString="dbname=gnm user=gnm",
                                            providerName="PostgresDBProvider")
        self._db_operator = DBOperator(data_source=self._db_data_source)

    def is_available(self):
        try:
            connection = self._db_data_source.getNewConnection()
        except BasicError:
            return False
        return connection is not None

    def jobs(self, **kwargs):
        """
        :param path:
        :param data_path:
        :param source:
        :return:
        """
        if "raw" in kwargs.get("source", "processed"):
            for job in FileDataSource.jobs(self, **kwargs):
                yield job
        else:
            with SQLCommand(dataSource=self._db_data_source) as sqlCommand:
                path = kwargs.get("path", self.default_path)
                level = directory_level(path)
                job_object = DBJobObject(valid=True, completed=True)
                if level == RUN_LEVEL:
                    base_path, workernode, run = next(relevant_directories(path=path), (None, None, None))
                    job_object.run = run
                    workernode_object = self._db_operator.load_or_create_workernode(data=workernode)
                    job_object.workernode_id = workernode_object.id_value
                elif level == WORKERNODE_LEVEL:
                    workernode = os.path.split(path)[1]
                    workernode_object = self._db_operator.load_or_create_workernode(data=workernode)
                    job_object.workernode_id = workernode_object.id_value

                for job_result in sqlCommand.find(job_object):
                    current_path = path
                    if level == BASE_LEVEL:
                        # join different workernodes and runs
                        workernode_object = self._db_operator.load_one(DBWorkernodeObject(id=job_result.workernode_id))
                        current_path = os.path.join(os.path.join(path, workernode_object.name), job_result.run)
                    elif level == WORKERNODE_LEVEL:
                        # join different runs
                        current_path = os.path.join(path, job_result.run)

                    for job in FileDataSource.read_job(
                        self,
                        path=current_path,
                        name=job_result.id_value):
                        yield job

    def read_job(self, **kwargs):
        """
        :param data:
        :param path:
        :return:
        """
        job = kwargs.get("data", None)
        workernode_object = self._db_operator.load_or_create_workernode(data=job.workernode)
        configuration_object = self._db_operator.load_or_create_configuration(data=job.configuration)
        job_object = DBJobObject(
            run=job.run, gpid=job.gpid, tme=job.tme, workernode_id=workernode_object.id_value,
            configuration_id=configuration_object.id_value)
        try:
            job_object = self._db_operator.load_job(data=job_object)
        except:
            raise RethrowException("The job has not been found")
        else:
            if job_object is not None:
                logging.getLogger(self.__class__.__name__).debug("loaded job %d from database" % job_object.id_value)
                return FileDataSource.read_job(
                    self,
                    path=kwargs.get("path", self.default_path),
                    name=job_object.id_value)
            else:
                logging.getLogger(self.__class__.__name__).warning(
                        "did not find job (run=%s, gpid=%s, tme=%s, workernode_id=%s) in database" %
                        (job.run, job.gpid, job.tme, workernode_object.id_value))
                return None

    def write_job(self, **kwargs):
        job = kwargs["data"]
        workernode_object = self._db_operator.load_or_create_workernode(data=job.workernode)
        configuration_object = self._db_operator.load_or_create_configuration(data=job.configuration)
        job_object = DBJobObject(
            job_id=job.job_id, run=job.run, uid=job.uid, gpid=job.gpid, tme=job.tme, exit_tme=job.exit_tme,
            workernode_id=workernode_object.id_value, configuration_id=configuration_object.id_value,
            valid=job.is_valid(), last_tme=job.last_tme, completed=job.is_complete())
        try:
            self._db_operator.save_or_update(data=job_object)
            job.db_id = job_object.id_value
            FileDataSource.write_job(self, **kwargs)
        except:
            raise RethrowException("The job could not be created")
        else:
            logging.debug("saved job for index %s" % job_object.id_value)
            return job

    def write_payload(self, **kwargs):
        """
        :param path:
        :param data:
        :return:
        """
        payload = kwargs["data"]
        with SQLCommand(dataSource=self._db_data_source) as sqlCommand:
            payload_object = DBPayloadObject(id=payload.db_id,
                                             tme=payload.tme,
                                             exit_tme=payload.exit_tme,
                                             exit_code=payload.exit_code,
                                             job_id=payload.job_id)
            try:
                sqlCommand.startTransaction()
                payload_object = sqlCommand.save(payload_object)
                self._write_payload(**kwargs)
                sqlCommand.commitTransaction()
            except:
                sqlCommand.rollbackTransaction()
                raise RethrowException("The payload could not be created")
            else:
                logging.debug("saved payload for index %s" % payload_object.id_value)
                return payload_object

    def write_payload_result(self, **kwargs):
        # TODO: maybe put the operations into one single SQLCommand, how to realize?
        payload_result = kwargs["data"]
        with SQLCommand(dataSource=self._db_data_source) as sqlCommand:
            workernode_object = self._db_operator.load_or_create_workernode(sql_command=sqlCommand,
                                                                            data=kwargs.get("workernode", None))
            payload_result_object = DBPayloadResultObject(
                job_id=payload_result["jobId"],
                task_monitor_id=payload_result["TaskMonitorId"],
                activity=kwargs["activity"],
                tme=payload_result["StartedRunningTimeStamp"],
                exit_tme=payload_result["FinishedTimeStamp"],
                exit_code=payload_result["JobExecExitCode"],
                status_name=payload_result["gridStatusName"],
                status_reason=payload_result["gridStatusReason"],
                workernode_id=workernode_object.id_value
            )
            workernode_object = self._db_operator.save_or_update(sql_command=sqlCommand,
                                                                 data=payload_result_object)
        return workernode_object

    def job_description(self, **kwargs):
        job = kwargs.get("data", None)

        workernode_object = self._db_operator.load_or_create_workernode(data=job.workernode)
        job_object = DBJobObject(run=job.run, gpid=job.gpid, tme=job.tme, workernode_id=workernode_object.id_value)
        try:
            job_object = self._db_operator.load_job(data=job_object)
        except Exception as e:
            logging.getLogger(self.__class__.__name__).info("No matching job has been found (%s)" % e)
            return None
        else:
            # trying to fix a bug where job_object is none and therefore no access possible
            if job_object is None:
                return None
            job.db_id = job_object.id_value
            job.last_tme = max(job_object.exit_tme, job_object.last_tme)
        return job
