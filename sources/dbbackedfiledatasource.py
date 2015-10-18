import os

from gnmutils.sources.filedatasource import FileDataSource
from gnmutils.db.dbobjects import DBJobObject, DBPayloadObject, DBPayloadResultObject
from gnmutils.db.dboperator import DBOperator

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
        if "raw" in kwargs.get("source", "processed"):
            for job in FileDataSource.jobs(self, **kwargs):
                yield job
        else:
            with SQLCommand(dataSource=self._db_data_source) as sqlCommand:
                job_object = DBJobObject(valid=True, completed=True)
                for job_result in sqlCommand.find(job_object):
                    path = os.path.join(
                        kwargs.get("output_path", self.default_path),
                        kwargs.get("pattern", "%s-process.csv" % job_result.id_value)
                    )
                    parsed_job = self._read_job(path=path)
                    if parsed_job:
                        yield parsed_job

    def read_job(self, **kwargs):
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
            logging.getLogger(self.__class__.__name__).debug("loaded job %d from database" % job_object.id_value)
            return self._read_job(path=os.path.join(self.default_path, "%s-process.csv" % job_object.id_value))

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
        payload = kwargs["data"]
        with SQLCommand(dataSource=self._db_data_source) as sqlCommand:
            payload_object = DBPayloadObject(payload_id=payload.job_id,
                                             tme=payload.tme,
                                             exit_tme=payload.exit_tme,
                                             exit_code=payload.exit_code,
                                             job_id=payload.job_id.split("-")[0])
            try:
                sqlCommand.startTransaction()
                payload_object = sqlCommand.save(payload_object)
                sqlCommand.commitTransaction()
                self._write_payload(kwargs)
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
