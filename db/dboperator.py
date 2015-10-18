from gnmutils.db.dbobjects import DBWorkernodeObject, DBConfigurationObject, DBAffiliationObject
from dbutils.sqlcommand import SQLCommand

from utility.exceptions import *


class DBOperator(object):
    def __init__(self, data_source=None):
        self._data_source = data_source

    def load_one(self, **kwargs):
        sqlCommand = kwargs.get("sql_command", SQLCommand(dataSource=self._data_source))
        db_object = kwargs.get("data", None)
        try:
            db_object = sqlCommand.findOne(db_object)
        except Exception as e:
            logging.getLogger(self.__class__.__name__).info("load_one: object has not been found")
            # TODO: create specific exception
            raise
        else:
            return db_object

    def load_job(self, **kwargs):
        sql_command = kwargs.get("sql_command", SQLCommand(dataSource=self._data_source))
        job_object = kwargs.get("data", None)
        job_object.add_filter('tme', '<=')
        if not job_object.exit_tme and not job_object.last_tme:
            job_object.add_order_by('tme', 'DESC')
            logging.getLogger(self.__class__.__name__).debug("trying to find ordered jobs")
            return sql_command.find(job_object).next()
        if job_object.exit_tme:
          job_object.add_filter('exit_tme', '>=')
        if job_object.last_tme:
          job_object.add_filter('last_tme', '>=')
        return sql_command.findOne(job_object)

    def load_or_create_affiliation(self, **kwargs):
        sqlCommand = kwargs.get("sql_command", SQLCommand(dataSource=self._data_source))
        affiliation_object = DBAffiliationObject(uid=kwargs.get("data", None), name="unknown")
        try:
            affiliation_object = self.load_one(sql_command=sqlCommand, data=affiliation_object)
        except Exception as e:
            try:
                sqlCommand.startTransaction()
                affiliation_object = sqlCommand.save(affiliation_object)
                sqlCommand.commitTransaction()
            except Exception as e:
                sqlCommand.rollbackTransaction()
                raise
        return affiliation_object

    def load_or_create_workernode(self, **kwargs):
        sqlCommand = kwargs.get("sql_command", SQLCommand(dataSource=self._data_source))
        workernode_object = DBWorkernodeObject(name=kwargs.get("data", None))
        try:
            workernode_object = self.load_one(sql_command=sqlCommand, data=workernode_object)
        except Exception as e:
            try:
                sqlCommand.startTransaction()
                workernode_object = sqlCommand.save(workernode_object)
                sqlCommand.commitTransaction()
            except Exception as e:
                sqlCommand.rollbackTransaction()
                raise
        return workernode_object

    def load_or_create_configuration(self, **kwargs):
        sqlCommand = kwargs.get("sql_command", SQLCommand(dataSource=self._data_source))
        config = kwargs.get("data", None)
        configuration_object = DBConfigurationObject(
            version=config.version,
            interval=config.interval,
            level=config.level,
            grouping=config.grouping,
            skip_other_pids=config.skip_other_pids)
        try:
            configuration_object = self.load_one(sql_command=sqlCommand, data=configuration_object)
        except Exception as e:
            try:
                sqlCommand.startTransaction()
                configuration_object = sqlCommand.save(configuration_object)
                sqlCommand.commitTransaction()
            except Exception as e:
                sqlCommand.rollbackTransaction()
                raise
        return configuration_object

    def save_or_update(self, **kwargs):
        sqlCommand = kwargs.get("sql_command", SQLCommand(dataSource=self._data_source))
        db_object = kwargs.get("data", None)
        if db_object and db_object.id_value:
            # update object
            try:
                sqlCommand.startTransaction()
                db_object = sqlCommand.update(db_object)
                sqlCommand.commitTransaction()
            except Exception as e:
                sqlCommand.rollbackTransaction()
                try:
                    if db_object.uid and db_object.uid > 0:
                        self.load_or_create_affiliation(data=db_object.uid)
                except Exception:
                    logging.getLogger(self.__class__.__name__).warning(
                        "save_or_update: affiliation %d could not be created or loaded (%s)" % (db_object.uid, e))
                    raise
                else:
                    try:
                        sqlCommand.startTransaction()
                        db_object = sqlCommand.update(db_object)
                        sqlCommand.commitTransaction()
                    except Exception:
                        sqlCommand.rollbackTransaction()
                        logging.getLogger(self.__class__.__name__).warning(
                            "save_or_update: object %d could not be updated (%s)" % (db_object.id_value, e))
                        raise
        else:
            # save object
            try:
                sqlCommand.startTransaction()
                db_object = sqlCommand.save(db_object)
                sqlCommand.commitTransaction()
            except Exception as e:
                sqlCommand.rollbackTransaction()
                print(e)
                try:
                    if db_object.uid and db_object.uid > 0:
                        self.load_or_create_affiliation(data=db_object.uid)
                except Exception:
                    logging.getLogger(self.__class__.__name__).warning(
                        "save_or_update: affiliation %d could not be created or loaded (%s)" % (db_object.uid, e))
                    raise
                else:
                    try:
                        sqlCommand.startTransaction()
                        db_object = sqlCommand.save(db_object)
                        sqlCommand.commitTransaction()
                    except Exception:
                        sqlCommand.rollbackTransaction()
                        logging.getLogger(self.__class__.__name__).warning(
                            "save_or_update: object %s could not be saved (%s)" % (db_object, e))
                        raise
        return db_object