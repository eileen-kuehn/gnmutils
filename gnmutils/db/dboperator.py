import logging

from gnmutils.db.dbobjects import DBWorkernodeObject, DBConfigurationObject, DBAffiliationObject

from dbutils.sqlcommand import SQLCommand
from dbutils.exceptions import UniqueConstrainedViolatedException


class DBOperator(object):
    def __init__(self, data_source=None):
        self._data_source = data_source

    def load_one(self, **kwargs):
        sql_command = kwargs.get("sql_command", SQLCommand(dataSource=self._data_source))
        db_object = kwargs.get("data", None)
        try:
            db_object = sql_command.findOne(db_object)
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
            return next(sql_command.find(job_object), None)
        if job_object.exit_tme:
            job_object.add_filter('exit_tme', '>=')
        if job_object.last_tme:
            job_object.add_filter('last_tme', '>=')
        return sql_command.findOne(job_object)

    def load_or_create_affiliation(self, **kwargs):
        sql_command = kwargs.get("sql_command", SQLCommand(dataSource=self._data_source))
        affiliation_object = DBAffiliationObject(uid=kwargs.get("data", None), name="unknown")
        try:
            affiliation_object = self.load_one(sql_command=sql_command, data=affiliation_object)
        except Exception as e:
            try:
                sql_command.startTransaction()
                affiliation_object = sql_command.save(affiliation_object)
                sql_command.commitTransaction()
            except Exception as e:
                sql_command.rollbackTransaction()
                raise
        return affiliation_object

    def load_or_create_workernode(self, **kwargs):
        sql_command = kwargs.get("sql_command", SQLCommand(dataSource=self._data_source))
        workernode_object = DBWorkernodeObject(name=kwargs.get("data", None))
        try:
            workernode_object = self.load_one(sql_command=sql_command, data=workernode_object)
        except Exception as e:
            try:
                sql_command.startTransaction()
                workernode_object = sql_command.save(workernode_object)
                sql_command.commitTransaction()
            except Exception as e:
                sql_command.rollbackTransaction()
                raise
        return workernode_object

    def load_or_create_configuration(self, **kwargs):
        sql_command = kwargs.get("sql_command", SQLCommand(dataSource=self._data_source))
        config = kwargs.get("data", None)
        configuration_object = DBConfigurationObject(
            version=config.version,
            interval=config.interval,
            level=config.level,
            grouping=config.grouping,
            skip_other_pids=config.skip_other_pids)
        try:
            configuration_object = self.load_one(sql_command=sql_command, data=configuration_object)
        except Exception:
            try:
                sql_command.startTransaction()
                configuration_object = sql_command.save(configuration_object)
                sql_command.commitTransaction()
            except UniqueConstrainedViolatedException:
                sql_command.rollbackTransaction()
                configuration_object = self.load_one(
                    sql_command=sql_command,
                    data=configuration_object
                )
            except Exception:
                sql_command.rollbackTransaction()
                raise
        return configuration_object

    def save_or_update(self, **kwargs):
        sql_command = kwargs.get("sql_command", SQLCommand(dataSource=self._data_source))
        db_object = kwargs.get("data", None)
        if db_object and db_object.id_value:
            # update object
            try:
                sql_command.startTransaction()
                db_object = sql_command.update(db_object)
                sql_command.commitTransaction()
            except Exception as outer_exception:
                sql_command.rollbackTransaction()
                # TODO: this should not be needed, when updating, uid should be there
                try:
                    if db_object.uid and db_object.uid > 0:
                        self.load_or_create_affiliation(data=db_object.uid)
                except AttributeError:
                    logging.getLogger(self.__class__.__name__).info(
                        "Object %s has no attribute uid", db_object
                    )
                    # TODO: what else might be done?
                except Exception as base_exception:
                    logging.getLogger(self.__class__.__name__).warning(
                        "update: affiliation %d could not be created or loaded (%s - %s)",
                        db_object.uid, outer_exception, base_exception)
                    raise
                else:
                    try:
                        sql_command.startTransaction()
                        db_object = sql_command.update(db_object)
                        sql_command.commitTransaction()
                    except Exception as base_exception:
                        sql_command.rollbackTransaction()
                        logging.getLogger(self.__class__.__name__).warning(
                            "final update: object %d could not be updated (%s)",
                            db_object.id_value, base_exception)
                        raise
        else:
            # save object
            try:
                sql_command.startTransaction()
                db_object = sql_command.save(db_object)
                sql_command.commitTransaction()
            except UniqueConstrainedViolatedException as unique_exception:
                description_object = db_object.getDescriptionObject()
                description_object = self.load_one(data=description_object)
                logging.getLogger(self.__class__.__name__).warning(
                    "%s\nold: %s\nnew: %s", unique_exception, description_object, db_object
                )
            except Exception as base_exception:
                logging.getLogger(self.__class__.__name__).warning(base_exception)
                sql_command.rollbackTransaction()
                try:
                    if db_object.uid and db_object.uid > 0:
                        self.load_or_create_affiliation(data=db_object.uid)
                except AttributeError:
                    logging.getLogger(self.__class__.__name__).info(
                        "Object %s has no attribute uid", db_object
                    )
                    # TODO: what else might be done?
                except Exception as exception:
                    logging.getLogger(self.__class__.__name__).warning(
                        "save: object %s could not be saved (%s - %s)",
                        db_object, base_exception, exception
                    )
                    raise
                else:
                    try:
                        sql_command.startTransaction()
                        db_object = sql_command.save(db_object)
                        sql_command.commitTransaction()
                    except Exception as exception:
                        sql_command.rollbackTransaction()
                        logging.getLogger(self.__class__.__name__).warning(
                            "final save: object %s could not be saved (%s)",
                            db_object, exception
                        )
                        raise
        return db_object
