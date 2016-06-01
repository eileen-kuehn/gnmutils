"""
The module offers convenience methods to access objects from the database.
"""
import logging

from gnmutils.db.dbobjects import DBWorkernodeObject, DBConfigurationObject, DBAffiliationObject

from dbutils.sqlcommand import SQLCommand
from dbutils.exceptions import UniqueConstrainedViolatedException


class DBOperator(object):
    """
    The class :py:class:`DBOperator` offers convenience methods to load and look for different
    domain objects for GNM workflow.
    """
    def __init__(self, data_source=None):
        self._data_source = data_source

    def load_one(self, data=None, **kwargs):
        """
        Method to load one specific object defined by :py:attr:`data` from the actual database.

        :param data: a database object describing the data to load
        :param kwargs: additional arguments
        :return: loaded object from database
        """
        sql_command = kwargs.get("sql_command", SQLCommand(dataSource=self._data_source))
        db_object = data
        try:
            db_object = sql_command.findOne(db_object)
        except Exception:
            logging.getLogger(self.__class__.__name__).info("load_one: object has not been found")
            # TODO: create specific exception
            raise
        else:
            return db_object

    def load_job(self, data=None, **kwargs):
        """
        This method takes care on loading a job from the database that is described by the given
        attribute :py:attr:`data`. For a job, it is checked that the job to load has a `tme`
        bigger than `data.tme` and smaller then `data.exit_tme`.
        If neither `last_tme` nor `exit_tme` is given, the `tme` is sorted descending and the first
        one is returned.

        Attention: only first of the selected jobs is returned.

        :param data: description of object to load from database
        :param kwargs: additional arguments
        :return: object loaded from database
        """
        sql_command = kwargs.get("sql_command", SQLCommand(dataSource=self._data_source))
        job_object = data
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

    def load_or_create_affiliation(self, data=None, **kwargs):
        """
        Method to either load an already existing affiliation from the database specified by given
        `uid` in :py:attr:`data` or create a knew one for unknown user.

        :param data: data specifying the uid to use
        :param kwargs: additional arguments
        :return: loaded or created affiliation
        :rtype: :py:class:`DBAffiliationObject`
        :raise: Exception
        """
        sql_command = kwargs.get("sql_command", SQLCommand(dataSource=self._data_source))
        affiliation_object = DBAffiliationObject(uid=data, name="unknown")
        try:
            affiliation_object = self.load_one(sql_command=sql_command, data=affiliation_object)
        except Exception:
            try:
                sql_command.startTransaction()
                affiliation_object = sql_command.save(affiliation_object)
                sql_command.commitTransaction()
            except Exception:
                sql_command.rollbackTransaction()
                raise
        return affiliation_object

    def load_or_create_workernode(self, data=None, **kwargs):
        """
        Method to either load an already existing workernode from the database specified by given
        `name` in :py:attr:`data` or create a knew one.

        :param data: data specifying the `name` to use
        :param kwargs: additional arguments
        :return: loaded or created workernode
        :rtype: :py:class:`DBWorkernodeObject`
        :raise: Exception
        """
        sql_command = kwargs.get("sql_command", SQLCommand(dataSource=self._data_source))
        workernode_object = DBWorkernodeObject(name=data)
        try:
            workernode_object = self.load_one(sql_command=sql_command, data=workernode_object)
        except Exception:
            try:
                sql_command.startTransaction()
                workernode_object = sql_command.save(workernode_object)
                sql_command.commitTransaction()
            except Exception:
                sql_command.rollbackTransaction()
                raise
        return workernode_object

    def load_or_create_configuration(self, data=None, **kwargs):
        """
        Method to either load an already existing configuration from the database specified by given
        :py:attr:`data` or create a knew one.

        :param data: data specifying the configuration
        :param kwargs: additional arguments
        :return: loaded or created configuration
        :rtype: :py:class:`DBConfigurationObject`
        :raise: Exception
        """
        sql_command = kwargs.get("sql_command", SQLCommand(dataSource=self._data_source))
        config = data
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

    def save_or_update(self, data=None, **kwargs):
        """
        Method to save or update an database object given in :py:attr:`data`.

        :param data: object to save or update
        :param kwargs: additional arguments
        :return: saved/updated database object
        :rtype: :py:class:`DBObject`
        """
        sql_command = kwargs.get("sql_command", SQLCommand(dataSource=self._data_source))
        db_object = data
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
