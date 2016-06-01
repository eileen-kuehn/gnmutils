"""
This module implements different representations of GNM objects for database handling.
"""
from dbutils.dbobject import DBObject


class DBAffiliationObject(DBObject):
    """
    The :py:class:`DBAffiliationObject` represents the VO - uid mapping inside the database.
    """
    def __init__(self, uid=None, name=None):
        super(DBAffiliationObject, self).__init__("affiliation", {"uid": uid})
        self.name = name


class DBConfigurationObject(DBObject):
    """
    The :py:class:`DBConfigurationObject` represents the different configurations that were used
    for different runs of the GNM tool.
    """
    def __init__(self, id=None, version=None, interval=None, level=None, grouping=None,
                 skip_other_pids=None):
        super(DBConfigurationObject, self).__init__("configuration", {"id": id})
        self.version = version
        self.interval = interval
        self.level = level
        self.grouping = grouping
        self.skip_other_pids = skip_other_pids


class DBJobObject(DBObject):
    """
    The :py:class:`DBJobObject` represents the different jobs itself inside the database.
    """
    def __init__(self, id=None, job_id=None, run=None, completed=None, valid=None, uid=None,
                 gpid=None, tme=None, exit_tme=None, last_tme=None, workernode_id=None,
                 configuration_id=None):
        super(DBJobObject, self).__init__("job", {"id": id})
        self.job_id = job_id
        self.run = run
        self.gpid = gpid
        self.tme = tme
        self.exit_tme = exit_tme
        self.last_tme = last_tme
        self.completed = completed
        self.valid = valid
        self.uid = uid
        self.workernode_id = workernode_id
        self.configuration_id = configuration_id

    def unique_constraint_keys(self):
        return ["job_id", "tme", "run", "workernode_id"]


class DBWorkernodeObject(DBObject):
    """
    The :py:class:`DBWorkernodeObject` represents a single workernode within the database.
    """
    def __init__(self, id=None, name=None):
        super(DBWorkernodeObject, self).__init__("workernode", {"id": id})
        self.name = name


class DBPayloadObject(DBObject):
    """
    The :py:class:`DBPayloadObject` represents a single payload within the database. The payload
    itself is a child of a :py:class:`DBJobObject`.
    """
    def __init__(self, id=None, tme=None, exit_tme=None, exit_code=None,
                 job_id=None):
        super(DBPayloadObject, self).__init__("payload", {"id": id})
        self.tme = tme
        self.exit_tme = exit_tme
        self.exit_code = exit_code
        self.job_id = job_id


class DBPayloadResultObject(DBObject):
    """
    The :py:class:`DBPayloadResultObject` represents data was loaded from external sources and
    completing the knowledge of payloads.
    """
    def __init__(self, id=None, job_id=None, task_monitor_id=None, activity=None, tme=None,
                 exit_tme=None, exit_code=None, status_name=None, status_reason=None,
                 workernode_id=None, payload_id=None, time_offset_id=None):
        super(DBPayloadResultObject, self).__init__("payload_result", {"id": id})
        self.job_id = job_id
        self.task_monitor_id = task_monitor_id
        self.activity = activity
        self.tme = tme
        self.exit_tme = exit_tme
        self.exit_code = exit_code
        self.status_name = status_name
        self.status_reason = status_reason
        self.workernode_id = workernode_id
        self.payload_id = payload_id
        self.time_offset_id = time_offset_id

    def unique_constraint_keys(self):
        return ["job_id", "task_monitor_id", "workernode_id"]


class DBDataOffsetObject(DBObject):
    """
    The :py:class:`DBDataOffsetObject` represents the time offset of externally load data.
    """
    def __init__(self, id=None, dataoffset=None):
        super(DBDataOffsetObject, self).__init__("dataoffset", {"id": id})
        self.dataoffset = dataoffset
