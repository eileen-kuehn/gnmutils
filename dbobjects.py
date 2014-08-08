from dbutils.dbobject import DBObject

class DBAffiliationObject(DBObject):
  def __init__ (self, uid=None, name=None):
    super(DBAffiliationObject, self).__init__("affiliation", {"uid": uid})
    self.name = name

class DBConfigurationObject(DBObject):
  def __init__ (self, id=None, version=None, interval=None, config_name=None):
    super(DBConfigurationObject, self).__init__("configuration", {"id": id})
    self.version = version
    self.interval = interval
    self.config_name = config_name

class DBJobObject(DBObject):
  def __init__ (self, id=None, job_id=None, run=None, completed=None, valid=None, uid=None, gpid=None, start_tme=None, exit_tme=None, last_tme=None, workernode_id=None, configuration_id=None):
    super(DBJobObject, self).__init__("job", {"id": id})
    self.job_id = job_id
    self.run = run
    self.gpid = gpid
    self.start_tme = start_tme
    self.exit_tme = exit_tme
    self.last_tme = last_tme
    self.completed = completed
    self.valid = valid
    self.uid = uid
    self.workernode_id = workernode_id
    self.configuration_id = configuration_id

class DBWorkernodeObject(DBObject):
  def __init__ (self, id=None, name=None):
    super(DBWorkernodeObject, self).__init__("workernode", {"id": id})
    self.name = name

