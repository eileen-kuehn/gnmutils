from dbutils.dbobjects import DBJobObject

class Job(object):
    def __init__ (self, id=None, job_id=None, run=None, gpid=None, start_tme=None, exit_tme=None, last_tme=None, uid=None, workernode_id=None, configuration_id=None):
        self.id = id
        self.run = run
        self.job_id = job_id
        self.gpid = gpid
        self.start_tme = start_tme
        self.exit_tme = exit_tme
        self.uid = uid
        self.last_tme = last_tme
        self.workernode_id = workernode_id
        self.configuration_id = configuration_id

    def isComplete(self):
        return self.start_tme and self.exit_tme

    def isValid(self):
        return self.exit_tme >= self.last_tme

    def getDuration(self):
        return self.exit_tme - self.start_tme

    def toDAO(self):
        jobDict = vars(self)
        jobDict['completed'] = self.isComplete()
        jobDict['valid'] = self.isValid()
        return DBJobObject(**jobDict)

