class Job(object):
    def __init__ (self, job_id=None, gpid=None, start_tme=None, exit_tme=None, uid=None):
        self.job_id = job_id
        self.gpid = gpid
        self.start_tme = start_tme
        self.exit_tme = exit_tme
        self.uid = uid

    def isComplete(self):
        return self.start_tme and self.exit_tme

    def getDuration(self):
        return self.exit_tme - self.start_tme

