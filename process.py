class Process(object):
    def __init__ (self, name=None, pid=None, ppid=None, uid=None, start_tme=None, exit_tme=None, error_code=None, signal=None, gpid=None, job_id=None):
        self.name = name
        self.pid = pid
        self.ppid = ppid
        self.uid = uid
        self.start_tme = start_tme
        self.exit_tme = exit_tme
        self.error_code = error_code
        self.signal = signal
        self.gpid = gpid
        self.job_id = job_id

    def getDuration(self):
        return self.exit_tme - self.start_tme

    def setExitCode(self, exitCode):
        self.error_code = exitCode >> 8
        self.signal = exitCode & 255

    def isComplete(self):
        return self.start_tme and self.exit_tme

