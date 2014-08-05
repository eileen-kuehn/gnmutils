class Process(object):
    def __init__ (self, name=None, cmd=None, pid=None, ppid=None, uid=None, start_tme=None, exit_tme=None, error_code=None, signal=None, gpid=None, job_id=None):
        self.name = name
        self.cmd = cmd
        self.pid = pid
        self.ppid = ppid
        self.uid = uid
        self.start_tme = start_tme
        self.exit_tme = exit_tme
        self.error_code = error_code
        self.signal = signal
        self.gpid = gpid
        self.job_id = job_id
        self._valid = True

    def setValid(self, valid):
        self._valid = valid

    def getDuration(self):
        return self._valid and (self.exit_tme - self.start_tme)

    def setExitCode(self, exitCode):
        exitCode = int(exitCode)
        self.error_code = exitCode >> 8
        self.signal = exitCode & 255

    def isComplete(self):
        return self._valid and (self.start_tme and self.exit_tme)

    def getRow(self):
        return "%d,%d,%d,%d,%d,%d,%s,%s,%d,%d,%d" %(int(self.start_tme), int(self.exit_tme), int(self.pid), int(self.ppid), int(self.gpid), int(self.uid), self.name, self.cmd, self.error_code, self.signal, int(self.isComplete()))

    def getHeader(self):
        return "start_tme,exit_tme,pid,ppid,gpid,uid,name,cmd,error_code,signal,valid"

