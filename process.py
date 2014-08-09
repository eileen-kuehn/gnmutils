from evenmoreutils import string as stringutils

class Process(object):
    def __init__ (self, name=None, cmd=None, pid=None, ppid=None, uid=None,
            tme=None, exit_tme=None, error_code=None, signal=None,
            gpid=None, job_id=None, valid=True):
        self.name = name
        self.cmd = cmd
        self.pid = pid
        self.ppid = ppid
        self.uid = uid
        self.tme = tme
        self.exit_tme = exit_tme
        self.error_code = error_code
        self.signal = signal
        self.gpid = gpid
        self.job_id = job_id
        self.valid = valid

    def setValid(self, valid):
        self.valid = valid

    def getDuration(self):
        return self._valid and (self.exit_tme - self.tme)

    def setExitCode(self, exitCode):
        exitCode = int(exitCode)
        self.error_code = exitCode >> 8
        self.signal = exitCode & 255

    def isComplete(self):
        return self.valid and (self.tme and self.exit_tme) > 0

    def getRow(self):
        return ("%s,%s,%d,%d,%d,%d,%s,%s,%s,%s,%d"
                %(stringutils.xstr(self.tme),
                    stringutils.xstr(self.exit_tme), int(self.pid),
                    int(self.ppid), int(self.gpid), int(self.uid), self.name,
                    self.cmd, stringutils.xstr(self.error_code),
                    stringutils.xstr(self.signal), int(self.isComplete())))

    def getHeader(self):
        return ("tme,exit_tme,pid,ppid,gpid,uid,name,cmd,error_code,"\
                "signal,valid")

