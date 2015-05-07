from evenmoreutils import string as stringutils

class Process(object):
    def __init__ (self, name=None, cmd=None, pid=None, ppid=None, uid=None,
            tme=None, exit_code=None, gpid=None, state=None, exit_tme=None):
        self.name = name
        self.cmd = cmd
        self.pid = int(pid)
        self.ppid = int(ppid)
        self.uid = int(uid)
        self.tme = tme
        self.exit_code = exit_code
        self.gpid = int(gpid)
        self.state = state
        self.exit_tme = exit_tme

    def setValid(self, valid):
        self.valid = valid

    def getDuration(self):
        return self.valid and (int(self.exit_tme) - int(self.tme))

    def setExitCode(self, exitCode):
        exitCode = int(exitCode)
        self.error_code = exitCode >> 8
        self.signal = exitCode & 255

    def isComplete(self):
        return self.valid and (self.tme and self.exit_tme) > 0

    def getRow(self):
        return ("%s,%d,%d,%d,%s,%s,%s,%s,%d"
                %(stringutils.xstr(self.tme),
                self.pid, self.ppid, self.uid, self.name, self.cmd,
                self.exit_code, self.state, self.gpid))

    def getHeader(self):
        return ("tme,pid,ppid,uid,name,cmd,exit_code,state,gpid")

