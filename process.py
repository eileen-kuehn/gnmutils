from evenmoreutils import string as stringutils

class Process(object):
    def __init__ (self, name=None, cmd=None, pid=None, ppid=None, uid=None,
            tme=None, error_code=None, gpid=None, state=None, exit_tme=None):
        self.name = name
        self.cmd = cmd
        self.pid = int(pid)
        self.ppid = int(ppid)
        self.uid = int(uid)
        self.tme = tme
        self.error_code = error_code
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
        return ("%s,%s,%d,%d,%d,%d,%s,%s,%s,%s,%d,%s,%s,%s,%s,%s,%s,%s"
                %(stringutils.xstr(self.tme),
                    stringutils.xstr(self.exit_tme), self.pid,
                    self.ppid, self.gpid, self.uid, self.name,
                    self.cmd, stringutils.xstr(self.error_code),
                    stringutils.xstr(self.signal), int(self.isComplete()),
                    self.int_in_volume, self.int_out_volume,
                    self.ext_in_volume, self.ext_out_volume,
                    stringutils.xint(self.tree_depth),stringutils.xstr(self.process_type),
                    stringutils.xstr(self.color)))

    def getHeader(self):
        return ("tme,pid,ppid,uid,name,cmd,exit_code,state,gpid")
        return {"tme": 0, "pid": 1, "ppid": 2, "uid": 3, "name": 4, "cmd": 5, "exit_code": 6, "state": 7, "gpid": 8}

