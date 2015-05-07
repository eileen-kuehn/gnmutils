from evenmoreutils import string as stringutils

class Process(object):
    def __init__ (self, name=None, cmd=None, pid=None, ppid=None, uid=None,
            tme=None, exit_code=None, gpid=None, state=None):
        self.name = name
        self.cmd = cmd
        self.pid = int(pid)
        self.ppid = int(ppid)
        self.uid = int(uid)
        self.tme = tme
        self.exit_code = stringutils.xint(exit_code)
        self.gpid = int(gpid)
        self.state = stringutils.xstr(state)

    def getRow(self):
        return ("%s,%d,%d,%d,%s,%s,%s,%s,%d"
                %(stringutils.xstr(self.tme), self.pid, self.ppid, self.uid,
                self.name, self.cmd, stringutils.xint(self.exit_code),
                stringutils.xstr(self.state), self.gpid))

    def getHeader(self):
        return ("tme,pid,ppid,uid,name,cmd,exit_code,state,gpid")

