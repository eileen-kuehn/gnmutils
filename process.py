from evenmoreutils import string as stringutils

class Process(object):
    def __init__ (self, name=None, cmd=None, pid=None, ppid=None, uid=None,
            tme=None, exit_tme=None, error_code=None, signal=None,
            gpid=None, job_id=None, valid=True, int_in_volume=None, int_out_volume=None,
            ext_in_volume=None, ext_out_volume=None, tree_depth=None, process_type=None, color=None):
        self.name = name
        self.cmd = cmd
        self.pid = int(pid)
        self.ppid = int(ppid)
        self.uid = int(uid)
        self.tme = tme
        self.exit_tme = exit_tme
        self.error_code = error_code
        self.signal = signal
        self.gpid = int(gpid)
        self.job_id = job_id
        self.valid = valid
        self.int_in_volume = stringutils.xfloat(int_in_volume)
        self.int_out_volume = stringutils.xfloat(int_out_volume)
        self.ext_in_volume = stringutils.xfloat(ext_in_volume)
        self.ext_out_volume = stringutils.xfloat(ext_out_volume)
        self.tree_depth = tree_depth
        self.process_type = process_type
        self.color = color

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
        return ("tme,exit_tme,pid,ppid,gpid,uid,name,cmd,error_code,"\
                "signal,valid,int_in_volume,int_out_volume,ext_in_volume,"\
                "ext_out_volume,tree_depth,process_type,color")

