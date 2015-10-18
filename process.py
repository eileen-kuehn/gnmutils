from evenmoreutils import string as stringutils

from gnmutils.exceptions import ProcessMismatchException


class Process(object):
    def __init__ (self, name=None, cmd=None, pid=None, ppid=None, uid=None,
            tme=None, exit_tme=None, error_code=None, signal=None, exit_code=0,
            gpid=None, state=None, job_id=None, int_in_volume=None, int_out_volume=None,
            ext_in_volume=None, ext_out_volume=None, tree_depth=None,
            process_type=None, color=None, valid=False):
        self._name = self._check_is_none(value=name)
        self._cmd = self._check_is_none(value=cmd)
        self._pid = self._check_is_none(value=pid)
        self._ppid = self._check_is_none(value=ppid)
        self._uid = self._check_is_none(value=uid)
        self._gpid = self._check_is_none(value=gpid)
        self._valid = valid

        self._tme = self._check_is_none(value=tme)
        self._exit_tme = self._check_is_none(value=exit_tme)
        self._state = stringutils.xstr(state)

        # set exit_code first, but can be overwritten by error_code and signal
        if self._check_is_none(value=str(exit_code)) is not None:
            self._setExitCode(exit_code)
        else:
            self._error_code = self._check_is_none(value=error_code)
            self._signal = self._check_is_none(value=signal)

        self.job_id = self._check_is_none(value=job_id)
        self.tree_depth = self._check_is_none(value=tree_depth)
        self.process_type = self._check_is_none(value=process_type)
        self.color = self._check_is_none(value=color)

        self.int_in_volume = stringutils.xfloat(int_in_volume)
        self.int_out_volume = stringutils.xfloat(int_out_volume)
        self.ext_in_volume = stringutils.xfloat(ext_in_volume)
        self.ext_out_volume = stringutils.xfloat(ext_out_volume)

    def _check_is_none(self, value=None):
        if value is not None and len(str(value)) > 0:
            return str(value)
        return None

    @property
    def batchsystemId(self):
        if "sge_shepherd" in self.cmd:
            return int(self.cmd.rpartition("-")[2])
        return None

    @property
    def state(self):
        return stringutils.xstr(self._state)

    @property
    def cmd(self):
        return stringutils.xstr(self._cmd)

    @property
    def name(self):
        return stringutils.xstr(self._name)

    @property
    def pid(self):
        try:
            return int(self._pid)
        except TypeError:
            return 0

    @property
    def ppid(self):
        try:
            return int(self._ppid)
        except TypeError:
            return 0

    @property
    def uid(self):
        try:
            return int(self._uid)
        except TypeError:
            return 0

    @property
    def gpid(self):
        try:
            return int(self._gpid)
        except TypeError:
            return 0

    @property
    def tme(self):
        try:
            return int(self._tme)
        except TypeError:
            return 0

    @property
    def exit_tme(self):
        try:
            return int(self._exit_tme)
        except TypeError:
            return 0

    @property
    def signal(self):
        return int(self._signal)

    @property
    def exit_code(self):
        return self._error_code and self._signal and ((self._error_code << 8) + self._signal)
        
    @property
    def valid(self):
        return int(self._valid)

    def getDuration(self):
        return self._valid and (self.exit_tme - self.tme)

    def getRow(self):
        return ("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%s,%s,%s,%s,%s,%s,%s"
                %(stringutils.xint(self._tme), stringutils.xint(self._exit_tme),
                    stringutils.xint(self._pid), stringutils.xint(self._ppid),
                    stringutils.xint(self._gpid), stringutils.xint(self._uid),
                    self.name, self.cmd, stringutils.xint(self._error_code),
                    stringutils.xint(self._signal), self.valid, self.int_in_volume,
                    self.int_out_volume, self.ext_in_volume, self.ext_out_volume,
                    stringutils.xint(self.tree_depth), stringutils.xstr(self.process_type),
                    stringutils.xstr(self.color), self._state))

    def getHeader(self):
        return ("tme,exit_tme,pid,ppid,gpid,uid,name,cmd,error_code,"\
                "signal,valid,int_in_volume,int_out_volume,ext_in_volume,"\
                "ext_out_volume,tree_depth,process_type,color,state")

    def toProcessEvent(self):
        event_dict = {"name": self.name, "cmd": self.cmd, "pid": self.pid, "ppid": self.ppid,
                      "uid": self.uid, "gpid": self.gpid, "state": self.state}
        if "exit" in self.state:
            event_dict["tme"] = self.exit_tme
            event_dict["exit_code"] = self.exit_code
        else:
            event_dict["tme"] = self.tme
        return event_dict

    def addProcessEvent(self, name=None, cmd=None, pid=None, ppid=None,
            uid=None, tme=None, exit_code=None, gpid=None, state=None):
        if self._pid and self.pid != pid and self.ppid != ppid and \
                        self.name not in name and self.cmd not in cmd:
            raise ProcessMismatchException
        if "exit" in state:
            if self._state != ".":
                self._valid = True
            self._setExitCode(exit_code)
            self._exit_tme = tme
            self._state = state
        else:
            # maybe exit process event arrives first...
            if "exit" in self._state and state != ".":
                self._valid = True
            elif "exit" in self._state and state == ".":
                self._valid = False
            self._tme = self._tme or tme
        self._state = self._state or state
        self._name = self._name or name
        self._cmd = self._cmd or cmd
        self._pid = self._pid or pid
        self._ppid = self._ppid or ppid
        self._uid = self._uid or uid
        self._gpid = self._gpid or gpid

    def _setExitCode(self, exit_code):
        self._error_code = int(exit_code) >> 8
        self._signal = int(exit_code) & 255

    def __repr__(self):
        return "%s: name (%s), cmd (%s), pid (%d), ppid (%d), uid (%d), gpid (%d), valid (%s), tme (%d), " \
               "exit_tme (%d), state (%s), error_code (%s), signal (%s), job_id (%s), tree_depth (%s), " \
               "process_type (%s), color (%s), int_in_volume (%s), int_out_volume (%s), ext_in_volume (%s), " \
               "ext_out_volume (%s)" % (
            self.__class__.__name__, self.name, self.cmd, self.pid, self.ppid, self.uid, self.gpid, self.valid,
            self.tme, self.exit_tme, self.state, stringutils.xint(self._error_code), self.signal, self.job_id,
            self.tree_depth, self.process_type, self.color, self.int_in_volume, self.int_out_volume, self.ext_in_volume,
            self.ext_out_volume)
