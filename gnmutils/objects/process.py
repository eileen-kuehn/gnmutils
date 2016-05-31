"""
This module implements a single process that is tracked by GNM tool
"""
from evenmoreutils import strings as stringutils

from gnmutils.objects.gnm_object import GNMObject, check_id, check_tme
from gnmutils.exceptions import ProcessMismatchException


class Process(GNMObject):
    """
    A :py:class:`Process` is one of the most important objects within the GNM workflow. It is
    the actual UNIX process that is being monitored. Each process has some attributes as well
    as :py:attrib:`traffic` attached.
    Associated processes can be read into a single :py:class:`Job`. Then their hierarchy becomes
    directly visible.
    """
    default_key_type = {
        'name': stringutils.xstr,
        'cmd': stringutils.xstr,
        'pid': check_id,
        'ppid': check_id,
        'uid': check_id,
        'gpid': check_id,
        'tme': check_tme,
        'exit_tme': check_tme,
        'error_code': int,
        'signal': int,
        'exit_code': int,
        'state': stringutils.xstr,
        'job_id': str,
        'int_in_volume': stringutils.xfloat,
        'int_out_volume': stringutils.xfloat,
        'ext_in_volume': stringutils.xfloat,
        'ext_out_volume': stringutils.xfloat,
        'valid': int,
        # not relevant in many cases
        'tree_depth': int,
        'process_type': str,
        'color': str,
        'traffic': list
    }

    def __init__(self, name=None, cmd=None, pid=None, ppid=None, uid=None, tme=None, exit_tme=None,
                 error_code=None, signal=None, exit_code=0, gpid=None, state=None, job_id=None,
                 int_in_volume=None, int_out_volume=None, ext_in_volume=None, ext_out_volume=None,
                 tree_depth=None, process_type=None, color=None, valid=False, traffic=None):
        GNMObject.__init__(self, pid=pid, ppid=ppid, uid=uid, tme=tme, gpid=gpid)
        self.name = self._convert_to_default_type("name", name)
        self.cmd = self._convert_to_default_type("cmd", cmd)

        self.exit_tme = self._convert_to_default_type("exit_tme", exit_tme)

        self.state = self._convert_to_default_type("state", state)

        self._valid = valid
        self._traffic = traffic or []

        # set exit_code first, but can be overwritten by error_code and signal
        if exit_code is not None:
            self._set_exit_code(exit_code)
        self.error_code = self._convert_to_default_type("error_code", error_code)
        self.signal = self._convert_to_default_type("signal", signal)

        self.job_id = self._convert_to_default_type("job_id", job_id)
        self.tree_depth = self._convert_to_default_type("tree_depth", tree_depth)
        self.process_type = self._convert_to_default_type("process_type", process_type)
        self.color = self._convert_to_default_type("color", color)

        self.int_in_volume = self.default_key_type["int_in_volume"](int_in_volume)
        self.int_out_volume = self.default_key_type["int_out_volume"](int_out_volume)
        self.ext_in_volume = self.default_key_type["ext_in_volume"](ext_in_volume)
        self.ext_out_volume = self.default_key_type["ext_out_volume"](ext_out_volume)

    @staticmethod
    def process_from_row(row):
        """
        Convert all known items of a row to their appropriate types

        :param row: Row dictionary
        """
        for key, value in row.iteritems():
            try:
                row[key] = Process.default_key_type[key](value)
            except ValueError:
                if not value:  # empty string -> type default
                    row[key] = Process.default_key_type[key]()
                else:
                    raise
            except KeyError:
                pass
        return Process(**row)

    @property
    def traffic(self):
        """
        Method that returns access to associated traffic.

        :return: the attached traffic
        :rtype: list
        """
        return self._traffic

    @property
    def batchsystemId(self):
        """
        Method that returns the associated batchsystem ID if existent, otherwise `None`.

        :return: batchsystem ID, otherwise `None`
        :rtype: str
        """
        if "sge_shepherd" in self.cmd:
            return int(self.cmd.rpartition("-")[2])
        return None

    @property
    def exit_code(self):
        """
        Method that returns the exit code of the process.
        Attention: the exit code depends on the :py:attrib:`error_code` and :py:attrib:`signal` of
        the process. And is also needs to be finished, that the exit code can be existent.

        :return: exit_code
        :rtype: int
        """
        return self.error_code and self.signal and ((self.error_code << 8) + self.signal)
        
    @property
    def valid(self):
        """
        Method returns if the process seems to be valid.

        :return: valid
        :rtype: int
        """
        return int(self._valid)

    def getDuration(self):
        """
        Method returns the duration of the process. The duration is given by the start
        :py:attrib:`tme` and the :py:attrib:`exit_tme`.

        :return: duration
        :rtype: float
        """
        return self._valid and self.exit_tme >= self.tme and (self.exit_tme - self.tme)

    def getRow(self):
        return ("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%s,%s,%s,%s,%s,%s,%s" %
                (stringutils.xint(self.tme), stringutils.xint(self.exit_tme),
                 stringutils.xint(self.pid), stringutils.xint(self.ppid),
                 stringutils.xint(self.gpid), stringutils.xint(self.uid),
                 self.name, self.cmd, stringutils.xint(self.error_code),
                 stringutils.xint(self.signal), self.valid, self.int_in_volume,
                 self.int_out_volume, self.ext_in_volume, self.ext_out_volume,
                 stringutils.xint(self.tree_depth), stringutils.xstr(self.process_type),
                 stringutils.xstr(self.color), self.state))

    def getHeader(self):
        return "tme,exit_tme,pid,ppid,gpid,uid,name,cmd,error_code,"\
               "signal,valid,int_in_volume,int_out_volume,ext_in_volume,"\
               "ext_out_volume,tree_depth,process_type,color,state"

    def toProcessEvent(self):
        """
        Method converts the process into a process event. Depending on the current state, it is
        either a start or finishing event.

        :return: process event dict
        :rtype: dict
        """
        event_dict = {"name": self.name, "cmd": self.cmd, "pid": self.pid, "ppid": self.ppid,
                      "uid": self.uid, "gpid": self.gpid, "state": self.state}
        if "exit" in self.state:
            event_dict["tme"] = self.exit_tme
            event_dict["exit_code"] = self.exit_code
        else:
            event_dict["tme"] = self.tme
        return event_dict

    def addProcessEvent(self, name=None, cmd=None, pid=None, ppid=None, uid=None, tme=None,
                        exit_code=None, gpid=None, state=None):
        """
        Method to add a process event to finish the actual process.

        :param name: name of process
        :param cmd: cmd of process
        :param pid: pid of process
        :param ppid: ppid of process
        :param uid: uid of process
        :param tme: tme of process
        :param exit_code: exit_code of process
        :param gpid: gpid of process
        :param state: state of process
        """
        if self.pid and self.pid != pid and self.ppid != ppid and \
                        self.name not in name and self.cmd not in cmd:
            raise ProcessMismatchException
        if "exit" in state:
            if self.state != ".":
                self._valid = True
            self._set_exit_code(exit_code)
            self.exit_tme = tme
            self.state = state
        else:
            # maybe exit process event arrives first...
            if "exit" in self.state and state != ".":
                self._valid = True
            elif "exit" in self.state and state == ".":
                self._valid = False
            self.tme = self.tme or tme
        self.state = self.state or state
        self.name = self.name or name
        self.cmd = self.cmd or cmd
        self.pid = self.pid or pid
        self.ppid = self.ppid or ppid
        self.uid = self.uid or uid
        self.gpid = self.gpid or gpid

    def _set_exit_code(self, exit_code):
        exit_code = self._convert_to_default_type("exit_code", exit_code)
        self.error_code = exit_code >> 8
        self._signal = exit_code & 255

    def __repr__(self):
        return "%s: name (%s), cmd (%s), pid (%d), ppid (%d), uid (%d), gpid (%d), valid (%s), " \
               "tme (%d), exit_tme (%d), state (%s), error_code (%s), signal (%s), job_id (%s), " \
               "tree_depth (%s), process_type (%s), color (%s), int_in_volume (%s), " \
               "int_out_volume (%s), ext_in_volume (%s), ext_out_volume (%s)" % \
               (self.__class__.__name__, self.name, self.cmd, self.pid, self.ppid, self.uid,
                self.gpid, self.valid, self.tme, self.exit_tme, self.state,
                stringutils.xint(self._error_code), self.signal, self.job_id, self.tree_depth,
                self.process_type, self.color, self.int_in_volume, self.int_out_volume,
                self.ext_in_volume, self.ext_out_volume)
