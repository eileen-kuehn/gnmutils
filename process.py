from evenmoreutils import string as stringutils

class ProcessEvent(object):
    def __init__ (self, process=None):
        self._process = process
        
    def getRow(self):
        return ("%d,%d,%d,%d,%s,%s,%d,%s,%d"
                %(self._process.tme, self._process.pid, self._process.ppid, self._process.uid,
                self._process.name, self._process.cmd, self._process.exit_code(), 
                self._process.state, self._process.gpid))
    
    def getProcessEventHeader(self):
        return ("tme,pid,ppid,uid,name,cmd,exit_code,state,gpid")

class Process(object):
    def __init__ (self, name=None, cmd=None, pid=None, ppid=None, uid=None,
            tme=None, exit_tme=None, error_code=None, signal=None, exit_code=0,
            gpid=None, state=None, job_id=None, int_in_volume=None, int_out_volume=None,
            ext_in_volume=None, ext_out_volume=None, tree_depth=None,
            process_type=None, color=None, valid=False):
        self._name = name
        self.cmd = cmd
        self._pid = pid
        self._ppid = ppid
        self._uid = uid
        self._gpid = gpid
        self._tme = tme
        self._exit_tme = exit_tme
        self.valid = valid
        self._state = stringutils.xstr(state)
        
        # set exit_code first, but can be overwritten by error_code and signal
        self._setExitCode(exit_code)
        self._error_code = error_code
        self._signal = signal
        
        self.job_id = job_id
        self.tree_depth = tree_depth
        self.process_type = process_type
        self.color = color
        
        self.int_in_volume = stringutils.xfloat(int_in_volume)
        self.int_out_volume = stringutils.xfloat(int_out_volume)
        self.ext_in_volume = stringutils.xfloat(ext_in_volume)
        self.ext_out_volume = stringutils.xfloat(ext_out_volume)
    
    @property
    def name(self):
        return stringutils.xstr(self._name)
        
    @property
    def pid(self):
        return int(self._pid)
        
    @property
    def ppid(self):
        return int(self._ppid)
        
    @property
    def uid(self):
        return int(self._uid)
        
    @property
    def gpid(self):
        return int(self._gpid)
        
    @property
    def tme(self):
        return int(self._tme)
        
    @property
    def exit_tme(self):
        return int(self._exit_tme)
        
    @property
    def exit_code(self):
        return int(self._exit_code)
        
    @property
    def signal(self):
        return int(self._signal)

    @property
    def exit_code(self):
        return self._error_code and self._signal and ((self._error_code << 8) + self._signal)
    
    def getDuration(self):
        return self.valid and (self.exit_tme - self.tme)
        
    def getRow(self):
        return ("%s,%s,%d,%d,%d,%d,%s,%s,%s,%s,%s,%d,%s,%s,%s,%s,%s,%s"
                %(stringutils.xint(self._tme),
                    stringutils.xint(self._exit_tme), stringutils.xint(self._pid),
                    stringutils.xint(self._ppid), stringutils.xint(self._gpid),
                    stringutils.xint(self._uid), self.name, self.cmd,
                    stringutils.xint(self._error_code), 
                    stringutils.xint(self._signal), self.int_in_volume,
                    self.valid, self.int_out_volume, self.ext_in_volume,
                    self.ext_out_volume, stringutils.xint(self.tree_depth),
                    stringutils.xstr(self.process_type), stringutils.xstr(self.color)))
                    
    def toProcessEvent(self):
        return ProcessEvent(process=self)
    
    def getHeader(self):
        return ("tme,exit_tme,pid,ppid,gpid,uid,name,cmd,error_code,"\
                "signal,int_in_volume,int_out_volume,ext_in_volume,"\
                "ext_out_volume,tree_depth,process_type,color")
        
    def addProcessEvent(self, name=None, cmd=None, pid=None, ppid=None, 
            uid=None, tme=None, exit_code=None, gpid=None, state=None):
        if "exit" == state:
            if self._state != ".":
                self.valid = True
            self._setExitCode(exit_code)
            self._exit_tme = tme
        else:
            # maybe exit process event arrives first...
            if "exit" == self._state and state != ".":
                self.valid = True
            self._tme = self._tme or tme
        self._state = self._state or state
        self._name = self._name or name
        self.cmd = self.cmd or cmd
        self._pid = self._pid or pid
        self._ppid = self._ppid or ppid
        self._uid = self._uid or uid
        self._gpid = self._gpid or gpid

    def _setExitCode(self, exit_code):
        self._error_code = int(exit_code) >> 8
        self._signal = int(exit_code) & 255

