import re
import logging

from evenmoreutils import string as stringutils


class Traffic(object):
    def __init__(
            self, conn=None, pid=None, ppid=None, uid=None, tme=None, in_rate=None, out_rate=None, in_cnt=None,
            out_cnt=None, gpid=None, source_ip=None, dest_ip=None, source_port=None, dest_port=None, conn_cat=None,
            workernode=None, interval=20, ext_in_rate=None, int_in_rate=None, ext_out_rate=None, int_out_rate=None,
            ext_in_cnt=None, int_in_cnt=None, ext_out_cnt=None, int_out_cnt=None
    ):
        self.tme = int(tme)
        self._pid = pid
        self._ppid = ppid
        self._uid = uid
        self._gpid = gpid
        self.source_ip = source_ip
        self.dest_ip = dest_ip
        self.source_port = source_port
        self.dest_port = dest_port
        self.conn_cat = conn_cat
        self.in_rate = in_rate
        self.out_rate = out_rate
        self.in_cnt = in_cnt
        self.out_cnt = out_cnt
        self.interval = interval
        if conn:
            self.setConnection(conn=conn, workernode=workernode)
        if ((ext_out_cnt or ext_in_cnt or ext_out_rate or ext_in_rate) and
                (float(ext_out_cnt) > 0 or float(ext_in_cnt) > 0 or float(ext_out_rate) > 0 or float(ext_in_rate) > 0)):
            if "ext" not in self.conn_cat:
                logging.getLogger(self.__class__.__name__).warning(
                    "The calculated connection category %s does not match that of log file %s for ip %s" %
                    (self.conn_cat, "ext", self.dest_ip)
                )
            self.in_rate = ext_in_rate
            self.out_rate = ext_out_rate
            self.in_cnt = ext_in_cnt
            self.out_cnt = ext_out_cnt
        if ((int_out_cnt or int_in_cnt or int_out_rate or int_in_rate) and
                (float(int_out_cnt) > 0 or float(int_in_cnt) > 0 or float(int_out_rate) > 0 or float(int_in_rate) > 0)):
            if "int" not in self.conn_cat:
                logging.getLogger(self.__class__.__name__).warning(
                    "The calculated connection category %s does not match that of log file %s for ip %s" %
                    (self.conn_cat, "int", self.dest_ip)
                )
            self.in_rate = int_in_rate
            self.out_rate = int_out_rate
            self.in_cnt = int_in_cnt
            self.out_cnt = int_out_cnt

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

    def setConnection(self, conn=None, workernode=None):
        splittedConnection = conn.split("-")
        splittedSource = splittedConnection[0].split(":")
        splittedTarget = splittedConnection[1].split(":")
        if workernode:
            lastIPPart = ".".join(map(str, map(int, workernode.split("-")[1:])))
            internalIP = "10.1." + lastIPPart
            if internalIP in splittedSource[0]:
                # everything is fine
                pass
            elif internalIP in splittedTarget[0]:
                # splitted values need to be exchanged
                logging.getLogger(self.__class__.__name__).info(
                    "exchanging traffic values for %s and %s" %
                    (splittedSource, splittedTarget)
                )
                tmp = splittedSource
                splittedSource = splittedTarget
                splittedTarget = tmp

                tmpRate = self.in_rate
                self.in_rate = self.out_rate
                self.out_rate = tmpRate

                tmpCnt = self.in_cnt
                self.in_cnt = self.out_cnt
                self.out_cnt = tmpCnt
            else:
                logging.getLogger(self.__class__.__name__).warning(
                    "Calculated internal IP (%s) does not match anything (%s and %s)" %
                    (internalIP, splittedSource, splittedTarget)
                )

        self.source_ip = splittedSource[0]
        self.source_port = splittedSource[1]
        self.dest_ip = splittedTarget[0]
        self.dest_port = splittedTarget[1]

        if ((re.match("^10\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})", self.dest_ip)) or
                (re.match("^192\.108\.([0-9]{1,3})\.([0-9]{1,3})", self.dest_ip))):
            # internal interface
            self.conn_cat = "int"
        else:
            # external interface
            self.conn_cat = "ext"

    def getRow(self):
        return ("%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
                % (self.tme, stringutils.xstr(self._pid),
                   stringutils.xstr(self._ppid), stringutils.xstr(self._uid),
                   stringutils.xstr(self.in_rate),
                   stringutils.xstr(self.out_rate),
                   stringutils.xstr(self.in_cnt),
                   stringutils.xstr(self.out_cnt),
                   stringutils.xstr(self._gpid),
                   stringutils.xstr(self.source_ip),
                   stringutils.xstr(self.dest_ip),
                   stringutils.xstr(self.source_port),
                   stringutils.xstr(self.dest_port),
                   stringutils.xstr(self.conn_cat)))

    def getHeader(self):
        return (
            "tme,pid,ppid,uid,in_rate,out_rate,in_cnt,out_cnt,gpid,source_ip,dest_ip,source_port,dest_port,conn_cat"
        )

    @staticmethod
    def default_header(length=10):
        if length > 10:
            return {"tme": 0, "pid": 1, "ppid": 2, "uid": 3, "int_in_rate": 4, "ext_in_rate": 5, "int_out_rate": 6,
                    "ext_out_rate": 7, "int_in_cnt": 8, "ext_in_cnt": 9, "int_out_cnt": 10, "ext_out_cnt": 11,
                    "conn": 12, "gpid": 13}
        return {"tme": 0, "pid": 1, "ppid": 2, "uid": 3, "in_rate": 4, "out_rate": 5, "in_cnt": 6, "out_cnt": 7,
                "conn": 8, "gpid": 9}

    def __repr__(self):
        return self.getRow()
