import re
import logging

from evenmoreutils import string as stringutils

class Traffic(object):
    def __init__ (self, conn=None, pid=None, ppid=None, uid=None, tme=None,
            in_rate=None, out_rate=None, in_cnt=None, out_cnt=None, gpid=None,
            source_ip=None, dest_ip=None, source_port=None, dest_port=None,
            conn_cat=None, workernode=None, interval=20):
        self.tme = tme
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
                logging.info("exchanging traffic values for %s and %s" %(splittedSource, splittedTarget))
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
                logging.warn("Calculated internal IP (%s) does not match anything (%s and %s)" %(internalIP, splittedSource, splittedTarget))

        self.source_ip = splittedSource[0]
        self.source_port = splittedSource[1]
        self.dest_ip = splittedTarget[0]
        self.dest_port = splittedTarget[1]

        if ((re.match("^10\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})",splittedTarget[0]))
                or (re.match("^192\.108\.([0-9]{1,3})\.([0-9]{1,3})",splittedTarget[0]))):
            # internal interface
            self.conn_cat = "int"
        else:
            # external interface
            self.conn_cat = "ext"

    def getRow(self):
        return ("%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
                %(self.tme, stringutils.xstr(self._pid),
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
        return ("tme,pid,ppid,uid,in_rate,out_rate,in_cnt,out_cnt,gpid,"\
                "source_ip,dest_ip,source_port,dest_port,conn_cat")

