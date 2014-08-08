import re

from evenmoreutils import string as stringutils

class Traffic(object):
    def __init__ (self, conn=None, pid=None, ppid=None, uid=None, tme=None,
            in_rate=None, out_rate=None, in_cnt=None, out_cnt=None, gpid=None,
            source_ip=None, dest_ip=None, source_port=None, dest_port=None,
            conn_cat=None):
        self.tme = tme
        self.pid = pid
        self.ppid = ppid
        self.uid = uid
        self.gpid = gpid
        self.source_ip = source_ip
        self.dest_ip = dest_ip
        self.source_port = source_port
        self.dest_port = dest_port
        self.conn_cat = conn_cat
        self.in_rate = in_rate
        self.out_rate = out_rate
        self.in_cnt = in_cnt
        self.out_cnt = out_cnt
        if conn:
          self.setConnection(conn)

    def setConnection(self, conn):
        splittedConnection = conn.split("-")
        splittedSource = splittedConnection[0].split(":")
        splittedTarget = splittedConnection[1].split(":")
        self.source_ip = splittedSource[0]
        self.source_port = splittedSource[1]
        self.dest_ip = splittedTarget[0]
        self.dest_port = splittedTarget[1]

        if (re.match("^10\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})",splittedTarget[0]))
            or (re.match("^192\.108\.([0-9]{1,3})\.([0-9]{1,3})",splittedTarget[0])):
            # internal interface
            self.conn_cat = "int"
        else:
            # external interface
            self.conn_cat = "ext"

    def getRow(self):
        return ("%d,%s,%s,%s,%s,%s,%s,%s,%d,%s,%s,%s,%s,%s"
                %(self.tme, stringutils.xstr(self.pid),
                    stringutils.xstr(self.ppid), stringutils.xstr(self.uid),
                    stringutils.xstr(self.in_rate),
                    stringutils.xstr(self.out_rate),
                    stringutils.xstr(self.in_cnt),
                    stringutils.xstr(self.out_cnt),
                    self.gpid, stringutils.xstr(self.source_ip),
                    stringutils.xstr(self.dest_ip),
                    stringutils.xstr(self.source_port),
                    stringutils.xstr(self.dest_port),
                    stringutils.xstr(self.conn_cat)))

    def getHeader(self):
        return ("tme,pid,ppid,uid,in_rate,out_rate,in_cnt,out_cnt,gpid,"\
                "source_ip,dest_ip,source_port,dest_port,conn_cat")

