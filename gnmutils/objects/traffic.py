"""
Module implements a representation of traffic that is attached to a :py:class:`Process`.
"""
import re
import logging

from gnmutils.objects.gnm_object import GNMObject, check_tme, check_id
from gnmutils.exceptions import ArgumentNotDefinedException, TrafficMismatchException
from gnmutils.utility import strings as stringutils


class Traffic(GNMObject):
    """
    Implementation of a traffic entry that is monitored from GNM tool.
    """
    default_key_type = {
        'pid': check_id,
        'ppid': check_id,
        'uid': check_id,
        'gpid': check_id,
        'tme': check_tme,
        'in_rate': float,
        'out_rate': float,
        'in_cnt': int,
        'out_cnt': int,
        'source_ip': stringutils.xstr,
        'dest_ip': stringutils.xstr,
        'source_port': stringutils.xint,
        'dest_port': stringutils.xint,
        'conn_cat': stringutils.xstr,
        'workernode': stringutils.xstr,
        'interval': int,
        'ext_in_rate': float,
        'int_in_rate': float,
        'ext_out_rate': float,
        'int_out_rate': float,
        'ext_in_cnt': int,
        'int_in_cnt': int,
        'ext_out_cnt': int,
        'int_out_cnt': int,
        'conn': stringutils.xstr,
    }

    def __init__(self, conn=None, pid=None, ppid=None, uid=None, tme=None, in_rate=None,
                 out_rate=None, in_cnt=None, out_cnt=None, gpid=None, source_ip=None, dest_ip=None,
                 source_port=None, dest_port=None, conn_cat=None, workernode=None, interval=20,
                 ext_in_rate=None, int_in_rate=None, ext_out_rate=None, int_out_rate=None,
                 ext_in_cnt=None, int_in_cnt=None, ext_out_cnt=None, int_out_cnt=None):
        GNMObject.__init__(self, pid=pid, ppid=ppid, uid=uid, tme=tme, gpid=gpid)
        self.source_ip = self._convert_to_default_type("source_ip", source_ip)
        self.dest_ip = self._convert_to_default_type("dest_ip", dest_ip)
        self.source_port = self._convert_to_default_type("source_port", source_port)
        self.dest_port = self._convert_to_default_type("dest_port", dest_port)
        self.conn_cat = self._convert_to_default_type("conn_cat", conn_cat)
        self.in_rate = self._convert_to_default_type("in_rate", in_rate)
        self.out_rate = self._convert_to_default_type("out_rate", out_rate)
        self.in_cnt = self._convert_to_default_type("in_cnt", in_cnt)
        self.out_cnt = self._convert_to_default_type("out_cnt", out_cnt)
        self.interval = self._convert_to_default_type("interval", interval)
        if conn:
            self.setConnection(conn=conn, workernode=workernode)
        if ext_out_cnt or ext_in_cnt or ext_out_rate or ext_in_rate:
            if (self._convert_to_default_type("ext_out_cnt", ext_out_cnt) > 0 or
                    self._convert_to_default_type("ext_in_cnt", ext_in_cnt) > 0 or
                    self._convert_to_default_type("ext_out_rate", ext_out_rate) > 0 or
                    self._convert_to_default_type("ext_in_rate", ext_in_rate) > 0):
                if "ext" not in self.conn_cat:
                    logging.getLogger(self.__class__.__name__).warning(
                        "The calculated connection category %s does not match that of "
                        "log file %s for ip %s", self.conn_cat, "ext", self.dest_ip
                    )
                self.in_rate = self._convert_to_default_type("ext_in_rate", ext_in_rate)
                self.out_rate = self._convert_to_default_type("ext_out_rate", ext_out_rate)
                self.in_cnt = self._convert_to_default_type("ext_in_cnt", ext_in_cnt)
                self.out_cnt = self._convert_to_default_type("ext_out_cnt", ext_out_cnt)
        if int_out_cnt or int_in_cnt or int_out_rate or int_in_rate:
            if (self._convert_to_default_type("int_out_cnt", int_out_cnt) > 0 or
                    self._convert_to_default_type("int_in_cnt", int_in_cnt) > 0 or
                    self._convert_to_default_type("int_out_rate", int_out_rate) > 0 or
                    self._convert_to_default_type("int_in_rate", int_in_rate)):
                if "int" not in self.conn_cat:
                    logging.getLogger(self.__class__.__name__).warning(
                        "The calculated connection category %s does not match that of "
                        "log file %s for ip %s", self.conn_cat, "int", self.dest_ip
                    )
                self.in_rate = self._convert_to_default_type("int_in_rate", int_in_rate)
                self.out_rate = self._convert_to_default_type("int_out_rate", int_out_rate)
                self.in_cnt = self._convert_to_default_type("int_in_cnt", int_in_cnt)
                self.out_cnt = self._convert_to_default_type("int_out_cnt", int_out_cnt)

    @staticmethod
    def from_dict(row):
        """
        Convert all known items of a row to their appropriate types

        :param row: Row dictionary
        # TODO: still need to do stuff that is done in init!
        """
        for key, value in row.iteritems():
            try:
                row[key] = Traffic.default_key_type[key](value)
            except ValueError:
                if not value:  # empty string -> type default
                    row[key] = Traffic.default_key_type[key]()
                else:
                    raise
            except KeyError:
                raise ArgumentNotDefinedException(key, value)
        return Traffic(**row)

    def setConnection(self, conn, workernode=None):
        """
        Method to set the connection of traffic object.

        :param str conn: the connection to set
        :param str workernode: the workernode to set
        """
        splitted_connection = conn.split("-")
        splitted_source = splitted_connection[0].split(":")
        splitted_target = splitted_connection[1].split(":")
        if workernode:
            last_ip_part = ".".join(map(str, map(int, workernode.split("-")[1:])))
            internal_ip = "10.1." + last_ip_part
            if internal_ip in splitted_source[0]:
                # everything is fine
                pass
            elif internal_ip in splitted_target[0]:
                # splitted values need to be exchanged
                logging.getLogger(self.__class__.__name__).info(
                    "exchanging traffic values for %s and %s", splitted_source, splitted_target
                )
                tmp = splitted_source
                splitted_source = splitted_target
                splitted_target = tmp

                tmp_rate = self.in_rate
                self.in_rate = self.out_rate
                self.out_rate = tmp_rate

                tmp_cnt = self.in_cnt
                self.in_cnt = self.out_cnt
                self.out_cnt = tmp_cnt
            else:
                logging.getLogger(self.__class__.__name__).warning(
                    "Calculated internal IP (%s) does not match anything (%s and %s)",
                    internal_ip, splitted_source, splitted_target
                )
                raise TrafficMismatchException(conn=conn, workernode=workernode)

        self.source_ip = self._convert_to_default_type("source_ip", splitted_source[0])
        self.source_port = self._convert_to_default_type("source_port", splitted_source[1])
        self.dest_ip = self._convert_to_default_type("dest_ip", splitted_target[0])
        self.dest_port = self._convert_to_default_type("dest_port", splitted_target[1])

        if ((re.match("^10\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})", self.dest_ip)) or
                (re.match("^192\.108\.([0-9]{1,3})\.([0-9]{1,3})", self.dest_ip))):
            # internal interface
            self.conn_cat = self._convert_to_default_type("conn_cat", "int")
        else:
            # external interface
            self.conn_cat = self._convert_to_default_type("conn_cat", "ext")

    def getRow(self):
        return ("%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
                % (self.tme, stringutils.xstr(self.pid),
                   stringutils.xstr(self.ppid), stringutils.xstr(self.uid),
                   stringutils.xstr(self.in_rate),
                   stringutils.xstr(self.out_rate),
                   stringutils.xstr(self.in_cnt),
                   stringutils.xstr(self.out_cnt),
                   stringutils.xstr(self.gpid),
                   stringutils.xstr(self.source_ip),
                   stringutils.xstr(self.dest_ip),
                   stringutils.xstr(self.source_port),
                   stringutils.xstr(self.dest_port),
                   stringutils.xstr(self.conn_cat)))

    def getHeader(self):
        return (
            "tme,pid,ppid,uid,in_rate,out_rate,in_cnt,out_cnt,gpid,source_ip,dest_ip,source_port,"
            "dest_port,conn_cat"
        )

    @staticmethod
    def default_header(length=None, **kwargs):
        # TODO: here I seem to have some problems... I guess I have three different types of header?
        # * one see above
        # * two in the bottom
        # :(
        if length > 10:
            return {"tme": 0, "pid": 1, "ppid": 2, "uid": 3, "int_in_rate": 4, "ext_in_rate": 5,
                    "int_out_rate": 6, "ext_out_rate": 7, "int_in_cnt": 8, "ext_in_cnt": 9,
                    "int_out_cnt": 10, "ext_out_cnt": 11, "conn": 12, "gpid": 13}
        return {"tme": 0, "pid": 1, "ppid": 2, "uid": 3, "in_rate": 4, "out_rate": 5, "in_cnt": 6,
                "out_cnt": 7, "conn": 8, "gpid": 9}
