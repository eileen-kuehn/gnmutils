import unittest

from gnmutils.objects.traffic import Traffic
from gnmutils.exceptions import ArgumentNotDefinedException, TrafficMismatchException


class TestTraffic(unittest.TestCase):
    def test_creation(self):
        traffic = Traffic()
        self.assertEqual(0, traffic.pid)
        self.assertEqual(0, traffic.ppid)
        self.assertEqual(0, traffic.uid)
        self.assertEqual(0, traffic.tme)
        self.assertEqual(0, traffic.gpid)
        self.assertEqual(20, traffic.interval)
        self.assertEqual("", traffic.source_ip)
        self.assertEqual("", traffic.source_port)

    def test_from_dict(self):
        traffic_header = "tme,pid,ppid,uid,in_rate,out_rate,in_cnt,out_cnt,gpid,source_ip,dest_ip,source_port,dest_port,conn_cat"
        traffic_row = "1405011326,30744,30742,14808,2.10142,0.225146,32,31,30726,,,,,"
        traffic = Traffic.from_dict(dict(zip(traffic_header.split(","), traffic_row.split(","))))
        self.assertIsNotNone(traffic)
        self.assertEqual(traffic_row, traffic.getRow())

        traffic_row = "1405011326,30744,30742,14808,muh,0.225146,32,31,30726,,,,,"
        self.assertRaises(ValueError, Traffic.from_dict, dict(zip(traffic_header.split(","), traffic_row.split(","))))

        traffic_header = "tme,pid,ppid,uid,in_rate,out_rate,in_cnt,out_cnt,gpid,source_ips,dest_ip,source_port,dest_port,conn_cat"
        traffic_row = "1405011326,30744,30742,14808,2.10142,0.225146,32,31,30726,,,,,"
        self.assertRaises(ArgumentNotDefinedException, Traffic.from_dict, dict(zip(traffic_header.split(","), traffic_row.split(","))))

    def test_connection(self):
        traffic = Traffic()
        traffic.setConnection("127.0.0.1:80-127.0.0.1:8080")
        self.assertEqual("127.0.0.1", traffic.source_ip)
        self.assertEqual("80", traffic.source_port)
        self.assertEqual("127.0.0.1", traffic.dest_ip)
        self.assertEqual("8080", traffic.dest_port)
        self.assertEqual("ext", traffic.conn_cat)

        traffic = Traffic()
        self.assertRaises(TrafficMismatchException, traffic.setConnection, "127.0.0.1:80-127.0.0.1:8080", workernode="c01-007-102")
        # TODO: what happens to the data?

        traffic = Traffic()
        traffic.setConnection("127.0.0.1:80-10.1.7.102:8080", workernode="c01-007-102")
        self.assertEqual("127.0.0.1", traffic.dest_ip)
        self.assertEqual("80", traffic.dest_port)
        self.assertEqual("10.1.7.102", traffic.source_ip)
        self.assertEqual("8080", traffic.source_port)
        self.assertEqual("ext", traffic.conn_cat)
