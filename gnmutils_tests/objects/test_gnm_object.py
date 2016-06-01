import unittest

from gnmutils.objects.gnm_object import GNMObject, check_tme, check_id
from gnmutils.exceptions import ArgumentNotDefinedException


class TestGNMObject(unittest.TestCase):
    def test_check_id(self):
        self.assertEqual(check_id("1"), 1)
        self.assertEqual(check_id("-1"), -1)
        self.assertEqual(check_id(None), 0)
        self.assertEqual(check_id("name"), 0)

    def test_check_tme(self):
        self.assertEqual(check_tme("1"), 1)
        self.assertEqual(check_tme(None), 0)
        self.assertEqual(check_tme("name"), 0)

    def test_creation(self):
        gnm_object = GNMObject()
        self.assertEqual(gnm_object.pid, 0)
        self.assertEqual(gnm_object.ppid, 0)
        self.assertEqual(gnm_object.gpid, 0)
        self.assertEqual(gnm_object.tme, 0)
        self.assertEqual(gnm_object.uid, 0)

    def test_get_row(self):
        gnm_object = GNMObject()
        self.assertRaises(NotImplementedError, gnm_object.getRow)

    def test_get_header(self):
        gnm_object = GNMObject()
        self.assertRaises(NotImplementedError, gnm_object.getHeader)

    def test_default_header(self):
        self.assertRaises(NotImplementedError, GNMObject.default_header)

    def test_key_error(self):
        gnm_object = GNMObject()
        self.assertRaises(ArgumentNotDefinedException, gnm_object._convert_to_default_type,
                          "test", "muh")

    def test_repr(self):
        gnm_object = GNMObject()
        self.assertRaises(NotImplementedError, gnm_object.__repr__)
