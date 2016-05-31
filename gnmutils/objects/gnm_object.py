def check_id(value=None):
    """
    Method converts any id value to an integer, if this fails, 0 is returned.

    :param value: value to convert into int
    :return: int value or 0
    """
    try:
        return int(value)
    except TypeError:
        return 0


def check_tme(value=None):
    """
    Method converts any timestamp value to an integer, if this fails, 0 is returned.

    :param value: tme to convert into int
    :return: int value or 0
    """
    try:
        return int(value)
    except TypeError:
        return 0


class GNMObject(object):
    default_key_type = {}

    def __init__(self, pid, ppid, uid, tme, gpid=None, **kwargs):
        self.pid = self._convert_to_default_type("pid", pid)
        self.ppid = self._convert_to_default_type("ppid", ppid)
        self.uid = self._convert_to_default_type("uid", uid)
        self.tme = self._convert_to_default_type("tme", tme)
        self.gpid = self._convert_to_default_type("gpid", gpid)

    def getRow(self):
        """
        Returns the values that can be printed into CSV. The formatting is comma-separated.
        The order is conform to header order.

        :return: row as comma-separated string of values
        """
        raise NotImplementedError

    def getHeader(self):
        """
        Returns the header that can be printed into CSV. The formatting is comma-separated.
        The order conforms to row order.

        :return: header as comma-separated string of fields
        """
        raise NotImplementedError

    @staticmethod
    def default_header(length, **kwargs):
        """
        Returns the default header of the current GNMObject in dependency of the expected length.
        This is mainly done for conversion between different versions of the GNM toolchain.

        :param length: expected length of header
        :return: dictionary with position of header fields
        """
        raise NotImplementedError

    def _convert_to_default_type(self, key, value):
        try:
            result = self.default_key_type[key](value)
        except TypeError:
            if not value:
                result = self.default_key_type[key]()
            else:
                raise
        except KeyError:
            pass
        return result

    def __repr__(self):
        return self.getRow()
