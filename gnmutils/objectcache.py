"""
The :py:class:`ObjectCache` allows to sort values by a key and another value.
"""
import bisect
import logging

from gnmutils.exceptions import DataNotInCacheException


class ObjectCache(object):
    """
    The class :py:class:`ObjectCache` ...
    """
    def __init__(self):
        self._object_cache = {}
        self.faulty_nodes = set()
        self.unfound = set()

    def add_data(self, data=None, key=None, value=None, key_name="pid", value_name="tme"):
        """
        This method takes care to add the given data object into the cache. The :py:attr:`key` and
        :py:attr:`value` to be stored can be given. Otherwise the method tries to access the
        parameters directly by utilising :py:attr:`key_name` and :py:attr:`value_name`.

        Attention: The method also takes care, that the actual value is stored in data. If it is not
        available, it is extended.

        :param data: data object to store in cache
        :param key: discriminating key for data object
        :param value: discriminating value for key of data object
        :param str key_name: name of key, defaults to pid
        :param str value_name: name of value, defaults to tme
        """
        if key is None:
            key = getattr(data, key_name)
        if value is None:
            value = getattr(data, value_name)
        # Ensure that the actual discriminating value is also set in data object
        if getattr(data, value_name, None) is None:
            setattr(data, value_name, value)

        try:
            value_list = [getattr(process, value_name) for process in self._object_cache[key]]
            index = bisect.bisect_left(value_list, int(value))
            self._object_cache[key].insert(index, data)
        except KeyError:
            self._object_cache[key] = [data]

    def get_data(self, value=None, key=None, remember_error=False, value_name="tme"):
        """
        Method returns the closest matching data objects specified by given :py:attr:`key` and
        :py:attr:`value`. If no data is found, `None` is returned.
        If specified, unmatched keys are remembered for further reference.

        :param value: value to look for
        :param key: key to look for
        :param remember_error: remember unmatched keys, defaults to False
        :param value_name: name of attribute to which :py:attr:`value` is compared
        :return: closest data object, otherwise `None`
        """
        try:
            index = self.data_index(
                value=value,
                key=key,
                remember_error=remember_error,
                value_name=value_name
            )
        except DataNotInCacheException:
            raise
        if index is not None:
            return self._object_cache[key][index]
        return None

    def remove_data(self, data=None, key=None, key_name="pid"):
        """
        Method that removes a given data object from cache. Returns `True` if the object could be
        removed otherwise `False`.

        :param data: data object to be removed
        :param key: key where data is stored
        :param str key_name: attribute name in use as key, default pid
        :return: `True` if removal was successful, `False` otherwise
        :rtype: bool
        """
        if key is None:
            key = getattr(data, key_name)
        try:
            data_array = self._object_cache[key]
            data_array.remove(data)
            if len(self._object_cache[key]) == 0:
                del self._object_cache[key]
            return True
        except ValueError:
            pass
        except KeyError:
            pass
        return False

    def data_index(self, value=None, key=None, remember_error=False, value_name="tme"):
        """
        Method returns index of closest value specified by :py:attr:`key` and :py:attr:`value`.

        :param value: value to look for
        :param key: key where data is stored
        :param remember_error: remember mismatched keys, defaults to `False`
        :param value_name: attribute name in use for values, default tme
        :return: closest data object
        """
        try:
            data_array = self._object_cache[key]
            value_array = [getattr(node, value_name) for node in data_array]
            index = bisect.bisect_right(value_array, value) - 1
            return index
        except KeyError:
            if remember_error:
                self.faulty_nodes.add(key)
                logging.getLogger(self.__class__.__name__).info("error for %s (%d)", key, value)
            raise DataNotInCacheException(key=key, value=value)

    def addNodeObject(self, nodeObject):
        """
        deprecated
        :param nodeObject:
        """
        try:
            tmeList = [process.value.tme for process in self._object_cache[nodeObject.value.pid]]
            index = bisect.bisect_left(tmeList, int(nodeObject.value.tme))
            self._object_cache[nodeObject.value.pid].insert(index, nodeObject)
        except KeyError:
            self._object_cache[nodeObject.value.pid] = [nodeObject]

    def getNodeObject(self, tme=None, pid=None, rememberError=False):
        """
        deprecated
        :param tme:
        :param pid:
        :param rememberError:
        :return:
        """
        try:
            processArray = self._object_cache[pid]
            tmeArray = [node.value.tme for node in processArray]
            index = bisect.bisect_right(tmeArray, tme) - 1
            return processArray[index]
        except KeyError:
            if rememberError:
                self.faulty_nodes.add(pid)
                logging.getLogger(self.__class__.__name__).info("error for %s (%d)" %(pid, tme))

    def clear(self):
        """
        Method to clear the current state of the cache.
        """
        del self._object_cache
        self._object_cache = {}
        del self.faulty_nodes
        self.faulty_nodes = set()
        del self.unfound
        self.unfound = set()

    @property
    def object_cache(self):
        """
        Method that returns internal cache representation.

        :return: object cache
        :rtype: dict
        """
        return self._object_cache
