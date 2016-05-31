from gnmutils.parser.dataparser import DataParser
from gnmutils.objects.traffic import Traffic


class TrafficParser(DataParser):
    def __init__(self, data_source=None, data_reader=None, **kwargs):
        DataParser.__init__(self, data_source, data_reader, **kwargs)
        self._data = []

    def clear_caches(self):
        self._data = []

    def _add_piece(self, piece=None):
        self._data.append(piece)

    def _piece_from_dict(self, data_dict=None):
        return Traffic(**data_dict)

    def _parsing_finished(self):
        yield self._data
