class DataSource(object):
    def is_available(self):
        raise NotImplementedError

    @staticmethod
    def best_available_data_source():
        """
        Method returns best availabe datasource. Denpending on availability of database, it either returns
        :py:class:`DBBackedFileDataSource` or :py:class:`FileDataSource`.

        :return: best available datasource
        :rtype: :py:class:`DBBackedFileDataSource` or :py:class:`FileDataSource`
        """
        from dbbackedfiledatasource import DBBackedFileDataSource
        from filedatasource import FileDataSource
        data_source = DBBackedFileDataSource()
        if data_source.is_available():
            return data_source
        return FileDataSource()

    def object_data(self, **kwargs):
        raise NotImplementedError

    # TODO: not a very good solution...
    def job_description(self, **kwargs):
        raise NotImplementedError

    def traffics(self, **kwargs):
        raise NotImplementedError

    def jobs(self, **kwargs):
        raise NotImplementedError

    def payloads(self, **kwargs):
        pass

    def network_statistics(self, **kwargs):
        raise NotImplementedError

    # TODO: make more general function read!
    def read_job(self, **kwargs):
        raise NotImplementedError

    # TODO: make more general function write!
    def write_job(self, **kwargs):
        raise NotImplementedError

    def write_payload(self, **kwargs):
        raise NotImplementedError

    def write_payload_result(self, **kwargs):
        raise NotImplementedError

    def write_network_statistics(self, **kwargs):
        raise NotImplementedError

    def archive(self, **kwargs):
        raise NotImplementedError
