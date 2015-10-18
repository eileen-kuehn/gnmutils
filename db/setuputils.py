from gnmutils.db.dbobjects import DBWorkernodeObject
from dbutils.sqlcommand import SQLCommand


class SetupUtils(object):
    @staticmethod
    def generate_db_objects_for_rack(rack=None):
        result = []
        result.extend([DBWorkernodeObject(name="c01-%s-%d" % (rack, i)) for i in range(102, 108)])
        result.extend([DBWorkernodeObject(name="c01-%s-%d" % (rack, i)) for i in range(112, 118)])
        result.extend([DBWorkernodeObject(name="c01-%s-%d" % (rack, i)) for i in range(124, 128)])
        result.extend([DBWorkernodeObject(name="c01-%s-%d" % (rack, i)) for i in range(152, 158)])
        result.extend([DBWorkernodeObject(name="c01-%s-%d" % (rack, i)) for i in range(162, 168)])
        result.extend([DBWorkernodeObject(name="c01-%s-%d" % (rack, i)) for i in range(174, 178)])
        return result

    @staticmethod
    def setup_workernodes():
        rack007 = SetupUtils.generate_db_objects_for_rack("007")
        rack008 = SetupUtils.generate_db_objects_for_rack("008")

        with SQLCommand(providerName="PostgresDBProvider",
                        connectionString="dbname=gnmtest user=gnm port=5433 "
                                         "password=gridka") as sqlCommand:
            try:
                # save the first rack
                sqlCommand.startTransaction()
                map(sqlCommand.save, rack007)
                sqlCommand.commitTransaction()

                # save the second rack
                sqlCommand.startTransaction()
                map(sqlCommand.save, rack008)
                sqlCommand.commitTransaction()
            except Exception:
                sqlCommand.rollbackTransaction()

if __name__ == "__main__":
    SetupUtils.setup_workernodes()
