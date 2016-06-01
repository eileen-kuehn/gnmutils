from gnmutils.objects.gnm_object import GNMObject


class FeatureFactory:
    factories = {}

    @staticmethod
    def addFactory(id, featureFactory):
        FeatureFactory.factories[id] = featureFactory

    @staticmethod
    def createFeature(id, **kwargs):
        if (not FeatureFactory.factories.has_key(id)):
            FeatureFactory.factories[id] = eval(id + '.Factory()')
        return FeatureFactory.factories[id].craete(**kwargs)


class Feature(GNMObject):
    def __init__(self, pid, uid, tme):
        GNMObject.__init__(pid=pid, uid=uid, tme=tme)

    def getRow(self):
        raise NotImplementedError

    def getHeader(self):
        raise NotImplementedError

    @staticmethod
    def default_header(length, **kwargs):
        raise NotImplementedError
