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

class Feature(object): pass

