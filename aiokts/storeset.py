class StoreSet(object):
    def __init__(self, config=None):
        self.stores = {}

        if config is None:
            config = {}

        for key, config in config.items():
            self.stores[key] = self.init_store(config)

    def __getattribute__(self, *args, **kwargs):
        store = super().__getattribute__('stores').get(args[0])
        if store is not None:
            return store
        return super().__getattribute__(*args, **kwargs)

    def init_store(self, config):
        type = config['type']
        utype = type[0].upper() + type[1:]
        name = '{0}Store'.format(utype)
        mod = __import__('aiokts.stores', fromlist=[name])

        return getattr(mod, name)(config)
