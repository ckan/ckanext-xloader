from ckan.tests import helpers

class PluginsMixin(object):
    @classmethod
    def setup_class(cls):
        import ckan.plugins as p
        for plugin in getattr(cls, '_load_plugins', []):
            p.load(plugin)

    @classmethod
    def teardown_class(cls):
        import ckan.plugins as p
        for plugin in reversed(getattr(cls, '_load_plugins', [])):
            p.unload(plugin)
        helpers.reset_db()
