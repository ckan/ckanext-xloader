import sqlalchemy.orm as orm

from ckan.tests import helpers
from ckanext.datastore.tests import helpers as datastore_helpers
import ckanext.datastore.backend.postgres as db


class PluginsMixin(object):
    @classmethod
    def setup_class(cls):
        import ckan.plugins as p
        for plugin in getattr(cls, '_load_plugins', []):
            p.load(plugin)
        helpers.reset_db()

        engine = db.get_write_engine()
        Session = orm.scoped_session(orm.sessionmaker(bind=engine))
        datastore_helpers.clear_db(Session)

    @classmethod
    def teardown_class(cls):
        import ckan.plugins as p
        for plugin in reversed(getattr(cls, '_load_plugins', [])):
            p.unload(plugin)
        helpers.reset_db()
