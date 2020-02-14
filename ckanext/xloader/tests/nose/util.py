import sqlalchemy
import sqlalchemy.orm as orm
import os

from ckan.tests import helpers
from ckanext.datastore.tests import helpers as datastore_helpers
from ckanext.xloader.loader import get_write_engine

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__))
)


class PluginsMixin(object):
    @classmethod
    def setup_class(cls):
        import ckan.plugins as p
        for plugin in getattr(cls, '_load_plugins', []):
            if not p.plugin_loaded(plugin):
                p.load(plugin)
        helpers.reset_db()
        reset_datastore_db()
        add_full_text_trigger_function()

    @classmethod
    def teardown_class(cls):
        import ckan.plugins as p
        for plugin in reversed(getattr(cls, '_load_plugins', [])):
            p.unload(plugin)
        helpers.reset_db()


def reset_datastore_db():
    engine = get_write_engine()
    Session = orm.scoped_session(orm.sessionmaker(bind=engine))
    datastore_helpers.clear_db(Session)


def add_full_text_trigger_function():
    engine = get_write_engine()
    Session = orm.scoped_session(orm.sessionmaker(bind=engine))
    c = Session.connection()
    with open(os.path.join(__location__, '..', '..', '..', 'full_text_function.sql'), 'r') as full_text_sql:
        c.execute(sqlalchemy.text(full_text_sql.read()))
    Session.commit()
    Session.remove()
