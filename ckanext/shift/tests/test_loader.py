import os

import sqlalchemy.orm as orm
from nose.tools import assert_equal

from ckan.common import config
import ckanext.datastore.backend.postgres as db
from ckanext.shift import loader

def get_sample_filepath(filename):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), 'samples',
                                        filename))


class TestLoadCsv():
    @classmethod
    def setup_class(cls):
        engine = db.get_write_engine()
        cls.Session = orm.scoped_session(orm.sessionmaker(bind=engine))

    def test_simple(self):
        csv_filepath = get_sample_filepath('simple.csv')
        def get_config_value(key):
            return config[key]
        loader.load_csv(csv_filepath, get_config_value, table_name='test1',
                        mimetype='text/csv')

        assert_equal(self._get_records('test1'),
                     [(u'2011-01-01', u'1', u'Galway'),
                      (u'2011-01-02', u'-1', u'Galway'),
                      (u'2011-01-03', u'0', u'Galway'),
                      (u'2011-01-01', u'6', u'Berkeley'),
                      (u'2011-01-02', u'8', u'Berkeley'),
                      (u'2011-01-03', u'5', u'Berkeley')])
        assert_equal(self._get_column_names('test1'),
                     [u'date', u'temperature', u'place'])
        assert_equal(self._get_column_types('test1'),
                     [u'varchar', u'varchar', u'varchar'])

    def _get_records(self, table_name):
        c = self.Session.connection()
        sql = 'SELECT * FROM "{}"'.format(table_name)
        results = c.execute(sql)
        return results.fetchall()

    def _get_column_names(self, table_name):
        # SELECT column_name FROM information_schema.columns WHERE table_name='test1';
        c = self.Session.connection()
        sql = "SELECT column_name FROM information_schema.columns " \
              "WHERE table_name='{}';".format(table_name)
        results = c.execute(sql)
        records = results.fetchall()
        return [r[0] for r in records]

    def _get_column_types(self, table_name):
        c = self.Session.connection()
        sql = "SELECT udt_name FROM information_schema.columns " \
              "WHERE table_name='{}';".format(table_name)
        results = c.execute(sql)
        records = results.fetchall()
        return [r[0] for r in records]
