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
        pass

    def setup(self):
        engine = db.get_write_engine()
        self.Session = orm.scoped_session(orm.sessionmaker(bind=engine))

    def teardown(self):
        self.Session.close()

    def test_simple(self):
        csv_filepath = get_sample_filepath('simple.csv')
        def get_config_value(key):
            return config[key]
        loader.load_csv(csv_filepath, get_config_value, table_name='test1',
                        mimetype='text/csv', logger=loader.PrintLogger())

        assert_equal(self._get_records('test1'),
                     [(1, None, u'2011-01-01', u'1', u'Galway'),
                      (2, None, u'2011-01-02', u'-1', u'Galway'),
                      (3, None, u'2011-01-03', u'0', u'Galway'),
                      (4, None, u'2011-01-01', u'6', u'Berkeley'),
                      (5, None, u'2011-01-02', u'8', u'Berkeley'),
                      (6, None, u'2011-01-03', u'5', u'Berkeley')])
        assert_equal(
            self._get_column_names('test1'),
            [u'_id', u'_full_text', u'date', u'temperature', u'place'])
        assert_equal(
            self._get_column_types('test1'),
            [u'int4', u'tsvector', u'varchar', u'varchar', u'varchar'])

    def test_boston_311(self):
        csv_filepath = get_sample_filepath('boston_311_sample.csv')
        def get_config_value(key):
            return config[key]
        loader.load_csv(csv_filepath, get_config_value, table_name='test1',
                        mimetype='text/csv', logger=loader.PrintLogger())

        records = self._get_records('test1')
        print records
        assert_equal(
            records,
            [(1, None, u'101002153891', u'2017-07-06 23:38:43', u'2017-07-21 08:30:00', None, u'ONTIME', u'Open', u' ', u'Street Light Outages', u'Public Works Department', u'Street Lights', u'Street Light Outages', u'PWDx_Street Light Outages', u'PWDx', None, None, u'480 Harvard St  Dorchester  MA  02124', u'8', u'07', u'4', u'B3', u'Greater Mattapan', u'9', u'Ward 14', u'1411', u'480 Harvard St', u'02124', u'42.288', u'-71.0927', u'Citizens Connect App'),
             (2, None, u'101002153890', u'2017-07-06 23:29:13', u'2017-09-11 08:30:00', None, u'ONTIME', u'Open', u' ', u'Graffiti Removal', u'Property Management', u'Graffiti', u'Graffiti Removal', u'PROP_GRAF_GraffitiRemoval', u'PROP', u' https://mayors24.cityofboston.gov/media/boston/report/photos/595f0000048560f46d94b9fa/report.jpg', None, u'522 Saratoga St  East Boston  MA  02128', u'1', u'09', u'1', u'A7', u'East Boston', u'1', u'Ward 1', u'0110', u'522 Saratoga St', u'02128', u'42.3807', u'-71.0259', u'Citizens Connect App'),
             (3, None, u'101002153889', u'2017-07-06 23:24:20', u'2017-09-11 08:30:00', None, u'ONTIME', u'Open', u' ', u'Graffiti Removal', u'Property Management', u'Graffiti', u'Graffiti Removal', u'PROP_GRAF_GraffitiRemoval', u'PROP', u' https://mayors24.cityofboston.gov/media/boston/report/photos/595efedb048560f46d94b9ef/report.jpg', None, u'965 Bennington St  East Boston  MA  02128', u'1', u'09', u'1', u'A7', u'East Boston', u'1', u'Ward 1', u'0112', u'965 Bennington St', u'02128', u'42.386', u'-71.008', u'Citizens Connect App')]
            )
        print self._get_column_names('test1')
        assert_equal(
            self._get_column_names('test1'),
            [u'_id', u'_full_text', u'CASE_ENQUIRY_ID', u'open_dt', u'target_dt', u'closed_dt', u'OnTime_Status', u'CASE_STATUS', u'CLOSURE_REASON', u'CASE_TITLE', u'SUBJECT', u'REASON', u'TYPE', u'QUEUE', u'Department', u'SubmittedPhoto', u'ClosedPhoto', u'Location', u'Fire_district', u'pwd_district', u'city_council_district', u'police_district', u'neighborhood', u'neighborhood_services_district', u'ward', u'precinct', u'LOCATION_STREET_NAME', u'LOCATION_ZIPCODE', u'Latitude', u'Longitude', u'Source'])
        print self._get_column_types('test1')
        assert_equal(self._get_column_types('test1'),
                     [u'int4', u'tsvector'] +
                     [u'varchar'] * (len(records[0]) - 2))

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

# TODO:
# * Postgres has a limit of 63 characters for a column name
# * Duplicate column names
# * type