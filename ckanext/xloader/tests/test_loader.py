import os

import sqlalchemy.orm as orm
from nose.tools import assert_equal, assert_raises, assert_in, nottest
from nose.plugins.skip import SkipTest
import datetime
from decimal import Decimal

from ckan.tests import helpers, factories
from ckanext.xloader import loader
from ckanext.xloader.loader import get_write_engine
from ckanext.xloader.job_exceptions import LoaderError

import ckan.plugins as p
import util


def get_sample_filepath(filename):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), 'samples',
                                        filename))


class PrintLogger(object):
    def __getattr__(self, log_level):
        def print_func(msg):
            time = datetime.datetime.now().strftime('%H:%M:%S')
            print '{} {}: {}'.format(time, log_level.capitalize(), msg)
        return print_func


class TestLoadBase(util.PluginsMixin):
    _load_plugins = ['datastore']

    def setup(self):
        engine = get_write_engine()
        self.Session = orm.scoped_session(orm.sessionmaker(bind=engine))
        helpers.reset_db()
        util.reset_datastore_db()

    def teardown(self):
        self.Session.close()

    def _get_records(self, table_name, limit=None,
                     exclude_full_text_column=True):
        c = self.Session.connection()
        if exclude_full_text_column:
            cols = self._get_column_names(table_name)
            cols = ', '.join('"{}"'.format(col) for col in cols
                             if col != '_full_text')
        else:
            cols = '*'
        sql = 'SELECT {cols} FROM "{table_name}"' \
            .format(cols=cols, table_name=table_name)
        if limit is not None:
            sql += ' LIMIT {}'.format(limit)
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


class TestLoadCsv(TestLoadBase):

    def test_simple(self):
        csv_filepath = get_sample_filepath('simple.csv')
        resource_id = 'test1'
        factories.Resource(id=resource_id)
        loader.load_csv(csv_filepath, resource_id=resource_id,
                        mimetype='text/csv', logger=PrintLogger())

        assert_equal(self._get_records(
            'test1', limit=1, exclude_full_text_column=False),
                     [(1, "'-01':2,3 '1':4 '2011':1 'galway':5", u'2011-01-01', u'1', u'Galway')])
        assert_equal(self._get_records('test1'),
                     [(1, u'2011-01-01', u'1', u'Galway'),
                      (2, u'2011-01-02', u'-1', u'Galway'),
                      (3, u'2011-01-03', u'0', u'Galway'),
                      (4, u'2011-01-01', u'6', u'Berkeley'),
                      (5, None, None, u'Berkeley'),
                      (6, u'2011-01-03', u'5', None)])
        assert_equal(
            self._get_column_names('test1'),
            [u'_id', u'_full_text', u'date', u'temperature', u'place'])
        assert_equal(
            self._get_column_types('test1'),
            [u'int4', u'tsvector', u'text', u'text', u'text'])

    def test_simple_with_indexing(self):
        csv_filepath = get_sample_filepath('simple.csv')
        resource_id = 'test1'
        factories.Resource(id=resource_id)
        fields = loader.load_csv(csv_filepath, resource_id=resource_id,
                                 mimetype='text/csv', logger=PrintLogger())
        loader.create_column_indexes(fields=fields, resource_id=resource_id,
                                     logger=PrintLogger())

        assert_equal(self._get_records(
            'test1', limit=1, exclude_full_text_column=False)[0][1],
                     "'-01':2,3 '1':4 '2011':1 'galway':5")

    # test disabled by default to avoid adding large file to repo and slow test
    @nottest
    def test_boston_311_complete(self):
        # to get the test file:
        # curl -o ckanext/xloader/tests/samples/boston_311.csv https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/2968e2c0-d479-49ba-a884-4ef523ada3c0/download/311.csv
        csv_filepath = get_sample_filepath('boston_311.csv')
        resource_id = 'test1'
        factories.Resource(id=resource_id)
        import time
        t0 = time.time()
        print '{} Start load'.format(time.strftime('%H:%M:%S', time.localtime(t0)))
        loader.load_csv(csv_filepath, resource_id=resource_id,
                        mimetype='text/csv', logger=PrintLogger())
        print 'Load: {}s'.format(time.time() - t0)

    # test disabled by default to avoid adding large file to repo and slow test
    @nottest
    def test_boston_311_sample5(self):
        # to create the test file:
        # head -n 100001 ckanext/xloader/tests/samples/boston_311.csv > ckanext/xloader/tests/samples/boston_311_sample5.csv
        csv_filepath = get_sample_filepath('boston_311_sample5.csv')
        resource_id = 'test1'
        factories.Resource(id=resource_id)
        import time
        t0 = time.time()
        print '{} Start load'.format(time.strftime('%H:%M:%S', time.localtime(t0)))
        loader.load_csv(csv_filepath, resource_id=resource_id,
                        mimetype='text/csv', logger=PrintLogger())
        print 'Load: {}s'.format(time.time() - t0)

    def test_boston_311(self):
        csv_filepath = get_sample_filepath('boston_311_sample.csv')
        resource_id = 'test1'
        factories.Resource(id=resource_id)
        loader.load_csv(csv_filepath, resource_id=resource_id,
                        mimetype='text/csv', logger=PrintLogger())

        records = self._get_records('test1')
        print records
        assert_equal(
            records,
            [(1, u'101002153891', u'2017-07-06 23:38:43', u'2017-07-21 08:30:00', None, u'ONTIME', u'Open', u' ', u'Street Light Outages', u'Public Works Department', u'Street Lights', u'Street Light Outages', u'PWDx_Street Light Outages', u'PWDx', None, None, u'480 Harvard St  Dorchester  MA  02124', u'8', u'07', u'4', u'B3', u'Greater Mattapan', u'9', u'Ward 14', u'1411', u'480 Harvard St', u'02124', u'42.288', u'-71.0927', u'Citizens Connect App'),
             (2, u'101002153890', u'2017-07-06 23:29:13', u'2017-09-11 08:30:00', None, u'ONTIME', u'Open', u' ', u'Graffiti Removal', u'Property Management', u'Graffiti', u'Graffiti Removal', u'PROP_GRAF_GraffitiRemoval', u'PROP', u' https://mayors24.cityofboston.gov/media/boston/report/photos/595f0000048560f46d94b9fa/report.jpg', None, u'522 Saratoga St  East Boston  MA  02128', u'1', u'09', u'1', u'A7', u'East Boston', u'1', u'Ward 1', u'0110', u'522 Saratoga St', u'02128', u'42.3807', u'-71.0259', u'Citizens Connect App'),
             (3, u'101002153889', u'2017-07-06 23:24:20', u'2017-09-11 08:30:00', None, u'ONTIME', u'Open', u' ', u'Graffiti Removal', u'Property Management', u'Graffiti', u'Graffiti Removal', u'PROP_GRAF_GraffitiRemoval', u'PROP', u' https://mayors24.cityofboston.gov/media/boston/report/photos/595efedb048560f46d94b9ef/report.jpg', None, u'965 Bennington St  East Boston  MA  02128', u'1', u'09', u'1', u'A7', u'East Boston', u'1', u'Ward 1', u'0112', u'965 Bennington St', u'02128', u'42.386', u'-71.008', u'Citizens Connect App')]
            )
        print self._get_column_names('test1')
        assert_equal(
            self._get_column_names('test1'),
            [u'_id', u'_full_text', u'CASE_ENQUIRY_ID', u'open_dt', u'target_dt', u'closed_dt', u'OnTime_Status', u'CASE_STATUS', u'CLOSURE_REASON', u'CASE_TITLE', u'SUBJECT', u'REASON', u'TYPE', u'QUEUE', u'Department', u'SubmittedPhoto', u'ClosedPhoto', u'Location', u'Fire_district', u'pwd_district', u'city_council_district', u'police_district', u'neighborhood', u'neighborhood_services_district', u'ward', u'precinct', u'LOCATION_STREET_NAME', u'LOCATION_ZIPCODE', u'Latitude', u'Longitude', u'Source'])
        print self._get_column_types('test1')
        assert_equal(self._get_column_types('test1'),
                     [u'int4', u'tsvector'] +
                     [u'text'] * (len(records[0]) - 1))

    def test_brazilian(self):
        csv_filepath = get_sample_filepath('brazilian_sample.csv')
        resource_id = 'test1'
        factories.Resource(id=resource_id)
        loader.load_csv(csv_filepath, resource_id=resource_id,
                        mimetype='text/csv', logger=PrintLogger())

        records = self._get_records('test1')
        print records
        assert_equal(
            records[0],
            (1, u'01/01/1996 12:00:00 AM', u'1100015', u"ALTA FLORESTA D'OESTE", u'RO', None, u'128', u'0', u'8', u'119', u'1', u'0', u'3613', u'3051', u'130', u'7', u'121', u'3716', u'3078', u'127', u'7', None, None, None, None, u'6794', u'5036', u'1758', None, None, None, None, None, None, u'337', u'0.26112759', u'0.17210683', u'0.43323442', u'0.13353115', u'24.833692447908199', None, None, u'22.704964', u'67.080006197818605', u'65.144188573097907', u'74.672390253375497', u'16.7913561569619', u'19.4894563570641', u'8.649237411458509', u'7.60165422117368', u'11.1540090366186', u'17.263407056738099', u'8.5269823', u'9.2213373', u'5.3085136', u'52.472769803217503', None, None, None, None, None, None, u'25.0011414302354', u'22.830887000000001', u'66.8150490097632', u'64.893674212235595', u'74.288246611754104', u'17.0725384713319', u'19.8404105332814', u'8.856561911292371', u'7.74275834336647', u'11.357671741889', u'17.9410577459881', u'8.3696527', u'8.9979973', u'5.0570836', u'53.286314230720798', None, None, None, None, None, u'122988', None, u'10.155015000000001', u'14.826086999999999', u'11.671533', u'9.072917', None, None, None, None, None, None, None, None))
        print self._get_column_names('test1')
        assert_equal(
            self._get_column_names('test1'),
            [u'_id', u'_full_text', u'NU_ANO_CENSO', u'CO_MUNICIPIO', u'MUNIC', u'SIGLA', u'CO_UF', u'SCHOOLS_NU', u'SCHOOLS_FED_NU', u'SCHOOLS_ESTADUAL_NU', u'SCHOOLS_MUN_NU', u'SCHOOLS_PRIV_NU', u'SCHOOLS_FED_STUD', u'SCHOOLS_ESTADUAL_STUD', u'SCHOOLS_MUN_STUD', u'SCHOOLS_PRIV_STUD', u'SCHOOLS_URBAN_NU', u'SCHOOLS_RURAL_NU', u'SCHOOLS_URBAN_STUD', u'SCHOOLS_RURAL_STUD', u'SCHOOLS_NIVFUND_1_NU', u'SCHOOLS_NIVFUND_2_NU', u'SCHOOLS_EIGHTYEARS_NU', u'SCHOOLS_NINEYEARS_NU', u'SCHOOLS_EIGHTYEARS_STUD', u'SCHOOLS_NINEYEARS_STUD', u'MATFUND_NU', u'MATFUND_I_NU', u'MATFUND_T_NU', u'SCHOOLS_INTERNET_AVG', u'SCHOOLS_WATER_PUBLIC_AVG', u'SCHOOLS_WATER_AVG', u'SCHOOLS_ELECTR_PUB_AVG', u'SCHOOLS_SEWAGE_PUB_AVG', u'SCHOOLS_SEWAGE_AVG', u'PROFFUNDTOT_NU', u'PROFFUNDINC_PC', u'PROFFUNDCOMP_PC', u'PROFMED_PC', u'PROFSUP_PC', u'CLASSSIZE', u'CLASSSIZE_I', u'CLASSSIZE_T', u'STUDTEACH', u'RATE_APROV', u'RATE_APROV_I', u'RATE_APROV_T', u'RATE_FAILURE', u'RATE_FAILURE_I', u'RATE_FAILURE_T', u'RATE_ABANDON', u'RATE_ABANDON_I', u'RATE_ABANDON_T', u'RATE_TRANSFER', u'RATE_TRANSFER_I', u'RATE_TRANSFER_T', u'RATE_OVERAGE', u'RATE_OVERAGE_I', u'RATE_OVERAGE_T', u'PROVA_MEAN_PORT_I', u'PROVA_MEAN_PORT_T', u'PROVA_MEAN_MAT_I', u'PROVA_MEAN_MAT_T', u'CLASSSIZE_PUB', u'STUDTEACH_PUB', u'RATE_APROV_PUB', u'RATE_APROV_I_PUB', u'RATE_APROV_T_PUB', u'RATE_FAILURE_PUB', u'RATE_FAILURE_I_PUB', u'RATE_FAILURE_T_PUB', u'RATE_ABANDON_PUB', u'RATE_ABANDON_I_PUB', u'RATE_ABANDON_T_PUB', u'RATE_TRANSFER_PUB', u'RATE_TRANSFER_I_PUB', u'RATE_TRANSFER_T_PUB', u'RATE_OVERAGE_PUB', u'RATE_OVERAGE_I_PUB', u'RATE_OVERAGE_T_PUB', u'PROVA_MEAN_PORT_I_PUB', u'PROVA_MEAN_PORT_T_PUB', u'PROVA_MEAN_MAT_I_PUB', u'PROFFUNDTOT_NU_PUB', u'PROVA_MEAN_MAT_T_PUB', u'EDUCTEACH_PUB', u'EDUCTEACH_FEDERAL', u'EDUCTEACH_STATE', u'EDUCTEACH_MUN', u'PROVA_MEAN_PORT_I_STATE', u'PROVA_MEAN_PORT_T_STATE', u'PROVA_MEAN_MAT_I_STATE', u'PROVA_MEAN_MAT_T_STATE', u'PROVA_MEAN_PORT_I_MUN', u'PROVA_MEAN_PORT_T_MUN', u'PROVA_MEAN_MAT_I_MUN', u'PROVA_MEAN_MAT_T_MUN'])
        print self._get_column_types('test1')
        assert_equal(self._get_column_types('test1'),
                     [u'int4', u'tsvector'] +
                     [u'text'] * (len(records[0]) - 1))

    def test_reload(self):
        csv_filepath = get_sample_filepath('simple.csv')
        resource_id = 'test1'
        factories.Resource(id=resource_id)
        loader.load_csv(csv_filepath, resource_id=resource_id,
                        mimetype='text/csv', logger=PrintLogger())

        # Load it again unchanged
        loader.load_csv(csv_filepath, resource_id=resource_id,
                        mimetype='text/csv', logger=PrintLogger())

        assert_equal(len(self._get_records('test1')), 6)
        assert_equal(
            self._get_column_names('test1'),
            [u'_id', u'_full_text', u'date', u'temperature', u'place'])
        assert_equal(
            self._get_column_types('test1'),
            [u'int4', u'tsvector', u'text', u'text', u'text'])

    def test_reload_with_overridden_types(self):
        if not p.toolkit.check_ckan_version(min_version='2.7'):
            raise SkipTest('Requires CKAN 2.7 - see https://github.com/ckan/ckan/pull/3557')
        csv_filepath = get_sample_filepath('simple.csv')
        resource_id = 'test1'
        factories.Resource(id=resource_id)
        loader.load_csv(csv_filepath, resource_id=resource_id,
                        mimetype='text/csv', logger=PrintLogger())
        # Change types, as it would be done by Data Dictionary
        rec = p.toolkit.get_action('datastore_search')(None, {
            'resource_id': resource_id,
            'limit': 0})
        fields = [f for f in rec['fields'] if not f['id'].startswith('_')]
        fields[0]['info'] = {'type_override': 'timestamp'}
        fields[1]['info'] = {'type_override': 'numeric'}
        p.toolkit.get_action('datastore_create')({'ignore_auth': True}, {
            'resource_id': resource_id,
            'force': True,
            'fields': fields
            })
        # [{
        #         'id': f['id'],
        #         'type': f['type'],
        #         'info': fi if isinstance(fi, dict) else {}
        #         } for f, fi in izip_longest(fields, info)]

        # Load it again with new types
        fields = loader.load_csv(csv_filepath, resource_id=resource_id,
                                 mimetype='text/csv', logger=PrintLogger())
        loader.create_column_indexes(fields=fields, resource_id=resource_id,
                                     logger=PrintLogger())

        assert_equal(len(self._get_records('test1')), 6)
        assert_equal(
            self._get_column_names('test1'),
            [u'_id', u'_full_text', u'date', u'temperature', u'place'])
        assert_equal(
            self._get_column_types('test1'),
            [u'int4', u'tsvector', u'timestamp', u'numeric', u'text'])

        # check that rows with nulls are indexed correctly
        records = self._get_records('test1', exclude_full_text_column=False)
        print records
        assert_equal(
            records[4][1],
            "'berkeley':1"
            )
        assert_equal(
            records[5][1],
            "'-01':2 '-03':3 '00':4,5,6 '2011':1 '5':7"
            )

class TestLoadUnhandledTypes(TestLoadBase):

    def test_kml(self):
        filepath = get_sample_filepath('polling_locations.kml')
        resource_id = 'test1'
        factories.Resource(id=resource_id)
        with assert_raises(LoaderError) as exception:
            loader.load_csv(filepath, resource_id=resource_id,
                            mimetype='text/csv', logger=PrintLogger())
        assert_in('Error with field definition',
                  str(exception.exception))
        assert_in('"<?xml version="1.0" encoding="utf-8" ?>" is not a valid field name',
                  str(exception.exception))

    def test_geojson(self):
        filepath = get_sample_filepath('polling_locations.geojson')
        resource_id = 'test1'
        factories.Resource(id=resource_id)
        with assert_raises(LoaderError) as exception:
            loader.load_csv(filepath, resource_id=resource_id,
                            mimetype='text/csv', logger=PrintLogger())
        assert_in('Error with field definition',
                  str(exception.exception))
        assert_in('"{"type":"FeatureCollection"" is not a valid field name',
                  str(exception.exception))

    def test_shapefile_zip(self):
        filepath = get_sample_filepath('polling_locations.shapefile.zip')
        resource_id = 'test1'
        factories.Resource(id=resource_id)
        with assert_raises(LoaderError) as exception:
            loader.load_csv(filepath, resource_id=resource_id,
                            mimetype='text/csv', logger=PrintLogger())
        assert_in('Error during the load into PostgreSQL: '
                  'unquoted carriage return found in data',
                  str(exception.exception))


class TestLoadMessytables(TestLoadBase):

    def test_simple(self):
        csv_filepath = get_sample_filepath('simple.xls')
        resource_id = 'test1'
        factories.Resource(id=resource_id)
        loader.load_table(csv_filepath, resource_id=resource_id,
                          mimetype='xls', logger=PrintLogger())

        assert_equal(self._get_records(
            'test1', limit=1, exclude_full_text_column=False),
            [(1,
              "'-01':2,3 '00':4,5,6 '1':7 '2011':1 'galway':8",
              datetime.datetime(2011, 1, 1, 0, 0),
              Decimal('1'),
              u'Galway')])
        assert_equal(
            self._get_records('test1'),
            [(1, datetime.datetime(2011, 1, 1, 0, 0), Decimal('1'), u'Galway'),
             (2, datetime.datetime(2011, 1, 2, 0, 0), Decimal('-1'), u'Galway'),
             (3, datetime.datetime(2011, 1, 3, 0, 0), Decimal('0'), u'Galway'),
             (4, datetime.datetime(2011, 1, 1, 0, 0), Decimal('6'), u'Berkeley'),
             (5, datetime.datetime(2011, 1, 2, 0, 0), Decimal('8'), u'Berkeley'),
             (6, datetime.datetime(2011, 1, 3, 0, 0), Decimal('5'), u'Berkeley')])
        assert_equal(
            self._get_column_names('test1'),
            [u'_id', u'_full_text', u'date', u'temperature', u'place'])
        assert_equal(
            self._get_column_types('test1'),
            [u'int4', u'tsvector', u'timestamp', u'numeric', u'text'])

    # test disabled by default to avoid adding large file to repo and slow test
    @nottest
    def test_boston_311_complete(self):
        # to get the test file:
        # curl -o ckanext/xloader/tests/samples/boston_311.csv https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/2968e2c0-d479-49ba-a884-4ef523ada3c0/download/311.csv
        csv_filepath = get_sample_filepath('boston_311.csv')
        resource_id = 'test1'
        factories.Resource(id=resource_id)
        import time
        t0 = time.time()
        print '{} Start load'.format(time.strftime('%H:%M:%S', time.localtime(t0)))
        loader.load_table(csv_filepath, resource_id=resource_id,
                          mimetype='csv', logger=PrintLogger())
        print 'Load: {}s'.format(time.time() - t0)

    # test disabled by default to avoid adding large file to repo and slow test
    @nottest
    def test_boston_311_sample5(self):
        # to create the test file:
        # head -n 100001 ckanext/xloader/tests/samples/boston_311.csv > ckanext/xloader/tests/samples/boston_311_sample5.csv
        csv_filepath = get_sample_filepath('boston_311_sample5.csv')
        resource_id = 'test1'
        factories.Resource(id=resource_id)
        import time
        t0 = time.time()
        print '{} Start load'.format(time.strftime('%H:%M:%S', time.localtime(t0)))
        loader.load_table(csv_filepath, resource_id=resource_id,
                          mimetype='csv', logger=PrintLogger())
        print 'Load: {}s'.format(time.time() - t0)

    def test_boston_311(self):
        csv_filepath = get_sample_filepath('boston_311_sample.csv')
        resource_id = 'test1'
        factories.Resource(id=resource_id)
        loader.load_table(csv_filepath, resource_id=resource_id,
                          mimetype='csv', logger=PrintLogger())

        records = self._get_records('test1')
        print records
        assert_equal(
            records,
            [(1, Decimal('101002153891'), datetime.datetime(2017, 7, 6, 23, 38, 43), datetime.datetime(2017, 7, 21, 8, 30), u'', u'ONTIME', u'Open', u' ', u'Street Light Outages', u'Public Works Department', u'Street Lights', u'Street Light Outages', u'PWDx_Street Light Outages', u'PWDx', u'', u'', u'480 Harvard St  Dorchester  MA  02124', Decimal('8'), Decimal('7'), Decimal('4'), u'B3', u'Greater Mattapan', Decimal('9'), u'Ward 14', Decimal('1411'), u'480 Harvard St', Decimal('2124'), Decimal('42.288'), Decimal('-71.0927'), u'Citizens Connect App'),
            (2, Decimal('101002153890'), datetime.datetime(2017, 7, 6, 23, 29, 13), datetime.datetime(2017, 9, 11, 8, 30), u'', u'ONTIME', u'Open', u' ', u'Graffiti Removal', u'Property Management', u'Graffiti', u'Graffiti Removal', u'PROP_GRAF_GraffitiRemoval', u'PROP', u' https://mayors24.cityofboston.gov/media/boston/report/photos/595f0000048560f46d94b9fa/report.jpg', u'', u'522 Saratoga St  East Boston  MA  02128', Decimal('1'), Decimal('9'), Decimal('1'), u'A7', u'East Boston', Decimal('1'), u'Ward 1', Decimal('110'), u'522 Saratoga St', Decimal('2128'), Decimal('42.3807'), Decimal('-71.0259'), u'Citizens Connect App'),
            (3, Decimal('101002153889'), datetime.datetime(2017, 7, 6, 23, 24, 20), datetime.datetime(2017, 9, 11, 8, 30), u'', u'ONTIME', u'Open', u' ', u'Graffiti Removal', u'Property Management', u'Graffiti', u'Graffiti Removal', u'PROP_GRAF_GraffitiRemoval', u'PROP', u' https://mayors24.cityofboston.gov/media/boston/report/photos/595efedb048560f46d94b9ef/report.jpg', u'', u'965 Bennington St  East Boston  MA  02128', Decimal('1'), Decimal('9'), Decimal('1'), u'A7', u'East Boston', Decimal('1'), u'Ward 1', Decimal('112'), u'965 Bennington St', Decimal('2128'), Decimal('42.386'), Decimal('-71.008'), u'Citizens Connect App')]
            )
        print self._get_column_names('test1')
        assert_equal(
            self._get_column_names('test1'),
            [u'_id', u'_full_text', u'CASE_ENQUIRY_ID', u'open_dt', u'target_dt', u'closed_dt', u'OnTime_Status', u'CASE_STATUS', u'CLOSURE_REASON', u'CASE_TITLE', u'SUBJECT', u'REASON', u'TYPE', u'QUEUE', u'Department', u'SubmittedPhoto', u'ClosedPhoto', u'Location', u'Fire_district', u'pwd_district', u'city_council_district', u'police_district', u'neighborhood', u'neighborhood_services_district', u'ward', u'precinct', u'LOCATION_STREET_NAME', u'LOCATION_ZIPCODE', u'Latitude', u'Longitude', u'Source'])
        print self._get_column_types('test1')
        assert_equal(self._get_column_types('test1'),
                     [u'int4', u'tsvector',
                      u'numeric', u'timestamp', u'timestamp', u'text', u'text', u'text', u'text', u'text', u'text', u'text', u'text', u'text', u'text', u'text', u'text', u'text', u'numeric', u'numeric', u'numeric', u'text', u'text', u'numeric', u'text', u'numeric', u'text', u'numeric', u'numeric', u'numeric', u'text'])
