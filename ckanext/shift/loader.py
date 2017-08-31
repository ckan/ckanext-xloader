'Load a CSV into postgres'
import argparse
import os
import os.path
import tempfile
import psycopg2

try:
    import ckanext.datastore.backend.postgres as datastore_db
    get_write_engine = datastore_db.get_write_engine
except ImportError:
    # older versions of ckan
    def get_write_engine():
        from ckanext.datastore.db import _get_engine
        from pylons import config
        data_dict = {'connection_url': config['ckan.datastore.write_url']}
        return _get_engine(data_dict)

from sqlalchemy import String, Integer, Table, Column
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy import create_engine, MetaData
import messytables

import ckan.plugins as p
from job_exceptions import JobError


def load_csv(csv_filepath, resource_id, get_config_value=None,
             mimetype='text/csv', logger=None):
    # hash
    # file_hash = hashlib.md5(f.read()).hexdigest()
    # f.seek(0)
    # if (resource.get('hash') == file_hash
    #         and not data.get('ignore_hash')):
    #     logger.info('Ignoring resource - the file hash hasn\'t changed: '
    #                 '{hash}.'.format(hash=file_hash))
    #     return
    # resource['hash'] = file_hash

    # http_content_type = \
    #     response.info().getheader('content-type').split(';', 1)[0]
    extension = os.path.splitext(csv_filepath)[1]
    with open(csv_filepath, 'rb') as f:
        try:
            table_set = messytables.any_tableset(f, mimetype=mimetype,
                                                 extension=extension)
        except messytables.ReadError as e:
            # # try again with format
            # f.seek(0)
            # try:
            #     format = resource.get('format')
            #     table_set = messytables.any_tableset(f, mimetype=format,
            #                                          extension=format)
            # except Exception:
                raise 'Messytables error: {}'.format(e)

        row_set = table_set.tables.pop()
        header_offset, headers = messytables.headers_guess(row_set.sample)

    # Some headers might have been converted from strings to floats and such.
    headers = [unicode(header) for header in headers]

    # Setup the converters that run when you iterate over the row_set.
    # With pgloader only the headers will be iterated over.
    row_set.register_processor(messytables.headers_processor(headers))
    row_set.register_processor(
        messytables.offset_processor(header_offset + 1))
    # types = messytables.type_guess(row_set.sample, types=TYPES, strict=True)

    headers = [header.strip() for header in headers if header.strip()]
    # headers_dicts = [dict(id=field[0], type=TYPE_MAPPING[str(field[1])])
    #                  for field in zip(headers, types)]

    # TODO worry about csv header name problems
    # e.g. duplicate names

    # encoding (and line ending?)- use chardet
    # It is easier to reencode it as UTF8 than convert the name of the encoding
    # to one that pgloader will understand.
    logger.info('Ensuring character coding is UTF8')
    f_write = tempfile.NamedTemporaryFile(suffix=extension, delete=False)
    try:
        with open(csv_filepath, 'rb') as f_read:
            csv_decoder = messytables.commas.UTF8Recoder(f_read, encoding=None)
            for line in csv_decoder:
                f_write.write(line)
            f_write.close()   # ensures the last line is written
            csv_filepath = f_write.name

        # check tables exists

        # datastore db connection
        if not get_config_value:
            engine = datastore_db.get_write_engine()
        else:
            # i.e. when running from this file's cli
            datastore_sqlalchemy_url = \
                get_config_value('ckan.datastore.write_url')
            engine = create_engine(datastore_sqlalchemy_url)

        # get column info from existing table
        existing = datastore_resource_exists(resource_id)
        existing_info = {}
        if existing:
            existing_info = dict((f['id'], f['info'])
                                 for f in existing.get('fields', [])
                                 if 'info' in f)

            '''
            Delete existing datastore table before proceeding. Otherwise
            the COPY will append to the existing table. And if
            the fields have significantly changed, it may also fail.
            '''
            logger.info('Deleting "{res_id}" from DataStore.'.format(
                res_id=resource_id))
            delete_datastore_resource(resource_id)

        # Columns types are either set (overridden) in the Data Dictionary page
        # or default to text type (which is robust)
        fields = [
            {'id': header_name,
             'type': existing_info.get(header_name, {})\
             .get('type_override') or 'text',
             }
            for header_name in headers]

        # Maintain data dictionaries from matching column names
        if existing_info:
            for f in fields:
                if f['id'] in existing_info:
                    f['info'] = existing_info[f['id']]

        logger.info('Fields: {}'.format(fields))

        # Create table
        from ckan import model
        context = {'model': model, 'ignore_auth': True}
        p.toolkit.get_action('datastore_create')(context, dict(
            resource_id=resource_id,
            fields=fields,
            records=None,  # just create an empty table
            force=True,  # TODO check this - I don't fully understand
                         # read-only/datastore resources
            ))
        connection = engine.connect()
        if not fulltext_trigger_exists(connection, resource_id):
            _create_fulltext_trigger(connection, resource_id)

        logger.info('Copying to database...')

        # Options for loading into postgres:
        # 1. \copy - can't use as that is a psql meta-command and not accessible
        #    via psycopg2
        # 2. COPY - requires the db user to have superuser privileges. This is
        #    dangerous. It is also not available on AWS, for example.
        # 3. pgloader method? - as described in its docs:
        #    Note that while the COPY command is restricted to read either from its standard input or from a local file on the server's file system, the command line tool psql implements a \copy command that knows how to stream a file local to the client over the network and into the PostgreSQL server, using the same protocol as pgloader uses.
        # 4. COPY FROM STDIN - not quite as fast as COPY from a file, but avoids
        #    the superuser issue. <-- picked

        # with psycopg2.connect(DSN) as conn:
        #     with conn.cursor() as curs:
        #         curs.execute(SQL)
        connection = engine.raw_connection()
        try:
            cur = connection.cursor()
            try:
                with open(csv_filepath, 'rb') as f:
                    # can't use :param for table name because params are only
                    # for filter values that are single quoted.
                    try:
                        cur.copy_expert(
                            "COPY \"{resource_id}\" ({column_names}) "
                            "FROM STDIN "
                            "WITH (DELIMITER ',', FORMAT csv, HEADER 1, "
                            "      ENCODING '{encoding}');"
                        .format(
                                resource_id=resource_id,
                                column_names=', '.join(['"{}"'.format(h)
                                                        for h in headers]),
                                encoding='UTF8',
                                ),
                            f)
                    except psycopg2.DataError as e:
                        logger.error(e)
                        raise JobError('Error during the load into PostgreSQL:'
                                       ' {}'.format(e))

            finally:
                cur.close()
        finally:
            connection.commit()
    finally:
        os.remove(csv_filepath)  # i.e. the tempfile

    logger.info('...copying done')


def datastore_resource_exists(resource_id):
    from ckan import model
    context = {'model': model, 'ignore_auth': True}
    try:
        response = p.toolkit.get_action('datastore_search')(context, dict(
            id=resource_id, limit=0))
    except p.toolkit.ObjectNotFound:
        return False
    return response or {'fields': []}


def delete_datastore_resource(resource_id):
    from ckan import model
    context = {'model': model, 'ignore_auth': True}
    try:
        response = p.toolkit.get_action('datastore_delete')(context, dict(
            id=resource_id, force=True))
    except p.toolkit.ObjectNotFound:
        # this is ok
        return
    return


def get_config_value_without_loading_ckan_environment(config_filepath, key):
    '''May raise exception ValueError'''
    import ConfigParser
    config = ConfigParser.ConfigParser()
    try:
        config.read(os.path.expanduser(config_filepath))
        return config.get('app:main', key)
    except ConfigParser.Error, e:
        err = 'Error reading CKAN config file %s to get key %s: %s' % (
            config_filepath, key, e)
        raise ValueError(err)

def fulltext_function_exists(connection):
    '''Check to see if the fulltext function is set-up in postgres.
    This is done during install of CKAN if it is new enough to have:
    https://github.com/ckan/ckan/pull/3786
    or otherwise it is checked on startup of this plugin.
    '''
    res = connection.execute('''
        select * from pg_proc where proname = 'populate_full_text_trigger';
        ''')
    return bool(res.rowcount)

def fulltext_trigger_exists(connection, resource_id):
    '''Check to see if the fulltext trigger is set-up on this resource's table.
    This will only be the case if your CKAN is new enough to have:
    https://github.com/ckan/ckan/pull/3786
    '''
    res = connection.execute('''
        SELECT pg_trigger.tgname FROM pg_class
        JOIN pg_trigger ON pg_class.oid=pg_trigger.tgrelid
        WHERE pg_class.relname={table}
        AND pg_trigger.tgname='zfulltext';
        '''.format(
        table=literal_string(resource_id)))
    return bool(res.rowcount)

################################
#    datastore copied code     #
# (for use with older ckans that lack this)

def _create_fulltext_trigger(connection, resource_id):
    connection.execute(
        u'''CREATE TRIGGER zfulltext
        BEFORE INSERT OR UPDATE ON {table}
        FOR EACH ROW EXECUTE PROCEDURE populate_full_text_trigger()'''.format(
            table=identifier(resource_id)))

def identifier(s):
    return u'"' + s.replace(u'"', u'""').replace(u'\0', '') + u'"'

def literal_string(s):
    return u"'" + s.replace(u"'", u"''").replace(u'\0', '') + u"'"

# end of datastore copied code #
################################

class PrintLogger(object):
    def __getattr__(self, log_level):
        def print_func(msg):
            print '{}: {}'.format(log_level.capitalize(), msg)
        return print_func


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('ckan_ini', metavar='CKAN_INI',
                        help='CSV configuration (.ini) filepath')
    parser.add_argument('csv_filepath', metavar='csv-filepath',
                        help='CSV filepath')
    args = parser.parse_args()
    def get_config_value(key):
        return get_config_value_without_loading_ckan_environment(
            args.ckan_ini, key)
    load_csv(args.csv_filepath, get_config_value=get_config_value,
             mimetype='text/csv', logger=PrintLogger())
