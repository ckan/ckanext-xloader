'Load a CSV into postgres'
import os
import os.path
import tempfile
import itertools

import psycopg2
from sqlalchemy import Text, Integer, Table, Column
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy import create_engine, MetaData
import messytables

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
    import ckanext.datastore.db as datastore_db
create_indexes = datastore_db.create_indexes
_drop_indexes = datastore_db._drop_indexes

try:
    from ckan.plugins.toolkit import config
except ImportError:
    # older versions of ckan
    from pylons import config

import ckan.plugins as p
from job_exceptions import LoaderError


def load_csv(csv_filepath, resource_id, mimetype='text/csv', logger=None):
    '''Loads a CSV into DataStore. Does not create the indexes.'''

    # use messytables to determine the header row
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
                raise LoaderError('Messytables error: {}'.format(e))

        if not table_set.tables:
            raise LoaderError('Could not detect tabular data in this file')
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
        engine = get_write_engine()

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
        data_dict = dict(
            resource_id=resource_id,
            fields=fields,
            )
        data_dict['records'] = None  # just create an empty table
        data_dict['force'] = True  # TODO check this - I don't fully
            # understand read-only/datastore resources
        try:
            p.toolkit.get_action('datastore_create')(context, data_dict)
        except p.toolkit.ValidationError as e:
            if 'fields' in e.error_dict:
                # e.g. {'message': None, 'error_dict': {'fields': [u'"***" is not a valid field name']}, '_error_summary': None}
                error_message = e.error_dict['fields'][0]
                raise LoaderError('Error with field definition: {}'
                                  .format(error_message))
            else:
                raise LoaderError(
                    'Validation error when creating the database table: {}'
                    .format(str(e)))
        except Exception as e:
            raise LoaderError('Could not create the database table: {}'
                              .format(e))
        connection = context['connection'] = engine.connect()
        if not fulltext_trigger_exists(connection, resource_id):
            logger.info('Trigger created')
            _create_fulltext_trigger(connection, resource_id)

        # logger.info('Disabling row index trigger')
        _disable_fulltext_trigger(connection, resource_id)
        # logger.info('Dropping indexes')
        _drop_indexes(context, data_dict, False)

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
        raw_connection = engine.raw_connection()
        try:
            cur = raw_connection.cursor()
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
                        raise LoaderError('Error during the load into PostgreSQL:'
                                          ' {}'.format(e))

            finally:
                cur.close()
        finally:
            raw_connection.commit()
    finally:
        os.remove(csv_filepath)  # i.e. the tempfile

    logger.info('...copying done')

    logger.info('Creating search index...')
    _populate_fulltext(connection, resource_id, fields=fields)
    logger.info('...search index created')

    return fields


def create_column_indexes(fields, resource_id, logger):
    logger.info('Creating column indexes (a speed optimization for queries)...')
    from ckan import model
    context = {'model': model, 'ignore_auth': True}
    data_dict = dict(
        resource_id=resource_id,
        fields=fields,
        )
    engine = get_write_engine()
    connection = context['connection'] = engine.connect()

    create_indexes(context, data_dict)
    _enable_fulltext_trigger(connection, resource_id)

    logger.info('...column indexes created.')


def load_table(table_filepath, resource_id, mimetype='text/csv', logger=None):
    '''Loads an Excel file (or other tabular data recognized by messytables)
    into Datastore and creates indexes.

    Largely copied from datapusher - see below. Is slower than load_csv.
    '''

    # use messytables to determine the header row
    logger.info('Determining column names and types')
    ct = mimetype
    format = os.path.splitext(table_filepath)[1]  # filename extension
    with open(table_filepath, 'rb') as tmp:

        #
        # Copied from datapusher/jobs.py:push_to_datastore
        #

        try:
            table_set = messytables.any_tableset(tmp, mimetype=ct, extension=ct)
        except messytables.ReadError as e:
            ## try again with format
            tmp.seek(0)
            try:
                ### Assume format is csv
                ### format = resource.get('format')
                table_set = messytables.any_tableset(tmp, mimetype=format, extension=format)
            except:
                raise LoaderError(e)

        row_set = table_set.tables.pop()
        offset, headers = messytables.headers_guess(row_set.sample)

        existing = datastore_resource_exists(resource_id)
        existing_info = None
        if existing:
            existing_info = dict((f['id'], f['info'])
                for f in existing.get('fields', []) if 'info' in f)

        # Some headers might have been converted from strings to floats and such.
        headers = [unicode(header) for header in headers]

        row_set.register_processor(messytables.headers_processor(headers))
        row_set.register_processor(messytables.offset_processor(offset + 1))
        TYPES, TYPE_MAPPING = get_types()
        types = messytables.type_guess(row_set.sample, types=TYPES, strict=True)

        # override with types user requested
        if existing_info:
            types = [{
                'text': messytables.StringType(),
                'numeric': messytables.DecimalType(),
                'timestamp': messytables.DateUtilType(),
                }.get(existing_info.get(h, {}).get('type_override'), t)
                for t, h in zip(types, headers)]

        row_set.register_processor(messytables.types_processor(types))

        headers = [header.strip() for header in headers if header.strip()]
        headers_set = set(headers)

        def row_iterator():
            for row in row_set:
                data_row = {}
                for index, cell in enumerate(row):
                    column_name = cell.column.strip()
                    if column_name not in headers_set:
                        continue
                    data_row[column_name] = cell.value
                yield data_row
        result = row_iterator()

        '''
        Delete existing datstore resource before proceeding. Otherwise
        'datastore_create' will append to the existing datastore. And if
        the fields have significantly changed, it may also fail.
        '''
        if existing:
            logger.info('Deleting "{res_id}" from datastore.'.format(
                res_id=resource_id))
            delete_datastore_resource(resource_id)

        headers_dicts = [dict(id=field[0], type=TYPE_MAPPING[str(field[1])])
                         for field in zip(headers, types)]

        # Maintain data dictionaries from matching column names
        if existing_info:
            for h in headers_dicts:
                if h['id'] in existing_info:
                    h['info'] = existing_info[h['id']]
                    # create columns with types user requested
                    type_override = existing_info[h['id']].get('type_override')
                    if type_override in _TYPE_MAPPING.values():
                        h['type'] = type_override

        logger.info('Determined headers and types: {headers}'.format(
            headers=headers_dicts))

        ### Commented - this is only for tests
        # if dry_run:
        #     return headers_dicts, result

        logger.info('Copying to database...')
        count = 0
        for i, records in enumerate(chunky(result, 250)):
            count += len(records)
            logger.info('Saving chunk {number}'.format(number=i))
            send_resource_to_datastore(resource_id, headers_dicts, records)
        logger.info('...copying done')

        logger.info('Successfully pushed {n} entries to "{res_id}".'.format(
            n=count, res_id=resource_id))

        ### Commented - this is done by the caller in jobs.py
        # if data.get('set_url_type', False):
        #     update_resource(resource, api_key, ckan_url)


_TYPE_MAPPING = {
    'String': 'text',
    # 'int' may not be big enough,
    # and type detection may not realize it needs to be big
    'Integer': 'numeric',
    'Decimal': 'numeric',
    'DateUtil': 'timestamp'
}


def get_types():
    _TYPES = [messytables.StringType, messytables.DecimalType,
              messytables.IntegerType, messytables.DateUtilType]
    # TODO make this configurable
    #TYPES = web.app.config.get('TYPES', _TYPES)
    TYPE_MAPPING = config.get('TYPE_MAPPING', _TYPE_MAPPING)
    return _TYPES, TYPE_MAPPING


def chunky(iterable, n):
    """
    Generates chunks of data that can be loaded into ckan

    :param n: Size of each chunks
    :type n: int
    """
    it = iter(iterable)
    item = list(itertools.islice(it, n))
    while item:
        yield item
        item = list(itertools.islice(it, n))


def send_resource_to_datastore(resource_id, headers, records):
    """
    Stores records in CKAN datastore
    """
    request = {'resource_id': resource_id,
               'fields': headers,
               'force': True,
               'records': records}

    from ckan import model
    context = {'model': model, 'ignore_auth': True}
    try:
        p.toolkit.get_action('datastore_create')(context, request)
    except p.toolkit.ValidationError as e:
        raise LoaderError('Validation error writing rows to db: {}'
                          .format(str(e)))


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
    context = {'model': model, 'user': '', 'ignore_auth': True}
    try:
        response = p.toolkit.get_action('datastore_delete')(context, dict(
            id=resource_id, force=True))
    except p.toolkit.ObjectNotFound:
        # this is ok
        return
    return


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

def _disable_fulltext_trigger(connection, resource_id):
    connection.execute('ALTER TABLE {table} DISABLE TRIGGER zfulltext;'
                       .format(table=identifier(resource_id)))

def _enable_fulltext_trigger(connection, resource_id):
    connection.execute('ALTER TABLE {table} ENABLE TRIGGER zfulltext;'
                       .format(table=identifier(resource_id)))

def _populate_fulltext(connection, resource_id, fields):
    '''Populates the _full_text column. i.e. the same as datastore_run_triggers
    but it runs in 1/9 of the time.

    The downside is that it reimplements the code that calculates the text to
    index, breaking DRY. And its annoying to pass in the column names.

    fields: list of dicts giving the each column's 'id' (name) and 'type'
            (text/numeric/timestamp)
    '''
    sql = \
        u'''
        UPDATE {table}
        SET _full_text = to_tsvector({cols});
        '''.format(
            # coalesce copes with blank cells
            table=identifier(resource_id),
            cols=" || ' ' || ".join(
                'coalesce({}, \'\')'.format(
                    identifier(field['id'])
                    + ('::text' if field['type'] != 'text' else '')
                    )
                for field in fields
                if not field['id'].startswith('_')
                )
            )
    connection.execute(sql)


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
