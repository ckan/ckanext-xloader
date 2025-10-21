'Load a CSV into postgres'
from __future__ import absolute_import

import datetime
from enum import Enum
import itertools
from six import text_type as str, binary_type
import os
import tempfile
from decimal import Decimal

import psycopg2
from chardet.universaldetector import UniversalDetector
from six.moves import zip
from tabulator import config as tabulator_config, EncodingError, Stream, TabulatorException
from unidecode import unidecode
import sqlalchemy as sa

import ckan.plugins as p

from .job_exceptions import FileCouldNotBeLoadedError, LoaderError
from .parser import CSV_SAMPLE_LINES, TypeConverter
from .utils import datastore_resource_exists, headers_guess, type_guess

from ckan.plugins.toolkit import config

import ckanext.datastore.backend.postgres as datastore_db

get_write_engine = datastore_db.get_write_engine
create_indexes = datastore_db.create_indexes
_drop_indexes = datastore_db._drop_indexes

MAX_COLUMN_LENGTH = 63
tabulator_config.CSV_SAMPLE_LINES = CSV_SAMPLE_LINES

SINGLE_BYTE_ENCODING = 'cp1252'


class FieldMatch(Enum):
    """ Enumerates the possible match results between existing and new fields.

    EXACT_MATCH indicates that the field count, names and types are identical,
    allowing a datastore table to be truncated and reused.

    NAME_MATCH indicates that the field count and names are the same,
    but one or more field types have changed, so the table must be dropped
    and recreated, but the Data Dictionary can be preserved if applicable.

    MISMATCH indicates that the field count or names have changed,
    so the table must be dropped and recreated by guessing the types.
    """
    EXACT_MATCH = 1,
    NAME_MATCH = 2,
    MISMATCH = 3


class UnknownEncodingStream(object):
    """ Provides a context manager that wraps a Tabulator stream
    and tries multiple encodings if one fails.

    This is particularly relevant in cases like Latin-1 encoding,
    which is usually ASCII and thus the sample could be sniffed as UTF-8,
    only to run into problems later in the file.
    """

    def __init__(self, filepath, file_format, decoding_result, **kwargs):
        self.filepath = filepath
        self.file_format = file_format
        self.stream_args = kwargs
        self.decoding_result = decoding_result  # {'encoding': 'EUC-JP', 'confidence': 0.99}

    def __enter__(self):
        try:

            if (self.decoding_result and self.decoding_result['confidence'] and self.decoding_result['confidence'] > 0.7):
                self.stream = Stream(self.filepath, format=self.file_format, encoding=self.decoding_result['encoding'],
                                     ** self.stream_args).__enter__()
            else:
                self.stream = Stream(self.filepath, format=self.file_format, ** self.stream_args).__enter__()

        except (EncodingError, UnicodeDecodeError):
            self.stream = Stream(self.filepath, format=self.file_format,
                                 encoding=SINGLE_BYTE_ENCODING, **self.stream_args).__enter__()
        return self.stream

    def __exit__(self, *args):
        return self.stream.__exit__(*args)


def detect_encoding(file_path):
    detector = UniversalDetector()
    with open(file_path, 'rb') as file:
        for line in file:
            detector.feed(line)
            if detector.done:
                break
    detector.close()
    return detector.result  # e.g. {'encoding': 'EUC-JP', 'confidence': 0.99}


def _fields_match(fields, existing_fields, logger):
    ''' Check whether all columns have the same names and types as previously,
    independent of ordering.

    Returns one of the values of FieldMatch.
    '''
    # drop the generated '_id' field
    for index in range(len(existing_fields)):
        if existing_fields[index]['id'] == '_id':
            existing_fields.pop(index)
            break

    # fail fast if number of fields doesn't match
    field_count = len(fields)
    if field_count != len(existing_fields):
        logger.info("Fields do not match; there are now %s fields but previously %s", field_count, len(existing_fields))
        return FieldMatch.MISMATCH

    # ensure each field is present in both collections and check for type changes
    type_changed = False
    for index in range(field_count):
        field_id = fields[index]['id']
        for existing_index in range(field_count):
            existing_field_id = existing_fields[existing_index]['id']
            if field_id == existing_field_id:
                if fields[index]['type'] != existing_fields[existing_index]['type']:
                    logger.info("Field has changed; new type for %s field is %s but existing type is %s",
                                field_id, fields[index]["type"], existing_fields[existing_index]['type'])
                    type_changed = True
                break
        else:
            logger.info("Fields do not match; no existing entry found for %s", field_id)
            return FieldMatch.MISMATCH
    return FieldMatch.NAME_MATCH if type_changed else FieldMatch.EXACT_MATCH


def _clear_datastore_resource(resource_id):
    ''' Delete all records from the datastore table, without dropping the table itself.
    '''
    engine = get_write_engine()
    with engine.begin() as conn:
        conn.execute(sa.text("SET LOCAL lock_timeout = '15s'"))
        conn.execute(sa.text('TRUNCATE TABLE "{}" RESTART IDENTITY'.format(resource_id)))


def copy_file(csv_filepath, engine, logger, resource_id, headers, delimiter):
    # Options for loading into postgres:
    # 1. \copy - can't use as that is a psql meta-command and not accessible
    #    via psycopg2
    # 2. COPY - requires the db user to have superuser privileges. This is
    #    dangerous. It is also not available on AWS, for example.
    # 3. pgloader method? - as described in its docs:
    #
    #    Note that while the COPY command is restricted to read either from
    #    its standard input or from a local file on the server's file system,
    #    the command line tool psql implements a \copy command that knows
    #    how to stream a file local to the client over the network and into
    #    the PostgreSQL server, using the same protocol as pgloader uses.
    # 4. COPY FROM STDIN - not quite as fast as COPY from a file, but avoids
    #    the superuser issue. <-- picked

    with engine.begin() as conn:
        cur = conn.connection.cursor()
        #cur.execute('SET DATESTYLE TO "SQL , MDY"')
        try:
            with open(csv_filepath, 'rb') as f:
                # can't use :param for table name because params are only
                # for filter values that are single quoted.
                try:
                    cur.copy_expert(
                        "COPY \"{resource_id}\" ({column_names}) "
                        "FROM STDIN "
                        "WITH (DELIMITER '{delimiter}', FORMAT csv, HEADER 1, "
                        "      ENCODING '{encoding}');"
                        .format(
                            resource_id=resource_id,
                            column_names=', '.join(['"{}"'.format(h)
                                                    for h in headers]),
                            delimiter=delimiter,
                            encoding='UTF8',
                        ),
                        f)
                except psycopg2.DataError as e:
                    # e is a str but with foreign chars e.g.
                    # 'extra data: "paul,pa\xc3\xbcl"\n'
                    # but logging and exceptions need a normal (7 bit) str
                    error_str = str(e)
                    logger.warning('%s: %s', resource_id, error_str)
                    raise LoaderError('Error during the load into PostgreSQL:'
                                        ' {}'.format(error_str))  
        finally:
            cur.close()

def split_copy_by_size(input_file, engine, logger,  resource_id, headers, delimiter = ',',  max_size=1024**3, encoding='utf-8'):  # 1 Gigabyte
    """
    Reads a CSV file, splits it into chunks of maximum size, and writes each chunk
    to PostgreSQL COPY command to load the data into a table.

    Args:
        input_file (str): Path to the input CSV file.
        max_size (int, optional): Maximum size (in bytes) of each output file. Defaults to 1 Gigabyte.
        tablename (str, optional): Name of the target table in PostgreSQL for the COPY command. Defaults to None.
        columns (list, optional): List of column names for the COPY command, matching the CSV header. Defaults to None.
        connection (str, optional): Connection string for the PostgreSQL database. Defaults to an empty string.
        delimiter (str, optional): Delimiter character used in the CSV file. Defaults to ','.
    """
    
    chunk_count = 0
    file_size = os.path.getsize(input_file)
    logger.info('Starting chunked processing for file size: %s bytes with chunk size: %s bytes', file_size, max_size)

    with open(input_file, 'r', encoding = encoding) as infile:
        current_file = None
        output_filename = f'/tmp/output_{resource_id}.csv'
        header = False
        for row in infile:
            if current_file is None or current_file.tell() >= max_size:
                # Close previous file if necessary
                if current_file:
                    chunk_count += 1
                    logger.debug('Before copying chunk %s: %s', chunk_count, output_filename)
                    copy_file(output_filename, engine, logger, resource_id, headers, delimiter)
                    logger.debug('Copied chunk %s: %s', chunk_count, output_filename)
                    current_file.close()
                    header = True

                current_file = open(output_filename, 'w')
                if header:
                    current_file.write(delimiter.join(headers) + '\n')
            current_file.write(row)
            

        # Close the last file if open
        if current_file:
            current_file.close()

        # Copy the last file
        chunk_count += 1
        logger.debug('Before copying final chunk %s: %s', chunk_count, output_filename)
        copy_file(output_filename, engine, logger, resource_id, headers, delimiter)
        logger.debug('Copied final chunk %s: %s', chunk_count, output_filename)
        os.remove(output_filename)
        
    logger.info('Completed chunked processing: %s chunks processed for file size %s bytes', chunk_count, file_size)
    if infile:
        infile.close()        


def _read_metadata(table_filepath, mimetype, logger):
    # Determine the header row
    logger.info('Determining column names and types')
    decoding_result = detect_encoding(table_filepath)
    logger.debug("Decoded encoding: %s", decoding_result)
    try:
        file_format = os.path.splitext(table_filepath)[1].strip('.')
        with UnknownEncodingStream(table_filepath, file_format, decoding_result,
                                   skip_rows=[{'type': 'preset', 'value': 'blank'}],
                                   post_parse=[TypeConverter().convert_types]) as stream:
            header_offset, headers = headers_guess(stream.sample)
    except TabulatorException:
        try:
            file_format = mimetype.lower().split('/')[-1]
            with UnknownEncodingStream(table_filepath, file_format, decoding_result,
                                       skip_rows=[{'type': 'preset', 'value': 'blank'}],
                                       post_parse=[TypeConverter().convert_types]) as stream:
                header_offset, headers = headers_guess(stream.sample)
        except TabulatorException as e:
            raise LoaderError('Tabulator error: {}'.format(e))
    except Exception as e:
        raise FileCouldNotBeLoadedError(e)

    # Some headers might have been converted from strings to floats and such.
    return (
        file_format,
        decoding_result,
        header_offset,
        encode_headers(headers),
        stream,
    )


def _read_existing_fields(resource_id):
    # get column info from existing table
    existing = datastore_resource_exists(resource_id)
    if existing:
        if p.toolkit.check_ckan_version(min_version='2.11'):
            ds_info = p.toolkit.get_action('datastore_info')({'ignore_auth': True}, {'id': resource_id})
            existing_fields = ds_info.get('fields', [])
        else:
            existing_fields = existing.get('fields', [])
        existing_info = dict((f['id'], f['info'])
                             for f in existing_fields
                             if 'info' in f)
        existing_fields_by_headers = dict((f['id'], f)
                                          for f in existing_fields)
        return (True, existing_info, existing_fields, existing_fields_by_headers)
    else:
        return (False, None, None, None)


def load_csv(csv_filepath, resource_id, mimetype='text/csv', allow_type_guessing=False, logger=None):
    '''Loads a CSV into DataStore. Does not create the indexes.

    allow_type_guessing: Whether to fall back to Tabulator type-guessing
    in the event that the resource already existed but its structure has
    changed.
    '''

    file_format, decoding_result, header_offset, headers, stream = _read_metadata(csv_filepath, mimetype, logger)

    # Get the list of rows to skip. The rows in the tabulator stream are
    # numbered starting with 1.
    skip_rows = list(range(1, header_offset + 1))
    skip_rows.append({'type': 'preset', 'value': 'blank'})

    # Get the delimiter used in the file
    delimiter = stream.dialect.get('delimiter')
    if delimiter is None:
        logger.warning('Could not determine delimiter from file, use default ","')
        delimiter = ','

    # Strip leading and trailing whitespace, then truncate to maximum length,
    # then strip again in case the truncation exposed a space.
    headers = [
        header.strip()[:MAX_COLUMN_LENGTH].strip()
        for header in headers
        if header and header.strip()
    ]

    # TODO worry about csv header name problems
    # e.g. duplicate names

    # encoding (and line ending?)- use chardet
    # It is easier to reencode it as UTF8 than convert the name of the encoding
    # to one that pgloader will understand.
    logger.info('Ensuring character coding is UTF8')
    f_write = tempfile.NamedTemporaryFile(suffix=file_format, delete=False)

    # get column info from existing table
    existing, existing_info, existing_fields, existing_fields_by_headers = _read_existing_fields(resource_id)
    if existing:
        # Column types are either set (overridden) in the Data Dictionary page
        # or default to text type (which is robust)
        fields = [
            {'id': header_name,
             'type': existing_info.get(header_name, {})
                .get('type_override') or 'text',
             }
            for header_name in headers]

        # Maintain data dictionaries from matching column names
        for f in fields:
            if f['id'] in existing_info:
                f['info'] = existing_info[f['id']]
                f['strip_extra_white'] = existing_info[f['id']].get('strip_extra_white') if 'strip_extra_white' in existing_info[f['id']] \
                    else existing_fields_by_headers[f['id']].get('strip_extra_white', True)

        '''
        Delete or truncate existing datastore table before proceeding,
        depending on whether any fields have changed.
        Otherwise the COPY will append to the existing table.
        And if the fields have significantly changed, it may also fail.
        '''
        fields_match = _fields_match(fields, existing_fields, logger)
        if fields_match == FieldMatch.EXACT_MATCH:
            logger.info('Clearing records for "%s" from DataStore.', resource_id)
            _clear_datastore_resource(resource_id)
        else:
            logger.info('Deleting "%s" from DataStore.', resource_id)
            delete_datastore_resource(resource_id)
            # if file structure has changed,
            # and it wasn't just from a Data Dictionary override,
            # then we need to re-guess types
            if allow_type_guessing and fields_match == FieldMatch.MISMATCH:
                raise LoaderError("File structure has changed, reverting to Tabulator")
    else:
        fields = [
            {'id': header_name,
             'type': 'text',
             'strip_extra_white': True}
            for header_name in headers]

    logger.info('Fields: %s', fields)

    def _make_whitespace_stripping_iter(super_iter):
        def strip_white_space_iter():
            for row in super_iter():
                if len(row) == len(fields):
                    for _index, _cell in enumerate(row):
                        # only strip white space if strip_extra_white is True
                        if fields[_index].get('strip_extra_white', True) and isinstance(_cell, str):
                            row[_index] = _cell.strip()
                yield row
        return strip_white_space_iter

    save_args = {'target': f_write.name, 'format': 'csv', 'encoding': 'utf-8', 'delimiter': delimiter}
    try:
        with UnknownEncodingStream(csv_filepath, file_format, decoding_result,
                                   skip_rows=skip_rows) as stream:
            stream.iter = _make_whitespace_stripping_iter(stream.iter)
            stream.save(**save_args)
    except (EncodingError, UnicodeDecodeError):
        with Stream(csv_filepath, format=file_format, encoding=SINGLE_BYTE_ENCODING,
                    skip_rows=skip_rows) as stream:
            stream.iter = _make_whitespace_stripping_iter(stream.iter)
            stream.save(**save_args)
    csv_filepath = f_write.name

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
            # e.g. {'message': None, 'error_dict': {'fields': [u'"***" is not a valid field name']}, '_error_summary': None}  # noqa
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

    # datastore_active is switched on by datastore_create
    # TODO temporarily disable it until the load is complete

    engine = get_write_engine()
    with engine.begin() as conn:
        _disable_fulltext_trigger(conn, resource_id)

    with engine.begin() as conn:
        context['connection'] = conn
        _drop_indexes(context, data_dict, False)

    logger.info('Copying to database...')

    # Copy file to datastore db, split to chunks.
    max_size = config.get('ckanext.xloader.copy_chunk_size', 1024**3)
    logger.debug('Using chunk size: %s bytes for resource %s', max_size, resource_id)
    split_copy_by_size(csv_filepath, engine, logger,  resource_id, headers, delimiter, int(max_size))

    logger.info('...copying done')

    logger.info('Creating search index...')
    with engine.begin() as conn:
        _populate_fulltext(conn, resource_id, fields=fields, logger=logger)
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


def _save_type_overrides(headers_dicts):
    # copy 'type' to 'type_override' if it's not the default type (text)
    # and there isn't already an override in place
    for h in headers_dicts:
        if h['type'] != 'text':
            if 'info' in h:
                if 'type_override' not in h['info']:
                    h['info']['type_override'] = h['type']
            else:
                h['info'] = {'type_override': h['type']}


def load_table(table_filepath, resource_id, mimetype='text/csv', logger=None):
    '''Loads an Excel file (or other tabular data recognized by tabulator)
    into Datastore and creates indexes.

    Largely copied from datapusher - see below. Is slower than load_csv.
    '''

    file_format, decoding_result, header_offset, headers, stream = _read_metadata(table_filepath, mimetype, logger)

    existing, existing_info, existing_fields, existing_fields_by_headers = _read_existing_fields(resource_id)

    # Get the list of rows to skip. The rows in the tabulator stream are
    # numbered starting with 1. We also want to skip the header row.
    skip_rows = list(range(1, header_offset + 2))
    skip_rows.append({'type': 'preset', 'value': 'blank'})

    TYPES, TYPE_MAPPING = get_types()
    strict_guessing = p.toolkit.asbool(
        config.get('ckanext.xloader.strict_type_guessing', True))
    types = type_guess(stream.sample[1:], types=TYPES, strict=strict_guessing)
    fields = []

    # override with types user requested
    if existing:
        types = [
            {
                'text': str,
                'numeric': Decimal,
                'timestamp': datetime.datetime,
            }.get(existing_info.get(h, {}).get('type_override'), t)
            for t, h in zip(types, headers)]
        for h in headers:
            fields.append(existing_fields_by_headers.get(h, {}))
    else:
        # default strip_extra_white
        for h in headers:
            fields.append({'strip_extra_white': True})

    # Strip leading and trailing whitespace, then truncate to maximum length,
    # then strip again in case the truncation exposed a space.
    headers = [
        header.strip()[:MAX_COLUMN_LENGTH].strip()
        for header in headers
        if header and header.strip()
    ]
    header_count = len(headers)
    type_converter = TypeConverter(types=types, fields=fields)

    with UnknownEncodingStream(table_filepath, file_format, decoding_result,
                               skip_rows=skip_rows,
                               post_parse=[type_converter.convert_types]) as stream:
        def row_iterator():
            for row in stream:
                data_row = {}
                for index, cell in enumerate(row):
                    # Handle files that have extra blank cells in heading and body
                    # eg from Microsoft Excel adding lots of empty cells on export.
                    # Blank header cells won't generate a column,
                    # so row length won't match column count.
                    if index >= header_count:
                        # error if there's actual data out of bounds, otherwise ignore
                        if cell:
                            raise LoaderError("Found data in column %s but resource only has %s header(s)",
                                              index + 1, header_count)
                        else:
                            continue
                    data_row[headers[index]] = cell
                yield data_row
        result = row_iterator()

        headers_dicts = [dict(id=field[0], type=TYPE_MAPPING[str(field[1])])
                         for field in zip(headers, types)]

        # Maintain data dictionaries from matching column names
        if existing:
            for h in headers_dicts:
                if h['id'] in existing_info:
                    h['info'] = existing_info[h['id']]
                    h['strip_extra_white'] = existing_info[h['id']].get('strip_extra_white') if 'strip_extra_white' in existing_info[h['id']] \
                        else existing_fields_by_headers[h['id']].get('strip_extra_white', True)
                    # create columns with types user requested
                    type_override = existing_info[h['id']].get('type_override')
                    if type_override in list(_TYPE_MAPPING.values()):
                        h['type'] = type_override
        else:
            # default strip_extra_white
            for h in headers_dicts:
                h['strip_extra_white'] = True

        # preserve any types that we have sniffed unless told otherwise
        _save_type_overrides(headers_dicts)

        logger.info('Determined headers and types: %s', headers_dicts)

        '''
        Delete or truncate existing datastore table before proceeding,
        depending on whether any fields have changed.
        Otherwise 'datastore_create' will append to the existing datastore.
        And if the fields have significantly changed, it may also fail.
        '''
        if existing:
            if _fields_match(headers_dicts, existing_fields, logger) == FieldMatch.EXACT_MATCH:
                logger.info('Clearing records for "%s" from DataStore.', resource_id)
                _clear_datastore_resource(resource_id)
            else:
                logger.info('Deleting "%s" from datastore.', resource_id)
                delete_datastore_resource(resource_id)

        logger.info('Copying to database...')
        count = 0
        # Some types cannot be stored as empty strings and must be converted to None,
        # https://github.com/ckan/ckanext-xloader/issues/182
        non_empty_types = ['timestamp', 'numeric']
        for i, records in enumerate(chunky(result, 250)):
            count += len(records)
            logger.info('Saving chunk %s', i)
            for row in records:
                for column_index, column_name in enumerate(row):
                    if headers_dicts[column_index]['type'] in non_empty_types and row[column_name] == '':
                        row[column_name] = None
            send_resource_to_datastore(resource_id, headers_dicts, records)
        logger.info('...copying done')

    if count:
        logger.info('Successfully pushed %s entries to "%s".', count, resource_id)
    else:
        # no datastore table is created
        raise LoaderError('No entries found - nothing to load')


_TYPE_MAPPING = {
    "<type 'str'>": 'text',
    "<type 'unicode'>": 'text',
    "<type 'bytes'>": 'text',
    "<type 'bool'>": 'text',
    "<type 'int'>": 'numeric',
    "<type 'float'>": 'numeric',
    "<class 'decimal.Decimal'>": 'numeric',
    "<type 'datetime.datetime'>": 'timestamp',
    "<class 'str'>": 'text',
    "<class 'unicode'>": 'text',
    "<class 'bytes'>": 'text',
    "<class 'bool'>": 'text',
    "<class 'int'>": 'numeric',
    "<class 'float'>": 'numeric',
    "<class 'datetime.datetime'>": 'timestamp',
}


def get_types():
    _TYPES = [int, bool, str, binary_type, datetime.datetime, float, Decimal]
    TYPE_MAPPING = config.get('TYPE_MAPPING', _TYPE_MAPPING)
    return _TYPES, TYPE_MAPPING


def encode_headers(headers):
    encoded_headers = []
    for header in headers:
        try:
            encoded_headers.append(unidecode(header))
        except AttributeError:
            encoded_headers.append(unidecode(str(header)))

    return encoded_headers


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


def delete_datastore_resource(resource_id):
    from ckan import model
    context = {'model': model, 'user': '', 'ignore_auth': True}
    try:
        p.toolkit.get_action('datastore_delete')(context, dict(
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
    res = connection.execute(sa.text('''
        select * from pg_proc where proname = 'populate_full_text_trigger';
        '''))
    return bool(res.rowcount)


def fulltext_trigger_exists(connection, resource_id):
    '''Check to see if the fulltext trigger is set-up on this resource's table.
    This will only be the case if your CKAN is new enough to have:
    https://github.com/ckan/ckan/pull/3786
    '''
    res = connection.execute(sa.text('''
        SELECT pg_trigger.tgname FROM pg_class
        JOIN pg_trigger ON pg_class.oid=pg_trigger.tgrelid
        WHERE pg_class.relname={table}
        AND pg_trigger.tgname='zfulltext';
        '''.format(
        table=literal_string(resource_id))))
    return bool(res.rowcount)


def _disable_fulltext_trigger(connection, resource_id):
    connection.execute(sa.text('ALTER TABLE {table} DISABLE TRIGGER zfulltext;'
                       .format(table=identifier(resource_id, True))))


def _enable_fulltext_trigger(connection, resource_id):
    connection.execute(sa.text(
        'ALTER TABLE {table} ENABLE TRIGGER zfulltext;'
        .format(table=identifier(resource_id, True))))


def _get_rows_count_of_resource(connection, table):
    count_query = ''' SELECT count(_id) from {table} '''.format(table=table)
    results = connection.execute(count_query)
    rows_count = int(results.first()[0])
    return rows_count


def _populate_fulltext(connection, resource_id, fields, logger):
    '''Populates the _full_text column for full-text search functionality.
    
    This function creates a PostgreSQL tsvector (text search vector) for each row
    by concatenating all non-system columns. It's equivalent to datastore_run_triggers
    but runs approximately 9x faster by using direct SQL updates.
    
    To handle very large datasets (e.g., 4GB+ files with millions of rows), the update
    operation is partitioned into chunks to prevent:
    - Database statement timeouts
    - Memory exhaustion
    - Lock contention that could block other operations
    - Transaction log overflow
    
    The chunking mechanism processes rows in batches based on their _id values,
    with chunk size configurable via 'ckanext.xloader.search_update_chunks' 
    (default: 100,000 rows per chunk).
    
    Args:
        connection: Database connection object
        resource_id (str): The datastore table identifier
        fields (list): List of dicts with column 'id' (name) and 'type' 
            (text/numeric/timestamp)
        logger: Logger instance for progress tracking
        
    Note:
        This reimplements CKAN's text indexing logic for performance,
        breaking DRY principle but providing significant speed improvements.
    '''
    try:
        # Get total row count to determine chunking strategy
        rows_count = _get_rows_count_of_resource(connection, identifier(resource_id))
    except Exception as e:
        rows_count = ''
        logger.info("Failed to get resource rows count: {} ".format(str(e)))
        raise
        
    if rows_count:
        # Configure chunk size - prevents timeouts and memory issues on large datasets
        # Default 100,000 rows per chunk balances performance vs. resource usage
        chunks = int(config.get('ckanext.xloader.search_update_chunks', 100000))
        
        # Process table in chunks using _id range queries
        # This approach ensures consistent chunk sizes and allows resuming if interrupted
        for start in range(0, rows_count, chunks):
            try:
                # Build SQL to update _full_text column with concatenated searchable content
                sql = \
                    '''
                    UPDATE {table}
                    SET _full_text = to_tsvector({cols}) WHERE _id BETWEEN {first} and {end};
                    '''.format(
                        table=identifier(resource_id),
                        # Concatenate all user columns (excluding system columns starting with '_')
                        # coalesce() handles NULL values by converting them to empty strings
                        cols=" || ' ' || ".join(
                            'coalesce({}, \'\')'.format(
                                identifier(field['id'])
                                + ('::text' if field['type'] != 'text' else '')  # Cast non-text types
                            )
                            # Skip system columns like _id, _full_text
                            for field in fields if not field['id'].startswith('_')
                        ),
                        first=start,
                        end=start + chunks
                    )
                connection.execute(sql)
                logger.info("Indexed rows {first} to {end} of {total}".format(
                    first=start, end=min(start + chunks, rows_count), total=rows_count))
            except Exception as e:
                # Log chunk-specific errors but continue processing remaining chunks
                logger.error("Failed to index rows {first}-{end}: {error}".format(
                    first=start, end=start + chunks, error=str(e)))


def calculate_record_count(resource_id, logger):
    '''
    Calculate an estimate of the record/row count and store it in
    Postgresql's pg_stat_user_tables. This number will be used when
    specifying `total_estimation_threshold`
    '''
    logger.info('Calculating record count (running ANALYZE on the table)')
    engine = get_write_engine()
    conn = engine.connect()
    conn.execute(sa.text("ANALYZE \"{resource_id}\";"
                         .format(resource_id=resource_id)))


def identifier(s, escape_binds=False):
    # "%" needs to be escaped, otherwise connection.execute thinks it is for
    # substituting a bind parameter
    escaped = s.replace(u'"', u'""').replace(u'\0', '')
    if escape_binds:
        escaped = escaped.replace('%', '%%')

    return u'"' + escaped + u'"'


def literal_string(s):
    return u"'" + s.replace(u"'", u"''").replace(u'\0', '') + u"'"
