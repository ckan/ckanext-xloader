'Load a CSV into postgres'
import argparse
import os.path

import ckanext.datastore.backend.postgres as datastore_db

from sqlalchemy import String, Integer, Table, Column
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy import create_engine, MetaData
import messytables


def load_csv(csv_filepath, get_config_value=None, table_name='test1',
             mimetype='text/csv'):

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

    # check tables exists

    # datastore db connection
    if not get_config_value:
        engine = datastore_db.get_write_engine()
    else:
        # i.e. when running from this file's cli
        datastore_sqlalchemy_url = get_config_value('ckan.datastore.write_url')
        engine = create_engine(datastore_sqlalchemy_url)

    # If table exists, delete (TODO something more sophis)
    metadata = MetaData(engine)
    if engine.dialect.has_table(engine, table_name):
        table = Table(table_name, metadata, autoload=True,
                      autoload_with=engine)
        table.drop()

    # Create table
    # All columns are text type - convert them later
    columns = [Column(header_name, String) for header_name in headers]
    columns.insert(0, Column('_id', Integer, primary_key=True))
    columns.insert(1, Column('_full_text', TSVECTOR))
    Table(table_name, metadata,
          *columns,
          extend_existing=True)  # edit columns
    # Implement the creation
    metadata.create_all()

    # COPY zip_codes FROM '/path/to/csv/ZIP_CODES.txt' DELIMITER ',' CSV;
    print('Copying...')

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
                # can't use :param for table name because params are only for
                # filter values that are single quoted.
                cur.copy_expert(
                    "COPY \"{table_name}\" ({column_names}) "
                    "FROM STDIN "
                    "WITH (DELIMITER ',', FORMAT csv, HEADER 1);"
                    .format(
                        table_name=table_name,
                        column_names=', '.join(['"{}"'.format(h)
                                                for h in headers])),
                    f)

                # TODO populate _full_text column, something like this.
                # Maybe make this a datastore function, called from the queue?
                # full_text = [_to_full_text(fields, record)]
                # cur.execute(
                #     'UPDATE "{table_name}" '
                #     'SET _full_text = to_tsvector({text});'
                #     .format(table_name=table_name,
                #             full_text=full_text)
                #     ))
        finally:
            cur.close()
    finally:
        connection.commit()

    print('Done')


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
             mimetype='text/csv')
