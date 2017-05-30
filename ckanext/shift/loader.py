'Load a CSV into postgres'
import argparse
import os.path

from sqlalchemy import String, Integer, Table, Column
from sqlalchemy import create_engine, MetaData
import sqlalchemy as sa
import messytables


def load_csv(ckan_ini, csv_filepath, mimetype='text/csv'):

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
    datastore_sqlalchemy_url = \
        get_config_value_without_loading_ckan_environment(
            ckan_ini, 'ckan.datastore.write_url')
    datastore_su_sqlalchemy_url = \
        get_config_value_without_loading_ckan_environment(
            ckan_ini, 'ckan.datastore.su_url')

    table_name = 'test1'
    engine = create_engine(datastore_sqlalchemy_url)
    #engine_su = create_engine(datastore_su_sqlalchemy_url)

    # If table exists, delete (TODO something more sophis)
    metadata = MetaData(engine)
    if engine.dialect.has_table(engine, table_name):
        table = Table(table_name, metadata, autoload=True,
                      autoload_with=engine)
        table.drop()
        #metadata.Session().commit()
        #engine.execute('DROP TABLE {};'.format(table_name))
    #connection = engine.connect()
    # Create table
    # Create a table with text columns
    columns = [Column(header_name, String) for header_name in headers]
    Table(table_name, metadata,
          # Column('id', Integer, primary_key=True, nullable=False),
          *columns,
          extend_existing=True)  # edit columns
    # Implement the creation
    metadata.create_all()

    # COPY zip_codes FROM '/path/to/csv/ZIP_CODES.txt' DELIMITER ',' CSV;
    print('Copying...')
    # can't use :param for table name because params are only for filter values
    # that are single quoted.
    # connection.execute(sa.text(
    #     "\copy {} FROM :filepath DELIMITER ',' CSV".format(table_name)),
    #     {'filepath': csv_filepath})
    # NB can't use "\copy" as that is a psql meta-command and not accessible
    #    via psycopg2
    # NB COPY requires the db user to have superuser privileges. This is dangerous. It is not available on AWS, for example.
    # TODO look at doing what pgloader does - as described in its docs:
    # Note that while the COPY command is restricted to read either from its standard input or from a local file on the server's file system, the command line tool psql implements a \copy command that knows how to stream a file local to the client over the network and into the PostgreSQL server, using the same protocol as pgloader uses.

    # COULDNT GET THIS TO WORK
    # csv_filepath = os.path.abspath(csv_filepath)
    # connection_su = engine_su.connect()
    # ret = connection_su.execute(sa.text(
    #     "COPY {} FROM :filepath WITH (DELIMITER ',', FORMAT csv, HEADER 1);".format(
    #         table_name, csv_filepath)),
    #     {'filepath': csv_filepath})
    # print('...done: {}'.format(ret.rowcount))
    # connection_su.close()
    # cur.close()

    connection = engine.raw_connection()
    cur = connection.cursor()
    with open(csv_filepath, 'rb') as f:
        cur.copy_expert("COPY {} FROM STDIN WITH (DELIMITER ',', FORMAT csv, HEADER 1);".format(
             table_name), f)
        connection.commit()
        cur.close()


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
    load_csv(args.ckan_ini, args.csv_filepath, mimetype='text/csv')
