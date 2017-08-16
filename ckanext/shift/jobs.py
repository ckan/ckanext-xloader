import logging
import hashlib
import cStringIO
import time
import tempfile
import requests
import json
import datetime
import urlparse
from rq import get_current_job
import traceback
import sys

from pylons import config
import ckan.lib.search as search
import sqlalchemy as sa

import loader
import db

DOWNLOAD_TIMEOUT = 30

if config.get('ckanext.shift.ssl_verify') in [False, 'False', 'FALSE', '0']:
    SSL_VERIFY = False
else:
    SSL_VERIFY = True
if not SSL_VERIFY:
    requests.packages.urllib3.disable_warnings()

MAX_CONTENT_LENGTH = config.get('ckanext.shift.max_content_length') \
    or 10485760

# 'api_key': user['apikey'],
# 'job_type': 'push_to_datastore',
# 'result_url': callback_url,
# 'metadata': {
#     'ignore_hash': data_dict.get('ignore_hash', False),
#     'ckan_url': site_url,
#     'resource_id': res_id,
#     'set_url_type': data_dict.get('set_url_type', False),
#     'task_created': task['last_updated'],
#     'original_url': resource_dict.get('url'),
#     }

def shift_data_into_datastore(input):
    '''This is the func that is queued. It is a wrapper for
    shift_data_into_datastore, and makes sure it finishes by calling
    shift_hook to update the task_status with the result.

    Errors are stored in task_status / job log. If saving those fails, then
    we return False (but that's not stored anywhere currently).
    '''
    # First flag that this task is running, to indicate the job is not
    # stillborn, for when shift_submit is deciding whether another job would
    # be a duplicate or not
    job_dict = dict(metadata=input['metadata'],
                    status='running')
    callback_shift_hook(result_url=input['result_url'],
                        api_key=input['api_key'],
                        job_dict=job_dict)

    job_id = get_current_job().id
    try:
        shift_data_into_datastore_(input)
        job_dict['status'] = 'complete'
        db.mark_job_as_completed(job_id, job_dict)
    except JobError as e:
        db.mark_job_as_errored(job_id, e.as_dict())
        job_dict['status'] = 'error'
        job_dict['error'] = str(e)
        log = logging.getLogger(__name__)
        log.error('Shift error: {}'.format(e))
    except Exception as e:
        db.mark_job_as_errored(
            job_id, traceback.format_tb(sys.exc_traceback)[-1] + repr(e))
        job_dict['status'] = 'error'
        job_dict['error'] = str(e)
        log = logging.getLogger(__name__)
        log.error('Shift error: {}'.format(e))
    finally:
        # job_dict is defined in shift_hook's docstring
        return callback_shift_hook(result_url=input['result_url'],
                                   api_key=input['api_key'],
                                   job_dict=job_dict)


def shift_data_into_datastore_(input):
    '''This function:
    * downloads the resource (metadata) from CKAN
    * downloads the data
    * calls the loader to load the data into DataStore
    * calls back to CKAN with the new status

    (ckanext-shift called this function 'shift_to_datastore')
    '''
    job_id = get_current_job().id
    db.init(config)

    # Store details of the job in the db
    try:
        db.add_pending_job(job_id, **input)
    except sa.exc.IntegrityError:
        raise JobError('job_id {} already exists'.format(job_id))

    # Set-up logging to the db
    handler = StoringHandler(job_id, input)
    level = logging.DEBUG
    handler.setLevel(level)
    logger = logging.getLogger(job_id)
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(handler)  # saves logs to the db TODO
    # also show logs on stderr
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)

    validate_input(input)

    data = input['metadata']

    ckan_url = data['ckan_url']
    resource_id = data['resource_id']
    api_key = input.get('api_key')

    try:
        resource = get_resource(resource_id, ckan_url, api_key)
    except JobError, e:
        # try again in 5 seconds just in case CKAN is slow at adding resource
        time.sleep(5)
        resource = get_resource(resource_id, ckan_url, api_key)

    # check if the resource url_type is a datastore
    if resource.get('url_type') == 'datastore':
        logger.info('Ignoring resource - url_type=datastore - dump files are '
                    'managed with the Datastore API')
        return

    # fetch the resource data
    logger.info('Fetching from: {0}'.format(resource.get('url')))
    try:
        headers = {}
        if resource.get('url_type') == 'upload':
            # If this is an uploaded file to CKAN, authenticate the request,
            # otherwise we won't get file from private resources
            headers['Authorization'] = api_key

        response = requests.get(
            resource.get('url'), headers=headers, timeout=DOWNLOAD_TIMEOUT)
        response.raise_for_status()
    except requests.exceptions.HTTPError as error:
        # status code error
        logger.error('HTTP error: {}'.format(error))
        raise HTTPError(
            "DataPusher received a bad HTTP response when trying to download "
            "the data file", status_code=error.response.status_code,
            request_url=resource.get('url'), response=error)
    except requests.exceptions.Timeout:
        logger.error('URL time out after {0}s'.format(DOWNLOAD_TIMEOUT))
        raise JobError('Connection timed out after {}s'.format(
                       DOWNLOAD_TIMEOUT))
    except requests.exceptions.RequestException as e:
        try:
            err_message = str(e.reason)
        except AttributeError:
            err_message = str(e)
        logger.error('URL error: {}'.format(err_message))
        raise HTTPError(
            message=err_message, status_code=None,
            request_url=resource.get('url'), response=None)
    logger.info('Downloaded ok')

    cl = response.headers.get('content-length')
    if cl and int(cl) > MAX_CONTENT_LENGTH:
        error_msg = 'Resource too large to download: {cl} > max ({max_cl}).'\
            .format(cl=cl, max_cl=MAX_CONTENT_LENGTH)
        logger.error(error_msg)
        raise JobError(error_msg)

    f = cStringIO.StringIO(response.content)
    file_hash = hashlib.md5(f.read()).hexdigest()
    f.seek(0)

    if (resource.get('hash') == file_hash
            and not data.get('ignore_hash')):
        logger.info('Ignoring resource - the file hash hasn\'t changed: '
                    '{hash}.'.format(hash=file_hash))
        return
    logger.info('File hash: {}'.format(file_hash))

    resource['hash'] = file_hash

    # Write it to a tempfile on disk
    # TODO Can't we just pass the stringio object?
    # TODO or stream it straight to disk and avoid it all being in memory?
    url = resource.get('url')
    filename = url.split('/')[-1].split('#')[0].split('?')[0]
    with tempfile.NamedTemporaryFile(suffix=filename) as f_:
        f_.write(f.read())
        f_.seek(0)

        # Load it
        def get_config_value(key):
            return config[key]
        logger.info('Loading CSV')
        loader.load_csv(f_.name, get_config_value=get_config_value,
                        table_name=resource['id'],
                        mimetype=resource.get('format'),
                        logger=logger)
        logger.info('Finished loading CSV')

    # try:
    #     table_set = messytables.any_tableset(f, mimetype=ct, extension=ct)
    # except messytables.ReadError as e:
    #     # try again with format
    #     f.seek(0)
    #     try:
    #         format = resource.get('format')
    #         table_set = messytables.any_tableset(f, mimetype=format,
    #                                              extension=format)
    #     except Exception:
    #         raise JobError(e)

    # row_set = table_set.tables.pop()
    # header_offset, headers = messytables.headers_guess(row_set.sample)

    # # Some headers might have been converted from strings to floats and such.
    # headers = [unicode(header) for header in headers]

    # # Setup the converters that run when you iterate over the row_set.
    # # With pgloader only the headers will be iterated over.
    # row_set.register_processor(messytables.headers_processor(headers))
    # row_set.register_processor(
    #     messytables.offset_processor(header_offset + 1))
    # types = messytables.type_guess(row_set.sample, types=TYPES, strict=True)

    # headers = [header.strip() for header in headers if header.strip()]
    # headers_dicts = [dict(id=field[0], type=TYPE_MAPPING[str(field[1])])
    #                  for field in zip(headers, types)]

    # # pgloader only handles csv
    # use_pgloader = web.app.config.get('USE_PGLOADER', True) and \
    #     isinstance(row_set, messytables.CSVRowSet)

    # # Delete existing datastore resource before proceeding. Otherwise
    # # 'datastore_create' will append to the existing datastore. And if
    # # the fields have significantly changed, it may also fail.
    # existing = datastore_resource_exists(resource_id, api_key, ckan_url)
    # if existing:
    #     if not dry_run:
    #         logger.info('Deleting "{res_id}" from datastore.'.format(
    #             res_id=resource_id))
    #         delete_datastore_resource(resource_id, api_key, ckan_url)
    # elif use_pgloader:
    #     # Create datastore table - pgloader needs this
    #     logger.info('Creating datastore table for resource: %s',
    #                 resource['id'])
    #     # create it by calling update with 0 records
    #     send_resource_to_datastore(resource['id'], headers_dicts,
    #                                [], api_key, ckan_url)
    #     # it also sets "datastore_active=True" on the resource

    # # Maintain data dictionaries from matching column names
    # if existing:
    #     existing_info = dict(
    #         (f['id'], f['info'])
    #         for f in existing.get('fields', []) if 'info' in f)
    #     for h in headers_dicts:
    #         if h['id'] in existing_info:
    #             h['info'] = existing_info[h['id']]

    # logger.info('Determined headers and types: {headers}'.format(
    #     headers=headers_dicts))

    # if use_pgloader:
    #     csv_dialect = row_set._dialect()
    #     f.seek(0)
    #     # Save CSV to a file
    #     # TODO rather than save it, pipe in the data:
    #     # http://stackoverflow.com/questions/163542/python-how-do-i-pass-a-string-into-subprocess-popen-using-the-stdin-argument
    #     # then it won't be all in memory at once.
    #     with tempfile.NamedTemporaryFile() as saved_file:
    #         # csv_buffer = f.read()
    #         # pgloader doesn't detect encoding. Use chardet then. It is easier
    #         # to reencode it as UTF8 than convert the name of the encoding to
    #         # one that pgloader will understand.
    #         csv_decoder = messytables.commas.UTF8Recoder(f, encoding=None)
    #         csv_unicode = csv_decoder.reader.read()
    #         csv_buffer = csv_unicode.encode('utf8')
    #         # pgloader only allows a single character line terminator. See:
    #         # https://github.com/dimitri/pgloader/issues/508#issuecomment-275878600
    #         # However we can't use that solution because the last column may
    #         # not be of type text. Therefore change the line endings before
    #         # giving it to pgloader.
    #         if len(csv_dialect.lineterminator) > 1:
    #             csv_buffer = csv_buffer.replace(
    #                 csv_dialect.lineterminator, b'\n')
    #             csv_dialect.lineterminator = b'\n'
    #         saved_file.write(csv_buffer)
    #         saved_file.flush()
    #         csv_filepath = saved_file.name
    #         skip_header_rows = header_offset + 1
    #         load_data_with_pgloader(
    #             resource['id'], csv_filepath, headers, skip_header_rows,
    #             csv_dialect, ckan_url, api_key, dry_run, logger)
    # else:
    #     row_set.register_processor(messytables.types_processor(types))

    #     ret = convert_and_load_data(
    #         resource['id'], row_set, headers, headers_dicts,
    #         ckan_url, api_key, dry_run, logger)
    #     if dry_run:
    #         return ret

    # Set resource.url_type = 'datapusher'
    if data.get('set_url_type', False):
        logger.info('Setting resource.url_type = \'datapusher\'')
        update_resource(resource, api_key, ckan_url)

    # Set resource.datastore_active = True
    if resource.get('datastore_active') is not True:
        from ckan import model
        logger.info('Setting resource.datastore_active = True')
        set_datastore_active_flag(model=model, data_dict=data, flag=True)

    logger.info('Shift completed')

def callback_shift_hook(result_url, api_key, job_dict):
    '''Tells CKAN about the result of the shift (i.e. calls the callback
    function 'shift_hook'). Usually called by the shift queue job.
    Returns whether it managed to call the sh
    '''
    api_key_from_job = job_dict.pop('api_key', None)
    if not api_key:
        api_key = api_key_from_job
    headers = {'Content-Type': 'application/json'}
    if api_key:
        if ':' in api_key:
            header, key = api_key.split(':')
        else:
            header, key = 'Authorization', api_key
        headers[header] = key

    try:
        result = requests.post(
            result_url,
            data=json.dumps(job_dict, cls=DatetimeJsonEncoder),
            headers=headers)
    except requests.ConnectionError:
        return False

    return result.status_code == requests.codes.ok


# def set_datastore_active_flag(resource_id):
#     # equivalent to datapusher's set_datastore_active_flag, but it doesn't have
#     # to work from outside ckan.
#     resource = model.Resource.get(resource_id)
#     resource.extras['datastore_active'] = True
#     model.Session.commit()
#     model.Session.remove()

def set_datastore_active_flag(model, data_dict, flag):
    '''
    Set appropriate datastore_active flag on CKAN resource.

    Called after creation or deletion of DataStore table.
    '''
    # We're modifying the resource extra directly here to avoid a
    # race condition, see issue #3245 for details and plan for a
    # better fix
    update_dict = {'datastore_active': flag}

    # get extras(for entity update) and package_id(for search index update)
    res_query = model.Session.query(
        model.resource_table.c.extras,
        model.resource_table.c.package_id
    ).filter(
        model.Resource.id == data_dict['resource_id']
    )
    extras, package_id = res_query.one()

    # update extras in database for record and its revision
    extras.update(update_dict)
    res_query.update({'extras': extras}, synchronize_session=False)
    model.Session.query(model.resource_revision_table).filter(
        model.ResourceRevision.id == data_dict['resource_id'],
        model.ResourceRevision.current is True
    ).update({'extras': extras}, synchronize_session=False)

    model.Session.commit()

    # get package with updated resource from solr
    # find changed resource, patch it and reindex package
    psi = search.PackageSearchIndex()
    solr_query = search.PackageSearchQuery()
    q = {
        'q': 'id:"{0}"'.format(package_id),
        'fl': 'data_dict',
        'wt': 'json',
        'fq': 'site_id:"%s"' % config.get('ckan.site_id'),
        'rows': 1
    }
    for record in solr_query.run(q)['results']:
        solr_data_dict = json.loads(record['data_dict'])
        for resource in solr_data_dict['resources']:
            if resource['id'] == data_dict['resource_id']:
                resource.update(update_dict)
                psi.index_package(solr_data_dict)
                break


def validate_input(input):
    # Especially validate metadata which is provided by the user
    if 'metadata' not in input:
        raise JobError('Metadata missing')

    data = input['metadata']

    if 'resource_id' not in data:
        raise JobError('No id provided.')
    if 'ckan_url' not in data:
        raise JobError('No ckan_url provided.')
    if not input.get('api_key'):
        raise JobError('No CKAN API key provided')

def update_resource(resource, api_key, ckan_url):
    """
    Update the given CKAN resource to say that it has been stored in datastore
    ok.
    """

    resource['url_type'] = 'datapusher'

    url = get_url('resource_update', ckan_url)
    r = requests.post(
        url,
        verify=SSL_VERIFY,
        data=json.dumps(resource),
        headers={'Content-Type': 'application/json',
                 'Authorization': api_key}
    )

    check_response(r, url, 'CKAN')

def get_resource(resource_id, ckan_url, api_key):
    """
    Gets available information about the resource from CKAN

    Could simply use the ckan model (the http request is a hangover from
    datapusher).
    """
    url = get_url('resource_show', ckan_url)
    r = requests.post(url,
                      verify=SSL_VERIFY,
                      data=json.dumps({'id': resource_id}),
                      headers={'Content-Type': 'application/json',
                               'Authorization': api_key}
                      )
    check_response(r, url, 'CKAN')

    return r.json()['result']

def get_url(action, ckan_url):
    """
    Get url for ckan action
    """
    if not urlparse.urlsplit(ckan_url).scheme:
        ckan_url = 'http://' + ckan_url.lstrip('/')
    ckan_url = ckan_url.rstrip('/')
    return '{ckan_url}/api/3/action/{action}'.format(
        ckan_url=ckan_url, action=action)

def check_response(response, request_url, who, good_status=(201, 200),
                   ignore_no_success=False):
    """
    Checks the response and raises exceptions if something went terribly wrong

    :param who: A short name that indicated where the error occurred
                (for example "CKAN")
    :param good_status: Status codes that should not raise an exception

    """
    if not response.status_code:
        raise HTTPError(
            'DataPusher received an HTTP response with no status code',
            status_code=None, request_url=request_url, response=response.text)

    message = '{who} bad response. Status code: {code} {reason}. At: {url}.'
    try:
        if response.status_code not in good_status:
            json_response = response.json()
            if not ignore_no_success or json_response.get('success'):
                try:
                    message = json_response["error"]["message"]
                except Exception:
                    message = message.format(
                        who=who, code=response.status_code,
                        reason=response.reason, url=request_url)
                raise HTTPError(
                    message, status_code=response.status_code,
                    request_url=request_url, response=response.text)
    except ValueError:
        message = message.format(
            who=who, code=response.status_code, reason=response.reason,
            url=request_url, resp=response.text[:200])
        raise HTTPError(
            message, status_code=response.status_code, request_url=request_url,
            response=response.text)

class StoringHandler(logging.Handler):
    '''A handler that stores the logging records in a database.'''
    def __init__(self, task_id, input):
        logging.Handler.__init__(self)
        self.task_id = task_id
        self.input = input

    def emit(self, record):
        conn = db.ENGINE.connect()
        try:
            # Turn strings into unicode to stop SQLAlchemy
            # "Unicode type received non-unicode bind param value" warnings.
            message = unicode(record.getMessage())
            level = unicode(record.levelname)
            module = unicode(record.module)
            funcName = unicode(record.funcName)

            conn.execute(db.LOGS_TABLE.insert().values(
                job_id=self.task_id,
                timestamp=datetime.datetime.now(),
                message=message,
                level=level,
                module=module,
                funcName=funcName,
                lineno=record.lineno))
        finally:
            conn.close()


class JobError(Exception):
    pass


class HTTPError(JobError):
    """Exception that's raised if a job fails due to an HTTP problem."""

    def __init__(self, message, status_code, request_url, response):
        """Initialise a new HTTPError.

        :param message: A human-readable error message
        :type message: string

        :param status_code: The status code of the errored HTTP response,
            e.g. 500
        :type status_code: int

        :param request_url: The URL that was requested
        :type request_url: string

        :param response: The body of the errored HTTP response as unicode
            (if you have a requests.Response object then response.text will
            give you this)
        :type response: unicode

        """
        super(HTTPError, self).__init__(message)
        self.status_code = status_code
        self.request_url = request_url
        self.response = response

    def __str__(self):
        return u'{} status={} url={} response={}'.format(
            self.message, self.status_code, self.request_url, self.response) \
            .encode('ascii', 'replace')

class DatetimeJsonEncoder(json.JSONEncoder):
    # Custon JSON encoder
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()

        return json.JSONEncoder.default(self, obj)
