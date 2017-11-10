import logging
import hashlib
import time
import tempfile
import requests
import json
import datetime
import urlparse
from rq import get_current_job
import traceback
import sys

try:
    from ckan.plugins.toolkit import config
except ImportError:
    from pylons import config
import ckan.lib.search as search
import sqlalchemy as sa

import loader
import db
from job_exceptions import JobError, HTTPError

if config.get('ckanext.xloader.ssl_verify') in ['False', 'FALSE', '0', False, 0]:
    SSL_VERIFY = False
else:
    SSL_VERIFY = True
if not SSL_VERIFY:
    requests.packages.urllib3.disable_warnings()

MAX_CONTENT_LENGTH = config.get('ckanext.xloader.max_content_length') or 1e9
CHUNK_SIZE = 16 * 1024  # 16kb
DOWNLOAD_TIMEOUT = 30


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

def xloader_data_into_datastore(input):
    '''This is the func that is queued. It is a wrapper for
    xloader_data_into_datastore, and makes sure it finishes by calling
    xloader_hook to update the task_status with the result.

    Errors are stored in task_status and job log and this method returns
    'error' to let RQ know too. Should task_status fails, then we also return
    'error'.
    '''
    # First flag that this task is running, to indicate the job is not
    # stillborn, for when xloader_submit is deciding whether another job would
    # be a duplicate or not
    job_dict = dict(metadata=input['metadata'],
                    status='running')
    callback_xloader_hook(result_url=input['result_url'],
                          api_key=input['api_key'],
                          job_dict=job_dict)

    job_id = get_current_job().id
    errored = False
    try:
        xloader_data_into_datastore_(input)
        job_dict['status'] = 'complete'
        db.mark_job_as_completed(job_id, job_dict)
    except JobError as e:
        db.mark_job_as_errored(job_id, str(e))
        job_dict['status'] = 'error'
        job_dict['error'] = str(e)
        log = logging.getLogger(__name__)
        log.error('xloader error: {}'.format(e))
        errored = True
    except Exception as e:
        db.mark_job_as_errored(
            job_id, traceback.format_tb(sys.exc_traceback)[-1] + repr(e))
        job_dict['status'] = 'error'
        job_dict['error'] = str(e)
        log = logging.getLogger(__name__)
        log.error('xloader error: {}'.format(e))
        errored = True
    finally:
        # job_dict is defined in xloader_hook's docstring
        is_saved_ok = callback_xloader_hook(result_url=input['result_url'],
                                            api_key=input['api_key'],
                                            job_dict=job_dict)
        errored = errored or not is_saved_ok
    return 'error' if errored else None


def xloader_data_into_datastore_(input):
    '''This function:
    * downloads the resource (metadata) from CKAN
    * downloads the data
    * calls the loader to load the data into DataStore
    * calls back to CKAN with the new status

    (ckanext-xloader called this function 'xloader_to_datastore')
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
    logger.addHandler(handler)
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

    # check scheme
    url = resource.get('url')
    scheme = urlparse.urlsplit(url).scheme
    if scheme not in ('http', 'https', 'ftp'):
        raise JobError(
            'Only http, https, and ftp resources may be fetched.'
        )

    # fetch the resource data
    logger.info('Fetching from: {0}'.format(url))
    try:
        headers = {}
        if resource.get('url_type') == 'upload':
            # If this is an uploaded file to CKAN, authenticate the request,
            # otherwise we won't get file from private resources
            headers['Authorization'] = api_key

        response = requests.get(
            url,
            headers=headers,
            timeout=DOWNLOAD_TIMEOUT,
            verify=SSL_VERIFY,
            stream=True,  # just gets the headers for now
            )
        response.raise_for_status()

        cl = response.headers.get('content-length')
        if cl and int(cl) > MAX_CONTENT_LENGTH:
            error_msg = 'Resource too large to download: ' \
                '{cl} > max ({max_cl}).' \
                .format(cl=cl, max_cl=MAX_CONTENT_LENGTH)
            logger.error(error_msg)
            raise JobError(error_msg)

        # download the file to a tempfile on disk
        filename = url.split('/')[-1].split('#')[0].split('?')[0]
        tmp_file = tempfile.NamedTemporaryFile(suffix=filename)
        length = 0
        m = hashlib.md5()
        for chunk in response.iter_content(CHUNK_SIZE):
            length += len(chunk)
            if length > MAX_CONTENT_LENGTH:
                raise JobError(
                    'Resource too large to process: {cl} > max ({max_cl}).'
                    .format(cl=length, max_cl=MAX_CONTENT_LENGTH))
            tmp_file.write(chunk)
            m.update(chunk)

    except requests.exceptions.HTTPError as error:
        # status code error
        logger.error('HTTP error: {}'.format(error))
        raise HTTPError(
            "DataPusher received a bad HTTP response when trying to download "
            "the data file", status_code=error.response.status_code,
            request_url=url, response=error)
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
            request_url=url, response=None)

    logger.info('Downloaded ok')
    file_hash = m.hexdigest()
    tmp_file.seek(0)

    if (resource.get('hash') == file_hash
            and not data.get('ignore_hash')):
        logger.info('Ignoring resource - the file hash hasn\'t changed: '
                    '{hash}.'.format(hash=file_hash))
        return
    logger.info('File hash: {}'.format(file_hash))
    resource['hash'] = file_hash

    # Load it
    logger.info('Loading CSV')
    try:
        fields = loader.load_csv(
            tmp_file.name,
            resource_id=resource['id'],
            mimetype=resource.get('format'),
            logger=logger)
        logger.info('Data now available to users.')
        logger.info('Creating column indexes for optimization...')
        set_datastore_active(data, resource, api_key, ckan_url, logger)
        loader.create_column_indexes(
            fields=fields,
            resource_id=resource['id'],
            logger=logger)
        logger.info('Finished creating the indexes.')
    except JobError as e:
        logger.error('Error during load: {}'.format(e))
        logger.info('Trying again with messytables')
        try:
            loader.load_table(tmp_file.name,
                              resource_id=resource['id'],
                              mimetype=resource.get('format'),
                              logger=logger)
        except JobError as e:
            logger.error('Error during messytables load: {}'.format(e))
            raise
        set_datastore_active(data, resource, api_key, ckan_url, logger)
        logger.info('Finished loading with messytables')

    tmp_file.close()

    logger.info('Express Load completed')


def set_datastore_active(data, resource, api_key, ckan_url, logger):
    # Set resource.url_type = 'datapusher'
    if data.get('set_url_type', False):
        logger.info('Setting resource.url_type = \'datapusher\'')
        update_resource(resource, api_key, ckan_url)

    # Set resource.datastore_active = True
    if resource.get('datastore_active') is not True:
        from ckan import model
        logger.info('Setting resource.datastore_active = True')
        set_datastore_active_flag(model=model, data_dict=data, flag=True)


def callback_xloader_hook(result_url, api_key, job_dict):
    '''Tells CKAN about the result of the xloader (i.e. calls the callback
    function 'xloader_hook'). Usually called by the xloader queue job.
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


class DatetimeJsonEncoder(json.JSONEncoder):
    # Custon JSON encoder
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()

        return json.JSONEncoder.default(self, obj)
