from __future__ import division
from __future__ import absolute_import
import math
import logging
import hashlib
import time
import tempfile
import json
import datetime
import os
import traceback
import sys

from psycopg2 import errors
from six.moves.urllib.parse import urlsplit
import requests
from rq import get_current_job
from rq.timeouts import JobTimeoutException
import sqlalchemy as sa

from ckan.lib.jobs import DEFAULT_QUEUE_NAME
from ckan.plugins.toolkit import get_action, asbool, enqueue_job, ObjectNotFound, config, h

from . import db, loader
from .job_exceptions import JobError, HTTPError, DataTooBigError, FileCouldNotBeLoadedError, LoaderError, XLoaderTimeoutError
from .utils import cleanup_temp_file, datastore_resource_exists, set_resource_metadata, modify_input_url


from ckan.lib.api_token import get_user_from_token

log = logging.getLogger(__name__)

CHUNK_SIZE = 16 * 1024  # 16kb
DOWNLOAD_TIMEOUT = 30

RETRYABLE_ERRORS = (
    errors.DeadlockDetected,
    errors.LockNotAvailable,
    errors.ObjectInUse,
    HTTPError,
    XLoaderTimeoutError
)

# Variables that are set from config values and must be available to all jobs
ssl_verify = None
max_content_length = None
max_type_guessing_length = None
max_excerpt_lines = None
max_retries = None
retried_job_timeout = None
apitoken_header_name = None
default_queue_names = None


def is_retryable_error(error):
    """
    Determine if an error should trigger a retry attempt.

    Checks if the error is a temporary/transient condition that might
    succeed on retry. Returns True for retryable HTTP status codes and
    other temporary errors.

    Retryable HTTP status codes:
    - 408 Request Timeout
    - 429 Too Many Requests
    - 502 Bad Gateway
    - 503 Service Unavailable
    - 504 Gateway Timeout
    - 507 Insufficient Storage
    - 522 Connection Timed Out (Cloudflare)
    - 524 A Timeout Occurred (Cloudflare)

    :param error: Exception object to check
    :type error: Exception
    :return: True if error should be retried, False otherwise
    :rtype: bool
    """
    if isinstance(error, HTTPError):
        retryable_status_codes = {408, 429, 502, 503, 504, 507, 522, 524}
        return error.status_code in retryable_status_codes
    else:
        return True
    return False


# input = {
# 'api_key': user['apikey'],
# 'job_type': 'xloader_to_datastore',
# 'result_url': callback_url,
# 'metadata': {
#     'ignore_hash': data_dict.get('ignore_hash', False),
#     'ckan_url': site_url,
#     'resource_id': res_id,
#     'set_url_type': data_dict.get('set_url_type', False),
#     'task_created': task['last_updated'],
#     'original_url': resource_dict.get('url'),
#     }
# }


def get_default_queue_name(package_id=None):
    """ Retrieve the queue to be used in submitting jobs for the specified dataset.

    By sending all jobs for a dataset to the same queue, lock conflicts are reduced.
    """
    if not default_queue_names:
        return DEFAULT_QUEUE_NAME
    if not package_id:
        return default_queue_names[0]

    # Pick a queue by taking the first character of the package name
    # and converting it into a numeric index to the list of queue names.
    # We don't want a proper hash function, because those tend to add
    # complications for the sake of (unnecessary) cryptographic strength.
    queue_index = ord(package_id[0]) % len(default_queue_names)
    return default_queue_names[queue_index]


def xloader_data_into_datastore(input):
    '''This is the func that is queued. It is a wrapper for
    xloader_data_into_datastore_, and makes sure it finishes by calling
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

    db.init(config)
    try:
        # Store details of the job in the db
        db.add_pending_job(job_id, **input)
        xloader_data_into_datastore_(input, job_dict, logger)
        job_dict['status'] = 'complete'
        db.mark_job_as_completed(job_id, job_dict)
    except sa.exc.IntegrityError as e:
        db.mark_job_as_errored(job_id, str(e))
        job_dict['status'] = 'error'
        job_dict['error'] = str(e)
        log.error('xloader error: job_id %s already exists', job_id)
        errored = True
    except Exception as e:
        error_state = {'errored': errored}
        retry = handle_retryable_error(e, input, job_id, job_dict, logger, error_state)
        if retry:
            return None
        errored = error_state['errored']
    finally:
        # job_dict is defined in xloader_hook's docstring
        is_saved_ok = callback_xloader_hook(result_url=input['result_url'],
                                            api_key=input['api_key'],
                                            job_dict=job_dict)
        errored = errored or not is_saved_ok
    return 'error' if errored else None


def handle_retryable_error(e, input, job_id, job_dict, logger, error_state):
    """
    Handle retryable errors by attempting to retry the job or marking it as failed.

    Checks if the error is retryable (database deadlocks, HTTP timeouts, etc.) and
    within the retry limit. If so, enqueues a new job attempt. Otherwise, marks
    the job as errored.

    :param e: The exception that occurred
    :type e: Exception
    :param input: Job input data containing metadata and API key
    :type input: dict
    :param job_id: Unique identifier for the current job
    :type job_id: str
    :param job_dict: Job status dictionary with metadata and status
    :type job_dict: dict
    :param logger: Logger instance for the current job
    :type logger: logging.Logger
    :param error_state: Mutable dict to track error state {'errored': bool}
    :type error_state: dict

    :returns: True if job was retried, None otherwise
    :rtype: bool or None
    """
    if isinstance(e, RETRYABLE_ERRORS) and is_retryable_error(e):
        tries = job_dict['metadata'].get('tries', 0)
        if tries < max_retries:
            tries = tries + 1
            log.info("Job %s failed due to temporary error [%s], retrying", job_id, e)
            logger.info("Job failed due to temporary error [%s], retrying", e)
            job_dict['status'] = 'pending'
            job_dict['metadata']['tries'] = tries
            enqueue_job(
                xloader_data_into_datastore,
                [input],
                title="retry xloader_data_into_datastore: resource: {} attempt {}".format(
                    job_dict['metadata']['resource_id'], tries),
                rq_kwargs=dict(timeout=retried_job_timeout)
            )
            return True
    db.mark_job_as_errored(
        job_id, traceback.format_tb(sys.exc_info()[2])[-1] + repr(e))
    job_dict['status'] = 'error'
    job_dict['error'] = str(e)
    log.error('xloader error: %s, %s', e, traceback.format_exc())
    error_state['errored'] = True


def xloader_data_into_datastore_(input, job_dict, logger):
    '''This function:
    * downloads the resource (metadata) from CKAN
    * downloads the data
    * calls the loader to load the data into DataStore
    * calls back to CKAN with the new status

    (datapusher called this function 'push_to_datastore')
    '''
    validate_input(input)

    data = input['metadata']

    resource_id = data['resource_id']
    api_key = input.get('api_key')
    try:
        resource, dataset = get_resource_and_dataset(resource_id, api_key)
    except (JobError, ObjectNotFound):
        # try again in 5 seconds just in case CKAN is slow at adding resource
        time.sleep(5)
        resource, dataset = get_resource_and_dataset(resource_id, api_key)
    resource_ckan_url = '/dataset/{}/resource/{}' \
        .format(dataset['name'], resource['id'])
    logger.info('Express Load starting: %s', resource_ckan_url)

    # check if the resource url_type is a datastore
    if hasattr(h, "datastore_rw_resource_url_types"):
        datastore_rw_resource_url_types = h.datastore_rw_resource_url_types()
    else:
        # fallback for 2.10.x or older.
        datastore_rw_resource_url_types = ['datastore']

    if resource.get('url_type') in datastore_rw_resource_url_types:
        logger.info('Ignoring resource - R/W DataStore resources are '
                    'managed with the Datastore API')
        return

    # download resource
    tmp_file, file_hash = _download_resource_data(resource, data, api_key,
                                                  logger)

    try:
        if (resource.get('hash') == file_hash
                and not data.get('ignore_hash')):
            logger.info('Ignoring resource - the file hash hasn\'t changed: '
                        '{hash}.'.format(hash=file_hash))
            return
        logger.info('File hash: %s', file_hash)
        resource['hash'] = file_hash

        def direct_load(allow_type_guessing=False):
            fields = loader.load_csv(
                tmp_file.name,
                resource_id=resource['id'],
                mimetype=resource.get('format'),
                allow_type_guessing=allow_type_guessing,
                logger=logger)
            loader.calculate_record_count(
                resource_id=resource['id'], logger=logger)
            set_datastore_active(data, resource, logger)
            if 'result_url' in input:
                job_dict['status'] = 'running_but_viewable'
                callback_xloader_hook(result_url=input['result_url'],
                                      api_key=api_key,
                                      job_dict=job_dict)
            logger.info('Data now available to users: %s', resource_ckan_url)
            loader.create_column_indexes(
                fields=fields,
                resource_id=resource['id'],
                logger=logger)
            update_resource(resource={'id': resource['id'], 'hash': resource['hash']},
                            patch_only=True)
            logger.info('File Hash updated for resource: %s', resource['hash'])

        def tabulator_load():
            try:
                loader.load_table(tmp_file.name,
                                  resource_id=resource['id'],
                                  mimetype=resource.get('format'),
                                  logger=logger)
            except JobError as e:
                logger.error('Error during tabulator load: %s', e)
                raise
            loader.calculate_record_count(
                resource_id=resource['id'], logger=logger)
            set_datastore_active(data, resource, logger)
            logger.info('Finished loading with tabulator')
            update_resource(resource={'id': resource['id'], 'hash': resource['hash']},
                            patch_only=True)
            logger.info('File Hash updated for resource: %s', resource['hash'])

        # Load it
        logger.info('Loading CSV')
        # If ckanext.xloader.use_type_guessing is not configured, fall back to
        # deprecated ckanext.xloader.just_load_with_messytables
        use_type_guessing = asbool(
            config.get('ckanext.xloader.use_type_guessing', config.get(
                'ckanext.xloader.just_load_with_messytables', False))) \
            and not datastore_resource_exists(resource['id']) \
            and os.path.getsize(tmp_file.name) <= max_type_guessing_length
        logger.info("'use_type_guessing' mode is: %s", use_type_guessing)
        try:
            if use_type_guessing:
                try:
                    tabulator_load()
                except JobError as e:
                    logger.warning('Load using tabulator failed: %s', e)
                    logger.info('Trying again with direct COPY')
                    direct_load()
            else:
                try:
                    direct_load(allow_type_guessing=True)
                except (JobError, LoaderError) as e:
                    logger.warning('Load using COPY failed: %s', e)
                    logger.info('Trying again with tabulator')
                    tabulator_load()
        except JobTimeoutException:
            logger.warning('Job timed out after %ss', retried_job_timeout)
            raise JobError('Job timed out after {}s'.format(retried_job_timeout))
        except FileCouldNotBeLoadedError as e:
            logger.warning('Loading excerpt for this format not supported.')
            logger.error('Loading file raised an error: %s', e)
            raise JobError('Loading file raised an error: {}'.format(e))
    finally:
        cleanup_temp_file(tmp_file)

    logger.info('Express Load completed')


def _download_resource_data(resource, data, api_key, logger):
    '''Downloads the resource['url'] as a tempfile.

    :param resource: resource (i.e. metadata) dict (from the job dict)
    :param data: job dict - may be written to during this function
    :param api_key: CKAN api key - needed to obtain resources that are private
    :param logger:

    If the download is bigger than max_content_length then it just downloads a
    excerpt (of max_excerpt_lines) for preview, and flags it by setting
    data['datastore_contains_all_records_of_source_file'] = False
    which will be saved to the resource later on.
    '''
    # update base url (for possible local loopback)
    url = modify_input_url(resource.get('url'))
    # check scheme
    url_parts = urlsplit(url)
    scheme = url_parts.scheme
    if scheme not in ('http', 'https', 'ftp'):
        raise JobError(
            'Only http, https, and ftp resources may be fetched.'
        )

    # fetch the resource data
    logger.info('Fetching from: {0}'.format(url))
    tmp_file = get_tmp_file(url)
    length = 0
    m = hashlib.md5(usedforsecurity=False)
    cl = None
    try:
        headers = {}
        if resource.get('url_type') == 'upload':
            # If this is an uploaded file to CKAN, authenticate the request,
            # otherwise we won't get file from private resources
            headers[apitoken_header_name] = api_key

            # Add a constantly changing parameter to bypass URL caching.
            # If we're running XLoader, then either the resource has
            # changed, or something went wrong and we want a clean start.
            # Either way, we don't want a cached file.
            download_url = url_parts._replace(
                query='{}&nonce={}'.format(url_parts.query, time.time())
            ).geturl()
        else:
            download_url = url

        response = get_response(download_url, headers)

        cl = response.headers.get('content-length')
        if cl and int(cl) > max_content_length:
            response.close()
            raise DataTooBigError()

        # download the file to a tempfile on disk
        for chunk in response.iter_content(CHUNK_SIZE):
            length += len(chunk)
            if length > max_content_length:
                raise DataTooBigError
            tmp_file.write(chunk)
            m.update(chunk)
        response.close()
        data['datastore_contains_all_records_of_source_file'] = True

    except DataTooBigError:
        cleanup_temp_file(tmp_file)
        message = 'Data too large to load into Datastore: ' \
            '{cl} bytes > max {max_cl} bytes.' \
            .format(cl=cl or length, max_cl=max_content_length)
        logger.warning(message)
        if max_excerpt_lines <= 0:
            raise JobError(message)
        logger.info('Loading excerpt of ~{max_lines} lines to '
                    'DataStore.'
                    .format(max_lines=max_excerpt_lines))
        tmp_file = get_tmp_file(url)
        response = get_response(url, headers)
        length = 0
        line_count = 0
        m = hashlib.md5()
        for line in response.iter_lines(CHUNK_SIZE):
            tmp_file.write(line + b'\n')
            m.update(line)
            length += len(line)
            line_count += 1
            if length > max_content_length or line_count >= max_excerpt_lines:
                break
        response.close()
        data['datastore_contains_all_records_of_source_file'] = False
    except requests.exceptions.HTTPError as error:
        cleanup_temp_file(tmp_file)
        # status code error
        logger.debug('HTTP error: %s', error)
        raise HTTPError(
            "Xloader received a bad HTTP response when trying to download "
            "the data file", status_code=error.response.status_code,
            request_url=url, response=error)
    except requests.exceptions.Timeout:
        logger.warning('URL time out after %ss', DOWNLOAD_TIMEOUT)
        raise XLoaderTimeoutError('Connection timed out after {}s'.format(
                                  DOWNLOAD_TIMEOUT))
    except requests.exceptions.RequestException as e:
        cleanup_temp_file(tmp_file)
        try:
            err_message = str(e.reason)
        except AttributeError:
            err_message = str(e)
        logger.warning('URL error: %s', err_message)
        raise HTTPError(
            message=err_message, status_code=None,
            request_url=url, response=None)
    except JobTimeoutException:
        cleanup_temp_file(tmp_file)
        logger.warning('Job timed out after %ss', retried_job_timeout)
        raise JobError('Job timed out after {}s'.format(retried_job_timeout))

    logger.info('Downloaded ok - %s', printable_file_size(length))
    file_hash = m.hexdigest()
    tmp_file.seek(0)
    return tmp_file, file_hash


def get_response(url, headers):
    def get_url():
        kwargs = {'headers': headers, 'timeout': DOWNLOAD_TIMEOUT,
                  'verify': ssl_verify, 'stream': True}  # just gets the headers for now
        if 'ckan.download_proxy' in config:
            proxy = config.get('ckan.download_proxy')
            kwargs['proxies'] = {'http': proxy, 'https': proxy}
        return requests.get(url, **kwargs)
    response = get_url()
    if response.status_code == 202:
        # Seen: https://data-cdfw.opendata.arcgis.com/datasets
        # In this case it means it's still processing, so do retries.
        # 202 can mean other things, but there's no harm in retries.
        wait = 1
        while wait < 120 and response.status_code == 202:
            # logger.info('Retrying after %ss', wait)
            time.sleep(wait)
            response = get_url()
            wait *= 3
    response.raise_for_status()
    return response


def get_tmp_file(url):
    filename = url.split('/')[-1].split('#')[0].split('?')[0]
    tmp_file = tempfile.NamedTemporaryFile(suffix=filename)
    return tmp_file


def set_datastore_active(data, resource, logger):
    if data.get('set_url_type', False):
        logger.debug('Setting resource.url_type = \'datapusher\'')
        resource['url_type'] = 'datapusher'
        update_resource(resource)

    data['datastore_active'] = True
    logger.info('Setting resource.datastore_active = True')
    contains_all_records = data.get(
        'datastore_contains_all_records_of_source_file', True)
    data['datastore_contains_all_records_of_source_file'] = contains_all_records
    logger.info(
        'Setting resource.datastore_contains_all_records_of_source_file = %s',
        contains_all_records)
    set_resource_metadata(update_dict=data)


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
            header, key = apitoken_header_name, api_key
        headers[header] = key

    try:
        result = requests.post(
            modify_input_url(result_url),  # modify with local config
            data=json.dumps(job_dict, cls=DatetimeJsonEncoder),
            verify=ssl_verify,
            headers=headers)
    except requests.ConnectionError:
        return False

    return result.status_code == requests.codes.ok


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


def update_resource(resource, patch_only=False):
    """
    Update the given CKAN resource to say that it has been stored in datastore
    ok.
    or patch the given CKAN resource for file hash
    """
    action = 'resource_update' if not patch_only else 'resource_patch'
    user = get_action('get_site_user')({'ignore_auth': True}, {})
    context = {
        'ignore_auth': True,
        'user': user['name'],
        'auth_user_obj': None
    }
    get_action(action)(context, resource)


def _get_user_from_key(api_key_or_token):
    """ Gets the user using the API Token or API Key.
    """
    return get_user_from_token(api_key_or_token)


def get_resource_and_dataset(resource_id, api_key):
    """
    Gets available information about the resource and its dataset from CKAN
    """
    context = None
    user = _get_user_from_key(api_key)
    if user is not None:
        context = {'user': user.name}

    res_dict = get_action('resource_show')(context, {'id': resource_id})
    pkg_dict = get_action('package_show')(context, {'id': res_dict['package_id']})
    return res_dict, pkg_dict


def get_url(action, ckan_url):
    """
    Get url for ckan action
    """
    if not urlsplit(ckan_url).scheme:
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
            'Xloader received an HTTP response with no status code',
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
        with db.ENGINE.connect() as conn:
            # Turn strings into unicode to stop SQLAlchemy
            # "Unicode type received non-unicode bind param value" warnings.
            message = str(record.getMessage())
            level = str(record.levelname)
            module = str(record.module)
            funcName = str(record.funcName)

            conn.execute(db.LOGS_TABLE.insert().values(
                job_id=self.task_id,
                timestamp=datetime.datetime.utcnow(),
                message=message,
                level=level,
                module=module,
                funcName=funcName,
                lineno=record.lineno))


class DatetimeJsonEncoder(json.JSONEncoder):
    # Custom JSON encoder
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()

        return json.JSONEncoder.default(self, obj)


def printable_file_size(size_bytes):
    if size_bytes == 0:
        return '0 bytes'
    size_name = ('bytes', 'KB', 'MB', 'GB', 'TB')
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(float(size_bytes) / p, 1)
    return "%s %s" % (s, size_name[i])
