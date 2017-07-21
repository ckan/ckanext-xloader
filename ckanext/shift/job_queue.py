'''Encapsulates the queue.

In DataPusher this was done by ckan-service-provider
CKAN 2.7 has a much fuller version of this in ckan.lib.jobs, but we do it here
to be compatible with earlier versions of CKAN.
'''
from rq import Queue
from redis import Redis

from pylons import config

_queue = None

def get_queue():
    global _queue
    if not _queue:
        redis_conn = Redis()
        _queue = Queue(_get_queue_name(), connection=redis_conn)
    return _queue

def get_queued_jobs():
    return get_queue().jobs

# bits taken from ckan.lib.jobs

DEFAULT_QUEUE_NAME = u'default'

def _get_queue_name(name=DEFAULT_QUEUE_NAME):
    return _get_queue_name_prefix() + name

def _get_queue_name_prefix():
    u'''
    Get the queue name prefix.
    '''
    # This must be done at runtime since we need a loaded config
    return u'ckan:{}:'.format(config[u'ckan.site_id'])
