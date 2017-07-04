'''Encapsulates the queue.

(In DataPusher this was done by ckan-service-provider)
'''
from rq import Queue
from redis import Redis

_queue = None

def get_queue():
    global _queue
    if not _queue:
        redis_conn = Redis()
        _queue = Queue('ckanext-shift', connection=redis_conn)
    return _queue

def get_queued_jobs():
    return get_queue().jobs
