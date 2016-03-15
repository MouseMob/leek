Leek is a Redis-backed distributed scheduler for celery. Because it stores task 
execution data in Redis, it can safely be run on multiple nodes without 
duplicating work. Unlike other distributed schedulers which use locking to 
prevent more than a single instance from running simultaneously, all leek nodes 
are running all the time; they compete to generate the next invocation of a 
scheduled task, and only one node will successfully do so. This means automatic 
failover in the event of a node outage.

To install:

    python setup.py install

To use:

    celery beat -S leek.LeekScheduler
