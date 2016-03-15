import sys
import calendar
from datetime import datetime, timedelta
import traceback
import redis
from celery import current_app
from celery.five import reraise, monotonic
from celery.utils.timeutils import maybe_make_aware, timezone
from celery.utils.log import LOG_LEVELS, get_logger
from celery.beat import ScheduleEntry, Scheduler, Service, SchedulingError


__version__ = '0.1.0'


logger = get_logger('leek')


def timestamp_from_dt(dt):
    """Convert a datetime to seconds since epoch."""
    return calendar.timegm(maybe_make_aware(dt).utctimetuple()) if dt else 0


class LeekScheduler(Scheduler):
    Entry = ScheduleEntry

    def __init__(self, *args, **kwargs):
        self.data = {}
        self.last_refresh = None

        if 'LEEK_REDIS_URL' not in current_app.conf:
            raise Exception('Missing LEEK_REDIS_URL celery config')
        self.hash_key = current_app.conf.get('LEEK_HASH_KEY', 'leek')
        self.redis = redis.from_url(current_app.conf.get('LEEK_REDIS_URL'))
        self.tz = timezone.get_timezone(current_app.conf.get('CELERY_TIMEZONE', 'UTC'))

        Scheduler.__init__(self, *args, **kwargs)

        self.max_interval = self.app.conf.get('LEEK_REFRESH_INTERVAL', 5)
        self.refresh_frequency = timedelta(seconds=self.max_interval)

    def setup_schedule(self):
        self.install_default_entries(self.schedule)
        self.update_from_dict(self.app.conf.CELERYBEAT_SCHEDULE)
        self.refresh()

    def reserve(self, entry):
        lock_key = '{}.{}'.format(self.hash_key, entry.name)

        pipe = self.redis.pipeline()
        try:
            # watch the lock key for changes
            pipe.watch(lock_key)

            # get its current value
            old_entry = self.get_entry(entry.name, r=pipe)
            old_timestamp = timestamp_from_dt(old_entry.last_run_at)
            due, next_time = old_entry.is_due()
            if not due:
                logger.info("not due; not scheduling task")
                return None

            pipe.setnx(lock_key, old_timestamp)
            value = pipe.get(lock_key)

            try:
                value = int(value)
            except (TypeError, ValueError):
                value = None

            new_entry = next(old_entry)
            new_timestamp = timestamp_from_dt(new_entry.last_run_at)
            new_entry.last_run_at = datetime.fromtimestamp(new_timestamp, self.tz)

            logger.debug(value)
            logger.debug(old_timestamp)
            logger.debug(new_timestamp)

            if value == old_timestamp:
                # we're in sync, try to update the lock key
                pipe.multi()
                pipe.set(lock_key, new_timestamp)
                pipe.hset(self.hash_key, entry.name, new_timestamp)
                pipe.execute()
            else:
                # we're out of sync
                logger.info("too late; not scheduling task")
                pipe.reset()
                return None
        except redis.exceptions.WatchError:
            logger.info("locked out; not scheduling task")
            pipe.reset()
            return None

        logger.info("won; will schedule task")
        self.data[entry.name] = new_entry
        return old_entry

    def maybe_due(self, entry, publisher=None):
        is_due, next_time_to_run = entry.is_due()

        if is_due:
            logger.info('Scheduler: Sending due task %s (%s)', entry.name, entry.task)
            try:
                result = self.apply_async(entry, publisher=publisher)
            except Exception as exc:
                logger.error('Message Error: %s\n%s',
                      exc, traceback.format_stack(), exc_info=True)
            else:
                if result:
                    logger.debug('%s sent. id->%s', entry.task, result.id)
        return next_time_to_run

    def apply_async(self, entry, producer=None, advance=True, **kwargs):
        if advance:
            # attempt to reserve a schedule entry for this leek instance;
            # if reserve returns None, do nothing
            entry = self.reserve(entry) if advance else entry

        if entry:
            task = self.app.tasks.get(entry.task)

            try:
                if task:
                    return task.apply_async(entry.args, entry.kwargs,
                                            producer=producer,
                                            **entry.options)
                else:
                    return self.send_task(entry.task, entry.args, entry.kwargs,
                                          producer=producer,
                                          **entry.options)
            except Exception as exc:
                reraise(SchedulingError, SchedulingError(
                    "Couldn't apply scheduled task {0.name}: {exc}".format(
                        entry, exc=exc)), sys.exc_info()[2])

    def sync(self):
        pass

    def refresh(self):
        pipe = self.redis.pipeline()
        pipe.watch(self.hash_key)

        data = self.redis.hgetall(self.hash_key)
        if data:
            for key, value in data.items():
                if key in self.data:
                    try:
                        self.data.get(key).last_run_at = datetime.fromtimestamp(int(value), self.tz)
                    except (TypeError, ValueError):
                        pass
        elif self.data:
            # no hash exists; try to create it
            try:
                pipe.multi()
                pipe.hmset(self.hash_key, {key: timestamp_from_dt(value.last_run_at) for key, value in self.data.items()})
                for key, value in self.data.items():
                    pipe.set('{}.{}'.format(self.hash_key, key), timestamp_from_dt(value.last_run_at))
                pipe.execute()
            except redis.exceptions.WatchError:
                pass

        self.last_refresh = self.app.now()
        pipe.reset()

    def get_entry(self, name, r=None):
        r = r or self.redis
        entry = self.data.get(name)
        value = r.hget(self.hash_key, name)
        if value:
            try:
                tz = self.app.conf.CELERY_TIMEZONE
                entry.last_run_at = datetime.fromtimestamp(int(value), self.tz)
            except (TypeError, ValueError):
                pass
        return entry

    def update_from_dict(self, dict_):
        self.data.update({name: self._maybe_entry(name, entry) for name, entry in dict_.items()})

    @property
    def schedule(self):
        if self.last_refresh is None or self.app.now() - self.last_refresh > self.refresh_frequency:
            self.refresh()
        return self.data
