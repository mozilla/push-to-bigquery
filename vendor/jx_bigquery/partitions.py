from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning

from mo_dots import Null
from mo_future import first
from mo_kwargs import override
from mo_logs import Log
from mo_times import Duration, DAY, Date, YEAR

NEVER = 10 * YEAR


class Partition(object):
    """
    DESCRIBE HOW TO PARTITION TABLE
    """

    __slots__ = ["field", "interval", "expire"]

    @override
    def __new__(cls, field=None, interval=DAY, expire=NEVER, kwargs=None):
        if field == None:
            return Null
        return object.__new__(cls)

    @override
    def __init__(self, field, interval=DAY, expire=NEVER, schema=Null, kwargs=None):
        column = first(schema.leaves(field))
        if not column:
            Log.error("expecting {{field}} in schema for partitioning", field=field)
        self.field = column.es_column
        self.interval = Duration(interval)
        self.expire = Duration(expire)
        if not isinstance(self.interval, Duration) or not isinstance(
            self.expire, Duration
        ):
            Log.error("expecting durations")

    def apply(self, row):
        """
        ENSURE PARTITION FIELD IS STEP TO TIMESTAMP
        :param row:
        :return:
        """
        row[self.field] = Date(row[self.field]).format()

    @property
    def bq_time_partitioning(self):
        return TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=self.field,
            expiration_ms=int(self.expire.milli),
        )
