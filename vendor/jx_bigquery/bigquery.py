import re

from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning
from google.oauth2 import service_account

from jx_base import jx_expression, Container, Facts
from jx_bigquery.sql import quote_column, unescape_name, ALLOWED, sql_call, sql_alias, escape_name
from jx_bigquery.typed_encoder import (
    schema_to_bq_schema,
    bq_type_to_json_type,
    NESTED_TYPE,
    typed_encode,
    PARTITION_FIELD, BQ_PARITITION_FIELD, typed_to_bq_type, REPEATED)
from jx_python import jx
from mo_dots import concat_field, listwrap, unwrap, split_field, join_field, Null, unwraplist, is_data, \
    set_default
from mo_future import is_text, text
from mo_kwargs import override
from mo_logs import Log, Except
from mo_math.randoms import Random
from mo_threads import Till
from mo_times import Duration, MINUTE, YEAR, DAY
from mo_times.dates import Date
from pyLibrary.sql import ConcatSQL, SQL, SQL_SELECT, JoinSQL, SQL_NULL, SQL_FROM, SQL_COMMA, SQL_AS, SQL_ORDERBY, \
    SQL_CR, SQL_UNION_ALL, SQL_SELECT_AS_STRUCT, SQL_INSERT, SQL_STAR

EXTEND_LIMIT = 2 * MINUTE  # EMIT ERROR IF ADDING RECORDS TO TABLE TOO OFTEN
NEVER = 10 * YEAR

SUFFIX_PATTERN = re.compile(r"__\w{20}")


class Partition(object):
    """
    DESCRIBE HOW TO PARTITION TABLE
    """

    __slots__ = ["field", "interval", "expire"]

    @override
    def __init__(self, field, interval=DAY, expire=NEVER, kwargs=None):
        self.field = jx_expression(field)
        self.interval = Duration(interval)
        self.expire = Duration(expire)
        if not isinstance(self.interval, Duration) or not isinstance(
            self.expire, Duration
        ):
            Log.error("expecting durations")

    def add_partition(self, row):
        """
        MARKUP row WITH _partition VALUE
        """
        timestamp = self.field(row)
        row[PARTITION_FIELD] = Date(timestamp).format()

    @property
    def bq_time_partitioning(self):
        return TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=escape_name(PARTITION_FIELD),
            expiration_ms=int(self.expire.milli),
        )


class Dataset(Container):
    """
    REPRESENT A BIGQUERY DATASET; aka A CONTAINER FOR TABLES; aka A DATABASE
    """

    @override
    def __init__(self, dataset, account_info, kwargs):
        creds = service_account.Credentials.from_service_account_info(info=account_info)
        self.client = bigquery.Client(
            project=account_info.project_id, credentials=creds
        )
        self.short_name = dataset
        esc_name = escape_name(dataset)
        self.full_name = concat_field(account_info.project_id, esc_name)

        datasets = list(self.client.list_datasets())
        for _dataset in datasets:
            if _dataset.dataset_id == esc_name:
                self.dataset = _dataset.reference
                break
        else:
            _dataset = bigquery.Dataset(self.full_name)
            _dataset.location = "US"
            self.dataset = self.client.create_dataset(_dataset)

    @override
    def get_or_create_table(
        self,
        table,
        schema=None,
        typed=True,
        read_only=False,
        sharded=False,
        partition=None,
        cluster=None,  # TUPLE OF FIELDS TO SORT DATA
        kwargs=None,
    ):
        try:
            return Table(container=self, partition=Partition(partition), cluster=cluster, kwargs=kwargs)
        except Exception as e:
            e = Except.wrap(e)
            if "Not found: Table" in e:
                return self.create_table(kwargs)
            Log.error("could not get table {{table}}", table=table, cause=e)

    @override
    def create_or_replace_table(
        self,
        table,
        schema=None,
        typed=True,
        read_only=False,
        partition=None,
        cluster=None,  # TUPLE OF FIELDS TO SORT DATA
        sharded=False,
        kwargs=None,
    ):
        try:
            self.delete_table(table)
        except Exception as e:
            e = Except.wrap(e)
            if "Not found: Table" not in e:
                Log.error("could not get table {{table}}", table=table, cause=e)
        return self.create_table(kwargs)

    def delete_table(self, name):
        full_name = concat_field(self.full_name, escape_name(name))
        self.client.delete_table(full_name)

    @override
    def create_table(
        self,
        table,
        schema=None,
        typed=True,
        read_only=True,  # TO PREVENT ACCIDENTAL WRITING
        sharded=False,
        partition=None,  # PARTITION RULES
        cluster=None,  # TUPLE OF FIELDS TO SORT DATA
        kwargs=None,
    ):
        esc_name = escape_name(table)
        full_name = concat_field(self.full_name, esc_name)
        partition = Partition(partition) if partition else Null
        _table = bigquery.Table(
            full_name,
            schema=BQ_PARITITION_FIELD + schema_to_bq_schema(schema),
        )
        _table.time_partitioning = unwrap(partition.bq_time_partitioning)
        _table.clustering_fields = unwrap(unwraplist([join_field(escape_name(p) for p in split_field(f)) for f in listwrap(cluster)]))

        self.client.create_table(_table)
        Log.note("created table {{table}}", table=_table.table_id)
        job = self.client.query("SELECT count(1) FROM " + concat_field(_table.dataset_id, _table.table_id))
        result = job.result()

        return Table(
            table=table,
            typed=typed,
            read_only=read_only,
            sharded=sharded,
            partition=partition,
            kwargs=kwargs,
            container=self,
        )


class Table(Facts):
    @override
    def __init__(self, table, typed, read_only, sharded, partition, container):
        esc_name = escape_name(table)
        self.full_name = concat_field(container.full_name, esc_name)
        self.short_name = table
        self.typed = typed
        self.read_only = read_only

        primary_shard = container.client.get_table(self.full_name)
        if not sharded:
            self.shard = primary_shard
        else:
            self.shard = None
        self.partition = partition
        self._schema = parse_schema(primary_shard.schema)
        self.container = container
        self.last_extend = Date.now()-EXTEND_LIMIT

    @property
    def schema(self):
        return self._schema

    def _create_new_shard(self):
        primary_shard = self.container.create_table(
            self.short_name + "_" + "".join(Random.sample(ALLOWED, 20)),
            typed=self.typed,
            read_only=False,
            sharded=False,
            schema=self._schema,
            )
        self.shard = primary_shard.shard

    def extend(self, rows):
        if self.read_only:
            Log.error("not for writing")

        try:
            update = {}
            while True:
                output = []
                for rownum, row in enumerate(rows):
                    typed, more, add_nested = typed_encode(row, self.schema)
                    if add_nested:
                        # row HAS NEW NESTED COLUMN!
                        # GO OVER THE rows AGAIN SO "RECORD" GET MAPPED TO "REPEATED"
                        break
                    typed[escape_name(PARTITION_FIELD)] = Date(self.partition.field(row)).format()
                    update.update(more)
                    output.append(typed)
                else:
                    break

            if update or not self.shard:
                # BATCH HAS ADDITIONAL COLUMNS!!
                # WE CAN NOT USE THE EXISTING SHARD, MAKE A NEW ONE:
                self._create_new_shard()
                Log.note("added new shard with name: {{shard}}", shard=self.shard.table_id)

            failures = self.container.client.insert_rows(self.shard, output)
            if failures:
                Log.error("expecting no failures:\n{{failures}}", failure=failures)
            else:
                Log.note("{{num}} rows added", num=len(output))
        except Exception as e:
            Log.error("Do not know how to handle", cause=e)
        finally:
            self.last_extend = Date.now()

    def add(self, row):
        self.extend([row])

    def merge_shards(self):
        shards = []
        tables = list(self.container.client.list_tables(self.container.dataset))
        esc_name = escape_name(self.short_name)
        for table_item in tables:
            table = table_item.reference
            n = table.table_id
            if n.startswith(self.short_name):
                if n == esc_name:
                    shards.append(table)
                elif SUFFIX_PATTERN.match(n[len(esc_name):]):
                    shards.append(table)

        shard_schemas = [parse_schema(self.container.client.get_table(shard).schema) for shard in shards]
        total_schema = unwrap(set_default({}, *shard_schemas))

        new_table_name = "testing_" + "".join(Random.sample(ALLOWED, 20))
        destination = self.container.create_table(new_table_name, schema=total_schema)
        shard_schemas.insert(0, total_schema)
        shards.insert(0, destination.shard.reference)

        selects = []
        for schema, table in zip(shard_schemas, shards):
            q = ConcatSQL((
                SQL_SELECT,
                quote_column(PARTITION_FIELD),
                SQL_COMMA,
                JoinSQL(ConcatSQL((SQL_COMMA, SQL_CR)), gen_select([], total_schema, schema)),
                SQL_FROM,
                SQL(concat_field(table.dataset_id, table.table_id))

            ))
            selects.append(q)

        Log.note("inserting into table {{table}}", table=new_table_name)
        command = ConcatSQL((
            SQL_INSERT,
            SQL(concat_field(self.container.dataset.dataset_id, escape_name(new_table_name))),
            JoinSQL(SQL_UNION_ALL, selects[:2])
        ))
        job = self.container.client.query(command.sql)
        while job.state == "RUNNING":
            Log.note("job {{id}} state = {{state}}", id=job.job_id, state=job.state)
            Till(seconds=1).wait()
            job = self.container.client.get_job(job.job_id)
        Log.note("job {{id}} state = {{state}}", id=job.job_id, state=job.state)

        if job.errors:
            Log.error("Did not fill table:\n{{reason|json|indent}}", reason=job.errors)


# WRAP WITH etl.timestamp BEST SELECTION

def gen_select(full_path, total_schema, schema):
    if NESTED_TYPE in total_schema:
        k = NESTED_TYPE
        # PROMOTE EVERYTHING TO REPEATED
        v = schema.get(k)
        t = total_schema.get(k)

        if not v:
            inner = [ConcatSQL([
                SQL_SELECT_AS_STRUCT,
                JoinSQL(ConcatSQL((SQL_COMMA, SQL_CR)), gen_select(full_path, t, schema)),
            ])]
        else:
            row_name = "row" + text(len(full_path))
            ord_name = "ordering" + text(len(full_path))
            inner = [ConcatSQL([
                SQL_SELECT_AS_STRUCT,
                JoinSQL(ConcatSQL((SQL_COMMA, SQL_CR)), gen_select([row_name], t, v)),
                SQL_FROM,
                sql_call("UNNEST", [quote_column(*(full_path+[k]))]),
                SQL_AS,
                SQL(row_name),
                SQL(" WITH OFFSET AS "),
                SQL(ord_name),
                SQL_ORDERBY,
                SQL(ord_name)
            ])]

        return [sql_alias(sql_call("ARRAY", inner), k)]

    selection = []
    for k, t in jx.sort(total_schema.items(), 0):
        v = schema.get(k)
        if not v:
            selection.append(sql_alias(SQL_NULL, k))

        elif is_text(v):
            if v == t:
                selection.append(ConcatSQL((
                    quote_column(*(full_path + [k])),
                    SQL_AS,
                    quote_column(k)
                )))
            else:
                Log.error("Datatype mismatch on {{field}}: Can not merge {{type}} into {{main}}", field=join_field(full_path+[k]), type=v, main=t)
        elif is_data(v):
            if is_data(t):
                selects = gen_select(full_path + [k], t, v)
                inner = [ConcatSQL([
                    SQL_SELECT_AS_STRUCT,
                    JoinSQL(ConcatSQL((SQL_COMMA, SQL_CR)), selects),
                ])]
                selection.append(
                    sql_alias(sql_call("", inner), k)
                )
            else:
                Log.error("Datatype mismatch on {{field}}: Can not merge {{type}} into {{main}}",  field=join_field(full_path+[k]), type=v, main=t)
        else:
            Log.error("not expected")
    return selection


def parse_schema(schema, depth=0):
    """
    PARSE A BIGQUERY SCHEMA
    :param schema: bq schema
    :return:
    """
    output = {}

    if any(e.name == REPEATED for e in schema):
        schema = [e for e in schema if e.name == REPEATED]
    for e in schema:
        if not depth and e.name.startswith("_") and unescape_name(e.name) not in typed_to_bq_type:
            continue
        json_type = bq_type_to_json_type[e.field_type]
        name = unescape_name(e.name)
        if e.field_type == "RECORD":
            output[name] = parse_schema(e.fields)
        else:
            output[name] = json_type
    return output
