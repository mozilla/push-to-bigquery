import re
from collections import OrderedDict

from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning
from google.oauth2 import service_account

from jx_base import jx_expression, Container, Facts
from jx_bigquery.sql import (
    quote_column,
    unescape_name,
    ALLOWED,
    sql_call,
    sql_alias,
    escape_name,
    ApiName,
)
from jx_bigquery.typed_encoder import (
    schema_to_bq_schema,
    bq_type_to_json_type,
    NESTED_TYPE,
    typed_encode,
    PARTITION_FIELD,
    BQ_PARITITION_FIELD,
    typed_to_bq_type,
    REPEATED,
    json_type_to_bq_type,
)
from jx_python import jx
from mo_dots import listwrap, unwrap, split_field, join_field, Null, unwraplist, is_data
from mo_future import is_text, text, first
from mo_kwargs import override
from mo_logs import Log, Except
from mo_math.randoms import Random
from mo_threads import Till
from mo_times import Duration, MINUTE, YEAR, DAY
from mo_times.dates import Date
from pyLibrary.sql import (
    ConcatSQL,
    SQL,
    SQL_SELECT,
    JoinSQL,
    SQL_NULL,
    SQL_FROM,
    SQL_COMMA,
    SQL_AS,
    SQL_ORDERBY,
    SQL_CR,
    SQL_SELECT_AS_STRUCT,
    SQL_INSERT,
    SQL_STAR,
    SQL_CREATE,
    SQL_DESC,
)

EXTEND_LIMIT = 2 * MINUTE  # EMIT ERROR IF ADDING RECORDS TO TABLE TOO OFTEN
NEVER = 10 * YEAR

SUFFIX_PATTERN = re.compile(r"__\w{20}")


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
            field=str(escape_name(PARTITION_FIELD)),
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
        self.full_name = ApiName(account_info.project_id) + esc_name

        datasets = list(self.client.list_datasets())
        for _dataset in datasets:
            if ApiName(_dataset.dataset_id) == esc_name:
                self.dataset = _dataset.reference
                break
        else:
            _dataset = bigquery.Dataset(str(self.full_name))
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
            return Table(
                container=self,
                partition=Partition(partition),
                cluster=cluster,
                kwargs=kwargs,
            )
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
        full_name = self.full_name + escape_name(name)
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
        partition = Partition(partition)

        if read_only:
            Log.error("Can not create a table for read-only use")

        if sharded:
            view_sql_name = quote_column(self.full_name + escape_name(table))
            shard_name = escape_name(table + "_" + "".join(Random.sample(ALLOWED, 20)))
            shard_api_name = self.full_name + shard_name
            _shard = bigquery.Table(
                str(shard_api_name),
                schema=BQ_PARITITION_FIELD + schema_to_bq_schema(schema),
            )
            _shard.time_partitioning = unwrap(partition.bq_time_partitioning)
            _shard.clustering_fields = unwrap(
                unwraplist([str(ApiName(*split_field(f))) for f in listwrap(cluster)])
            )
            self.shard = self.client.create_table(_shard)

            self.client.query(
                ConcatSQL(
                    (
                        SQL("CREATE VIEW\n"),
                        view_sql_name,
                        SQL_AS,
                        SQL_SELECT,
                        SQL_STAR,
                        SQL_FROM,
                        quote_column(shard_api_name),
                    )
                ).sql
            )
        else:
            api_name = escape_name(table)
            full_name = self.full_name + api_name
            _table = bigquery.Table(
                str(full_name), schema=BQ_PARITITION_FIELD + schema_to_bq_schema(schema)
            )
            _table.time_partitioning = unwrap(partition.bq_time_partitioning)
            _table.clustering_fields = unwrap(
                unwraplist(
                    [
                        join_field(escape_name(p) for p in split_field(f))
                        for f in listwrap(cluster)
                    ]
                )
            )
            self.client.create_table(_table)
            Log.note("created table {{table}}", table=_table.table_id)

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
    def __init__(
        self, table, typed, read_only, sharded, partition, cluster, id, container
    ):
        esc_name = escape_name(table)
        self.full_name = container.full_name + esc_name
        self.short_name = table
        self.typed = typed
        self.read_only = read_only

        alias_view = container.client.get_table(str(self.full_name))
        if not sharded:
            if not read_only and alias_view.table_type == "VIEW":
                Log.error("Expecting a table, not a view")
            self.shard = alias_view
        else:
            if alias_view.table_type != "VIEW":
                Log.error("Sharded tables require a view")
            self.shard = None
        self.partition = partition
        self.cluster = cluster
        self.id = id
        self._schema = parse_schema(alias_view.schema)
        self.container = container
        self.last_extend = Date.now() - EXTEND_LIMIT

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
                    typed[str(escape_name(PARTITION_FIELD))] = Date(
                        self.partition.field(row)
                    ).format()
                    update.update(more)
                    output.append(typed)
                else:
                    break

            if update or not self.shard:
                # BATCH HAS ADDITIONAL COLUMNS!!
                # WE CAN NOT USE THE EXISTING SHARD, MAKE A NEW ONE:
                self._create_new_shard()
                Log.note(
                    "added new shard with name: {{shard}}", shard=self.shard.table_id
                )

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
        current_view = Null  # VIEW THAT POINTS TO PRIMARY SHARD
        primary_shard_name = None  # PRIMARY SHARD
        api_name = escape_name(self.short_name)

        for table_item in tables:
            table = table_item.reference
            table_api_name = ApiName(table.table_id)
            if str(table_api_name).startswith(str(api_name)):
                if table_api_name == api_name:
                    if table_item.table_type != "VIEW":
                        Log.error("expecting {{table}} to be a view", table=api_name)
                    current_view = self.container.client.get_table(table)
                    view_sql = current_view.view_query
                    # TODO: USE REAL PARSER
                    e = view_sql.lower().find("from ")
                    primary_shard_name = ApiName(
                        view_sql[e + 5 :].strip().split(".")[-1].strip("`")
                    )
                elif SUFFIX_PATTERN.match(str(table_api_name)[len(str(api_name)) :]):
                    shards.append(self.container.client.get_table(table))

        if not current_view:
            Log.error(
                "expecting {{table}} to be a view pointing to a table", table=api_name
            )

        shard_schemas = [parse_schema(shard.schema) for shard in shards]
        total_schema = merge_schema(*shard_schemas)

        for i, s in enumerate(shards):
            if ApiName(s.table_id) == primary_shard_name:
                if identical_schema(total_schema, shard_schemas[i]):
                    del shards[i]
                    del shard_schemas[i]
                    break
        else:
            name = "testing_" + "".join(Random.sample(ALLOWED, 20))
            primary_shard_name = escape_name(name)
            self.container.create_table(name, schema=total_schema, read_only=False)

        primary_full_name = self.container.full_name + primary_shard_name

        # REQUIRED FOR CLARITY OF SCHEMA
        # shard_schemas.insert(0, total_schema)
        # shards.insert(0, destination.shard.reference)

        selects = []
        for schema, table in zip(shard_schemas, shards):
            q = ConcatSQL(
                (
                    SQL_SELECT,
                    quote_column(escape_name(PARTITION_FIELD)),
                    SQL_COMMA,
                    JoinSQL(
                        ConcatSQL((SQL_COMMA, SQL_CR)),
                        gen_select([], total_schema, schema),
                    ),
                    SQL_FROM,
                    quote_column(ApiName(table.dataset_id, table.table_id)),
                )
            )
            selects.append(q)

        Log.note("inserting into table {{table}}", table=str(primary_shard_name))
        for s, shard in zip(selects, shards):
            command = ConcatSQL((SQL_INSERT, quote_column(primary_full_name), s))
            job = self.container.client.query(command.sql)
            while job.state == "RUNNING":
                Log.note("job {{id}} state = {{state}}", id=job.job_id, state=job.state)
                Till(seconds=1).wait()
                job = self.container.client.get_job(job.job_id)
            Log.note("job {{id}} state = {{state}}", id=job.job_id, state=job.state)

            if job.errors:
                Log.error(
                    "\n{{sql}}\nDid not fill table:\n{{reason|json|indent}}",
                    sql=s,
                    reason=job.errors,
                )

            self.container.client.delete_table(shard)

        # REMOVE OLD VIEW
        view_full_name = self.container.full_name + api_name
        if current_view:
            self.container.client.delete_table(current_view)

        # CREATE NEW VIEW
        self.container.client.query(
            ConcatSQL(
                (
                    SQL("CREATE VIEW\n"),
                    quote_column(view_full_name),
                    SQL_AS,
                    SQL_SELECT,
                    SQL_STAR,
                    SQL_FROM,
                    quote_column(primary_full_name),
                )
            ).sql
        )

    def condense(self):
        """
        :return:
        """
        # MAKE NEW SHARD
        partition = JoinSQL(
            SQL_COMMA,
            [
                quote_column(c.es_field)
                for f in listwrap(self.id.field)
                for c in self.schema.leaves(f)
            ],
        )
        order_by = JoinSQL(
            SQL_COMMA,
            [
                ConcatSQL((quote_column(c.es_field), SQL_DESC))
                for f in listwrap(self.id.version)
                for c in self.schema.leaves(f)
            ],
        )
        # WRAP WITH etl.timestamp BEST SELECTION

        self.container.client.query(
            ConcatSQL(
                (
                    SQL(
                        "SELECT * EXCEPT(_rank) FROM (SELECT *, RANK() OVER (PARTITION BY "
                    ),
                    partition,
                    SQL_ORDERBY,
                    order_by,
                    SQL(" AS _rank FROM "),
                    quote_column(self.full_name),
                    SQL(") a WHERE _rank=1"),
                )
            ).sql
        )


def gen_select(full_path, total_schema, schema):

    if identical_schema(total_schema, schema):
        if not full_path:
            return [quote_column(escape_name(k)) for k in total_schema.keys()]
        else:
            return [quote_column(*full_path)]

    if NESTED_TYPE in total_schema:
        k = NESTED_TYPE
        # PROMOTE EVERYTHING TO REPEATED
        v = schema.get(k)
        t = total_schema.get(k)

        if not v:
            inner = [
                ConcatSQL(
                    [
                        SQL_SELECT_AS_STRUCT,
                        JoinSQL(
                            ConcatSQL((SQL_COMMA, SQL_CR)),
                            gen_select(full_path, t, schema),
                        ),
                    ]
                )
            ]
        else:
            row_name = "row" + text(len(full_path))
            ord_name = "ordering" + text(len(full_path))
            inner = [
                ConcatSQL(
                    [
                        SQL_SELECT_AS_STRUCT,
                        JoinSQL(
                            ConcatSQL((SQL_COMMA, SQL_CR)), gen_select([row_name], t, v)
                        ),
                        SQL_FROM,
                        sql_call("UNNEST", [quote_column(*(full_path + [k]))]),
                        SQL_AS,
                        SQL(row_name),
                        SQL(" WITH OFFSET AS "),
                        SQL(ord_name),
                        SQL_ORDERBY,
                        SQL(ord_name),
                    ]
                )
            ]

        return [sql_alias(sql_call("ARRAY", inner), escape_name(k))]

    selection = []
    for k, t in jx.sort(total_schema.items(), 0):
        v = schema.get(k)
        if is_data(t):
            if not v:
                selects = gen_select(full_path + [k], t, {})
            elif is_data(v):
                selects = gen_select(full_path + [k], t, v)
            else:
                raise Log.error(
                    "Datatype mismatch on {{field}}: Can not merge {{type}} into {{main}}",
                    field=join_field(full_path + [k]),
                    type=v,
                    main=t,
                )
            inner = [
                ConcatSQL(
                    [
                        SQL_SELECT_AS_STRUCT,
                        JoinSQL(ConcatSQL((SQL_COMMA, SQL_CR)), selects),
                    ]
                )
            ]
            selection.append(sql_alias(sql_call("", inner), escape_name(k)))
        elif is_text(t):
            if not v:
                selection.append(
                    ConcatSQL(
                        (
                            sql_call(
                                "CAST",
                                [
                                    ConcatSQL(
                                        (SQL_NULL, SQL_AS, SQL(json_type_to_bq_type[t]))
                                    )
                                ],
                            ),
                            SQL_AS,
                            quote_column(escape_name(k)),
                        )
                    )
                )
            elif v == t:
                selection.append(
                    ConcatSQL(
                        (
                            quote_column(*(full_path + [k])),
                            SQL_AS,
                            quote_column(escape_name(k)),
                        )
                    )
                )
            else:
                Log.error(
                    "Datatype mismatch on {{field}}: Can not merge {{type}} into {{main}}",
                    field=join_field(full_path + [k]),
                    type=v,
                    main=t,
                )
        else:
            Log.error("not expected")
    return selection


def parse_schema(schema, depth=0):
    """
    PARSE A BIGQUERY SCHEMA
    :param schema: bq schema
    :return:
    """
    output = OrderedDict()

    if any(ApiName(e.name) == REPEATED for e in schema):
        schema = [e for e in schema if ApiName(e.name) == REPEATED]
    for e in schema:
        if (
            not depth
            and e.name.startswith("_")
            and unescape_name(ApiName(e.name)) not in typed_to_bq_type
        ):
            continue
        json_type = bq_type_to_json_type[e.field_type]
        name = unescape_name(ApiName(e.name))
        if e.field_type == "RECORD":
            output[name] = parse_schema(e.fields)
        else:
            output[name] = json_type
    return output


def identical_schema(a, b):
    if is_text(b):
        return a == b
    if len(a.keys()) != len(b.keys()):
        return False
    for (ka, va), (kb, vb) in zip(a.items(), b.items()):
        # WARNING!  ASSUMES dicts ARE OrderedDict
        if ka != kb or not identical_schema(va, vb):
            return False
    return True


def merge_schema(*schemas):
    try:
        return OrderedDict(
            (k, merge_schema(*[ss for s in schemas for ss in [s.get(k)] if ss]))
            for k in jx.sort(set(k for s in schemas for k in s.keys()))
        )
    except Exception:
        t = list(set(schemas))
        if len(t) == 1:
            return t[0]
        else:
            Log.error("Expecting types to match {{types|json}}", types=schemas)
