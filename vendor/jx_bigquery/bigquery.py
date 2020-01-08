import re

from google.cloud import bigquery
from google.oauth2 import service_account

from jx_base import Container, Facts
from jx_bigquery import snowflakes
from jx_bigquery.partitions import Partition
from jx_bigquery.snowflakes import Snowflake
from jx_bigquery.sql import (
    quote_column,
    ALLOWED,
    sql_call,
    sql_alias,
    escape_name,
    ApiName,
    sql_query)
from jx_bigquery.typed_encoder import (
    NESTED_TYPE,
    typed_encode,
    REPEATED,
    json_type_to_bq_type,
)
from jx_python import jx
from mo_dots import (
    listwrap,
    unwrap,
    split_field,
    join_field,
    Null,
    unwraplist,
    is_data,
    Data, coalesce, wrap)
from mo_future import is_text, text, first
from mo_kwargs import override
from mo_logs import Log, Except
from mo_math.randoms import Random
from mo_threads import Till
from mo_times import MINUTE, Timer
from mo_times.dates import Date
from mo_sql import (
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
    SQL_DESC,
    SQL_UNION_ALL,
)

EXTEND_LIMIT = 2 * MINUTE  # EMIT ERROR IF ADDING RECORDS TO TABLE TOO OFTEN

SUFFIX_PATTERN = re.compile(r"__\w{20}")


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
            _dataset = bigquery.Dataset(text(self.full_name))
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
        id=None,
        kwargs=None,
    ):
        try:
            return Table(kwargs=kwargs, container=self)
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
        return self.create_table(kwargs=kwargs)

    def delete_table(self, name):
        full_name = self.full_name + escape_name(name)
        self.client.delete_table(full_name)

    @override
    def create_table(
        self,
        table,
        lookup=None,
        typed=True,
        read_only=True,  # TO PREVENT ACCIDENTAL WRITING
        sharded=False,
        partition=None,  # PARTITION RULES
        cluster=None,  # TUPLE OF FIELDS TO SORT DATA
        top_level_fields=None,
        kwargs=None,
    ):
        full_name = self.full_name + escape_name(table)
        schema = Snowflake(text(full_name), top_level_fields, partition, lookup=lookup)

        if read_only:
            Log.error("Can not create a table for read-only use")

        if sharded:
            shard_name = escape_name(table + "_" + "".join(Random.sample(ALLOWED, 20)))
            shard_api_name = self.full_name + shard_name
            _shard = bigquery.Table(text(shard_api_name), schema=schema.to_bq_schema())
            _shard.time_partitioning = unwrap(schema._partition.bq_time_partitioning)
            _shard.clustering_fields = [
                c.es_column
                for f in listwrap(cluster)
                for c in [first(schema.leaves(f))]
                if c
            ] or None
            self.shard = self.client.create_table(_shard)

            if lookup:
                # ONLY MAKE THE VIEW IF THERE IS A SCHEMA
                self.create_view(full_name, shard_api_name)
        else:
            _table = bigquery.Table(text(full_name), schema=schema.to_bq_schema())
            _table.time_partitioning = unwrap(schema._partition.bq_time_partitioning)
            _table.clustering_fields = [
                l.es_column for f in listwrap(cluster) for l in schema.leaves(f)
            ]
            self.client.create_table(_table)
            Log.note("created table {{table}}", table=_table.table_id)

        return Table(
            table=table,
            typed=typed,
            read_only=read_only,
            sharded=sharded,
            partition=partition,
            top_level_fields=top_level_fields,
            kwargs=kwargs,
            container=self,
        )

    def create_view(self, view_api_name, shard_api_name):
        job = self.client.query(
            ConcatSQL(
                (
                    SQL("CREATE VIEW\n"),
                    quote_column(view_api_name),
                    SQL_AS,
                    sql_query({"from": shard_api_name})
                )
            ).sql
        )

class Table(Facts):
    @override
    def __init__(
        self,
        table,
        typed,
        read_only,
        sharded,
        container,
        id=Null,
        partition=Null,
        cluster=Null,
        top_level_fields=Null,
        kwargs=None,
    ):
        self.short_name = table
        self.typed = typed
        self.read_only = read_only
        self.cluster = cluster
        self.id = id
        self.top_level_fields = top_level_fields
        self.config = Data(  # USED TO REPLICATE THIS
            typed=typed,
            read_only=read_only,
            sharded=sharded,
            id=id,
            partition=partition,
            cluster=cluster,
            top_level_fields=top_level_fields
        )

        esc_name = escape_name(table)
        self.full_name = container.full_name + esc_name
        self.alias_view = alias_view = container.client.get_table(text(self.full_name))
        if not sharded:
            if not read_only and alias_view.table_type == "VIEW":
                Log.error("Expecting a table, not a view")
            self.shard = alias_view
        else:
            if alias_view.table_type != "VIEW":
                Log.error("Sharded tables require a view")
            current_view = container.client.get_table(text(self.full_name))
            view_sql = current_view.view_query
            self.shard = container.client.get_table(text(container.full_name+_extract_primary_shard_name(view_sql)))
        self._schema = Snowflake.parse(
            alias_view.schema, text(self.full_name), self.top_level_fields, partition
        )
        self.partition = partition
        self.container = container
        self.last_extend = Date.now() - EXTEND_LIMIT

    @property
    def schema(self):
        return self._schema

    def _create_new_shard(self):
        primary_shard = self.container.create_table(
            table=self.short_name + "_" + "".join(Random.sample(ALLOWED, 20)),
            sharded=False,
            lookup=self._schema.lookup,
            kwargs=self.config,
        )
        self.shard = primary_shard.shard

    def extend(self, rows):
        if self.read_only:
            Log.error("not for writing")

        try:
            update = {}
            with Timer("encoding"):
                while True:
                    output = []
                    for rownum, row in enumerate(rows):
                        typed, more, add_nested = typed_encode(row, self.schema)
                        update.update(more)
                        if add_nested:
                            # row HAS NEW NESTED COLUMN!
                            # GO OVER THE rows AGAIN SO "RECORD" GET MAPPED TO "REPEATED"
                            break
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
            with Timer("insert to bq"):
                failures = self.container.client.insert_rows_json(
                    self.shard,
                    json_rows=output,
                    row_ids=[None] * len(output),
                    skip_invalid_rows=False,
                    ignore_unknown_values=False,
                )
            if failures:
                if all(r == "stopped" for r in wrap(failures).errors.reason):
                    self._create_new_shard()
                    Log.note(
                        "STOPPED encountered: Added new shard with name: {{shard}}", shard=self.shard.table_id
                    )
                Log.error("Got {{num}} failures:\n{{failures|json}}", num=len(failures), failures=failures[:5])
            else:
                Log.note("{{num}} rows added", num=len(output))
        except Exception as e:
            e = Except.wrap(e)
            if "Request payload size exceeds the limit" in e:
                pass
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
            if text(table_api_name).startswith(text(api_name)):
                if table_api_name == api_name:
                    if table_item.table_type != "VIEW":
                        Log.error("expecting {{table}} to be a view", table=api_name)
                    current_view = self.container.client.get_table(table)
                    view_sql = current_view.view_query
                    primary_shard_name = _extract_primary_shard_name(view_sql)
                elif SUFFIX_PATTERN.match(text(table_api_name)[len(text(api_name)) :]):
                    shards.append(self.container.client.get_table(table))

        if not current_view:
            Log.error(
                "expecting {{table}} to be a view pointing to a table", table=api_name
            )

        shard_schemas = [
            Snowflake.parse(
                schema=shard.schema,
                es_index=text(self.container.full_name + ApiName(shard.table_id)),
                top_level_fields=self.top_level_fields,
                partition=self.partition,
            )
            for shard in shards
        ]
        total_schema = snowflakes.merge(
            shard_schemas,
            es_index=text(self.full_name),
            top_level_fields=self.top_level_fields,
            partition=self.partition,
        )

        for i, s in enumerate(shards):
            if ApiName(s.table_id) == primary_shard_name:
                if total_schema == shard_schemas[i]:
                    # USE THE CURRENT PRIMARY SHARD AS A DESTINATION
                    del shards[i]
                    del shard_schemas[i]
                    break
        else:
            name = self.short_name + "_" + "".join(Random.sample(ALLOWED, 20))
            primary_shard_name = escape_name(name)
            self.container.create_table(
                table=name,
                schema=total_schema,
                sharded=False,
                read_only=False,
                kwargs=self.config,
            )

        primary_full_name = self.container.full_name + primary_shard_name

        selects = []
        for schema, table in zip(shard_schemas, shards):
            q = ConcatSQL(
                (
                    SQL_SELECT,
                    JoinSQL(
                        ConcatSQL((SQL_COMMA, SQL_CR)),
                        gen_select(total_schema, schema),
                    ),
                    SQL_FROM,
                    quote_column(ApiName(table.dataset_id, table.table_id)),
                )
            )
            selects.append(q)

        Log.note("inserting into table {{table}}", table=text(primary_shard_name))
        matched = []
        unmatched = []
        for sel, shard, schema in zip(selects, shards, shard_schemas):
            if schema == total_schema:
                matched.append((sel, shard, schema))
            else:
                unmatched.append((sel, shard, schema))

        # EVERYTHING THAT IS IDENTICAL TO PRIMARY CAN BE MERGED IN A SINGLE QUERY
        if matched:
            command = ConcatSQL(
                (
                    SQL_INSERT,
                    quote_column(primary_full_name),
                    JoinSQL(
                        SQL_UNION_ALL,
                        ConcatSQL(
                            (SQL_SELECT, SQL_STAR, SQL_FROM, shard.full_name)
                            for _, shard, _ in matched
                        ),
                    ),
                )
            )
            job = self.container.client.query(command.sql)
            while job.state == "RUNNING":
                Log.note("job {{id}} state = {{state}}", id=job.job_id, state=job.state)
                Till(seconds=1).wait()
                job = self.container.client.get_job(job.job_id)
            Log.note("job {{id}} state = {{state}}", id=job.job_id, state=job.state)

            if job.errors:
                Log.error(
                    "\n{{sql}}\nDid not fill table:\n{{reason|json|indent}}",
                    sql=command.sql,
                    reason=job.errors,
                )
            for _, shard, _ in matched:
                self.container.client.delete_table(shard)

        # ALL OTHER SCHEMAS MISMATCH
        for s, shard, _ in unmatched:
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
                    sql=command.sql,
                    reason=job.errors,
                )

            self.container.client.delete_table(shard)

        # REMOVE OLD VIEW
        view_full_name = self.container.full_name + api_name
        if current_view:
            self.container.client.delete_table(current_view)

        # CREATE NEW VIEW
        self.container.create_view(view_full_name, primary_full_name)

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
                        "SELECT * EXCEPT (_rank) FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY "
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


def _extract_primary_shard_name(view_sql):
    # TODO: USE REAL PARSER
    e = view_sql.lower().find("from ")
    return ApiName(
        view_sql[e + 5 :].strip().split(".")[-1].strip("`")
    )


def gen_select(total_schema, schema):

    def _gen_select(jx_path, es_path, total_tops, total_schema, source_tops, source_schema):
        if total_schema == source_schema and total_tops == source_tops:
            if not jx_path:  # TOP LEVEL FIELDS
                return [quote_column(escape_name(k)) for k in total_schema.keys() if not is_text(total_tops[k])]
            else:
                Log.error("should not happen")

        if NESTED_TYPE in total_schema:
            k = NESTED_TYPE
            # PROMOTE EVERYTHING TO REPEATED
            v = source_schema.get(k)
            t = total_schema.get(k)

            if not v:
                # CONVERT INNER OBJECT TO ARRAY OF ONE STRUCT
                inner = [
                    ConcatSQL(
                        [
                            SQL_SELECT_AS_STRUCT,
                            JoinSQL(
                                ConcatSQL((SQL_COMMA, SQL_CR)),
                                _gen_select(jx_path, es_path + REPEATED, Null, t, Null, source_schema),
                            ),
                        ]
                    )
                ]
            else:
                row_name = "row" + text(len(jx_path))
                ord_name = "ordering" + text(len(jx_path))
                inner = [
                    ConcatSQL(
                        [
                            SQL_SELECT_AS_STRUCT,
                            JoinSQL(
                                ConcatSQL((SQL_COMMA, SQL_CR)), _gen_select([row_name], ApiName(row_name), Null, t, Null, v)
                            ),
                            SQL_FROM,
                            sql_call("UNNEST", [es_path+escape_name(k)]),
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
            k_total_tops = total_tops if is_text(total_tops) else total_tops[k]
            k_tops = source_tops if is_text(source_tops) else source_tops[k]
            v = source_schema.get(k)
            if is_text(k_total_tops):
                # DO NOT INCLUDE TOP_LEVEL_FIELDS
                pass
            elif t == v and k_total_tops == k_tops:
                selection.append(
                    ConcatSQL(
                        (
                            quote_column(es_path + escape_name(k)),
                            SQL_AS,
                            quote_column(escape_name(k)),
                        )
                    )
                )
            elif is_data(t):
                if not v:
                    selects = _gen_select(jx_path + [k], es_path + escape_name(k), k_total_tops, t, source_tops, {})
                elif is_data(v):
                    selects = _gen_select(jx_path + [k], es_path + escape_name(k), k_total_tops, t, source_tops, v)
                else:
                    raise Log.error(
                        "Datatype mismatch on {{field}}: Can not merge {{type}} into {{main}}",
                        field=join_field(jx_path + [k]),
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
                if is_text(k_tops):
                    # THE SOURCE HAS THIS PROPERTY AS A TOP_LEVEL_FIELD
                    selection.append(
                        ConcatSQL(
                            (
                                SQL(k_tops),
                                SQL_AS,
                                quote_column(escape_name(k)),
                            )
                        )
                    )
                elif v == t:
                    selection.append(
                        ConcatSQL(
                            (
                                quote_column(es_path + escape_name(k)),
                                SQL_AS,
                                quote_column(escape_name(k)),
                            )
                        )
                    )
                else:
                    if v:
                        Log.note(
                            "Datatype mismatch on {{field}}: Can not merge {{type}} into {{main}}",
                            field=join_field(jx_path + [k]),
                            type=v,
                            main=t,
                        )
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
            else:
                Log.error("not expected")
        return selection

    output = _gen_select([], ApiName(), total_schema.top_level_fields, total_schema.lookup, schema.top_level_fields, schema.lookup)
    tops = []
    as_timestamp = first(schema.leaves(total_schema.partition.field))  # PARTITION FIELD IS A TIMESTAMP

    for path, name in total_schema.top_level_fields.leaves():
        source = schema.top_level_fields[path]
        if source:
            # ALREADY TOP LEVEL FIELD
            source = SQL(source)
        else:
            # PULL OUT TOP LEVEL FIELD
            column = first(schema.leaves(path))
            if column is as_timestamp:
                source = ConcatSQL((
                    SQL("TIMESTAMP_MILLIS(CAST("),
                    SQL(column.es_column),
                    SQL("*1000 AS INT64))")
                ))
            else:
                source = SQL(column.es_column)

        tops.append(ConcatSQL((
            source,
            SQL_AS,
            quote_column(ApiName(name))
        )))

    return tops + output
