# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from jx_base import Column, container
from jx_base.container import Container
from jx_base.dimensions import Dimension
from jx_base.expressions import jx_expression
from jx_base.language import is_op
from jx_base.query import QueryOp
from jx_elasticsearch.es52.agg_op import es_aggsop, is_aggsop
from jx_elasticsearch.es52.agg_bulk import is_bulk_agg, es_bulkaggsop
from jx_elasticsearch.es52.deep import es_deepop, is_deepop
from jx_elasticsearch.es52.set_bulk import is_bulk_set, es_bulksetop
from jx_elasticsearch.es52.set_op import es_setop, is_setop
from jx_elasticsearch.es52.stats import QueryStats
from jx_elasticsearch.es52.util import aggregates, temper_limit
from jx_elasticsearch.meta import ElasticsearchMetadata, Table
from jx_python import jx
from mo_dots import Data, coalesce, is_list, join_field, listwrap, split_field, startswith_field, unwrap, wrap
from mo_dots.lists import last
from mo_future import sort_using_key
from mo_json import OBJECT, value2json
from mo_json.typed_encoder import EXISTS_TYPE, NESTED_TYPE
from mo_kwargs import override
from mo_logs import Except, Log
from mo_times import Date
from pyLibrary.env import http
from jx_elasticsearch import elasticsearch


class ES52(Container):
    """
    SEND jx QUERIES TO ElasticSearch
    """

    def __new__(cls, *args, **kwargs):
        if (len(args) == 1 and args[0].get("index") == "meta") or kwargs.get("index") == "meta":
            output = ElasticsearchMetadata.__new__(ElasticsearchMetadata, *args, **kwargs)
            output.__init__(*args, **kwargs)
            return output
        else:
            return Container.__new__(cls)

    @override
    def __init__(
        self,
        host,
        index,  # THE NAME OF THE SNOWFLAKE (IF WRITING)
        alias=None,  # THE NAME OF THE SNOWFLAKE (FOR READING)
        type=None,
        name=None,  # THE FULL NAME OF THE TABLE (THE NESTED PATH INTO THE SNOWFLAKE)
        port=9200,
        read_only=True,
        timeout=None,  # NUMBER OF SECONDS TO WAIT FOR RESPONSE, OR SECONDS TO WAIT FOR DOWNLOAD (PASSED TO requests)
        wait_for_active_shards=1,  # ES WRITE CONSISTENCY (https://www.elastic.co/guide/en/elasticsearch/reference/1.7/docs-index_.html#index-consistency)
        typed=None,
        kwargs=None
    ):
        Container.__init__(self)
        if not container.config.default:
            container.config.default = {
                "type": "elasticsearch",
                "settings": unwrap(kwargs)
            }
        self.edges = Data()  # SET EARLY, SO OTHER PROCESSES CAN REQUEST IT
        self.worker = None
        self.settings = kwargs
        self._namespace = ElasticsearchMetadata(kwargs=kwargs)
        self.name = name = self._namespace._find_alias(coalesce(alias, index, name))
        if read_only:
            self.es = elasticsearch.Alias(alias=name, index=None, kwargs=kwargs)
        else:
            self.es = elasticsearch.Cluster(kwargs=kwargs).get_index(read_only=read_only, kwargs=kwargs)

        self._ensure_max_result_window_set(name)
        self.settings.type = self.es.settings.type
        self.stats = QueryStats(self.es.cluster)

        columns = self.snowflake.columns  # ABSOLUTE COLUMNS
        is_typed = any(c.es_column == EXISTS_TYPE for c in columns)

        if typed == None:
            # SWITCH ON TYPED MODE
            self.typed = is_typed
        else:
            if is_typed != typed:
                Log.error("Expecting given typed {{typed}} to match {{is_typed}}", typed=typed, is_typed=is_typed)
            self.typed = typed

        if not typed:
            # ADD EXISTENCE COLUMNS
            all_paths = {'.': None}  # MAP FROM path TO parent TO MAKE A TREE

            def nested_path_of(v):
                if v == '.':
                    return ('.',)
                return (v,) + nested_path_of(all_paths[v])

            query_paths = sort_using_key(set(step for path in self.snowflake.query_paths for step in path), key=lambda p: len(split_field(p)))
            for step in query_paths:
                if step in all_paths:
                    continue
                else:
                    best = '.'
                    for candidate in all_paths.keys():
                        if startswith_field(step, candidate):
                            if startswith_field(candidate, best):
                                best = candidate
                    all_paths[step] = best
            for p in all_paths.keys():
                nested_path = nested_path_of(p)
                try:
                    self.namespace.meta.columns.add(Column(
                        name=p,
                        es_column=p,
                        es_index=self.name,
                        es_type=OBJECT,
                        jx_type=OBJECT,
                        nested_path=nested_path,
                        multi=1001 if last(split_field(p)) == NESTED_TYPE else None,
                        last_updated=Date.now()
                    ))
                except Exception as e:
                    raise e

    @property
    def snowflake(self):
        return self._namespace.get_snowflake(self.es.settings.alias)

    @property
    def namespace(self):
        return self._namespace

    def get_table(self, full_name):
        return Table(full_name, self)

    def get_schema(self, query_path):
        return self._namespace.get_schema(query_path)

    def __data__(self):
        settings = self.settings.copy()
        settings.settings = None
        return settings

    def __enter__(self):
        Log.error("No longer used")
        return self

    def __exit__(self, type, value, traceback):
        if not self.worker:
            return

        if isinstance(value, Exception):
            self.worker.stop()
            self.worker.join()
        else:
            self.worker.join()

    @property
    def query_path(self):
        return join_field(split_field(self.name)[1:])

    @property
    def url(self):
        return self.es.url

    def _ensure_max_result_window_set(self, name):
        # TODO : CHECK IF THIS IS ALREADY SET, IT TAKES TOO LONG
        for i, s in self.es.cluster.get_metadata().indices.items():
            if name == i or name in s.aliases:
                if s.settings.index.max_result_window != '100000' or s.settings.index.max_inner_result_window != '100000':
                    Log.note("setting max_result_window")
                    self.es.cluster.put("/" + name + "/_settings", data={"index": {
                        "max_inner_result_window": 100000,
                        "max_result_window": 100000
                    }})
                    break

    def query(self, _query):
        try:
            query = QueryOp.wrap(_query, container=self, namespace=self.namespace)

            self.stats.record(query)

            for s in listwrap(query.select):
                if s.aggregate != None and not aggregates.get(s.aggregate):
                    Log.error(
                        "ES can not aggregate {{name}} because {{aggregate|quote}} is not a recognized aggregate",
                        name=s.name,
                        aggregate=s.aggregate
                    )

            frum = query["from"]
            if is_op(frum, QueryOp):
                result = self.query(frum)
                q2 = query.copy()
                q2.frum = result
                return jx.run(q2)

            if is_bulk_agg(self.es, query):
                return es_bulkaggsop(self, frum, query)
            if is_bulk_set(self.es, query):
                return es_bulksetop(self, frum, query)

            query.limit = temper_limit(query.limit, query)

            if is_deepop(self.es, query):
                return es_deepop(self.es, query)
            if is_aggsop(self.es, query):
                return es_aggsop(self.es, frum, query)
            if is_setop(self.es, query):
                return es_setop(self.es, query)
            Log.error("Can not handle")
        except Exception as e:
            e = Except.wrap(e)
            if "Data too large, data for" in e:
                http.post(self.es.cluster.url / "_cache/clear")
                Log.error("Problem (Tried to clear Elasticsearch cache)", e)
            Log.error("problem", e)

    def addDimension(self, dim):
        if is_list(dim):
            Log.error("Expecting dimension to be a object, not a list:\n{{dim}}",  dim= dim)
        self._addDimension(dim, [])

    def _addDimension(self, dim, path):
        dim.full_name = dim.name
        for e in dim.edges:
            d = Dimension(e, dim, self)
            self.edges[d.full_name] = d

    def __getitem__(self, item):
        c = self.get_columns(table_name=self.name, column_name=item)
        if c:
            if len(c) > 1:
                Log.error("Do not know how to handle multipole matches")
            return c[0]

        e = self.edges[item]
        if not c:
            Log.warning("Column with name {{column|quote}} can not be found in {{table}}", column=item, table=self.name)
        return e

    def __getattr__(self, item):
        return self.edges[item]

    def update(self, command):
        """
        EXPECTING command == {"set":term, "where":where}
        THE set CLAUSE IS A DICT MAPPING NAMES TO VALUES
        THE where CLAUSE IS AN ES FILTER
        """
        command = wrap(command)
        table = self.get_table(command['update'])

        es_index = self.es.cluster.get_index(read_only=False, alias=None, kwargs=self.es.settings)

        schema = table.schema

        # GET IDS OF DOCUMENTS
        query = {
            "from": command['update'],
            "select": [{"value": "_id"}] + [
                {"name": k, "value": v}
                for k, v in command.set.items()
            ],
            "where": command.where,
            "format": "list",
            "limit": 10000
        }

        results = self.query(query)

        if results.data:
            content = "".join(
                t
                for r in results.data
                for _id, row in [(r._id, r)]
                for _ in [row.__setitem__('_id', None)]  # WARNING! DESTRUCTIVE TO row
                for update in map(value2json, ({"update": {"_id": _id}}, {"doc": row}))
                for t in (update, "\n")
            )
            response = self.es.cluster.post(
                es_index.path + "/" + "_bulk",
                data=content,
                timeout=self.settings.timeout,
                params={"wait_for_active_shards": self.settings.wait_for_active_shards}
            )
            if response.errors:
                Log.error("could not update: {{error}}", error=[e.error for i in response["items"] for e in i.values() if e.status not in (200, 201)])

        # DELETE BY QUERY, IF NEEDED
        if "." in listwrap(command.clear):
            es_filter = self.es.cluster.lang[jx_expression(command.where)].to_esfilter(schema)
            self.es.delete_record(es_filter)
            return

        es_index.flush()

