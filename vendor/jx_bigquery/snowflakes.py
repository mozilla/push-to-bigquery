from collections import OrderedDict

from google.cloud.bigquery import SchemaField

import jx_base
from jx_bigquery.partitions import Partition
from jx_bigquery.sql import (
    unescape_name,
    ApiName,
    escape_name)
from jx_bigquery.typed_encoder import (
    bq_type_to_json_type,
    NESTED_TYPE,
    typed_to_bq_type,
    REPEATED,
    json_type_to_bq_type, json_type_to_inserter_type)
from jx_python import jx
from mo_dots import (
    join_field,
    startswith_field,
    coalesce, Data, wrap, split_field, SLOT)
from mo_future import is_text, first, sort_using_key
from mo_json import NESTED, STRUCT, OBJECT
from mo_logs import Log
from mo_logs.strings import common_prefix
from mo_times.dates import Date


class Snowflake(jx_base.Snowflake):
    def __init__(self, es_index, top_level_fields, partition):
        self.es_index = es_index
        self.lookup = {}
        self._columns = None
        self.top_level_fields = top_level_fields
        self._top_level_fields = None
        self._es_type_info = Data()
        self._es_type_info[partition.field] = "TIMESTAMP"
        self.partition = partition
        self._partition = None

    @property
    def columns(self):
        if not self._columns:
            now = Date.now()
            columns = []

            def parse_schema(schema, tops, es_type_info, jx_path, nested_path, es_path):
                if is_text(schema):
                    json_type = schema
                    c = jx_base.Column(
                        name=join_field(jx_path),
                        es_column=coalesce(tops, str(es_path)),
                        es_index=self.es_index,
                        es_type=coalesce(es_type_info, json_type_to_bq_type[json_type]),
                        jx_type=json_type,
                        nested_path=nested_path,
                        last_updated=now,
                    )
                    columns.append(c)
                else:
                    c = jx_base.Column(
                        name=join_field(jx_path),
                        es_column=str(es_path),
                        es_index=self.es_index,
                        es_type="RECORD",
                        jx_type=OBJECT,
                        nested_path=nested_path,
                        last_updated=now,
                    )
                    columns.append(c)
                    count = len(columns)
                    for k, s in schema.items():
                        if k == NESTED_TYPE:
                            c.jx_type = NESTED
                            parse_schema(
                                s,
                                tops if is_text(tops) else tops[k],
                                es_type_info if is_text(es_type_info) else es_type_info[k],
                                jx_path+(k,),
                                (jx_path,)+nested_path,
                                es_path+escape_name(k)
                            )
                        else:
                            parse_schema(
                                s,
                                tops if is_text(tops) else tops[k],
                                es_type_info if is_text(es_type_info) else es_type_info[k],
                                jx_path+(k,),
                                nested_path,
                                es_path+escape_name(k)
                            )
                    if is_text(tops) and len(columns) > count + 1:
                        Log.error("too many top level fields at {{field}}", field=join_field(jx_path))

            parse_schema(self.lookup, self.top_level_fields, self._es_type_info, (), (".",), ApiName())
            self._columns = columns

            self._top_level_fields = {}
            for path, field in wrap(self.top_level_fields).leaves():
                leaves = self.leaves(path)
                if not leaves:
                    continue
                if len(leaves) > 1:
                    Log.error("expecting {{path}} to have just one primitive value", path=path)
                specific_path = first(leaves).name
                self._top_level_fields[".".join(map(str, map(escape_name, split_field(specific_path))))] = field

            self._partition = Partition(kwargs=self.partition, schema=self)

        return self._columns

    @classmethod
    def parse(cls, schema, es_index, top_level_fields, partition):
        """
        PARSE A BIGQUERY SCHEMA
        :param schema: bq schema
        :return:
        """
        def parse_schema(schema, jx_path, nested_path, es_path):
            output = OrderedDict()

            if any(ApiName(e.name) == REPEATED for e in schema):
                schema = [e for e in schema if ApiName(e.name) == REPEATED]

            for e in schema:
                json_type = bq_type_to_json_type[e.field_type]
                name = unescape_name(ApiName(e.name))
                full_name = jx_path + (name,)
                full_es_path = es_path + (e.name,)

                if e.field_type == "RECORD":
                    if e.mode == "REPEATED":
                        output[name] = parse_schema(
                            e.fields, full_name, (jx_path,) + nested_path, full_es_path
                        )
                    else:
                        output[name] = parse_schema(
                            e.fields, full_name, nested_path, full_es_path
                        )
                else:
                    if e.mode == "REPEATED":
                        output[name] = {NESTED_TYPE: json_type}
                    else:
                        output[name] = json_type
            return output

        output = Snowflake(es_index, top_level_fields, partition)

        # GRAB THE TOP-LEVEL FIELDS
        top_fields = [field for path, field in top_level_fields.leaves()]
        i = 0
        while schema[i].name in top_fields:
            i = i + 1

        output.lookup = parse_schema(schema[i:], (), (".",), ())

        # INSERT TOP-LEVEL FIELDS
        lookup = wrap(output.lookup)

        for column in schema[:i]:
            path = first(name for name, field in top_level_fields.leaves() if field == column.name)
            json_type = bq_type_to_json_type[column.field_type]
            lookup[path] = OrderedDict([(json_type_to_inserter_type[json_type], json_type)])

        return output

    def leaves(self, name):
        return [
            c
            for c in self.columns
            if c.jx_type not in STRUCT and startswith_field(c.name, name)
        ]

    def __eq__(self, other):
        if not isinstance(other, Snowflake):
            return False

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
        return identical_schema(self.lookup, other.lookup)

    def __or__(self, other):
        return merge(self, other)

    def to_bq_schema(self):
        top_fields = []

        def _schema_to_bq_schema(jx_path, es_path, schema):
            output = []
            nt = schema.get(NESTED_TYPE)
            if nt:
                schema = {NESTED_TYPE: nt}
            for t, sub_schema in jx.sort(schema.items(), 0):
                bqt = typed_to_bq_type.get(t, {"field_type": "RECORD", "mode": "NULLABLE"})
                full_name = es_path+escape_name(t)
                top_field = self._top_level_fields.get(str(full_name))
                if is_text(sub_schema):
                    new_field_type = json_type_to_bq_type.get(sub_schema, sub_schema)
                    if new_field_type != bqt["field_type"]:
                        # OVERRIDE TYPE
                        bqt = bqt.copy()
                        bqt['field_type'] = new_field_type
                    fields = ()
                else:
                    fields = _schema_to_bq_schema(jx_path+(t,), full_name, sub_schema)

                if top_field:
                    if fields:
                        Log.error("not expecting a structure")
                    if self._partition.field == top_field:
                        # PARTITION FIELD MUST BE A TIMESTAMP
                        bqt = bqt.copy()
                        bqt['field_type'] = "TIMESTAMP"
                    struct = SchemaField(name=top_field, fields=fields, **bqt)
                    top_fields.append(struct)
                elif not fields and bqt['field_type'] == "RECORD":
                    # THIS CAN HAPPEN WHEN WE MOVE A PRIMITIVE FIELD TO top_fields
                    pass
                else:
                    struct = SchemaField(name=str(escape_name(t)), fields=fields, **bqt)
                    output.append(struct)
            return output

        if not self.lookup:
            return []
        main_schema = _schema_to_bq_schema((), ApiName(), self.lookup)
        output = sort_using_key(top_fields, key=lambda v: v.name) + main_schema
        return output


def merge(*schemas):
    def _merge(*schemas):
        if len(schemas) == 1:
            return schemas[0]
        try:
            return OrderedDict(
                (k, _merge(*[ss for s in schemas for ss in [s.get(k)] if ss]))
                for k in jx.sort(set(k for s in schemas for k in s.keys()))
            )
        except Exception:
            t = list(set(schemas))
            if len(t) == 1:
                return t[0]
            else:
                Log.error("Expecting types to match {{types|json}}", types=schemas)

    output = Snowflake(es_index=common_prefix(*(s.es_index for s in schemas)))
    output.lookup = _merge(*(s.lookup for s in schemas))
    return output