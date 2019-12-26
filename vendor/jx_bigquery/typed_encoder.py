import string

from google.cloud.bigquery import SchemaField

from jx_bigquery.sql import escape_name
from jx_python import jx
from mo_dots import is_many, is_data
from mo_future import is_text
from mo_json import (
    BOOLEAN,
    NUMBER,
    STRING,
    OBJECT,
    NESTED,
    python_type_to_json_type,
    INTEGER,
)
from mo_logs import Log

ALLOWED = string.ascii_letters + string.digits
PARTITION_FIELD ="_partition"  # name of column to use for partitioning
BQ_PARITITION_FIELD = [SchemaField(name=str(escape_name(PARTITION_FIELD)), field_type="TIMESTAMP", mode="NULLABLE")]


def typed_encode(value, schema):
    """
    RETURN (typed_value, schema_update, added_nested) TUPLES
    :param value:
    :param schema:
    :return:
    """
    if is_many(value):
        output = []
        update = {}
        nest_added = False
        child_schema = schema.get(NESTED_TYPE)
        if not child_schema:
            child_schema = schema[NESTED_TYPE] = {}

        for r in value:
            v, m, n = typed_encode(r, child_schema)
            output.append(v)
            update.update(m)
            nest_added |= n

        if update:
            return {str(REPEATED): output}, {NESTED_TYPE: update}, True
        else:
            return {str(REPEATED): output}, None, nest_added
    elif NESTED_TYPE in schema:
        if not value:
            return {str(REPEATED): []}, None, False
        else:
            return typed_encode([value], schema)
    elif is_data(value):
        output = {}
        update = {}
        nest_added = False
        for k, v in value.items():
            child_schema = schema.get(k)
            if not child_schema:
                child_schema = schema[k] = {}
            result, more_update, n = typed_encode(v, child_schema)
            output[str(escape_name(k))] = result
            if more_update:
                update.update({k: more_update})
                nest_added |= n
        return output, update, nest_added
    elif is_text(schema):
        value_type = python_type_to_json_type[value.__class__]
        if schema != value_type:
            Log.error(
                "Can not convert {{existing_type}} to {{expected_type}}",
                existing_type=value_type,
                expected_type=schema,
            )
        return value, None, False
    elif value is None:
        return {str(escape_name(t)): None for t, child_schema in schema}, None, False
    else:
        t, json_type = schema_type(value)
        child_schema = schema.get(t)
        update = None
        if not child_schema:
            schema[t] = json_type
            update = {t: json_type}
        return {str(escape_name(t)): value}, update, False


def schema_type(value):
    jt = python_type_to_json_type[value.__class__]
    return json_type_to_inserter_type[jt], jt


def schema_to_bq_schema(schema):
    if not schema:
        return []
    output = _schema_to_bq_schema(schema)
    return output


def _schema_to_bq_schema(schema):
    output = []
    nt = schema.get(NESTED_TYPE)
    if nt:
        schema = {NESTED_TYPE: nt}
    for t, sub_schema in jx.sort(schema.items(), 0):
        bqt = typed_to_bq_type.get(t, {"field_type": "RECORD", "mode": "NULLABLE"})
        if is_text(sub_schema):
            new_field_type = json_type_to_bq_type.get(sub_schema, sub_schema)
            if new_field_type != bqt["field_type"]:
                # OVERRIDE TYPE
                bqt = bqt.copy()
                bqt['field_type'] = new_field_type
            fields = ()
        else:
            fields = _schema_to_bq_schema(sub_schema)
        struct = SchemaField(name=str(escape_name(t)), fields=fields, **bqt)
        output.append(struct)
    return output


json_type_to_bq_type = {
    BOOLEAN: "BOOLEAN",
    NUMBER: "NUMERIC",
    STRING: "STRING",
    NESTED: "RECORD",
}

bq_type_to_json_type = {
    "INT64": NUMBER,
    "FLOAT64": NUMBER,
    "NUMERIC": NUMBER,
    "BOOL": BOOLEAN,
    "STRING": STRING,
    "BYTES": STRING,
    "DATE": NUMBER,
    "DATETIME": STRING,
    "TIME": STRING,
    "TIMESTAMP": STRING,
    "RECORD": OBJECT,
}

BOOLEAN_TYPE = "_b_"
NUMBER_TYPE = "_n_"
STRING_TYPE = "_s_"
NESTED_TYPE = "_a_"

json_type_to_inserter_type = {
    BOOLEAN: BOOLEAN_TYPE,
    INTEGER: NUMBER_TYPE,
    NUMBER: NUMBER_TYPE,
    STRING: STRING_TYPE,
    NESTED: NESTED_TYPE,
}

typed_to_bq_type = {
    BOOLEAN_TYPE: {"field_type": "BOOLEAN", "mode": "NULLABLE"},
    NUMBER_TYPE: {"field_type": "NUMERIC", "mode": "NULLABLE"},
    STRING_TYPE: {"field_type": "STRING", "mode": "NULLABLE"},
    NESTED_TYPE: {"field_type": "RECORD", "mode": "REPEATED"},
}

REPEATED = escape_name(NESTED_TYPE)
