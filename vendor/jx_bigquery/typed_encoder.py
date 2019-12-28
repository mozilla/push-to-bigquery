import string

from jx_bigquery.sql import escape_name
from mo_dots import is_many, is_data, Null, wrap
from mo_future import is_text, first
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


def typed_encode(value, schema):
    """
    RETURN (typed_value, schema_update, added_nested) TUPLES
    :param value: THE RECORD TO CONVERT TO STRICT TYPED FORM
    :param schema: LOOKUP SCHEMA, WILL BE UPDATED WITH CHANGES
    :param top_level_fields: MAP TO TOP LEVEL FIELDS
    :return: (record, update, nested) TUPLE
    """
    output, update, nested = _typed_encode(value, schema.lookup)
    if update:
        # REFRESH COLUMNS
        schema._columns = None
        _ = schema.columns

    worker = wrap(output)
    for path, field in schema._top_level_fields.items():
        worker[field] = worker[path]
        worker[path] = None

    schema._partition.apply(output)

    return output, update, nested


def _typed_encode(value, schema):
    if is_many(value):
        output = []
        update = {}
        nest_added = False
        child_schema = schema.get(NESTED_TYPE)
        if not child_schema:
            child_schema = schema[NESTED_TYPE] = {}

        for r in value:
            v, m, n = _typed_encode(r, child_schema)
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
            return _typed_encode([value], schema)
    elif is_data(value):
        output = {}
        update = {}
        nest_added = False
        for k, v in value.items():
            child_schema = schema.get(k)
            if not child_schema:
                child_schema = schema[k] = {}
            result, more_update, n = _typed_encode(v, child_schema)
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
    "TIMESTAMP": NUMBER,
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
