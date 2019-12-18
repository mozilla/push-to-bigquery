# CREATE snowfla


# LOAD EXISTING TABLE
# PUSH RECORDS
import string

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.oauth2 import service_account

from jx_python import jx
from mo_dots import is_many, is_data, concat_field
from mo_files.url import hex2chr
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
from mo_kwargs import override
from mo_logs import Log, Except

ALLOWED = string.ascii_letters + string.digits


def quote_column(name):
    def quote(c):
        if c == "_":
            return "__"
        if c in ALLOWED:
            return c
        return "_" + hex(ord(c))[2:] + "_"

    esc_name = "".join(map(quote, name))
    return esc_name


def unquote_column(esc_name):
    parts = esc_name.split("_")
    result = parts[:1]
    for i, (p, q) in jx.chunk(parts[1:], 2):
        if len(p) == 0:
            result.append("_")
        else:
            result.append(hex2chr(p))
        result.append(q)
    name = "".join(result)
    return name


class Container(object):
    @override
    def __init__(
        self,
        name,
        type,
        project_id,
        private_key_id,
        private_key,
        client_email,
        client_id,
        auth_uri,
        token_uri,
        auth_provider_x509_cert_url,
        client_x509_cert_url,
        kwargs,
    ):
        creds = service_account.Credentials.from_service_account_info(info=kwargs)
        self.client = bigquery.Client(project=project_id, credentials=creds)
        self.short_name = name
        esc_name = quote_column(name)
        self.full_name = concat_field(project_id, esc_name)

        datasets = list(self.client.list_datasets())
        for dataset in datasets:
            if dataset.dataset_id == esc_name:
                self.dataset = datasets
                break
        else:
            dataset = bigquery.Dataset(self.full_name)
            dataset.location = "US"
            self.dataset = self.client.create_dataset(dataset)

    @override
    def get_or_create_index(
        self, name, schema=None, typed=True, read_only=False, kwargs=None
    ):
        try:
            return Index(kwargs=kwargs, container=self)
        except Exception as e:
            e = Except.wrap(e)
            if "Not found: Table" in e:
                return self.create_index(kwargs)
            Log.error("could not get table {{table}}", table=name, cause=e)

    @override
    def create_or_replace_index(
        self, name, schema=None, typed=True, read_only=False, kwargs=None
    ):
        try:
            self.delete_index(name)
        except Exception as e:
            e = Except.wrap(e)
            if "Not found: Table" not in e:
                Log.error("could not get table {{table}}", table=name, cause=e)
        return self.create_index(kwargs)

    def delete_index(self, name):
        full_name = concat_field(self.full_name, quote_column(name))
        self.client.delete_table(full_name)

    @override
    def create_index(self, name, schema=None, typed=True, read_only=True, kwargs=None):
        esc_name = quote_column(name)
        full_name = concat_field(self.full_name, esc_name)
        table = bigquery.Table(
            full_name, schema=schema_to_bq_schema({"_id": {STRING_TYPE: {}}})
        )
        self.client.create_table(table)
        return Index(kwargs=kwargs, container=self)


class Index(object):
    @override
    def __init__(self, name, typed, container):
        esc_name = quote_column(name)
        self.full_name = concat_field(container.full_name, esc_name)
        self.table = container.client.get_table(self.full_name)

        self.short_name = name
        self.typed = typed
        self.container = container
        self.schema = parse_schema(self.table.schema)

    def extend(self, rows):
        # VERIFY SCHEMA
        # ALTER SCHEMA
        update = {}
        output = []
        for row in rows:
            typed, more = typed_encode(row, self.schema)
            update.update(more)
            output.append(typed)

        if update:
            new_schema = self.table.schema[:] + schema_to_bq_schema(update)
            self.table.schema = new_schema
            # UPDATE JUST THE schema OF THE TABLE
            self.table = self.container.client.update_table(self.table, ["schema"])

        results = self.container.client.insert_rows(self.table, output)
        Log.note("results = {{results|json}}", results=results)

    def add(self, row):
        self.extend([row])


def parse_schema(schema):
    output = {}
    for e in schema:
        json_type = bq_type_to_json_type[e.field_type]
        name = unquote_column(e.name)
        if e.mode == "REPEATED":
            if e.field_type == "RECORD":
                output[name] = {NESTED_TYPE: parse_schema(e.fields)}
            else:
                output[name] = {NESTED_TYPE: json_type}
        else:
            if e.field_type == "RECORD":
                output[name] = parse_schema(e.fields)
            else:
                output[name] = json_type
    return output


def typed_encode(value, schema):
    if is_many(value):
        output = []
        update = {}
        child_schema = schema.get(quote_column(NESTED_TYPE))
        if not child_schema:
            child_schema = schema[quote_column(NESTED_TYPE)] = {}

        for r in value:
            v, m = typed_encode(r, child_schema)
            output.append(v)
            update.update(m)

        if update:
            return {quote_column(NESTED_TYPE): output}, {NESTED_TYPE: update}
        else:
            return {quote_column(NESTED_TYPE): output}, None
    elif NESTED_TYPE in schema:
        if not value:
            return {quote_column(NESTED_TYPE): []}, None
        else:
            return typed_encode([value], schema)
    elif is_data(value):
        output = {}
        update = {}
        for k, v in value.items():
            child_schema = schema.get(k)
            if not child_schema:
                child_schema = schema[k] = {}
            result, more_update = typed_encode(v, child_schema)
            output[quote_column(k)] = result
            if more_update:
                update.update({k: more_update})
        return output, update
    elif is_text(schema):
        value_type = python_type_to_json_type[value.__class__]
        if schema != value_type:
            Log.error(
                "Can not convert {{existing_type}} to {{expected_type}}",
                existing_type=value_type,
                expected_type=schema,
            )
        return value, None
    elif value is None:
        return {
            quote_column(t): None
            for t, child_schema in schema
        }, None
    else:
        t, json_type = schema_type(value)
        child_schema = schema.get(t)
        update = None
        if not child_schema:
            schema[t] = json_type
            update = {t: json_type}
        return {quote_column(t): value}, update


def schema_type(value):
    jt = python_type_to_json_type[value.__class__]
    return json_type_to_inserter_type[jt], jt


def schema_to_bq_schema(schema):
    output = []
    for t, sub_schema in schema.items():
        bqt = typed_to_bq_type.get(t, {"field_type": "RECORD", "mode": "NULLABLE"})
        if is_text(sub_schema):
            new_field_type = json_type_to_bq_type.get(sub_schema)
            if new_field_type != bqt["field_type"]:
                # OVERRIDE TYPE
                bqt = bqt.copy()
                bqt.field_type = new_field_type
            fields = ()
        else:
            fields = schema_to_bq_schema(sub_schema)
        struct = SchemaField(name=quote_column(t), fields=fields, **bqt)
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
    "DATETIME": NUMBER,
    "TIME": NUMBER,
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

