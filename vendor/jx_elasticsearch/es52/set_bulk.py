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

from jx_elasticsearch.es52 import agg_bulk
from jx_elasticsearch.es52.agg_bulk import write_status, upload, URL_PREFIX
from jx_elasticsearch.es52.expressions import split_expression_by_path, ES52
from jx_elasticsearch.es52.set_format import set_formatters
from jx_elasticsearch.es52.set_op import get_selects, es_query_proto
from jx_elasticsearch.es52.util import jx_sort_to_es_sort
from mo_dots import wrap, Null
from mo_files import TempFile
from mo_json import value2json
from mo_logs import Log, Except
from mo_math import MIN
from mo_math.randoms import Random
from mo_threads import Thread
from mo_times import Date, Timer

DEBUG = True
MAX_CHUNK_SIZE = 2000
MAX_DOCUMENTS = 10 * 1000 * 1000


def is_bulk_set(esq, query):
    # ONLY ACCEPTING ONE DIMENSION AT THIS TIME
    if not agg_bulk.S3_CONFIG:
        return False
    if query.destination not in {"s3", "url"}:
        return False
    if query.format not in {"list"}:
        return False
    if query.groupby or query.edges:
        return False
    return True


def es_bulksetop(esq, frum, query):
    abs_limit = MIN([query.limit, MAX_DOCUMENTS])
    guid = Random.base64(32, extra="-_")

    schema = query.frum.schema
    query_path = schema.query_path[0]
    new_select, split_select = get_selects(query)
    split_wheres = split_expression_by_path(query.where, schema, lang=ES52)
    es_query = es_query_proto(query_path, split_select, split_wheres, schema)
    es_query.size = MIN([query.chunk_size, MAX_CHUNK_SIZE])
    es_query.sort = jx_sort_to_es_sort(query.sort, schema)
    if not es_query.sort:
        es_query.sort = ["_doc"]


    formatter, setup_row_formatter, mime_type = set_formatters[query.format]

    Thread.run(
        "Download " + guid,
        extractor,
        guid,
        abs_limit,
        esq,
        es_query,
        setup_row_formatter(new_select, query),
        parent_thread=Null,
    )

    output = wrap(
        {
            "url": URL_PREFIX / (guid + ".json"),
            "status": URL_PREFIX / (guid + ".status.json"),
            "meta": {"format": "list", "es_query": es_query, "limit": abs_limit},
        }
    )
    return output


def extractor(guid, abs_limit, esq, es_query, row_formatter, please_stop):
    start_time = Date.now()
    total = 0
    write_status(
        guid,
        {
            "status": "starting",
            "limit": abs_limit,
            "start_time": start_time,
            "timestamp": Date.now(),
        },
    )

    try:
        with TempFile() as temp_file:
            with open(temp_file.abspath, "wb") as output:
                output.write(b"[\n")
                comma = b""

                result = esq.es.search(es_query, scroll="5m")

                while not please_stop:
                    scroll_id = result._scroll_id
                    hits = result.hits.hits
                    if not hits:
                        break

                    chunk_limit = abs_limit - total
                    hits = hits[:chunk_limit]

                    for doc in hits:
                        output.write(comma)
                        comma = b",\n"
                        output.write(value2json(row_formatter(doc)).encode("utf8"))

                    total += len(hits)
                    DEBUG and Log.note(
                        "{{num}} of {{total}} downloaded",
                        num=total,
                        total=result.hits.total,
                    )
                    if total >= abs_limit:
                        break
                    write_status(
                        guid,
                        {
                            "status": "working",
                            "row": total,
                            "rows": result.hits.total,
                            "start_time": start_time,
                            "timestamp": Date.now(),
                        },
                    )
                    with Timer("get more", verbose=DEBUG):
                        result = esq.es.scroll(scroll_id)

                output.write(b"\n]")

            write_status(
                guid,
                {
                    "status": "uploading to s3",
                    "rows": total,
                    "start_time": start_time,
                    "timestamp": Date.now(),
                },
            )
            upload(guid + ".json", temp_file)
        if please_stop:
            Log.error("shutdown requested, did not complete download")
        DEBUG and Log.note("Done. {{total}} uploaded", total=total)
        write_status(
            guid,
            {
                "ok": True,
                "status": "done",
                "rows": total,
                "start_time": start_time,
                "end_time": Date.now(),
                "timestamp": Date.now(),
            },
        )
    except Exception as e:
        e = Except.wrap(e)
        write_status(
            guid,
            {
                "ok": False,
                "status": "error",
                "error": e,
                "start_time": start_time,
                "end_time": Date.now(),
                "timestamp": Date.now(),
            },
        )
        Log.warning("Could not extract", cause=e)
