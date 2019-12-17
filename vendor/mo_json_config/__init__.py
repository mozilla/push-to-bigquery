# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

import os

from mo_dots import is_data, is_list, set_default, unwrap, wrap, is_sequence, coalesce, get_attr
from mo_files import File
from mo_files.url import URL
from mo_future import is_text
from mo_future import text
from mo_json import json2value
from mo_json_config.convert import ini2value
from mo_logs import Except, Log

DEBUG = False


def get_file(file):
    file = File(file)
    if os.sep=="\\":
        return get("file:///" + file.abspath)
    else:
        return get("file://" + file.abspath)


def get(url):
    """
    USE json.net CONVENTIONS TO LINK TO INLINE OTHER JSON
    """
    url = text(url)
    if url.find("://") == -1:
        Log.error("{{url}} must have a prototcol (eg http://) declared", url=url)

    base = URL("")
    if url.startswith("file://") and url[7] != "/":
        if os.sep=="\\":
            base = URL("file:///" + os.getcwd().replace(os.sep, "/").rstrip("/") + "/.")
        else:
            base = URL("file://" + os.getcwd().rstrip("/") + "/.")
    elif url[url.find("://") + 3] != "/":
        Log.error("{{url}} must be absolute", url=url)

    phase1 = _replace_ref(wrap({"$ref": url}), base)  # BLANK URL ONLY WORKS IF url IS ABSOLUTE
    try:
        phase2 = _replace_locals(phase1, [phase1])
        return wrap(phase2)
    except Exception as e:
        Log.error("problem replacing locals in\n{{phase1}}", phase1=phase1, cause=e)


def expand(doc, doc_url="param://", params=None):
    """
    ASSUMING YOU ALREADY PULED THE doc FROM doc_url, YOU CAN STILL USE THE
    EXPANDING FEATURE

    USE mo_json_config.expand({}) TO ASSUME CURRENT WORKING DIRECTORY

    :param doc: THE DATA STRUCTURE FROM JSON SOURCE
    :param doc_url: THE URL THIS doc CAME FROM (DEFAULT USES params AS A DOCUMENT SOURCE)
    :param params: EXTRA PARAMETERS NOT FOUND IN THE doc_url PARAMETERS (WILL SUPERSEDE PARAMETERS FROM doc_url)
    :return: EXPANDED JSON-SERIALIZABLE STRUCTURE
    """
    if doc_url.find("://") == -1:
        Log.error("{{url}} must have a prototcol (eg http://) declared", url=doc_url)

    url = URL(doc_url)
    url.query = set_default(url.query, params)
    phase1 = _replace_ref(doc, url)  # BLANK URL ONLY WORKS IF url IS ABSOLUTE
    phase2 = _replace_locals(phase1, [phase1])
    return wrap(phase2)


def _replace_ref(node, url):
    if url.path.endswith("/"):
        url.path = url.path[:-1]

    if is_data(node):
        ref = None
        output = {}
        for k, v in node.items():
            if k == "$ref":
                ref = URL(v)
            else:
                output[k] = _replace_ref(v, url)

        if not ref:
            return output

        node = output

        if not ref.scheme and not ref.path:
            # DO NOT TOUCH LOCAL REF YET
            output["$ref"] = ref
            return output

        if not ref.scheme:
            # SCHEME RELATIVE IMPLIES SAME PROTOCOL AS LAST TIME, WHICH
            # REQUIRES THE CURRENT DOCUMENT'S SCHEME
            ref.scheme = url.scheme

        # FIND THE SCHEME AND LOAD IT
        if ref.scheme in scheme_loaders:
            new_value = scheme_loaders[ref.scheme](ref, url)
        else:
            raise Log.error("unknown protocol {{scheme}}", scheme=ref.scheme)

        if ref.fragment:
            new_value = get_attr(new_value, ref.fragment)

        DEBUG and Log.note("Replace {{ref}} with {{new_value}}", ref=ref, new_value=new_value)

        if not output:
            output = new_value
        elif is_text(output):
            Log.error("Can not handle set_default({{output}},{{new_value}})", output=output, new_value=new_value)
        else:
            output = unwrap(set_default(output, new_value))

        DEBUG and Log.note("Return {{output}}", output=output)

        return output
    elif is_list(node):
        output = [_replace_ref(n, url) for n in node]
        # if all(p[0] is p[1] for p in zip(output, node)):
        #     return node
        return output

    return node


def _replace_locals(node, doc_path):
    if is_data(node):
        # RECURS, DEEP COPY
        ref = None
        output = {}
        for k, v in node.items():
            if k == "$ref":
                ref = v
            elif k == "$concat":
                if not is_sequence(v):
                    Log.error("$concat expects an array of strings")
                return coalesce(node.get("separator"), "").join(v)
            elif v == None:
                continue
            else:
                output[k] = _replace_locals(v, [v] + doc_path)

        if not ref:
            return output

        # REFER TO SELF
        frag = ref.fragment
        if frag[0] == ".":
            # RELATIVE
            for i, p in enumerate(frag):
                if p != ".":
                    if i>len(doc_path):
                        Log.error("{{frag|quote}} reaches up past the root document",  frag=frag)
                    new_value = get_attr(doc_path[i-1], frag[i::])
                    break
            else:
                new_value = doc_path[len(frag) - 1]
        else:
            # ABSOLUTE
            new_value = get_attr(doc_path[-1], frag)

        new_value = _replace_locals(new_value, [new_value] + doc_path)

        if not output:
            return new_value  # OPTIMIZATION FOR CASE WHEN node IS {}
        else:
            return unwrap(set_default(output, new_value))

    elif is_list(node):
        candidate = [_replace_locals(n, [n] + doc_path) for n in node]
        # if all(p[0] is p[1] for p in zip(candidate, node)):
        #     return node
        return candidate

    return node


###############################################################################
## SCHEME LOADERS ARE BELOW THIS LINE
###############################################################################

def _get_file(ref, url):

    if ref.path.startswith("~"):
        home_path = os.path.expanduser("~")
        if os.sep == "\\":
            home_path = "/" + home_path.replace(os.sep, "/")
        if home_path.endswith("/"):
            home_path = home_path[:-1]

        ref.path = home_path + ref.path[1::]
    elif not ref.path.startswith("/"):
        # CONVERT RELATIVE TO ABSOLUTE
        if ref.path[0] == ".":
            num_dot = 1
            while ref.path[num_dot] == ".":
                num_dot += 1

            parent = url.path.rstrip("/").split("/")[:-num_dot]
            ref.path = "/".join(parent) + ref.path[num_dot:]
        else:
            parent = url.path.rstrip("/").split("/")[:-1]
            ref.path = "/".join(parent) + "/" + ref.path

    path = ref.path if os.sep != "\\" else ref.path[1::].replace("/", "\\")

    try:
        DEBUG and Log.note("reading file {{path}}", path=path)
        content = File(path).read()
    except Exception as e:
        content = None
        Log.error("Could not read file {{filename}}", filename=path, cause=e)

    try:
        new_value = json2value(content, params=ref.query, flexible=True, leaves=True)
    except Exception as e:
        e = Except.wrap(e)
        try:
            new_value = ini2value(content)
        except Exception:
            raise Log.error("Can not read {{file}}", file=path, cause=e)
    new_value = _replace_ref(new_value, ref)
    return new_value


def get_http(ref, url):
    import requests

    params = url.query
    new_value = json2value(requests.get(ref), params=params, flexible=True, leaves=True)
    return new_value


def _get_env(ref, url):
    # GET ENVIRONMENT VARIABLES
    ref = ref.host
    try:
        new_value = json2value(os.environ[ref])
    except Exception as e:
        new_value = os.environ.get(ref)
    return new_value


def _get_param(ref, url):
    # GET PARAMETERS FROM url
    param = url.query
    new_value = param[ref.host]
    return new_value


scheme_loaders = {
    "http": get_http,
    "file": _get_file,
    "env": _get_env,
    "param": _get_param
}

